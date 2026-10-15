"""Weekly volume review: raise, hold or step back each set-up inbox's daily
campaign limit. Pure: no network, nothing written.

Spec: docs/superpowers/specs/2026-10-15-weekly-volume-review-design.md.
Steps and maximums live in inbox_standard.json (weekly_step, max_daily).

Three signals per inbox, each GOOD, SLIP or WAIT (not enough data):
  * placement: newest counting test passed (80%+) and an older one, 7+ days
    before it, passed too; newest failed = SLIP;
  * bounces: under 2% of 20+ campaign sends over 14 days;
  * warmup reputation: the type's healthy level (90%, SMTP 70%).
All GOOD and no change in 7 days = raise one step (never above the maximum).
Two or more SLIP = step back one step (never below the start; no cooldown).
Anything else = hold.
"""
from __future__ import annotations

import json
from collections import Counter
from datetime import date

from smartlead import inbox_standard as st
from smartlead import kill_rule as kr

GOOD, SLIP, WAIT = "good", "slip", "wait"
RAISE, HOLD, STEP_BACK, SKIP = "raise", "hold", "step back", "skip"
BOUNCE_LIMIT = 2.0
MIN_SENT = 20
COOLDOWN_DAYS = 7
NOT_OURS = "not set up by the bot"


def _day(d: date | str) -> str:
    d = date.fromisoformat(d) if isinstance(d, str) else d
    return f"{d.day} {d.strftime('%b')}"


def _num(x: float) -> str:
    return f"{round(x, 1):g}"


def placement_signal(tests: list[dict], today: date) -> tuple[str, str]:
    ct = kr.counting_tests(tests)
    fresh = [(t, g) for t, g in ct if (today - date.fromisoformat(t["date"])).days <= kr.FRESH_DAYS]
    if not fresh:
        return WAIT, f"no placement test in {kr.FRESH_DAYS} days"
    t1, g1 = fresh[0]
    if g1 < kr.GOOD_AT:
        return SLIP, f"failed its last test ({g1:.0f}% inbox, {_day(t1['date'])})"
    older = [(t, g) for t, g in ct[1:]
             if (date.fromisoformat(t1["date"]) - date.fromisoformat(t["date"])).days >= kr.MIN_DAYS_BETWEEN]
    if not older:
        return WAIT, "one passing test so far"
    if older[0][1] < kr.GOOD_AT:
        return WAIT, "passed its last test after failing the one before"
    return GOOD, f"passed 2 tests ({g1:.0f}%, {older[0][1]:.0f}%)"


def bounce_signal(sent: int, bounced: int) -> tuple[str, str]:
    sent, bounced = int(sent or 0), int(bounced or 0)
    if sent < MIN_SENT:
        return WAIT, f"only {sent} sent in 14 days"
    pct = 100.0 * bounced / sent
    if pct >= BOUNCE_LIMIT:
        return SLIP, f"bounced {_num(pct)}% of {sent} sent"
    return GOOD, f"bounced {_num(pct)}% of {sent} sent"


def reputation_signal(rep, kind: str = "google") -> tuple[str, str]:
    r = st._int(rep)
    if r is None:
        return WAIT, "no warmup reputation"
    if r < st.good_reputation(kind):
        return SLIP, f"warmup reputation {r}%"
    return GOOD, f"warmup reputation {r}%"


def signals(tests: list[dict], sent: int, bounced: int, rep, today: date, kind: str = "google") -> dict:
    return {"placement": placement_signal(tests, today), "bounces": bounce_signal(sent, bounced),
            "reputation": reputation_signal(rep, kind)}


def decide(kind: str, limit: int, sig: dict, *, last_change: date | None, today: date,
           verdict: str | None = None) -> dict:
    s = st.standard_for(kind)
    step, start, top = int(s.get("weekly_step") or 0), s["max_email_per_day"], s.get("max_daily", s["max_email_per_day"])
    out = {"kind": kind, "from": limit, "to": limit}
    if verdict == kr.KILL:
        return {**out, "action": SKIP, "why": "on the kill list; see Kill warnings"}
    if not step:
        return {**out, "action": SKIP, "why": f"{s['label']}: no weekly step set"}
    slips = [why for state, why in sig.values() if state == SLIP]
    waits = [why for state, why in sig.values() if state == WAIT]
    if len(slips) >= 2:
        to = max(start, limit - step)
        if to >= limit:
            return {**out, "action": HOLD, "why": "already at the starting limit; " + "; ".join(slips)}
        return {**out, "action": STEP_BACK, "to": to, "why": "; ".join(slips)}
    if slips or waits:
        return {**out, "action": HOLD, "why": "; ".join(slips + waits)}
    if last_change and (today - last_change).days < COOLDOWN_DAYS:
        return {**out, "action": HOLD, "why": f"changed {(today - last_change).days} days ago"}
    if limit >= top:
        return {**out, "action": HOLD, "why": f"at the maximum ({top})"}
    return {**out, "action": RAISE, "to": min(top, limit + step), "why": "all signals good"}


def eligible(account: dict, kind: str, today: date, *, sending: bool, approved: int | None) -> str | None:
    """None when the review should judge this inbox, else why not."""
    names = [str(t.get("tag_name") or t.get("name") or "") for t in account.get("tags") or []]
    if st.SETUP_TAG not in names:
        return NOT_OURS
    ready = st.ready_date(account, kind)
    if ready is None:
        return "no warmup start date"
    if today < ready:
        return f"warming until {_day(ready)}"
    if not sending:
        return "no campaign sent from it in 7 days"
    want = approved if approved is not None else st.standard_for(kind)["max_email_per_day"]
    cur = st._int(account.get("message_per_day"))
    if cur != want:
        return f"changed by hand to {cur} (set {want})"
    return None


# Message -----------------------------------------------------------------------

def _label(kind: str) -> str:
    return st.standard_for(kind)["label"]


def _plural(n: int, word: str = "inbox") -> str:
    return f"{n} {word}{'es' if n != 1 and word.endswith('x') else ('s' if n != 1 else '')}"


def _changes(rows: list[dict], action: str) -> str:
    picked = [r for r in rows if r["action"] == action]
    if action == RAISE:
        groups = Counter((r["kind"], r["from"], r["to"]) for r in picked)
        parts = [f"{_label(k)} {a} -> {b}" for (k, a, b) in groups]
        if len(groups) > 1:
            parts = [f"{n} {_label(k)} {a} -> {b}" for (k, a, b), n in groups.items()]
    else:
        groups = Counter((r["email"].split("@")[-1], r["kind"], r["from"], r["to"]) for r in picked)
        parts = [f"{d} {_label(k)} {a} -> {b}" + (f" ({n} inboxes)" if n > 1 else "")
                 for (d, k, a, b), n in groups.items()]
    return f"{action} {len(picked)} ({', '.join(parts)})"


def client_lines(rows: list[dict]) -> list[tuple[str, str, int]]:
    """[(client, line, inboxes to change)], busiest client first."""
    out = []
    for client in sorted({r["client"] for r in rows}):
        mine = [r for r in rows if r["client"] == client]
        parts = [_changes(mine, a) for a in (RAISE, STEP_BACK) if any(r["action"] == a for r in mine)]
        held = sum(r["action"] == HOLD for r in mine)
        if held:
            parts.append(f"hold {held}")
        changing = sum(r["action"] in (RAISE, STEP_BACK) for r in mine)
        out.append((client, f"{client}: " + " · ".join(parts), changing))
    return sorted(out, key=lambda x: -x[2])


def _headline(rows: list[dict], today: date) -> str:
    n = Counter(r["action"] for r in rows)
    if not (n[RAISE] or n[HOLD] or n[STEP_BACK]):
        return f"Volume review, {_day(today)}: nothing to change this week"
    return (f"Volume review, {_day(today)}: raise {_plural(n[RAISE])}, hold {n[HOLD]}, "
            f"step back {n[STEP_BACK]}")


def _notes(rows: list[dict], waiting: dict) -> list[str]:
    out = []
    held = [r for r in rows if r["action"] == HOLD]
    if held:
        out.append("Held: " + "; ".join(f"{r['email']} ({r['why']})" for r in held[:10])
                   + (f"; and {len(held) - 10} more" if len(held) > 10 else ""))
    if waiting.get("warming"):
        first = f" (first ready {_day(waiting['first_ready'])})" if waiting.get("first_ready") else ""
        out.append(f"{_plural(waiting['warming'])} {'are' if waiting['warming'] != 1 else 'is'} still warming{first}.")
    if waiting.get("no_campaign"):
        out.append(f"{_plural(waiting['no_campaign'])} past their ready date sent nothing from a campaign in 7 days.")
    if waiting.get("rotate_ip"):
        out.append("SMTP reputation under 50%, ask ScaledMail to rotate the IP: " + ", ".join(waiting["rotate_ip"]))
    for line in waiting.get("by_hand") or []:
        out.append(f"Changed by hand, left alone: {line}")
    for line in waiting.get("skipped") or []:
        out.append(f"Not reviewed: {line}")
    return out


def review_text(rows: list[dict], waiting: dict, today: date) -> str:
    lines = [_headline(rows, today)] + [line for _, line, _ in client_lines(rows)] + _notes(rows, waiting)
    if any(r["action"] in (RAISE, STEP_BACK) for r in rows):
        lines.append("Nothing changes until someone presses Apply.")
    return "\n".join(lines)


def review_blocks(rows: list[dict], waiting: dict, today: date, review_id: str) -> list[dict]:
    blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": f"*{_headline(rows, today)}*"}}]
    for i, (client, line, changing) in enumerate(client_lines(rows)):
        b = {"type": "section", "text": {"type": "mrkdwn", "text": line}}
        if changing:
            b["accessory"] = {
                "type": "button", "action_id": f"volume_apply_{i}", "style": "primary",
                "text": {"type": "plain_text", "text": f"Apply {client}"[:75]},
                "value": json.dumps({"r": review_id, "c": client}),
                "confirm": {"title": {"type": "plain_text", "text": "Change daily limits?"},
                            "text": {"type": "mrkdwn", "text": f"Set the new daily limit on {_plural(changing)} "
                                                              f"for {client} in Smartlead, as listed?"},
                            "confirm": {"type": "plain_text", "text": "Apply"},
                            "deny": {"type": "plain_text", "text": "Cancel"}}}
        blocks.append(b)
    notes = _notes(rows, waiting)
    if any(r["action"] in (RAISE, STEP_BACK) for r in rows):
        notes.append("Nothing changes until someone presses Apply (approvers only).")
    if notes:
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": "\n".join(notes)[:2900]}]})
    return blocks


# Apply ---------------------------------------------------------------------------

REVIEW_MAX_AGE_DAYS = 7


async def apply_review(sls: dict, store, review_id: str, client: str, *, by: str, today: date) -> dict:
    """Write one client's raises and step-backs from a saved review: each inbox
    is re-read first (a limit moved since the review is skipped), written, read
    back, and only a matching read-back is recorded as the approved limit.
    ``sls`` = {Smartlead account name: client}."""
    from smartlead.notify import redact
    rev = store.review(review_id)
    if not rev:
        return {"error": f"No volume review saved for {review_id}."}
    if (today - date.fromisoformat(review_id)).days > REVIEW_MAX_AGE_DAYS:
        return {"error": f"That review ({_day(review_id)}) is older than {REVIEW_MAX_AGE_DAYS} days; use this week's."}
    done = (rev.get("applied") or {}).get(client)
    if done:
        return {"error": f"{client} was already applied by <@{done.get('by')}>."}
    rows = [r for r in rev.get("rows") or [] if r["client"] == client and r["action"] in (RAISE, STEP_BACK)]
    out = {"client": client, "changed": [], "skipped": [], "failed": []}
    for account in sorted({r["account"] for r in rows}):
        sl = sls.get(account)
        mine = [r for r in rows if r["account"] == account]
        if sl is None:
            out["failed"] += [f"{r['email']}: Smartlead account {account} not available" for r in mine]
            continue
        try:
            now = {int(a["id"]): st._int(a.get("message_per_day")) for a in await sl.list_email_accounts()}
        except Exception as exc:  # noqa: BLE001 - reported, nothing written
            out["failed"] += [f"{r['email']}: could not read Smartlead ({redact(str(exc))[:120]})" for r in mine]
            continue
        for r in mine:
            cur = now.get(int(r["id"]))
            if cur != r["from"]:
                out["skipped"].append(f"{r['email']}: now {cur}, not {r['from']}")
                continue
            try:
                await sl.update_email_account(r["id"], {"max_email_per_day": r["to"]})
                back = st._int((await sl.get_email_account(r["id"])).get("message_per_day"))
            except Exception as exc:  # noqa: BLE001 - one inbox must not stop the rest
                out["failed"].append(f"{r['email']}: {redact(str(exc))[:120]}")
                continue
            if back != r["to"]:
                out["failed"].append(f"{r['email']}: Smartlead shows {back} after the write")
                continue
            store.approve_limit(r["email"], r["to"], by=by, action=r["action"], on=today)
            out["changed"].append(f"{r['email']} {r['from']} -> {r['to']}")
    store.mark_applied(review_id, client, by, {k: len(v) for k, v in out.items() if k != "client"})
    return out
