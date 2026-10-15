"""Weekly volume review, judged per DOMAIN: raise, hold or step back the daily
campaign limit of every inbox on it. Pure apart from apply_rows (Smartlead
writes through the client it is given).

Spec: docs/superpowers/specs/2026-10-15-weekly-volume-review-design.md (rule
revised after the 15 Oct research). Steps and maximums are ScaledMail's
weekly plan, from inbox_standard.json (weekly_step, max_daily).

Why per domain: mailbox providers keep reputation per domain (and per IP for
SMTP), and at 5-20 sends a day one inbox's numbers are mostly chance.

Signals per domain, each GOOD, SLIP, WAIT (not enough data) or BAD:
  * placement: each inbox's newest counting test since the last change;
    one failing inbox = SLIP, two (or a pooled 40+ seeds under 80%) = BAD;
  * bounces: pooled over 14 days, 100+ sends; 2%+ SLIP, 4%+ BAD;
  * warmup reputation: an alarm only (any inbox under its type's level = SLIP);
  * kill list: any inbox SHOULD KILL = BAD;
  * usage: the limit is reached on most sending days (raises only).
Any BAD = step back one step (no cooldown, never below the start).
Any SLIP or WAIT = hold. Else raise one step if 7+ days since the last change
and under the maximum.
"""
from __future__ import annotations

from datetime import date

from smartlead import inbox_standard as st
from smartlead import kill_rule as kr

GOOD, SLIP, WAIT, BAD = "good", "slip", "wait", "bad"
RAISE, HOLD, STEP_BACK = "raise", "hold", "step back"
BOUNCE_SLIP, BOUNCE_BAD, MIN_SENT = 2.0, 4.0, 100
POOL_MIN_SEEDS = 40
USAGE_DAYS_AT_LIMIT = 0.6          # share of sending days an inbox sends (nearly) its limit
COOLDOWN_DAYS = 7
NOT_OURS = "not managed by the bot"


def _day(d: date | str) -> str:
    d = date.fromisoformat(d) if isinstance(d, str) else d
    return f"{d.day} {d.strftime('%b')}"


def _num(x: float) -> str:
    return f"{round(x, 1):g}"


def _plural(n: int, word: str = "inbox") -> str:
    return f"{n} {word}{'es' if n != 1 and word.endswith('x') else ('s' if n != 1 else '')}"


# Signals ---------------------------------------------------------------------

def placement_signal(tests_by_email: dict[str, list[dict]], today: date, *, since: date | None) -> tuple[str, str]:
    """Each inbox's newest counting test, fresh and newer than the last change."""
    newest = []
    for tests in tests_by_email.values():
        for t, g in kr.counting_tests(tests):
            d = date.fromisoformat(t["date"])
            if (today - d).days <= kr.FRESH_DAYS and (since is None or d > since):
                newest.append((t, g))
            break
    n = len(tests_by_email)
    if not newest or len(newest) * 2 < n:
        after = f" since {_day(since)}" if since else ""
        return WAIT, f"only {len(newest)} of {n} inboxes tested{after}"
    failed = sum(g < kr.GOOD_AT for _, g in newest)
    seeds = sum(t["seeds"] for t, _ in newest)
    pooled = 100.0 * sum(t["seeds"] - t["spam"] for t, _ in newest) / seeds
    if failed >= 2 or (seeds >= POOL_MIN_SEEDS and pooled < kr.GOOD_AT):
        return BAD, f"{failed} of {len(newest)} tested inboxes failed; {pooled:.0f}% of {seeds} test emails reached the inbox"
    if failed:
        return SLIP, f"{failed} of {len(newest)} tested inboxes failed its test"
    return GOOD, f"{len(newest)} of {n} inboxes passed ({pooled:.0f}% inbox)"


def bounce_signal(sent: int, bounced: int) -> tuple[str, str]:
    sent, bounced = int(sent or 0), int(bounced or 0)
    if sent < MIN_SENT:
        return WAIT, f"only {sent} sent in 14 days (bounces judged from {MIN_SENT})"
    pct = 100.0 * bounced / sent
    state = BAD if pct >= BOUNCE_BAD else SLIP if pct >= BOUNCE_SLIP else GOOD
    return state, f"bounced {_num(pct)}% of {sent} sent"


def reputation_signal(reps: list, kind: str) -> tuple[str, str]:
    vals = [r for r in (st._int(x) for x in reps) if r is not None]
    if not vals:
        return GOOD, "no warmup reputation"
    low = min(vals)
    if low < st.good_reputation(kind):
        return SLIP, f"warmup reputation down to {low}%"
    return GOOD, f"warmup reputation {low}%+"


def usage_signal(days_per_inbox: list[list[int]], limit: int) -> tuple[str, str]:
    """Raise only a domain that uses its limit: an inbox counts as full on a
    day it sent the limit or one under."""
    days = [d for inbox in days_per_inbox for d in inbox]
    if not days:
        return WAIT, "no daily sending numbers"
    full = sum(d >= limit - 1 for d in days) / len(days)
    if full >= USAGE_DAYS_AT_LIMIT:
        return GOOD, f"at its limit on {full:.0%} of sending days"
    return SLIP, f"sends about {round(sum(days) / len(days))} of {limit} a day, so more room would not be used"


def kill_signal(verdicts: list[str]) -> tuple[str, str]:
    n = sum(v == kr.KILL for v in verdicts)
    return (BAD, f"{_plural(n)} on the kill list") if n else (GOOD, "")


# Decision --------------------------------------------------------------------

def decide(kind: str, limits: list[int], sig: dict, *, last_change: date | None, today: date) -> dict:
    s = st.standard_for(kind)
    step, start, top = int(s.get("weekly_step") or 0), s["max_email_per_day"], s.get("max_daily", s["max_email_per_day"])
    out = {"kind": kind, "from": list(limits), "to": list(limits)}
    bad = [why for state, why in sig.values() if state == BAD]
    if bad:
        to = [max(start, x - step) if x > start else x for x in limits]
        if to == list(limits):
            return {**out, "action": HOLD, "why": "already at the starting limit; " + "; ".join(bad)}
        return {**out, "action": STEP_BACK, "to": to, "why": "; ".join(bad)}
    held = [why for state, why in sig.values() if state in (SLIP, WAIT)]
    if held:
        return {**out, "action": HOLD, "why": "; ".join(held)}
    if not step:
        return {**out, "action": HOLD, "why": f"{s['label']}: no weekly step set"}
    if last_change and (today - last_change).days < COOLDOWN_DAYS:
        return {**out, "action": HOLD, "why": f"changed {(today - last_change).days} days ago"}
    to = [max(x, min(top, x + step)) for x in limits]
    if to == list(limits):
        return {**out, "action": HOLD, "why": f"at the maximum ({top})"}
    return {**out, "action": RAISE, "to": to, "why": "; ".join(why for _, why in sig.values() if why)}


def eligible(account: dict, kind: str, today: date, *, sending: bool, approved: int | None,
             managed: bool = False) -> str | None:
    """None when the review should judge this inbox, else why not. ``managed``:
    its domain is in the pilot (inboxes the bot did not set up)."""
    names = [str(t.get("tag_name") or t.get("name") or "") for t in account.get("tags") or []]
    if not managed and st.SETUP_TAG not in names:
        return NOT_OURS
    ready = st.ready_date(account, kind)
    if ready is None and not managed:
        return "no warmup start date"
    if ready and today < ready:
        return f"warming until {_day(ready)}"
    if not sending:
        return "no campaign sent from it in 7 days"
    want = approved if approved is not None else st.standard_for(kind)["max_email_per_day"]
    cur = st._int(account.get("message_per_day"))
    if cur != want:
        return f"changed by hand to {cur} (set {want})"
    return None


# Message ------------------------------------------------------------------------

def _label(kind: str) -> str:
    return st.standard_for(kind)["label"]


def _move(r: dict) -> str:
    frm = "/".join(map(str, sorted(set(r["from"]))))
    to = "/".join(map(str, sorted(set(r["to"]))))
    return f"{r['domain']} {_label(r['kind'])} {frm} -> {to} ({_plural(len(r['from']))})"


def review_text(rows: list[dict], waiting: dict, today: date, *, applied: dict[str, bool]) -> str:
    """``applied`` = {domain: written and read back} for the pilot domains changed this run."""
    lines = [f"Volume review, {_day(today)} (by domain, ScaledMail's weekly steps)"]
    pilot = [r for r in rows if r.get("pilot") and r["action"] in (RAISE, STEP_BACK)]
    done = [r for r in pilot if applied.get(r["domain"])]
    failed = [r for r in pilot if r["domain"] in applied and not applied[r["domain"]]]
    if done:
        lines.append("Pilot, changed now: " + "; ".join(
            ("" if r["action"] == RAISE else "STEPPED BACK ") + _move(r) + f" ({r['why']})" for r in done))
    if failed:
        lines.append("Pilot, NOT changed (see below): " + "; ".join(r["domain"] for r in failed))
    dry = [r for r in pilot if r["domain"] not in applied]
    if dry:
        lines.append("Pilot, would change (dry run): " + "; ".join(
            ("" if r["action"] == RAISE else "step back ") + _move(r) for r in dry))
    ctrl = [r for r in rows if r.get("control")]
    if ctrl:
        lines.append("Comparison, kept flat: " + "; ".join(
            f"{r['domain']} {_label(r['kind'])} {'/'.join(map(str, sorted(set(r['from']))))} ("
            + "; ".join(why for k, (_, why) in (r.get("signals") or {}).items() if k in ("placement", "bounces") and why)
            + ")" for r in ctrl))
    untested = [r["domain"] for r in rows if (r.get("pilot") or r.get("control"))
                and ((r.get("signals") or {}).get("placement") or [None])[0] == WAIT]
    if untested:
        lines.append("No placement test yet, add to Monday's test: " + ", ".join(untested))
    suggested = [r for r in rows if not r.get("pilot") and not r.get("control") and r["action"] in (RAISE, STEP_BACK)]
    if suggested:
        lines.append("Suggested only (not in the pilot): " + "; ".join(f"{r['action']} {_move(r)}" for r in suggested))
    held = [r for r in rows if r["action"] == HOLD and not r.get("control")]
    for r in held[:15]:
        lines.append(f"Hold: {r['domain']} ({r['why']})")
    if len(held) > 15:
        lines.append(f"Hold: {len(held) - 15} more domains")
    if not rows:
        lines.append("No domain to judge this week.")
    return "\n".join(lines + _notes(waiting))


def _notes(waiting: dict) -> list[str]:
    out = []
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
    for line in waiting.get("apply_problems") or []:
        out.append(f":warning: {line}")
    return out


# Apply (pilot domains only) -------------------------------------------------------

async def apply_rows(sls: dict, store, rows: list[dict], *, by: str, today: date) -> dict:
    """Write each domain's new limits: re-read every inbox first (a limit moved
    since the review is left alone), write, read back, and record only a
    matching read-back as the approved limit. ``sls`` = {account: client}.
    Returns {domain: {"changed": [...], "skipped": [...], "failed": [...]}}."""
    from smartlead.notify import redact
    out: dict[str, dict] = {}
    now_by_account: dict[str, dict] = {}
    for r in rows:
        res = out.setdefault(r["domain"], {"changed": [], "skipped": [], "failed": []})
        sl = sls.get(r["account"])
        if sl is None:
            res["failed"].append(f"{r['domain']}: Smartlead account {r['account']} not available")
            continue
        if r["account"] not in now_by_account:
            try:
                now_by_account[r["account"]] = {int(a["id"]): st._int(a.get("message_per_day"))
                                                for a in await sl.list_email_accounts()}
            except Exception as exc:  # noqa: BLE001 - reported, nothing written
                res["failed"].append(f"{r['domain']}: could not read Smartlead ({redact(str(exc))[:120]})")
                continue
        now = now_by_account[r["account"]]
        for box, frm, to in zip(r["inboxes"], r["from"], r["to"]):
            if frm == to:
                continue
            cur = now.get(int(box["id"]))
            if cur != frm:
                res["skipped"].append(f"{box['email']}: now {cur}, not {frm}")
                continue
            try:
                await sl.update_email_account(box["id"], {"max_email_per_day": to})
                back = st._int((await sl.get_email_account(box["id"])).get("message_per_day"))
            except Exception as exc:  # noqa: BLE001 - one inbox must not stop the rest
                res["failed"].append(f"{box['email']}: {redact(str(exc))[:120]}")
                continue
            if back != to:
                res["failed"].append(f"{box['email']}: Smartlead shows {back} after the write")
                continue
            store.approve_limit(box["email"], to, by=by, action=r["action"], on=today)
            res["changed"].append(f"{box['email']} {frm} -> {to}")
    return out
