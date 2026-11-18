"""Warm-up repair tracker (t16, 17 Nov 2026). Manveen, 16 Nov: "make sure we are of track of improving them and
change warmup's stuff and trak thye imporve also work on simmilar inboxes in all clients".

Every inbox of PL, Melior or BettrData whose warm-up reputation is under the All Inboxes cutoff
(config.MIN_WARMUP_REP_PCT, 90, the inbox list's low_rep cutoff; one cutoff for every inbox type, parent 17 Nov) has one
document in Mongo ``warmup_repair`` until it recovers or leaves. Fleet watch's daily reading (inbox_daily ``rep``) updates it:

  * recovered: the latest reading is at or above the cutoff and warm-up is not blocked;
  * worse: the latest reading is 3+ points below the reputation at the start (a flag, not a status);
  * no_improvement: 21+ days in repair and the latest reading not above the start + 3. These are cancel candidates
    in the digest; cancelling is Manveen's call;
  * gone: the inbox left the watched inboxes (same evidence fleet watch uses to close a case).

Report-only. Nothing here changes a warm-up setting, cancels, deletes or calls Smartlead: the lead changed the warm-up
of the first ones by hand, with Manveen's approval, and seeded them from local files (warmup_repair_cli.py). Inboxes the
bot enrols by itself carry settings_after None. A recovered inbox is freed by the inbox list on its own (low_rep
clears; processing.py + usable.py), nothing here holds it back.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

from smartlead import config
from smartlead import inbox_standard as st

WORSE_BY = 3
IMPROVE_BY = 3
NO_IMPROVEMENT_DAYS = 21
# inbox_daily's client label -> the name the digest uses. Other labels (OSC, client_id 145916) are never enrolled.
CLIENTS = {"Precise Leads": "PL", "Melior": "Melior", "Bettrdata": "BettrData"}
_SEED_CLIENT = {"PL": "Precise Leads", "PRECISE LEADS": "Precise Leads", "MELIOR": "Melior", "BETTRDATA": "Bettrdata"}
SETTING_KEYS = ("status", "warmup_min_count", "warmup_max_count", "max_email_per_day", "reply_rate",
                "daily_reply_limit", "is_rampup_enabled", "rampup_value", "auto_adjust_warmup",
                "send_warmups_only_on_weekdays", "warmup_key_id")
IN_REPAIR = ("repairing", "no_improvement")


def level(kind: str | None = None) -> int:
    """The reputation counted as recovered: the All Inboxes cutoff (90) for every inbox type. inbox_standard's
    per-type good_reputation (SMTP 70) is not used here (parent, 17 Nov: one recovery cutoff)."""
    return int(config.MIN_WARMUP_REP_PCT)


def _rep(x) -> int | None:
    try:
        return int(round(float(str(x).replace("%", ""))))
    except (TypeError, ValueError):
        return None


def _kind(t: str | None) -> str:
    return st.inbox_kind({"type": t}, 1)


def _client(c: str | None) -> str:
    c = str(c or "")
    return c if c in CLIENTS else _SEED_CLIENT.get(c.upper(), c)


def _why_blocked(reason: str | None) -> str:
    r = str(reason or "").lower()
    if not r or "authentic" in r or "credential" in r:
        return "warm-up blocked: credentials must be fixed first"
    return f"warm-up blocked: {reason}"


def new_doc(email: str, account: str, client: str, typ: str | None, kind: str, started: str, rep: int | None, *,
            before: dict | None, after: dict | None, why: str | None, blocked: bool, by: str) -> dict:
    return {"email": email, "account": account, "client": client, "type": typ, "kind": kind, "started": started,
            "rep_at_start": rep, "settings_before": before, "settings_after": after, "why_not_changed": why,
            "blocked": bool(blocked), "status": "repairing", "enrolled_by": by, "recovered_on": None,
            "latest_rep": rep, "latest_on": None, "gone_on": None}


def seed_docs(changes: dict, before: dict, today: date) -> list[dict]:
    """One doc per inbox in the lead's before-snapshot (``before['inboxes']``), with the settings change from
    ``changes`` ({email: {on, before, after, ok}}) or why the settings were left alone. Pure."""
    by_email = {str(k).lower(): v for k, v in (changes or {}).items()}
    out = []
    for x in (before or {}).get("inboxes") or []:
        email = str(x.get("email") or x.get("from_email") or "").lower()
        if not email:
            continue
        w = x.get("warmup_details") or {}
        c = by_email.get(email)
        blocked = bool(w.get("is_warmup_blocked") or w.get("blocked_reason"))
        current = {k: w[k] for k in SETTING_KEYS if k in w}
        if c and c.get("ok"):
            settings_before, after, why = c.get("before") or current, c.get("after"), None
        elif c:
            settings_before, after, why = c.get("before") or current, None, \
                "the settings change did not apply; settings left as they were"
        elif blocked:
            settings_before, after, why = current, None, _why_blocked(w.get("blocked_reason"))
        else:
            settings_before, after = current, None
            why = (f"already at {w.get('warmup_max_count')}/day, {w.get('reply_rate')}% reply; "
                   "settings left as they were")
        started = str((c or {}).get("on") or today.isoformat())[:10]
        out.append(new_doc(email, str(x.get("account") or ""), _client(x.get("client")), x.get("type"),
                           _kind(x.get("type")), started, _rep(x.get("warmup_reputation") or (c or {}).get("rep_before")),
                           before=settings_before, after=after, why=why, blocked=blocked, by="seed"))
    return out


def worse(d: dict) -> bool:
    rep, start = d.get("latest_rep"), d.get("rep_at_start")
    return rep is not None and start is not None and rep <= start - WORSE_BY


def update(d: dict, reading: dict | None, today: date) -> dict:
    """The doc after today's reading (None = no reading today: the last one stands). Pure."""
    d = dict(d)
    if reading:
        if reading.get("rep") is not None:
            d["latest_rep"], d["latest_on"] = _rep(reading["rep"]), reading.get("date") or today.isoformat()
        d["kind"] = reading.get("kind") or d.get("kind")
        if reading.get("warmup_blocked") is not None:
            d["blocked"] = bool(reading["warmup_blocked"])
        d["gone_on"] = None
    rep, start = d.get("latest_rep"), d.get("rep_at_start")
    if rep is not None and rep >= level(d.get("kind")) and not d.get("blocked"):
        d["status"] = "recovered"
        d["recovered_on"] = d.get("recovered_on") or today.isoformat()
        return d
    d["recovered_on"] = None
    days = (today - date.fromisoformat(str(d["started"])[:10])).days
    stuck = rep is None or start is None or rep <= start + IMPROVE_BY
    d["status"] = "no_improvement" if days >= NO_IMPROVEMENT_DAYS and stuck and not d.get("blocked") else "repairing"
    return d


def _left(email: str, roster: dict | None) -> bool:
    from smartlead import fleet as fl
    return fl.left({"domain": email.split("@")[-1], "scope": "inbox", "target": email}, roster)


def run(docs: list[dict], records: list[dict], today: date, *, read: set[str], roster: dict | None
        ) -> tuple[list[dict], list[dict]]:
    """(every doc after today's readings, the docs enrolled today). A doc missing from today's records turns
    'gone' only when its account was read in full and the roster shows the inbox left. Pure."""
    by_email = {r["email"]: r for r in records}
    out = []
    for d in docs:
        e = d["email"]
        if e in by_email:
            out.append(update(d, by_email[e], today))
        elif d.get("status") == "gone":
            out.append(d)
        elif d.get("account") in read and _left(e, roster):
            out.append({**d, "status": "gone", "gone_on": today.isoformat(), "recovered_on": None})
        else:
            out.append(update(d, None, today))
    known = {d["email"] for d in docs}
    new = []
    for r in records:
        rep = _rep(r.get("rep"))
        if (r["email"] in known or r.get("client") not in CLIENTS or r.get("connected") is not True or rep is None
                or rep >= level(r.get("kind")) or r.get("warmup_by")):      # warmup_by: its vendor warms it (t248)
            continue
        known.add(r["email"])
        new.append(update(new_doc(r["email"], r.get("account") or "", r["client"], None, r.get("kind") or "google",
                                  today.isoformat(), rep, before=None, after=None,
                                  why="report-only: enrolled by the bot, warm-up settings unchanged",
                                  blocked=bool(r.get("warmup_blocked")), by="auto"), r, today))
    return out + new, new


# --- the digest -----------------------------------------------------------------------------------------------------

def _day(d) -> str:
    d = date.fromisoformat(str(d)[:10])
    return f"{d.day} {d.strftime('%b')}"


def _this_week(d: str | None, today: date) -> bool:
    return bool(d) and today - timedelta(days=6) <= date.fromisoformat(str(d)[:10]) <= today


def _n(k: int) -> str:
    return f"{k} inbox{'es' if k != 1 else ''}"


def week_line(client: str, docs: list[dict], today: date) -> str | None:
    """'Warm-up repair, BettrData: 30 repairing, 4 recovered this week, 2 worse.' None when there is nothing."""
    rep = [d for d in docs if d.get("status") == "repairing"]
    stuck = [d for d in docs if d.get("status") == "no_improvement"]
    rec = [d for d in docs if d.get("status") == "recovered" and _this_week(d.get("recovered_on"), today)]
    bad = [d for d in docs if d.get("status") in IN_REPAIR and worse(d)]
    gone = [d for d in docs if d.get("status") == "gone" and _this_week(d.get("gone_on"), today)]
    blocked = sum(1 for d in rep if d.get("blocked"))
    if not (rep or stuck or rec or gone):
        return None
    parts = [f"{len(rep)} recovering" + (f" ({blocked} warm-up blocked)" if blocked else "")]
    if stuck:
        parts.append(f"{len(stuck)} not improving after 3 weeks")
    if rec:
        parts.append(f"{len(rec)} recovered this week")
    if bad:
        parts.append(f"{len(bad)} worse")
    if gone:
        parts.append(f"{len(gone)} gone")
    return f"Warm-up repair, {CLIENTS.get(client, client)}: {', '.join(parts)}."


def _detail_line(d: dict, today: date) -> str:
    days = (today - date.fromisoformat(str(d["started"])[:10])).days
    rep = d.get("latest_rep")
    flags = [{"repairing": "recovering", "no_improvement": "no improvement in 3 weeks (cancel candidate)",
              "recovered": f"recovered {_day(d['recovered_on'])}" if d.get("recovered_on") else "recovered",
              "gone": "gone"}.get(d.get("status"), str(d.get("status")))]
    if d.get("status") in IN_REPAIR and worse(d):
        flags.append("worse")
    if d.get("blocked"):
        flags.append("warm-up blocked")
    b, a = d.get("settings_before") or {}, d.get("settings_after")
    setting = (f"warm-up {b.get('warmup_max_count')}→{a.get('warmup_max_count')}/day, reply "
               f"{b.get('reply_rate')}→{a.get('reply_rate')}%" if a else (d.get("why_not_changed") or "settings unchanged"))
    return (f"• {d['email']} · {d.get('kind')} · {d.get('rep_at_start')}% → {rep if rep is not None else '?'}% "
            f"(good from {level(d.get('kind'))}%) · day {days} · {', '.join(flags)} · {setting}")


def digest_items(docs: list[dict], today: date, weekly: bool | None = None) -> tuple[list, list, list[str]]:
    """(asks, done, detail) for the digest. The weekly line per client is a Done line on Mondays (``weekly``
    overrides the day). The only ask: inboxes 3 weeks in repair without improving, as cancel candidates."""
    from smartlead import digest as dg
    weekly = today.weekday() == 0 if weekly is None else weekly
    by_client: dict[str, list[dict]] = {}
    for d in docs:
        by_client.setdefault(d.get("client"), []).append(d)
    done = []
    if weekly:
        for client in list(CLIENTS) + sorted(c for c in by_client if c not in CLIENTS):
            line = week_line(client, by_client.get(client) or [], today)
            if line:
                done.append(dg.Done(line, key=f"warmup-repair:week:{CLIENTS.get(client, client)}:{today.isoformat()}"))
    stuck = [d for d in docs if d.get("status") == "no_improvement"]
    asks = []
    if stuck:
        n = len(stuck)
        asks.append(dg.Ask(f"{_n(n)} did not improve in 3 weeks of warm-up repair: cancel candidates, your call.",
                           "Say which of those listed to cancel; the bot never cancels or deletes an inbox.",
                           key="warmup-repair:cancel-candidates", detail=[_detail_line(d, today) for d in stuck]))
    live = [d for d in docs if d.get("status") in IN_REPAIR
            or (d.get("status") == "recovered" and _this_week(d.get("recovered_on"), today))
            or (d.get("status") == "gone" and _this_week(d.get("gone_on"), today))]
    detail = []
    if live and (weekly or asks):
        counts = Counter(d.get("status") for d in live)
        head = (f"Warm-up repair, {_day(today)}: {counts['repairing'] + counts['no_improvement']} recovering, "
                f"{counts['recovered']} recovered this week (report-only: the bot changes no warm-up setting)")
        order = {"no_improvement": 0, "repairing": 1, "recovered": 2, "gone": 3}
        lines = [head]
        for client in sorted({d.get("client") for d in live}, key=lambda c: list(CLIENTS).index(c) if c in CLIENTS else 9):
            mine = sorted((d for d in live if d.get("client") == client),
                          key=lambda d: (order.get(d.get("status"), 9), d.get("latest_rep") or 0, d["email"]))
            lines.append(f"\n{CLIENTS.get(client, client)}:\n" + "\n".join(_detail_line(d, today) for d in mine))
        detail = ["\n".join(lines)]
    return asks, done, detail


# --- Mongo ----------------------------------------------------------------------------------------------------------

class RepairStore:
    """warmup_repair: one document per inbox, by email."""

    def __init__(self, col) -> None:
        self.col = col

    def all(self) -> list[dict]:
        return [d for d in self.col.find({}, {"_id": 0})]

    def save(self, docs: list[dict]) -> None:
        for d in docs:
            self.col.update_one({"email": d["email"]}, {"$set": d}, upsert=True)

    def seed(self, docs: list[dict]) -> int:
        """Write the seed docs; an inbox already seeded keeps its doc, one the bot enrolled by itself is replaced
        (the seed knows the real start and the settings change). Returns how many were written."""
        have = {d["email"]: d for d in self.all()}
        todo = [d for d in docs if d["email"] not in have or have[d["email"]].get("enrolled_by") == "auto"]
        self.save(todo)
        return len(todo)
