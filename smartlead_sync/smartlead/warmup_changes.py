"""Warm-up learning (t16, 17 Nov 2026). Manveen, 17 Nov: "mae sure we save thta on db as well so we know what works".

Mongo ``warmup_changes``: one event per inbox per warm-up settings change (who changed what, when, why, before and
after), with what happened next:

  * outcomes.week1/week2/week3: the reading nearest 7, 14 and 21 days after the change (within 3 days): warm-up
    reputation and 7-day warm-up sent/spam from inbox_daily, the newest v2 result on or before that day from
    v2_verdicts, and real sends/replies/out-of-office from inbox_evidence (the weekly evidence job, PR #84). Until that
    job has a week, ``real`` is None and ``real_why`` says why. A checkpoint with no reading within 3 days is kept as
    {"missing": ...}; a filled checkpoint is never overwritten;
  * recovered_on / days_to_recover: the first reading after the change at the All Inboxes cutoff (90) with warm-up
    not blocked.

Fleet watch (10:30 IST) fills the outcomes every day from what it already keeps: no Smartlead call. The first Monday
of the month the digest gets one "what worked" line (source warmup-repair) and the numbers per settings, inbox type
and vendor in the thread. Numbers only: no inbox address leaves Mongo.

Report-only. Nothing here changes a warm-up setting. Whoever changes settings appends the events (warmup_repair_cli.py
changes ...; a change script can call ``ChangeStore.append`` with ``events_from_changes``).
"""
from __future__ import annotations

from datetime import date, timedelta
from statistics import median

from smartlead import warmup_repair as wr

WEEKS = (1, 2, 3)
NEAR_DAYS = 3                 # a checkpoint reading may be up to 3 days off the 7/14/21-day mark
JUDGE_AFTER = "week3"         # an event counts in "what worked" once its 3-week outcome is in
NO_EVIDENCE = ("inbox_evidence has no week yet (the weekly real-send evidence job, PR #84, is not live): "
               "replies and out-of-office unknown")


def summary(s: dict | None) -> dict | None:
    """Smartlead's warm-up settings as the numbers that matter: per day, minimum, ramp (0 = off), reply rate, reply
    limit."""
    if not s:
        return None
    ramp = s.get("rampup_value") if s.get("is_rampup_enabled", True) else 0
    return {"per_day": s.get("warmup_max_count"), "min": s.get("warmup_min_count"), "ramp": ramp,
            "reply_rate": s.get("reply_rate"), "reply_limit": s.get("daily_reply_limit")}


def label(after: dict | None) -> str:
    if not after:
        return "settings unchanged"
    ramp = f"ramp {after.get('ramp')}" if after.get("ramp") else "ramp off"
    return f"{after.get('per_day')}/day, min {after.get('min')}, {ramp}, {after.get('reply_rate')}% reply"


def new_event(email: str, *, account: str, client: str, typ: str | None, on: str, rep, before: dict | None,
              after: dict | None, why: str, who: str, vendor: str | None = None) -> dict:
    email = email.strip().lower()
    on = str(on)[:10]
    return {"key": f"{email}|{on}", "email": email, "account": account, "client": wr._client(client), "type": typ,
            "kind": wr._kind(typ), "vendor": vendor, "date": on, "rep_before": wr._rep(rep),
            "before": summary(before), "after": summary(after), "raw_before": before, "raw_after": after,
            "why": why, "who": who, "outcomes": {}, "recovered_on": None, "days_to_recover": None}


def events_from_changes(changes: dict, before: dict | None, *, who: str, why: str,
                        vendors: dict[str, str] | None = None) -> tuple[list[dict], list[str]]:
    """(events, emails skipped because the change did not apply) from a change file {email: {on, before, after, ok,
    type, rep_before}} and its before-snapshot ({inboxes: [{email, account, client, type}]}). Pure."""
    info = {str(x.get("email") or x.get("from_email") or "").lower(): x for x in (before or {}).get("inboxes") or []}
    vendors = {k.lower(): v for k, v in (vendors or {}).items()}
    out, skipped = [], []
    for email, c in (changes or {}).items():
        e = str(email).lower()
        if not c.get("ok"):
            skipped.append(e)
            continue
        x = info.get(e, {})
        out.append(new_event(e, account=str(x.get("account") or ""), client=x.get("client") or "",
                             typ=c.get("type") or x.get("type"), on=c.get("on"),
                             rep=c.get("rep_before", x.get("warmup_reputation")), before=c.get("before"),
                             after=c.get("after"), why=why, who=who, vendor=vendors.get(e)))
    return out, skipped


_VENDORS = {"scaledmail": "ScaledMail", "zapmail": "Zapmail", "inboxkit": "InboxKit"}


def vendor_name(p: str | None) -> str | None:
    """One spelling per vendor ("Scaled Mail", "Scaledmail" -> "ScaledMail")."""
    if not p:
        return None
    return _VENDORS.get(str(p).replace(" ", "").lower(), str(p).strip())


def vendors_by_email(emails: list[str], daily: dict[str, str], assets: list[dict]) -> dict[str, str]:
    """{email: vendor}: inbox_daily's vendor when it names one, else the asset tracker's provider for the inbox's
    domain (an Active row first). Inboxes neither knows are left out (unknown, never guessed). Pure."""
    by_domain: dict[str, str] = {}
    for a in sorted(assets, key=lambda a: str(a.get("status")) != "Active"):   # Active rows win
        name, prov = str(a.get("name") or "").lower(), vendor_name(a.get("provider"))
        if name and prov and name not in by_domain:
            by_domain[name] = prov
    out = {}
    for e in emails:
        v = vendor_name(daily.get(e)) or by_domain.get(e.split("@")[-1])
        if v:
            out[e] = v
    return out


# --- outcomes -------------------------------------------------------------------------------------------------------

def _d(x) -> date:
    return date.fromisoformat(str(x)[:10])


def _day(d: date) -> str:
    return f"{d.day} {d.strftime('%b')}"


def _nearest(hist: list[dict], target: date, today: date) -> dict | None:
    best = None
    for h in hist:
        if h.get("rep") is None:
            continue
        d = _d(h["date"])
        if d > today or abs((d - target).days) > NEAR_DAYS:
            continue
        k = (abs((d - target).days), d)                      # nearest; a tie goes to the earlier reading
        if best is None or k < best[0]:
            best = (k, h)
    return best[1] if best else None


def _v2(verdicts: list[dict], target: date) -> dict | None:
    ok = [v for v in verdicts if v.get("night") and _d(v["night"]) <= target]
    if not ok:
        return None
    v = max(ok, key=lambda v: str(v["night"]))
    return {"verdict": v.get("verdict"), "night": str(v["night"])[:10]}


def _real(email: str, evidence: list[dict], target: date) -> tuple[dict | None, str | None]:
    if not evidence:
        return None, NO_EVIDENCE
    weeks = [w for w in evidence if target - timedelta(days=7) <= _d(w["_id"]) <= target]
    if not weeks:
        return None, f"no inbox_evidence week in the 7 days up to {_day(target)}"
    w = max(weeks, key=lambda w: str(w["_id"]))
    row = (w.get("inboxes") or {}).get(email)
    if not row:
        return {"week": str(w["_id"])[:10], "sent": 0, "replies": 0, "ooo": 0,
                "label": "no real sends in the evidence window"}, None
    provs = (row.get("providers") or {}).values()
    return {"week": str(w["_id"])[:10], "sent": row.get("sent", 0),
            "replies": sum(p.get("replies", 0) for p in provs), "ooo": sum(p.get("ooo", 0) for p in provs),
            "label": row.get("label")}, None


def checkpoint(email: str, reading: dict, verdicts: list[dict], evidence: list[dict], target: date) -> dict:
    sent, spam = reading.get("warm_sent_7d"), reading.get("warm_spam_7d")
    pct = round(100 * spam / sent, 1) if isinstance(sent, (int, float)) and sent and isinstance(spam, (int, float)) \
        else None
    real, why = _real(email, evidence, target)
    out = {"on": str(reading["date"])[:10], "rep": wr._rep(reading.get("rep")), "warm_sent_7d": sent,
           "warm_spam_7d": spam, "warm_spam_pct": pct, "v2": _v2(verdicts, target), "real": real}
    if why:
        out["real_why"] = why
    return out


def fill(event: dict, hist: list[dict], verdicts: list[dict], evidence: list[dict], today: date) -> dict:
    """The event with every checkpoint due by ``today`` filled (never overwriting one) and its recovery date. Pure."""
    e = {**event, "outcomes": dict(event.get("outcomes") or {})}
    start = _d(e["date"])
    for w in WEEKS:
        key, target = f"week{w}", start + timedelta(days=7 * w)
        if key in e["outcomes"] or today < target:
            continue
        r = _nearest(hist, target, today)
        if r:
            e["outcomes"][key] = checkpoint(e["email"], r, verdicts, evidence, target)
        elif today > target + timedelta(days=NEAR_DAYS):
            e["outcomes"][key] = {"missing": f"no reading within {NEAR_DAYS} days of {_day(target)}"}
    if not e.get("recovered_on"):
        for h in sorted(hist, key=lambda h: str(h["date"])):
            d, rep = _d(h["date"]), wr._rep(h.get("rep"))
            if start < d <= today and rep is not None and rep >= wr.level() and not h.get("warmup_blocked"):
                e["recovered_on"], e["days_to_recover"] = d.isoformat(), (d - start).days
                break
    return e


def due(e: dict) -> bool:
    return any(f"week{w}" not in (e.get("outcomes") or {}) for w in WEEKS) or not e.get("recovered_on")


# --- what worked ----------------------------------------------------------------------------------------------------

def _judged(e: dict) -> dict | None:
    w = (e.get("outcomes") or {}).get(JUDGE_AFTER)
    return w if w and "rep" in w else None


def worse(e: dict) -> bool:
    w, start = _judged(e), e.get("rep_before")
    return bool(w) and not e.get("recovered_on") and w.get("rep") is not None and start is not None \
        and w["rep"] <= start - wr.WORSE_BY


def what_worked(events: list[dict], today: date) -> list[dict]:
    """Per settings combination x inbox type x vendor, over events with a 3-week outcome: how many, recovered,
    median days to recover, worse (3+ points under the start, not recovered), still under the cutoff. Pure."""
    groups: dict[tuple, list[dict]] = {}
    for e in events:
        if _judged(e):
            groups.setdefault((label(e.get("after")), e.get("kind"), e.get("vendor") or "vendor unknown"), []).append(e)
    rows = []
    for (settings, kind, vendor), es in groups.items():
        rec = [e for e in es if e.get("recovered_on")]
        bad = [e for e in es if worse(e)]
        rows.append({"settings": settings, "kind": kind, "vendor": vendor, "n": len(es), "recovered": len(rec),
                     "median_days": float(median(e["days_to_recover"] for e in rec)) if rec else None,
                     "worse": len(bad), "still_low": len(es) - len(rec) - len(bad)})
    return sorted(rows, key=lambda r: (-r["n"], r["settings"], str(r["kind"]), r["vendor"]))


def first_monday(d: date) -> bool:
    return d.weekday() == 0 and d.day <= 7


def _n(k: int) -> str:
    return f"{k} inbox{'es' if k != 1 else ''}"


def month_items(events: list[dict], today: date, monthly: bool | None = None) -> tuple[list, list[str]]:
    """(done, detail) for the digest: one line on the first Monday of the month (``monthly`` overrides the day),
    silent when no event has its 3-week outcome yet."""
    from smartlead import digest as dg
    if not (first_monday(today) if monthly is None else monthly):
        return [], []
    rows = what_worked(events, today)
    if not rows:
        return [], []
    judged = [e for e in events if _judged(e)]
    rec = [e for e in judged if e.get("recovered_on")]
    med = f" (median {median(e['days_to_recover'] for e in rec):g} days)" if rec else ""
    bad = sum(1 for e in judged if worse(e))
    text = (f"Warm-up changes, what worked: {_n(len(judged))} 3+ weeks after a settings change, "
            f"{len(rec)} recovered{med}, {bad} worse.")
    lines = [f"Warm-up changes, what worked ({today.strftime('%b %Y')}; recovered = reputation {wr.level()}%+ with "
             f"warm-up not blocked; worse = 3+ points under the start at 3 weeks):"]
    for r in rows:
        md = f"median {r['median_days']:g} days" if r["median_days"] is not None else "none recovered"
        lines.append(f"• {r['settings']} · {r['kind']} · {r['vendor']}: {_n(r['n'])}, {r['recovered']} recovered, "
                     f"{md}, {r['worse']} worse, {r['still_low']} still under {wr.level()}")
    return [dg.Done(text, key=f"warmup-repair:what-worked:{today.strftime('%Y-%m')}")], ["\n".join(lines)]


# --- Mongo ----------------------------------------------------------------------------------------------------------

class ChangeStore:
    """warmup_changes: one document per inbox per settings change, by ``key`` (email|date)."""

    def __init__(self, col) -> None:
        self.col = col

    def all(self) -> list[dict]:
        return [d for d in self.col.find({}, {"_id": 0})]

    def append(self, events: list[dict]) -> int:
        """Write the events not already there (an existing event keeps its outcomes). Returns how many were new."""
        have = {d["key"] for d in self.col.find({}, {"key": 1})}
        new = [e for e in events if e["key"] not in have]
        for e in new:
            self.col.update_one({"key": e["key"]}, {"$set": e}, upsert=True)
        return len(new)

    def save_outcomes(self, events: list[dict]) -> None:
        for e in events:
            self.col.update_one({"key": e["key"]}, {"$set": {k: e[k] for k in
                                                            ("outcomes", "recovered_on", "days_to_recover")}})


def daily(changes, daily_col, verdicts_col, evidence_col, today: date) -> list[dict]:
    """Fill the outcomes due today and save them; every event after. Reads Mongo only."""
    store = ChangeStore(changes)
    events = store.all()
    open_ = [e for e in events if due(e)]
    if open_:
        emails = sorted({e["email"] for e in open_})
        since = min(e["date"] for e in open_)
        hist: dict[str, list[dict]] = {}
        for d in daily_col.find({"email": {"$in": emails}, "date": {"$gte": since}}, {"_id": 0}):
            if "rep" in d:
                hist.setdefault(d["email"], []).append(d)
        v2: dict[str, list[dict]] = {}
        if verdicts_col is not None:
            for v in verdicts_col.find({"email": {"$in": emails}}, {"_id": 0}):
                v2.setdefault(v["email"], []).append(v)
        ev = list(evidence_col.find({"_id": {"$gte": since}})) if evidence_col is not None else []
        filled = {e["key"]: fill(e, hist.get(e["email"], []), v2.get(e["email"], []), ev, today) for e in open_}
        changed = [f for k, f in filled.items() if f != next(e for e in open_ if e["key"] == k)]
        store.save_outcomes(changed)
        events = [filled.get(e["key"], e) for e in events]
    return events
