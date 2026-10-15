"""Fleet watch: problems in the daily inbox record, what to do about each, and
whether it was acted on and cleared. Pure: no network, nothing written.

Spec: docs/superpowers/specs/2026-10-15-fleet-watch-design.md. A record is one
inbox on one day (see fleet_watch.record). A case is one problem on one inbox or
domain, open from the day a check fires until the day it stops firing.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta
from statistics import median

from smartlead import inbox_standard as st
from smartlead import kill_rule as kr

BOUNCE_HIGH, BOUNCE_WATCH, BOUNCE_MIN_SENT = 4.0, 2.0, 100
LOW_REP_DAYS = 7
SMTP_WARMUP_DAYS = 7
NO_TEST_DAYS = 21
JUDGE_AFTER_DAYS = 14

# kind: (label, suggested action, scope)
KINDS = {
    "warmup_off": ("warmup off", "Turn warmup back on", "inbox"),
    "disconnected": ("disconnected", "Reconnect it in Smartlead", "inbox"),
    "bounce_high": ("bounces 4%+", "Lower the domain's limit and check the lead list", "domain"),
    "bounce_watch": ("bounces 2-4%", "Keep volume, check the lead list", "domain"),
    "failed_test": ("one failed placement test", "Keep volume, retest next Monday", "inbox"),
    "kill": ("kill list", "Take it out of campaigns, do not renew", "inbox"),
    "low_rep": ("warmup reputation low 7 days", "Keep volume, check SPF/DKIM/DMARC", "inbox"),
    "smtp_ip": ("SMTP reputation under 50%", "Ask ScaledMail to rotate the IP", "domain"),
    "blocklist": ("on a blocklist", "Pause the domain and request delisting", "domain"),
    "early_campaign": ("in a campaign before its ready date", "Take it out until it is ready", "inbox"),
    "no_test": ("no placement test in 21 days", "Add it to Monday's test", "domain"),
    "hand_limit": ("limit changed by hand", "Confirm the change was meant", "inbox"),
}


def _day(d: date | str) -> str:
    d = date.fromisoformat(d) if isinstance(d, str) else d
    return f"{d.day} {d.strftime('%b')}"


def _case(kind: str, target: str, r: dict, line: str | None = None) -> dict:
    label, action, scope = KINDS[kind]
    return {"key": f"{kind}:{target}", "kind": kind, "scope": scope, "target": target, "domain": r["domain"],
            "client": r.get("client"), "action": action, "line": line or target}


def _want(r: dict) -> int | None:
    if r.get("approved_limit") is not None:
        return int(r["approved_limit"])
    if r.get("setup"):
        return st.standard_for(r["kind"])["max_email_per_day"]
    return None


# Checks ----------------------------------------------------------------------

def checks(records: list[dict], history: dict[str, list[dict]], listed: dict[str, list[str]], today: date) -> list[dict]:
    """Every problem in today's records. history = {email: [records, oldest first, today last]};
    listed = {domain: [blocklists it is on]}."""
    out = []
    by_dom: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_dom[r["domain"]].append(r)
        e = r["email"]
        if not r.get("warmup_on"):
            out.append(_case("warmup_off", e, r))
        if r.get("connected") is False:
            out.append(_case("disconnected", e, r))
        if r.get("verdict") == kr.KILL:
            out.append(_case("kill", e, r))
        elif r.get("verdict") == kr.WATCH:
            out.append(_case("failed_test", e, r))
        level = st.good_reputation(r["kind"])
        last = [h.get("rep") for h in (history.get(e) or [])][-LOW_REP_DAYS:]      # history includes today
        if len(last) >= LOW_REP_DAYS and all(x is not None and x < level for x in last):
            out.append(_case("low_rep", e, r, f"{e} ({r.get('rep')}%)"))
        if r.get("ready_on") and today < date.fromisoformat(r["ready_on"]) and r.get("campaigns"):
            out.append(_case("early_campaign", e, r, f"{e} (ready {_day(r['ready_on'])})"))
        want = _want(r)
        if want is not None and r.get("limit") is not None and int(r["limit"]) != want:
            out.append(_case("hand_limit", e, r, f"{e} ({r['limit']}, set {want})"))
    for dom, rs in by_dom.items():
        r0 = rs[0]
        sent = sum(int(r.get("sent_14d") or 0) for r in rs)
        bounced = sum(int(r.get("bounced_14d") or 0) for r in rs)
        if sent >= BOUNCE_MIN_SENT:
            pct = 100.0 * bounced / sent
            if pct >= BOUNCE_WATCH:
                kind = "bounce_high" if pct >= BOUNCE_HIGH else "bounce_watch"
                out.append(_case(kind, dom, r0, f"{dom} bounced {round(pct, 1):g}% of {sent} sent"))
        smtp_low = [r for r in rs if r["kind"] == "smtp" and st.needs_ip_rotation("smtp", r.get("rep"))
                    and r.get("warmup_started")
                    and (today - date.fromisoformat(r["warmup_started"])).days >= SMTP_WARMUP_DAYS]
        if smtp_low:
            out.append(_case("smtp_ip", dom, r0, f"{dom} ({min(r['rep'] for r in smtp_low)}%)"))
        if listed.get(dom):
            out.append(_case("blocklist", dom, r0, f"{dom} ({', '.join(listed[dom])})"))
        if any(r.get("sending") for r in rs):
            newest = max((r.get("test_date") or "" for r in rs), default="")
            if not newest or (today - date.fromisoformat(newest)).days > NO_TEST_DAYS:
                out.append(_case("no_test", dom, r0, f"{dom} (last {_day(newest)})" if newest else f"{dom} (never)"))
    return out


# Cases -------------------------------------------------------------------------

def observe(case: dict, records: list[dict], fired_keys: set[str]) -> dict:
    """The numbers 'acted on' is read from, for the case's inbox or domain now."""
    mine = [r for r in records if (r["email"] if case["scope"] == "inbox" else r["domain"]) == case["target"]]
    reps = [r.get("rep") for r in mine if r.get("rep") is not None]
    out = {"limit_sum": sum(int(r.get("limit") or 0) for r in mine),
           "campaigns": sum(int(r.get("campaigns") or 0) for r in mine),
           "warmup_on": bool(mine) and all(r.get("warmup_on") for r in mine),
           "connected": bool(mine) and all(r.get("connected") is not False for r in mine),
           "test_date": max((r.get("test_date") or "" for r in mine), default="") or None,
           "rep": min(reps) if reps else None,
           "level": st.good_reputation(mine[0]["kind"]) if mine else None,
           "listed": case["key"] in fired_keys and case["kind"] == "blocklist"}
    if case["scope"] == "inbox" and mine:
        out["limit"], out["want"] = mine[0].get("limit"), _want(mine[0])
    return out


def acted(kind: str, at_open: dict, now: dict) -> bool:
    if kind == "warmup_off":
        return bool(now.get("warmup_on"))
    if kind == "disconnected":
        return bool(now.get("connected"))
    if kind in ("bounce_high", "bounce_watch"):
        return (now.get("limit_sum") or 0) < (at_open.get("limit_sum") or 0) or \
            (now.get("campaigns") or 0) < (at_open.get("campaigns") or 0)
    if kind in ("failed_test", "no_test"):
        return (now.get("test_date") or "") > (at_open.get("test_date") or "")
    if kind in ("kill", "early_campaign"):
        return (at_open.get("campaigns") or 0) > 0 and not now.get("campaigns")
    if kind in ("low_rep", "smtp_ip"):
        return now.get("rep") is not None and now.get("level") is not None and now["rep"] >= now["level"]
    if kind == "blocklist":
        return not now.get("listed")
    if kind == "hand_limit":
        return now.get("limit") is not None and now.get("limit") == now.get("want")
    return False


def update_cases(open_cases: dict[str, dict], fired: list[dict], records: list[dict],
                 today: date) -> tuple[list[dict], list[dict], list[dict]]:
    """(opened today, open after today, closed today). A case closes the day its
    check stops firing; 'acted_on' is the first day what changed shows it was handled."""
    keys = {c["key"] for c in fired}
    t = today.isoformat()
    opened, open_now, closed = [], [], []
    for key, c in open_cases.items():
        c = dict(c)
        if not c.get("acted_on") and acted(c["kind"], c.get("at_open") or {}, observe(c, records, keys)):
            c["acted_on"] = t
        if key in keys:
            open_now.append(c)
        else:
            c["closed_on"] = t
            closed.append(c)
    for c in fired:
        if c["key"] in open_cases:
            continue
        new = {**c, "opened_on": t, "acted_on": None, "closed_on": None, "at_open": observe(c, records, keys)}
        opened.append(new)
        open_now.append(new)
    return opened, open_now, closed


# Messages ----------------------------------------------------------------------

def watch_text(new: list[dict], open_count: dict[str, int], today: date, suggestions_only: bool = True) -> str:
    n = len(new)
    head = f"Fleet watch, {_day(today)}: {n} new problem{'s' if n != 1 else ''}"
    lines = [head + (" (suggestions only, nothing was changed)" if suggestions_only else "")]
    groups: dict[str, list[dict]] = defaultdict(list)
    for c in new:
        groups[c["kind"]].append(c)
    for kind in KINDS:
        cs = groups.get(kind)
        if not cs:
            continue
        label, action, _ = KINDS[kind]
        shown = ", ".join(c["line"] for c in cs[:10]) + (f", and {len(cs) - 10} more" if len(cs) > 10 else "")
        lines.append(f"{action} ({label}): {shown}")
    total = sum(open_count.values())
    if total:
        lines.append(f"Still open: {total} (" + ", ".join(
            f"{KINDS[k][0]} {v}" for k, v in open_count.items() if v and k in KINDS) + ")")
    return "\n".join(lines)


def _cap(s: str) -> str:
    return s[:1].upper() + s[1:]


def scorecard_text(cases: list[dict], today: date, *, pilot: set[str], control: set[str]) -> str:
    """Per kind of problem: opened, acted on, and of the cases 14+ days old, how
    many cleared within 14 days when acted on vs not."""
    lines = [f"Fleet watch scorecard, {_day(today)}"]
    by_kind: dict[str, list[dict]] = defaultdict(list)
    for c in cases:
        by_kind[c["kind"]].append(c)
    for kind in KINDS:
        cs = by_kind.get(kind)
        if not cs:
            continue
        acted_n = sum(bool(c.get("acted_on")) for c in cs)
        head = f"{_cap(KINDS[kind][0])}: {len(cs)} opened, {acted_n} acted on"
        old = [c for c in cs if (today - date.fromisoformat(c["opened_on"])).days >= JUDGE_AFTER_DAYS]
        if not old:
            lines.append(f"{head}; too recent to judge ({len(cs)})")
            continue

        def cleared(c):
            return bool(c.get("closed_on")) and (date.fromisoformat(c["closed_on"])
                                                 - date.fromisoformat(c["opened_on"])).days <= JUDGE_AFTER_DAYS
        a = [c for c in old if c.get("acted_on")]
        na = [c for c in old if not c.get("acted_on")]
        days = [(date.fromisoformat(c["closed_on"]) - date.fromisoformat(c["opened_on"])).days
                for c in cs if c.get("closed_on")]
        tail = f"; median {median(days):g} days to clear" if days else ""
        lines.append(f"{head}; cleared within {JUDGE_AFTER_DAYS} days: {sum(map(cleared, a))} of {len(a)} acted on, "
                     f"{sum(map(cleared, na))} of {len(na)} not{tail}")

    def group(doms: set[str]) -> str:
        mine = [c for c in cases if c.get("domain") in doms]
        n = len(mine)
        detail = Counter(KINDS[c["kind"]][0] for c in mine)
        return f"{n} case{'s' if n != 1 else ''}" + (" (" + ", ".join(f"{k} {v}" for k, v in detail.items()) + ")"
                                                      if n else "")
    if pilot or control:
        lines.append(f"Pilot domains: {group(pilot)}. Comparison domains: {group(control)}.")
    return "\n".join(lines)


def days_ago(today: date, n: int) -> str:
    return (today - timedelta(days=n)).isoformat()
