"""Fleet watch: problems in the daily inbox record, what to do about each, and
whether it was acted on and cleared. Pure: no network, nothing written.

Spec: docs/superpowers/specs/2026-10-15-fleet-watch-design.md. A record is one
inbox on one day (see fleet_watch.record). A case is one problem on one inbox or
domain, open from the day a check fires until there is evidence the problem is
gone. A day with missing data is not evidence (audit 7 Nov 2026: cases closed
when an account could not be read, and blocklisted domains showed as cleared).
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
BLOCKLIST_STALE_DAYS = 8         # an older blocklist check still counts; the line says when it was checked
SENDER_LISTS = ("spamhaus", "uribl")     # lists that judge the sending domain (SURBL lists links in the copy)
DO_NOT_USE = "do not use"        # tag prefix, any case ("DO NOT USE - failed v2 (auto)" and hand-made tags)

# kind: (label, suggested action, scope). The post shows them in this order, most urgent first.
# A case can carry a more exact action (over_limit: the number; dnu_live: the campaign).
KINDS = {
    "listed_live": ("on Spamhaus DBL or URIBL and in live campaigns",
                    "Decide with the client lead: keep sending or move volume; no blind removal requests", "domain"),
    "dnu_live": ("tagged DO NOT USE, still in a live campaign", "Take it out of the campaign or remove the tag",
                 "inbox"),
    "warmup_off": ("warmup off", "Turn warmup back on", "inbox"),
    "disconnected": ("disconnected", "Reconnect it in Smartlead", "inbox"),
    "bounce_high": ("bounces 4%+", "Lower the domain's limit and check the lead list", "domain"),
    "bounce_watch": ("bounces 2-4%", "Keep volume, check the lead list", "domain"),
    "failed_test": ("one failed placement test", "Keep volume, retest next Monday", "inbox"),
    "kill": ("kill list", "Take it out of campaigns, do not renew", "inbox"),
    "low_rep": ("warmup reputation low 7 days", "Keep volume, check SPF/DKIM/DMARC", "inbox"),
    "smtp_ip": ("SMTP reputation under 50%", "Ask ScaledMail to rotate the IP", "domain"),
    "blocklist": ("on a blocklist", "Find out why before it sends more; no blind removal requests", "domain"),
    "early_campaign": ("in a campaign before its ready date", "Take it out until it is ready", "inbox"),
    "no_test": ("no placement test in 21 days", "Add it to Monday's test", "domain"),
    "over_limit": ("above the standard daily limit", "Lower it to the standard or record it as approved", "inbox"),
    "hand_limit": ("limit changed by hand", "Confirm the change was meant", "inbox"),
}
BLOCKLIST_KINDS = ("blocklist", "listed_live")
# A case whose check stops firing because a stronger case on the same inbox fired is replaced, not cleared:
# a disconnected inbox cannot warm up, a second failed test is the kill list, a raise above the standard is over_limit.
SUPERSEDED_BY = {"warmup_off": "disconnected", "failed_test": "kill", "hand_limit": "over_limit"}


def _day(d: date | str) -> str:
    d = date.fromisoformat(str(d)[:10]) if not isinstance(d, date) else d
    return f"{d.day} {d.strftime('%b')}"


def _n(k: int) -> str:
    return f"{k} inbox{'es' if k != 1 else ''}"


def _case(kind: str, target: str, r: dict, line: str | None = None, action: str | None = None, **extra) -> dict:
    label, default, scope = KINDS[kind]
    return {"key": f"{kind}:{target}", "kind": kind, "scope": scope, "target": target, "domain": r["domain"],
            "client": r.get("client"), "account": r.get("account"), "action": action or default,
            "line": line or target, **extra}


def _want(r: dict) -> int | None:
    if r.get("approved_limit") is not None:
        return int(r["approved_limit"])
    if r.get("setup"):
        return st.standard_for(r["kind"])["max_email_per_day"]
    return None


def _standard_max(r: dict) -> int:
    return int(st.standard_for(r["kind"])["max_email_per_day"])


def _dnu(r: dict) -> bool:
    return any(str(t).strip().lower().startswith(DO_NOT_USE) for t in r.get("tags") or [])


# Blocklist rows (Mongo blacklist_checks) ------------------------------------------

def _bl_row(v) -> dict:
    """A blacklist_checks row {date, listed_on, lookup_errors, inconclusive_zones}; a bare list of
    listings (older callers) is a row with no date."""
    return v if isinstance(v, dict) else {"listed_on": list(v or []), "date": None}


def _zones(listed_on) -> list[str]:
    """'Spamhaus DBL (127.0.1.2)' -> 'Spamhaus DBL', the list name the lookup errors use."""
    return [str(s).split(" (")[0].strip() for s in listed_on or []]


def _listing(listed_on: list[str], checked, today: date) -> str:
    s = ", ".join(listed_on)
    if checked and (today - date.fromisoformat(str(checked)[:10])).days > BLOCKLIST_STALE_DAYS:
        s += f", last checked {_day(checked)}"
    return s


def _clean(row: dict | None, zones: list[str] | None) -> bool:
    """The row shows the domain off the lists the case was about, and those lists answered.
    Without the case's lists (older cases), every list must be clean and must have answered."""
    if not row:
        return False
    listed = set(_zones(row.get("listed_on")))
    unanswered = set(row.get("lookup_errors") or []) | set(row.get("inconclusive_zones") or [])
    if zones:
        return not (set(zones) & (listed | unanswered))
    return not listed and not unanswered


# Checks ----------------------------------------------------------------------

def checks(records: list[dict], history: dict[str, list[dict]], listed: dict, today: date) -> list[dict]:
    """Every problem in today's records. history = {email: [records, oldest first, today last]};
    listed = {domain: newest blacklist_checks row, however old} (a bare list of listings also works)."""
    out = []
    by_dom: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_dom[r["domain"]].append(r)
        e = r["email"]
        if r.get("connected") is False:
            out.append(_case("disconnected", e, r))
        elif not r.get("warmup_on"):          # a disconnected inbox cannot warm up: reconnecting comes first
            out.append(_case("warmup_off", e, r))
        live = list(r.get("live_campaigns") or [])
        if live and _dnu(r):
            names = ", ".join(live)
            out.append(_case("dnu_live", e, r, f"{e} ({names})", action=f"Take it out of {names} or remove the tag"))
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
        lim, std, want = r.get("limit"), _standard_max(r), _want(r)
        if lim is not None and r.get("approved_limit") is None and int(lim) > std:
            # Never raise (3 Nov): above the standard with no approved limit is flagged, never changed by the bot.
            out.append(_case("over_limit", e, r, f"{e} ({lim}/day, standard {std})",
                             action=f"Lower to {std} or record it as approved"))
        elif want is not None and lim is not None and int(lim) != want:
            out.append(_case("hand_limit", e, r, f"{e} ({lim}, set {want})"))
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
        row = _bl_row(listed[dom]) if listed.get(dom) else None
        if row and row.get("listed_on"):
            on = list(row["listed_on"])
            out.append(_case("blocklist", dom, r0, f"{dom} ({_listing(on, row.get('date'), today)})", listed_on=on))
            sender = [s for s in on if any(k in s.lower() for k in SENDER_LISTS)]
            live = [r for r in rs if r.get("live_campaigns")]
            if sender and live:
                camps = {c for r in live for c in r["live_campaigns"]}
                out.append(_case("listed_live", dom, r0,
                                 f"{dom} ({_listing(sender, row.get('date'), today)}; {_n(len(live))} in "
                                 f"{len(camps)} live campaign{'s' if len(camps) != 1 else ''})", listed_on=sender))
        if any(r.get("sending") for r in rs):
            newest = max((r.get("test_date") or "" for r in rs), default="")
            if not newest or (today - date.fromisoformat(newest)).days > NO_TEST_DAYS:
                out.append(_case("no_test", dom, r0, f"{dom} (last {_day(newest)})" if newest else f"{dom} (never)"))
    return out


# Cases -------------------------------------------------------------------------

def _mine(case: dict, records: list[dict]) -> list[dict]:
    return [r for r in records if (r["email"] if case["scope"] == "inbox" else r["domain"]) == case["target"]]


def observe(case: dict, records: list[dict], fired_keys: set[str]) -> dict:
    """The numbers 'acted on' is read from, for the case's inbox or domain now."""
    mine = _mine(case, records)
    reps = [r.get("rep") for r in mine if r.get("rep") is not None]
    out = {"limit_sum": sum(int(r.get("limit") or 0) for r in mine),
           "campaigns": sum(int(r.get("campaigns") or 0) for r in mine),
           "live": sum(len(r.get("live_campaigns") or []) for r in mine),
           "dnu": any(_dnu(r) for r in mine),
           "warmup_on": bool(mine) and all(r.get("warmup_on") for r in mine),
           "connected": bool(mine) and all(r.get("connected") is not False for r in mine),
           "test_date": max((r.get("test_date") or "" for r in mine), default="") or None,
           "rep": min(reps) if reps else None,
           "level": st.good_reputation(mine[0]["kind"]) if mine else None,
           "listed": case["key"] in fired_keys and case["kind"] in BLOCKLIST_KINDS}
    if case["scope"] == "inbox" and mine:
        out["limit"], out["want"] = mine[0].get("limit"), _want(mine[0])
        out["approved"], out["std"] = mine[0].get("approved_limit"), _standard_max(mine[0])
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
    if kind == "listed_live":
        return (now.get("live") or 0) < (at_open.get("live") or 0) or \
            (now.get("limit_sum") or 0) < (at_open.get("limit_sum") or 0)
    if kind == "dnu_live":
        return not now.get("live") or not now.get("dnu")
    if kind == "over_limit":
        return now.get("limit") is not None and (now.get("approved") is not None
                                                 or (now.get("std") is not None and now["limit"] <= now["std"]))
    if kind == "hand_limit":
        return now.get("limit") is not None and now.get("limit") == now.get("want")
    return False


def _shows_fixed(kind: str, mine: list[dict]) -> bool:
    """Today's records hold what the check reads, and it reads fine (the check did not fire on them)."""
    if kind == "warmup_off":
        return all(r.get("warmup_on") for r in mine)
    if kind == "disconnected":
        return all(r.get("connected") is True for r in mine)
    if kind in ("kill", "failed_test"):
        return all(r.get("verdict") == kr.GOOD for r in mine)        # a newer test passed; NO DATA proves nothing
    if kind == "low_rep":
        return all(r.get("rep") is not None and r["rep"] >= st.good_reputation(r["kind"]) for r in mine)
    if kind == "smtp_ip":
        return all(r.get("rep") is not None for r in mine if r["kind"] == "smtp")
    if kind in ("hand_limit", "over_limit"):
        return all(r.get("limit") is not None for r in mine)
    return True


def left(case: dict, roster: dict | None) -> bool:
    """Evidence that the case's inbox or domain really left the inboxes we watch, rather than went missing
    from one day's read (audit 7 Nov: the inbox list can stop early without an error, 77 PL inboxes went
    missing that way on 31 Oct). ``roster`` (fleet_watch.gather): ``tracked`` = the deliverability sheet's
    domains (None = sheet not read), ``present`` = every inbox Smartlead lists today (the facts read, which
    fails loudly on a bad page, plus the inbox list), ``dropped`` = inboxes listed today but left out on
    purpose (past client, excluded tag, domain not in the sheet). Evidence: the domain left the sheet,
    the inbox (every inbox of the domain) is gone from Smartlead, or it is left out on purpose."""
    if not roster:
        return False
    dom = case["domain"]
    tracked = roster.get("tracked")
    if tracked is not None and dom not in tracked:
        return True
    present, dropped = set(roster.get("present") or ()), set(roster.get("dropped") or ())
    if case["scope"] == "inbox":
        return case["target"] not in present or case["target"] in dropped
    here = {e for e in present if e.split("@")[-1] == dom}
    return not here or here <= dropped


def evidence(case: dict, mine: list[dict], account_read: bool, fired_keys: set[str], blocklist: dict | None,
             roster: dict | None = None) -> str:
    """For an open case whose check did not fire today: 'cleared' (today's data shows the problem gone),
    'superseded' (a stronger case on the same inbox fired), 'gone' (its account was read in full, the inbox
    or domain is not in today's records, and ``left`` has evidence it really left), or 'unknown' (no
    evidence: the case stays open). ``blocklist`` = {domain: newest row}, None when it could not be read."""
    kind = case["kind"]
    stronger = SUPERSEDED_BY.get(kind)
    if stronger and f"{stronger}:{case['target']}" in fired_keys:
        return "superseded"
    if kind in BLOCKLIST_KINDS:
        row = _bl_row(blocklist[case["domain"]]) if blocklist and blocklist.get(case["domain"]) else None
        if row and str(row.get("date") or "") >= str(case.get("opened_on") or "~") \
                and _clean(row, _zones(case.get("listed_on"))):
            return "cleared"                  # a check on or after the day the case opened shows it clean
        if kind == "listed_live" and mine and account_read and not any(r.get("live_campaigns") for r in mine):
            return "cleared"                  # no longer in a live campaign
        return "gone" if account_read and not mine and left(case, roster) else "unknown"
    if not account_read:
        return "unknown"
    if not mine:
        return "gone" if left(case, roster) else "unknown"
    return "cleared" if _shows_fixed(kind, mine) else "unknown"


def update_cases(open_cases: dict[str, dict], fired: list[dict], records: list[dict], today: date,
                 baseline: bool = False, *, read: set[str] | None = None,
                 blocklist: dict | None = None, roster: dict | None = None
                 ) -> tuple[list[dict], list[dict], list[dict]]:
    """(opened today, open after today, closed today).

    A case closes only on evidence (see ``evidence``); without it, it stays open with ``unknown`` set for
    the day. ``read``: the Smartlead accounts read in full today (default: the accounts in ``records``).
    ``blocklist``: {domain: newest blacklist_checks row}, or None when it could not be read.
    ``roster``: what Smartlead and the sheet hold today (see ``left``); None = no evidence that anything left,
    so a case missing from today's records stays open.
    'acted_on' is the first day what changed shows it was handled, read only from real data.
    A new case starts with ``announced`` False; the caller sets it once Slack accepts the post.
    ``baseline``: record what is already wrong without reporting it (Manveen, 15 Oct: act on what happens
    from now on); baseline cases are never posted daily or scored (the Monday still-open list names them)."""
    keys = {c["key"] for c in fired}
    by_key = {c["key"]: c for c in fired}
    t = today.isoformat()
    read = {r.get("account") for r in records} if read is None else set(read)
    account_of: dict = {}
    for r in records:
        account_of.setdefault(r.get("client"), r.get("account"))
    opened, open_now, closed = [], [], []
    for key, c in open_cases.items():
        c = dict(c)
        mine = _mine(c, records)
        c["account"] = c.get("account") or (mine[0].get("account") if mine else account_of.get(c.get("client")))
        if key in keys:
            f = by_key[key]
            c.update({k: f[k] for k in ("line", "action", "listed_on") if k in f})   # what it is today
            why = None
        else:
            why = evidence(c, mine, c["account"] in read if c["account"] else False, keys, blocklist, roster)
        if why in (None, "cleared") and not c.get("acted_on") and \
                acted(c["kind"], c.get("at_open") or {}, observe(c, records, keys)):
            c["acted_on"] = t
        if why in (None, "unknown"):
            c["unknown"] = why == "unknown"
            open_now.append(c)
        else:
            c["closed_on"], c["closed_why"] = t, why
            closed.append(c)
    for c in fired:
        if c["key"] in open_cases:
            continue
        new = {**c, "opened_on": t, "acted_on": None, "closed_on": None, "at_open": observe(c, records, keys),
               "baseline": baseline, "announced": None if baseline else False, "unknown": False}
        if not baseline:
            opened.append(new)
        open_now.append(new)
    return opened, open_now, closed


# Messages ----------------------------------------------------------------------

def _cap(s: str) -> str:
    return s[:1].upper() + s[1:]


def as_text(head: list[str], thread: list[str]) -> str:
    return "\n".join(head) + ("\n\n" + "\n\n".join(thread) if thread else "")


def _groups(cases: list[dict]) -> list[tuple[tuple[str, str], list[dict]]]:
    """[((kind, action), cases)] in KINDS order. A domain that has a listed_live case is shown there only."""
    live = {c["target"] for c in cases if c["kind"] == "listed_live"}
    groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for c in cases:
        if c["kind"] == "blocklist" and c["target"] in live:
            continue
        groups[(c["kind"], c.get("action") or KINDS[c["kind"]][1])].append(c)
    order = list(KINDS)
    return sorted(groups.items(), key=lambda kv: (order.index(kv[0][0]), kv[0][1]))


def _group_part(kind: str, action: str, cs: list[dict], note) -> str:
    return f"*{action}* ({KINDS[kind][0]}, {len(cs)}):\n" + "\n".join(f"• {c['line']}{note(c)}" for c in cs)


def watch_post(new: list[dict], open_count: dict[str, int], today: date, *, unknown: int = 0
               ) -> tuple[list[str], list[str]]:
    """(headline lines, thread parts) for the daily post: a count per action in the channel, the inboxes
    and domains in the thread. ``new``: cases to announce, today's and any from earlier days whose post
    did not go out; ``open_count``: the other open cases by kind; ``unknown``: how many of those could
    not be checked today."""
    t = today.isoformat()
    groups = _groups(new)
    n = sum(len(cs) for _, cs in groups)
    head = [f"⚠️ Fleet watch, {_day(today)}: {n} new problem{'s' if n != 1 else ''} "
            f"(suggestions only, nothing was changed)"]
    per_kind = Counter()
    for (kind, _), cs in groups:
        per_kind[kind] += len(cs)
    head += [f"{_cap(KINDS[kind][0])}: {k}" for kind, k in per_kind.items()]      # what is wrong; what to do is in the thread
    thread = [_group_part(kind, action, cs, lambda c: f", found {_day(c['opened_on'])}" if c.get("opened_on", t) < t else "")
              for (kind, action), cs in groups]
    earlier = sorted({c["opened_on"] for _, cs in groups for c in cs if c.get("opened_on", t) < t})
    if earlier:
        k = sum(1 for _, cs in groups for c in cs if c.get("opened_on", t) < t)
        head.append(f"{k} of them {'was' if k == 1 else 'were'} found on {', '.join(_day(d) for d in earlier)}, "
                    f"but that post did not go out.")
    total = sum(open_count.values())
    if total:
        head.append(f"Still open: {total} (" + ", ".join(
            f"{KINDS[k][0]} {v}" for k, v in open_count.items() if v and k in KINDS) + ")"
            + (f"; {unknown} of them could not be checked today" if unknown else ""))
    return head, thread


def watch_text(new: list[dict], open_count: dict[str, int], today: date, *, unknown: int = 0) -> str:
    return as_text(*watch_post(new, open_count, today, unknown=unknown))


def backlog_post(cases: list[dict], today: date) -> tuple[list[str], list[str]]:
    """Open cases from before cases were marked as announced (8 Nov 2026): one count line, the list in the
    thread, posted once."""
    groups = _groups(cases)
    n = sum(len(cs) for _, cs in groups)
    first = min((c["opened_on"] for _, cs in groups for c in cs), default=today.isoformat())
    head = [f"📋 Fleet watch: {n} problem{'s' if n != 1 else ''} found since {_day(first)} "
            f"{'are' if n != 1 else 'is'} still open (list in the thread, shown once; suggestions only, "
            f"nothing was changed)"]
    thread = [_group_part(kind, action, cs,
                          lambda c: f", open since {_day(c['opened_on'])}"
                                    + (", could not be checked today" if c.get("unknown") else ""))
              for (kind, action), cs in groups]
    return head, thread


def _cleared(c: dict) -> bool:
    """Closed because the problem went away (older cases have no closed_why: they closed that way)."""
    return bool(c.get("closed_on")) and c.get("closed_why", "cleared") == "cleared"


def scorecard_post(cases: list[dict], today: date, *, pilot: set[str], control: set[str]
                   ) -> tuple[list[str], list[str]]:
    """Per kind of problem: opened, acted on, and of the cases 14+ days old, how many cleared within
    14 days when acted on vs not. Then the domains still on a blocklist (baseline ones too: a weekly
    reminder; the daily post never repeats them) as a count, with the list in the thread."""
    lines = [f"Fleet watch scorecard, {_day(today)}"]
    everything = cases
    cases = [c for c in cases if not c.get("baseline")]
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
            return _cleared(c) and (date.fromisoformat(c["closed_on"])
                                    - date.fromisoformat(c["opened_on"])).days <= JUDGE_AFTER_DAYS
        a = [c for c in old if c.get("acted_on")]
        na = [c for c in old if not c.get("acted_on")]
        days = [(date.fromisoformat(c["closed_on"]) - date.fromisoformat(c["opened_on"])).days
                for c in cs if _cleared(c)]
        tail = f"; median {median(days):g} day{'s' if median(days) != 1 else ''} to clear" if days else ""
        lines.append(f"{head}; cleared within {JUDGE_AFTER_DAYS} days: {sum(map(cleared, a))} of {len(a)} acted on, "
                     f"{sum(map(cleared, na))} of {len(na)} not{tail}")

    def group(doms: set[str]) -> str:
        mine = [c for c in cases if c.get("domain") in doms]
        n = len(mine)
        detail = Counter(KINDS[c["kind"]][0] for c in mine if c["kind"] in KINDS)
        return f"{n} case{'s' if n != 1 else ''}" + (" (" + ", ".join(f"{k} {v}" for k, v in detail.items()) + ")"
                                                      if n else "")
    if pilot or control:
        lines.append(f"Pilot domains: {group(pilot)}. Comparison domains: {group(control)}.")
    unknown = sum(1 for c in everything if not c.get("closed_on") and c.get("unknown"))
    if unknown:
        lines.append(f"Could not be checked at the last daily run: {unknown} open case{'s' if unknown != 1 else ''} "
                     f"(account or blocklist data missing; kept open)")

    still = sorted((c for c in everything if c["kind"] == "blocklist" and not c.get("closed_on")),
                   key=lambda c: (c.get("opened_on") or "", c["target"]))
    live = {c["target"] for c in everything if c["kind"] == "listed_live" and not c.get("closed_on")}
    thread = []
    if still:
        k = len(still)
        lines.insert(1, f"Still on a blocklist: {k} domain{'s' if k != 1 else ''} (list in the thread)")
        thread.append("Domains still on a blocklist, including ones already listed when fleet watch started:\n"
                      + "\n".join(f"• {c['line']}, {c.get('client') or 'client unknown'}, open since "
                                  f"{_day(c['opened_on'])}"
                                  + (" (already listed when fleet watch started)" if c.get("baseline") else "")
                                  + (", in live campaigns" if c["target"] in live else "")
                                  + (", could not be checked at the last run" if c.get("unknown") else "")
                                  for c in still))
    return lines, thread


def scorecard_text(cases: list[dict], today: date, *, pilot: set[str], control: set[str]) -> str:
    return as_text(*scorecard_post(cases, today, pilot=pilot, control=control))


def days_ago(today: date, n: int) -> str:
    return (today - timedelta(days=n)).isoformat()
