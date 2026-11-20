"""One status per inbox, kept for all time, and the one daily post that says what needs a person.

Until 11 Nov 2026 an inbox's story was spread over seven posts and four stores: the availability sheet,
the v2 verdicts, the fleet-watch cases, the renewal flags. Nobody could ask "what is this inbox doing, and
since when?", and no post said "these things need you today" in one place.

Each daily sync gives every inbox exactly one state (first match wins):

  recovering         a person tagged it DO NOT USE to take it out of campaigns while its warm-up repairs
                     (reputation under 90; usable.recovering, Manveen 17 Nov 2026: "instead of pulled make it recovering")
  do_not_use         a person tagged it DO NOT USE in Smartlead
  pulled             the v2 job took it out of campaigns after a failed test; it returns after two passes
  failed             its newest Smart Delivery v2 verdict is FAIL and it is not pulled
  disconnected       Smartlead cannot connect to it
  set_aside          its newest v2 verdict is FAIL, but that fail is not counted (usable.fail_set_aside: a Google
                     inbox, or a person put it back after the result); it may send, nothing asks to take it out
  sending            in a campaign that is sending, with a v2 pass in the last 10 days
  sending_unproven   in a campaign that is sending, without that pass
  free               FREE on the list: may be given to a campaign
  warming            warm-up still ramping
  idle               none of the above (no capacity, an old test, low warm-up reputation ...)
  gone               no longer in Smartlead (set by the store, only when every account was read)

Mongo (db HEALTH_HISTORY_DB):
  inbox_status           one doc per inbox: its state today and the day that state began (``since``)
  inbox_status_history   one doc per change of state, never deleted: the all-time record

``states`` and ``needs_you`` are pure. ``save`` never raises: a Mongo problem costs the history for a day,
never the sync.
"""
from __future__ import annotations

import os
from collections import Counter, defaultdict
from datetime import date, timedelta

from smartlead import usable

AUTO_TAG = "do not use - failed v2 (auto)"      # v2_weekly.TAG, lower case
SENDING_STATUSES = {"ACTIVE", "IN_PROGRESS"}
# v2_weekly.REPORT_ONLY_CLIENTS: the job only reports for these. A safety stage until the job is proven, the same
# rules as every client (Manveen, 11 Nov 2026); never framed as the client's or anyone's call.
# 16 Nov 2026 (Manveen, "Yes, Outlook only"): no client is report-only; Melior acts like the others (Google inboxes
# stay report-only through usable.V2_NOT_TRUSTED for every client). Lower-case name: display name.
REPORT_ONLY: dict[str, str] = {}
STATUS_COLLECTION = "inbox_status"
HISTORY_COLLECTION = "inbox_status_history"
META_COLLECTION = "inbox_status_meta"           # {_id: "post", date}: the post goes out once a day
HELD = ("recovering", "do_not_use", "pulled", "failed")       # must not send
ORDER = ("sending", "sending_unproven", "set_aside", "free", "warming", "idle", "failed", "pulled", "recovering",
         "do_not_use",
         "disconnected", "gone")
LABEL = {"sending": "sending, passed the weekly test", "sending_unproven": "sending, no test result in 10 days",
         "set_aside": "failed the weekly test but it is not counted (test not trusted for its type, or put back by hand)",
         "free": "free to give to a campaign", "warming": "warming up", "idle": "not in use",
         "failed": "failed the weekly test", "pulled": "taken out by the weekly job",
         "recovering": "recovering (out of campaigns while warm-up repairs)", "do_not_use": "tagged DO NOT USE", "disconnected": "disconnected", "gone": "gone from Smartlead"}


def _fresh_pass(v2_status: str, v2_date: str, today: date) -> bool:
    if str(v2_status or "").upper() != "PASS":
        return False
    try:
        return (today - date.fromisoformat(str(v2_date)[:10])).days <= usable.V2_FRESH_DAYS
    except ValueError:
        return False


def state_of(tags, v2_status: str, v2_date: str, connected: bool, sending: bool, free: bool,
             warmup_state: str, today: date, set_aside: str = "", rep=None) -> str:
    names = [str(t).strip().lower() for t in tags or []]
    if any(t.startswith(usable.DO_NOT_USE) and t != AUTO_TAG for t in names):
        return usable.RECOVERING if usable.recovering(tags, rep) else "do_not_use"
    if AUTO_TAG in names:
        return "pulled"
    if str(v2_status or "").upper() == "FAIL" and not set_aside:
        return "failed"
    if not connected:
        return "disconnected"
    if str(v2_status or "").upper() == "FAIL":
        return "set_aside"
    if sending:
        return "sending" if _fresh_pass(v2_status, v2_date, today) else "sending_unproven"
    if free:
        return "free"
    if str(warmup_state or "").lower() == "warming":
        return "warming"
    return "idle"


def states(rows: list[dict], today: date) -> list[dict]:
    """The sync's campaign-rows (one per inbox per campaign) -> one record per inbox. Pure."""
    from smartlead.sheets import _is_test_campaign
    by_email: dict[str, list[dict]] = defaultdict(list)
    for r in rows or []:
        email = str(r.get("email") or "").strip().lower()
        if email:
            by_email[email].append(r)
    out = []
    for email, rs in sorted(by_email.items()):
        first = rs[0]
        sending = sorted({str(r.get("campaign_name") or "") for r in rs
                          if str(r.get("campaign_status") or "").upper() in SENDING_STATUSES
                          and not _is_test_campaign(r.get("campaign_name", ""))})
        tests = sorted({str(r.get("campaign_name") or "") for r in rs if _is_test_campaign(r.get("campaign_name", ""))})
        tags = sorted({t for r in rs for t in r.get("tags") or []})
        aside = first.get("v2_set_aside")
        if aside is None:   # a row from before the list carried it: the type rule still applies
            aside = usable.fail_set_aside({"verdict": first.get("v2_status")}, first.get("provider"))
        state = state_of(tags, first.get("v2_status", ""), first.get("v2_date", ""), bool(first.get("connection_ok")),
                         bool(sending), str(first.get("availability") or "").upper() == "FREE",
                         first.get("warmup_state", ""), today, set_aside=aside or "",
                         rep=first.get("warmup_rep_pct"))
        out.append({"email": email, "domain": email.split("@")[-1],
                    "client": str(first.get("sub_client") or first.get("client") or ""),
                    "account": str(first.get("client") or ""), "provider": str(first.get("provider") or ""),
                    "account_id": str(first.get("account_id") or ""), "state": state, "campaigns": sending,
                    "in_test": bool(tests), "tests": tests,
                    "group": usable.team_group(first.get("busy_reason"), first.get("provider"), first.get("use_notes")),
                    "v2_status": str(first.get("v2_status") or ""), "v2_date": str(first.get("v2_date") or ""),
                    "set_aside": str(aside or ""),
                    "availability": str(first.get("availability") or ""),
                    "busy_reason": str(first.get("busy_reason") or ""), "tags": tags, "date": today.isoformat()})
    return out


# ── what needs a person ──────────────────────────────────────────────────────

def _n(k: int, word: str = "inbox") -> str:
    return f"{k} {word}{'' if k == 1 else 'es' if word == 'inbox' else 's'}"


def _names(emails: list[str], limit: int = 6) -> str:
    more = len(emails) - limit
    return ", ".join(emails[:limit]) + (f" and {more} more" if more > 0 else "")


def needs_you(records: list[dict], today: date, require_pass: bool | None = None) -> list[dict]:
    """[{"key", "text", "detail": [lines]}], most urgent first; [] = nothing needs a person. Pure.

    1. an inbox that must not send (tagged, pulled, failed v2) is still in a sending campaign;
    2. a sending campaign has no usable inbox left, or half or more of its inboxes cannot send;
    3. an inbox in a sending campaign cannot connect (one that sits idle is listed in the thread, not here:
       an item that never clears teaches people to skip the post; the renewal review asks about it at its bill);
    4. once a v2 pass is required, inboxes sending without one, split by why (20 Nov 2026, Manveen on the digest:
       "this is still not fixed"; 25 inboxes already in an active weekly test were told to go on the sheet):
       a. on no test campaign: add the domain to the deliverability sheet (an ask);
       b. on a test campaign, but its active weekly test leaves them out (dt_senders' coverage record, Mongo
          test_coverage; a test keeps the inboxes it was created with): add them to that test (an ask);
       c. in the weekly test, whose last runs came back short or incomplete (Mongo v2_verdicts), or not known to be
          missing from it: nothing for a person to do, one line for info (``info``: no step, not counted).
       ``weekly_test`` ("missing" or "in", None = not known) and ``short_runs`` come from ``add_test_facts``."""
    if require_pass is None:
        require_pass = usable.require_v2_pass(today)
    items: list[dict] = []
    by_campaign: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in records:
        for c in r["campaigns"]:
            by_campaign[(r["client"], c)].append(r)

    held_live = [(k, [r for r in rs if r["state"] in HELD]) for k, rs in sorted(by_campaign.items())]
    held_live = [(k, rs) for k, rs in held_live if rs]
    if held_live:
        acting = [(k, rs) for k, rs in held_live if k[0].lower() not in REPORT_ONLY]
        report = [(k, rs) for k, rs in held_live if k[0].lower() in REPORT_ONLY]
        for group, who in ((acting, ""), (report, "report-only")):
            if not group:
                continue
            emails = sorted({r["email"] for _, rs in group for r in rs})
            one = len(emails) == 1
            where = _n(len(group), "live campaign")
            if who:
                names = " and ".join(sorted({REPORT_ONLY[k[0].lower()] for k, _ in group}))
                text = (f"{_n(len(emails))} of {names} that failed the weekly test or {'is' if one else 'are'} tagged "
                        f"DO NOT USE still {'sends' if one else 'send'} in {where}. The weekly job does not act on "
                        f"{names} yet while it is being proven, so {'it stays' if one else 'they stay'} until "
                        f"someone takes {'it' if one else 'them'} out")
            else:
                text = (f"{_n(len(emails))} that failed the weekly test or {'is' if one else 'are'} tagged DO NOT USE "
                        f"{'is' if one else 'are'} still in {where}. Take {'it' if one else 'them'} out, or the weekly "
                        f"job takes out the failed ones when this week's results are in")
            detail = [f"• {k[0]} · {k[1]}: " + "; ".join(
                f"{_names(sorted(r['email'] for r in rs if r['state'] == s))} ({LABEL[s]})"
                for s in HELD if any(r["state"] == s for r in rs)) for k, rs in group]
            items.append({"key": "held_but_sending" + ("_report_only" if who else ""), "text": text, "detail": detail})

    short, short_clients = [], set()
    for (client, camp), rs in sorted(by_campaign.items()):
        out_of = Counter(r["state"] for r in rs if r["state"] in HELD or r["state"] == "disconnected")
        usable_n = len(rs) - sum(out_of.values())
        if usable_n == 0 or (len(rs) - usable_n) * 2 >= len(rs):
            why = ", ".join(f"{out_of[s]} {LABEL[s]}" for s in ("disconnected",) + HELD if out_of.get(s))
            short.append(f"• {client} · {camp}: {usable_n} of {len(rs)} inboxes can send ({why})")
            short_clients.add(client)
    if short:
        n_short, one = len(short), len(short) == 1
        # Where the step's "inboxes that passed the weekly test" are: free ones with a fresh pass, per client.
        for client in sorted(short_clients):
            ok = sum(1 for r in records if r["client"] == client and r["state"] == "free"
                     and _fresh_pass(r["v2_status"], r["v2_date"], today))
            short.append(f"• {client} has {ok} free inbox{'' if ok == 1 else 'es'} with a weekly-test pass in the last "
                         f"{usable.V2_FRESH_DAYS} days (All Inboxes tab, availability FREE)" if ok else
                         f"• {client} has no free inbox with a weekly-test pass in the last {usable.V2_FRESH_DAYS} "
                         f"days, so lower the daily sending")
        items.append({"key": "campaign_short", "detail": short,
                      "text": f"{_n(n_short, 'live campaign')} {'has' if one else 'have'} lost half or more of "
                              f"{'its' if one else 'their'} inboxes. Add inboxes that passed the weekly test, or lower "
                              f"{'its' if one else 'their'} daily sending"})

    off = [r for r in records if r["state"] == "disconnected" and r["campaigns"]]
    if off:
        # One line per live campaign, every inbox named: the step is per inbox, so none may hide behind "and N more".
        by_camp: dict[tuple[str, str], list[str]] = defaultdict(list)
        for r in off:
            for c in r["campaigns"]:
                by_camp[(r["client"], c)].append(r["email"])
        items.append({"key": "disconnected",
                      "text": f"{_n(len(off))} in live campaigns {'is' if len(off) == 1 else 'are'} disconnected in "
                              f"Smartlead. Reconnect {'it' if len(off) == 1 else 'them'}",
                      "detail": [f"• {c} · {camp}: {_names(sorted(es), 12)}" for (c, camp), es in sorted(by_camp.items())]})

    unproven = [r for r in records if r["state"] == "sending_unproven" and not r.get("in_test")]
    left_out = [r for r in records if r["state"] == "sending_unproven" and r.get("in_test")
                and r.get("weekly_test") == "missing"]
    waiting = [r for r in records if r["state"] == "sending_unproven" and r.get("in_test")
               and r.get("weekly_test") != "missing"]
    if unproven and require_pass:
        # By client and domain: the deliverability sheet (the step) is kept per domain.
        by_dom: dict[tuple[str, str], list[dict]] = defaultdict(list)
        for r in unproven:
            by_dom[(r["client"], r["domain"])].append(r)
        items.append({"key": "sending_unproven",
                      "text": f"{_n(len(unproven))} {'is' if len(unproven) == 1 else 'are'} sending with no weekly test "
                              f"result in the last {usable.V2_FRESH_DAYS} days and {'is' if len(unproven) == 1 else 'are'} "
                              f"on no test campaign, so no weekly test will check {'it' if len(unproven) == 1 else 'them'}",
                      "detail": [f"• {c} · {d}: {_n(len(rs))}, in {', '.join(sorted({x for r in rs for x in r['campaigns']}))}"
                                 + (f"; last test {max(r['v2_date'] for r in rs)[:10]}"
                                    if any(r["v2_date"] for r in rs) else "; never tested")
                                 for (c, d), rs in sorted(by_dom.items())]})
    if left_out and require_pass:
        k = len(left_out)
        items.append({"key": "sending_not_in_test",
                      "text": f"{_n(k)} sending with no weekly test result in the last {usable.V2_FRESH_DAYS} days "
                              f"{'sits' if k == 1 else 'sit'} on a test campaign, but its weekly test leaves "
                              f"{'it' if k == 1 else 'them'} out (a test keeps the inboxes it was created with)",
                      "detail": [f"• {c} · {d}: {_n(len(rs))}, on {_test_names(rs)}"
                                 for (c, d), rs in _by_client_domain(left_out)]})
    if waiting and require_pass:
        items.append(_waiting_item(waiting, today))
    return items


def _test_names(rs: list[dict]) -> str:
    """The test campaigns these inboxes sit on, the DT ones only when there are any (the weekly tests run on the
    "DT <client> #n" campaigns; the main test campaign is the copy check)."""
    names = sorted({t for r in rs for t in r.get("tests") or []})
    dt = [n for n in names if n.upper().startswith("DT ")]
    return ", ".join(dt or names)


def _by_client_domain(records: list[dict]) -> list[tuple[tuple[str, str], list[dict]]]:
    out: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in records:
        out[(r["client"], r["domain"])].append(r)
    return sorted(out.items())


def next_weekly_run(today: date) -> date:
    """The next day the weekly v2 tests run: Saturday (dt_senders.SATURDAY: the weekly tests are set up to start on
    Saturdays after the 05:30 IST seed refill), today when today is a Saturday."""
    return today + timedelta(days=(5 - today.weekday()) % 7)


def _day(d: date) -> str:
    return f"{d:%a} {d.day} {d:%b}"


def _runs_text(runs: list[dict]) -> str:
    """'last runs 14 Nov and 16 Nov sent 1 email per provider (4 needed)' for one domain's short runs."""
    nights = sorted({r["night"] for r in runs})
    when = " and ".join(f"{date.fromisoformat(n).day} {date.fromisoformat(n):%b}" for n in nights[-2:])
    sent = sorted({r["sent"] for r in runs if r.get("sent")})
    word = "run" if len(nights[-2:]) == 1 else "runs"
    if sent and max(sent) < _seeds_needed():
        many = "-".join(str(x) for x in (sent[:1] + sent[-1:] if len(sent) > 1 else sent))
        return f"last {word} {when} sent {many} email{'s' if sent[-1] != 1 else ''} per provider ({_seeds_needed()} needed)"
    return f"last {word} {when} came back incomplete (not every seed result came in)"


def _seeds_needed() -> int:
    from smartlead import v2_results
    return int(v2_results.PASS_SEEDS)


def _waiting_item(waiting: list[dict], today: date) -> dict:
    """One info line for inboxes in the weekly test that have no result to judge (c. in ``needs_you``)."""
    k = len(waiting)
    nxt = _day(next_weekly_run(today))
    short = [r for r in waiting if r.get("short_runs")]
    few = all(0 < x["sent"] < _seeds_needed() for r in short for x in r["short_runs"])
    if len(short) == k:
        what = "sent too few emails to judge" if few else "came back short or incomplete, too little to judge"
        text = (f"{_n(k)} sending with no weekly test result in the last {usable.V2_FRESH_DAYS} days "
                f"{'is' if k == 1 else 'are'} in the weekly test, but the last Smart Delivery runs {what} "
                f"{'it' if k == 1 else 'them'}; next run {nxt}")
    else:
        text = (f"{_n(k)} sending with no weekly test result in the last {usable.V2_FRESH_DAYS} days "
                f"{'is' if k == 1 else 'are'} on a test campaign, so the next weekly run ({nxt}) checks "
                f"{'it' if k == 1 else 'them'}")
    detail = []
    for (c, d), rs in _by_client_domain(waiting):
        runs = [x for r in rs for x in r.get("short_runs") or []]
        on = _test_names(rs)
        if runs:
            why = _runs_text(runs)
        elif any(r.get("weekly_test") == "in" for r in rs):
            why = f"no run in the last {usable.V2_FRESH_DAYS} days"
        else:
            why = "the next weekly run checks them"
        where = "in the weekly test of" if any(r.get("weekly_test") == "in" for r in rs) else "on"
        detail.append(f"• {c} · {d}: {_n(len(rs))}, {where} {on}; {why}")
    return {"key": "sending_test_short", "info": True, "text": text, "detail": detail}


def add_test_facts(records: list[dict], missing: set[str] | None, v2_rows, today: date) -> list[dict]:
    """``records`` with, for each inbox sending without a fresh pass, ``weekly_test`` ("missing": in no active
    weekly test per dt_senders' coverage record; "in": in one; None: not known, ``missing`` None) and
    ``short_runs`` ([{night, verdict, sent}]: its v2 rows in the last V2_FRESH_DAYS days that are no verdict,
    newest last; ``sent`` = fewest emails a seed provider got). ``v2_rows`` = Mongo v2_verdicts rows. Pure."""
    rows: dict[str, list[dict]] = defaultdict(list)
    for x in v2_rows or []:
        rows[str(x.get("email") or "").lower()].append(x)
    out = []
    for r in records:
        if r.get("state") != "sending_unproven":
            out.append(r)
            continue
        weekly = None if missing is None else ("missing" if r["email"] in missing else "in")
        short = []
        for x in sorted(rows.get(r["email"]) or [], key=lambda x: str(x.get("night") or "")):
            night = str(x.get("night") or "")[:10]
            try:
                age = (today - date.fromisoformat(night)).days
            except ValueError:
                continue
            if age > usable.V2_FRESH_DAYS or str(x.get("verdict") or "").upper() in usable.JUDGED:
                continue
            sent = [int((x.get(p) or {}).get("sent") or 0) for p in ("google", "microsoft") if x.get(p)]
            short.append({"night": night, "verdict": str(x.get("verdict") or ""),
                          "sent": min((s for s in sent if s), default=0)})
        out.append({**r, "weekly_test": weekly, "short_runs": short})
    return out


def test_facts(db, today: date, emails: set[str]) -> tuple[set[str] | None, list[dict]]:
    """(inboxes in no active weekly test, from dt_senders' newest coverage record of today or yesterday when every
    account's tests were read, else None; the v2_verdicts rows of ``emails``). Never raises: a read that fails
    leaves that fact unknown."""
    missing = None
    try:
        doc = next(iter(db["test_coverage"].find({"_id": {"$gte": (today - timedelta(days=1)).isoformat()}})
                        .sort("_id", -1).limit(1)), None)
        read = (doc or {}).get("tests_read") or {}
        if doc and read and all(read.values()):
            missing = {str(m.get("email") or "").lower() for c in (doc.get("clients") or {}).values()
                       for m in c.get("missing") or []}
    except Exception as exc:  # noqa: BLE001
        print(f"  [Status] test coverage not read: {str(exc)[:120]}")
    rows = []
    if emails:
        try:
            rows = list(db["v2_verdicts"].find({"email": {"$in": sorted(emails)}},
                                               {"_id": 0, "email": 1, "night": 1, "verdict": 1, "google": 1,
                                                "microsoft": 1}))
        except Exception as exc:  # noqa: BLE001
            print(f"  [Status] v2 verdicts not read: {str(exc)[:120]}")
    return missing, rows


def format_post(records: list[dict], changes: list[dict], items: list[dict], today: date,
                saved_error: str = "") -> tuple[list[str], list[str]]:
    """(headline lines, thread parts) for the one daily fleet post. Pure."""
    count = Counter(r["state"] for r in records)
    day = f"{today.day} {today.strftime('%b')}"
    info = [it for it in items if it.get("info")]
    items = [it for it in items if not it.get("info")]
    head = [(f"⚠️ Inboxes, {day}: {len(items)} thing{'s' if len(items) != 1 else ''} need{'' if len(items) != 1 else 's'} you"
             if items else f"✅ Inboxes, {day}: nothing needs you")]
    head += [f"{i}. {it['text']}." for i, it in enumerate(items, 1)]
    head += [f"For info: {it['text']}." for it in info]
    live = sum(1 for r in records if r["campaigns"])
    head.append(f"{_n(len(records))} in Smartlead, {live} in live campaigns. The list is in the thread.")
    if saved_error:
        head.append(f"History not saved today ({saved_error})")
    parts = ["\n".join([f"*{i}. {it['text']}*"] + it["detail"]) for i, it in enumerate(items, 1)]
    parts += ["\n".join([f"*For info: {it['text']}*"] + it["detail"]) for it in info]
    waiting = sum(1 for r in records if r["state"] == "sending_unproven" and r.get("in_test"))
    parts.append("\n".join(["*Every inbox, by where it stands*"]
                           + [f"• {LABEL[s]}: {count[s]}" for s in ORDER if count.get(s)]
                           + ([f"{_n(waiting)} sending with no result in {usable.V2_FRESH_DAYS} days "
                               f"{'is' if waiting == 1 else 'are'} on a test campaign: the next weekly test covers "
                               f"{'it' if waiting == 1 else 'them'}."] if waiting and not info else [])))
    by_client: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        by_client[r["client"] or "no client"][r["state"]] += 1
    parts.append("\n".join(["*By client*"] + [
        f"• {c}: " + ", ".join(f"{k[s]} {LABEL[s]}" for s in ORDER if k.get(s)) for c, k in sorted(by_client.items())]))
    idle_off: dict[str, list[str]] = defaultdict(list)
    for r in records:
        if r["state"] == "disconnected" and not r["campaigns"]:
            idle_off[r["client"] or "no client"].append(r["domain"])
    if idle_off:
        parts.append("\n".join(["*Not connected and in no sending campaign (the renewal review asks about these at their bill)*"]
                               + [f"• {c}: {_n(len(ds))} on {', '.join(sorted(set(ds))[:12])}"
                                  + (f" and {len(set(ds)) - 12} more domains" if len(set(ds)) > 12 else "")
                                  for c, ds in sorted(idle_off.items())]))
    if changes:
        moved = Counter((c.get("from") or "new", c["to"]) for c in changes)
        lines = [f"• {k}: {LABEL.get(a, a)} → {LABEL.get(b, b)}" for (a, b), k in moved.most_common()]
        lines += [f"• {c['email']}: {LABEL.get(c.get('from') or '', 'new')} → {LABEL.get(c['to'], c['to'])}"
                  for c in changes if c.get("from")][:40]
        parts.append("\n".join(["*Changes since the last run*"] + lines))
    return head, parts


# ── Mongo ────────────────────────────────────────────────────────────────────

def changes_since(previous: dict[str, dict], records: list[dict], today: date, complete: bool) -> list[dict]:
    """History docs for every inbox whose state differs from ``previous`` ({email: status doc}). An inbox not
    seen today becomes ``gone`` only when every account was read (``complete``). Pure."""
    out = []
    seen = set()
    for r in records:
        seen.add(r["email"])
        before = (previous.get(r["email"]) or {}).get("state")
        if before != r["state"]:
            out.append({"email": r["email"], "client": r["client"], "domain": r["domain"], "date": today.isoformat(),
                        "from": before, "to": r["state"], "v2_status": r["v2_status"], "v2_date": r["v2_date"],
                        "campaigns": r["campaigns"], "busy_reason": r["busy_reason"]})
    if complete:
        for email, doc in previous.items():
            if email not in seen and doc.get("state") != "gone":
                out.append({"email": email, "client": doc.get("client", ""), "domain": email.split("@")[-1],
                            "date": today.isoformat(), "from": doc.get("state"), "to": "gone", "v2_status": "",
                            "v2_date": "", "campaigns": [], "busy_reason": ""})
    return out


def save(records: list[dict], today: date, complete: bool, db=None) -> dict:
    """Write today's states and the changes. {"changes": [...], "saved": n, "error": ""}. Never raises."""
    try:
        from pymongo import UpdateOne
        if db is None:
            db = usable._db()
        col, hist = db[STATUS_COLLECTION], db[HISTORY_COLLECTION]
        previous = {d["email"]: d for d in col.find({}, {"_id": 0, "email": 1, "state": 1, "since": 1, "client": 1})}
        changes = changes_since(previous, records, today, complete)
        changed = {c["email"] for c in changes}
        ops = []
        for r in records:
            since = today.isoformat() if r["email"] in changed else (previous.get(r["email"]) or {}).get("since") or today.isoformat()
            ops.append(UpdateOne({"email": r["email"]}, {"$set": {**r, "since": since},
                                                         "$setOnInsert": {"first_seen": today.isoformat()}}, upsert=True))
        for c in changes:
            if c["to"] == "gone":
                ops.append(UpdateOne({"email": c["email"]}, {"$set": {"state": "gone", "since": today.isoformat(),
                                                                       "date": today.isoformat(), "campaigns": []}}))
        if ops:
            col.bulk_write(ops, ordered=False)
        if changes:
            # one doc per (inbox, day): a second run the same day replaces it instead of adding a duplicate
            hist.bulk_write([UpdateOne({"email": c["email"], "date": c["date"]}, {"$set": c}, upsert=True)
                             for c in changes], ordered=False)
        return {"changes": changes, "saved": len(records), "error": ""}
    except Exception as exc:  # noqa: BLE001 - the history is a record; the sync must go on
        print(f"  [Status] inbox status not saved: {str(exc)[:150]}")
        return {"changes": [], "saved": 0, "error": str(exc)[:120]}


def _posted_today(db, today: date) -> bool:
    try:
        return (db[META_COLLECTION].find_one({"_id": "post"}) or {}).get("date") == today.isoformat()
    except Exception:  # noqa: BLE001 - not knowing is not a reason to stay silent
        return False


def _mark_posted(db, today: date) -> None:
    try:
        db[META_COLLECTION].replace_one({"_id": "post"}, {"_id": "post", "date": today.isoformat()}, upsert=True)
    except Exception as exc:  # noqa: BLE001
        print(f"  [Status] post marker not saved: {str(exc)[:120]}")


# The digest's version of each item (16 Nov 2026): what is wrong, and the exact step. A live campaign losing half
# its inboxes is posted at once as well.
# Each step works without a thread: the ask carries its list (17 Nov 2026; an urgent post has no thread).
DIGEST_STEPS = {
    "held_but_sending": "In Smartlead, open each campaign listed and remove those inboxes (Email Accounts tab). "
                        "The weekly job also takes out the failed ones once the week's results are in.",
    "held_but_sending_report_only": "In Smartlead, open each campaign listed and remove those inboxes.",
    "campaign_short": "In Smartlead, open each campaign listed and add free inboxes that passed the weekly test "
                      "(campaign, Email Accounts), or lower its daily sending (campaign, Settings) until it has "
                      "enough. If the campaign should not be sending at all, pause it.",
    "disconnected": "Smartlead, Email Accounts, filter Disconnected, Reconnect each inbox listed (or delete it "
                    "if it was cancelled).",
    "sending_unproven": "Add each domain listed to the deliverability sheet (its client's tab, column A); the 06:00 "
                        "IST test-campaign job then puts its inboxes on the weekly test.",
    "sending_not_in_test": "In Smart Delivery, open the weekly test of each test campaign listed and add those inboxes "
                           "to it (or re-create the test from its campaign). The sheet is not the fix: they are on "
                           "the test campaign already.",
}


def numbers(records: list[dict]) -> dict:
    """{client: {group: n}}: every inbox in the All Inboxes groups (usable.team_group), for the digest's numbers."""
    out: dict[str, Counter] = defaultdict(Counter)
    for r in records:
        if r.get("group"):
            out[r["client"] or "no client"][r["group"]] += 1
    return {c: dict(k) for c, k in sorted(out.items())}


def digest_asks(items: list[dict]) -> list:
    from smartlead import digest as dg
    asks = []
    for it in items:
        if it.get("info"):           # a fact, no step: the digest shows it as info, not as a thing to do
            asks.append(dg.Ask(it["text"] + ".", "", key=f"inbox-status:{it['key']}", kind="info",
                               detail=it.get("detail") or []))
            continue
        text = it["text"].split(". ")[0] + "." if it["key"] in DIGEST_STEPS else it["text"] + "."
        asks.append(dg.Ask(text, DIGEST_STEPS.get(it["key"], ""), key=f"inbox-status:{it['key']}",
                           urgent=it["key"] == "campaign_short", detail=it.get("detail") or []))
    return asks


def run(rows: list[dict], today: date, complete: bool, post: bool, db=None) -> dict:
    """The daily step (run.py): states, history, and the post when ``post`` and FLEET_STATUS_POST is not off.
    The post goes out once a day, so a second sync the same day (the /sync button) does not repeat it."""
    from smartlead import notify
    records = states(rows, today)
    if db is None:
        try:
            db = usable._db()
        except Exception as exc:  # noqa: BLE001
            print(f"  [Status] Mongo not reachable: {str(exc)[:120]}")
    res = save(records, today, complete, db) if db is not None else {"changes": [], "saved": 0, "error": "Mongo not reachable"}
    if db is not None:
        unproven = {r["email"] for r in records if r["state"] == "sending_unproven"}
        if unproven:
            records = add_test_facts(records, *test_facts(db, today, unproven), today)
    # the first run records every inbox as new: that is the baseline, not a day's news
    changes = [] if res["changes"] and all(c.get("from") is None for c in res["changes"]) else res["changes"]
    items = needs_you(records, today)
    head, parts = format_post(records, changes, items, today, res["error"])
    out = {"inboxes": len(records), "changes": len(res["changes"]), "needs_you": [i["key"] for i in items if not i.get("info")],
           "headline": head, "thread": parts, "posted": False}
    if post and records and os.getenv("FLEET_STATUS_POST", "on").strip().lower() != "off":
        from smartlead import digest as dg
        if dg.on() and db is not None and dg.report("inbox-status", digest_asks(items), [], ["\n".join(head)] + parts, db=db,
                                                    numbers=numbers(records)):
            out["digest"] = True
            return out
        token = os.getenv("SLACK_BOT_TOKEN", "").strip()
        if token and not (db is not None and _posted_today(db, today)):
            out["posted"] = bool(notify.post_with_thread(token, notify.infra_channel("FLEET_STATUS_CHANNEL"), head, parts))
            if out["posted"] and db is not None:
                _mark_posted(db, today)
    return out
