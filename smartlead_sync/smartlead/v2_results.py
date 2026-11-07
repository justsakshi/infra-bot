"""Smart Delivery v2 results: read the CSV, judge each inbox, decide pull or return. Pure: no network.

v2 has no API (Smartlead support, 5 and 6 Nov 2026): the only way out is the CSV download
(Mailbox, Provider, Landed, Sent at, Send status; 4 Google + 4 Microsoft seeds per inbox).
v2_weekly.py reads the files Manveen downloads and acts on what this module decides.

Rules (pipeline doc PL_INBOX_FLEET_PIPELINE_SHORT_2026-11-06, Manveen 4 Nov: "remove all inboxes
affected from any future campaign unless they improve"; 7 Nov audit PLACEMENT-10: never pull on a
result that is not complete):
  * per provider, 4 seeds: PASS = all 4 in the inbox, FAIL = 3 or more of 4 in spam, WATCH = all 4
    answered with 2-3 in the inbox. Anything else (a seed still pending, a BOUNCED seed, which is a
    seed-side problem, or a test that sent fewer than 4) is not judged and never acted on.
  * an inbox FAILS if either provider fails; PASSES only if both pass.
  * FAIL -> pull it from live campaigns. WATCH twice running -> pull, but only when both are Monday
    (weekly) tests from new files, 6+ days apart; a one-off or history result never counts.
  * a pulled inbox comes back after two passes, at least 6 days apart, both after the pull.
  * if nearly every inbox in one night's tests, or in one test file, is below 4 of 4 at one provider, the
    test is suspect: hold (inboxes already known bad are left out of that count).
"""
from __future__ import annotations

import csv
import io
from datetime import date, datetime, timedelta, timezone

HEADER = ["Mailbox", "Provider", "Landed", "Sent at", "Send status"]
PROVIDERS = {"GOOGLE_WORKSPACE": "google", "MICROSOFT_365": "microsoft"}
REPORTED = {"Inbox", "Spam"}                    # a seed that answered with a placement
FINAL = REPORTED | {"BOUNCED"}                  # a seed whose result will not change any more

PASS, WATCH, FAIL = "PASS", "WATCH", "FAIL"
UNKNOWN, INCOMPLETE, NOT_SENT = "UNKNOWN", "INCOMPLETE", "NOT_SENT"
JUDGED = {PASS, WATCH, FAIL}
PULL, RETURN = "PULL", "RETURN"

PASS_SEEDS = 4              # seeds per provider; a test that sent another number is reported, never judged
FAIL_SPAM = 3               # this many of 4 seeds in spam: fail
PENDING_SHARE = 0.2         # more results than this still pending: the file was downloaded too early
HOLD_SHARE, HOLD_MIN = 0.8, 10
RETURN_GAP_DAYS = 6         # two results 6+ days apart: two different weeks (return, and WATCH-WATCH pull)
RULES = "2026-11-07"        # results stored under older rules are judged again from the kept CSV copies
IST = timezone(timedelta(hours=5, minutes=30))

LIVE = {"ACTIVE", "PAUSED", "DRAFTED", "SCHEDULED"}
TEST_WORDS = ("deliverability", "spam test", "placement test", "placement-test")


# --- reading ---------------------------------------------------------------------

def _header(text: str) -> list[str]:
    first = text.lstrip("﻿").split("\n", 1)[0].strip().strip("\r")
    return [c.strip().strip('"') for c in first.split(",")]


def is_v2_csv(text: str) -> bool:
    return _header(text) == HEADER


def looks_like_v2(text: str) -> bool:
    """A CSV with a Mailbox column that is not the v2 header: likely a v2 download whose format changed.
    Reported, never read (a silent skip made the Tuesday reminder say 'not in yet' forever)."""
    return not is_v2_csv(text) and any(c.lower() == "mailbox" for c in _header(text))


def parse(text: str) -> list[dict]:
    out = []
    for row in csv.DictReader(io.StringIO(text.lstrip("﻿"))):
        mb = str(row.get("Mailbox") or "").strip().lower()
        if "@" not in mb:
            continue
        out.append({"mailbox": mb, "provider": str(row.get("Provider") or "").strip(),
                    "landed": str(row.get("Landed") or "").strip(), "sent_at": str(row.get("Sent at") or "").strip(),
                    "send_status": str(row.get("Send status") or "").strip()})
    return out


def _ts(s: str) -> datetime | None:
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None


def night_of(rows: list[dict]) -> str | None:
    """India date of the first send: Monday 20:00 IST runs past midnight but stay Monday's night."""
    times = [t for t in (_ts(r["sent_at"]) for r in rows if r.get("sent_at")) if t]
    return min(times).astimezone(IST).date().isoformat() if times else None


def last_sent(rows: list[dict]) -> str:
    times = [t for t in (_ts(r["sent_at"]) for r in rows if r.get("sent_at")) if t]
    return max(times).isoformat() if times else ""


def emails_per_provider(rows: list[dict]) -> int:
    """How many emails the test sent each inbox per provider (the most common count). Smart Delivery lets a test
    send 1-4; the job needs 4 to judge (pass = 4 of 4), so a 1-email test is reported, never judged."""
    per: dict[tuple[str, str], int] = {}
    for r in rows:
        if r["provider"] in PROVIDERS and r["landed"] != "NOT_SENT":
            k = (r["mailbox"], r["provider"])
            per[k] = per.get(k, 0) + 1
    if not per:
        return 0
    counts: dict[int, int] = {}
    for n in per.values():
        counts[n] = counts.get(n, 0) + 1
    return max(counts, key=lambda n: (counts[n], n))


def is_weekly_night(night: str) -> bool:
    """The DT tests run every Monday 20:00 IST; any other night is a one-off test, so 'missing this week'
    checks do not apply to it."""
    return date.fromisoformat(night).weekday() == 0


def pending_seeds(rows: list[dict]) -> int:
    """Seeds sent but with no final result yet (neither Inbox, Spam nor BOUNCED)."""
    return sum(1 for r in rows if r["landed"] != "NOT_SENT" and r["landed"] not in FINAL)


def file_ready(rows: list[dict]) -> bool:
    """False when too many seeds have not answered yet (downloaded before the results came in)."""
    sent = [r for r in rows if r["landed"] != "NOT_SENT"]
    if not sent:
        return True
    return pending_seeds(rows) / len(sent) <= PENDING_SHARE


# --- verdicts --------------------------------------------------------------------

def provider_verdict(landed: list[str]) -> dict:
    """One provider's seeds for one inbox. PASS = 4 of 4 in the inbox; FAIL = 3+ of 4 in spam; WATCH = all
    4 answered Inbox/Spam with 2-3 in the inbox. Not judged, so never acted on: a test that sent a number
    other than 4 (UNKNOWN), or a seed still pending or BOUNCED (INCOMPLETE; a bounce at a seed address is
    the seed's problem, not the sender's)."""
    inbox, spam, bounced = landed.count("Inbox"), landed.count("Spam"), landed.count("BOUNCED")
    sent = sum(1 for x in landed if x != "NOT_SENT")
    if landed and not sent:
        v = NOT_SENT
    elif sent != PASS_SEEDS:
        v = UNKNOWN
    elif spam >= FAIL_SPAM:
        v = FAIL
    elif inbox + spam < sent:
        v = INCOMPLETE
    elif inbox == sent:
        v = PASS
    else:
        v = WATCH
    return {"inbox": inbox, "reported": inbox + spam, "sent": sent, "bounced": bounced, "verdict": v}


def combine(google: str, microsoft: str) -> str:
    vs = (google, microsoft)
    if FAIL in vs:
        return FAIL
    if vs == (PASS, PASS):
        return PASS
    if vs == (NOT_SENT, NOT_SENT):
        return NOT_SENT
    if all(v in JUDGED for v in vs):
        return WATCH
    return INCOMPLETE                       # a provider not judged and nothing failed: no action either way


def summarise(rows: list[dict]) -> dict[str, dict]:
    """{mailbox: {google: {...}, microsoft: {...}, verdict}} for one file."""
    by: dict[str, dict[str, list[str]]] = {}
    for r in rows:
        p = PROVIDERS.get(r["provider"])
        if p:
            by.setdefault(r["mailbox"], {"google": [], "microsoft": []})[p].append(r["landed"])
    out = {}
    for mb, provs in by.items():
        g, m = provider_verdict(provs["google"]), provider_verdict(provs["microsoft"])
        if not provs["google"]:
            g["verdict"] = UNKNOWN
        if not provs["microsoft"]:
            m["verdict"] = UNKNOWN
        out[mb] = {"google": g, "microsoft": m, "verdict": combine(g["verdict"], m["verdict"])}
    return out


def held_providers(night: dict[str, dict], exclude: set[str] = frozenset()) -> set[str]:
    """Providers where at least 80% of one night's (or one test file's) judged inboxes (10+) were below
    4 of 4: a seed or test problem (one dead seed costs every sender a seed) is likelier than every sender
    going bad at once. ``exclude``: inboxes already known bad, which would push a good night toward a hold."""
    held = set()
    for p in ("google", "microsoft"):
        judged = [s[p]["verdict"] for mb, s in night.items()
                  if mb not in exclude and (s.get(p) or {}).get("verdict") in JUDGED]
        if len(judged) >= HOLD_MIN and (len(judged) - judged.count(PASS)) / len(judged) >= HOLD_SHARE:
            held.add(p)
    return held


def apply_hold(summary: dict, held: set[str]) -> str:
    """The inbox verdict once held providers are set aside."""
    if not held:
        return summary["verdict"]
    g = UNKNOWN if "google" in held else (summary.get("google") or {}).get("verdict", UNKNOWN)
    m = UNKNOWN if "microsoft" in held else (summary.get("microsoft") or {}).get("verdict", UNKNOWN)
    return combine(g, m)


def merge_results(results: list[dict], new: list[dict]) -> list[dict]:
    """One result per night per inbox; a later run of the same night replaces an earlier one."""
    by_night = {x["night"]: x for x in results}
    for x in new:
        old = by_night.get(x["night"])
        if old is None or str(x.get("sent_last") or "") >= str(old.get("sent_last") or ""):
            by_night[x["night"]] = x
    return [by_night[k] for k in sorted(by_night)]


# --- decisions ---------------------------------------------------------------------

def two_weeks_running(a: dict, b: dict) -> bool:
    """a then b count as two weeks running: both Monday (weekly) tests from new files, 6+ days apart.
    A one-off midweek test, or a history file, never pairs (audit PLACEMENT-10)."""
    if a.get("history") or b.get("history") or not (is_weekly_night(a["night"]) and is_weekly_night(b["night"])):
        return False
    return (date.fromisoformat(b["night"]) - date.fromisoformat(a["night"])).days >= RETURN_GAP_DAYS


def decide(results: list[dict], pulled: dict | None) -> dict:
    """results: oldest first, each {night, verdict, history}. pulled: {on: date} if the bot pulled it.
    Only a result from a new file (not history) can start an action."""
    judged = [x for x in results if x["verdict"] in JUDGED]
    if not judged:
        return {"action": None, "why": "no judged result"}
    last = judged[-1]
    if pulled:
        after = [x for x in judged if x["night"] > str(pulled.get("on") or "")]
        if len(after) >= 2 and not last.get("history"):
            a, b = after[-2], after[-1]
            gap = (date.fromisoformat(b["night"]) - date.fromisoformat(a["night"])).days
            if a["verdict"] == PASS and b["verdict"] == PASS and gap >= RETURN_GAP_DAYS:
                return {"action": RETURN, "why": f"passed {a['night']} and {b['night']}"}
        return {"action": None, "why": "pulled; waiting for two passes"}
    if last.get("history"):
        return {"action": None, "why": "latest result is history"}
    if last["verdict"] == FAIL:
        return {"action": PULL, "why": f"failed {last['night']}"}
    if last["verdict"] == WATCH and len(judged) >= 2 and judged[-2]["verdict"] in (WATCH, FAIL):
        if two_weeks_running(judged[-2], last):
            return {"action": PULL, "why": f"below 4 of 4 two weeks running ({judged[-2]['night']}, {last['night']})"}
        return {"action": None, "why": "watch; the result before it is not last week's Monday test"}
    return {"action": None, "why": last["verdict"].lower()}


# --- campaigns ---------------------------------------------------------------------

def is_test_campaign(name: str) -> bool:
    n = str(name or "").strip().lower()
    return n.startswith("dt ") or any(w in n for w in TEST_WORDS)


def is_live(status: str) -> bool:
    return str(status or "").upper() in LIVE


def pull_plan(ids: set[int], camps: dict[int, dict], also_bad: set[int] = frozenset()) -> dict:
    """camps: {campaign_id: {name, status, senders: set of inbox ids}}. Which inboxes come out of
    which live, non-test campaigns. A campaign that would be left with no good sender (``also_bad``:
    inboxes already tagged DO NOT USE) is not touched: its leads would only move to a bad inbox."""
    remove, blocked = {}, {}
    for cid, c in sorted(camps.items()):
        if not is_live(c["status"]) or is_test_campaign(c["name"]):
            continue
        mine = sorted(ids & set(c["senders"]))
        if not mine:
            continue
        if len(set(c["senders"]) - set(mine) - set(also_bad)) < 1:
            blocked[cid] = mine
        else:
            remove[cid] = mine
    return {"remove": remove, "blocked": blocked}


# --- the week --------------------------------------------------------------------

def last_monday(d: date) -> date:
    return d - timedelta(days=d.weekday())


def dt_missing(dt_tests: dict[str, list[str]], tested: set[str]) -> list[str]:
    """The DT test campaigns (name -> sender emails) none of whose inboxes has a result: each one's CSV
    was not downloaded (or its test did not run)."""
    return sorted(name for name, emails in dt_tests.items() if emails and not set(emails) & tested)


def reminder_due(now_ist: tuple[date, int], nights_in: set[str], reminded: set[str],
                 since: str = "", missing: list[str] | None = None) -> str | None:
    """From Tuesday 12:00 IST (the pipeline doc's upload time), the Monday whose results never came in,
    or whose expected DT tests (``missing``) did not all come in; once per Monday, and only for Mondays
    from ``since`` (the job's first run) on."""
    today, hour = now_ist
    mon = last_monday(today)
    if (today - mon).days < 1 or ((today - mon).days == 1 and hour < 12):
        return None
    night = mon.isoformat()
    if night < since or night in reminded or (any(n >= night for n in nights_in) and not missing):
        return None
    return night
