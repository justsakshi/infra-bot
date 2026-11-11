"""Numbers for the 10:15 stats post, counted by date (Manveen, 2 Nov 2026).

"It should only track replies and emails sent of the last working day, and
maybe a weekly summary on Friday and a monthly summary on the first working
day. Make sure every number is accurate."

Decided with him the same day:
  * daily: the last working day only (a Monday post covers Friday);
  * weekly, on Fridays: the 7 full days Friday to Thursday;
  * monthly, on the first working day: the whole previous month;
  * replies are people, not messages; everything in the one post.

A day is a UTC day (05:30 to 05:30 IST), the day Smartlead's own analytics
use: counted that way, emails sent match Smartlead's per-day figure exactly
(checked 2 Nov on Clay in Kochi and Melior Executive Search, 27-30 Oct).
HeyReach and the Expandi lead cache bucket by UTC day too.

The old post differenced two 09:00 snapshots, which broke whenever a snapshot
was missing or taken late (2 Nov: Kochi showed 3 replies, its button 10).
Every number here comes from the platform's dated records instead:
  * Smartlead: each sent email's sent_time / reply_time / category;
  * HeyReach: GetOverallStats over the period;
  * Expandi: per-lead invite / accept days cached by the 09:00 run, and the
    n8n "Expandi Responses" log for replies;
  * Waalaxy: the n8n reply and accept logs.
"""
from __future__ import annotations

import asyncio
import calendar
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from smartlead.reply_view import parse_time

# PL's Waalaxy replies reach the n8n log from this day (router
# ErgTadfS9Or2dgyy, 30 Oct 2026): a period starting earlier is incomplete.
WAALAXY_LOG_START = date(2026, 10, 30)

# Smartlead's categories by name (config.SMARTLEAD_POSITIVE_CATEGORY_IDS
# holds the same three by id).
POSITIVE = {"interested", "meeting request", "information request"}
NOT_INTERESTED = {"not interested"}
WRONG_PERSON = {"wrong person"}
OUT_OF_OFFICE = {"out of office"}
BOUNCE_NOTICE = {"sender originated bounce"}   # a bounce, not a reply

EMAIL_FIELDS = ("reached", "sent", "replies", "positive", "not_interested",
                "wrong_person", "out_of_office", "bounced", "later_touch")
# later_touch: of the people who replied, those who answered a follow-up
# (email 2 or later), for the button label (Manveen, 3 Nov: "2nd touch").
LINKEDIN_FIELDS = ("invites", "accepted", "replies", "positive")


@dataclass(frozen=True)
class Period:
    key: str          # "day", "week" or "month"
    start: date       # first UTC day, inclusive
    end: date         # last UTC day, inclusive

    @property
    def window(self) -> tuple[datetime, datetime]:
        """[start, end) as UTC datetimes."""
        lo = datetime.combine(self.start, time.min, timezone.utc)
        return lo, datetime.combine(self.end + timedelta(days=1), time.min, timezone.utc)

    def holds(self, value) -> bool:
        t = parse_time(value)
        lo, hi = self.window
        return t is not None and lo <= t < hi


def previous_working_day(today: date) -> date:
    d = today - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def is_first_working_day(today: date) -> bool:
    if today.weekday() >= 5:
        return False
    return all((today - timedelta(days=n)).weekday() >= 5 for n in range(1, today.day))


def periods_for(today: date) -> list[Period]:
    """The day always; the week on Fridays; last month on its first working day."""
    day = previous_working_day(today)
    out = [Period("day", day, day)]
    if today.weekday() == 4:
        out.append(Period("week", today - timedelta(days=7), today - timedelta(days=1)))
    if is_first_working_day(today):
        last = today.replace(day=1) - timedelta(days=1)
        out.append(Period("month", last.replace(day=1), last))
    return out


def widest(periods: list[Period]) -> tuple[date, date]:
    return min(p.start for p in periods), max(p.end for p in periods)


# ── email ──────────────────────────────────────────────────────────────────

def _kind(category) -> str:
    c = str(category or "").strip().lower()
    if c in BOUNCE_NOTICE:
        return "bounce"
    if c in OUT_OF_OFFICE:
        return "ooo"
    if c in POSITIVE:
        return "positive"
    if c in NOT_INTERESTED:
        return "not_interested"
    if c in WRONG_PERSON:
        return "wrong_person"
    return "other"


def email_counts(stats: list[dict], period: Period) -> dict:
    """One campaign's numbers for one period, from its per-email statistics.

    sent: emails whose sent_time falls in the period (Smartlead's per-day
    sent_count, exactly). reached: of those, first emails (sequence 1).
    bounced: of those, the ones that bounced (Smartlead's bounce_count).
    replies: people whose FIRST reply falls in the period, one each, by their
    category (Manveen 11 Nov: "count first reply and not all replies"; from 30
    Oct to 11 Nov a later message counted the person again that day). Bounce
    notices (a reply on a bounced email, tagged or not yet: 30 Oct had two
    untagged, seconds after sending) are not replies; out-of-office replies
    are counted on their own.
    """
    out = dict.fromkeys(EMAIL_FIELDS, 0)
    category: dict[str, str] = {}
    touch: dict[str, int] = {}
    first: dict[str, datetime] = {}
    for r in stats:
        if period.holds(r.get("sent_time")):
            out["sent"] += 1
            if str(r.get("sequence_number") or "") == "1":
                out["reached"] += 1
            if r.get("is_bounced"):
                out["bounced"] += 1
        email = str(r.get("lead_email") or "").strip().lower()
        t = parse_time(r.get("reply_time"))
        if not email or t is None or r.get("is_bounced"):
            continue
        kind = _kind(r.get("lead_category"))
        if kind == "bounce":
            continue
        touch[email] = max(touch.get(email, 0), int(r.get("sequence_number") or 1))
        if category.get(email, "other") == "other":
            category[email] = kind
        if email not in first or t < first[email]:
            first[email] = t
    for email, t in first.items():
        if not period.holds(t):
            continue
        kind = category.get(email, "other")
        if kind == "ooo":
            out["out_of_office"] += 1
            continue
        out["replies"] += 1
        if kind in ("positive", "not_interested", "wrong_person"):
            out[kind] += 1
        if touch.get(email, 1) >= 2:
            out["later_touch"] += 1
    return out


def moved(counts: dict) -> bool:
    return any(counts.get(f) for f in counts)


async def _smartlead(periods: list[Period], failed: list[str], log: str) -> list[dict]:
    from smartlead import campaign_metrics as cm
    from smartlead.accounts import discover_accounts
    from smartlead.api import SmartleadClient
    from smartlead.client_filter import is_past_client_campaign
    from smartlead.config import CAMPAIGN_METRICS_CLIENTS, CAMPAIGN_METRICS_EXCLUDE
    from smartlead.metrics_collect import _each_with_retry

    lo, hi = widest(periods)
    check_from = min(lo, hi - timedelta(days=30))
    if (hi - check_from).days > 30:
        check_from = lo
    rows: list[dict] = []
    for acc in discover_accounts():
        if not cm.account_in_scope(acc.name, CAMPAIGN_METRICS_CLIENTS):
            continue
        try:
            async with SmartleadClient(acc.api_key, acc.name) as slc:
                wanted = []
                for camp in await slc.list_campaigns():
                    cid = str(camp.get("id") or "")
                    status = str(camp.get("status", "")).upper()
                    if not cid or status.startswith("DRAFT"):
                        continue
                    if (cm.is_excluded_campaign_name(camp.get("name", ""), CAMPAIGN_METRICS_EXCLUDE)
                            or is_past_client_campaign(camp)):
                        continue
                    client = cm.display_client(cm.metrics_client_for(acc.name, camp.get("client_id")))
                    if cm.row_client_wanted(client, CAMPAIGN_METRICS_CLIENTS):
                        wanted.append((camp, cid, client))

                async def fetch(item, slc=slc):
                    camp, cid, client = item
                    # One cheap call first: most campaigns did nothing in the
                    # period, and their statistics run to thousands of rows.
                    # The range reaches back 30 days (Smartlead's maximum).
                    a = await slc.get_analytics_by_date(cid, check_from.isoformat(), hi.isoformat())
                    if not any(int(a.get(f) or 0) for f in
                               ("sent_count", "total_reply_count", "reply_count", "bounce_count")):
                        return None
                    stats = await slc.get_campaign_statistics(cid)
                    return {"client": client, "channel": "Email", "platform": "Smartlead",
                            "campaign": str(camp.get("name", "")), "campaign_id": cid,
                            "counts": {p.key: email_counts(stats, p) for p in periods}}

                found = await _each_with_retry(
                    wanted, fetch, lambda it, a=acc.name: f"Smartlead {a}: {it[0].get('name', '')}", log,
                    failed=failed)
                rows += found
                print(f"{log} Smartlead {acc.name}: {len(found)} of {len(wanted)} campaign(s) moved")
        except Exception as exc:  # noqa: BLE001
            print(f"{log} Smartlead {acc.name} failed: {exc}")
            failed.append(f"Smartlead {acc.name}")
    return rows


# ── LinkedIn ───────────────────────────────────────────────────────────────

def heyreach_counts(overall: dict) -> dict:
    """GetOverallStats for a period. Its reply figure counts a conversation
    once, on its first reply, which is what the post counts (Manveen 11 Nov).
    From 27 Oct to 11 Nov it was replaced by everyone who wrote in the period."""
    o = (overall or {}).get("overallStats") or {}
    n = lambda k: int(o.get(k) or 0)  # noqa: E731
    return {"invites": n("connectionsSent"), "accepted": n("connectionsAccepted"),
            "replies": n("totalMessageReplies") + n("totalInmailReplies"),
            "positive": n("autoTaggedInterested")}


async def _heyreach(periods: list[Period], failed: list[str], log: str) -> list[dict]:
    from smartlead import campaign_metrics as cm
    from smartlead.config import CAMPAIGN_METRICS_CLIENTS
    from smartlead.heyreach import HeyReachClient
    from smartlead.heyreach_accounts import discover_heyreach_workspaces
    from smartlead.metrics_collect import _each_with_retry

    rows: list[dict] = []
    for ws in discover_heyreach_workspaces():
        if not cm.row_client_wanted(ws.name, CAMPAIGN_METRICS_CLIENTS):
            continue
        try:
            async with HeyReachClient(ws.api_key, ws.name) as hrc:
                camps = await hrc.list_campaigns()

                async def fetch(camp, hrc=hrc):
                    counts = {}
                    for p in periods:
                        lo, hi = p.window
                        counts[p.key] = heyreach_counts(await hrc.get_overall_stats(
                            camp["id"], start=lo.isoformat().replace("+00:00", "Z"),
                            end=(hi - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")))
                    if not any(moved(c) for c in counts.values()):
                        return None
                    return {"client": cm.display_client(ws.name), "channel": "LinkedIn",
                            "platform": "Heyreach", "campaign": camp.get("name", ""),
                            "campaign_id": str(camp["id"]), "counts": counts}

                rows += await _each_with_retry(
                    camps, fetch, lambda c, w=ws.name: f"HeyReach {w}: {c.get('name', c['id'])}", log,
                    failed=failed)
        except Exception as exc:  # noqa: BLE001
            print(f"{log} HeyReach {ws.name} failed: {exc}")
            failed.append(f"HeyReach {ws.name}")
    return rows


def _person(r: dict) -> str:
    return str(r.get("lead") or r.get("name") or "").strip().lower()


def first_reply_people(every: list[dict], window: list[dict]) -> tuple[int, int]:
    """(people, positive people) whose first row in the whole reply log
    (`every`, one campaign) is among `window`, the rows inside the period.
    A person who writes again later is not counted again (Manveen 11 Nov).
    Positive if any of their rows is."""
    first: dict[str, str] = {}
    pos: set[str] = set()
    for r in every:
        k, t = _person(r), str(r.get("time") or "")
        if k not in first or t < first[k]:
            first[k] = t
        if str(r.get("category") or "").strip().lower() in ("interested", "positive"):
            pos.add(k)
    people = {k for r in window if first.get(k := _person(r)) == str(r.get("time") or "")}
    return len(people), len(people & pos)


def expandi_counts(messengers: list[dict], period: Period) -> dict:
    """Invites and accepts in a period from Expandi's per-lead rows
    (invited_at / connected_at); one row per lead and campaign instance."""
    from smartlead.expandi_leads import _day
    lo, hi = period.start.isoformat(), period.end.isoformat()
    inside = lambda v: (d := _day(v)) is not None and lo <= d <= hi  # noqa: E731
    return {"invites": sum(1 for m in messengers if inside(m.get("invited_at"))),
            "accepted": sum(1 for m in messengers if inside(m.get("connected_at")))}


async def _expandi_leads(exc_client, camp: dict, tries: int = 2) -> tuple[list[dict], str]:
    """(every lead row of a campaign across its LinkedIn accounts, why the
    read fell short or ""). A short read is tried again once: one failed page
    out of hundreds should not cost the day's numbers."""
    why = ""
    for _ in range(tries):
        leads: list[dict] = []
        why = ""
        for acc_id, inst_id in camp.get("_instance_refs", []):
            try:
                got, done = await exc_client.list_messengers(acc_id, inst_id, page_size=100,
                                                             max_pages=1000)
            except Exception as exc:  # noqa: BLE001
                got, done = [], False
                exc_client.last_error = type(exc).__name__
            if not done:
                why = getattr(exc_client, "last_error", "") or "stopped early"
                break
            leads += got
        if not why:
            return leads, ""
    return [], why


async def _expandi(periods: list[Period], failed: list[str], log: str) -> list[dict]:
    """Invites and accepts read live from each campaign's lead list; replies
    from the n8n Expandi Responses log (the button's source).

    Live, not the per-lead cache the 09:00 run tops up: that sweep stops at the
    first page of leads it already holds, so a lead cached before being invited
    never gets its invite date (3 Nov: the post said 0 invites for Monday,
    Expandi had 31+). Every campaign with people is read, paused ones too,
    since invitations sent earlier are still accepted after a pause.
    """
    from smartlead import campaign_metrics as cm
    from smartlead.config import BETTRDATA_TRACKER_SHEET_ID, CAMPAIGN_METRICS_CLIENTS
    from smartlead.expandi import ExpandiClient
    from smartlead.expandi_accounts import discover_expandi_workspaces
    from smartlead.expandi_leads import ExpandiLeadStore
    from smartlead.expandi_rows import _merge_by_name
    from smartlead.reply_view import EXPANDI_LOG_TAB, expandi_log_replies
    from smartlead.sheets import SheetsWriter

    rows: list[dict] = []
    for ws in discover_expandi_workspaces():
        if not cm.row_client_wanted(ws.name, CAMPAIGN_METRICS_CLIENTS):
            continue
        client = cm.display_client(ws.name)
        try:
            log_values = (await asyncio.to_thread(
                lambda: SheetsWriter(BETTRDATA_TRACKER_SHEET_ID)._sh
                .worksheet(EXPANDI_LOG_TAB).get_all_values())
                if client == "BettrData" else [])
            store = ExpandiLeadStore()
            async with ExpandiClient(ws.api_key, ws.api_secret, ws.name) as exc_client:
                raw: list[dict] = []
                for acc in await exc_client.list_li_accounts():
                    if acc.get("id") is not None:
                        raw.extend(await exc_client.list_campaigns(acc["id"]))
                camps = [c for c in _merge_by_name(raw) if not c.get("archived")
                         and int((c.get("stats") or {}).get("people_in_campaign") or 0)]
                short: list[str] = []
                for camp in camps:
                    name = str(camp.get("name", "")).strip()
                    leads, why = await _expandi_leads(exc_client, camp)
                    if why:
                        # Until 11 Nov one short read dropped every BettrData
                        # LinkedIn number, replies too (4-10 Nov posts said
                        # "no activity"). Now only this campaign's invites and
                        # accepts are left out; replies come from the log.
                        print(f"{log} Expandi {ws.name}: {name} lead list not fully read ({why})")
                        short.append(why)
                    else:
                        store.save(ws.name, name, leads)  # keeps the 09:00 cache current too
                    every = expandi_log_replies(log_values, name, None, None)
                    counts = {}
                    for p in periods:
                        lo, hi = p.window
                        people, pos = first_reply_people(every, expandi_log_replies(log_values, name, lo, hi))
                        known = expandi_counts(leads, p) if not why else {"invites": None, "accepted": None}
                        counts[p.key] = {**known, "replies": people, "positive": pos}
                    if any(moved(c) for c in counts.values()):
                        rows.append({"client": client, "channel": "LinkedIn", "platform": "Expandi",
                                     "campaign": name, "campaign_id": name, "counts": counts})
                if short:
                    failed.append(f"Expandi {ws.name} invites and accepts for {len(short)} of "
                                  f"{len(camps)} campaigns ({short[0]}); replies are counted")
        except Exception as exc:  # noqa: BLE001
            print(f"{log} Expandi {ws.name} failed: {exc}")
            failed.append(f"Expandi {ws.name}")
    return rows


def waalaxy_rows(replies_log: list[list[str]], accepts_log: list[list[str]],
                 periods: list[Period]) -> list[dict]:
    """PL LinkedIn from the n8n logs: replies by Received at, accepts by
    Connected on. Waalaxy has no invite count anywhere."""
    from smartlead import waalaxy_log as wl

    names = {r["Campaign"].strip() for r in wl._records(replies_log)}
    accepts = [r for r in wl._rows(accepts_log) if r.get("Campaign", "").strip()
               and r.get("Connected on", "").strip()]
    names |= {r["Campaign"].strip() for r in accepts}
    bad = [r for r in accepts if parse_time(_day_or_time(r["Connected on"])) is None]
    if bad:
        raise ValueError(f"{len(bad)} Waalaxy accept date(s) unreadable")
    tracked = {r["Campaign"].strip() for r in accepts}
    rows = []
    for name in sorted(names):
        counts = {}
        for p in periods:
            lo, hi = p.window
            people, pos = first_reply_people(wl.replies(replies_log, name, None, None),
                                             wl.replies(replies_log, name, lo, hi))
            # Accepts exist only for campaigns with the CRM Sync step: for
            # the rest the number is unknown, not 0.
            acc = (sum(1 for r in accepts if r["Campaign"].strip() == name
                       and p.holds(_day_or_time(r["Connected on"])))
                   if name in tracked else None)
            counts[p.key] = {"invites": None, "accepted": acc, "replies": people, "positive": pos}
        if any(moved(c) for c in counts.values()):
            rows.append({"client": wl.CLIENT, "channel": "LinkedIn", "platform": "Waalaxy",
                         "campaign": name, "campaign_id": name, "counts": counts})
    return rows


def _day_or_time(value: str) -> str:
    """A log cell as an ISO time: '2026-10-30' becomes midnight UTC that day."""
    s = str(value or "").strip()
    return f"{s}T00:00:00+00:00" if len(s) == 10 else s


def _waalaxy(periods: list[Period], failed: list[str], log: str) -> list[dict]:
    from smartlead import campaign_metrics as cm
    from smartlead import waalaxy_log as wl
    from smartlead.config import CAMPAIGN_METRICS_CLIENTS

    if not cm.row_client_wanted(wl.CLIENT, CAMPAIGN_METRICS_CLIENTS):
        return []
    try:
        return waalaxy_rows(wl.read_log(), wl.read_log(wl.ACCEPTS_TAB), periods)
    except Exception as exc:  # noqa: BLE001
        print(f"{log} Waalaxy logs failed: {exc}")
        failed.append("Waalaxy logs")
        return []


async def collect(periods: list[Period], log: str = "[stats-post]") -> tuple[list[dict], list[str]]:
    """Every campaign that moved in any period, and what could not be read."""
    failed: list[str] = []
    # Side by side: Smartlead takes ~8 min, reading every Expandi lead ~5.5.
    parts = await asyncio.gather(_smartlead(periods, failed, log), _heyreach(periods, failed, log),
                                 _expandi(periods, failed, log),
                                 asyncio.to_thread(_waalaxy, periods, failed, log))
    return [r for part in parts for r in part], failed


def month_name(p: Period) -> str:
    return f"{calendar.month_name[p.start.month]} {p.start.year}"
