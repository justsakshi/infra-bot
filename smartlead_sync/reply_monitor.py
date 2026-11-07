#!/usr/bin/env python3
"""Reply check per sending domain (daily 13:00 IST, read-only on Smartlead).

Each day: read every ACTIVE campaign's per-sender totals, store one snapshot per sending domain (with a
per-campaign breakdown) in Mongo domain_reply_stats, then judge each domain's last 14 days against the
reply rate of the other domains in its campaigns (smartlead/reply_stats.py has the rule and why).

What it posts to the infra channel (on by default since the 7 Nov rule; REPLY_MONITOR_POST=off silences it):
  * any day: a domain that newly reached ALERT, and "Not checked today: <client>" for an account it
    could not read;
  * Mondays: the weekly summary, every client, every judged domain counted.
Everything is printed to the log as well. Report only: it never pauses or changes anything."""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date, datetime, timedelta, timezone

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

from smartlead import reply_stats as rs
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.config import HEALTH_HISTORY_DB, REPLY_STATS_COLLECTION

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

IST = timezone(timedelta(hours=5, minutes=30))


def _col():
    """Connect to the reply-stats collection. Returns None (graceful no-op)
    if MONGO_URI is unset, pymongo isn't installed, or the connection/ping
    fails — matches smartlead/health_store.py's fail-open pattern so a
    Mongo outage never breaks the (more important) live API collection."""
    uri = os.getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        print("  [ReplyMon] Mongo unavailable (no MONGO_URI) - history disabled.")
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        col = client[HEALTH_HISTORY_DB][REPLY_STATS_COLLECTION]
        col.create_index([("domain", 1), ("date", 1)], unique=True)
        return col
    except Exception as exc:  # noqa: BLE001
        print(f"  [ReplyMon] Mongo connect failed ({exc}) - history disabled.")
        return None


def _why(exc: Exception, key: str = "") -> str:
    """Status and Smartlead's short message only (the request URL holds the key)."""
    import httpx
    from smartlead.notify import redact
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            msg = str(exc.response.json().get("message") or "")
        except Exception:  # noqa: BLE001 - not JSON: the raw text, short
            msg = ""
        msg = " ".join((msg or exc.response.text).split())[:80]
        msg = msg.replace(key, "<key>") if key else msg
        return redact(f"HTTP {exc.response.status_code} {msg}".strip())
    return type(exc).__name__


def clients_of(account: str) -> str:
    """Who an account's domains belong to, for a "Not checked today" line."""
    from smartlead.report_sheet import client_label
    if account.strip().lower().replace("_", " ") == "precise leads":
        return "Precise Leads and Melior"
    return client_label(account, None)


async def collect() -> tuple[dict[str, dict], dict[str, str]]:
    """Today's snapshot {domain: doc} across all ACTIVE campaigns, and {account: why} for accounts not read.
    An account is merged only when every one of its campaigns was read, so a half-read account never
    looks like a domain that stopped sending."""
    from smartlead.client_filter import is_past_client_campaign
    from smartlead.report_sheet import client_label
    fleet: dict[str, dict] = {}
    unread: dict[str, str] = {}
    for acc in discover_accounts():
        mine: dict[str, dict] = {}
        try:
            async with SmartleadClient(acc.api_key, acc.name) as c:
                camps = [x for x in await c.list_campaigns() or []
                         if str(x.get("status", "")).upper() == "ACTIVE" and not is_past_client_campaign(x)]
                for camp in camps:
                    rows = await c.get_campaign_mailbox_statistics(str(camp["id"]))
                    rs.merge_campaign(mine, camp["id"], client_label(acc.name, camp.get("client_id")), acc.name,
                                      rs.aggregate_domain_stats(rows))
        except Exception as exc:  # noqa: BLE001 - one dead account must not hide the others
            unread[acc.name] = _why(exc, acc.api_key)
            print(f"  [ReplyMon] Not checked today: {clients_of(acc.name)} ({unread[acc.name]})")
            continue
        for dom, doc in mine.items():
            if dom in fleet:                             # the same domain in two accounts: keep both totals
                for k in rs.COUNTS:
                    fleet[dom][k] += doc[k]
                fleet[dom]["campaigns"].update(doc["campaigns"])
            else:
                fleet[dom] = doc
    return fleet, unread


def legacy_client(doc: dict) -> dict:
    """An older snapshot (no "account" field) has the account name as its client; give it the client label
    the new snapshots use (the latest snapshot of a domain decides its client anyway)."""
    if "account" in doc:
        return doc
    from smartlead.report_sheet import client_label
    acc = str(doc.get("client") or "")
    return {**doc, "account": acc, "client": client_label(acc, None) if acc else ""}


def _history(col, today: date) -> list[dict] | None:
    """Stored snapshots before today, or None when there is no history to read (nothing can be judged)."""
    if col is None:
        return None
    since = (today - timedelta(days=rs.WINDOW_DAYS + rs.LOOKBACK_DAYS)).isoformat()
    try:
        return [legacy_client(d) for d in col.find({"date": {"$gte": since, "$lt": today.isoformat()}}, {"_id": 0})]
    except PyMongoError as exc:
        print(f"  [ReplyMon] Mongo read failed: {exc}")
        return None


def prior_verdicts(history: list[dict]) -> dict[str, str]:
    """{domain: verdict on its last judged day before today}."""
    out: dict[str, tuple[str, str]] = {}
    for d in history:
        if d.get("verdict") and d["date"] > out.get(d["domain"], ("", ""))[0]:
            out[d["domain"]] = (d["date"], d["verdict"])
    return {k: v for k, (_, v) in out.items()}


# ── Slack text ──────────────────────────────────────────────────────────────

def _day(d: date) -> str:
    return f"{d.day} {d:%b}"


def _n(k: int, one: str, many: str) -> str:
    return f"{k:,} {one if k == 1 else many}"


def _alert_line(dom: str, client: str, alert: str) -> str:
    """'• *d.com* (Melior): reply rate 0.1% vs 1.2% baseline'; no empty '()'."""
    return f"• *{dom}*" + (f" ({client})" if client else "") + f": {alert}"


_REFERENCE = {"campaigns": "the other domains in its campaigns", "client": "the client's other domains",
              "mixed": "its campaigns' other domains and the client's"}


def verdict_text(v: dict, days: int = rs.WINDOW_DAYS) -> str:
    """'1 reply on 304 sends in 14 days, 7.5 expected at the reply rate of the other domains in its
    campaigns (0.13x)'. Names the reference actually used; no '(0.0x)' for zero replies."""
    ratio = f" ({v['ratio']}x)" if v["observed"] and v.get("ratio") is not None else ""
    return (f"{_n(v['observed'], 'reply', 'replies')} on {_n(v['judged_sent'], 'send', 'sends')} in {days} days, "
            f"{v['expected']} expected at the reply rate of {_REFERENCE.get(v.get('reference'), 'the other domains')}"
            f"{ratio}")


def _alert_parts(alert_lines: list[str], today: date | None = None, note: str = "") -> tuple[list[str], list[str]]:
    """(headline_lines, thread_parts). The headline is at most 4 lines: the title, the note if any, then the
    alerts if they fit; a longer list goes in the thread (all of it, split by post_with_thread)."""
    n = len(alert_lines)
    when = f", {_day(today)}" if today else ""
    head = [f"⚠️ Reply-rate alert{when}: {n} drop{'s' if n != 1 else ''}, "
            f"{'a sending domain' if n == 1 else 'sending domains'} with far fewer replies than expected "
            f"over {rs.WINDOW_DAYS} days"]
    if note:
        head.append(note)
    if n <= 4 - len(head):
        return head + alert_lines, []
    return head + ["The list is in the thread."], ["\n".join(alert_lines)]


def unread_line(unread: dict[str, str]) -> str:
    return "Not checked today: " + "; ".join(f"{clients_of(a)} ({why})" for a, why in sorted(unread.items()))


HOW = (f"How this is judged: each sending domain's replies over the last {rs.WINDOW_DAYS} days are compared with "
       "the reply rate of the other domains in the same campaigns, so a weak list or weak copy does not count "
       "against the domain. Where there is not enough campaign data, the client's other domains are used. "
       "ALERT: this few replies would happen by chance less than 1 time in 90. WATCH: under "
       f"{int(rs.WATCH_RATIO * 100)}% of expected, and less than 1 time in 10 by chance. Domains with under "
       f"{rs.MIN_SENT} sends are not judged.\n\n"
       "Far fewer replies can mean the domain's mail is going to spam for some recipients. Check the domain in "
       "the latest Smart Delivery v2 test before pulling it.\n\n"
       "Report only: nothing was paused or changed.")


def daily_parts(verdicts: dict[str, dict], fresh: list[str], unread: dict[str, str],
                today: date) -> tuple[list[str], list[str]] | None:
    """Any day: new ALERTs and unread accounts. None when there is nothing to say."""
    note = unread_line(unread) if unread else ""
    if fresh:
        lines = [_alert_line(d, verdicts[d]["client"], verdict_text(verdicts[d])) for d in fresh]
        head, thread = _alert_parts(lines, today, note)
        return head, thread + [HOW]
    if unread:
        return [f"⚠️ Reply check, {_day(today)}: {_n(len(unread), 'Smartlead account', 'Smartlead accounts')} "
                "not read", note], []
    return None


_NOT_SENDING = (rs.NO_SENDS, rs.NOT_COUNTED)


def weekly_parts(verdicts: dict[str, dict], unread: dict[str, str],
                 today: date, fresh: list[str] = ()) -> tuple[list[str], list[str]]:
    """Mondays: every client, every domain listed in an ACTIVE campaign today. A domain counts as sending only
    when it sent in the window (review 7 Nov: idle domains keep their rows); the rest are listed apart, so
    alert + watch + in line + under 50 = sending domains, and the sends shown are those domains' sends.
    An ALERT that is new today is also in the headline when it fits, as it would be on any other day."""
    by_client: dict[str, list[str]] = {}
    for d, v in verdicts.items():
        by_client.setdefault(v["client"], []).append(d)
    count = {k: sum(v["verdict"] == k for v in verdicts.values()) for k in (rs.ALERT, rs.WATCH)}
    n = sum(v["verdict"] not in _NOT_SENDING for v in verdicts.values())
    title = (f"{'⚠️ ' if count[rs.ALERT] else ''}Reply check, {rs.WINDOW_DAYS} days to {_day(today)}: "
             f"{_n(n, 'sending domain', 'sending domains')}, {_n(count[rs.ALERT], 'alert', 'alerts')}, "
             f"{count[rs.WATCH]} to watch")
    head = [title] + ([unread_line(unread)] if unread else [])
    if fresh and len(head) + len(fresh) <= 4:
        head += [_alert_line(d, verdicts[d]["client"], f"new ALERT today, {verdict_text(verdicts[d])}") for d in fresh]
    elif fresh:
        head.append(f"New ALERT today: {_n(len(fresh), 'domain', 'domains')}, in the thread.")
    blocks = []
    for client in sorted(by_client):
        doms = sorted(by_client[client])
        k = {v: sum(verdicts[d]["verdict"] == v for d in doms) for v in rs.VERDICTS}
        sending = [d for d in doms if verdicts[d]["verdict"] not in _NOT_SENDING]
        sent = sum(verdicts[d]["sent"] for d in sending)
        replies = sum(verdicts[d]["replies"] for d in sending)
        rate = f" ({100 * replies / sent:.1f}%)" if sent else ""
        if sending:
            lines = [f"*{client or 'No client'}*: {_n(sent, 'send', 'sends')}, {_n(replies, 'reply', 'replies')}"
                     f"{rate} in {rs.WINDOW_DAYS} days from {_n(len(sending), 'sending domain', 'sending domains')}"]
        else:
            lines = [f"*{client or 'No client'}*: no sends counted in {rs.WINDOW_DAYS} days"]
        for verdict in (rs.ALERT, rs.WATCH):
            lines += [_alert_line(d, "", f"{verdict}, {verdict_text(verdicts[d])}")
                      for d in doms if verdicts[d]["verdict"] == verdict]
        rest = [f"{k[rs.OK]} in line with the other domains"] if k[rs.OK] else []
        rest += [f"{k[rs.TOO_FEW]} under {rs.MIN_SENT} sends (not judged)"] if k[rs.TOO_FEW] else []
        rest += ([f"{k[rs.NO_REFERENCE]} with too few sends on other domains to compare (not judged)"]
                 if k[rs.NO_REFERENCE] else [])
        if rest:
            lines.append("• " + ", ".join(rest))
        idle = [f"{k[rs.NO_SENDS]} with no sends in {rs.WINDOW_DAYS} days"] if k[rs.NO_SENDS] else []
        idle += [f"{k[rs.NOT_COUNTED]} new or moved with no sends counted yet"] if k[rs.NOT_COUNTED] else []
        if idle:
            lines.append("• Also listed in active campaigns: " + ", ".join(idle))
        blocks.append("\n".join(lines))
    if not blocks:
        blocks.append("No domain is listed in an ACTIVE campaign today.")
    return head, ["\n\n".join(blocks), HOW]


# ── the job ─────────────────────────────────────────────────────────────────

def _post(head: list[str], thread: list[str]) -> bool | None:
    """Post unless REPLY_MONITOR_POST=off. True posted, False failed, None not asked to post."""
    from smartlead.notify import infra_channel, post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    # On by default: replayed over 17 Oct-7 Nov the pooled 14-day rule made 5 posts in 22 days, one of them a real
    # alert two days before v2 caught the same domain (the old one-day rule: 91 alerts, 89 noise). =off silences it.
    if os.getenv("REPLY_MONITOR_POST", "").strip().lower() == "off":
        token = ""
    if not token:
        return None
    return bool(post_with_thread(token, infra_channel("REPLY_MONITOR_CHANNEL", "HEALTH_NOTIFY_CHANNEL"), head, thread))


def run(today: date, fleet: dict[str, dict], unread: dict[str, str], history: list[dict] | None,
        weekly: bool) -> tuple[dict[str, dict], list[tuple[list[str], list[str]]]]:
    """Pure: verdicts for today's domains, and the posts to make (headline lines, thread parts).
    history None = Mongo could not be read: nothing is judged, and that is said rather than a clean week."""
    if history is None:
        head = [f"⚠️ Reply check, {_day(today)}: nothing judged today",
                "No stored history to compare with (Mongo not reachable)."]
        return {}, [(head + ([unread_line(unread)] if unread else []), [])]
    stamp = today.isoformat()
    todays = [{**doc, "date": stamp} for doc in fleet.values()]
    window = rs.window_stats(history + todays, today)
    verdicts = rs.judge(window, fleet)
    fresh = rs.new_alerts(verdicts, prior_verdicts(history))
    posts = []
    if weekly:                                            # the weekly post names every ALERT, new or not
        posts.append(weekly_parts(verdicts, unread, today, fresh))
    else:
        daily = daily_parts(verdicts, fresh, unread, today)
        if daily:
            posts.append(daily)
    return verdicts, posts


def _save(col, today: date, fleet: dict[str, dict], verdicts: dict[str, dict]) -> None:
    if col is None or not fleet:
        return
    stamp = today.isoformat()
    ops = []
    for d, doc in fleet.items():
        v = verdicts.get(d, {})
        ops.append(UpdateOne({"domain": d, "date": stamp}, {"$set": {
            **doc, "date": stamp, "verdict": v.get("verdict"), "window_sent": v.get("sent"),
            "window_replies": v.get("replies"), "expected": v.get("expected"), "p": v.get("p")}}, upsert=True))
    try:
        col.bulk_write(ops, ordered=False)
    except PyMongoError as exc:
        print(f"  [ReplyMon] Mongo write failed: {exc}")


async def main() -> int:
    today = datetime.now(IST).date()
    fleet, unread = await collect()
    print(f"[ReplyMon] collected stats for {len(fleet)} domain(s)")
    col = _col()
    history = _history(col, today)
    verdicts, posts = run(today, fleet, unread, history, weekly=today.weekday() == 0)
    for d, v in verdicts.items():
        if v["verdict"] in (rs.ALERT, rs.WATCH):
            print(f"  [{v['verdict']}] {d} ({v['client']}): {verdict_text(v)}, p={v['p']}")
    counts = {k: sum(v["verdict"] == k for v in verdicts.values()) for k in rs.VERDICTS}
    print(f"[ReplyMon] {len(verdicts)} domain(s): " + ", ".join(f"{k} {n}" for k, n in counts.items()))
    if unread:
        print(f"[ReplyMon] {unread_line(unread)}")
    _save(col, today, fleet, verdicts)
    failed = False
    for head, thread in posts:
        print("[ReplyMon] post:\n" + "\n".join(head) + "\n---\n" + "\n---\n".join(thread))
        if _post(head, thread) is False:
            print("[ReplyMon] Slack post failed.")
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
