"""Weekly placement report: who lands in spam, from which provider to which,
and whether those inboxes are attached to a campaign that is really sending.

Pure functions only, so the numbers can be tested without Smartlead.

Receiving provider comes from the SEED's MX record. SmartDelivery only gives
the Google/Microsoft split for a whole test, never per sender; resolving each
seed domain's MX reproduces that split exactly (verified 2026-10-06 on test
547669: 126 inbox / 54 spam at Google, 381 / 47 at Microsoft).
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta

import httpx

SPAM_FOLDERS = {"spam", "junk"}
GOOD_AT = 80.0     # a mailbox at or above this inbox rate lands in the inbox; below it, spam
BAD_BELOW = 50.0   # below this the mailbox is "bad" (worst of the spam group)

_PROVIDER = {"GMAIL": "Google", "OUTLOOK": "Outlook", "SMTP": "SMTP"}


def mx_provider(domain: str) -> str:
    """Google / Outlook / other, from the domain's MX records (DNS over HTTPS: the
    server image has no nslookup, which silently made every seed 'other')."""
    try:
        r = httpx.get("https://dns.google/resolve", params={"name": domain, "type": "MX"}, timeout=20)
        out = " ".join(a.get("data", "") for a in r.json().get("Answer") or []).lower()
    except Exception:
        return "other"
    if "protection.outlook.com" in out:
        return "Outlook"
    if "google" in out or "googlemail" in out:
        return "Google"
    return "other"


def select_tests(listing: list[dict], since: str) -> list[dict]:
    """Tests from the SmartDelivery report list whose latest run started on or after
    `since` (YYYY-MM-DD, UTC), each with `run_no` and `created_at` set to that run's day.

    A manual test runs once: it counts when COMPLETED. A scheduled ("auto") test keeps
    one id and stays ACTIVE until its end date, re-running every `every_days` from
    `schedule_start_time`; `current_test_run_no` is 0 until its first run.
    """
    out = []
    for t in listing:
        if t.get("test_type") == "auto":
            run = int(t.get("current_test_run_no") or 0)
            if run < 1 or not t.get("schedule_start_time"):
                continue
            start = datetime.fromisoformat(str(t["schedule_start_time"]).replace("Z", "+00:00"))
            day = (start + timedelta(days=(run - 1) * int(t.get("every_days") or 0))).date().isoformat()
        elif t.get("status") == "COMPLETED":
            run, day = 0, str(t.get("created_at", ""))[:10]
        else:
            continue
        if day >= since:
            out.append({**t, "created_at": day, "run_no": run})
    return out


def sender_provider(account_type: str) -> str:
    return _PROVIDER.get(str(account_type or "").upper(), "other")


def status_for(pct: float | None) -> str:
    if pct is None:
        return "untested"
    if pct >= GOOD_AT:
        return "good"
    if pct >= BAD_BELOW:
        return "watch"
    return "bad"


def active_campaign(analytics: dict) -> bool:
    """A campaign is 'active' when it is running and still has leads to send:
    status ACTIVE and Leads Not Started + Leads In Progress > 0."""
    if str(analytics.get("status", "")).upper() != "ACTIVE":
        return False
    st = analytics.get("campaign_lead_stats") or {}
    return int(st.get("notStarted") or 0) + int(st.get("inprogress") or 0) > 0


def domain_verdict(n_mailboxes: int, n_spam: int) -> str:
    """The team's rule (Anjali, 2026-10-06), per domain:

    - no mailbox in spam                      -> good
    - fewer than half the mailboxes in spam   -> avoid_new: still counts as inbox,
      but keep it out of NEW campaigns (its reputation drops within 2-3 days)
    - half or more in spam                    -> spam (close to 50% is spam)
    """
    if n_spam == 0:
        return "good"
    return "avoid_new" if n_spam * 2 < n_mailboxes else "spam"


def build_report(senders: dict[str, list[dict]], type_by_email: dict[str, str],
                 seed_provider, attached: dict[str, list[str]] | None = None) -> dict:
    """senders: {email: [{"seed": address, "folder": str}, ...]}.

    seed_provider(domain) -> 'Google' | 'Outlook' | 'other'.
    attached: {email: [active campaign names]}.
    """
    attached = attached or {}
    inboxes: dict[str, dict] = {}
    pair = defaultdict(lambda: {"seeds": 0, "spam": 0, "inboxes_hit": set()})
    for email, rows in senders.items():
        email = email.lower()
        sp = sender_provider(type_by_email.get(email))
        got = spam = 0
        recv = defaultdict(lambda: [0, 0])      # receiver provider -> [seeds, spam]
        for r in rows:
            folder = str(r.get("folder", "")).strip().lower()
            if not folder:
                continue                          # not classified yet: unmeasured, not failing
            is_spam = folder in SPAM_FOLDERS
            rp = seed_provider(r["seed"].split("@")[1].lower())
            got += 1
            spam += is_spam
            recv[rp][0] += 1
            recv[rp][1] += is_spam
            p = pair[(sp, rp)]
            p["seeds"] += 1
            p["spam"] += is_spam
            if is_spam:
                p["inboxes_hit"].add(email)
        pct = round(100.0 * (got - spam) / got, 1) if got else None
        inboxes[email] = {"email": email, "domain": email.split("@")[1], "sender": sp,
                          "seeds": got, "spam": spam, "pct": pct, "status": status_for(pct),
                          "campaigns": attached.get(email, []),
                          "by_receiver": {k: {"seeds": v[0], "spam": v[1]} for k, v in recv.items()}}

    domains: dict[str, dict] = {}
    for i in inboxes.values():
        d = domains.setdefault(i["domain"], {"domain": i["domain"], "sender": i["sender"],
                                              "seeds": 0, "spam": 0, "inboxes": 0,
                                              "mailboxes_spam": 0, "emails": []})
        d["seeds"] += i["seeds"]
        d["spam"] += i["spam"]
        d["inboxes"] += 1
        d["emails"].append(i["email"])
        d["mailboxes_spam"] += i["status"] in ("watch", "bad")
    for d in domains.values():
        d["pct"] = round(100.0 * (d["seeds"] - d["spam"]) / d["seeds"], 1) if d["seeds"] else None
        d["verdict"] = domain_verdict(d["inboxes"], d["mailboxes_spam"])
        d["status"] = {"good": "good", "avoid_new": "watch", "spam": "bad"}[d["verdict"]]
        # The grid has one cell per receiving provider (G suite / Outlook).
        d["cells"] = {}
        for rp in ("Google", "Outlook"):
            per = [inboxes[e]["by_receiver"][rp] for e in d["emails"] if rp in inboxes[e]["by_receiver"]]
            if not per:
                continue
            bad = sum(1 for v in per if v["seeds"] and 100.0 * (v["seeds"] - v["spam"]) / v["seeds"] < GOOD_AT)
            d["cells"][rp] = "spam" if bad * 2 >= len(per) else "Inbox"

    return {"inboxes": inboxes, "domains": domains,
            "pairs": {k: {"seeds": v["seeds"], "spam": v["spam"],
                          "inboxes_hit": len(v["inboxes_hit"])} for k, v in pair.items()}}


def counts(items: dict) -> dict:
    out = {"good": 0, "watch": 0, "bad": 0, "untested": 0}
    for v in items.values():
        out[v["status"]] += 1
    return out


PAIR_ORDER = [("Google", "Google"), ("Google", "Outlook"),
              ("Outlook", "Outlook"), ("Outlook", "Google")]


ROUTE_LINE_OVER = 5.0   # a route over this spam % gets its own line under the headline


def _day(tested_on: str) -> str:
    if not tested_on:
        return ""
    try:
        d = datetime.fromisoformat(tested_on)
        return f"{d.day} {d:%b}"
    except ValueError:
        return tested_on


LABEL = "Classic seed test (early warning, not proof)"
# Standing footer (audit PLACEMENT-5, 7 Nov 2026: classic posts said every mailbox landed in
# the inbox in the same week the v2 test found many of them in Google spam).
EARLY_WARNING = ("Early warning only: Smartlead's own Google seeds have missed Google spam that the weekly "
                 "v2 test caught, so a clean result here does not clear a mailbox.")
GONE_SHOWN = 25


def _receiver(rp: str) -> str:
    """Route label for a receiving provider: the Google seeds are Smartlead's own."""
    return "Google (Smartlead's own seeds)" if rp == "Google" else rp


def format_slack(client: str, report: dict, test_names: list[str],
                 tested_on: str = "", *, blocklist: dict | None = None,
                 gone: list[str] | tuple = ()) -> tuple[list[str], list[str]]:
    """(headline_lines, thread_parts) for notify.post_with_thread (spec 2026-10-13).

    Channel: one line with the result, labelled an early warning (only bad news gets
    an icon), a blocklist line when one of the client's domains is on DBL/URIBL or the
    checks can't be read, and one line naming the routes over 5% spam.
    Thread: 1) each flagged mailbox with what to do, 2) the domains to keep out of new
    campaigns and the domains in spam, 3) the listed domains, 4) test senders that are
    not in Smartlead now, 5) where the test emails landed, 6) the early-warning note.

    ``blocklist`` = classic_blocklist.summarise(...) for this client (None: not read, no
    line). ``gone`` = test senders not in the Smartlead account now (not in ``report``)."""
    from smartlead import classic_blocklist as cb
    ib, dm = report["inboxes"], report["domains"]
    day = _day(tested_on)
    lead = f"{LABEL}, {client}, {day}" if day else f"{LABEL}, {client}"
    flagged = sorted((i for i in ib.values() if i["status"] in ("watch", "bad")),
                     key=lambda i: (not i["campaigns"], i["pct"] if i["pct"] is not None else 101))
    measured = len(ib) - counts(ib)["untested"]
    spam_domains = sorted(d["domain"] for d in dm.values() if d["verdict"] == "spam")
    avoid = sorted(d["domain"] for d in dm.values() if d["verdict"] == "avoid_new")
    listed = bool(blocklist and blocklist.get("listed"))
    gone = sorted(gone or ())

    if not ib and gone:
        head = [f"\u26a0\ufe0f {lead}: none of the {len(gone)} test senders is in Smartlead now, nothing to grade"]
    elif not measured:
        head = [f"\u26a0\ufe0f {lead}: no mailbox was measured yet"]
    elif not flagged:
        # Never an all-clear: classic can flag a mailbox, not clear it; a listed domain is bad news.
        warn = "⚠️ " if listed else ""
        head = [f"{warn}{lead}: no mailbox flagged ({measured} tested)"]
    else:
        # Act today only when a flagged mailbox is still sending (same rule as kill warnings).
        live = sum(1 for i in flagged if i.get("campaigns"))
        icon = "\U0001f6a8" if live else "\u26a0\ufe0f"
        doms = f" ({len(spam_domains)} domain{'s' if len(spam_domains) != 1 else ''} in spam)" if spam_domains else ""
        head = [f"{icon} {lead}: {len(flagged)} of {measured} mailboxes flagged{doms}"]
        if live:
            head.append(f"{live} still in live campaigns")
    bl_line = cb.headline_line(blocklist)
    if bl_line:
        head.append(bl_line)

    routes, hot = [], []
    for sp, rp in PAIR_ORDER:
        p = report["pairs"].get((sp, rp))
        if p and p["seeds"]:
            pct = 100.0 * p["spam"] / p["seeds"]
            routes.append(f"\u2022 {sp} \u2192 {_receiver(rp)}: {p['spam']} of {p['seeds']} in spam ({pct:.0f}%)")
            if pct > ROUTE_LINE_OVER:
                hot.append((pct, f"{sp}\u2192{rp}", rp))
    if hot:
        hot.sort(key=lambda h: -h[0])
        line = " \u00b7 ".join(f"{name} {pct:.0f}%{' spam' if n == 0 else ''}"
                                for n, (pct, name, _) in enumerate(hot))
        if any(rp == "Google" for _, _, rp in hot):
            line += " (Google = Smartlead's own seeds)"
        head.append(line)

    parts = []
    if flagged:
        rows = [f"*Flagged mailboxes ({len(flagged)})*"]
        for i in flagged:
            todo = (f"remove from {', '.join(i['campaigns'])}" if i["campaigns"]
                    else "not in a live campaign, nothing to remove")
            rows.append(f"\u2022 {i['email']} \u00b7 {todo}")
        parts.append("\n".join(rows))
    if avoid or spam_domains:
        rows = ["*Domains*"]
        if avoid:
            rows.append(f"Keep out of new campaigns (some mailboxes in spam): {', '.join(avoid)}")
        if spam_domains:
            rows.append(f"In spam (half or more of their mailboxes): {', '.join(spam_domains)}")
        parts.append("\n".join(rows))
    bl_part, bl_note = cb.detail(blocklist)
    if bl_part:
        parts.append(bl_part)
    if gone:
        shown = gone[:GONE_SHOWN]
        rows = [f"*Not in Smartlead now ({len(gone)})*",
                "These test senders are not in the Smartlead account any more, so they are left out of the "
                "counts and the Inbox Status tab."]
        rows += [f"\u2022 {e}" for e in shown]
        if len(gone) > len(shown):
            rows.append(f"and {len(gone) - len(shown)} more")
        parts.append("\n".join(rows))
    if routes:
        parts.append("\n".join(["*Where the test emails landed* (sent from \u2192 received at)", *routes]))
    parts.append("_" + " ".join(x for x in (EARLY_WARNING, bl_note) if x) + "_")
    return head, parts


# Weekly scheduled classic tests, by Smartlead account -> client -> weekday (Mon=0) -> what
# should have run. Read from Smart Delivery on 7 Nov 2026: Melior #1-2 Mondays 06:00 IST (end
# 29 Nov 2026); Melior #3, PL #1-3 and BettrData #1-2 Tuesdays 06:30 IST (from 10 Nov 2026,
# end 31 Oct 2027). Change this when a test is created, moved or retired, or the 13:00 run
# warns every week. Days are the UTC run day the cron passes as --since.
EXPECTED_TESTS: dict[str, dict[str, dict[int, str]]] = {
    "PRECISE_LEADS": {"Precise Leads": {1: "DT PL #1, #2 and #3"},
                      "Melior": {0: "DT Melior #1 and #2", 1: "DT Melior #3"}},
    "BETTRDATA": {"Bettrdata": {1: "DT BettrData #1 and #2"}},
}


def missing_tests(account: str, day: str, covered: set[str]) -> list[tuple[str, str]]:
    """[(client, expected test campaigns)] for clients expected to have a scheduled test
    on ``day`` (YYYY-MM-DD) that have no reported scheduled test that day. Pure."""
    wd = datetime.fromisoformat(day[:10]).weekday()
    out = []
    for client, days in sorted(EXPECTED_TESTS.get(account.upper(), {}).items()):
        if wd in days and client not in covered:
            out.append((client, days[wd]))
    return out


def missing_test_text(client: str, expected: str, day: str) -> tuple[list[str], list[str]]:
    """The 13:00 final run's warning for a client whose weekly test has no result."""
    head = [f"\u26a0\ufe0f {LABEL}, {client}, {_day(day)}: no result today",
            f"Expected the scheduled test on {expected}."]
    thread = [f"Nothing was measured, so this says nothing about the {client} inboxes either way. "
              "Check the scheduled test in Smart Delivery: it may have ended, been stopped, lost its "
              "senders, or not run. The bot cannot create or edit scheduled tests."]
    return head, thread


def find_scheduled(listing: list[dict], known: dict[int, str], fetch_page,
                   page_size: int = 100, max_pages: int = 20) -> tuple[list[dict], list[int]]:
    """Listing rows for remembered scheduled tests that fell out of the newest-N window.

    ``known`` = {test_id: created_at} of ACTIVE scheduled tests seen before.
    ``fetch_page(offset)`` returns the report list from that offset (newest first). The
    by-id endpoint can't be used: its `test_run_no` is not the run counter (measured
    7 Nov 2026: 6 on a test that had run 4 times, 2 on one that had not run yet).
    Pages back until every known test is found, a page is short, or the page is older
    than the oldest one still missing. Returns (rows found, ids still missing)."""
    have = {t.get("spam_test_id") for t in listing}
    missing = {i: str(c or "") for i, c in known.items() if i not in have}
    found: list[dict] = []
    offset = len(listing)
    if listing and str(listing[-1].get("created_at") or "") < min(missing.values(), default=""):
        return found, sorted(missing)            # the window already reaches back past them: deleted
    for _ in range(max_pages):
        if not missing:
            break
        page = fetch_page(offset) or []
        for t in page:
            if t.get("spam_test_id") in missing:
                missing.pop(t["spam_test_id"])
                found.append(t)
        if len(page) < page_size or str(page[-1].get("created_at") or "") < min(missing.values(), default=""):
            break
        offset += len(page)
    return found, sorted(missing)


def inbox_rows(report: dict, client_by_email: dict[str, str] | None = None,
               tested_on: str = "") -> list[list]:
    """One row per inbox for the Campaign Desk tab. `Usable` is the column it reads:
    YES (use anywhere), EXISTING (domain has a mailbox in spam: keep out of new
    campaigns), NO (mailbox or domain in spam)."""
    client_by_email = client_by_email or {}
    rows = []
    for i in sorted(report["inboxes"].values(), key=lambda x: (x["domain"], x["email"])):
        d = report["domains"][i["domain"]]
        in_inbox = i["status"] == "good"
        if in_inbox and d["verdict"] == "good":
            usable = "YES"
        elif in_inbox and d["verdict"] == "avoid_new":
            usable = "EXISTING"
        else:
            usable = "NO"
        g = i["by_receiver"].get("Google")
        o = i["by_receiver"].get("Outlook")
        pct = lambda v: round(100.0 * (v["seeds"] - v["spam"]) / v["seeds"]) if v and v["seeds"] else ""
        rows.append([i["email"], i["domain"], client_by_email.get(i["email"], ""), i["sender"],
                     "" if i["pct"] is None else round(i["pct"]), pct(g), pct(o),
                     "Inbox" if in_inbox else "Spam", d["verdict"], usable, tested_on,
                     ", ".join(i["campaigns"])])
    return rows


INBOX_HEADER = ["Email", "Domain", "Client", "Sender type", "Inbox %", "Inbox % at Google",
                "Inbox % at Outlook", "Mailbox verdict", "Domain verdict", "Usable",
                "Tested on", "In active campaign"]
