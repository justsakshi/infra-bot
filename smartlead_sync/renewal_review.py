#!/usr/bin/env python3
"""Renewal review: before each Zapmail / ScaledMail bill, KEEP / DROP / CHECK /
UNTESTED every inbox on it, from Smart Delivery v2 results (Mongo v2_verdicts),
DO NOT USE tags (Smartlead) and Smartlead. Classic tests and a Spamhaus DBL
listing (Mongo blacklist_checks) only add a note. READ-ONLY, apart from --flag-tracker.

    python renewal_review.py                  # bills in the next 3 days
    python renewal_review.py --days 14 --json
    python renewal_review.py --post           # to RENEWAL_REVIEW_CHANNEL (else ZAPMAIL_NOTIFY_CHANNEL, else infra)

Rules: smartlead/renewal_review.py. Retiring is a separate, approver-only
button (Zapmail) — this script never changes a provider or Smartlead. With
--post it also posts when a source could not be read or the run crashed, even
when no bill is due.

Daily digest (16 Nov 2026, smartlead/digest.py): with the digest on, --post hands the digest one ask per bill
with inboxes to drop or check (a keep-only bill says nothing), a "broken" ask for unread sources or a crash,
and the full review for the digest's thread. If the digest can't be written, the old post goes out.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

import httpx

from smartlead.api import raise_for_status
from smartlead import digest as dg
from smartlead import kill_rule as kr
from smartlead import renewal_review as rr
from smartlead.accounts import discover_accounts
from smartlead.config import BLACKLIST_COLLECTION, SMARTDELIVERY_BASE_URL
from smartlead.report_sheet import client_label

SL = "https://server.smartlead.ai/api/v1"
UA = {"User-Agent": "Mozilla/5.0"}
PAST = {"DARLEAN", "BELARDI WONG", "MYTHIC"}     # past clients' Smartlead accounts
TESTS_PER_ACCOUNT = 10            # newest one-off classic tests read per account
WEEKLY_TESTS_PER_ACCOUNT = 20     # scheduled (weekly) classic tests read per account, each at its latest run
V2_COLLECTION = "v2_verdicts"     # written by v2_weekly.py on Manveen's PC (shared contract, 7 Nov 2026)
V2_LOOKBACK_DAYS = 120            # enough history for "two weekly fails with no pass since"
SOURCE = "renewal-review"         # its name in the daily digest


def _get(url, key, **params):
    import time
    for attempt in range(5):           # Smartlead rate-limits hard: back off on 429
        r = httpx.get(url, params={"api_key": key, **params}, headers=UA, timeout=120)
        if r.status_code != 429:
            break
        time.sleep(10 * (attempt + 1))
    raise_for_status(r)  # no key in the error text
    return r.json()


_SEED_MX: dict[str, str] = {}
# SmartDelivery's seed inboxes are the same every week: their providers are
# kept between runs (2026-10-14: ~100 lookups took 16 s on every run).
SEED_MX_FILE = Path(os.getenv("SEED_MX_FILE") or Path(__file__).resolve().parent / ".cache" / "seed_mx.json")
REPORT_WORKERS = 5      # SmartDelivery reports read side by side (one at a time took 65 s for 20)


def _load_seed_mx() -> None:
    if not _SEED_MX and SEED_MX_FILE.exists():
        try:
            _SEED_MX.update(json.loads(SEED_MX_FILE.read_text(encoding="utf-8")))
        except Exception:  # noqa: BLE001 - a bad cache only costs lookups
            pass


def _save_seed_mx() -> None:
    try:
        SEED_MX_FILE.parent.mkdir(parents=True, exist_ok=True)
        SEED_MX_FILE.write_text(json.dumps(_SEED_MX, sort_keys=True), encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass


def _resolve_seed_domains(domains: set[str]) -> None:
    """Look up every seed domain not known yet, side by side, then save."""
    from concurrent.futures import ThreadPoolExecutor
    _load_seed_mx()
    new = sorted(d for d in domains if d and d not in _SEED_MX)
    if not new:
        return
    with ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(seed_provider, new))
    _save_seed_mx()


def seed_provider(domain: str) -> str:
    """Google / Outlook / other for a seed's domain, from its MX (DoH, cached)."""
    if domain not in _SEED_MX:
        try:
            ans = httpx.get("https://dns.google/resolve", params={"name": domain, "type": "MX"},
                            timeout=10).json().get("Answer") or []
            mx = " ".join(a.get("data", "") for a in ans).lower()
            _SEED_MX[domain] = ("Outlook" if "protection.outlook.com" in mx
                                else "Google" if ("google" in mx or "googlemail" in mx) else "other")
        except Exception:  # noqa: BLE001
            _SEED_MX[domain] = "other"
    return _SEED_MX[domain]


def _tag_names(tags) -> list[str]:
    """Tag names from /email-accounts (dicts with tag_name / name; plain strings kept)."""
    out = []
    for t in tags or []:
        name = str(t.get("tag_name") or t.get("name") or "") if isinstance(t, dict) else str(t or "")
        if name.strip():
            out.append(name.strip())
    return out


def tests_to_read(listing: list[dict], limit: int = TESTS_PER_ACCOUNT,
                  weekly_limit: int = WEEKLY_TESTS_PER_ACCOUNT) -> list[dict]:
    """The classic tests worth reading, newest run first, each with ``created_at`` set to the day it last RAN.

    A weekly scheduled test keeps one id and stays ACTIVE: it is never COMPLETED, and its own created_at is
    the day the schedule was made. Reading only COMPLETED tests by created_at (until 10 Nov 2026) left every
    weekly result out, so fleet watch said "no test since 12 Oct" about domains tested the day before, and
    the kill warnings and the renewal note never saw a weekly fail. Every scheduled test that has run is
    read (newest ``weekly_limit``), plus the newest ``limit`` one-off tests. Pure."""
    from smartlead.placement_report import select_tests
    seen: set = set()
    rows = [t for t in listing if not (t.get("spam_test_id") in seen or seen.add(t.get("spam_test_id")))]
    made = {t.get("spam_test_id"): str(t.get("created_at") or "") for t in rows}

    def newest(ts: list[dict]) -> list[dict]:      # by run day, then by when the test was made
        return sorted(ts, key=lambda t: (t["created_at"], made.get(t.get("spam_test_id"), "")), reverse=True)
    picked = select_tests(rows, "")
    weekly = newest([t for t in picked if t.get("run_no")])[:weekly_limit]
    once = newest([t for t in picked if not t.get("run_no")])[:limit]
    return newest(weekly + once)


def _older_scheduled(listing: list[dict], key: str, account: str) -> list[dict]:
    """Scheduled tests the placement report remembers (Mongo) that have left the newest-50 list.
    Best effort: without Mongo the newest 50 are still read."""
    try:
        import placement_report as pr
        from smartlead.placement_report import find_scheduled
        col = pr._collection(pr.SCHEDULED)
        if col is None:
            return []
        older, _lost = find_scheduled(listing, pr._remembered(col, account),
                                      lambda off: pr._list_tests(key, pr.PAGE, off), page_size=pr.PAGE)
        return older
    except Exception as exc:  # noqa: BLE001 - never lose the tests that were listed
        print(f"  [Facts] older scheduled tests not read for {account}: {str(exc)[:120]}")
        return []


def smartlead_facts(errors: list[str]) -> dict[str, dict]:
    """{email: {in_smartlead, connected, reputation, tags, pct, tested_on, campaigns}}.

    Errors name what failed: "Smartlead <acc>" for /email-accounts (inboxes missing), and the
    note-only "campaign lists <acc>" / "classic tests <acc>" (rr.NOTE_ONLY), which never stop a
    verdict. Every error lands in ``errors``, so kill_report, fleet_watch and volume_review see it."""
    from placement_report import attachments
    facts: dict[str, dict] = {}
    for acc in discover_accounts():
        if acc.name.upper() in PAST:
            continue
        key = acc.api_key
        try:
            off = 0
            while True:
                page = _get(f"{SL}/email-accounts/", key, offset=off, limit=100)
                for a in page:
                    w = a.get("warmup_details") or {}
                    facts[a["from_email"].lower()] = {
                        "in_smartlead": True, "account": acc.name,
                        "client": client_label(acc.name, a.get("client_id")),
                        "connected": bool(a.get("is_smtp_success") and a.get("is_imap_success")),
                        "reputation": w.get("warmup_reputation"), "tags": _tag_names(a.get("tags")),
                        "type": str(a.get("type") or ""),       # judged at its own provider (kill_rule)
                        "pct": None, "tested_on": None,
                        "tests": [], "campaigns": [], "sending": []}
                if len(page) < 100:
                    break
                off += 100
        except Exception as exc:  # noqa: BLE001 - one account must not hide the others
            errors.append(f"Smartlead {acc.name}: {str(exc)[:150]}")
            continue
        try:
            for email, at in attachments(key, sending_days=7).items():
                if email in facts:
                    facts[email]["campaigns"] = at["campaigns"]
                    facts[email]["sending"] = at["sending"]
        except Exception as exc:  # noqa: BLE001 - only the still-in-a-campaign notes are lost
            errors.append(f"{rr.CAMPAIGNS_SOURCE} {acc.name}: {str(exc)[:150]}")
        try:
            r = httpx.post(f"{SMARTDELIVERY_BASE_URL}/spam-test/report", params={"api_key": key},
                           json={"limit": 50}, headers=UA, timeout=60)
            listing = r.json()
            listing = listing if isinstance(listing, list) else []
            tests = tests_to_read(listing + _older_scheduled(listing, key, acc.name))
            # Every test, newest first: the kill rule needs history (two strikes)
            # and each seed's receiving provider (graded on the inbox's own one).
            # Reports are fetched side by side; order is kept (pool.map).
            from concurrent.futures import ThreadPoolExecutor
            url = SMARTDELIVERY_BASE_URL + "/spam-test/report/{}/sender-account-wise"
            with ThreadPoolExecutor(max_workers=REPORT_WORKERS) as pool:
                reports = list(pool.map(lambda t: _get(url.format(t["spam_test_id"]), key), tests))
            _resolve_seed_domains({str(d.get("email", "")).split("@")[-1].lower()
                                   for rep in reports for e in rep for d in e.get("details") or []})
            for t, rep in zip(tests, reports):
                for e in rep:
                    em = e["email"].lower()
                    if em not in facts:
                        continue
                    seeds = spam = 0
                    by: dict[str, list[int]] = {}
                    for d in e.get("details") or []:
                        folder = str((d.get("reply") or {}).get("mail_folder", "")).strip().lower()
                        if not folder:
                            continue                       # unclassified: not evidence either way
                        is_spam = folder in ("spam", "junk")
                        seeds += 1
                        spam += is_spam
                        prov = seed_provider(str(d.get("email", "")).split("@")[-1].lower())
                        slot = by.setdefault(prov, [0, 0])
                        slot[0] += 1
                        slot[1] += is_spam
                    if not seeds:
                        continue
                    test = {"date": t["created_at"][:10], "seeds": seeds, "spam": spam, "by": by,
                            "test_id": t["spam_test_id"], "kind": facts[em].get("type") or ""}
                    facts[em].setdefault("tests", []).append(test)
                    if facts[em].get("tested_on") is None:      # newest first
                        # at its own receiving provider (kill_rule.own_counts); the whole test for SMTP / unknown
                        n, bad = kr.own_counts(test)
                        facts[em]["pct"] = round(100.0 * (n - bad) / n, 1) if n else None
                        facts[em]["tested_on"] = t["created_at"][:10]
        except Exception as exc:  # noqa: BLE001 - classic tests are an early warning only
            errors.append(f"{rr.CLASSIC_SOURCE} {acc.name}: {str(exc)[:150]}")
    return facts


def zapmail_bills(days: int, errors: list[str]) -> list[dict]:
    from smartlead.zapmail_fleet import subscriptions_billing
    errs: list[str] = []
    rows = asyncio.run(subscriptions_billing(days=days, errors=errs))
    errors += [f"Zapmail {e}" for e in errs]          # the source's name first: rr.read_warning reads it
    horizon = (date.today() + timedelta(days=days)).isoformat()
    out = []
    for r in rows:
        if not r["bills_on"] or r["bills_on"] > horizon:
            continue
        label = f"{r['mailboxes'] or '?'} {'Outlook' if r['provider'] == 'MICROSOFT' else 'Google'} {r['kind']}"
        bill = {"provider": "Zapmail", "label": label, "bills_on": r["bills_on"], "price": r["price"],
                "mailboxes": r["mailboxes"], "inboxes": r.get("inboxes") or [], "clients": r.get("clients") or [],
                "subscription_id": r["subscription_id"], "account": r["account"]}
        notes = []
        if r.get("payment_failure"):
            notes.append(f"payment failed: {r['payment_failure']}")
        if r["kind"] == "pre-warmed" and r.get("inboxes_by") == "count":
            notes.append("pre-warmed slots: Zapmail does not list the inboxes on this plan; these are the "
                         f"{len(r.get('inboxes') or [])} active ones it holds on no other subscription")
        elif r["kind"] == "pre-warmed":
            notes.append("pre-warmed slots: Zapmail does not list which inboxes sit on this plan; review them by domain")
        if notes:
            bill["note"] = " · ".join(notes)
        out.append(bill)
    return out


def scaledmail_bills(days: int, errors: list[str], with_inboxes: bool = True) -> list[dict]:
    """ScaledMail orders billing within ``days``, with their domains. The inbox
    lists cost one call per domain (ScaledMail rate-limits at 5/s); callers
    that only need domain -> bill (kill warnings) pass with_inboxes=False."""
    try:
        from smartlead.scaledmail import ScaledMailClient, configured
        if not configured():
            return []
        from smartlead.scaledmail_fleet import snapshot
        horizon = (date.today() + timedelta(days=days)).isoformat()
        with ScaledMailClient() as sm:
            snap = snapshot(sm)
            due = [o for o in snap["orders"] if o["billing_day"] and o["billing_day"] <= horizon]
            out = []
            for o in due:
                inboxes, setting_up = [], []
                doms = sorted(str(d.get("domain") or "").lower() for d in snap["domains"]
                              if d["order_id"] == o["id"] and d.get("domain"))
                for d in snap["domains"]:
                    if with_inboxes and d["order_id"] == o["id"] and d.get("domain_id"):
                        for m in sm.mailboxes(d["domain_id"]):
                            email = m.get("email", "").lower()
                            inboxes.append(email)
                            if str(m.get("status") or "").lower() not in ("active", ""):
                                setting_up.append(email)      # "In Progress": not uploaded to Smartlead yet
                out.append({"provider": "ScaledMail", "label": o["description"].replace("*", "×"),
                            "bills_on": o["billing_day"], "price": o["amount"], "inboxes": inboxes,
                            "setting_up": setting_up, "clients": o["clients"], "order_id": o["id"],
                            "domains": doms})
            return out
    except Exception as exc:  # noqa: BLE001
        errors.append(f"ScaledMail: {str(exc)[:150]}")
        return []


def _collection(name: str):
    """A collection in the health-history db, where v2_weekly.py and blacklist_monitor.py write."""
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    return MongoClient(uri, serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB][name]


def v2_results(domains: set[str], emails: set[str], today: date, errors: list[str]) -> dict[str, list[dict]] | None:
    """{email: [v2_verdicts rows]} for these inboxes (by domain or address), newest
    V2_LOOKBACK_DAYS. None, with an error, when the collection cannot be read: the
    review then keeps nothing."""
    if not domains and not emails:
        return {}
    try:
        since = (today - timedelta(days=V2_LOOKBACK_DAYS)).isoformat()
        query = {"night": {"$gte": since},
                 "$or": [{"domain": {"$in": sorted(domains)}}, {"email": {"$in": sorted(emails)}}]}
        fields = {"_id": 0, "email": 1, "night": 1, "verdict": 1, "weekly": 1, "pulled_on": 1}
        out: dict[str, list[dict]] = {}
        for d in _collection(V2_COLLECTION).find(query, fields):
            email = str((d or {}).get("email") or "").strip().lower()
            if "@" in email:
                out.setdefault(email, []).append(d)
        return out
    except Exception as exc:  # noqa: BLE001 - say so; never KEEP on a half read
        errors.append(f"v2 results (Mongo {V2_COLLECTION}): {str(exc)[:150]}")
        return None


def dbl_listings(domains: set[str], errors: list[str]) -> dict[str, dict] | None:
    """{domain: rr.dbl_status(its blacklist_checks rows)}: the newest check that could read
    Spamhaus DBL, and whether the newest check could not (lookup_errors / inconclusive_zones).
    None, with an error, when the collection cannot be read."""
    if not domains:
        return {}
    try:
        rows: dict[str, list[dict]] = {}
        fields = {"_id": 0, "domain": 1, "date": 1, "listed_on": 1, "lookup_errors": 1, "inconclusive_zones": 1}
        for d in _collection(BLACKLIST_COLLECTION).find({"domain": {"$in": sorted(domains)}}, fields):
            dom = str((d or {}).get("domain") or "").lower()
            if dom:
                rows.setdefault(dom, []).append(d)
        return {dom: rr.dbl_status(rs) for dom, rs in rows.items()}
    except Exception as exc:  # noqa: BLE001
        errors.append(f"blocklist checks (Mongo {BLACKLIST_COLLECTION}): {str(exc)[:150]}")
        return None


def second_opinions(bills: list[dict], today: date, errors: list[str]) -> dict[str, dict]:
    """{email: opinion} from Zapmail's own placement tests of the last two weeks, read only when a Zapmail bill
    is under review. A read failure is a ``second test`` error: the rows are then judged on v2 alone."""
    if not any(b.get("provider") == "Zapmail" for b in bills):
        return {}
    from smartlead import zapmail_placement as zp
    errs: list[str] = []
    try:
        rows = asyncio.run(zp.recent_rows(today.isoformat(), errors=errs))
    except Exception as exc:  # noqa: BLE001
        rows, errs = [], [type(exc).__name__]
    errors += [f"{rr.SECOND_SOURCE} (Zapmail placement): {e}" for e in errs]
    return zp.own_provider_opinion(rows)


def reply_opinions(domains: set[str], today: date, errors: list[str]) -> dict[str, dict]:
    """{"@domain": opinion} from the daily reply check's stored days (reply_monitor.py, Mongo). Every vendor: it is
    the only second opinion for inboxes with no second placement test. A read failure is a ``reply check`` error:
    those rows are then judged on v2 alone."""
    if not domains:
        return {}
    from smartlead import reply_stats as rs
    from smartlead.config import REPLY_STATS_COLLECTION
    try:
        since = (today - timedelta(days=rs.OPINION_FRESH_DAYS)).isoformat()
        fields = {"_id": 0, "domain": 1, "date": 1, "verdict": 1, "window_sent": 1, "window_replies": 1, "expected": 1}
        docs = list(_collection(REPLY_STATS_COLLECTION).find(
            {"date": {"$gte": since}, "domain": {"$in": sorted(domains)}}, fields))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{rr.REPLY_SOURCE} (Mongo {REPLY_STATS_COLLECTION}): {type(exc).__name__}")
        return {}
    return rs.second_opinions(docs, today)


def _targets(bills: list[dict], facts: dict[str, dict]) -> tuple[set[str], set[str]]:
    """The bills' domains, and their inboxes plus Smartlead's inboxes on those domains
    (a renamed domain is judged on what Smartlead holds)."""
    listed = {str(e).lower() for b in bills for e in b.get("inboxes") or [] if "@" in str(e)}
    domains = {e.split("@")[-1] for e in listed} | {str(d).lower() for b in bills for d in b.get("domains") or [] if d}
    return domains, listed | {e for e in facts if e.split("@")[-1] in domains}


def run(days: int) -> dict:
    errors: list[str] = []
    bills = zapmail_bills(days, errors) + scaledmail_bills(days, errors)
    n_before = len(errors)
    facts = smartlead_facts(errors) if bills else {}
    # every account's /email-accounts was read (classic tests and campaign lists only add notes)
    complete = not any(e.startswith("Smartlead ") for e in errors[n_before:])
    today = date.today()
    v2, dbl, second = {}, {}, {}
    if bills:
        domains, emails = _targets(bills, facts)
        v2 = v2_results(domains, emails, today, errors)
        dbl = dbl_listings(domains, errors)
        # With v2 unread nothing is kept or dropped on it, so the reply check has nothing to dispute: not read.
        replies = reply_opinions(domains, today, errors) if v2 is not None else {}
        second = {**replies, **second_opinions(bills, today, errors)}
    reviews = sorted((rr.review_bill(b, facts, today, complete, v2=v2, dbl=dbl, second=second) for b in bills),
                     key=lambda r: r["bills_on"])
    from smartlead.zapmail_clients import infer_client
    for rv in reviews:          # whose inbox it is: retiring is per client
        for row in rv["rows"]:
            row["client"] = infer_client(row["email"].split("@")[1], rv.get("account"))
    headline, thread = rr.format_review(reviews, days)
    if errors:
        # A half-read review keeps nothing it can't prove: say so up front, details in the thread.
        warning = rr.read_warning(errors, days, bool(reviews))
        if reviews:
            if headline[0].startswith("✅"):           # never a green title over a half read
                headline[0] = "⚠️" + headline[0][1:]
            headline.append(warning)
            thread.insert(len(thread) - 1, ":warning: could not read: " + "; ".join(errors))
        else:
            headline, thread = [warning], [":warning: could not read: " + "; ".join(errors)]
    return {"days": days, "reviews": reviews, "errors": errors, "headline": headline, "thread": thread,
            "text": _text(headline, thread)}


def _text(headline: list[str], thread: list[str]) -> str:
    return "\n".join(headline) + ("\n\n" + "\n\n".join(thread) if thread else "")


def flag_tracker(res: dict) -> dict:
    """Write renewalFlag on tracker inbox rows. Refuses when a source that can change a verdict
    could not be read (a half-read review must never mark good inboxes DROP); classic tests and
    campaign lists only add notes, so they don't stop it."""
    blocking = rr.decision_errors(res.get("errors") or [])
    if blocking:
        return {"written": 0, "skipped": "not written: " + "; ".join(blocking)[:200]}
    from datetime import datetime, timezone
    from smartlead.zapmail_asset_sync import _db
    db = _db()
    if db is None:
        return {"written": 0, "skipped": "Mongo unavailable"}
    from pymongo import UpdateOne
    now = datetime.now(timezone.utc)
    ops = []
    for rv in res["reviews"]:
        for row in rv["rows"]:
            # Same two flags as before (renewal_labels.js reads them): untested is "check before renewing".
            flag = {"RETIRE": "DROP", "CHECK": "CHECK", rr.UNTESTED: "CHECK"}.get(row["verdict"])
            if flag:
                ops.append(UpdateOne({"name": row["email"]}, {"$set": {
                    "renewalFlag": flag, "renewalFlagReason": row["why"][:200], "renewalFlagAt": now}}))
            else:
                ops.append(UpdateOne({"name": row["email"]}, {"$unset": {
                    "renewalFlag": "", "renewalFlagReason": "", "renewalFlagAt": ""}}))
    if not ops:
        return {"written": 0}
    r = db["assets"].bulk_write(ops, ordered=False)
    return {"written": int(r.modified_count)}


def main() -> int:
    ap = argparse.ArgumentParser(description="Review inboxes before their bill (read-only)")
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--flag-tracker", action="store_true",
                    help="mark each reviewed inbox in the /infra tracker: DROP / CHECK / (clear) so the "
                         "Daily Renewal Check shows it")
    args = ap.parse_args()
    try:
        res = run(args.days)
    except Exception as exc:  # noqa: BLE001
        err = f"{type(exc).__name__}: {str(exc)[:300]}"
        if args.post:      # a crash is posted too: a silent day looks like "nothing to renew"
            head = ["🚨 Renewal review crashed, so no bill was checked today"]
            thread = [f"Error: {err}", "The tracker's renewal flags were not changed."]
            crash = dg.Ask(f"The renewal review crashed ({type(exc).__name__}), so no bill was checked today.",
                           "Check the error in the Render log (search [renewal-review]); the tracker's renewal flags "
                           "were not changed.", key=f"{SOURCE}:crash", kind="broken", detail=[err])
            if not (dg.on() and dg.report(SOURCE, [crash], [], [_text(head, thread)])):
                _post(head, thread)
        print(json.dumps({"error": err}) if args.json else f"ERROR: {err}")
        return 0 if args.json else 1
    if args.flag_tracker:
        try:
            res["flagged"] = flag_tracker(res)
        except Exception as exc:  # noqa: BLE001
            res["flagged"] = {"written": 0, "skipped": f"tracker write failed: {type(exc).__name__}: {str(exc)[:150]}"}
        if res["flagged"].get("skipped") and res["reviews"]:
            why = ("some sources could not be read" if rr.decision_errors(res["errors"])
                   else res["flagged"]["skipped"])
            res["thread"].insert(len(res["thread"]) - 1, f"_Tracker renewal flags were not updated this run ({why})._")
            res["text"] = _text(res["headline"], res["thread"])
    if args.post:
        news = bool(res["reviews"] or res["errors"])
        detail = [_text(res["headline"], res["thread"])] if news else []
        if dg.on() and dg.report(SOURCE, digest_asks(res), [], detail):     # every run: a fixed bill clears
            res["digest"] = True
        elif news:     # read errors post even when no bill is due
            res["posted"] = _post(res["headline"], res["thread"])
    print(json.dumps(res, default=str) if args.json else res["text"])
    return 0


def _bill_step(r: dict, drop: int, check: int, today: date | None = None) -> str:
    """Where to act on one bill, before its date (or now, when the date has passed and it is not renewed)."""
    day = rr._day(r["bills_on"])
    zap = r["provider"] == "Zapmail"
    where = "in Zapmail (/domains, Before the bill)" if zap else "on ScaledMail's billing page (single domains)"
    bits = []
    if drop:
        bits.append(f"retire the {drop} {rr._ib(drop)} listed as drop {where}")
    if check:
        bits.append(f"decide keep or cancel for the {check} {rr._ib(check)} listed as check or untested"
                    + ("" if drop else f", and cancel {where}"))
    today = today or datetime.now(timezone(timedelta(hours=5, minutes=30))).date()   # IST
    when = (f"The {day} bill date has passed and it is not renewed yet, so today" if str(r["bills_on"]) < today.isoformat()
            else f"Before {day}")
    return f"{when}: " + "; ".join(bits) + "."


def bill_lines(r: dict) -> list[str]:
    """The inboxes behind one bill's ask, by what to do: drop (with why and any live campaign), check (with why),
    untested, paid for but not in Smartlead. Pure."""
    rows = r.get("rows") or []
    k_rows = [x for x in rows if x["verdict"] == "RETIRE"]
    u_rows = [x for x in rows if x.get("kind") == "unused"]
    c_rows = [x for x in rows if x["verdict"] == "CHECK" and x.get("kind") != "unused"]
    t_rows = [x for x in rows if x["verdict"] == rr.UNTESTED]
    out = []
    if k_rows:
        out.append(f"Drop: {rr._doms_why(k_rows)}")
        out += [f"Drop, still sending: {x['email']} is in {', '.join(x['campaigns'][:2])}" for x in k_rows
                if x.get("campaigns")]
    by_why: dict[str, list[dict]] = {}
    for x in c_rows:
        by_why.setdefault(x["why"], []).append(x)
    for why, xs in by_why.items():
        out.append(f"Check: {xs[0]['email'] if len(xs) == 1 else rr._doms(xs)}: {why}")
    if u_rows:
        out.append(f"Check, paid for but not in Smartlead: {rr._doms(u_rows)}")
    if t_rows:
        out.append(f"Untested (no v2 result in the last {rr.V2_FRESH_DAYS} days): {rr._doms(t_rows)}")
    return out


def digest_asks(res: dict, today: date | None = None) -> list:
    """One ask per bill with inboxes to drop or check (keep-only bills say nothing), and a "broken" ask when a
    source could not be read. Each bill's ask names the inboxes and domains and why (17 Nov 2026). Pure."""
    asks = []
    for r in res.get("reviews") or []:
        c = r.get("counts") or {}
        drop, untested = c.get("RETIRE", 0), c.get(rr.UNTESTED, 0)
        check = c.get("CHECK", 0) + untested
        if not (drop or check):
            continue
        price = rr.price_of(r)
        who = f" ({', '.join(r['clients'])})" if r.get("clients") else ""
        bill = r.get("subscription_id") or r.get("order_id") or r.get("label")
        n = len(r.get("rows") or [])
        text = (f"{r['provider']} bill on {rr._day(r['bills_on'])}, {r['label']}, "
                f"{rr._money(price) if price is not None else 'price not known'}{who}: of {n} {rr._ib(n)}, "
                f"{drop} to drop, {check} to check" + (f" ({untested} untested)" if untested else "")
                + f", {c.get('KEEP', 0)} to keep.")
        asks.append(dg.Ask(text, _bill_step(r, drop, check, today), detail=bill_lines(r),
                           key=f"{SOURCE}:{r['provider']}:{bill}:{r['bills_on']}"))
    if res.get("errors"):
        warn = rr.read_warning(res["errors"], res.get("days", 3), bool(res.get("reviews")))
        warn = warn.replace("⚠️ ", "").replace(":warning: ", "").replace(" (details in the thread)", "")
        if not warn.startswith("Renewal review"):
            warn = "Renewal review: " + warn[0].lower() + warn[1:]
        asks.append(dg.Ask(warn + ".", "It runs again tomorrow; the errors are in the Render log (search "
                                       "[renewal-review]).", detail=list(res["errors"])[:5],
                           key=f"{SOURCE}:read", kind="broken"))
    return asks


def _post(headline: list[str], thread: list[str]) -> bool:
    """Headline in the infra channel (or RENEWAL_REVIEW_CHANNEL), details in its thread."""
    from smartlead.notify import infra_channel, post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    channel = infra_channel("RENEWAL_REVIEW_CHANNEL", "ZAPMAIL_NOTIFY_CHANNEL")
    return bool(token and channel and post_with_thread(token, channel, headline, thread))


if __name__ == "__main__":
    raise SystemExit(main())
