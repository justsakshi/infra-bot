"""Per-sending-domain reply check: pure counting and judging, no I/O.

Why a pooled window (audit 7 Nov 2026): one day is 20-110 sends per domain, and at a 2.3% reply rate
0 replies in 28 sends happens about half the time. Replayed over 17 Oct-6 Nov, the old one-day rule raised
91 alerts, 89 of them noise, and missed the one domain a placement test later found in spam.

Rule (provider_watch style, critic fix 12):
  * Unit: one sending domain, its sends and replies over the last WINDOW_DAYS days.
  * Expected replies: the domain's sends in each of its campaigns x that campaign's reply rate on its OTHER
    domains, so a weak list or weak copy does not count against the domain. A campaign whose other domains
    sent under MIN_REFERENCE_SENT uses the client's rate on its other domains instead (never another
    client's: PL and Melior share a Smartlead account and are told apart by the campaign's client_id).
  * Judged only with >= MIN_SENT sends that have a reference.
  * ALERT: Poisson P(X <= replies) <= ALERT_P. WATCH: replies under WATCH_RATIO x expected and
    P(X <= replies) <= WATCH_P (so 0 replies on 1.2 expected is not a watch). Else OK.
  * A domain with no sends counted in the window is not a sending domain (review 7 Nov: idle domains still
    have rows in ACTIVE campaigns' mailbox-statistics). NO_SENDS: its totals never rose in the window, so it
    sent nothing. NOT_COUNTED: it or one of its campaigns was first seen inside the window with lifetime
    totals (or an old-format jump was dropped), so sends may have happened that the window could not place.
REPORT ONLY: nothing here pauses or changes anything.

Snapshots. GET /campaigns/{id}/mailbox-statistics returns campaign-lifetime totals per sender, so the job
stores one snapshot per sending domain per day with a per-campaign breakdown and diffs each campaign against
its own last snapshot (REPLIES-6: a campaign that pauses simply has no row that day, and when it resumes the
diff is against its pre-pause totals, so neither shows up as a fake day of sends). Snapshots stored before
the breakdown was added (Nov 2026) have domain totals only; for those the day-to-day change of the domain
total is used, and a fall or a jump of more than LEGACY_MAX_DAY sends a day (a campaign leaving or
rejoining ACTIVE) is dropped.

Field names of mailbox-statistics rows (verified live 2026-07-08): from_email, sent_count, reply_count,
open_count, click_count, bounce_count, sender_bounce_count, unsubscribed_count."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, timedelta

from smartlead.provider_watch import poisson_le

WINDOW_DAYS = 14
MIN_SENT = 50               # not provider_watch's 10 expected: a 300-send domain at 2.5% has only 7.5
MIN_REFERENCE_SENT = 200    # sends on the other domains before their rate is used as the reference
ALERT_P = 0.011
WATCH_RATIO = 0.7
WATCH_P = 0.1
LEGACY_MAX_DAY = 200        # domain-total snapshots: a bigger daily change is a campaign status change
LOOKBACK_DAYS = 60          # how far back to read snapshots (a paused campaign's last totals)

ALERT, WATCH, OK, TOO_FEW, NO_REFERENCE = "ALERT", "WATCH", "OK", "TOO FEW", "NO REFERENCE"
NO_SENDS, NOT_COUNTED = "NO SENDS", "NOT COUNTED YET"   # no sends in the window: not a sending domain
VERDICTS = (ALERT, WATCH, OK, TOO_FEW, NO_REFERENCE, NO_SENDS, NOT_COUNTED)
COUNTS = ("sent", "replies", "bounce_count", "sender_bounce_count")


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in str(email) else ""


def _int(v) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def aggregate_domain_stats(mailbox_rows: list[dict]) -> dict[str, dict]:
    """One campaign's mailbox-statistics rows -> {domain: {sent, replies, bounce_count, sender_bounce_count}}.
    "email" is accepted as well as from_email (fixtures, other callers)."""
    out: dict[str, dict] = {}
    for r in mailbox_rows:
        dom = _domain(str(r.get("email", "") or r.get("from_email", "")))
        if not dom:
            continue
        d = out.setdefault(dom, dict.fromkeys(COUNTS, 0))
        d["sent"] += _int(r.get("sent_count"))
        d["replies"] += _int(r.get("reply_count"))
        d["bounce_count"] += _int(r.get("bounce_count"))
        d["sender_bounce_count"] += _int(r.get("sender_bounce_count"))
    return out


def merge_campaign(fleet: dict[str, dict], campaign_id, client: str, account: str,
                   per_domain: dict[str, dict]) -> None:
    """Add one campaign's per-domain lifetime totals to the day's snapshot, {domain: doc}. The doc keeps the
    domain total (what older readers use) and the campaign breakdown. A domain's client is the client of the
    campaign that sent the most from it."""
    for dom, s in per_domain.items():
        doc = fleet.setdefault(dom, {"domain": dom, "account": account, **dict.fromkeys(COUNTS, 0),
                                     "campaigns": {}})
        for k in COUNTS:
            doc[k] += _int(s.get(k))
        doc["campaigns"][str(campaign_id)] = {**{k: _int(s.get(k)) for k in COUNTS}, "client": client}
        by_client: dict[str, int] = defaultdict(int)
        for c in doc["campaigns"].values():
            by_client[c["client"]] += c["sent"]
        doc["client"] = max(sorted(by_client), key=lambda k: by_client[k])


def window_stats(docs: list[dict], today: date, days: int = WINDOW_DAYS) -> dict[str, dict]:
    """Stored daily snapshots (any domains, any order) -> what each domain sent and got back in
    (today - days, today]: {domain: {"sent", "replies", "client", "by_campaign": {id: [sent, replies]},
    "unattributed": [sent, replies], "uncounted": bool}}. "unattributed" comes from the older domain-total
    snapshots. "uncounted" is True when the totals show sends the window could not place: the domain or one
    of its campaigns first seen inside the window with lifetime sends, or an old-format jump that was dropped."""
    start, end = (today - timedelta(days=days)).isoformat(), today.isoformat()
    by_dom: dict[str, list[dict]] = defaultdict(list)
    for d in docs:
        if d.get("domain") and str(d.get("date", "")) <= end:
            by_dom[d["domain"]].append(d)
    out: dict[str, dict] = {}
    for dom, ds in by_dom.items():
        ds.sort(key=lambda x: x["date"])
        by_campaign: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        legacy = [0, 0]
        last: dict[str, tuple[int, int]] = {}          # campaign -> its last stored (sent, replies)
        prev = None
        uncounted = False
        for doc in ds:
            camps = doc.get("campaigns") or {}
            if prev is None and doc["date"] > start:   # the domain first seen inside the window
                uncounted = uncounted or _int(doc.get("sent")) > 0
            if prev is not None and doc["date"] > start:
                if camps and last:
                    for cid, c in camps.items():
                        if cid not in last:            # first sighting: lifetime totals, not this window's sends
                            uncounted = uncounted or _int(c.get("sent")) > 0
                            continue
                        sent = _int(c.get("sent")) - last[cid][0]
                        if sent < 0:                   # totals went down (leads deleted): restart from here
                            continue
                        by_campaign[cid][0] += sent
                        by_campaign[cid][1] += max(0, _int(c.get("replies")) - last[cid][1])
                else:
                    gap = max(1, (date.fromisoformat(doc["date"][:10]) - date.fromisoformat(prev["date"][:10])).days)
                    sent = _int(doc.get("sent")) - _int(prev.get("sent"))
                    if 0 <= sent <= LEGACY_MAX_DAY * gap:
                        legacy[0] += sent
                        legacy[1] += max(0, _int(doc.get("replies")) - _int(prev.get("replies")))
                    elif sent > 0:                     # a campaign rejoining ACTIVE, dropped
                        uncounted = True
            for cid, c in camps.items():
                last[cid] = (_int(c.get("sent")), _int(c.get("replies")))
            prev = doc
        sent = legacy[0] + sum(v[0] for v in by_campaign.values())
        replies = legacy[1] + sum(v[1] for v in by_campaign.values())
        out[dom] = {"sent": sent, "replies": replies, "client": ds[-1].get("client") or "",
                    "by_campaign": dict(by_campaign), "unattributed": legacy, "uncounted": uncounted}
    return out


def client_totals(window: dict[str, dict]) -> dict[str, dict]:
    """{client: {"sent", "replies"}} over the window, every domain that sent for the client."""
    out: dict[str, dict] = defaultdict(lambda: {"sent": 0, "replies": 0})
    for w in window.values():
        out[w["client"]]["sent"] += w["sent"]
        out[w["client"]]["replies"] += w["replies"]
    return dict(out)


def judge(window: dict[str, dict], domains) -> dict[str, dict]:
    """Verdict for each of `domains` (the ones sending today), judged against window_stats output for every
    domain. -> {domain: {"verdict", "client", "sent", "replies", "judged_sent", "observed", "expected",
    "reference", "ratio", "p"}}; reference says what `expected` came from: "campaigns" (the other domains in
    its campaigns), "client" (the client's other domains) or "mixed". A domain with no sends counted in the
    window is NO_SENDS or NOT_COUNTED, never a judged or TOO_FEW sending domain."""
    camp = defaultdict(lambda: [0, 0])
    for w in window.values():
        for cid, (s, r) in w["by_campaign"].items():
            camp[cid][0] += s
            camp[cid][1] += r
    clients = client_totals(window)
    out: dict[str, dict] = {}
    for dom in sorted(domains):
        w = window.get(dom) or {"sent": 0, "replies": 0, "client": "", "by_campaign": {}, "unattributed": [0, 0],
                                "uncounted": True}
        n = obs = 0
        exp = 0.0
        rest = list(w["unattributed"])                  # sends judged at the client's rate
        for cid, (s, r) in w["by_campaign"].items():
            o_s, o_r = camp[cid][0] - s, camp[cid][1] - r
            if o_s >= MIN_REFERENCE_SENT:
                n, obs, exp = n + s, obs + r, exp + s * o_r / o_s
            else:
                rest[0] += s
                rest[1] += r
        by_campaigns = n
        c = clients.get(w["client"], {"sent": 0, "replies": 0})
        c_s, c_r = c["sent"] - w["sent"], c["replies"] - w["replies"]
        if rest[0] and c_s >= MIN_REFERENCE_SENT:
            n, obs, exp = n + rest[0], obs + rest[1], exp + rest[0] * c_r / c_s
        reference = "campaigns" if n == by_campaigns else "client" if not by_campaigns else "mixed"
        p = poisson_le(obs, exp)
        if w["sent"] <= 0:
            verdict = NOT_COUNTED if w.get("uncounted") else NO_SENDS
        elif w["sent"] < MIN_SENT:
            verdict = TOO_FEW
        elif n < MIN_SENT:
            verdict = NO_REFERENCE
        elif p <= ALERT_P:
            verdict = ALERT
        elif exp and obs / exp < WATCH_RATIO and p <= WATCH_P:
            verdict = WATCH
        else:
            verdict = OK
        out[dom] = {"verdict": verdict, "client": w["client"], "sent": w["sent"], "replies": w["replies"],
                    "judged_sent": n, "observed": obs, "expected": round(exp, 1), "reference": reference,
                    "ratio": round(obs / exp, 2) if exp else None, "p": round(p, 4)}
    return out


def new_alerts(verdicts: dict[str, dict], prior: dict[str, str]) -> list[str]:
    """Domains at ALERT today that were not at ALERT on their last judged day (prior = {domain: verdict})."""
    return [d for d, v in sorted(verdicts.items()) if v["verdict"] == ALERT and prior.get(d) != ALERT]


OPINION_SOURCE = "the reply check"
OPINION_FRESH_DAYS = 3      # a stored day older than this is not used as an opinion


def second_opinion(doc: dict) -> dict | None:
    """One stored day of a domain's reply check (reply_monitor's snapshot: date, verdict, window_sent,
    window_replies) as a second opinion for the bill review, or None when the check has no view. OK is a PASS
    (real recipients answer it like the client's other domains), ALERT is a FAIL. WATCH, too few sends, no
    reference and no sends give no opinion: the bill is then judged on the placement test alone. This reads
    replies from every recipient provider, so it says whether the domain reaches people, not where one
    provider files it."""
    verdict = {OK: "PASS", ALERT: "FAIL"}.get(str((doc or {}).get("verdict") or ""))
    sent, replies = _int((doc or {}).get("window_sent")), _int((doc or {}).get("window_replies"))
    if not verdict or not (doc or {}).get("date") or sent < MIN_SENT:
        return None
    level = "in line with" if verdict == "PASS" else "far below"
    return {"verdict": verdict, "on": str(doc["date"])[:10], "source": OPINION_SOURCE, "inbox": replies, "sent": sent,
            "said": (f"its domain got {replies} {'reply' if replies == 1 else 'replies'} on {sent} emails in "
                     f"{WINDOW_DAYS} days, {level} the client's other domains")}


def second_opinions(docs: list[dict], today: date) -> dict[str, dict]:
    """{"@domain": opinion} from stored reply-check days, each domain's newest day within OPINION_FRESH_DAYS.
    The "@" key marks a domain-wide opinion: an opinion about one inbox (a placement test) is looked up first."""
    newest: dict[str, dict] = {}
    floor = (today - timedelta(days=OPINION_FRESH_DAYS)).isoformat()
    for d in docs or []:
        dom, day = str(d.get("domain") or "").lower(), str(d.get("date") or "")[:10]
        if dom and floor <= day <= today.isoformat() and day >= str(newest.get(dom, {}).get("date") or "")[:10]:
            newest[dom] = d
    return {"@" + dom: op for dom, d in newest.items() if (op := second_opinion(d))}
