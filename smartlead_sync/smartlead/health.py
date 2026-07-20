"""Pure inbox-health scoring, action resolution, and trend (no I/O)."""
from __future__ import annotations

from datetime import date, datetime

from smartlead.config import (
    HEALTH_WEIGHTS, HEALTH_TEST_STALE_DAYS, HEALTH_TEST_DEAD_DAYS,
    HEALTH_WARMUP_FULL, HEALTH_WARMUP_ZERO, HEALTH_TREND_DROP,
    HEALTH_SPAM_FLAG_ENABLED, HEALTH_SPAM_COUNT_THRESHOLD,
    VOLUME_SAFE_MAX,
)

_FAIL = {"fail", "spam"}


def _num(v) -> float:
    try:
        return float(str(v).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _reasons(snapshot: dict) -> set[str]:
    return {r.strip() for r in str(snapshot.get("busy_reason", "")).split(",") if r.strip()}


def _test_age_days(test_date: str, today: date) -> int | None:
    if not test_date:
        return None
    try:
        d = datetime.fromisoformat(str(test_date)[:10]).date()
    except ValueError:
        return None
    return (today - d).days


def _placement_points(snapshot: dict, today: date) -> int:
    """40 max. inbox+fresh=40; fail=0; stale decays toward half; dead/untested=neutral 20."""
    maxp = HEALTH_WEIGHTS["placement"]
    status = str(snapshot.get("test_sheet_status", "")).strip().lower()
    if status in _FAIL:
        return 0
    age = _test_age_days(str(snapshot.get("test_date", "")), today)
    if status not in {"inbox", "stale"} or age is None:
        return maxp // 2  # untested -> neutral
    if age >= HEALTH_TEST_DEAD_DAYS:
        return maxp // 2  # too old -> neutral (treat as untested)
    if age <= HEALTH_TEST_STALE_DAYS:
        return maxp        # fresh inbox
    # linear decay between stale and dead: maxp -> maxp/2
    span = HEALTH_TEST_DEAD_DAYS - HEALTH_TEST_STALE_DAYS
    frac = (age - HEALTH_TEST_STALE_DAYS) / span
    return round(maxp - frac * (maxp / 2))


def _warmup_points(snapshot: dict) -> int:
    maxp = HEALTH_WEIGHTS["warmup"]
    rep = _num(snapshot.get("warmup_rep_pct"))
    if rep <= 0:
        return maxp // 2  # unknown -> neutral
    if rep >= HEALTH_WARMUP_FULL:
        return maxp
    if rep < HEALTH_WARMUP_ZERO:
        return 0
    frac = (rep - HEALTH_WARMUP_ZERO) / (HEALTH_WARMUP_FULL - HEALTH_WARMUP_ZERO)
    return round(frac * maxp)


def _bounce_points(snapshot: dict) -> int:
    """20 max. Bounce is campaign-level; inbox rows lack it -> do not penalize.
    If a bounce rate is present (%), <1%=full, >5%=0, linear between."""
    maxp = HEALTH_WEIGHTS["bounce"]
    raw = snapshot.get("bounce_rate")
    if raw in (None, ""):
        return maxp  # no per-inbox bounce signal -> do not penalize
    br = _num(raw)
    if br < 1.0:
        return maxp
    if br > 5.0:
        return 0
    frac = 1 - (br - 1.0) / 4.0
    return round(frac * maxp)


def _connection_points(snapshot: dict) -> int:
    maxp = HEALTH_WEIGHTS["connection"]
    if not snapshot.get("connection_ok", True):
        return 0
    score = maxp
    if not snapshot.get("dns_spf_ok", True):
        score -= 5
    if not snapshot.get("dns_dkim_ok", True):
        score -= 5
    if not snapshot.get("dns_dmarc_ok", True):
        score -= 3
    return max(0, score)


def _grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 70:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def compute_health_score(snapshot: dict, today: date) -> dict:
    drivers = {
        "placement": _placement_points(snapshot, today),
        "warmup": _warmup_points(snapshot),
        "bounce": _bounce_points(snapshot),
        "connection": _connection_points(snapshot),
    }
    score = int(sum(drivers.values()))
    return {"score": score, "grade": _grade(score), "drivers": drivers}


def resolve_action(snapshot: dict, score: int) -> dict:
    """First-match-wins problem -> fix/owner/priority. Same order as the queue."""
    reasons = _reasons(snapshot)
    status = str(snapshot.get("test_sheet_status", "")).strip().lower()

    def item(priority, problem, what, how, owner, skill):
        state = "broken" if priority == "P0" else ("needs_action" if priority else "healthy")
        return {"priority": priority, "top_problem": problem, "what_to_do": what,
                "how_long": how, "owner": owner, "owner_skill": skill, "status": state}

    # R3: warmup spam landings (toggle). Landing in spam even during warmup is a
    # top red flag — surfaces above failed-test since it's live-reputation damage.
    if HEALTH_SPAM_FLAG_ENABLED and _num(snapshot.get("warmup_spam_count", 0)) > HEALTH_SPAM_COUNT_THRESHOLD:
        return item("P0", "Landing in spam during warmup",
                    "Pause inbox; audit copy + auth (SPF/DKIM/DMARC); check Google Postmaster complaint rate; retest.",
                    "1-7 days", "human", "deliverability-incident-response")

    if "failed_test" in reasons or status in _FAIL:
        return item("P0", "Failed placement test",
                    "Pause inbox/domain; check SPF/DKIM/DMARC + copy + list; retest.",
                    "1-7 days", "human", "deliverability-incident-response")

    # Critical DNS SPF check. DNS is Zapmail's responsibility (2026-07-07):
    # we detect + escalate, we never edit records ourselves.
    if not snapshot.get("dns_spf_ok", True):
        spf_msg = snapshot.get("dns_spf_msg", "Missing or invalid SPF record")
        return item("P0", "SPF misconfigured or missing",
                    f"Escalate to Zapmail support (do NOT edit DNS yourself): {spf_msg}",
                    "1-2 days", "human", "deliverability-incident-response")

    # Critical DNS DKIM check. Outlook inboxes often legitimately lack a DKIM
    # record at the default selector — confirm with Zapmail before treating as
    # broken (meeting 2026-07-07), so Outlook gets P1/verify, others stay P0.
    if not snapshot.get("dns_dkim_ok", True):
        dkim_msg = snapshot.get("dns_dkim_msg", "Missing or invalid DKIM record")
        if "outlook" in str(snapshot.get("provider", "")).lower():
            return item("P1", "DKIM missing (Outlook — may be expected)",
                        f"Ask Zapmail if DKIM is expected for this Outlook domain: {dkim_msg}",
                        "1-3 days", "human", "deliverability-incident-response")
        return item("P0", "DKIM misconfigured or missing",
                    f"Escalate to Zapmail support (do NOT edit DNS yourself): {dkim_msg}",
                    "1-2 days", "human", "deliverability-incident-response")

    if "warmup_blocked" in reasons:
        return item("P0", "Warmup blocked",
                    "Investigate block reason; pause or retire inbox if it does not recover.",
                    "1-7 days", "human", "smartlead-inbox-manager")

    if "disconnected" in reasons or not snapshot.get("connection_ok", True):
        return item("P1", "SMTP/IMAP disconnected",
                    "Reconnect the inbox before any campaign assignment.",
                    "minutes", "human", "smartlead-inbox-manager")

    # Critical DNS DMARC check (escalate to Zapmail — never edit DNS ourselves)
    if not snapshot.get("dns_dmarc_ok", True):
        dmarc_msg = snapshot.get("dns_dmarc_msg", "Missing or invalid DMARC record")
        return item("P1", "DMARC misconfigured or missing",
                    f"Escalate to Zapmail support (do NOT edit DNS yourself): {dmarc_msg}",
                    "1-2 days", "human", "deliverability-incident-response")

    if "low_rep" in reasons:
        return item("P1", "Warmup reputation below threshold",
                    "Keep out of campaigns; auto-warmup sets RECOVERING profile "
                    "(15/day + reply rate 30% — boost replies, not volume); retest after recovery.",
                    "3-14 days", "human", "smartlead-inbox-manager")
    if "untested" in reasons or status in {"", "unknown"}:
        return item("P1", "No placement test on record",
                    "Auto-run initial GSuite + Outlook placement test.",
                    "auto", "auto", "deliverability-test-public")
    if "stale_test" in reasons or status == "stale":
        return item("P1", "Placement test is stale",
                    "Auto-run fresh placement test.",
                    "auto", "auto", "deliverability-test-public")
    if int(_num(snapshot.get("max_per_day"))) >= 35 and status != "inbox":
        return item("P2", "High-volume inbox needs routine retest",
                    "Auto-retest before scaling volume.",
                    "auto", "auto", "deliverability-test-public")
    # 2026 volume-cap watch: sending above the safe band is a reputation risk
    # (100+/day = 4.3x bounce). Fix = lower the CAMPAIGN cap, never warmup (Avi).
    if _num(snapshot.get("sent_today", 0)) > VOLUME_SAFE_MAX:
        return item("P2", f"Sending above 2026 safe volume (>{VOLUME_SAFE_MAX}/day)",
                    "Lower this inbox's campaign daily cap to 30-40; scale with more inboxes, not volume.",
                    "minutes", "human", "smartlead-inbox-manager")
    return item("", "", "", "", "", "")


def compute_trend(today_score: int, prior_score: int | None) -> dict:
    if prior_score is None:
        return {"delta_7d": None, "arrow": "—", "declining": False}
    delta = today_score - prior_score
    arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
    return {"delta_7d": delta, "arrow": arrow, "declining": delta <= -HEALTH_TREND_DROP}


def _domain(email: str) -> str:
    return str(email).split("@", 1)[1].strip().lower() if "@" in str(email) else ""


def build_health_rows(inbox_rows: list[dict], today, store, resolve_manager) -> list[dict]:
    """Score+action+trend+manager for each unique inbox. `today` is a date."""
    from smartlead.sheets import _dedupe_inbox_rows  # reuse existing dedup
    rows: list[dict] = []
    for snap in _dedupe_inbox_rows(inbox_rows):
        client = snap.get("client", "")
        email = str(snap.get("email", "")).strip()
        if not email:
            continue
        hs = compute_health_score(snap, today)
        act = resolve_action(snap, hs["score"])
        prior = store.prior_score(client, email, 7, today) if store else None
        tr = compute_trend(hs["score"], prior)
        # manager ownership follows the SUB-client (Melior/Bettrdata/OSC inside
        # the PL agency account), not the Smartlead account name
        sub_client = snap.get("sub_client") or client
        mgr = resolve_manager(sub_client)
        drivers = hs["drivers"]
        rows.append({
            "priority": act["priority"],
            "client": client,
            "sub_client": sub_client,
            "email": email,
            "domain": _domain(email),
            "provider": snap.get("provider", ""),
            "score": hs["score"],
            "grade": hs["grade"],
            "trend": (f"{tr['arrow']} {tr['delta_7d']:+d}" if tr["delta_7d"] is not None else tr["arrow"]),
            "status": act["status"],
            "top_problem": act["top_problem"],
            "what_to_do": act["what_to_do"],
            "owner": "🤖 Auto" if act["owner"] == "auto" else ("👤 You" if act["owner"] == "human" else ""),
            "how_long": act["how_long"],
            "manager": mgr["name"],
            "drivers": f"test {drivers['placement']}/40 · warmup {drivers['warmup']}/25 · bounce {drivers['bounce']}/20 · conn {drivers['connection']}/15",
            "warmup_rep_pct": snap.get("warmup_rep_pct", ""),
            "test_sheet_status": snap.get("test_sheet_status", ""),
            "test_date": snap.get("test_date", ""),
            "campaigns": snap.get("campaigns", 0),
            "campaign_name": snap.get("campaign_name", ""),
            "owner_skill": act["owner_skill"],
            "_declining": tr["declining"],
        })
    priority_order = {"P0": 0, "P1": 1, "P2": 2, "": 9}
    rows.sort(key=lambda r: (priority_order.get(r["priority"], 9), r["client"], r["score"]))
    return rows


def health_records_for_store(rows: list[dict], today) -> list[dict]:
    """Project workbook rows into Mongo history records."""
    ds = today.strftime("%Y-%m-%d")
    out = []
    for r in rows:
        out.append({
            "client": r["client"], "email": r["email"], "domain": r["domain"],
            "date": ds, "score": r["score"], "grade": r["grade"],
            "placement_status": r["test_sheet_status"], "placement_date": r["test_date"],
            "warmup_rep_pct": r["warmup_rep_pct"], "campaigns": r["campaigns"],
        })
    return out
