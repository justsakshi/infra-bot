"""Pure inbox-health scoring, action resolution, and trend (no I/O)."""
from __future__ import annotations

from datetime import date, datetime

from smartlead.config import (
    HEALTH_WEIGHTS, HEALTH_TEST_STALE_DAYS, HEALTH_TEST_DEAD_DAYS,
    HEALTH_WARMUP_FULL, HEALTH_WARMUP_ZERO, HEALTH_TREND_DROP,
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
    return maxp  # connected; reply-rate rule needs campaign data -> full when connected


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

    if "failed_test" in reasons or status in _FAIL:
        return item("P0", "Failed placement test",
                    "Pause inbox/domain; check SPF/DKIM/DMARC + copy + list; retest.",
                    "1-7 days", "human", "deliverability-incident-response")
    if "warmup_blocked" in reasons:
        return item("P0", "Warmup blocked",
                    "Investigate block reason; pause or retire inbox if it does not recover.",
                    "1-7 days", "human", "smartlead-inbox-manager")
    if "disconnected" in reasons or not snapshot.get("connection_ok", True):
        return item("P1", "SMTP/IMAP disconnected",
                    "Reconnect the inbox before any campaign assignment.",
                    "minutes", "human", "smartlead-inbox-manager")
    if "low_rep" in reasons:
        return item("P1", "Warmup reputation below threshold",
                    "Keep out of campaigns; continue/adjust warmup; retest after recovery.",
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
    return item("", "", "", "", "", "")


def compute_trend(today_score: int, prior_score: int | None) -> dict:
    if prior_score is None:
        return {"delta_7d": None, "arrow": "—", "declining": False}
    delta = today_score - prior_score
    arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
    return {"delta_7d": delta, "arrow": arrow, "declining": delta <= -HEALTH_TREND_DROP}
