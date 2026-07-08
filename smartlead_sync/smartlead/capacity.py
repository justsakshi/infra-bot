"""Pure capacity math: demand vs healthy supply vs bench. No I/O.

Sources for the constants (docs/SCALE_ROADMAP.md §1): bench 20-25% of active
fleet, 20% demand headroom, 30/day per-inbox send cap, 2 inboxes/domain,
order lead time ≈ 4-6 weeks (purchase 1-2d + warmup 21-30d)."""
from __future__ import annotations

import math
from datetime import date, timedelta

from smartlead.config import (
    CAPACITY_PER_INBOX_CAP, CAPACITY_HEADROOM, CAPACITY_BENCH_RATIO,
    CAPACITY_BENCH_MIN, CAPACITY_LEAD_TIME_DAYS,
)

_ON_STATES = {"warming", "ramped", "on"}


def _rep(row: dict) -> float:
    try:
        return float(str(row.get("warmup_rep_pct", "")).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _healthy(row: dict) -> bool:
    return (bool(row.get("connection_ok"))
            and str(row.get("warmup_state", "")).lower() in _ON_STATES
            and _rep(row) >= 90.0
            and str(row.get("test_sheet_status", "")).lower() == "inbox")


def _active(row: dict) -> bool:
    return str(row.get("campaign_status", "")).upper() == "ACTIVE"


def compute_client_capacity(rows: list[dict], demand_per_day: float,
                            churn_per_month: int) -> dict:
    """rows = deduped inbox rows for ONE client."""
    healthy = [r for r in rows if _healthy(r)]
    active = [r for r in healthy if _active(r)]
    bench = [r for r in healthy if not _active(r)]

    safe_capacity = sum(
        min(int(r.get("message_per_day", 0) or 0) or CAPACITY_PER_INBOX_CAP,
            CAPACITY_PER_INBOX_CAP)
        for r in active)
    bench_target = max(CAPACITY_BENCH_MIN,
                       math.ceil(len(active) * CAPACITY_BENCH_RATIO))

    needed = demand_per_day * CAPACITY_HEADROOM
    shortfall = max(0.0, needed - safe_capacity)
    order_inboxes = (math.ceil(shortfall / CAPACITY_PER_INBOX_CAP)
                     + max(0, bench_target - len(bench))
                     + churn_per_month)
    order_domains = math.ceil(order_inboxes / 2)  # 2 inboxes/domain policy

    headroom_pct = (round(100.0 * safe_capacity / needed) if needed
                    else (100 if safe_capacity else 0))
    status = "OK" if order_inboxes == 0 else "ORDER NOW"
    order_by = ((date.today() + timedelta(days=CAPACITY_LEAD_TIME_DAYS))
                .isoformat() if order_inboxes else "")

    return {
        "sendable_inboxes": len(active),
        "safe_capacity": safe_capacity,
        "bench": len(bench),
        "bench_target": bench_target,
        "demand_per_day": round(demand_per_day, 1),
        "headroom_pct": headroom_pct,
        "order_inboxes": order_inboxes,
        "order_domains": order_domains,
        "order_by": order_by,
        "status": status,
    }
