"""Pure per-domain reply-rate aggregation + early-warning rules. No I/O.

Why domain-level: placement shifts hit a DOMAIN's replies ~48h before
opens/bounces move, and campaign-level rates can't isolate which domain is
degrading (docs/SCALE_ROADMAP.md §3).

Field names verified LIVE against the real Smartlead API on 2026-07-08
(Task 4 Step 1): GET /campaigns/{id}/mailbox-statistics (base
https://server.smartlead.ai/api/v1), paginated via offset/limit (limit
caps at 20 - the API 400s above that). Response: {"ok": true, "data": [...]}
where each row has from_email, sent_count, reply_count (plus open_count,
click_count, bounce_count, sender_bounce_count, unsubscribed_count -
unused here). Cross-checked two ways: (1) raw httpx call via
SmartleadClient._get against campaign 3556327 (DARLEAN account), (2) the
get_campaign_mailbox_statistics MCP tool against the same campaign -
identical rows/field names from both."""
from __future__ import annotations

from smartlead.config import (
    REPLY_ALERT_DROP_RATIO, REPLY_ALERT_MIN_SENT, REPLY_ONE_PERCENT_MIN_SENT,
)


def _domain(email: str) -> str:
    return email.split("@", 1)[1].lower() if "@" in str(email) else ""


def aggregate_domain_stats(mailbox_rows: list[dict]) -> dict[str, dict]:
    """mailbox_rows: per-sender stats rows with email + sent/reply counts.
    Field names follow the live endpoint (verified in Task 4 Step 1):
    from_email, sent_count, reply_count. "email" is also accepted (test
    fixtures / other callers use that key)."""
    out: dict[str, dict] = {}
    for r in mailbox_rows:
        dom = _domain(str(r.get("email", "") or r.get("from_email", "")))
        if not dom:
            continue
        d = out.setdefault(dom, {"sent": 0, "replies": 0})
        d["sent"] += int(r.get("sent_count", 0) or 0)
        d["replies"] += int(r.get("reply_count", 0) or 0)
    return out


def _rate(sent: int, replies: int) -> float:
    return (replies / sent) if sent else 0.0


def evaluate_alerts(domain: str, current: dict, history: list[dict]) -> list[str]:
    """current = this week's {sent, replies}; history = prior daily records
    (each {sent, replies}), most recent first or any order — summed as the
    baseline window."""
    alerts: list[str] = []
    cur_rate = _rate(current.get("sent", 0), current.get("replies", 0))

    base_sent = sum(int(h.get("sent", 0) or 0) for h in history)
    base_replies = sum(int(h.get("replies", 0) or 0) for h in history)
    base_rate = _rate(base_sent, base_replies)

    if (base_rate > 0 and current.get("sent", 0) >= REPLY_ALERT_MIN_SENT
            and cur_rate < base_rate * REPLY_ALERT_DROP_RATIO):
        alerts.append(
            f"reply-rate drop: {cur_rate:.1%} vs own baseline {base_rate:.1%} "
            f"(>{100 - int(REPLY_ALERT_DROP_RATIO * 100)}% down) — placement "
            "likely shifting; act within 48h")

    if (current.get("sent", 0) >= REPLY_ONE_PERCENT_MIN_SENT and cur_rate < 0.01):
        alerts.append(
            f"1% rule: {cur_rate:.1%} reply rate after "
            f"{current.get('sent', 0)} sends — inbox/domain underperforming")
    return alerts
