"""Pure planner for inbox rotation: pick broken senders to swap out and the
best same-client bench inbox to swap in.

Works on RAW inbox rows from fetch_account_data (one row per inbox+campaign),
which carry: email, name, account_id, client, campaign_name, campaign_status,
test_sheet_status, warmup_rep_pct, warmup_state, connection_ok, capacity_left,
availability, busy_reason.

Rules (spec 2026-07-03):
- Victim: placement test failed (spam-landing) AND in an ACTIVE campaign.
- Bench (same client, best-first): test=inbox, rep>=90, connected, not blocked,
  capacity left, not the victim, not already in that campaign, used once per run.
- Persona match (same first name) is a SOFT preference for the swap and decides
  the in-flight policy (per Smartlead support 2026-07-07: removal leaves
  mid-sequence leads PENDING; same-thread continuation is impossible):
  match -> "resume" the stranded leads on the new same-name sender (new thread),
  no match -> "leave_paused" (drop the tail cleanly).
"""
from __future__ import annotations

from smartlead.config import HEALTH_WARMUP_ZERO

_FAIL = {"fail", "spam"}


def _rep(row: dict) -> float:
    try:
        return float(str(row.get("warmup_rep_pct", "")).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _num(v) -> float:
    try:
        return float(str(v).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _first_name(row: dict) -> str:
    return str(row.get("name", "")).strip().split(" ")[0].lower()


def _is_victim(row: dict) -> bool:
    status = str(row.get("test_sheet_status", "")).strip().lower()
    reasons = str(row.get("busy_reason", ""))
    in_live = str(row.get("campaign_status", "")).upper() == "ACTIVE"
    return in_live and (status in _FAIL or "failed_test" in reasons)


def _is_bench(row: dict) -> bool:
    status = str(row.get("test_sheet_status", "")).strip().lower()
    return (
        status == "inbox"
        and _rep(row) >= HEALTH_WARMUP_ZERO
        and bool(row.get("connection_ok", False))
        and str(row.get("warmup_state", "")).lower() != "blocked"
        and _num(row.get("capacity_left", row.get("available_capacity", 0))) > 0
    )


def select_swaps(rows: list[dict], per_client_cap: int,
                 already_swapped: set[tuple] | None = None) -> list[dict]:
    """Return [{client, campaign_name, victim_email, victim_account_id,
    replacement_email, replacement_account_id, inflight_policy, persona_match}]."""
    already_swapped = already_swapped or set()

    # victims: one entry per (email, campaign)
    victims: list[dict] = []
    seen_v: set[tuple] = set()
    for r in rows:
        if not _is_victim(r):
            continue
        key = (str(r.get("email", "")).lower(), r.get("campaign_name", ""))
        if key in seen_v or key in already_swapped or not key[0]:
            continue
        seen_v.add(key)
        victims.append(r)

    # bench: dedupe by email, per client
    bench_by_client: dict[str, list[dict]] = {}
    seen_b: set[str] = set()
    victim_emails = {str(v.get("email", "")).lower() for v in victims}
    for r in rows:
        email = str(r.get("email", "")).lower()
        if not email or email in seen_b or email in victim_emails:
            continue
        if not _is_bench(r):
            continue
        seen_b.add(email)
        bench_by_client.setdefault(r.get("client", ""), []).append(r)

    # which campaigns each bench inbox already sits in (skip same-campaign adds)
    campaigns_of: dict[str, set[str]] = {}
    for r in rows:
        email = str(r.get("email", "")).lower()
        camp = r.get("campaign_name", "")
        if email and camp:
            campaigns_of.setdefault(email, set()).add(camp)

    swaps: list[dict] = []
    used_bench: set[str] = set()
    per_client_count: dict[str, int] = {}

    for v in victims:
        client = v.get("client", "")
        if per_client_count.get(client, 0) >= per_client_cap:
            continue
        candidates = [
            b for b in bench_by_client.get(client, [])
            if str(b.get("email", "")).lower() not in used_bench
            and v.get("campaign_name", "") not in campaigns_of.get(str(b.get("email", "")).lower(), set())
        ]
        if not candidates:
            swaps.append({
                "client": client, "campaign_name": v.get("campaign_name", ""),
                "victim_email": v.get("email", ""), "victim_account_id": v.get("account_id", ""),
                "replacement_email": None, "replacement_account_id": None,
                "inflight_policy": "alert", "persona_match": False,
            })
            continue
        vname = _first_name(v)
        # best-first: persona match, then FREE availability, then highest rep
        candidates.sort(key=lambda b: (
            0 if _first_name(b) == vname and vname else 1,
            0 if str(b.get("availability", "")).upper() == "FREE" else 1,
            -_rep(b),
        ))
        best = candidates[0]
        match = bool(vname) and _first_name(best) == vname
        used_bench.add(str(best.get("email", "")).lower())
        per_client_count[client] = per_client_count.get(client, 0) + 1
        swaps.append({
            "client": client, "campaign_name": v.get("campaign_name", ""),
            "victim_email": v.get("email", ""), "victim_account_id": v.get("account_id", ""),
            "replacement_email": best.get("email", ""), "replacement_account_id": best.get("account_id", ""),
            "inflight_policy": "resume" if match else "leave_paused",
            "persona_match": match,
        })
    return swaps
