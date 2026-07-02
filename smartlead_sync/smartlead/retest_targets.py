"""Pure worst-first selection of inboxes to auto-retest, per-client capped."""
from __future__ import annotations

_UNTESTED = "No placement test on record"
_STALE = "Placement test is stale"
# problems whose owner is the auto-tester (must match health.resolve_action text)
_AUTO_PROBLEMS = {_UNTESTED, _STALE}


def _rank(row: dict) -> tuple:
    """Sort key: untested (0) before stale (1); within stale, oldest test_date first."""
    problem = row.get("top_problem", "")
    if problem == _UNTESTED:
        return (0, "")
    # stale: empty date sorts first (unknown = treat as very old)
    return (1, str(row.get("test_date", "")) or "0000-00-00")


def select_targets(health_rows: list[dict], per_client_cap: int,
                   pending_emails: set[str]) -> list[dict]:
    by_client: dict[str, list[dict]] = {}
    for r in health_rows:
        if r.get("top_problem") not in _AUTO_PROBLEMS:
            continue
        email = str(r.get("email", "")).strip()
        if not email or email in pending_emails:
            continue
        by_client.setdefault(r.get("client", ""), []).append(r)

    targets: list[dict] = []
    for client, rows in by_client.items():
        rows.sort(key=_rank)
        for r in rows[:per_client_cap]:
            targets.append({
                "client": client,
                "email": r.get("email", ""),
                "campaign_hint": r.get("campaign_name", ""),
                "reason": r.get("top_problem", ""),
            })
    return targets
