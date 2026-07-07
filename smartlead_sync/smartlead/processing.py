"""Data processing helpers - provider detection, availability rules, data assembly."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from smartlead.api import SmartleadClient
from smartlead.config import MIN_WARMUP_REP_PCT, ACTIVE_STATUSES, MAX_INBOX_LIMIT
from smartlead.dns_checker import audit_domain_dns

PAUSED_STALE_DAYS = 7  # paused campaigns older than this are treated as finished


def _is_active_status(status: str) -> bool:
    """Return True if status should be treated as active/paused for drill-in."""
    s = str(status).upper().strip()
    if not s:
        return False
    # Match exact or prefix (e.g., STARTED, PAUSED)
    return any(s == val or s.startswith(val) for val in ACTIVE_STATUSES)


def _campaign_id(campaign: dict) -> str:
    """Return a stable campaign id string from common keys."""
    if not isinstance(campaign, dict):
        return ""
    cid = campaign.get("id") or campaign.get("campaign_id")
    return str(cid) if cid is not None else ""


def _basic_campaign_row(campaign: dict) -> dict:
    """Return a summary row with only basic campaign info."""
    return {
        "campaign_id": _campaign_id(campaign),
        "name": campaign.get("name", ""),
        "status": campaign.get("status", ""),
        "total_leads": "-",
        "unique_sent": "-",
        "reach_pct": "-",
        "sent": "-",
        "opened": "-",
        "replied": "-",
        "bounced": "-",
        "not_started": "-",
        "in_progress": "-",
        "paused": "-",
        "completed": "-",
        "stopped": "-",
    }


# ── Utilities ────────────────────────────────────────────────────────────────

def get_domain_from_email(email: str) -> str:
    if not email or "@" not in email:
        return ""
    return email.split("@")[-1].strip().lower()


def detect_provider(type_field: str) -> str:
    upper = str(type_field).upper()
    if "GMAIL" in upper or "GOOGLE" in upper:
        return "Gmail"
    if "OUTLOOK" in upper or "OFFICE" in upper or "MICROSOFT" in upper or "EXCHANGE" in upper:
        return "Outlook"
    return "Other"


def format_daily_limit(account: dict) -> str:
    sent = account.get("daily_sent_count", 0)
    limit = account.get("message_per_day", 0)
    return f"{sent} / {limit}"


def _classify_warmup_state(warmup_details: dict, rep_val: float) -> str:
    """Return one of: blocked / off / warming / ramped."""
    from smartlead.config import WARMUP_RAMP_DAYS
    if not isinstance(warmup_details, dict):
        return "off"
    if warmup_details.get("is_warmup_blocked"):
        return "blocked"
    if warmup_details.get("status") != "ACTIVE":
        return "off"
    created = warmup_details.get("created_at", "")
    age_days = None
    try:
        created_dt = datetime.fromisoformat(str(created).replace("Z", "+00:00"))
        age_days = (datetime.now(timezone.utc) - created_dt).days
    except (ValueError, TypeError):
        age_days = None
    if age_days is None or age_days < WARMUP_RAMP_DAYS or rep_val < 90:
        return "warming"
    return "ramped"


def _latest_active_date(stats: object, fallback_updated_at: str) -> str:
    """Latest YYYY-MM-DD with sent_count>0 in warmup stats; else updated_at date."""
    latest = ""
    rows = []
    if isinstance(stats, dict):
        rows = stats.get("stats_by_date", []) or []
    elif isinstance(stats, list):
        for s in stats:
            if isinstance(s, dict):
                if s.get("stats_by_date"):
                    rows.extend(s.get("stats_by_date") or [])
                elif "date" in s:
                    rows.append(s)
    for r in rows:
        try:
            if int(r.get("sent_count", 0)) > 0:
                d = str(r.get("date", ""))[:10]
                if d > latest:
                    latest = d
        except (ValueError, TypeError):
            continue
    if latest:
        return latest
    return str(fallback_updated_at)[:10] if fallback_updated_at else ""


# ── Core data-fetch orchestration ────────────────────────────────────────────

async def fetch_account_data(
    client: SmartleadClient,
    deliverability_map: dict[str, str],
    active_only: bool = True,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Fetch campaigns, inboxes, and warmup data for one Smartlead account.

    Args:
        active_only: If True (default), only process ACTIVE/PAUSED campaigns.
                     Pass False to include COMPLETED/DRAFTED/STOPPED too.

    Returns ``(inbox_data, campaign_summary, warmup_data)``.
    """
    name = client.account_name
    print(f"  [{name}] key={client.masked_key}")

    # 1. Campaigns
    try:
        all_campaigns = await client.list_campaigns()
    except Exception as exc:
        print(f"  [!] Campaigns fetch failed: {exc}")
        return [], [], []

    # Split into active vs inactive campaigns
    # Paused campaigns older than PAUSED_STALE_DAYS are treated as finished
    now = datetime.now(timezone.utc)
    active_campaigns = []
    inactive_campaigns = []
    for c in all_campaigns:
        status = c.get("status", "")
        if not _is_active_status(status):
            inactive_campaigns.append(c)
            continue
        # Check if paused campaign is stale (based on last update, falling back to creation)
        if status.upper().startswith("PAUSE"):
            updated = c.get("updated_at") or c.get("created_at", "")
            try:
                updated_dt = datetime.fromisoformat(str(updated).replace("Z", "+00:00"))
                if (now - updated_dt) > timedelta(days=PAUSED_STALE_DAYS):
                    inactive_campaigns.append(c)
                    continue
            except (ValueError, TypeError):
                pass
        active_campaigns.append(c)

    if active_only:
        detail_campaign_summaries = active_campaigns
        print(f"  [{name}] {len(all_campaigns)} total campaigns, {len(active_campaigns)} active (fetching details), {len(inactive_campaigns)} inactive (summary only)")
    else:
        detail_campaign_summaries = all_campaigns
        print(f"  [{name}] {len(all_campaigns)} campaigns (all included)")

    # 2. Bulk fetch campaign details (to get daily limits/settings)
    # We do this for all "detail" campaigns to avoid 0 true_load
    detail_campaigns: list[dict] = []
    if detail_campaign_summaries:
        print(f"  [{name}] Fetching details for {len(detail_campaign_summaries)} campaigns...")
        detail_ids = [str(c["id"]) for c in detail_campaign_summaries if c.get("id") is not None]
        detail_campaigns = await client.fetch_all_campaign_details(detail_ids)

    # 3. All email-account details (bulk)
    try:
        raw_accounts = await client.list_email_accounts()
        all_accounts = await client.fetch_all_account_details(raw_accounts) if raw_accounts else []
        print(f"  [{name}] {len(all_accounts)}/{len(raw_accounts)} account details fetched")
    except Exception as exc:
        print(f"  [!] Account detail fetch failed: {exc}")
        all_accounts = []

    account_map: dict[str, dict] = {str(a["id"]): a for a in all_accounts}

    # 3a. DNS Authentication Audit for all unique domains concurrently
    unique_domains = {get_domain_from_email(a.get("from_email", "")) for a in all_accounts if a.get("from_email")}
    unique_domains.discard("")
    dns_tasks = {dom: audit_domain_dns(dom) for dom in unique_domains}
    dns_audit_map = {}
    if dns_tasks:
        dns_results_list = await asyncio.gather(*dns_tasks.values(), return_exceptions=True)
        for dom, res in zip(dns_tasks.keys(), dns_results_list):
            if isinstance(res, Exception):
                print(f"  [!] DNS check failed for {dom}: {res}")
                dns_audit_map[dom] = {
                    "spf_ok": True, "spf_msg": "DNS check skipped due to error",
                    "dkim_ok": True, "dkim_msg": "DNS check skipped due to error",
                    "dmarc_ok": True, "dmarc_msg": "DNS check skipped due to error",
                }
            else:
                dns_audit_map[dom] = res
        print(f"  [{name}] Audited {len(dns_tasks)} domain(s) for SPF/DKIM/DMARC")

    # 3. Map campaign -> email-accounts (fetched once, reused for inbox rows below)
    detail_ids = [str(c.get("id", "")) for c in detail_campaigns if c.get("id") is not None]
    campaign_accounts_map = await client.fetch_campaign_accounts_map(detail_ids)
    campaign_to_inboxes: dict[str, list[str]] = {
        c_id: [str(a.get("id")) for a in accs]
        for c_id, accs in campaign_accounts_map.items()
    }

    # 3b. Analytics for every detail campaign (fetched once, concurrently)
    analytics_map = await client.fetch_campaign_analytics_map(detail_ids)

    # 4a. Build FULL campaign summary (all campaigns - active get analytics, inactive get basic info)
    campaign_summary: list[dict] = []
    campaign_load_stats: dict[str, dict] = {}  # {c_id: {true_load: X, indiv_load: Y, count: Z}}
    inbox_data: list[dict] = []

    # Aggregated true load per inbox address {email: total_true_load}
    inbox_aggregate_load: dict[str, float] = {}

    # Active campaigns: full analytics + inbox rows
    for campaign in detail_campaigns:
        if not isinstance(campaign, dict):
            continue
        c_id = _campaign_id(campaign)
        c_name = campaign.get("name", "")
        c_status = campaign.get("status", "")
        if not c_id:
            campaign_summary.append(_basic_campaign_row(campaign))
            continue

        # Analytics (pre-fetched concurrently above)
        analytics = analytics_map.get(c_id, {})

        sent = int(analytics.get("sent_count", 0))
        opened = int(analytics.get("open_count", 0)) or int(analytics.get("unique_open_count", 0))
        lead_stats = analytics.get("campaign_lead_stats", {})
        total_leads = lead_stats.get("total", 0)
        not_started = lead_stats.get("notStarted", 0)
        in_progress = lead_stats.get("inprogress", 0)
        unique_sent = int(analytics.get("unique_sent_count", 0))
        reach_pct = f"{round(unique_sent / total_leads * 100, 1)}%" if total_leads else "0%"

        campaign_summary.append({
            "campaign_id": c_id,
            "name": c_name,
            "status": c_status,
            "total_leads": total_leads,
            "unique_sent": unique_sent,
            "reach_pct": reach_pct,
            "sent": sent,
            "opened": opened,
            "replied": int(analytics.get("reply_count", 0)),
            "bounced": int(analytics.get("bounce_count", 0)),
            "not_started": not_started,
            "in_progress": in_progress,
            "paused": lead_stats.get("paused", 0),
            "completed": lead_stats.get("completed", 0),
            "stopped": lead_stats.get("stopped", 0),
        })

        # Load stats
        leads_rem = not_started + in_progress
        inboxes_in_campaign = campaign_to_inboxes.get(c_id, [])
        inbox_count = len(inboxes_in_campaign)
        indiv_leads = round(leads_rem / inbox_count, 1) if inbox_count else 0
        
        # Aravind's Rule: True Load based on Daily Limit distribution
        # If no leads remain, load is 0. Otherwise, it's (limit / inboxes).
        c_daily_limit = int(campaign.get("max_leads_per_day", 0))
        true_load = round(c_daily_limit / inbox_count, 1) if (inbox_count and leads_rem > 0) else 0

        campaign_load_stats[c_id] = {
            "leads_remaining": leads_rem,
            "inbox_count": inbox_count,
            "individual_load": indiv_leads,
            "true_load": true_load,
        }

        # Inbox rows for this campaign (accounts pre-fetched into campaign_accounts_map)
        for acc_info in campaign_accounts_map.get(c_id, []):
            acc_id = str(acc_info.get("id"))
            full = account_map.get(acc_id, acc_info)
            email = full.get("from_email", "")
            stats = campaign_load_stats.get(c_id, {"leads_remaining": 0, "inbox_count": 0, "individual_load": 0, "true_load": 0})

            # Aggregate true load per unique mailbox across all campaigns
            inbox_aggregate_load[email] = inbox_aggregate_load.get(email, 0.0) + stats["true_load"]

            inbox_data.append(_build_inbox_row(
                full, email, c_name, c_status, stats, deliverability_map,
                dns_audit_map=dns_audit_map, client=name,
            ))

    # 4b. Inactive campaigns: basic summary row only (no API calls for analytics/inboxes)
    if active_only:
        for campaign in inactive_campaigns:
            if not isinstance(campaign, dict):
                continue
            campaign_summary.append(_basic_campaign_row(campaign))

    # 5. Orphaned accounts (accounts not attached to any active campaign)
    seen_emails = {d["email"] for d in inbox_data}
    orphan_count = 0
    for acc in all_accounts:
        if not isinstance(acc, dict):
            continue
        email = acc.get("from_email", "")
        if email in seen_emails:
            continue
        inbox_data.append(_build_inbox_row(
            acc, email, "N/A (No active campaign)", "N/A",
            {"leads_remaining": 0, "inbox_count": 0, "individual_load": 0, "true_load": 0},
            deliverability_map,
            dns_audit_map=dns_audit_map,
            client=name,
        ))
        seen_emails.add(email)
        orphan_count += 1
    if orphan_count:
        print(f"  [{name}] {orphan_count} accounts not in any active campaign (added to inbox tab)")

    # 6. Warmup data + reputation map
    warmup_data, warmup_rep_map = await _build_warmup_data(client, all_accounts)

    # 7. Fill warmup rep + capacity + availability on inbox rows
    process_inbox_availability(inbox_data, warmup_rep_map, inbox_aggregate_load)

    return inbox_data, campaign_summary, warmup_data


# ── Availability rule engine ─────────────────────────────────────────────────

def process_inbox_availability(
    inbox_data: list[dict],
    warmup_rep_map: dict[str, dict],
    inbox_aggregate_load: dict[str, float],
) -> None:
    """Mutate *inbox_data* in-place: fill reputation, total load, and availability."""
    for item in inbox_data:
        email = item.get("email", "")
        info = warmup_rep_map.get(email, {})
        rep_str = info.get("rep", "N/A") if isinstance(info, dict) else "N/A"
        item["warmup_rep_pct"] = rep_str
        item["warmup_state"] = info.get("warmup_state", "off") if isinstance(info, dict) else "off"
        item["warmup_max_count"] = info.get("warmup_max_count", 0) if isinstance(info, dict) else 0
        item["last_active_date"] = info.get("last_active_date", "") if isinstance(info, dict) else ""
        item["warmup_spam_count"] = info.get("warmup_spam_count", 0) if isinstance(info, dict) else 0

        # Aggregate True Load and Capacity (using inbox's actual daily limit)
        total_true_load = round(inbox_aggregate_load.get(email, 0.0), 1)
        inbox_limit = item.get("message_per_day", 0) or MAX_INBOX_LIMIT  # fallback to 35 if not set
        capacity = max(0.0, round(inbox_limit - total_true_load, 1))

        item["true_load"] = total_true_load
        item["available_capacity"] = capacity

        try:
            rep_val = float(rep_str.replace("%", "")) if "%" in rep_str else 0
        except (ValueError, TypeError):
            rep_val = 0
        test = item.get("test_sheet_status", "")

        # Selection rule: an inbox is FREE (connectable) only if EVERY check passes.
        # busy_reason lists every failed check so the downstream tool / a human
        # knows exactly why an inbox is excluded.
        reasons: list[str] = []
        if not item.get("connection_ok"):
            reasons.append("disconnected")
        if item.get("warmup_state") == "blocked":
            reasons.append("warmup_blocked")
        if capacity <= 0:
            reasons.append("no_capacity")
        if rep_val < MIN_WARMUP_REP_PCT:
            reasons.append("low_rep")
        if test == "stale":
            reasons.append("stale_test")
        elif test in ("fail", "spam"):
            reasons.append("failed_test")
        elif test != "inbox":
            reasons.append("untested")  # Unknown / blank

        # Check DNS records
        if not item.get("dns_spf_ok", True) or not item.get("dns_dkim_ok", True) or not item.get("dns_dmarc_ok", True):
            reasons.append("dns_error")

        item["availability"] = "FREE" if not reasons else "BUSY"
        item["busy_reason"] = "" if not reasons else ",".join(reasons)


# ── Private helpers ──────────────────────────────────────────────────────────

def _apply_staleness(status: str, date_str: str) -> str:
    """Return 'stale' if a passing test is older than TEST_STALE_DAYS, else status."""
    from smartlead.config import TEST_STALE_DAYS
    if status == "inbox" and date_str:
        try:
            d = datetime.fromisoformat(date_str)
            if (datetime.now() - d).days > TEST_STALE_DAYS:
                return "stale"
        except (ValueError, TypeError):
            pass
    return status


def _build_inbox_row(
    account: dict,
    email: str,
    campaign_name: str,
    campaign_status: str,
    load_info: dict,
    deliverability_map: dict[str, dict],
    dns_audit_map: dict[str, dict] = None,
    client: str = "",
) -> dict:
    email_key = email.strip().lower()
    domain = get_domain_from_email(email)
    entry = deliverability_map.get(domain) or deliverability_map.get(email_key) or {}
    if not isinstance(entry, dict):
        entry = {}
    raw_status = entry.get("status", "Unknown") or "Unknown"
    test_date = entry.get("date", "")
    test_status = _apply_staleness(raw_status, test_date)

    max_per_day = int(account.get("message_per_day", 0) or 0)
    sent_today = int(account.get("daily_sent_count", 0) or 0)
    capacity_left = max(0, max_per_day - sent_today)
    connection_ok = bool(
        account.get("is_smtp_success", True) and account.get("is_imap_success", True)
        and not account.get("is_suspended", False)
    )

    dns_info = (dns_audit_map or {}).get(domain) or {
        "spf_ok": True, "spf_msg": "",
        "dkim_ok": True, "dkim_msg": "",
        "dmarc_ok": True, "dmarc_msg": ""
    }

    return {
        "email": email,
        "name": account.get("from_name", ""),
        "provider": detect_provider(account.get("type", "")),
        "campaign_name": campaign_name,
        "campaign_status": campaign_status,
        "daily_limit": format_daily_limit(account),
        "message_per_day": int(account.get("message_per_day", 0)),  # raw limit for capacity calc
        "leads_remaining": load_info["leads_remaining"],
        "total_inboxes": load_info["inbox_count"],
        "individual_load": load_info["individual_load"],
        "true_load": 0.0,            # filled in process_inbox_availability
        "available_capacity": 0.0,   # filled in process_inbox_availability
        "warmup_rep_pct": "TBD",
        "test_sheet_status": test_status,
        "test_date": test_date,
        "availability": "TBD",
        "busy_reason": "",           # filled in process_inbox_availability
        "account_id": str(account.get("id", "")),
        "client": client,
        "connection_ok": connection_ok,
        "max_per_day": max_per_day,
        "sent_today": sent_today,
        "capacity_left": capacity_left,
        "warmup_state": "off",          # filled in process_inbox_availability
        "last_active_date": "",         # filled in process_inbox_availability
        "warmup_max_count": 0,          # filled in process_inbox_availability
        "dns_spf_ok": dns_info.get("spf_ok", True),
        "dns_spf_msg": dns_info.get("spf_msg", ""),
        "dns_dkim_ok": dns_info.get("dkim_ok", True),
        "dns_dkim_msg": dns_info.get("dkim_msg", ""),
        "dns_dmarc_ok": dns_info.get("dmarc_ok", True),
        "dns_dmarc_msg": dns_info.get("dmarc_msg", ""),
    }


async def _build_warmup_data(
    client: SmartleadClient,
    all_accounts: list[dict],
) -> tuple[list[dict], dict[str, dict]]:
    """Build warmup-reputation rows and a ``{email: dict}`` map."""
    warmup_data: list[dict] = []
    rep_map: dict[str, dict] = {}

    # Pre-fetch warmup stats for every account concurrently (instead of serially)
    account_ids = [
        str(a["id"]) for a in all_accounts
        if isinstance(a, dict) and a.get("id") is not None
    ]
    stats_map = await client.fetch_all_warmup_stats(account_ids)

    for acc in all_accounts:
        if not isinstance(acc, dict):
            continue
        email = acc.get("from_email", "")
        acc_id = acc.get("id")
        warmup_details = acc.get("warmup_details", {})

        # Use warmup_reputation directly from Smartlead's account detail
        # (matches what the Smartlead UI shows)
        sl_rep = warmup_details.get("warmup_reputation")
        rep_str = f"{sl_rep}%" if sl_rep is not None else "N/A"
        rep_val = float(sl_rep) if sl_rep is not None else 0.0

        entry: dict = {
            "email": email,
            "name": acc.get("from_name", ""),
            "provider": detect_provider(acc.get("type", "")),
            "warmup_enabled": warmup_details.get("status") == "ACTIVE",
            "daily_limit": format_daily_limit(acc),
            "warmup_limit": warmup_details.get("warmup_max_count", 0),
            "warmup_sent": 0,
            "landed_inbox": 0,
            "landed_spam": 0,
            "warmup_reputation": rep_str,
        }

        stats = stats_map.get(str(acc_id))
        if isinstance(stats, list) and stats:
            entry["warmup_limit"] = stats[0].get("warmup_limit", entry["warmup_limit"])
            total_sent = sum(int(d.get("sent_count", 0)) for d in stats)
            total_inbox = sum(int(d.get("inbox_count", 0)) for d in stats)
            total_spam = sum(int(d.get("spam_count", 0)) for d in stats)
            entry.update(warmup_sent=total_sent, landed_inbox=total_inbox, landed_spam=total_spam)
        elif isinstance(stats, dict):
            entry["warmup_limit"] = stats.get("warmup_limit", entry["warmup_limit"])
            total_sent = int(stats.get("sent_count", 0))
            total_inbox = int(stats.get("inbox_count", 0))
            total_spam = int(stats.get("spam_count", 0))
            entry.update(warmup_sent=total_sent, landed_inbox=total_inbox, landed_spam=total_spam)

        rep_map[email] = {
            "rep": entry["warmup_reputation"],
            "warmup_state": _classify_warmup_state(warmup_details, rep_val),
            "warmup_max_count": warmup_details.get("warmup_max_count", 0),
            "last_active_date": _latest_active_date(stats_map.get(str(acc_id)), acc.get("updated_at", "")),
            "warmup_spam_count": entry["landed_spam"],
        }
        warmup_data.append(entry)

    return warmup_data, rep_map
