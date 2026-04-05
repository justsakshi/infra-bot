"""Data processing helpers - provider detection, availability rules, data assembly."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone, timedelta
from smartlead.api import SmartleadClient
from smartlead.config import MIN_WARMUP_REP_PCT, ACTIVE_STATUSES, MAX_INBOX_LIMIT

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
    if "GMAIL" in upper:
        return "Gmail"
    if "OUTLOOK" in upper:
        return "Outlook"
    return "Other"


def format_daily_limit(account: dict) -> str:
    sent = account.get("daily_sent_count", 0)
    limit = account.get("message_per_day", 0)
    return f"{sent} / {limit}"


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
        # Check if paused campaign is stale
        if status.upper().startswith("PAUSE"):
            created = c.get("created_at", "")
            try:
                created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                if (now - created_dt) > timedelta(days=PAUSED_STALE_DAYS):
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
        # Note: We can add a fetch_all_campaign_details helper to SmartleadClient later if needed, 
        # but for now we'll do it in chunks here like accounts.
        from smartlead.config import API_CHUNK_SIZE, API_CHUNK_DELAY
        for i in range(0, len(detail_campaign_summaries), API_CHUNK_SIZE):
            chunk = detail_campaign_summaries[i : i + API_CHUNK_SIZE]
            tasks = [client._get(f"/campaigns/{str(c['id'])}") for c in chunk]
            res = await asyncio.gather(*tasks, return_exceptions=True)
            for r in res:
                if not isinstance(r, Exception):
                    detail_campaigns.append(r)
            if i + API_CHUNK_SIZE < len(detail_campaign_summaries):
                await asyncio.sleep(API_CHUNK_DELAY)

    # 3. All email-account details (bulk)
    try:
        raw_accounts = await client.list_email_accounts()
        all_accounts = await client.fetch_all_account_details(raw_accounts) if raw_accounts else []
        print(f"  [{name}] {len(all_accounts)}/{len(raw_accounts)} account details fetched")
    except Exception as exc:
        print(f"  [!] Account detail fetch failed: {exc}")
        all_accounts = []

    account_map: dict[str, dict] = {str(a["id"]): a for a in all_accounts}

    # 3. Map campaign -> email-account ids (only for campaigns we drill into)
    campaign_to_inboxes: dict[str, list[str]] = {}
    for campaign in detail_campaigns:
        c_id = str(campaign.get("id", ""))
        try:
            accs = await client.get_campaign_email_accounts(c_id)
            campaign_to_inboxes[c_id] = [str(a.get("id")) for a in accs]
        except Exception:
            campaign_to_inboxes[c_id] = []

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

        # Analytics
        analytics = {}
        try:
            analytics = await client.get_campaign_analytics(c_id)
        except Exception as exc:
            print(f"  [!] Analytics for '{c_name}': {exc}")

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

        # Inbox rows for this campaign
        try:
            accs = await client.get_campaign_email_accounts(c_id)
            for acc_info in accs:
                acc_id = str(acc_info.get("id"))
                full = account_map.get(acc_id, acc_info)
                email = full.get("from_email", "")
                stats = campaign_load_stats.get(c_id, {"leads_remaining": 0, "inbox_count": 0, "individual_load": 0, "true_load": 0})

                # Aggregate true load per unique mailbox across all campaigns
                inbox_aggregate_load[email] = inbox_aggregate_load.get(email, 0.0) + stats["true_load"]

                inbox_data.append(_build_inbox_row(
                    full, email, c_name, c_status, stats, deliverability_map,
                ))
        except Exception as exc:
            print(f"  [!] Accounts for '{c_name}': {exc}")

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
    warmup_rep_map: dict[str, str],
    inbox_aggregate_load: dict[str, float],
) -> None:
    """Mutate *inbox_data* in-place: fill reputation, total load, and availability."""
    for item in inbox_data:
        email = item.get("email", "")
        rep_str = warmup_rep_map.get(email, "N/A")
        item["warmup_rep_pct"] = rep_str

        # Aggregate True Load and Capacity (using inbox's actual daily limit)
        total_true_load = round(inbox_aggregate_load.get(email, 0.0), 1)
        inbox_limit = item.get("message_per_day", 0) or MAX_INBOX_LIMIT  # fallback to 35 if not set
        capacity = max(0.0, round(inbox_limit - total_true_load, 1))

        item["true_load"] = total_true_load
        item["available_capacity"] = capacity

        try:
            rep_val = float(rep_str.replace("%", "")) if "%" in rep_str else 0
            test = item.get("test_sheet_status", "")

            # Aravind/Anjali Selection Rule:
            # FREE if (Capacity > 0) AND (Rep >= 90%) AND (Test Status == "inbox")
            item["availability"] = (
                "FREE" if capacity > 0 and rep_val >= MIN_WARMUP_REP_PCT and test == "inbox"
                else "BUSY"
            )
        except (ValueError, TypeError):
            item["availability"] = "BUSY"


# ── Private helpers ──────────────────────────────────────────────────────────

def _build_inbox_row(
    account: dict,
    email: str,
    campaign_name: str,
    campaign_status: str,
    load_info: dict,
    deliverability_map: dict[str, str],
) -> dict:
    domain = get_domain_from_email(email)
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
        "test_sheet_status": deliverability_map.get(domain, "Unknown"),
        "availability": "TBD",
        "account_id": str(account.get("id", "")),
    }


async def _build_warmup_data(
    client: SmartleadClient,
    all_accounts: list[dict],
) -> tuple[list[dict], dict[str, str]]:
    """Build warmup-reputation rows and a ``{email: rep_str}`` map."""
    warmup_data: list[dict] = []
    rep_map: dict[str, str] = {}

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

        try:
            stats = await client.get_warmup_stats(str(acc_id))
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
        except Exception as exc:
            print(f"  [!] Warmup stats for {email}: {exc}")

        rep_map[email] = entry["warmup_reputation"]
        warmup_data.append(entry)

    return warmup_data, rep_map
