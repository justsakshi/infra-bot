"""Async HTTP client for the Smartlead REST API."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from smartlead.config import (
    BASE_URL, API_CHUNK_SIZE, API_TIMEOUT, API_CHUNK_DELAY,
    API_MAX_RETRIES, API_RETRY_BASE_DELAY, API_RETRY_MAX_DELAY,
)


class SmartleadClient:
    """Thin async wrapper around the Smartlead v1 API.

    Usage::

        async with SmartleadClient(api_key="...") as client:
            campaigns = await client.list_campaigns()
    """

    def __init__(self, api_key: str, account_name: str = "Default") -> None:
        self._api_key = api_key
        self.account_name = account_name
        self._client: httpx.AsyncClient | None = None

    # ── context-manager ──────────────────────────────────────────────────
    async def __aenter__(self) -> "SmartleadClient":
        self._client = httpx.AsyncClient(timeout=API_TIMEOUT)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── low-level ────────────────────────────────────────────────────────
    @staticmethod
    def _retry_delay(resp: httpx.Response, attempt: int) -> float:
        """Pick a backoff delay, preferring the server's Retry-After header."""
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(float(retry_after), API_RETRY_MAX_DELAY)
            except ValueError:
                pass  # HTTP-date form — fall back to exponential
        return min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)

    async def _get(self, endpoint: str, extra_params: dict | None = None) -> Any:
        """GET with automatic retry on 429, 5xx, and transient network errors."""
        assert self._client, "Use `async with SmartleadClient(...)` as a context manager."
        params = {"api_key": self._api_key}
        if extra_params:
            params.update(extra_params)
        for attempt in range(API_MAX_RETRIES + 1):
            # Network-level errors (timeouts, connection resets) — retry too
            try:
                resp = await self._client.get(f"{BASE_URL}{endpoint}", params=params)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [!] network error on {endpoint}: {exc!r} - retrying in {delay:.0f}s (attempt {attempt + 1}/{API_MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    continue
                raise

            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    delay = self._retry_delay(resp, attempt)
                    print(f"  [!] {resp.status_code} on {endpoint} - retrying in {delay:.0f}s (attempt {attempt + 1}/{API_MAX_RETRIES})")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()  # final attempt - let it raise

    # ── campaigns ────────────────────────────────────────────────────────
    async def list_campaigns(self) -> list[dict]:
        return await self._get("/campaigns")

    async def get_campaign_analytics(self, campaign_id: str) -> dict:
        return await self._get(f"/campaigns/{campaign_id}/analytics")

    async def get_campaign_email_accounts(self, campaign_id: str) -> list[dict]:
        return await self._get(f"/campaigns/{campaign_id}/email-accounts")

    # ── email accounts ───────────────────────────────────────────────────
    async def list_email_accounts(self) -> list[dict]:
        """Fetch all email accounts, paginating automatically (API caps at 100/page)."""
        all_accounts: list[dict] = []
        offset = 0
        page_size = 100
        while True:
            batch = await self._get("/email-accounts", extra_params={"offset": offset, "limit": page_size})
            if not isinstance(batch, list) or not batch:
                break
            all_accounts.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
        return all_accounts

    async def get_email_account(self, account_id: str) -> dict:
        return await self._get(f"/email-accounts/{account_id}")

    async def get_warmup_stats(self, account_id: str) -> list[dict] | dict:
        return await self._get(f"/email-accounts/{account_id}/warmup-stats")

    async def update_email_account(self, account_id: str, fields: dict) -> dict:
        """Partial-update an email account (POST /email-accounts/{id}).
        Documented fields incl. max_email_per_day — the SAME daily cap shared by
        warmup + campaign sends (Smartlead's own description)."""
        assert self._client, "Use `async with SmartleadClient(...)` as a context manager."
        url = f"{BASE_URL}/email-accounts/{account_id}"
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                resp = await self._client.post(url, params={"api_key": self._api_key}, json=fields)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [!] network error updating account {account_id}: {exc!r} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                raise
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    delay = self._retry_delay(resp, attempt)
                    print(f"  [!] {resp.status_code} updating account {account_id} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    async def set_warmup(self, account_id: str, enabled: bool,
                         total_per_day: int = 40, daily_rampup: int = 5,
                         reply_rate: int = 20, auto_adjust: bool | None = None) -> dict:
        """Enable/disable warmup on an inbox (POST /email-accounts/{id}/warmup).

        auto_adjust=True turns on Smartlead's smart-adjusting algorithm
        (auto-lowers warmup while the inbox sends in a live campaign); None
        leaves the account's current setting untouched."""
        assert self._client, "Use `async with SmartleadClient(...)` as a context manager."
        if enabled:
            body = {
                "warmup_enabled": "true",
                "total_warmup_per_day": total_per_day,
                "reply_rate_percentage": reply_rate,
            }
            # Smartlead rejects daily_rampup outside 5-20 with a 400 (found
            # live 2026-07-09: 50/55 writes failed on daily_rampup=0). To
            # disable ramping, omit the field and turn the feature off.
            if daily_rampup and daily_rampup > 0:
                body["daily_rampup"] = daily_rampup
                body["is_rampup_enabled"] = True
            else:
                body["is_rampup_enabled"] = False
            if auto_adjust is not None:
                body["auto_adjust_warmup"] = bool(auto_adjust)
        else:
            body = {"warmup_enabled": "false"}
        url = f"{BASE_URL}/email-accounts/{account_id}/warmup"
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                resp = await self._client.post(url, params={"api_key": self._api_key}, json=body)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [!] network error on warmup {account_id}: {exc!r} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                raise
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    delay = self._retry_delay(resp, attempt)
                    print(f"  [!] {resp.status_code} on warmup {account_id} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    # ── inbox rotation (add/remove senders on a campaign, lead reassignment) ──
    async def _request_json(self, method: str, endpoint: str, body: dict) -> Any:
        """POST/DELETE with json body + api_key param; single retry pass on 429/5xx."""
        assert self._client, "Use `async with SmartleadClient(...)` as a context manager."
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                resp = await self._client.request(
                    method, f"{BASE_URL}{endpoint}",
                    params={"api_key": self._api_key}, json=body,
                )
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [!] network error on {endpoint}: {exc!r} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                raise
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    delay = self._retry_delay(resp, attempt)
                    print(f"  [!] {resp.status_code} on {endpoint} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    # ── campaign cloning (campaign_twin.py) ──────────────────────────────
    async def get_campaign(self, campaign_id: str) -> dict:
        """Full campaign record (GET /campaigns/{id}): track_settings,
        scheduler_cron_value, stop_lead_settings, follow_up_percentage,
        max_leads_per_day, min_time_btwn_emails, send_as_plain_text, ...

        NOTE: track_settings comes back in a SHORT form (DONT_EMAIL_OPEN,
        DONT_LINK_CLICK) that the settings POST does not accept - see
        campaign_twin.to_post_track_settings(). bounce_autopause_threshold is
        NOT returned at all (verified 2026-07)."""
        return await self._get(f"/campaigns/{campaign_id}")

    async def get_campaign_sequences(self, campaign_id: str) -> list[dict]:
        """All sequence steps with nested `sequence_variants`
        (GET /campaigns/{id}/sequences). Variants may be empty on
        single-variant campaigns, in which case subject/email_body sit on the
        step itself."""
        return await self._get(f"/campaigns/{campaign_id}/sequences")

    async def save_campaign_sequences_full(self, campaign_id: str,
                                           sequences: list[dict]) -> dict:
        """Save a complete multi-step, multi-variant sequence
        (POST /campaigns/{id}/sequences). `sequences` must already be in POST
        shape: [{seq_number, seq_delay_details:{delay_in_days}, seq_variants:[
        {subject, email_body, variant_label}]}]."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/sequences",
                                        {"sequences": sequences})

    async def delete_campaign(self, campaign_id: str) -> dict:
        """Permanently delete a campaign (DELETE /campaigns/{id}). Used only to
        roll back a half-built twin or clean up a test clone."""
        return await self._request_json("DELETE", f"/campaigns/{campaign_id}", {})

    async def add_campaign_email_accounts(self, campaign_id: str, account_ids: list[int]) -> dict:
        """Attach sender inboxes to a campaign (verified: POST /campaigns/{id}/email-accounts)."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/email-accounts",
                                        {"email_account_ids": [int(a) for a in account_ids]})

    async def remove_campaign_email_accounts(self, campaign_id: str, account_ids: list[int]) -> dict:
        """Detach sender inboxes from a campaign (verified: DELETE /campaigns/{id}/email-accounts)."""
        return await self._request_json("DELETE", f"/campaigns/{campaign_id}/email-accounts",
                                        {"email_account_ids": [int(a) for a in account_ids]})

    async def resume_lead(self, campaign_id: str, lead_id: int) -> dict:
        """Resume a stranded/paused lead so it continues on the campaign's
        remaining sender(s) at its next sequence step (new thread, new sender —
        per Smartlead support 2026-07-07: threading across senders is a
        protocol-level impossibility; resume is the documented mechanism)."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/leads/{lead_id}/resume", {})

    async def pause_lead(self, campaign_id: str, lead_id: int) -> dict:
        """Pause a lead's sequence (policy B fallback for in-flight leads)."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/leads/{lead_id}/pause", {})

    # ── campaign creation (non-connected placement-test sender campaigns) ──
    async def create_campaign(self, name: str, client_id: int | None = None) -> dict:
        """Create an empty campaign (POST /campaigns/create). Returns {id, name, ...}."""
        body: dict = {"name": name}
        if client_id is not None:
            body["client_id"] = int(client_id)
        return await self._request_json("POST", "/campaigns/create", body)

    async def save_campaign_sequences(self, campaign_id: str, subject: str, email_body: str) -> dict:
        """Save a single-step sequence (POST /campaigns/{id}/sequences)."""
        body = {
            "sequences": [{
                "seq_number": 1,
                "seq_delay_details": {"delay_in_days": 0},
                "seq_variants": [{
                    "subject": subject,
                    "email_body": email_body,
                    "variant_label": "A",
                }],
            }]
        }
        return await self._request_json("POST", f"/campaigns/{campaign_id}/sequences", body)

    async def add_campaign_leads(self, campaign_id: str, emails: list[str],
                                 linkedin_urls: dict[str, str] | None = None) -> dict:
        """Add leads by email (POST /campaigns/{id}/leads). Ignores block/dup lists
        — seed addresses must never be filtered out.

        `linkedin_urls` maps email -> LinkedIn profile URL and is written to each
        lead's `linkedin_profile` field. Policy (2026-08-06) is to supply it
        whenever the URL is known: Smartlead keys leads by email and Expandi keys
        them by profile URL, so this field is the only join between the two. It
        cannot be backfilled once a campaign is running, and without it a lead who
        replies on LinkedIn keeps receiving email.

        Seed/placement-test callers pass emails only, which is correct — those
        addresses have no LinkedIn counterpart.
        """
        linkedin_urls = linkedin_urls or {}
        lead_list: list[dict] = []
        for e in emails:
            lead: dict = {"email": e}
            url = linkedin_urls.get(e)
            if url:
                lead["linkedin_profile"] = url
            lead_list.append(lead)
        body = {
            "lead_list": lead_list,
            "settings": {
                "ignore_global_block_list": True,
                "ignore_unsubscribe_list": True,
                "ignore_duplicate_leads_in_other_campaign": True,
            },
        }
        return await self._request_json("POST", f"/campaigns/{campaign_id}/leads", body)

    async def update_campaign_schedule(self, campaign_id: str, schedule: dict) -> dict:
        """Set campaign schedule (POST /campaigns/{id}/schedule)."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/schedule", schedule)

    async def set_campaign_status(self, campaign_id: str, status: str) -> dict:
        """Set campaign status (POST /campaigns/{id}/status). status: START|PAUSED|STOPPED."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/status", {"status": status})

    async def update_campaign_settings(self, campaign_id: str, settings: dict) -> dict:
        """Partial-update campaign general settings
        (POST /campaigns/{id}/settings — documented fields incl.
        bounce_autopause_threshold, enable_ai_esp_matching, send_as_plain_text)."""
        return await self._request_json("POST", f"/campaigns/{campaign_id}/settings", settings)

    # ── campaign leads / dated analytics (for Campaign Metrics dashboard) ──
    async def get_campaign_leads(self, campaign_id: str) -> list[dict]:
        """Fetch all leads for a campaign (paginated). Each: created_at, lead_category_id, status."""
        all_leads: list[dict] = []
        offset = 0
        page = 100
        while True:
            resp = await self._get(f"/campaigns/{campaign_id}/leads",
                                   extra_params={"offset": offset, "limit": page})
            data = resp.get("data", []) if isinstance(resp, dict) else []
            all_leads.extend(data)
            if len(data) < page:
                break
            offset += page
        return all_leads

    async def get_analytics_by_date(self, campaign_id: str, start_date: str, end_date: str) -> dict:
        """Range-aggregate analytics for [start_date, end_date] (YYYY-MM-DD). Values may be strings."""
        return await self._get(f"/campaigns/{campaign_id}/analytics-by-date",
                               extra_params={"start_date": start_date, "end_date": end_date})

    async def get_campaign_mailbox_statistics(self, campaign_id: str) -> list[dict]:
        """Per-sender-mailbox stats for a campaign (paginated).

        Endpoint + field names verified LIVE on 2026-07-08 (Task 4 Step 1):
        GET /campaigns/{id}/mailbox-statistics -> {"ok": true, "data": [...]}
        where each row has from_email, sent_count, reply_count (also
        open_count, click_count, bounce_count, sender_bounce_count,
        unsubscribed_count - unused here). `limit` caps at 20 - the API
        returns 400 for limit=25+ (confirmed live), so page at 20, not 100.
        Cross-checked against the get_campaign_mailbox_statistics MCP tool
        on the same campaign - identical rows."""
        all_rows: list[dict] = []
        offset = 0
        page = 20
        while True:
            resp = await self._get(f"/campaigns/{campaign_id}/mailbox-statistics",
                                   extra_params={"offset": offset, "limit": page})
            data = resp.get("data", []) if isinstance(resp, dict) else (resp or [])
            all_rows.extend(data)
            if len(data) < page:
                break
            offset += page
        return all_rows

    # ── bulk helpers ─────────────────────────────────────────────────────
    async def _gather_chunked(
        self, items: list, coro_fn, label: str = "",
    ) -> list[Any]:
        """Run *coro_fn* over *items* concurrently in rate-limited chunks.

        Returns results in the same order as *items*. Exceptions are returned
        in-place (gather with ``return_exceptions=True``) so callers decide how
        to handle per-item failures.
        """
        results: list[Any] = []
        total = len(items)
        if not total:
            return results
        total_chunks = (total - 1) // API_CHUNK_SIZE + 1
        for i in range(0, total, API_CHUNK_SIZE):
            chunk = items[i : i + API_CHUNK_SIZE]
            if label:
                chunk_num = i // API_CHUNK_SIZE + 1
                print(f"  [{self.account_name}] Fetching {label} chunk {chunk_num}/{total_chunks}...")
            tasks = [coro_fn(it) for it in chunk]
            res = await asyncio.gather(*tasks, return_exceptions=True)
            results.extend(res)
            if i + API_CHUNK_SIZE < total:
                await asyncio.sleep(API_CHUNK_DELAY)
        return results

    async def fetch_all_campaign_details(self, campaign_ids: list[str]) -> list[dict]:
        """Fetch full campaign objects concurrently in rate-limited chunks."""
        res = await self._gather_chunked(
            campaign_ids,
            lambda cid: self._get(f"/campaigns/{cid}"),
            label="campaign details",
        )
        out: list[dict] = []
        for cid, r in zip(campaign_ids, res):
            if isinstance(r, Exception):
                print(f"  [!] Campaign detail {cid}: {r}")
            elif r:
                out.append(r)
        return out

    async def fetch_campaign_accounts_map(self, campaign_ids: list[str]) -> dict[str, list[dict]]:
        """Return ``{campaign_id: [email-account dicts]}`` fetched concurrently."""
        res = await self._gather_chunked(
            campaign_ids,
            self.get_campaign_email_accounts,
            label="campaign email-accounts",
        )
        out: dict[str, list[dict]] = {}
        for cid, r in zip(campaign_ids, res):
            if isinstance(r, Exception):
                print(f"  [!] Accounts for campaign {cid}: {r}")
                out[cid] = []
            else:
                out[cid] = r if isinstance(r, list) else []
        return out

    async def fetch_campaign_analytics_map(self, campaign_ids: list[str]) -> dict[str, dict]:
        """Return ``{campaign_id: analytics dict}`` fetched concurrently."""
        res = await self._gather_chunked(
            campaign_ids,
            self.get_campaign_analytics,
            label="campaign analytics",
        )
        out: dict[str, dict] = {}
        for cid, r in zip(campaign_ids, res):
            if isinstance(r, Exception):
                print(f"  [!] Analytics for campaign {cid}: {r}")
                out[cid] = {}
            else:
                out[cid] = r if isinstance(r, dict) else {}
        return out

    async def fetch_all_warmup_stats(self, account_ids: list[str]) -> dict[str, Any]:
        """Return ``{account_id: warmup-stats}`` fetched concurrently (None on error)."""
        res = await self._gather_chunked(
            account_ids,
            self.get_warmup_stats,
            label="warmup stats",
        )
        out: dict[str, Any] = {}
        for aid, r in zip(account_ids, res):
            out[aid] = None if isinstance(r, Exception) else r
        return out

    async def fetch_all_account_details(self, raw_accounts: list[dict]) -> list[dict]:
        """Fetch detailed metadata for every account, in rate-limited chunks."""
        results: list[dict] = []
        total = len(raw_accounts)

        for i in range(0, total, API_CHUNK_SIZE):
            chunk = raw_accounts[i : i + API_CHUNK_SIZE]
            chunk_num = i // API_CHUNK_SIZE + 1
            total_chunks = (total - 1) // API_CHUNK_SIZE + 1
            print(f"  [{self.account_name}] Fetching account details chunk {chunk_num}/{total_chunks}...")

            tasks = [self.get_email_account(str(acc["id"])) for acc in chunk]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for j, res in enumerate(responses):
                if isinstance(res, Exception):
                    print(f"  [!] Could not fetch account {chunk[j].get('id')}: {res}")
                elif res:
                    results.append(res)

            if i + API_CHUNK_SIZE < total:
                await asyncio.sleep(API_CHUNK_DELAY)

        return results

    # ── pretty key mask (for logs) ───────────────────────────────────────
    @property
    def masked_key(self) -> str:
        k = self._api_key
        return f"{k[:4]}...{k[-4:]}" if len(k) > 8 else "****"
