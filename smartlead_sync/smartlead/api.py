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
