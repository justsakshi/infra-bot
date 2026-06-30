"""Async HTTP client for the HeyReach public API."""
from __future__ import annotations

import asyncio
from typing import Any

import httpx

from smartlead.config import (
    HEYREACH_BASE_URL, API_TIMEOUT,
    API_MAX_RETRIES, API_RETRY_BASE_DELAY, API_RETRY_MAX_DELAY,
)

_PAGE = 100


class HeyReachClient:
    """Thin async wrapper around the HeyReach public API (X-API-KEY auth)."""

    def __init__(self, api_key: str, workspace_name: str = "Default") -> None:
        self._api_key = api_key
        self.workspace_name = workspace_name
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "HeyReachClient":
        self._client = httpx.AsyncClient(
            timeout=API_TIMEOUT,
            headers={"X-API-KEY": self._api_key, "Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _post(self, path: str, body: dict) -> Any:
        assert self._client, "Use `async with HeyReachClient(...)`."
        url = f"{HEYREACH_BASE_URL}{path}"
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                resp = await self._client.post(url, json=body)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [HR {self.workspace_name}] network error {path}: {exc!r} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                raise
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    ra = resp.headers.get("Retry-After")
                    try:
                        delay = min(float(ra), API_RETRY_MAX_DELAY) if ra else min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    except ValueError:
                        delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [HR {self.workspace_name}] {resp.status_code} {path} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    async def _paginate(self, path: str, base_body: dict) -> list[dict]:
        out: list[dict] = []
        offset = 0
        while True:
            body = {**base_body, "offset": offset, "limit": _PAGE}
            data = await self._post(path, body)
            items = data.get("items", []) if isinstance(data, dict) else []
            out.extend(items)
            if len(items) < _PAGE:
                break
            offset += _PAGE
        return out

    async def list_campaigns(self) -> list[dict]:
        return await self._paginate("/campaign/GetAll", {})

    async def get_overall_stats(self, campaign_id: int, start: str | None = None, end: str | None = None) -> dict:
        return await self._post("/stats/GetOverallStats", {
            "campaignIds": [campaign_id], "accountIds": [], "startDate": start, "endDate": end,
        })

    async def get_campaign_leads(self, campaign_id: int) -> list[dict]:
        return await self._paginate("/campaign/GetLeadsFromCampaign", {"campaignId": campaign_id})

    @property
    def masked_key(self) -> str:
        k = self._api_key
        return f"{k[:4]}...{k[-4:]}" if len(k) > 8 else "****"
