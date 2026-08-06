"""Async HTTP client for the Expandi (Liaufa) open API.

Two things about this API are easy to get wrong, both learned the hard way:

  * The base path is `/api/v1/open-api/v2`. Probing `/api/v1` returns 401 on
    every endpoint, which reads as bad credentials but is really a wrong URL.

  * The published OpenAPI spec under-describes the campaign response. It lists
    only id/created/updated/name/li_account, which suggests the API exposes no
    statistics at all. The live response also carries a populated `stats`
    object per campaign — enough to build the metrics row from this one
    endpoint. Do not trust the spec's response schemas here.

Auth is `key` + `secret` headers on every request; there is no login step.
"""
from __future__ import annotations

import asyncio
from typing import Any

import httpx

from smartlead.config import (
    EXPANDI_BASE_URL, API_TIMEOUT,
    API_MAX_RETRIES, API_RETRY_BASE_DELAY, API_RETRY_MAX_DELAY,
)


class ExpandiClient:
    """Thin async wrapper around the Expandi/Liaufa open API (key+secret auth)."""

    def __init__(self, api_key: str, api_secret: str, workspace_name: str = "Default") -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self.workspace_name = workspace_name
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "ExpandiClient":
        self._client = httpx.AsyncClient(
            timeout=API_TIMEOUT,
            headers={
                "key": self._api_key,
                "secret": self._api_secret,
                "Content-Type": "application/json",
            },
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _get(self, url: str) -> Any:
        """GET with the same retry policy as the other clients.

        `url` is absolute so this can follow the `next` link straight from a
        paginated response without re-deriving the path.
        """
        assert self._client, "Use `async with ExpandiClient(...)`."
        for attempt in range(API_MAX_RETRIES + 1):
            try:
                resp = await self._client.get(url)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < API_MAX_RETRIES:
                    delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [EX {self.workspace_name}] network error: {exc!r} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
                raise
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt < API_MAX_RETRIES:
                    ra = resp.headers.get("Retry-After")
                    try:
                        delay = min(float(ra), API_RETRY_MAX_DELAY) if ra else min(
                            API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    except ValueError:
                        delay = min(API_RETRY_BASE_DELAY * (2 ** attempt), API_RETRY_MAX_DELAY)
                    print(f"  [EX {self.workspace_name}] {resp.status_code} - retry in {delay:.0f}s")
                    await asyncio.sleep(delay)
                    continue
            resp.raise_for_status()
            return resp.json()
        resp.raise_for_status()

    async def _paginate(self, path: str) -> list[dict]:
        """Collect every page. `count` is the total, NOT the page size — one
        live account returns 14 campaigns across 2 pages, so stopping after the
        first response silently drops most of the data."""
        out: list[dict] = []
        url = f"{EXPANDI_BASE_URL}{path}"
        seen = 0
        while url:
            data = await self._get(url)
            if not isinstance(data, dict):
                break
            out.extend(data.get("results") or [])
            url = data.get("next")
            seen += 1
            if seen > 100:  # runaway guard; no real account has 100 pages
                print(f"  [EX {self.workspace_name}] pagination exceeded 100 pages — stopping")
                break
        return out

    async def list_li_accounts(self) -> list[dict]:
        """LinkedIn accounts this key can see."""
        return await self._paginate("/li_accounts/")

    async def list_campaigns(self, li_account_id: int) -> list[dict]:
        """Campaigns for one LinkedIn account, each carrying a `stats` dict."""
        return await self._paginate(f"/li_accounts/{li_account_id}/campaign_instances/")

    async def pause_contact(self, campaign_instance_id: int, profile_link: str) -> dict:
        """Stop a campaign from progressing one contact.

        `active: false` rather than delete_contact: deleting loses the record of
        them having been in the campaign, and a later list refresh can re-add
        them, restarting the sequence this call exists to stop.
        """
        return await self._patch_contact(campaign_instance_id, profile_link, active=False)

    async def resume_contact(self, campaign_instance_id: int, profile_link: str) -> dict:
        return await self._patch_contact(campaign_instance_id, profile_link, active=True)

    async def _patch_contact(self, campaign_instance_id: int, profile_link: str,
                             active: bool) -> dict:
        assert self._client, "Use `async with ExpandiClient(...)`."
        url = (f"{EXPANDI_BASE_URL}/li_accounts/campaign_instances/"
               f"{campaign_instance_id}/update_contact/")
        resp = await self._client.patch(
            url, json={"profile_link": profile_link, "active": active})
        resp.raise_for_status()
        return resp.json() if resp.content else {}

    @property
    def masked_key(self) -> str:
        k = self._api_key
        return f"{k[:4]}...{k[-4:]}" if len(k) > 8 else "****"
