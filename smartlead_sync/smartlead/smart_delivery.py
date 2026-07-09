"""Async client for Smartlead SmartDelivery placement tests."""
from __future__ import annotations

import httpx

from smartlead.config import SMARTDELIVERY_BASE_URL, RETEST_MIN_TIME_MINUTES

_CREDIT_HINTS = ("credit", "upgrade", "payment", "subscription", "plan", "not enabled")


class SmartDeliveryError(Exception):
    pass


class CreditError(SmartDeliveryError):
    pass


class SmartDeliveryClient:
    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "SmartDeliveryClient":
        self._client = httpx.AsyncClient(timeout=30)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _url(self, path: str) -> str:
        sep = "&" if "?" in path else "?"
        return f"{SMARTDELIVERY_BASE_URL}{path}{sep}api_key={self._api_key}"

    async def create_test(self, campaign_id: int, sequence_mapping_id: int,
                          sender_emails: list[str], test_name: str,
                          is_warmup: bool = True) -> int:
        body = {
            "test_name": test_name,
            "description": f"auto placement test — {test_name}",
            "campaign_id": int(campaign_id),
            "sequence_mapping_id": int(sequence_mapping_id),
            "sender_accounts": sender_emails,
            "provider_ids": [20, 21],
            "spam_filters": ["spam_assassin"],
            "link_checker": True,
            "all_email_sent_without_time_gap": False,
            "min_time_btwn_emails": RETEST_MIN_TIME_MINUTES,
            "min_time_unit": "minutes",
            "is_warmup": is_warmup,
        }
        resp = await self._client.post(self._url("/spam-test/manual"),
                                       headers={"Content-Type": "application/json"}, json=body)
        if resp.status_code >= 400:
            text = resp.text.lower()
            if resp.status_code == 402 or any(h in text for h in _CREDIT_HINTS):
                raise CreditError(resp.text[:200])
            raise SmartDeliveryError(f"create failed {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        tid = data.get("id") or data.get("spamTestId")
        if not tid:
            raise SmartDeliveryError(f"no test id in response: {data}")
        return int(tid)

    async def poll_test(self, test_id: int) -> dict:
        resp = await self._client.get(self._url(f"/spam-test/{test_id}"))
        if resp.status_code >= 400:
            raise SmartDeliveryError(f"poll failed {resp.status_code}: {resp.text[:150]}")
        d = resp.json()
        status = d.get("status", "")
        done = bool(d.get("test_end_date")) or (status and status != "ACTIVE")
        return {"status": status, "done": done, "end_date": d.get("test_end_date")}

    async def get_report(self, test_id: int) -> dict:
        resp = await self._client.post(
            self._url(f"/spam-test/report/{test_id}/providerwise"),
            headers={"Content-Type": "application/json"}, json={},
        )
        if resp.status_code >= 400:
            raise SmartDeliveryError(f"report failed {resp.status_code}: {resp.text[:150]}")
        d = resp.json()
        # Real payload shape (verified live on test 475859, 2026-07-09):
        #   {"overallTotalCount": 60, "status": "COMPLETED", "result": [
        #     {"provider_name": "Office365", "inbox_count": 35, "spam_count": 0,
        #      "tab_count": 0, "adjusted_total_email_count": 35}, ...]}
        # The old parser read d["data"] rows with "inbox"/"spam" percentage
        # fields — neither exists, so every completed test scored 0%/0% and
        # would have been written to the sheet as a FALSE FAIL.
        rows = d.get("result") or d.get("data") or (d if isinstance(d, list) else [])
        total = sum(float(r.get("adjusted_total_email_count", 0) or 0)
                    for r in rows if isinstance(r, dict))
        inbox_n = sum(float(r.get("inbox_count", 0) or 0)
                      for r in rows if isinstance(r, dict))
        spam_n = sum(float(r.get("spam_count", 0) or 0)
                     for r in rows if isinstance(r, dict))
        inbox_pct = (100.0 * inbox_n / total) if total else 0.0
        spam_pct = (100.0 * spam_n / total) if total else 0.0
        return {"inbox_pct": inbox_pct, "spam_pct": spam_pct}
