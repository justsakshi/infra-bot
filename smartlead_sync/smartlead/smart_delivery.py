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
                          is_warmup: bool = True, no_time_gap: bool = False) -> int:
        body = {
            "test_name": test_name,
            "description": f"auto placement test — {test_name}",
            "campaign_id": int(campaign_id),
            "sequence_mapping_id": int(sequence_mapping_id),
            "sender_accounts": sender_emails,
            "provider_ids": [20, 21],
            "spam_filters": ["spam_assassin"],
            "link_checker": True,
            "is_warmup": is_warmup,
        }
        if no_time_gap:
            # Sending `min_time_unit` alongside no-gap is a 400
            # ("min_time_unit is not allowed", 2026-09-14).
            body["all_email_sent_without_time_gap"] = True
        else:
            body["all_email_sent_without_time_gap"] = False
            body["min_time_btwn_emails"] = RETEST_MIN_TIME_MINUTES
            body["min_time_unit"] = "minutes"
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

    async def stop_test(self, test_id: int) -> bool:
        """Cancel a running test. PUT — not POST or DELETE, which both 404
        (Smartlead support, 2026-09-15). Returns False rather than raising:
        stopping is a best-effort courtesy, never a reason to fail a run."""
        resp = await self._client.put(self._url(f"/spam-test/{test_id}/stop"))
        if resp.status_code >= 400:
            print(f"  [SmartDelivery] stop {test_id} failed "
                  f"{resp.status_code}: {resp.text[:120]}")
            return False
        return True

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
        return summarize_report(resp.json())

def summarize_report(d: dict) -> dict:
    """Turn a providerwise payload into a verdict, plus how much of the seed
    panel it is based on.

    Real payload shape (verified live on test 475859, 2026-07-09):
      {"overallTotalCount": 60, "status": "COMPLETED", "result": [
        {"provider_name": "Office365", "inbox_count": 35, "spam_count": 0,
         "tab_count": 0, "adjusted_total_email_count": 35}, ...]}
    The old parser read d["data"] rows with "inbox"/"spam" percentage fields —
    neither exists, so every completed test scored 0%/0% and would have been
    written to the sheet as a FALSE FAIL.

    `has_data` exists because of a second, distinct way to get a false fail: on
    2026-09-11 a batch of 15 concurrent tests was flipped to COMPLETED while the
    seed classifications were still arriving, so `result` came back empty
    against overallTotalCount=15. Scoring that as 0% inbox is indistinguishable
    from a domain whose every seed landed in spam. Callers must check
    `has_data` and treat a no-data report as "not measured yet", never as a
    failure.
    """
    rows = [r for r in (d.get("result") or d.get("data")
                        or (d if isinstance(d, list) else [])) if isinstance(r, dict)]
    total = sum(float(r.get("adjusted_total_email_count", 0) or 0) for r in rows)
    inbox_n = sum(float(r.get("inbox_count", 0) or 0) for r in rows)
    spam_n = sum(float(r.get("spam_count", 0) or 0) for r in rows)
    inbox_pct = (100.0 * inbox_n / total) if total else 0.0
    spam_pct = (100.0 * spam_n / total) if total else 0.0

    # Per-provider breakdown. The blended figure above hides the failure
    # mode we actually have: on 2026-07-27 a domain scored 50% overall,
    # which was 100% at Google and 0% at Microsoft. Averaging those made a
    # working domain and a dead one look identical, and nearly cost us two
    # healthy domains that were queued for retirement on that basis.
    by_provider: dict[str, dict] = {}
    for r in rows:
        name = str(r.get("provider_name") or "unknown")
        p_total = float(r.get("adjusted_total_email_count", 0) or 0)
        p_inbox = float(r.get("inbox_count", 0) or 0)
        p_spam = float(r.get("spam_count", 0) or 0)
        by_provider[name] = {
            "inbox_pct": round(100.0 * p_inbox / p_total, 1) if p_total else 0.0,
            "spam_pct": round(100.0 * p_spam / p_total, 1) if p_total else 0.0,
            "inbox": int(p_inbox), "spam": int(p_spam), "total": int(p_total),
        }
    # Worst provider drives the verdict: a domain that reaches Google but
    # not Microsoft is not healthy, it is half-dead, and should be treated
    # that way rather than passing on a blended average.
    worst = min((v["inbox_pct"] for v in by_provider.values()), default=inbox_pct)

    classified = int(total)
    dispatched = int(d.get("overallTotalCount") or 0) or classified
    return {"inbox_pct": inbox_pct, "spam_pct": spam_pct,
            "by_provider": by_provider, "worst_provider_inbox_pct": worst,
            "classified": classified, "dispatched": dispatched,
            "coverage": (classified / dispatched) if dispatched else 0.0,
            "has_data": classified > 0}
