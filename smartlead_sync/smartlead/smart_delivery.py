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

    async def get_mailbox_summary(self) -> list[dict]:
        """Every mailbox's placement across all tests, account-wide.

        Not tied to a test id — this is the rolled-up history, which makes it
        the cheapest way to spot a mailbox drifting without spending a credit.
        Percentages arrive as strings ("72.41%"); they are parsed to floats
        here so callers never have to strip a percent sign.
        """
        resp = await self._client.get(self._url("/spam-test/report/mailboxes-summary"))
        if resp.status_code >= 400:
            raise SmartDeliveryError(
                f"mailbox summary failed {resp.status_code}: {resp.text[:150]}")
        data = resp.json()
        rows = data if isinstance(data, list) else data.get("data", [])
        out = []
        for row in rows:
            pct = row.get("percentages") or {}

            def _num(key: str) -> float:
                try:
                    return float(str(pct.get(key, "0")).rstrip("%"))
                except ValueError:
                    return 0.0

            out.append({
                "email": row.get("from_email", ""),
                "esp": row.get("esp", ""),
                "inbox_pct": _num("inbox"), "spam_pct": _num("spam"),
                "tab_pct": _num("tab"),
                "dkim_pass_pct": _num("dkim_pass"), "spf_pass_pct": _num("spf_pass"),
                "tests": int(row.get("placement_count", 0) or 0),
                "total": int(row.get("adjusted_total_count", 0) or 0),
                # Raw counts as well as percentages. A ratio makes the weight
                # of evidence visible — 47% off 46 seeds is a finding, 47% off
                # 2 seeds is noise, and the percentage alone cannot tell them
                # apart.
                "inbox": int(row.get("inbox_count", 0) or 0),
                "spam": int(row.get("spam_count", 0) or 0),
                "tab": int(row.get("tab_count", 0) or 0),
            })
        return out

    async def get_sender_report(self, test_id: int) -> dict[str, dict]:
        """Per-sender placement: {sender email: summary}.

        `/providerwise` blends every sender in a test into one pair of provider
        totals, which is why we ran one test per domain. This endpoint breaks
        the same test down per sender mailbox, per seed, with the folder each
        landed in — so a single test can carry many domains.
        """
        from smartlead.sender_report import summarize_senders

        resp = await self._client.get(
            self._url(f"/spam-test/report/{test_id}/sender-account-wise"))
        if resp.status_code >= 400:
            raise SmartDeliveryError(
                f"sender report failed {resp.status_code}: {resp.text[:150]}")
        return summarize_senders(resp.json())

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


UNJUDGED = "unjudged"       # no result at the senders' own provider: never written as a verdict


def own_judgement(report: dict, kinds, threshold: float, sender: dict | None = None) -> dict:
    """What a classic test says about its sender(s), judged at their OWN provider (smartlead/own_provider.py; every
    campaign is ESP-matched, so Google senders are judged on the Google seeds and Outlook senders on the Microsoft
    seeds, 17 Nov 2026). Returns {"status": "inbox" | "fail" | UNJUDGED, "pct": float | None, "note": str}.

    ``report``: summarize_report (test-wide, per provider). ``kinds``: the Smartlead types of the test's senders.
    ``sender``: one sender's sender_report summary when the test had several (no provider split per sender).

    * Senders of mixed, SMTP or unknown type: the worst provider (or the sender's whole panel), as before.
    * One sender (or none given): its own provider's inbox %. The other provider below ``threshold`` is a signal in
      ``note``. No own-provider seeds: UNJUDGED.
    * Several senders: a sender at or above ``threshold`` over its whole panel passes. Below it, it fails only when
      the test's own-provider seeds were below ``threshold`` too; otherwise the spam was at the other provider and
      it is UNJUDGED (the panel has no per-sender split to say more). Pure."""
    from smartlead import own_provider as op
    own = op.own_provider(op.common_kind(kinds))
    provs = report.get("by_provider") or {}
    if own is None:
        pct = sender["inbox_pct"] if sender else report.get("worst_provider_inbox_pct", report.get("inbox_pct", 0.0))
        return {"status": "inbox" if pct >= threshold else "fail", "pct": pct, "note": ""}
    mine, other = op.split(provs, op.common_kind(kinds))
    mine = {p: v for p, v in mine.items() if int(v.get("total") or 0)}
    own_pct = min((float(v["inbox_pct"]) for v in mine.values()), default=None)
    note = "; ".join(op.signal(p, f"{float(v['inbox_pct']):.0f}% inbox") for p, v in sorted(other.items())
                     if int(v.get("total") or 0) and float(v["inbox_pct"]) < threshold)
    name = op.NAMES[own]
    if sender is not None:
        if sender["inbox_pct"] >= threshold:
            return {"status": "inbox", "pct": sender["inbox_pct"], "note": note}
        if own_pct is not None and own_pct < threshold:
            return {"status": "fail", "pct": sender["inbox_pct"], "note": note}
        why = (f"{sender['inbox_pct']:.0f}% over both providers, but the test's {name} seeds were "
               f"{'not measured' if own_pct is None else f'{own_pct:.0f}% inbox'}: not judged at its own provider")
        return {"status": UNJUDGED, "pct": None, "note": "; ".join(x for x in (why, note) if x)}
    if own_pct is None:
        return {"status": UNJUDGED, "pct": None,
                "note": "; ".join(x for x in (f"no {name} seeds measured: not judged at its own provider", note) if x)}
    return {"status": "inbox" if own_pct >= threshold else "fail", "pct": own_pct, "note": note}
