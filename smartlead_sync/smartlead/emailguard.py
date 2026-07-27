"""EmailGuard API client — placement testing without Smartlead credits.

Why this exists: Smartlead's SmartDelivery tests are billed per account, so
placement truth (40% of every inbox's health score) was rationed — one client
had ~90 credits, another zero, and testing a Belardi Wong inbox required a
human to create a non-connected test in another account's UI and paste a seed
list by hand.

EmailGuard sells the same seed-list mechanic against a workspace-level quota
and exposes it over a REST API, so the whole loop can run unattended:
create test -> get seeds + filter phrase -> send from the real inbox via its
own Smartlead account -> poll -> record.

Contract verified live 2026-07-27 (the OpenAPI spec ships no response
examples, so every field name below was confirmed against real responses).
"""
from __future__ import annotations

import os

import httpx

BASE_URL = "https://app.emailguard.io/api/v1"


class EmailGuardError(RuntimeError):
    pass


class QuotaError(EmailGuardError):
    """Workspace is out of placement-test credits."""


def api_key() -> str:
    # NOTE: the env var is spelled EMAILGURAD in .env (typo kept deliberately —
    # renaming it would break Render until someone updates it there too).
    return os.getenv("EMAILGUARD_API_KEY") or os.getenv("EMAILGURAD", "")


class EmailGuardClient:
    def __init__(self, token: str | None = None) -> None:
        self._token = token or api_key()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "EmailGuardClient":
        self._client = httpx.AsyncClient(
            timeout=45,
            headers={"Authorization": f"Bearer {self._token}",
                     "Accept": "application/json",
                     "Content-Type": "application/json"},
        )
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def _json(self, method: str, path: str, **kw):
        assert self._client, "use `async with EmailGuardClient()`"
        resp = await self._client.request(method, f"{BASE_URL}{path}", **kw)
        if resp.status_code == 429 or "quota" in resp.text.lower() or "limit" in resp.text.lower():
            if resp.status_code >= 400:
                raise QuotaError(resp.text[:200])
        if resp.status_code >= 400:
            raise EmailGuardError(f"{method} {path} -> {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    # ── quota ────────────────────────────────────────────────────────────
    async def workspace(self) -> dict:
        """Remaining credits per feature. Check BEFORE creating anything —
        placement tests are the scarce resource and a trial workspace has
        single digits of them."""
        return (await self._json("GET", "/workspaces/current")).get("data", {})

    async def remaining_placement_tests(self) -> int:
        return int((await self.workspace()).get("remaining_inbox_placement_tests") or 0)

    # ── placement tests ──────────────────────────────────────────────────
    async def create_placement_test(self, name: str) -> dict:
        """Returns the test: uuid, filter_phrase (must appear in the email
        body), and inbox_placement_test_emails (the seed addresses to send to).
        Costs one placement-test credit."""
        data = (await self._json("POST", "/inbox-placement-tests",
                                 json={"name": name})).get("data", {})
        if not data.get("uuid"):
            raise EmailGuardError(f"no uuid in create response: {str(data)[:200]}")
        return data

    async def get_placement_test(self, uuid: str) -> dict:
        return (await self._json("GET", f"/inbox-placement-tests/{uuid}")).get("data", {})

    # ── free diagnostics (no credit cost) ────────────────────────────────
    async def surbl_check(self, domain: str) -> dict:
        """Independent SURBL verdict — a second opinion on our own DNS-based
        blacklist monitor. Returns {'listed': bool, ...}."""
        return (await self._json("POST", "/surbl-blacklist-checks",
                                 json={"domain": domain})).get("data", {})

    async def auth_lookup(self, domain: str, selector: str = "google") -> dict:
        """SPF/DKIM/DMARC validity from a second implementation — catches
        false-clean results in our own DNS audit."""
        out: dict = {}
        for kind, params in (("spf", {"domain": domain}),
                             ("dmarc", {"domain": domain}),
                             ("dkim", {"domain": domain, "selector": selector})):
            try:
                d = await self._json("GET", f"/email-authentication/{kind}-lookup", params=params)
                res = (d.get("data") or {}).get("results") or {}
                out[kind] = {"valid": bool(res.get("valid")), "errors": res.get("errors") or []}
            except EmailGuardError as exc:
                out[kind] = {"valid": None, "errors": [str(exc)[:120]]}
        return out


def summarize_placement(test: dict) -> dict:
    """Reduce a placement test to the numbers the pipeline stores.

    Seed rows carry `folder` (Inbox / Spam / …) and a `status`; `overall_score`
    is EmailGuard's own percentage. We recompute from the seed rows so a
    partially-delivered test is never scored as if it were complete — the same
    trap that made every Smartlead test read as a false FAIL before.
    """
    seeds = test.get("inbox_placement_test_emails") or []
    landed = [s for s in seeds if (s.get("folder") or "").strip()]
    inbox_n = sum(1 for s in landed if (s.get("folder") or "").lower() == "inbox")
    spam_n = sum(1 for s in landed if "spam" in (s.get("folder") or "").lower())
    total = len(landed)
    return {
        "uuid": test.get("uuid"),
        "name": test.get("name"),
        "status": test.get("status"),
        "delivered": total,
        "seeds": len(seeds),
        "complete": bool(test.get("completed_at")) or (total == len(seeds) and total > 0),
        "inbox_pct": round(100.0 * inbox_n / total, 1) if total else 0.0,
        "spam_pct": round(100.0 * spam_n / total, 1) if total else 0.0,
        "overall_score": test.get("overall_score"),
        "by_provider": {
            p: sum(1 for s in landed
                   if s.get("provider") == p and (s.get("folder") or "").lower() == "inbox")
            for p in {s.get("provider") for s in seeds if s.get("provider")}
        },
    }
