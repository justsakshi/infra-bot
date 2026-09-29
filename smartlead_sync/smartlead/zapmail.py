"""Async HTTP client for the Zapmail vault API.

Zapmail is the domain + mailbox provider for the sending fleet. This module is
the single place the tools talk to `https://api.zapmail.ai/api`, so every
credential, header, rate-limit, and spend decision lives in one file.

The design rule that matters most:

    **Read-only calls are free. Spend and write calls are armed.**

Every method here is either

  * **READ-ONLY** — pure retrieval, no side effect, no cost; or
  * **WRITE / SPEND** — mutates the account and/or costs wallet balance or
    credits. These refuse to run unless the caller passes an explicit
    ``approve=True``, and the Slack layer only sets that after a human confirms
    the exact payload. A cold-email operator must never burn money because a
    script guessed.

SPEND is distinguished from WRITE because the operator's guardrail is about
money, not side effects: connecting a domain or adding a DMARC record is free
but still mutating, so it is also gated — just reported as a different class.

``approve`` is a per-call keyword, not mutable client state, so an approval
handed to one operation can never leak into the next.

See ``docs/ZAPMAIL_API_REFERENCE.md`` for the full endpoint map and the
spend/read-only classification of everything Zapmail exposes.
"""

from __future__ import annotations

import asyncio
import os
from typing import Any

import httpx

ZAPMAIL_BASE_URL = "https://api.zapmail.ai/api"

# ── Rate limits (from Zapmail's docs) ─────────────────────────────────────────
#   General requests:        5 rps, 200 rpm
#   Domain search:           100 requests / 30 min (general) — BUT the
#                            bulk-availability endpoint states 10 requests /
#                            30 min per client (cover up to 20 names/request).
#   Re-export mailboxes:     3 requests / mailbox / week
# The domain-search pacing itself lives in domain_availability.py, not here —
# this client just exposes the calls.

# ── TLDs Zapmail will actually register ───────────────────────────────────────
# The single-name /available and /available-bulk endpoints only allow these.
# The AI finder can return other TLDs (.io, .co, ...), so its results must be
# filtered to this set before they reach a purchase plan.
REGISTRABLE_TLDS: tuple[str, ...] = ("com", "net", "org", "biz", "live", "info")

# ── Mailbox providers ─────────────────────────────────────────────────────────
# Verified live 2026-09-28: list/renewal/mailbox endpoints answer for GOOGLE
# only unless the request carries `x-service-provider: MICROSOFT`. Anything
# that means "the whole fleet" must query both.
PROVIDERS: tuple[str, ...] = ("GOOGLE", "MICROSOFT")

# ── Third-party apps mailboxes can be exported to ─────────────────────────────
EXPORT_APPS: frozenset[str] = frozenset({
    "SMARTLEAD", "INSTANTLY", "REACHINBOX", "REPLY_IO", "QUICKMAIL", "EMELIA",
    "FIRSTQUADRANT", "WARMY", "SUPERAGI", "PIPL", "LUELLA", "MASTER_INBOX",
    "RECRUITERFLOW", "SAILE", "SNOV", "MAILTOASTER", "MANYREACH", "COLDSTATS",
    "BRANDJET", "HOTHAWK", "MAILIVERY", "EMAILGUARD", "UNIPILE", "APOLLO",
    "TRULYINBOX", "EMAILBISON", "VENTA", "LEMLIST",
})


class ZapmailError(RuntimeError):
    """Base for Zapmail client failures."""


class ZapmailSpendBlocked(ZapmailError):
    """A WRITE/SPEND call was attempted without explicit approval.

    Raised by design — see the module docstring. The only way through is the
    caller deliberately passing ``approve=True`` (and, for SPEND, the operator
    flipping ``ZAPMAIL_ALLOW_SPEND=true``).
    """


class ZapmailHTTPError(ZapmailError):
    """Zapmail answered with a definitive 4xx: the request was NOT processed."""

    def __init__(self, message: str, status_code: int) -> None:
        super().__init__(message)
        self.status_code = status_code


class ZapmailRateLimited(ZapmailHTTPError):
    """Zapmail returned 429; the caller should stop spending its budget."""


class ZapmailOutcomeUnknown(ZapmailError):
    """The request may or may not have been processed.

    Raised for network errors, timeouts, and 5xx answers on the final attempt.
    For a SPEND call this means "you might have been charged" — reconcile with
    a read (e.g. ``list_domains``) before trying again, never blindly retry.
    """


def spend_allowed() -> bool:
    """True only when the operator has flipped the spend kill-switch on.

    Deliberately a separate env var rather than anything a code path can flip
    for itself — the default is off everywhere (local, CI, Render). Every SPEND
    method in :class:`ZapmailClient` checks it, so no caller can skip it.
    """
    return os.getenv("ZAPMAIL_ALLOW_SPEND", "").strip().lower() == "true"


def zapmail_api_key() -> str | None:
    return os.getenv("ZAPMAIL_API_KEY") or None


def zapmail_workspace_key() -> str | None:
    """Optional workspace scope. Blank => primary workspace.

    Include ``x-workspace-key`` only when operating outside the primary
    workspace; most single-account work omits it.
    """
    return os.getenv("ZAPMAIL_WORKSPACE_KEY") or None


def zapmail_service_provider() -> str:
    """``GOOGLE`` or ``MICROSOFT`` for endpoints that require the header.

    Defaults to GOOGLE. Only sent on endpoints that need it (or when a client
    is built with an explicit provider), so Microsoft mailboxes are never
    filtered out of general list calls by a stray header.
    """
    return (os.getenv("ZAPMAIL_SERVICE_PROVIDER") or "GOOGLE").upper()


class ZapmailClient:
    """Thin async wrapper around the Zapmail v2 API.

    Usage::

        async with ZapmailClient() as z:
            me = await z.get_user()
            avail = await z.check_availability_bulk(["foo.com", "bar.com"])
            # spend requires an explicit, deliberate approval:
            await z.buy_domains(["foo.com"], approve=True)
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        workspace_key: str | None = None,
        service_provider: str | None = None,
        timeout: float = 60.0,
    ) -> None:
        self._api_key = api_key if api_key is not None else zapmail_api_key()
        # NB: workspace_key/service_provider distinguish "explicitly passed
        # None" from "unset -> use default". Defaults are resolved lazily so a
        # client built before dotenv loads still sees the env.
        self._workspace_key = workspace_key
        self._service_provider = service_provider
        self._timeout = timeout
        self._client: httpx.AsyncClient | None = None

    # ── context manager ──────────────────────────────────────────────────
    async def __aenter__(self) -> "ZapmailClient":
        self._client = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(self, *exc: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    # ── header / identity helpers ────────────────────────────────────────
    def _headers(self, *, service_provider: bool = False) -> dict[str, str]:
        if not self._api_key:
            raise ZapmailError("ZAPMAIL_API_KEY is not set")
        headers = {"x-auth-zapmail": self._api_key}
        wk = self._workspace_key if self._workspace_key is not None else zapmail_workspace_key()
        if wk:
            headers["x-workspace-key"] = wk
        # Explicit per-client provider always wins; otherwise the header is
        # only sent on the endpoints Zapmail documents as needing it.
        sp = self._service_provider or (
            zapmail_service_provider() if service_provider else None)
        if sp:
            headers["x-service-provider"] = sp.upper()
        return headers

    @property
    def masked_key(self) -> str:
        k = self._api_key or ""
        return f"{k[:4]}...{k[-4:]}" if len(k) > 8 else "****"

    @staticmethod
    def _require(approve: bool, label: str, kind: str) -> None:
        if not approve:
            raise ZapmailSpendBlocked(
                f"{label} is a {kind} operation. Pass approve=True only after a "
                "human has explicitly confirmed the exact payload."
            )
        if kind == "SPEND" and not spend_allowed():
            raise ZapmailSpendBlocked(
                f"{label} spends money and ZAPMAIL_ALLOW_SPEND is not 'true' "
                "(it is off by default everywhere)."
            )

    # ── low-level request ─────────────────────────────────────────────────
    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        json: dict | None = None,
        service_provider: bool = False,
        retry: bool = True,
    ) -> Any:
        """One request with retry on 429/5xx + transient network errors.

        ``retry`` is deliberately the caller's choice: read-only calls retry
        freely, but SPEND calls must NOT be auto-retried — a timeout after an
        actual purchase would bill twice, so those are single-shot and let
        the caller reconcile via a follow-up read.

        Failure classes: a 4xx raises :class:`ZapmailHTTPError` (definitely
        not processed); a network error, timeout, or final 5xx raises
        :class:`ZapmailOutcomeUnknown` (may have been processed).
        """
        assert self._client, "Use `async with ZapmailClient(...)` as a context manager."
        headers = self._headers(service_provider=service_provider)
        max_attempts = 4 if retry else 1
        last_resp: httpx.Response | None = None
        for attempt in range(max_attempts):
            try:
                resp = await self._client.request(
                    method, f"{ZAPMAIL_BASE_URL}{path}",
                    params=params, json=json, headers=headers,
                )
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if retry and attempt < max_attempts - 1:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
                raise ZapmailOutcomeUnknown(
                    f"network error on {path}: {exc!r}") from exc
            last_resp = resp
            if resp.status_code == 429:
                if retry and attempt < max_attempts - 1:
                    await asyncio.sleep(min(2 ** attempt * 2, 30))
                    continue
                raise ZapmailRateLimited(f"429 on {path}: {resp.text[:200]}", 429)
            if resp.status_code >= 500:
                if retry and attempt < max_attempts - 1:
                    await asyncio.sleep(min(2 ** attempt, 8))
                    continue
                raise ZapmailOutcomeUnknown(
                    f"{resp.status_code} on {path}: {resp.text[:300]}")
            try:
                resp.raise_for_status()
            except httpx.HTTPStatusError as exc:
                raise ZapmailHTTPError(
                    f"{resp.status_code} on {path}: {resp.text[:300]}",
                    resp.status_code) from exc
            if not resp.content:
                return None
            try:
                return resp.json()
            except ValueError as exc:
                raise ZapmailError(f"non-JSON response on {path}: {resp.text[:200]}") from exc
        # Unreachable (loop always returns/raises), but keep the type checker calm.
        raise ZapmailError(f"request failed on {path}: {getattr(last_resp, 'status_code', '?')}")

    # ── READ-ONLY (free) ──────────────────────────────────────────────────

    async def get_user(self) -> dict:
        """Authenticated user: plan, mailbox usage, wallet balance. READ-ONLY."""
        return await self._request("GET", "/v2/users")

    async def get_wallet_balance(self) -> dict:
        """``{walletBalance, autoRechargeEnabled, ...}``. READ-ONLY."""
        return await self._request("GET", "/v2/wallet/balance")

    async def list_domains(
        self,
        *,
        contains: str | None = None,
        page: int | None = None,
        limit: int | None = None,
        status: list[str] | None = None,
        tag_ids: list[str] | None = None,
    ) -> dict:
        """Domains in the workspace. READ-ONLY.

        GET /v2/domains for the simple case; POST /v2/domains when a filter
        body (status/tagIds) is needed, mirroring Zapmail's two endpoints.
        """
        if status or tag_ids:
            body: dict = _pruned({"status": status, "tagIds": tag_ids})
            return await self._request("POST", "/v2/domains", json=body)
        params = {"contains": contains, "page": page, "limit": limit}
        return await self._request("GET", "/v2/domains", params=_pruned(params))

    async def list_assignable_domains(
        self, *, contains: str | None = None, page: int | None = None,
        limit: int | None = None,
    ) -> dict:
        """Domains mailboxes can currently be assigned to. READ-ONLY."""
        params = {"contains": contains, "page": page, "limit": limit}
        return await self._request("GET", "/v2/domains/assignable", params=_pruned(params))

    async def list_mailboxes(
        self, *, page: int | None = None, limit: int | None = None,
        contains: str | None = None,
    ) -> dict:
        """All mailboxes + quota counters (purchased/available/scheduled). READ-ONLY."""
        params = {"page": page, "limit": limit, "contains": contains}
        return await self._request("GET", "/v2/mailboxes/list", params=_pruned(params))

    async def list_tags(self) -> list[dict]:
        """Workspace domain tags (``[{id, name, tagColor}]``). READ-ONLY."""
        return await self._request("GET", "/v2/domains/tags")

    async def get_dns_records(self, domain_id: str) -> dict:
        """DNS records currently on a domain (``{records, disabledRecords}``). READ-ONLY."""
        return await self._request("GET", "/v2/dns/", params={"id": domain_id})

    async def list_connection_requests(
        self, *, page: int | None = None, limit: int | None = None,
        status: str | None = None, contains: str | None = None,
    ) -> dict:
        """Domains still pending connection, with per-domain status. READ-ONLY."""
        params = {"page": page, "limit": limit, "status": status, "contains": contains}
        return await self._request(
            "GET", "/v2/domains/connection-requests", params=_pruned(params))

    async def list_renewal_soon(
        self, *, contains: str | None = None, tag_ids: list[str] | None = None,
        page: int | None = None, limit: int | None = None,
    ) -> dict:
        """Domains expiring within 2 months (excludes aged/pre-warmed). READ-ONLY.

        Null filters must be omitted, not sent: Zapmail answers
        ``422 Invalid value`` to ``{"contains": null, "tagIds": null}``.
        """
        return await self._request(
            "POST", "/v2/domains/renewal-soon",
            params=_pruned({"page": page, "limit": limit}),
            json=_pruned({"contains": contains, "tagIds": tag_ids}))

    async def get_renewal_price(
        self, *, domain_ids: list[str] | None = None,
        contains: str | None = None, tag_ids: list[str] | None = None,
    ) -> dict:
        """Renewal price for eligible domains. READ-ONLY."""
        return await self._request(
            "POST", "/v2/domains/get-renewal-price",
            json=_pruned({"domainIds": domain_ids, "contains": contains,
                          "tagIds": tag_ids}))

    async def check_availability_bulk(self, names: list[str]) -> dict:
        """Authoritative availability + price for up to 20 names in one call. READ-ONLY.

        Unlike the single-name /available endpoint (which answers with a
        suggestion list and a nullable ``exactMatch``), /available-bulk returns
        one clean row per name with ``status``, ``isPremiumDomain``,
        ``domainPrice``, and ``renewPrice``. That is the ground truth the
        suggester should buy on.
        """
        return await self._request(
            "POST", "/v2/domains/available-bulk",
            json={"domainNames": list(names)})

    async def get_domain_health(self, domain_id: str) -> dict:
        """Nameserver reputation score for a domain. READ-ONLY."""
        return await self._request(
            "GET", "/v2/domains/health-score", params={"domainId": domain_id})

    async def ai_domain_finder(
        self,
        keywords: list[str],
        tlds: list[str],
        desired_count: int,
        *,
        poll_interval_s: float = 5.0,
        max_polls: int = 12,
    ) -> dict:
        """AI-generated available domains from keywords. READ-ONLY (but needs provider header).

        First call starts generation; subsequent calls return progress. Zapmail
        caches the result for 5 minutes, so a rapid re-call returns the same
        batch rather than regenerating.

        Results may include TLDs Zapmail cannot register (``.io``, ``.co``, ...);
        callers must filter through :data:`REGISTRABLE_TLDS` before a buy plan.
        """
        body = {"keywords": keywords, "tlds": tlds, "desiredCount": desired_count}
        data = await self._request(
            "POST", "/v2/domains/ai-finder", json=body, service_provider=True)
        if data is None:
            raise ZapmailError("ai-finder returned no body")
        inner = data.get("data") or {}
        for _ in range(max_polls):
            if inner.get("status") == "completed" or inner.get("progress", 0) >= 100:
                break
            await asyncio.sleep(poll_interval_s)
            nxt = await self._request(
                "POST", "/v2/domains/ai-finder", json=body, service_provider=True)
            inner = (nxt or {}).get("data") or inner
        return {"_raw": data, **inner}

    # ── WRITE (free but mutating) — still approval-gated ──────────────────

    async def connect_domains(self, domain_names: list[str], *, approve: bool = False) -> dict:
        """Request connection of already-registered domains. WRITE (free).

        Domains must have their NS pointed at Zapmail (pns61-64.cloudns.*)
        before this call. Asynchronous (support, 2026-09-29): re-calling with
        the same names polls it — 207 = still in progress, 200 = every name
        final. Final: SUCCESS, or NS_NOT_CHANGED, WORKSPACE_ALREADY_EXISTS,
        DOMAIN_NOT_REGISTERED, BLACKLISTED_DOMAIN, BANNED_DOMAIN,
        PERMISSION_REQUIRED, FAILED. Poll every ~60s; NS changes usually land in
        5-30 min, but allow 24-48h before treating NS_NOT_CHANGED as final.
        Also visible via :meth:`list_connection_requests`.
        """
        self._require(approve, "connect_domains", "WRITE")
        return await self._request(
            "POST", "/v2/domains/connect-domain", json={"domainNames": domain_names})

    async def assign_mailboxes(
        self, domain_mailboxes: dict[str, list[dict]], *, approve: bool = False,
    ) -> dict:
        """Assign mailboxes keyed by domain ID. WRITE (consumes purchased quota).

        ``{domainId: [{firstName, lastName, mailboxUsername, domainName}]}``.
        Max 5 mailboxes per domain (default cap, configurable per account by
        Zapmail); no 24-hour rule on either provider (support, 2026-09-29).
        Status goes IN_PROGRESS -> ACTIVE | FAILED: Google usually within an
        hour, Microsoft a few hours; treat 24h as the escalation point.
        """
        self._require(approve, "assign_mailboxes", "WRITE")
        return await self._request("POST", "/v2/mailboxes", json=domain_mailboxes)

    async def export_mailboxes(
        self,
        apps: list[str],
        *,
        ids: list[str] | None = None,
        exclude_ids: list[str] | None = None,
        tag_ids: list[str] | None = None,
        contains: str | None = None,
        status: str | None = None,
        third_party_account_id: str | None = None,
        approve: bool = False,
    ) -> dict:
        """Export mailboxes to Smartlead/Instantly/... or CSV (``apps=["MANUAL"]``). WRITE.

        Re-export is capped at 3 requests/mailbox/week.
        """
        self._require(approve, "export_mailboxes", "WRITE")
        body = _pruned({
            "apps": apps, "ids": ids or [], "excludeIds": exclude_ids or [],
            "tagIds": tag_ids, "contains": contains, "status": status,
            "thirdPartyAccountId": third_party_account_id,
        })
        return await self._request("POST", "/v2/exports/mailboxes", json=body)

    async def add_third_party_account(
        self, email: str, password: str, app: str, *, approve: bool = False,
        extra: dict | None = None,
    ) -> dict:
        """Register a third-party app account for export. WRITE.

        ``extra`` carries app-specific fields the docs show for some apps
        (EmailBison adds ``url``; Google mailboxes add ``clientId`` /
        ``clientIdAppName``). For SMARTLEAD the credential convention is still
        unconfirmed — see docs/ZAPMAIL_QUESTIONS_2026-09.md Q7.
        """
        self._require(approve, "add_third_party_account", "WRITE")
        body = {"email": email, "password": password, "app": app, **(extra or {})}
        return await self._request(
            "POST", "/v2/exports/accounts/third-party", json=body)

    async def list_third_party_accounts(self, app: str) -> dict:
        """Third-party accounts registered for an app. READ-ONLY."""
        return await self._request(
            "GET", "/v2/exports/accounts/third-party", params={"app": app})

    async def fetch_export_workspaces(
        self, app: str, *, account_id: str | None = None,
    ) -> dict:
        """Workspaces available under a third-party app for export. READ-ONLY.

        Note: SMARTLEAD is not in Zapmail's supported list for this call —
        pin the export with ``thirdPartyAccountId`` instead.
        """
        params = {"app": app}
        if account_id:
            params["accountId"] = account_id
        return await self._request("GET", "/v2/exports/fetch-workspaces", params=params)

    async def get_export_status(self, export_id: int | str) -> dict:
        """Status of an export (``{export_id, status, failure_reason}``). READ-ONLY.

        Requires ``x-workspace-key``; set ZAPMAIL_WORKSPACE_KEY[_<NAME>] or this
        400s. The id is not returned by the export trigger in the documented
        example — see the questions doc.
        """
        return await self._request(
            "GET", "/v2/exports/status", params={"exportId": export_id})

    async def update_third_party_account(
        self, email: str, password: str, app: str, *,
        account_id: str | None = None, approve: bool = False,
    ) -> dict:
        """Update an existing third-party export account. WRITE."""
        self._require(approve, "update_third_party_account", "WRITE")
        return await self._request(
            "PUT", "/v2/exports/accounts/third-party",
            json={"email": email, "password": password, "app": app,
                  "accountId": account_id})

    async def add_dmarc(
        self,
        email: str,
        *,
        domain_ids: list[str] | None = None,
        contains: str | None = None,
        status: list[str] | None = None,
        tag_ids: list[str] | None = None,
        approve: bool = False,
    ) -> dict:
        """Set the DMARC aggregate-report address on matching domains. WRITE (free)."""
        self._require(approve, "add_dmarc", "WRITE")
        return await self._request(
            "POST", "/v2/domains/dmarc",
            json=_pruned({"domainIds": domain_ids, "email": email,
                          "contains": contains, "status": status,
                          "tagIds": tag_ids}))

    async def update_mailbox_names(
        self, changes: list[dict], *, approve: bool = False,
    ) -> dict:
        """Change mailboxes' first/last name. WRITE (free), processed async.

        ``changes``: ``[{mailboxId, firstName, lastName, username}]``. The
        endpoint (``PUT /v2/mailboxes``) also accepts a new username, which
        changes the ADDRESS - a warmed inbox would restart its reputation
        from zero - so callers pass the CURRENT username back unchanged.
        """
        self._require(approve, "update_mailbox_names", "WRITE")
        for c in changes:
            if not c.get("mailboxId") or not c.get("username"):
                raise ValueError("each change needs mailboxId and the current username")
        return await self._request("PUT", "/v2/mailboxes", json={"mailboxData": [
            {"mailboxId": c["mailboxId"], "firstName": c["firstName"],
             "lastName": c["lastName"], "username": c["username"]} for c in changes]})

    async def update_auto_renew(
        self, domain_ids: list[str], auto_renew: bool, *, approve: bool = False,
    ) -> dict:
        """Toggle auto-renewal on domains (aged/pre-warmed excluded). WRITE."""
        self._require(approve, "update_auto_renew", "WRITE")
        return await self._request(
            "POST", "/v2/domains/update-auto-renew",
            json={"domainIds": domain_ids, "autoRenew": auto_renew})

    async def assign_tag(
        self, tag_ids: list[str], domain_ids: list[str], *, approve: bool = False,
    ) -> dict:
        """Assign tags to domains (bulk). WRITE."""
        self._require(approve, "assign_tag", "WRITE")
        return await self._request(
            "POST", "/v2/domains/assign-tag",
            json={"tagIds": tag_ids, "domainIds": domain_ids})

    async def create_tags(self, tags: list[dict], *, approve: bool = False) -> dict:
        """Create tags (``[{name, tagColor}]``). WRITE."""
        self._require(approve, "create_tags", "WRITE")
        return await self._request("POST", "/v2/domains/tags", json=tags)

    async def delete_tag(self, tag_id: str, *, approve: bool = False) -> dict:
        """Permanently delete a tag. WRITE."""
        self._require(approve, "delete_tag", "WRITE")
        return await self._request("POST", "/v2/domains/tags/delete", json={"tagId": tag_id})

    async def remove_tags(
        self, tag_ids: list[str], domain_ids: list[str], *, approve: bool = False,
    ) -> dict:
        """Bulk-remove tags from domains. WRITE."""
        self._require(approve, "remove_tags", "WRITE")
        return await self._request(
            "POST", "/v2/domains/tags/remove",
            json={"tagIds": tag_ids, "domainIds": domain_ids})

    # ── SPEND (costs wallet/credits/invoice) — approval-gated ─────────────

    async def buy_domains(
        self,
        domains: list[str],
        *,
        years: int = 1,
        use_wallet: bool = True,
        enable_dns_shield: bool = False,
        approve: bool = False,
    ) -> dict:
        """Purchase/register domains. SPEND.

        Behaviour confirmed by Zapmail support (2026-09-29):
          * ``use_wallet=True`` puts the cart on one Stripe invoice and applies
            the wallet; when the wallet covers it all the invoice is paid at $0
            and registration starts. Returns ``{"message": "Domains purchased",
            "invoiceLink": "<url>"}`` — no domain ids (poll the domain list by
            name: PENDING -> ACTIVE).
          * A SHORT wallet is not an error: the invoice is issued for the
            remainder, nothing registers until it is paid, and the pending
            order is dropped after ~5 min. Check the balance first
            (``domain_purchase.require_wallet_covers``).
          * No registry check before charging; a taken name is refunded to the
            wallet after registration fails. Pre-verify with available-bulk.
          * It rejects names already in the account, banned phrases, and
            (400) names with an existing Google/Microsoft workspace.
        This is the call the entire approval flow exists to protect — never
        pass ``approve=True`` automatically.
        """
        self._require(approve, "buy_domains", "SPEND")
        body = {
            "domains": [{"domainName": d, "years": years} for d in domains],
            "useWallet": use_wallet,
            "enableDnsShield": enable_dns_shield,
        }
        return await self._request("POST", "/v2/domains/buy", json=body, retry=False)

    async def renew_domains(
        self, domain_ids: list[str], *, approve: bool = False,
    ) -> dict:
        """Renew specific domains expiring within 2 months. SPEND.

        Explicit ids only. Zapmail also accepts ``contains``/``tagIds`` filters
        (and its behaviour on an empty id list is undocumented), so a filter
        renewal could charge for far more domains than intended — not exposed.
        """
        ids = [str(d).strip() for d in (domain_ids or []) if str(d).strip()]
        if not ids:
            raise ZapmailSpendBlocked(
                "renew_domains needs an explicit, non-empty list of domain ids.")
        self._require(approve, "renew_domains", "SPEND")
        return await self._request(
            "POST", "/v2/domains/renew", json={"domainIds": ids}, retry=False)

    async def quick_setup(
        self,
        domains: list[str],
        mailboxes: dict[str, list[dict]],
        *,
        export_app: str | None = None,
        enable_dns_shield: bool = False,
        approve: bool = False,
    ) -> dict:
        """Purchase domains + assign mailboxes (+ optional export) in one call. SPEND.

        ``mailboxes`` maps ``domainName -> [{username, firstName, lastName, ...}]``.
        Per Zapmail support (2026-09-29): wallet-only; the slot cost is computed
        up front and an uncovered wallet gets ``400 "Insufficient wallet
        balance. Required: $X, Available: $Y"``. Needs billing details and an
        active base mailbox plan. Returns ``{quickSetupBatchId, slotsNeeded,
        invoiceUrl}`` (invoiceUrl = record of the PAID invoice). Track with
        :meth:`quick_setup_status`. ``export_app="SMARTLEAD"`` exports once the
        mailboxes are ACTIVE.
        """
        self._require(approve, "quick_setup", "SPEND")
        body: dict = {
            "domains": [{"domainName": d} for d in domains],
            "mailboxes": mailboxes,
            "enableDnsShield": enable_dns_shield,
        }
        if export_app:
            body["exportApp"] = export_app
        return await self._request("POST", "/v2/quick-setup", json=body, retry=False)

    async def quick_setup_status(self, domain: str) -> dict:
        """Progress of a quick-setup for one domain. READ-ONLY.

        Statuses (support, 2026-09-29): PENDING, DOMAIN_SUCCESS, DOMAIN_FAILED,
        MAILBOX_IN_PROGRESS, MAILBOX_FAILED, MAILBOX_SUCCESS,
        EXPORT_IN_PROGRESS, EXPORT_FAILED, COMPLETED.
        """
        return await self._request("GET", "/v2/quick-setup", params={"domain": domain})

    async def schedule_mailboxes(
        self, payload: dict, *, approve: bool = False,
    ) -> dict:
        """(DEPRECATED) Schedule mailbox creation for next renewal date. WRITE.

        Superseded by the quicker ``quick_setup`` / scheduled-flow, but surfaced
        here for completeness. Approve-gated like any mutation.
        """
        self._require(approve, "schedule_mailboxes", "WRITE")
        return await self._request(
            "POST", "/v2/mailboxes/schedule", json=payload, service_provider=True)

    # ── webhooks (READ + WRITE) ─────────────────────────────────────────
    # Deliveries are signed: header `X-Zapmail-Signature: t=<unix>,v1=<hex>`,
    # v1 = HMAC-SHA256(secret, f"{t}.{raw_body}"). Retries 1m→24h (8 tries);
    # 20 straight failures disable the endpoint. Verified in zapmail_webhooks.js.

    # Zapmail's own names (its 422 on 2026-09-29 listed the allowed set:
    # domain.updated, domain.connection_status_changed, mailbox.updated,
    # subscription.status_changed, subscription.billing_changed,
    # export.started/completed/failed/reconnected,
    # placement_test.status_changed, workspace.*). The first guess
    # ("domain.status_changed", "mailbox.status_changed") was rejected.
    WEBHOOK_EVENTS: tuple[str, ...] = (
        "domain.updated", "domain.connection_status_changed", "mailbox.updated",
        "export.completed", "export.failed", "placement_test.status_changed",
        "subscription.status_changed", "subscription.billing_changed",
    )

    async def list_webhook_endpoints(self) -> dict:
        """Registered webhook endpoints. READ-ONLY (verified live: [] on both)."""
        return await self._request("GET", "/v2/webhooks/endpoints")

    async def create_webhook_endpoint(
        self, url: str, events: list[str] | tuple[str, ...], *, approve: bool = False,
    ) -> dict:
        """Register a webhook endpoint. WRITE (free).

        The response's ``data.secret`` is shown ONCE and cannot be fetched
        again — store it (ZAPMAIL_WEBHOOK_SECRET_<ACCOUNT>) before anything else.
        """
        self._require(approve, "create_webhook_endpoint", "WRITE")
        return await self._request(
            "POST", "/v2/webhooks/endpoints",
            json={"url": url, "enabled_events": list(events)})

    # ── placement tests (READ + SPEND) ───────────────────────────────────

    async def placement_subscriptions(self) -> dict:
        """Placement-test subscriptions + credits. READ-ONLY."""
        return await self._request("GET", "/v2/placement-tests/subscriptions")

    async def placement_credits(self) -> dict:
        """Available placement-test credits. READ-ONLY."""
        return await self._request("GET", "/v2/placement-tests/available-slots")

    async def placement_overall_report(self) -> dict:
        """Aggregate placement performance. READ-ONLY."""
        return await self._request("GET", "/v2/placement-tests/overall-report")

    async def placement_orders(self, *, page: int | None = None,
                               limit: int | None = None) -> dict:
        """Placement-test orders + results. READ-ONLY."""
        return await self._request(
            "GET", "/v2/placement-tests/orders",
            params=_pruned({"page": page, "limit": limit}))

    async def placement_report(self, cart_order_id: str | int) -> dict:
        """Detailed report for one cart order. READ-ONLY."""
        return await self._request(
            "GET", "/v2/placement-tests/report",
            params={"cartOrderId": cart_order_id})

    async def placement_eligible_mailboxes(
        self, *, page: int = 1, limit: int = 50, status: str | None = None,
    ) -> dict:
        """Mailboxes eligible for a placement test. READ-ONLY (needs provider)."""
        return await self._request(
            "POST", "/v2/placement-tests/eligible-mailboxes",
            params=_pruned({"page": page, "limit": limit, "status": status}),
            service_provider=True)

    async def purchase_placement_test(
        self, *, placement_type: str, test_name: str,
        mailbox_ids: list[str], seed_accounts: list[str],
        approve: bool = False,
    ) -> dict:
        """Run a placement test. SPEND (MONTHLY uses credits, ONE_TIME $2/mailbox)."""
        self._require(approve, "purchase_placement_test", "SPEND")
        return await self._request(
            "POST", "/v2/placement-tests/purchase",
            json={"placementType": placement_type, "testName": test_name,
                  "mailboxIds": mailbox_ids, "seedAccounts": seed_accounts},
            retry=False)

    async def purchase_placement_plan(self, plan_name: str, *,
                                      approve: bool = False) -> dict:
        """Buy a placement-test plan. SPEND."""
        self._require(approve, "purchase_placement_plan", "SPEND")
        return await self._request(
            "POST", "/v2/placement-tests/purchase-plan",
            json={"planName": plan_name}, service_provider=True, retry=False)

    async def cancel_placement_subscription(
        self, subscription_id: str, *, revert: bool = False,
        approve: bool = False,
    ) -> dict:
        """Cancel/revert a placement-test subscription. WRITE."""
        self._require(approve, "cancel_placement_subscription", "WRITE")
        return await self._request(
            "POST", "/v2/placement-tests/cancel-subscription",
            json={"subscriptionId": subscription_id, "revertCancellation": revert})

    # ── DNS Shield (READ + WRITE + SPEND) ────────────────────────────────

    async def dns_shield_eligible_domains(
        self, *, page: int = 1, limit: int = 50,
    ) -> dict:
        """Domains eligible for DNS Shield. READ-ONLY."""
        return await self._request(
            "GET", "/v2/dns-shield/eligible-domains",
            params={"page": str(page), "limit": str(limit)})

    async def dns_shield_available_slots(self) -> dict:
        """Available DNS Shield slots. READ-ONLY."""
        return await self._request("GET", "/v2/dns-shield/available-slots")

    async def dns_shield_subscriptions(self) -> dict:
        """DNS Shield subscriptions. READ-ONLY."""
        return await self._request("GET", "/v2/dns-shield/subscriptions")

    async def dns_shield_allocated_domains(self, subscription_id: str) -> dict:
        """Domains allocated to a DNS Shield subscription. READ-ONLY."""
        return await self._request(
            "GET", "/v2/dns-shield/allocated-domains",
            params={"subscriptionId": subscription_id})

    async def allocate_dns_shield(self, domain_ids: list[str], *,
                                  approve: bool = False) -> dict:
        """Allocate domains to DNS Shield slots. WRITE (consumes slots)."""
        self._require(approve, "allocate_dns_shield", "WRITE")
        return await self._request(
            "POST", "/v2/dns-shield/allocate-domains",
            json={"domainIds": domain_ids})

    async def purchase_dns_shield(
        self, *, plan_type: str, plan_name: str | None = None,
        quantity: int | None = None, domain_names: list[str] | None = None,
        domain_ids: list[str] | None = None, approve: bool = False,
    ) -> dict:
        """Buy DNS Shield (LTD/MONTHLY/EXISTING). SPEND."""
        self._require(approve, "purchase_dns_shield", "SPEND")
        body: dict = {"planType": plan_type}
        if plan_name:
            body["planName"] = plan_name
        if quantity is not None:
            body["quantity"] = quantity
        if domain_names:
            body["domainNames"] = domain_names
        if domain_ids:
            body["domainIds"] = domain_ids
        return await self._request(
            "POST", "/v2/dns-shield/purchase", json=body,
            service_provider=True, retry=False)

    async def cancel_dns_shield(self, subscription_id: str, *,
                                approve: bool = False) -> dict:
        """Cancel a DNS Shield subscription. WRITE."""
        self._require(approve, "cancel_dns_shield", "WRITE")
        return await self._request(
            "POST", "/v2/dns-shield/cancel-subscription",
            json={"subscriptionId": subscription_id})

    # ── Pre-warmed domains (READ + WRITE + SPEND) ────────────────────────

    async def prewarmed_domains(
        self, *, page: int = 1, limit: int = 50, contains: str | None = None,
    ) -> dict:
        """Available pre-warmed domains. READ-ONLY."""
        return await self._request(
            "GET", "/v2/prewarmed-domains/get-domains",
            params=_pruned({"page": page, "limit": limit, "contains": contains}))

    async def prewarmed_count(self) -> dict:
        """Unsold pre-warmed domain counts by provider. READ-ONLY."""
        return await self._request("GET", "/v2/prewarmed-domains/count")

    async def prewarmed_subscriptions(
        self, *, status: str | None = None, page: int | None = None,
        limit: int | None = None,
    ) -> dict:
        """Pre-warm subscriptions. READ-ONLY."""
        return await self._request(
            "GET", "/v2/prewarmed-domains/subscriptions",
            params=_pruned({"status": status, "page": page, "limit": limit}))

    async def purchase_prewarmed(self, plan_type: str, *,
                                 approve: bool = False) -> dict:
        """Buy a pre-warmed plan. SPEND."""
        self._require(approve, "purchase_prewarmed", "SPEND")
        return await self._request(
            "POST", "/v2/prewarmed-domains/purchase",
            params={"planType": plan_type}, service_provider=True, retry=False)

    async def buy_addon_mailboxes(self, quantity: int, *, approve: bool = False) -> dict:
        """Buy extra mailbox slots. SPEND, never retried.

        ``POST /v2/wallet/buy-addon-mailboxes?quantity=N`` ($3.00-3.50 per
        mailbox per month by plan). Zapmail answers with an invoice /
        ``paymentLink``; callers confirm the slots actually appeared
        (``list_mailboxes`` quota) instead of trusting the response.
        """
        self._require(approve, "buy_addon_mailboxes", "SPEND")
        if not isinstance(quantity, int) or not 1 <= quantity <= 50:
            raise ValueError("quantity must be 1-50")
        return await self._request(
            "POST", "/v2/wallet/buy-addon-mailboxes",
            params={"quantity": str(quantity)}, retry=False)

    async def assign_prewarmed(self, domain_ids: list[str], *,
                               approve: bool = False) -> dict:
        """Assign pre-warmed domains to fill slots. WRITE."""
        self._require(approve, "assign_prewarmed", "WRITE")
        return await self._request(
            "POST", "/v2/prewarmed-domains/assign", json={"domainIds": domain_ids})

    # ── High-reputation (aged) domains (READ + SPEND) ────────────────────

    async def aged_domains(self, *, page: int | None = None,
                           limit: int | None = None) -> dict:
        """Marketplace aged domains. READ-ONLY."""
        return await self._request(
            "GET", "/v2/aged-domains/available-domains",
            params=_pruned({"page": page, "limit": limit}))

    async def purchase_aged_domains(self, domains: list[str], *,
                                    approve: bool = False) -> dict:
        """Buy high-reputation domains (max 50). SPEND."""
        self._require(approve, "purchase_aged_domains", "SPEND")
        return await self._request(
            "POST", "/v2/aged-domains/purchase", json={"domains": domains},
            service_provider=True, retry=False)


def _pruned(params: dict[str, Any]) -> dict[str, Any]:
    """Drop None values so optional query params don't ship as ``None``."""
    return {k: v for k, v in params.items() if v is not None}