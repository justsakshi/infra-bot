"""HTTP client for the ScaledMail API — the second mailbox provider after Zapmail.

ScaledMail sells Google / Outlook / SMTP mailboxes on its own domains (or
ours) as monthly subscriptions ("orders"). This module is the single place
the tools talk to ``https://server.scaledmail.com/api/v1``.

Same rule as ``zapmail.py``:

    **Read-only calls are free. Write and spend calls are armed.**

  * READ   — no side effect, no cost.
  * WRITE  — changes something but costs nothing (order tag, sender-name /
             redirect / domain swap requests). Needs ``approve=True``.
  * CANCEL — cancels a whole order (every mailbox in it stops). Needs
             ``approve=True`` AND ``SCALEDMAIL_ALLOW_CANCEL=true``.
  * SPEND  — charges the saved card at once (orders, domains, pre-warmed).
             Needs ``approve=True`` AND ``SCALEDMAIL_ALLOW_SPEND=true``,
             and is never retried (a timeout may still have charged).

Live facts (2026-10-08, account "preciseleads"):
  * Auth ``Authorization: Bearer <SCALEDMAIL_API_KEY>``; every call takes
    ``organization_id`` (``SCALEDMAIL_ORG_ID``, else the only organization).
  * Prices from ``/calculate-package`` and ``/packages`` are DOLLARS, although
    the docs say cents (40 Google mailboxes = 140 = the $140 package).
  * Google $3.50/mailbox (2-4 per domain), Outlook $50/domain of 25,
    SMTP $3.75/domain of 4. Domain registration ~$10.85, renewal ~$15.
  * 5 requests/second; domain search/suggest 15/minute.
Docs: https://api.scaledmail.com/llms.txt
"""

from __future__ import annotations

import os
import time
from typing import Any

import httpx

SCALEDMAIL_BASE_URL = "https://server.scaledmail.com/api/v1"
PROVIDERS: tuple[str, ...] = ("google", "outlook", "smtp")
# Mailboxes per domain ScaledMail creates (google is 2, 3 or 4 by choice).
MAILBOXES_PER_DOMAIN = {"google": (2, 3, 4), "outlook": (25,), "smtp": (4,)}
MAX_DOMAINS_PER_ORDER = {"google": 100, "outlook": 30, "smtp": 70}
_MIN_GAP = 0.25   # seconds between calls: 4/s, under the 5/s limit


class ScaledMailError(RuntimeError):
    """Base for ScaledMail client failures."""


class ScaledMailBlocked(ScaledMailError):
    """A WRITE / CANCEL / SPEND call was refused by a gate."""


class ScaledMailHTTPError(ScaledMailError):
    """A 4xx. Usually not processed — but see ``payment_pending`` in
    scaledmail_orders: "Subscription created but unable to charge" is a 400
    that DID create a subscription. ``body`` keeps the raw answer to learn from."""

    def __init__(self, message: str, status_code: int, body: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class ScaledMailOutcomeUnknown(ScaledMailError):
    """Network error, timeout or 5xx: the request may have been processed.
    For a SPEND call, read the orders back before trying again."""


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() == "true"


def spend_allowed() -> bool:
    return _flag("SCALEDMAIL_ALLOW_SPEND")


def cancel_allowed() -> bool:
    return _flag("SCALEDMAIL_ALLOW_CANCEL")


def configured() -> bool:
    return bool(os.getenv("SCALEDMAIL_API_KEY", "").strip())


class ScaledMailClient:
    """Thin sync wrapper. ``with ScaledMailClient() as sm: sm.domains()``."""

    def __init__(self, api_key: str | None = None, org_id: str | None = None,
                 *, timeout: float = 60.0) -> None:
        self._key = (api_key if api_key is not None else os.getenv("SCALEDMAIL_API_KEY", "")).strip()
        self._org = (org_id if org_id is not None else os.getenv("SCALEDMAIL_ORG_ID", "")).strip()
        self._timeout = timeout
        self._http: httpx.Client | None = None
        self._last = 0.0

    def __enter__(self) -> "ScaledMailClient":
        if not self._key:
            raise ScaledMailError("SCALEDMAIL_API_KEY is not set")
        self._http = httpx.Client(timeout=self._timeout, headers={
            "Authorization": f"Bearer {self._key}", "User-Agent": "infrabot/1.0"})
        return self

    def __exit__(self, *exc: object) -> None:
        if self._http:
            self._http.close()
            self._http = None

    # ── gates ────────────────────────────────────────────────────────────
    @staticmethod
    def _require(approve: bool, label: str, kind: str) -> None:
        if not approve:
            raise ScaledMailBlocked(
                f"{label} is a {kind} operation. Pass approve=True only after a "
                "human has confirmed the exact payload.")
        if kind == "SPEND" and not spend_allowed():
            raise ScaledMailBlocked(
                f"{label} charges the card and SCALEDMAIL_ALLOW_SPEND is not 'true'.")
        if kind == "CANCEL" and not cancel_allowed():
            raise ScaledMailBlocked(
                f"{label} stops every mailbox in the order and "
                "SCALEDMAIL_ALLOW_CANCEL is not 'true'.")

    # ── low level ────────────────────────────────────────────────────────
    def org_id(self) -> str:
        """SCALEDMAIL_ORG_ID, else the account's only organization."""
        if not self._org:
            orgs = self.organizations()
            if len(orgs) != 1:
                raise ScaledMailError(
                    f"{len(orgs)} ScaledMail organizations — set SCALEDMAIL_ORG_ID")
            self._org = orgs[0]["id"]
        return self._org

    def _request(self, method: str, path: str, *, params: dict | None = None,
                 json: Any = None, retry: bool = True, org: bool = True) -> Any:
        assert self._http, "use `with ScaledMailClient() as sm:`"
        q = dict(params or {})
        if org:
            q.setdefault("organization_id", self.org_id())
        attempts = 4 if retry else 1
        for attempt in range(attempts):
            wait = _MIN_GAP - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                r = self._http.request(method, SCALEDMAIL_BASE_URL + path, params=q, json=json)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < attempts - 1:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise ScaledMailOutcomeUnknown(f"network error on {path}: {exc!r}") from exc
            if r.status_code == 429 and attempt < attempts - 1:
                time.sleep(min(2 ** attempt * 4, 60))
                continue
            if r.status_code >= 500:
                if attempt < attempts - 1:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise ScaledMailOutcomeUnknown(f"{r.status_code} on {path}: {r.text[:200]}")
            if r.status_code >= 400:
                raise ScaledMailHTTPError(f"{r.status_code} on {path}: {_error_text(r)}", r.status_code,
                                          body=r.text[:2000])
            try:
                return r.json()
            except ValueError:
                return {"text": r.text[:500]}
        raise ScaledMailOutcomeUnknown(f"no answer on {path}")  # pragma: no cover

    # ── READ ─────────────────────────────────────────────────────────────
    def organizations(self) -> list[dict]:
        return (self._request("GET", "/organizations", org=False) or {}).get("organizations") or []

    def domains(self) -> list[dict]:
        """Active domains with mailbox names. Never asks for passwords."""
        return (self._request("GET", "/domains") or {}).get("domains") or []

    def mailboxes(self, domain_id: str) -> list[dict]:
        """Mailboxes on one domain. Never asks for passwords."""
        return (self._request("GET", f"/mailboxes/{domain_id}") or {}).get("mailboxes") or []

    def purchased_domains(self, *, available_only: bool = False) -> list[dict]:
        """Registrations bought through ScaledMail (renewal date + price)."""
        params = {"available": "true"} if available_only else None
        return (self._request("GET", "/purchased-domains", params=params) or {}).get("domains") or []

    def orders(self) -> list[dict]:
        return (self._request("GET", "/orders") or {}).get("payments") or []

    def order(self, order_id: str) -> dict:
        return self._request("GET", f"/orders/{order_id}") or {}

    def domain_masking(self) -> list[dict]:
        return (self._request("GET", "/domain-masking") or {}).get("domains") or []

    def reporting(self) -> list[dict]:
        """ScaledMail's own weekly placement tests (paid add-on)."""
        return (self._request("GET", "/reporting") or {}).get("reports") or []

    def packages(self) -> list[dict]:
        return (self._request("GET", "/packages") or {}).get("packages") or []

    def prewarmed(self) -> dict:
        return self._request("GET", "/pre-warm-inboxes") or {}

    def calculate(self, volume: int, providers: list[str], *, period: str = "month",
                  distribution: dict[str, int] | None = None, tier: str = "low") -> dict:
        """Domains, mailboxes and price for a sending volume. Free, no order."""
        body: dict = {"volume": int(volume), "period": period,
                      "providers": list(providers), "tier": tier}
        if distribution:
            body["distribution"] = distribution
        return (self._request("POST", "/calculate-package", json=body) or {}).get("data") or {}

    def search_domains(self, names: list[str]) -> Any:
        """Availability for up to 30 names (15 calls/minute)."""
        if len(names) > 30:
            raise ScaledMailError("search_domains takes at most 30 names per call")
        return self._request("POST", "/search-domains", json={"domains": list(names)})

    def suggest_domains(self, keyword: str, tlds: list[str] | None = None,
                        *, page: int = 1, limit: int = 20) -> dict:
        body: dict = {"domain": keyword}
        if tlds:
            body["tlds"] = tlds
        return self._request("POST", "/suggest-domains", json=body,
                             params={"page": page, "limit": limit}) or {}

    # ── WRITE (free, approve=True) ───────────────────────────────────────
    def set_order_tag(self, order_id: str, tag: str, *, approve: bool = False) -> dict:
        self._require(approve, "set_order_tag", "WRITE")
        if len(tag) > 320:
            raise ScaledMailError("tag is longer than 320 characters")
        return self._request("PATCH", f"/orders/{order_id}/tag", json={"tag": tag}, retry=False)

    def swap_sender_names(self, domain: str, names: list[tuple[str, str]], *,
                          mode: str = "name_only", approve: bool = False) -> dict:
        """Request new sender names. ``name_only`` keeps every address (no
        re-warm); ``name_and_alias`` deletes and recreates the mailboxes."""
        self._require(approve, "swap_sender_names", "WRITE")
        if mode not in ("name_only", "name_and_alias"):
            raise ScaledMailError("mode must be name_only or name_and_alias")
        body = {"sender_names": [{"first_name": f, "last_name": l} for f, l in names]}
        return self._request("POST", f"/swap-sender-name/{domain}", params={"mode": mode},
                             json=body, retry=False)

    def swap_redirect(self, domain: str, new_redirect: str, *, approve: bool = False) -> dict:
        """Request a new redirect URL ('' removes it)."""
        self._require(approve, "swap_redirect", "WRITE")
        return self._request("POST", f"/swap-redirect/{domain}",
                             json={"new_redirect": new_redirect}, retry=False)

    def swap_domain(self, old_domain: str, new_domain: str, *, source: str = "scaledmail",
                    approve: bool = False) -> dict:
        """Request replacing a domain. ``source='scaledmail'``: the new one must be
        in our unassigned purchased-domain inventory."""
        self._require(approve, "swap_domain", "WRITE")
        if source not in ("scaledmail", "other"):
            raise ScaledMailError("source must be scaledmail or other")
        return self._request("POST", f"/swap-domain/{old_domain}", params={"provider": source},
                             json={"new_domain": new_domain}, retry=False)

    def swap_masking(self, domain: str, new_primary: str, *, approve: bool = False) -> dict:
        self._require(approve, "swap_masking", "WRITE")
        return self._request("POST", f"/swap-domain-masking/{domain}",
                             json={"new_primary_domain": new_primary}, retry=False)

    # ── CANCEL (approve=True + SCALEDMAIL_ALLOW_CANCEL) ──────────────────
    def cancel_order(self, order_id: str, *, approve: bool = False) -> dict:
        self._require(approve, "cancel_order", "CANCEL")
        return self._request("DELETE", f"/orders/{order_id}", retry=False)

    # ── SPEND (approve=True + SCALEDMAIL_ALLOW_SPEND, single-shot) ───────
    def create_custom_order(self, providers: dict, *, source: str = "buy",
                            tag: str = "", approve: bool = False) -> dict:
        """Order mailboxes. ``source``: 'buy' (ScaledMail registers the domains),
        'scaledmail' (domains already bought there) or 'other' (ours)."""
        self._require(approve, "create_custom_order", "SPEND")
        body: dict = {"providers": providers}
        if tag:
            body["tag"] = tag[:20]
        return self._request("POST", "/create-custom-order", params={"provider": source},
                             json=body, retry=False)

    def buy_domains(self, domains: list[str], *, approve: bool = False) -> dict:
        self._require(approve, "buy_domains", "SPEND")
        return self._request("POST", "/buy-domains", json={"domains": list(domains)}, retry=False)

    def buy_prewarmed(self, domains: list[dict], *, tag: str = "", approve: bool = False) -> dict:
        self._require(approve, "buy_prewarmed", "SPEND")
        body: dict = {"domains": domains}
        if tag:
            body["tag"] = tag
        return self._request("POST", "/buy-pre-warm-inboxes", json=body, retry=False)


def _error_text(r: httpx.Response) -> str:
    try:
        j = r.json()
        return str(j.get("error") or j.get("message") or j)[:300]
    except ValueError:
        return r.text[:300]
