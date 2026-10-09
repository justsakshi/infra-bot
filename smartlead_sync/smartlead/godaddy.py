"""HTTP client for GoDaddy's Domains API v3 — where we register sending domains.

Decided 2026-10-09: domains come from GoDaddy (cheaper than ScaledMail's
$15.50, renews at ~$14.99 instead of $17), mailboxes from ScaledMail on those
domains (nameservers pointed at ScaledMail, their team sets up DNS).

Same rule as ``scaledmail.py`` / ``zapmail.py``:

    **Read-only calls are free. Write and spend calls are armed.**

  * READ   — availability, quotes, domain list. Free, no side effect.
  * WRITE  — nameserver change. Free but changes a live domain.
             Needs ``approve=True``.
  * SPEND  — ``POST /registrations`` charges the account's billing method and
             is not reversible. Needs ``approve=True`` AND
             ``GODADDY_ALLOW_SPEND=true``. Sent with an ``Idempotency-Key``:
             replaying the same key after a timeout is deduplicated by
             GoDaddy, so a lost answer can never buy a domain twice.

API facts (docs read 2026-10-09, developer.godaddy.com/llms.txt):
  * v3 only takes a Personal Access Token: ``Authorization: Bearer
    <GODADDY_PAT>`` with scopes ``domains.domain:read`` (+ ``:create`` to buy,
    + update for nameservers). The old sso-key works only on v1/v2.
  * v3 has no 50-domain rule (that was v1's availability API). Registration
    needs a billing method on file (or Good as Gold balance); management needs
    the account to hold at least one domain.
  * v3 discounted .com: $9.79 first year, $14.99 auto-renewal.
  * Register = quote (locks price, returns ``requiredAgreements``) → execute
    with ``quoteToken`` + consent → poll until COMPLETED / FAILED. One domain
    per call. Prices are minor units (cents).
  * Rate limit ~600 requests per ~23-minute window per token (429 + Retry-After).
"""

from __future__ import annotations

import os
import time
import uuid
from typing import Any

import httpx

GODADDY_BASE_URL = os.getenv("GODADDY_BASE_URL", "https://api.godaddy.com/v3/domains")
CHECK_BATCH = 25          # POST /check-availability takes 1-25 names
_MIN_GAP = 0.3


class GoDaddyError(RuntimeError):
    """Base for GoDaddy client failures."""


class GoDaddyBlocked(GoDaddyError):
    """A WRITE / SPEND call was refused by a gate."""


class GoDaddyHTTPError(GoDaddyError):
    """A definitive 4xx: the request was NOT processed."""

    def __init__(self, message: str, status_code: int, code: str = "") -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code


class GoDaddyOutcomeUnknown(GoDaddyError):
    """Network error, timeout or 5xx: the request may have been processed.
    For a registration, replay with the SAME Idempotency-Key or read it back."""


def spend_allowed() -> bool:
    return os.getenv("GODADDY_ALLOW_SPEND", "").strip().lower() == "true"


def configured() -> bool:
    return bool(os.getenv("GODADDY_PAT", "").strip())


def idempotency_key(*parts: str) -> str:
    """Deterministic UUID for one attempt: the same plan/domain/attempt always
    gets the same key, so a replay after a crash is deduplicated by GoDaddy."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "godaddy:" + ":".join(parts)))


def cents(money: dict | None) -> int | None:
    try:
        return int((money or {}).get("value"))
    except (TypeError, ValueError):
        return None


class GoDaddyClient:
    """Thin sync wrapper. ``with GoDaddyClient() as gd: gd.check(["a.com"])``."""

    def __init__(self, token: str | None = None, *, timeout: float = 60.0,
                 transport: httpx.BaseTransport | None = None) -> None:
        self._token = (token if token is not None else os.getenv("GODADDY_PAT", "")).strip()
        self._timeout = timeout
        self._transport = transport
        self._http: httpx.Client | None = None
        self._last = 0.0

    def __enter__(self) -> "GoDaddyClient":
        if not self._token:
            raise GoDaddyError("GODADDY_PAT is not set")
        self._http = httpx.Client(timeout=self._timeout, transport=self._transport, headers={
            "Authorization": f"Bearer {self._token}", "User-Agent": "infrabot/1.0",
            "Accept": "application/json"})
        return self

    def __exit__(self, *exc: object) -> None:
        if self._http:
            self._http.close()
            self._http = None

    @staticmethod
    def _require(approve: bool, label: str, kind: str) -> None:
        if not approve:
            raise GoDaddyBlocked(f"{label} is a {kind} operation. Pass approve=True only after "
                                 "a human has confirmed the exact domain and price.")
        if kind == "SPEND" and not spend_allowed():
            raise GoDaddyBlocked(f"{label} charges the account and GODADDY_ALLOW_SPEND is not 'true'.")

    def _request(self, method: str, path: str, *, params: dict | None = None, json: Any = None,
                 headers: dict | None = None, retry: bool = True, url: str | None = None) -> Any:
        assert self._http, "use `with GoDaddyClient() as gd:`"
        attempts = 4 if retry else 1
        for attempt in range(attempts):
            wait = _MIN_GAP - (time.monotonic() - self._last)
            if wait > 0:
                time.sleep(wait)
            self._last = time.monotonic()
            try:
                r = self._http.request(method, url or GODADDY_BASE_URL + path, params=params,
                                       json=json, headers=headers)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < attempts - 1:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise GoDaddyOutcomeUnknown(f"network error on {path}: {exc!r}") from exc
            if r.status_code == 429 and attempt < attempts - 1:
                try:
                    pause = float(r.headers.get("Retry-After") or 0)
                except ValueError:
                    pause = 0
                time.sleep(min(pause or 2 ** attempt * 5, 90))
                continue
            if r.status_code >= 500:
                if attempt < attempts - 1:
                    time.sleep(min(2 ** attempt, 8))
                    continue
                raise GoDaddyOutcomeUnknown(f"{r.status_code} on {path}: {r.text[:200]}")
            if r.status_code >= 400:
                code, msg = _error(r)
                raise GoDaddyHTTPError(f"{r.status_code} {code} on {path}: {msg}", r.status_code, code)
            if not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {"text": r.text[:500]}
        raise GoDaddyOutcomeUnknown(f"no answer on {path}")  # pragma: no cover

    # ── READ ─────────────────────────────────────────────────────────────
    def check(self, domains: list[str]) -> list[dict]:
        """Availability + indicative first-year / renewal price (cents), batched by 25."""
        out: list[dict] = []
        names = [d.strip().lower() for d in domains if d.strip()]
        for i in range(0, len(names), CHECK_BATCH):
            res = self._request("POST", "/check-availability", json={"domains": names[i:i + CHECK_BATCH]})
            for item in (res or {}).get("items") or []:
                one = next((p for p in item.get("prices") or [] if p.get("period") == 1),
                           (item.get("prices") or [{}])[0] if item.get("prices") else {})
                out.append({"domain": str(item.get("domain", "")).lower(),
                            "available": bool(item.get("available")),
                            "definitive": bool(item.get("definitive")),
                            "price_cents": cents(one.get("firstTermPrice")) or cents(one.get("price")),
                            "renewal_cents": cents(one.get("renewalPrice")),
                            "fees": one.get("fees") or [],
                            "error": item.get("error")})
        return out

    def quote(self, domain: str, period: int = 1) -> dict:
        """Locks the price. Never charges."""
        return self._request("POST", "/registration-quotes", json={"domain": domain, "period": period},
                             retry=False)

    def registration(self, registration_id: str) -> dict:
        return self._request("GET", f"/registrations/{registration_id}")

    def operation(self, operation_id: str) -> dict:
        return self._request("GET", f"/operations/{operation_id}")

    def domain(self, name: str) -> dict:
        return self._request("GET", f"/domain-names/{name}")

    def domains(self, page_size: int = 100) -> list[dict]:
        """Every registered domain (follows links[rel=next])."""
        out: list[dict] = []
        res = self._request("GET", "/domain-names", params={"pageSize": page_size})
        for _ in range(200):
            out.extend((res or {}).get("items") or [])
            nxt = next((l.get("href") for l in (res or {}).get("links") or [] if l.get("rel") == "next"), None)
            if not nxt:
                break
            res = self._request("GET", "/domain-names", url=nxt)
        return out

    # ── WRITE ────────────────────────────────────────────────────────────
    def set_nameservers(self, domain: str, nameservers: list[str], *, key: str,
                        approve: bool = False) -> dict:
        self._require(approve, "set_nameservers", "WRITE")
        return self._request("PUT", f"/domain-names/{domain}/nameservers", json=list(nameservers),
                             headers={"Idempotency-Key": key}, retry=False)

    # ── SPEND ────────────────────────────────────────────────────────────
    def register(self, domain: str, quote: dict, *, agreed_at: str, key: str,
                 period: int = 1, approve: bool = False) -> dict:
        """Charges the account. ``quote`` is the answer of :meth:`quote`."""
        self._require(approve, "register", "SPEND")
        consent: dict = {"agreedAt": agreed_at,
                         "agreementTypes": [a["agreementType"] for a in quote.get("requiredAgreements") or []]}
        fees = [f for item in quote.get("items") or [] for f in item.get("fees") or []]
        if fees:
            raise GoDaddyBlocked(f"{domain} carries extra fees {fees} (premium name) — not bought by the bot")
        body = {"quoteToken": quote["quoteToken"], "domain": domain, "period": period, "consent": consent}
        return self._request("POST", "/registrations", json=body,
                             headers={"Idempotency-Key": key}, retry=False)


def _error(r: httpx.Response) -> tuple[str, str]:
    try:
        j = r.json()
        return str(j.get("code") or ""), str(j.get("message") or j)[:300]
    except ValueError:
        return "", r.text[:300]
