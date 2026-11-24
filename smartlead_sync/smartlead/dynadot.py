"""HTTP client for Dynadot's api3 (JSON) — domains bought from the prepaid balance.

Decided 2026-11-24 (t406): buy ~100 .co in Dynadot's Black Friday sale
(about $2.60 a domain, 27 Nov 00:00 UTC to 5 Dec 16:00 UTC per the community
deal tracker). Dynadot API orders are paid ONLY from the account balance, so
the card OTP that blocked GoDaddy API buys never comes up: a person tops the
balance up once by hand, the script spends it.

Same rule as ``godaddy.py`` / ``zapmail.py``:

    **Read-only calls are free. Spend calls are armed.**

  * READ   — search (availability + price), balance, domain_info. Free.
  * SPEND  — ``register`` takes money from the balance and is not reversible.
             Needs ``approve=True``; on the live API also
             ``DYNADOT_ALLOW_SPEND=true``. The sandbox (pre-funded test
             account, no real money) needs only ``approve=True``.

API facts (docs read 2026-11-24, dynadot.com/domain/api-commands):
  * ``GET https://api.dynadot.com/api3.json?key=...&command=...``; sandbox
    host ``api-sandbox.dynadot.com`` with its own key.
  * The key travels in the QUERY STRING. Never print a request URL, never
    call ``raise_for_status()`` (httpx puts the URL, key included, in the
    message), never put ``repr(exc)`` of an httpx error in a message.
  * Regular accounts: 1 thread, 60 requests/min, ``search`` 1 domain per
    call. ``register`` is one domain per call, "You must have enough account
    balance to pay for the order".
  * ``search ... show_price=1`` answers ``"Price": "2.60 in USD and domain
    is not premium"``.
  * No idempotency key exists. A domain cannot be registered twice anyway:
    once ours, ``search`` says it is not available and ``domain_info`` finds
    it in the account.
"""

from __future__ import annotations

import os
import re
import time
from typing import Any

import httpx

LIVE_URL = "https://api.dynadot.com/api3.json"
SANDBOX_URL = "https://api-sandbox.dynadot.com/api3.json"
_MIN_GAP = 1.1            # regular account: 60 requests a minute, one at a time

_PRICE = re.compile(r"([\d,]+(?:\.\d+)?)\s*in\s*([A-Za-z]{3})")
_FUNDS = ("insufficient", "not enough", "balance", "fund")


class DynadotError(RuntimeError):
    """Base for Dynadot client failures."""


class DynadotBlocked(DynadotError):
    """A SPEND call was refused by a gate."""


class DynadotRefused(DynadotError):
    """Dynadot answered and said no (ResponseCode -1 / Status error, or a 4xx).
    The request was NOT carried out."""


class DynadotOutcomeUnknown(DynadotError):
    """Network error, timeout or 5xx: the request may have been carried out."""


def _flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() == "true"


def sandbox_mode() -> bool:
    return _flag("DYNADOT_SANDBOX")


def spend_allowed() -> bool:
    return _flag("DYNADOT_ALLOW_SPEND")


def key_name(sandbox: bool) -> str:
    return "DYNADOT_SANDBOX_KEY" if sandbox else "DYNADOT_API_KEY"


def funds_problem(error: str) -> bool:
    e = str(error or "").lower()
    return any(k in e for k in _FUNDS)


_REG_PRICE = re.compile(r"registration\s+price:\s*([\d,]+(?:\.\d+)?)\s*in\s*([A-Za-z]{3})", re.I)
_NOT_PREMIUM = re.compile(r"\bnot\s+(?:a\s+)?premium\b", re.I)


def parse_price(raw: Any) -> tuple[float | None, str | None, bool]:
    """First-year price, currency and premium flag from search's Price text.

    Docs example: '2.60 in USD and domain is not premium'.
    Live sandbox (24 Nov 2026): 'Registration Price: 88.00 in USD and Renewal
    price: 16.00 in USD and Domain is not a Premium Domain' (the first version
    of this parser read that as premium and skipped every name)."""
    text = str(raw or "")
    m = _REG_PRICE.search(text) or _PRICE.search(text)
    if not m:
        return None, None, False
    premium = "premium" in text.lower() and not _NOT_PREMIUM.search(text)
    return float(m.group(1).replace(",", "")), m.group(2).upper(), premium


def _body(data: Any) -> dict:
    """api3 wraps every answer in one named object ({"SearchResponse": {...}})."""
    if isinstance(data, dict) and len(data) == 1:
        (only,) = data.values()
        if isinstance(only, dict):
            return only
    return data if isinstance(data, dict) else {}


def _refusal(body: dict) -> str | None:
    """The error text when Dynadot said no; None when it said yes."""
    code = body.get("ResponseCode", body.get("SuccessCode"))
    status = str(body.get("Status") or "").lower()
    if str(code) not in ("0", "None") or status == "error" or (code is None and body.get("Error")):
        return str(body.get("Error") or body.get("Status") or f"ResponseCode {code}")[:300]
    return None


class DynadotClient:
    """Thin sync wrapper. ``with DynadotClient() as dd: dd.search("a.co")``."""

    def __init__(self, key: str | None = None, *, sandbox: bool | None = None, timeout: float = 90.0,
                 transport: httpx.BaseTransport | None = None, sleep=time.sleep) -> None:
        self.sandbox = sandbox_mode() if sandbox is None else sandbox
        self._key = (key if key is not None else os.getenv(key_name(self.sandbox), "")).strip()
        self._url = SANDBOX_URL if self.sandbox else LIVE_URL
        self._timeout = timeout
        self._transport = transport
        self._sleep = sleep
        self._http: httpx.Client | None = None
        self._last = 0.0

    def __enter__(self) -> "DynadotClient":
        if not self._key:
            raise DynadotError(f"{key_name(self.sandbox)} is not set")
        self._http = httpx.Client(timeout=self._timeout, transport=self._transport,
                                  headers={"User-Agent": "infrabot/1.0", "Accept": "application/json"})
        return self

    def __exit__(self, *exc: object) -> None:
        if self._http:
            self._http.close()
            self._http = None

    def _call(self, command: str, *, retry: bool = True, **params: Any) -> dict:
        """One api3 command. Error messages name the command only, never the URL."""
        assert self._http, "use `with DynadotClient() as dd:`"
        attempts = 4 if retry else 1
        query = {"key": self._key, "command": command, **{k: v for k, v in params.items() if v is not None}}
        for attempt in range(attempts):
            wait = _MIN_GAP - (time.monotonic() - self._last)
            if wait > 0:
                self._sleep(wait)
            self._last = time.monotonic()
            try:
                r = self._http.get(self._url, params=query)
            except (httpx.TransportError, httpx.TimeoutException) as exc:
                if attempt < attempts - 1:
                    self._sleep(min(2 ** attempt, 8))
                    continue
                raise DynadotOutcomeUnknown(f"{command}: no answer ({type(exc).__name__})") from None
            if r.status_code == 429 and attempt < attempts - 1:
                self._sleep(min(2 ** attempt * 5, 60))
                continue
            if r.status_code >= 500:
                if attempt < attempts - 1:
                    self._sleep(min(2 ** attempt, 8))
                    continue
                raise DynadotOutcomeUnknown(f"{command}: HTTP {r.status_code}")
            if r.status_code >= 400:
                raise DynadotRefused(f"{command}: HTTP {r.status_code}")
            try:
                body = _body(r.json())
            except ValueError:
                if command == "register":
                    raise DynadotOutcomeUnknown(f"{command}: answer was not JSON") from None
                raise DynadotRefused(f"{command}: answer was not JSON") from None
            err = _refusal(body)
            if err:
                raise DynadotRefused(f"{command}: {err}")
            return body
        raise DynadotOutcomeUnknown(f"{command}: rate-limited on every try")  # pragma: no cover

    # ── READ ─────────────────────────────────────────────────────────────
    def search(self, domain: str) -> dict:
        """Availability + first-year price of one domain (regular accounts: 1 per call)."""
        body = self._call("search", domain0=domain, show_price=1, currency="USD")
        rows = body.get("SearchResults") or body.get("SearchResponse") or []
        row = next((x for x in rows if str(x.get("DomainName", "")).lower() == domain), rows[0] if rows else {})
        price, currency, premium = parse_price(row.get("Price"))
        return {"domain": domain,
                "available": str(row.get("Available", "")).lower() == "yes",
                "price_usd": price if currency == "USD" else None,
                "currency": currency, "premium": premium,
                "price_text": str(row.get("Price") or "")[:120],
                "error": row.get("Error") or None}

    def balance(self) -> dict[str, float]:
        body = self._call("get_account_balance")
        out: dict[str, float] = {}
        for b in body.get("BalanceList") or []:
            try:
                out[str(b.get("Currency", "")).upper()] = float(str(b.get("Amount", "0")).replace(",", ""))
            except ValueError:
                continue
        return out

    def owned(self, domain: str) -> dict | None:
        """The domain's record when it is in our account; None when Dynadot says it isn't.
        Raises DynadotOutcomeUnknown when Dynadot can't be reached."""
        try:
            body = self._call("domain_info", domain=domain)
        except DynadotRefused:
            return None
        info = body.get("DomainInfo") or {}
        return info if str(info.get("Name", "")).lower() == domain else None

    # ── SPEND ────────────────────────────────────────────────────────────
    def register(self, domain: str, *, approve: bool, duration: int = 1) -> dict:
        """Buys one domain from the balance. Never retried here: a lost answer is
        settled by ``owned()``, not by sending the order again."""
        if not approve:
            raise DynadotBlocked("register spends the balance: pass approve=True only after a human "
                                 "confirmed the names, the price and the total")
        if not self.sandbox and not spend_allowed():
            raise DynadotBlocked("DYNADOT_ALLOW_SPEND is not 'true': buying is switched off")
        body = self._call("register", retry=False, domain=domain, duration=duration, currency="USD")
        return {"domain": domain, "status": str(body.get("Status") or ""),
                "expiration_ms": body.get("Expiration")}
