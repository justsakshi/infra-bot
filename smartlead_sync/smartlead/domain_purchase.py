"""Staged, approval-gated domain purchase.

The one operation here with money behind it, so it is written to *default to
doing nothing*. Buying requires all of these to line up:

  1. ``stage`` runs first and reports availability + price + total without
     spending — so a human sees exactly what will cost how much.
  2. The caller passes an explicit ``approve=True`` to ``execute``.
  3. The ``ZAPMAIL_ALLOW_SPEND`` kill-switch env var is ``"true"`` (it is off
     anywhere else, local machines included). :class:`ZapmailClient` enforces
     it again on every SPEND call.
  4. The client resolves STRICTLY to a configured Zapmail account — no silent
     fallback to the primary wallet.
  5. A live (uncached) availability + price re-check passes: every name still
     available, every price known, none above the ceiling, none above the
     price that was staged. (Zapmail's /buy does NOT check the registry before
     charging — a taken name is refunded after the fact — so this pre-check is
     the only place a taken name is caught before money moves.)
  6. The account's wallet alone covers the batch total; otherwise /buy would
     return "Domains purchased" with an unpaid invoice and register nothing.

The only caller that should buy is :func:`domain_batch.execute_one`, which adds
the ledger, the stagger date, and the double-buy protection on top. Nothing in
the Slack path reaches :func:`execute_purchase`.
"""

from __future__ import annotations

from dataclasses import dataclass

from smartlead.domain_availability import (
    BULK_MAX_NAMES, DEFAULT_PRICE_CEILING_USD, check_availability_bulk,
)
from smartlead.zapmail import ZapmailClient, ZapmailSpendBlocked, spend_allowed
from smartlead.zapmail_accounts import (
    api_key_for_client, resolve_account_for_client,
)

__all__ = [
    "PurchaseItem", "PurchasePlan", "execute_purchase", "normalize_domains",
    "spend_allowed", "stage_purchase", "verify_prices",
]

# A fresh price may drift by rounding; anything above this over the staged
# price means the plan a human approved is no longer the plan being bought.
PRICE_TOLERANCE_USD: float = 0.01


def normalize_domains(domains: list[str]) -> list[str]:
    """Lowercase, strip, drop blanks and duplicates, keep order."""
    seen: set[str] = set()
    out: list[str] = []
    for d in domains:
        d = (d or "").strip().lower()
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return out


@dataclass(frozen=True)
class PurchaseItem:
    domain: str
    available: bool | None
    price_usd: float | None


@dataclass
class PurchasePlan:
    """What a buy would cost, computed from a read-only availability check."""

    items: list[PurchaseItem]
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD

    @property
    def unknowns(self) -> list[str]:
        """Availability unknown, or available with no price to check."""
        return [i.domain for i in self.items
                if i.available is None or (i.available and i.price_usd is None)]

    @property
    def unavailable(self) -> list[str]:
        return [i.domain for i in self.items if i.available is False]

    @property
    def over_ceiling(self) -> list[str]:
        return [i.domain for i in self.items
                if i.available and i.price_usd is not None
                and i.price_usd > self.price_ceiling]

    @property
    def ready(self) -> list[PurchaseItem]:
        """Available, priced, and under the ceiling — the only buyable rows."""
        return [i for i in self.items
                if i.available is True and i.price_usd is not None
                and i.price_usd <= self.price_ceiling]

    @property
    def estimated_usd(self) -> float:
        return round(sum(i.price_usd or 0.0 for i in self.ready), 2)

    def to_dict(self) -> dict:
        return {
            "domains": [
                {"domain": i.domain, "available": i.available,
                 "price": i.price_usd}
                for i in self.items
            ],
            "buyable": [i.domain for i in self.ready],
            "unavailable": self.unavailable,
            "unknown": self.unknowns,
            "over_ceiling": self.over_ceiling,
            "price_ceiling": self.price_ceiling,
            "estimated_annual_usd": self.estimated_usd,
            "spend_allowed": spend_allowed(),
        }


async def stage_purchase(
    domains: list[str],
    *,
    client: str | None = None,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
) -> PurchasePlan:
    """Availability + price for a list of domains. Read-only, costs nothing.

    ``client`` scopes the check to that client's Zapmail account (via
    :mod:`zapmail_accounts`), so a BettrData purchase is staged against the
    BettrData tenant, not the primary account.
    """
    domains = normalize_domains(domains)
    api_key = api_key_for_client(client)
    checks = await check_availability_bulk(domains, api_key=api_key)
    items = []
    for d in domains:
        available, price = checks.get(d, (None, None))
        items.append(PurchaseItem(domain=d, available=available, price_usd=price))
    return PurchasePlan(items, price_ceiling=price_ceiling)


async def verify_prices(
    domains: list[str],
    *,
    api_key: str,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
    expected_prices: dict[str, float | None] | None = None,
) -> dict[str, float]:
    """Live pre-purchase re-check. Returns ``{domain: price}`` or refuses.

    Bypasses the cache on purpose. Refuses (``ZapmailSpendBlocked``) when any
    name is taken or unknown, has no price, costs more than ``price_ceiling``,
    or costs more than the price recorded when the plan was staged.
    """
    max_calls = -(-len(domains) // BULK_MAX_NAMES)  # ceil
    fresh = await check_availability_bulk(
        domains, api_key=api_key, use_cache=False, max_calls=max_calls)
    problems: list[str] = []
    prices: dict[str, float] = {}
    for d in domains:
        available, price = fresh.get(d, (None, None))
        if available is not True:
            problems.append(f"{d}: {'taken' if available is False else 'availability unknown'}")
            continue
        if price is None:
            problems.append(f"{d}: no price returned")
            continue
        if price > price_ceiling:
            problems.append(f"{d}: ${price:.2f} > ceiling ${price_ceiling:.2f}")
            continue
        staged = (expected_prices or {}).get(d)
        if staged is not None and price > staged + PRICE_TOLERANCE_USD:
            problems.append(f"{d}: ${price:.2f} now vs ${staged:.2f} staged")
            continue
        prices[d] = price
    if problems:
        raise ZapmailSpendBlocked(
            "refusing to spend: live re-check failed — " + "; ".join(problems))
    return prices


async def execute_purchase(
    domains: list[str],
    *,
    years: int = 1,
    use_wallet: bool = True,
    approve: bool = False,
    client: str | None = None,
    price_ceiling: float = DEFAULT_PRICE_CEILING_USD,
    expected_prices: dict[str, float | None] | None = None,
) -> dict:
    """Buy domains. Refuses unless every gate in the module docstring passes.

    Prefer :func:`domain_batch.execute_one` — it is the ledgered path. Returns
    ``{account, domains, verified_prices, response}`` where ``response`` is the
    raw Zapmail buy response.
    """
    if not approve or not spend_allowed():
        raise ZapmailSpendBlocked(
            "refusing to spend: purchase requires both approve=True and "
            "ZAPMAIL_ALLOW_SPEND=true (it is off by default everywhere).")
    domains = normalize_domains(domains)
    if not domains:
        raise ZapmailSpendBlocked("refusing to spend: no domains given.")
    account = resolve_account_for_client(client, strict=True)
    if account is None:
        raise ZapmailSpendBlocked(
            f"refusing to spend: client={client!r} does not resolve to a "
            "configured Zapmail account (check ZAPMAIL_CLIENT_ACCOUNTS and the "
            "matching ZAPMAIL_API_KEY_<NAME>; spends never fall back to the "
            "primary account).")
    prices = await verify_prices(
        domains, api_key=account.api_key, price_ceiling=price_ceiling,
        expected_prices=expected_prices)
    total = round(sum(prices.values()) * max(1, years), 2)
    async with ZapmailClient(
        api_key=account.api_key, workspace_key=account.workspace_key,
    ) as z:
        if use_wallet:
            await require_wallet_covers(z, total, account.name)
        response = await z.buy_domains(
            domains, years=years, use_wallet=use_wallet, approve=True)
    return {"account": account.name, "domains": domains,
            "verified_prices": prices, "total_usd": total,
            # Zapmail's success shape (support, 2026-09-29):
            # {"message": "Domains purchased", "invoiceLink": "<url>"} — no
            # domain ids; registration then runs async (PENDING -> ACTIVE), and
            # a name the registry rejects is refunded to the wallet and removed.
            "invoice_link": (response or {}).get("invoiceLink"),
            "message": (response or {}).get("message"),
            "response": response}


async def require_wallet_covers(z: ZapmailClient, total: float, account: str) -> float:
    """Refuse unless the wallet alone covers ``total``. Returns the balance.

    Zapmail (support, 2026-09-29): a short wallet does NOT make /buy fail. The
    whole cart goes on one Stripe invoice, the wallet pays what it can, and
    the call still answers "Domains purchased" with a link to an UNPAID
    invoice for the rest — nothing registers until someone pays it, and the
    pending order is dropped after ~5 minutes. So "success" would be a lie
    unless the balance is checked first.
    """
    wallet = await z.get_wallet_balance()
    try:
        balance = float((wallet or {}).get("walletBalance"))
    except (TypeError, ValueError):
        raise ZapmailSpendBlocked(
            f"refusing to spend: could not read the {account} wallet balance.")
    if balance + 1e-9 < total:
        raise ZapmailSpendBlocked(
            f"refusing to spend: {account} wallet has ${balance:.2f}, this batch "
            f"costs ${total:.2f}. Top up first — Zapmail would otherwise issue "
            "an unpaid invoice and register nothing.")
    return balance
