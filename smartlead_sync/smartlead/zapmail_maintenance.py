"""Renewal + tag maintenance. Read-only preview; gated actions.

Renewal is SPEND (deducts wallet / returns a payment link); auto-renew and tags
are WRITE. All actions are dry-run unless ``approve=True``; renewal additionally
requires the ``ZAPMAIL_ALLOW_SPEND`` kill-switch (checked here and again by the
client) and a strictly-resolved account — never the primary wallet by accident.

Read-only:
  * :func:`renewals_preview` — expiring domains + renewal price.

Gated:
  * :func:`renew_now` — renew specific domains.
  * :func:`set_auto_renew` — toggle auto-renewal.
  * :func:`ensure_tag` / :func:`assign_tag` — tag management.
"""

from __future__ import annotations

from smartlead.zapmail import (
    PROVIDERS, ZapmailClient, ZapmailError, ZapmailSpendBlocked, spend_allowed,
)
from smartlead.zapmail_accounts import open_client, resolve_account_for_client


def _renewal_price_map(price_resp: dict | None) -> dict[str, object]:
    data = (price_resp or {}).get("data")
    rows = data if isinstance(data, list) else (data or {}).get("domains") or []
    out: dict[str, object] = {}
    for r in rows or []:
        if isinstance(r, dict) and r.get("domainId"):
            out[str(r["domainId"])] = r.get("renewPrice")
    return out


async def renewals_preview(*, client: str | None = None) -> list[dict]:
    """Domains expiring ≤2 months + their renewal price, both providers. READ-ONLY."""
    out: list[dict] = []
    for provider in PROVIDERS:
        async with open_client(client, provider=provider) as z:
            soon = await z.list_renewal_soon(page=1, limit=200)
            domains = ((soon or {}).get("data") or {}).get("domains") or []
            ids = [str(d.get("id")) for d in domains if d.get("id")]
            prices: dict[str, object] = {}
            if ids:
                try:
                    prices = _renewal_price_map(await z.get_renewal_price(domain_ids=ids))
                except ZapmailError:
                    prices = {}
        out += [{
            "domain": d.get("domain"),
            "domain_id": d.get("id"),
            "provider": provider,
            "expire_on": str(d.get("expireOn", ""))[:10],
            "renew_price": prices.get(str(d.get("id"))),
        } for d in domains]
    return _only_client(out, client) if client else out


def _only_client(rows: list[dict], client: str) -> list[dict]:
    """Keep the client's own domains. ``client`` picks the Zapmail ACCOUNT,
    and one account holds several clients (Precise Leads holds Melior,
    BettrData and past clients), so without this ``--client Melior`` listed
    every renewal on that account."""
    from smartlead.zapmail_accounts import resolve_account_for_client
    from smartlead.zapmail_asset_sync import read_tracker
    from smartlead.zapmail_clients import client_from_tracker, infer_client

    want = client_from_tracker(client)
    if want is None:
        return []
    account = resolve_account_for_client(client)
    try:
        tracker = read_tracker()
    except Exception:  # noqa: BLE001 - names still identify most domains
        tracker = {}
    out = []
    for r in rows:
        who = infer_client(r["domain"] or "", account.name if account else None,
                           (tracker.get(str(r["domain"]).lower()) or {}).get("client"))
        if who == want:
            out.append({**r, "client": who})
    return out


async def renew_now(domain_ids: list[str], *, approve: bool = False,
                    client: str | None = None) -> dict:
    """Renew specific domains. SPEND; dry-run unless approved + kill-switch on."""
    domain_ids = [d for d in (domain_ids or []) if str(d).strip()]
    if not domain_ids:
        raise ZapmailSpendBlocked("renew_now needs explicit domain ids.")
    if not approve:
        return {"dry_run": True, "would_renew": domain_ids,
                "spend_allowed": spend_allowed()}
    if not spend_allowed():
        raise ZapmailSpendBlocked(
            "refusing to renew: ZAPMAIL_ALLOW_SPEND is not 'true'.")
    account = resolve_account_for_client(client, strict=True)
    if account is None:
        raise ZapmailSpendBlocked(
            f"refusing to renew: client={client!r} has no configured Zapmail account.")
    async with ZapmailClient(api_key=account.api_key,
                             workspace_key=account.workspace_key) as z:
        return await z.renew_domains(domain_ids, approve=True)


async def set_auto_renew(domain_ids: list[str], enabled: bool, *,
                         approve: bool = False, client: str | None = None) -> dict:
    """Toggle auto-renewal on domains. WRITE; dry-run unless approved."""
    if not approve:
        return {"dry_run": True, "would_set_auto_renew": enabled,
                "domains": domain_ids}
    async with open_client(client) as z:
        return await z.update_auto_renew(domain_ids, enabled, approve=True)


async def ensure_tag(name: str, *, color: str = "#625B97",
                     approve: bool = False, client: str | None = None) -> dict:
    """Return an existing tag id for *name*, or create it. WRITE when creating."""
    async with open_client(client) as z:
        tags = await z.list_tags()
        for t in tags or []:
            if str(t.get("name", "")).strip().lower() == name.strip().lower():
                return {"tag_id": t.get("id"), "name": t.get("name"),
                        "created": False}
        if not approve:
            return {"dry_run": True, "would_create": {"name": name, "color": color}}
        res = await z.create_tags([{"name": name, "tagColor": color}], approve=True)
        ids = ((res or {}).get("data") or {}).get("tagIds") or []
        return {"tag_id": ids[0] if ids else None, "name": name, "created": True}


async def assign_tag(tag_id: str, domain_ids: list[str], *,
                     approve: bool = False, client: str | None = None) -> dict:
    """Assign a tag to domains. WRITE; dry-run unless approved."""
    if not approve:
        return {"dry_run": True, "tag_id": tag_id, "domains": domain_ids}
    async with open_client(client) as z:
        return await z.assign_tag([tag_id], domain_ids, approve=True)