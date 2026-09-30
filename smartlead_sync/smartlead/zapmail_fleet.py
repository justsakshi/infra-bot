"""Read-only Zapmail fleet views across every account AND both providers.

Pure READ: wallet balance, plan/usage, mailbox counts, domain lists, and
domains expiring within 2 months. Nothing here spends money or mutates an
account, so it runs unattended.

**Both providers, always.** Zapmail's list/renewal/mailbox endpoints answer
for GOOGLE only unless the request carries ``x-service-provider: MICROSOFT``
(verified live 2026-09-28: Precise Leads showed 193 domains; the 20 Microsoft
ones only appear with the header). Every function here therefore queries
each account once per provider in :data:`zapmail.PROVIDERS`.

Failures are isolated per account/provider and reported, never swallowed: a
caller passes an ``errors`` list and gets ``"<account>/<provider>: ..."``
entries, so "nothing expiring" is never confused with "could not check".
"""

from __future__ import annotations

from datetime import datetime

from smartlead.zapmail import PROVIDERS, ZapmailClient, ZapmailError
from smartlead.zapmail_accounts import discover_zapmail_accounts

_PAGE_LIMIT = 100
_MAX_PAGES = 50


def _iso_to_date(value: str | None) -> str:
    """``'2026-05-01T00:00:00.000Z'`` -> ``'2026-05-01'``, or ``''``."""
    if not value:
        return ""
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")) \
            .strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return str(value)[:10]


def _client(acc, provider: str) -> ZapmailClient:
    return ZapmailClient(api_key=acc.api_key, workspace_key=acc.workspace_key,
                         service_provider=provider)


async def fleet_status() -> dict:
    """Snapshot of every configured account. READ-ONLY.

    Returns ``{account_name: {ok, email, active_plan, wallet_balance,
    auto_recharge, placement_credits, domains: {GOOGLE, MICROSOFT},
    active_mailboxes: {GOOGLE, MICROSOFT}, assigned_mailboxes,
    purchased_mailboxes, error}}``.
    """
    out: dict[str, dict] = {}
    for acc in discover_zapmail_accounts():
        entry: dict = {"ok": False, "account": acc.name}
        try:
            async with _client(acc, "GOOGLE") as z:
                user = await z.get_user()
                wallet = await z.get_wallet_balance()
                try:
                    credits = await z.placement_credits()
                except ZapmailError:
                    credits = None
            domains: dict[str, int | None] = {}
            active: dict[str, int | None] = {}
            for provider in PROVIDERS:
                async with _client(acc, provider) as z:
                    d = ((await z.list_domains(page=1, limit=1)) or {}).get("data") or {}
                    m = ((await z.list_mailboxes(page=1, limit=1)) or {}).get("data") or {}
                domains[provider] = d.get("totalSearchedCount")
                active[provider] = m.get("totalActiveMailboxes")
        except ZapmailError as exc:
            entry["error"] = str(exc)[:200]
            out[acc.name] = entry
            continue

        ud = (user or {}).get("data") or {}
        entry.update({
            "ok": True,
            "email": ud.get("email", ""),
            "active_plan": ud.get("activePlan", ""),
            "plan_ends_on": _iso_to_date(ud.get("planEndsOn")),
            "wallet_balance": (wallet or {}).get("walletBalance", ud.get("walletBalance")),
            "auto_recharge": (wallet or {}).get("autoRechargeEnabled"),
            "auto_recharge_details": (wallet or {}).get("autoRechargeDetails"),
            "purchased_mailboxes": ud.get("purchasedMailboxes"),
            "assigned_mailboxes": ud.get("assignedMailboxes"),
            "domains": domains,
            "active_mailboxes": active,
            "placement_credits": ((credits or {}).get("data") or {}).get(
                "totalAvailableCredits") if isinstance(credits, dict) else None,
        })
        out[acc.name] = entry
    return out


async def renewals_asof(errors: list[str] | None = None) -> list[dict]:
    """Domains expiring within 2 months, all accounts, both providers. READ-ONLY.

    Returns ``[{domain, domain_id, status, expire_on, provider,
    assigned_mailboxes, account}]``. Failures go to ``errors`` when given.
    """
    rows: list[dict] = []
    for acc in discover_zapmail_accounts():
        for provider in PROVIDERS:
            try:
                async with _client(acc, provider) as z:
                    resp = await z.list_renewal_soon(page=1, limit=200)
            except ZapmailError as exc:
                msg = f"{acc.name}/{provider}: {str(exc)[:160]}"
                print(f"  [Zapmail] renewal-soon failed — {msg}")
                if errors is not None:
                    errors.append(msg)
                continue
            for d in ((resp or {}).get("data") or {}).get("domains") or []:
                rows.append({
                    "domain": str(d.get("domain", "")).strip().lower(),
                    "domain_id": d.get("id", ""),
                    "status": d.get("status", ""),
                    "expire_on": _iso_to_date(d.get("expireOn")),
                    "provider": d.get("serviceProvider") or provider,
                    "assigned_mailboxes": d.get("assignedMailboxesCount", 0),
                    "account": acc.name,
                })
    # Which current client (if any) each belongs to — past-client domains are
    # being allowed to lapse (standup 2026-09-29), so callers treat them apart.
    if rows:
        from smartlead.zapmail_asset_sync import read_tracker
        from smartlead.zapmail_clients import infer_client
        tracker = read_tracker()
        for r in rows:
            r["client"] = infer_client(r["domain"], r["account"],
                                       (tracker.get(r["domain"]) or {}).get("client"))
    return rows


async def all_domains(errors: list[str] | None = None) -> list[dict]:
    """Every domain on every account, both providers (paginated). READ-ONLY.

    Returns ``[{domain, domain_id, status, expire_on, auto_renew, provider,
    assigned_mailboxes, account}]``.
    """
    rows: list[dict] = []
    for acc in discover_zapmail_accounts():
        for provider in PROVIDERS:
            try:
                async with _client(acc, provider) as z:
                    page = 1
                    while page <= _MAX_PAGES:
                        data = ((await z.list_domains(page=page, limit=_PAGE_LIMIT))
                                or {}).get("data") or {}
                        for d in data.get("domains") or []:
                            rows.append({
                                "domain": str(d.get("domain", "")).strip().lower(),
                                "domain_id": d.get("id", ""),
                                "status": d.get("status", ""),
                                "expire_on": _iso_to_date(d.get("expireOn")),
                                "registered_on": _iso_to_date(d.get("registeredOn")),
                                "auto_renew": d.get("autoRenew"),
                                "provider": provider,
                                "assigned_mailboxes": d.get("assignedMailboxesCount", 0),
                                "account": acc.name,
                            })
                        total_pages = int(data.get("totalPages") or 1)
                        if page >= total_pages:
                            break
                        page += 1
            except ZapmailError as exc:
                msg = f"{acc.name}/{provider}: {str(exc)[:160]}"
                print(f"  [Zapmail] domain list failed — {msg}")
                if errors is not None:
                    errors.append(msg)
    return rows


async def all_mailboxes(errors: list[str] | None = None,
                        domain: str | None = None) -> list[dict]:
    """Every mailbox on every account, both providers (paginated). READ-ONLY.

    Returns ``[{email, domain, mailbox_id, status, expire_on, assigned_on,
    warmed_up, provider, account}]``. No credentials: Zapmail's mailbox rows
    carry passwords/app passwords, which are never copied out of here.
    ``domain`` narrows the query (``contains``) for a single-domain refresh.
    """
    rows: list[dict] = []
    for acc in discover_zapmail_accounts():
        for provider in PROVIDERS:
            try:
                async with _client(acc, provider) as z:
                    page = 1
                    while page <= _MAX_PAGES:
                        data = ((await z.list_mailboxes(page=page, limit=_PAGE_LIMIT,
                                                        contains=domain))
                                or {}).get("data") or {}
                        for d in data.get("domains") or []:
                            for m in d.get("mailboxes") or []:
                                email = str(m.get("email") or "").strip().lower()
                                if not email or (domain and email.partition("@")[2] != domain):
                                    continue
                                rows.append({
                                    "email": email,
                                    "domain": email.partition("@")[2],
                                    "mailbox_id": m.get("id"),
                                    "status": m.get("status"),
                                    "expire_on": _iso_to_date(m.get("expireOn")),
                                    "assigned_on": _iso_to_date(m.get("assignedOn")),
                                    "warmed_up": m.get("isWarmedUp"),
                                    "provider": provider,
                                    "account": acc.name,
                                })
                        if page >= int(data.get("totalPages") or 1):
                            break
                        page += 1
            except ZapmailError as exc:
                msg = f"{acc.name}/{provider}: {str(exc)[:160]}"
                print(f"  [Zapmail] mailbox list failed — {msg}")
                if errors is not None:
                    errors.append(msg)
    return rows


async def prewarmed_overview(sample: int = 5) -> dict:
    """Pre-warmed mailboxes: our subscriptions, slots used, and Zapmail's stock.

    READ-ONLY. Live 2026-09-29: PL pays "Growth Addon" $105/mo for 15 Google and
    $84/mo for 12 Microsoft pre-warmed mailboxes (~$7/mailbox/month); stock was
    476 Google / 71 Microsoft. Returns ``{accounts: {name: {PROVIDER:
    {subscriptions, slots, assigned, free}}}, stock: {google, microsoft},
    for_sale: {PROVIDER: [{domain, mailboxes:[names]}]}}``.
    """
    out: dict = {"accounts": {}, "stock": {}, "for_sale": {}}
    for acc in discover_zapmail_accounts():
        per: dict = {}
        for provider in PROVIDERS:
            try:
                async with _client(acc, provider) as z:
                    subs = ((await z.prewarmed_subscriptions()) or {}).get("data") or {}
                    boxes = ((await z.list_mailboxes(page=1, limit=1)) or {}).get("data") or {}
                    if not out["stock"]:
                        out["stock"] = ((await z.prewarmed_count()) or {}).get("data") or {}
                    if provider not in out["for_sale"]:
                        sale = ((await z.prewarmed_domains(page=1, limit=sample))
                                or {}).get("data") or {}
                        out["for_sale"][provider] = [
                            {"domain": d.get("domain"),
                             # id: what an inbox job needs to assign exactly this domain
                             "id": d.get("id"), "price": d.get("price"),
                             "mailboxes": [f"{(m.get('mailbox') or {}).get('firstName', '')} "
                                           f"{(m.get('mailbox') or {}).get('lastName', '')}".strip()
                                           for m in d.get("preWarmedUpMailboxes") or []]}
                            for d in sale.get("domains") or []]
            except ZapmailError as exc:
                per[provider] = {"error": str(exc)[:160]}
                continue
            active = [s for s in subs.get("subscriptions") or []
                      if str(s.get("subscriptionStatus", "")).upper() == "ACTIVE"]
            slots = sum(int(s.get("totalMailboxQuantity") or 0) for s in active)
            assigned = int(boxes.get("totalAssignedPreWarmedUpMailboxes") or 0)
            per[provider] = {
                "subscriptions": [{"plan": s.get("plan"), "price": s.get("price"),
                                   "mailboxes": s.get("totalMailboxQuantity"),
                                   "renews": _iso_to_date(s.get("periodEnd"))}
                                  for s in active],
                "slots": slots, "assigned": assigned, "free": max(0, slots - assigned),
            }
        out["accounts"][acc.name] = per
    return out


async def locate_domain(domain: str) -> list[dict]:
    """Where a domain lives: every account/provider that has it. READ-ONLY.

    Lets someone ask "which account / what state is x.com in?" without knowing
    the client. Returns ``[{account, provider, domain, domain_id, status,
    expire_on, auto_renew, dns_shield, mailboxes: [{email, status,
    warmed_up}]}]``; empty when no account has it.
    """
    domain = domain.strip().lower()
    hits: list[dict] = []
    for acc in discover_zapmail_accounts():
        for provider in PROVIDERS:
            try:
                async with _client(acc, provider) as z:
                    resp = await z.list_domains(contains=domain, page=1, limit=50)
                    rows = [d for d in ((resp or {}).get("data") or {}).get("domains") or []
                            if str(d.get("domain", "")).strip().lower() == domain]
                    # The domain row's embedded mailbox list carries no emails;
                    # the mailbox endpoint is the one with email + status.
                    boxes = await _mailboxes_on(z, domain) if rows else []
                    health = {}
                    if rows:
                        try:
                            h = ((await z.get_domain_health(str(rows[0].get("id"))))
                                 or {}).get("data") or {}
                            health = {"score": h.get("score"), "label": h.get("label"),
                                      "reason": (h.get("reasons") or [None])[0]
                                      if isinstance(h.get("reasons"), list) else None}
                        except ZapmailError:
                            health = {}
            except ZapmailError as exc:
                hits.append({"account": acc.name, "provider": provider,
                             "error": str(exc)[:200]})
                continue
            for d in rows:
                hits.append({
                    "account": acc.name,
                    "provider": provider,
                    "domain": domain,
                    "domain_id": d.get("id"),
                    "status": d.get("status"),
                    "expire_on": _iso_to_date(d.get("expireOn")),
                    "auto_renew": d.get("autoRenew"),
                    "dns_shield": d.get("dnsShieldEnabled"),
                    # 0-100; 0-30 = critical (Zapmail support, 2026-09-29).
                    "health": health,
                    "mailboxes": boxes,
                })
    if hits:
        from smartlead.zapmail_asset_sync import read_tracker
        from smartlead.zapmail_clients import infer_client
        tracked = read_tracker().get(domain) or {}
        for h in hits:
            if not h.get("error"):
                h["tracker_client"] = tracked.get("client")
                h["client"] = infer_client(domain, h["account"], tracked.get("client"))
    return hits


async def _mailboxes_on(z: ZapmailClient, domain: str) -> list[dict]:
    """``[{email, status, warmed_up}]`` for mailboxes on exactly ``domain``."""
    resp = await z.list_mailboxes(contains=domain, page=1, limit=50)
    out: list[dict] = []
    for d in ((resp or {}).get("data") or {}).get("domains") or []:
        for m in d.get("mailboxes") or []:
            email = str(m.get("email") or "").strip().lower()
            if email.partition("@")[2] == domain:
                out.append({"email": email, "status": m.get("status"),
                            "warmed_up": m.get("isWarmedUp")})
    return out
