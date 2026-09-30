"""Retry failed inboxes and retire inboxes at renewal (Zapmail). WRITE, free.

* Retry: Zapmail's own retry for a domain's FAILED mailboxes.
* Retire: mailboxes are removed at their NEXT renewal - they keep working
  until then, the slot stops being billed after it, the domain stays. Can be
  undone before the renewal. Zapmail requires every mailbox on a domain when
  its admin mailbox is included; its message says so if that happens.

Dry-run unless ``approve``: the plan (which inboxes, which account) is shown
first, the same way every other change in Infra Bot works.
"""

from __future__ import annotations

from smartlead.zapmail import ZapmailError


async def _hit(domain: str, client: str) -> dict | None:
    from smartlead.zapmail_accounts import require_account
    from smartlead.zapmail_fleet import locate_domain
    acc = require_account(client).name
    hits = [h for h in await locate_domain(domain) if h.get("account") == acc and not h.get("error")]
    return hits[0] if hits else None


async def retry_failed(domain: str, *, client: str, approve: bool = False) -> dict:
    """Retry the FAILED mailboxes on one domain of ``client``'s account."""
    from smartlead.zapmail_accounts import open_client
    domain = domain.strip().lower()
    h = await _hit(domain, client)
    if not h:
        return {"domain": domain, "ok": False, "error": f"{domain} is not on {client}'s Zapmail account"}
    failed = [m["email"] for m in h.get("mailboxes") or [] if str(m.get("status")).upper() == "FAILED"]
    if not failed:
        return {"domain": domain, "ok": True, "detail": "no failed inboxes on this domain", "retried": []}
    if not approve:
        return {"domain": domain, "ok": None, "dry_run": True, "would_retry": failed}
    try:
        async with open_client(client, provider=h["provider"]) as z:
            await z.retry_failed_mailboxes([h["domain_id"]], approve=True)
    except ZapmailError as exc:
        return {"domain": domain, "ok": False, "error": str(exc)[:200]}
    return {"domain": domain, "ok": True, "retried": failed,
            "detail": "Zapmail is re-creating them (Google usually within an hour)"}


async def retire(emails: list[str], *, client: str, approve: bool = False,
                 undo: bool = False) -> dict:
    """Remove these inboxes at their next renewal (``undo`` cancels that)."""
    from smartlead.zapmail_accounts import open_client
    emails = sorted({e.strip().lower() for e in emails if "@" in e})
    if not emails:
        return {"ok": False, "error": "give at least one inbox address"}
    by_provider: dict[str, list[tuple[str, str]]] = {}
    missing = []
    for domain in sorted({e.split("@")[1] for e in emails}):
        h = await _hit(domain, client)
        if not h:
            missing += [e for e in emails if e.endswith("@" + domain)]
            continue
        async with open_client(client, provider=h["provider"]) as z:
            listing = await z.list_mailboxes(contains=domain, page=1, limit=50)
        for d in ((listing or {}).get("data") or {}).get("domains") or []:
            for m in d.get("mailboxes") or []:
                em = str(m.get("email") or "").lower()
                if em in emails:
                    by_provider.setdefault(h["provider"], []).append((em, str(m.get("id"))))
    found = {e for pairs in by_provider.values() for e, _ in pairs}
    missing += [e for e in emails if e not in found and e not in missing]
    if missing:
        return {"ok": False, "error": "not on this client's Zapmail account: " + ", ".join(sorted(missing))}
    action = "cancel the scheduled removal of" if undo else "remove at next renewal"
    if not approve:
        return {"ok": None, "dry_run": True, "action": action, "inboxes": sorted(found)}
    try:
        for provider, pairs in by_provider.items():
            async with open_client(client, provider=provider) as z:
                await z.schedule_mailbox_removal([i for _, i in pairs], remove=not undo, approve=True)
    except ZapmailError as exc:
        return {"ok": False, "error": str(exc)[:200]}
    return {"ok": True, "action": action, "inboxes": sorted(found),
            "detail": ("removal cancelled - they keep renewing" if undo else
                       "they keep working until their renewal date, then are removed and stop being billed")}
