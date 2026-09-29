"""Export created mailboxes to Smartlead (or another app), with a safe default.

Completes the domain lifecycle: after domains are bought, connected, and given
mailboxes, this pushes those mailboxes into Smartlead.

One honest caveat, flagged in docs/ZAPMAIL_QUESTIONS_2026-09.md (Q7): Zapmail's
docs do not state which credential the ``SMARTLEAD`` app expects in
``email``/``password`` (Smartlead API key vs login), and the export trigger's
documented response does not include the ``exportId`` that
``GET /v2/exports/status`` needs. So this module:

  * lists/creates the third-party account (so the account exists for export),
  * selects mailboxes by EXACT domain (Zapmail's ``contains`` filter is a
    substring match — ``acme.com`` would also export ``getacme.com``), and
    exports them by id,
  * pins the export to one third-party account (refuses to guess when the
    Zapmail account has several Smartlead accounts registered — each client
    has its own Smartlead),
  * and, if an export id can be read off the response, polls its status —
    otherwise it returns the raw trigger response for a human to confirm.

Everything here is WRITE (no new money) and dry-run unless ``approve=True``.
"""

from __future__ import annotations

import asyncio
import time

from smartlead.domain_lifecycle import domain_provider
from smartlead.zapmail import ZapmailClient, ZapmailError
from smartlead.zapmail_accounts import export_target_for_client, open_client

SMARTLEAD_APP = "SMARTLEAD"


def _extract_export_id(resp: dict | None) -> int | str | None:
    """Best-effort pull of an export id from the trigger response."""
    if not isinstance(resp, dict):
        return None
    data = resp.get("data")
    for container in (resp, data):
        if not isinstance(container, dict):
            continue
        for key in ("exportId", "export_id"):
            if container.get(key) is not None:
                return container[key]
    # A bare "id" is only trusted inside ``data`` — at the top level it could
    # be anything (request id, account id) and would poll the wrong export.
    if isinstance(data, dict) and data.get("id") is not None:
        return data["id"]
    return None


async def list_accounts(app: str = SMARTLEAD_APP, *, client: str | None = None) -> dict:
    """Read-only: third-party accounts registered for an app."""
    async with open_client(client) as z:
        return await z.list_third_party_accounts(app)


def _accounts(resp: dict | None) -> list[dict]:
    return [a for a in ((resp or {}).get("data") or {}).get("accounts") or []
            if isinstance(a, dict)]


def _account_id(acc: dict) -> str | None:
    for key in ("id", "accountId", "thirdPartyAccountId"):
        if acc.get(key) is not None:
            return str(acc[key])
    return None


async def ensure_account(
    email: str,
    password: str,
    *,
    app: str = SMARTLEAD_APP,
    client: str | None = None,
    approve: bool = False,
) -> dict:
    """Make sure an export account for THIS email exists; add it if not.

    Matching is by email — another client's Smartlead account being present
    does not count. Dry-run when ``approve`` is False.
    """
    want = email.strip().lower()
    async with open_client(client) as z:
        accounts = _accounts(await z.list_third_party_accounts(app))
        match = [a for a in accounts
                 if str(a.get("email", "")).strip().lower() == want]
        if match:
            return {"app": app, "ok": True, "existing": match,
                    "account_id": _account_id(match[0]),
                    "detail": "account already present"}
        if not approve:
            return {"app": app, "ok": None, "dry_run": True,
                    "would_add": {"email": email, "app": app},
                    "other_accounts": [a.get("email") for a in accounts]}
        await z.add_third_party_account(email, password, app, approve=True)
        return {"app": app, "ok": True, "added": {"email": email, "app": app}}


async def _mailbox_ids_for_domain(
    z: ZapmailClient, domain: str, *, status: str | None,
) -> tuple[list[str], list[str]]:
    """``(ids, emails)`` of mailboxes on exactly ``domain``."""
    resp = await z.list_mailboxes(contains=domain, page=1, limit=50)
    data = (resp or {}).get("data") or {}
    ids: list[str] = []
    emails: list[str] = []
    for d in data.get("domains") or []:
        for mb in d.get("mailboxes") or []:
            email = str(mb.get("email", "")).strip().lower()
            if email.partition("@")[2] != domain:
                continue
            if status and str(mb.get("status", "")).upper() != status.upper():
                continue
            mid = mb.get("id") or mb.get("mailboxId")
            if mid is None:
                continue
            ids.append(str(mid))
            emails.append(email)
    return ids, emails


async def _pick_account_id(
    z: ZapmailClient, app: str, requested: str | None,
) -> tuple[str | None, str | None]:
    """``(account_id, error)`` — the third-party account to export into.

    Never guessed, not even when only one is registered: live 2026-09-28 the
    Precise Leads Zapmail account's ONLY Smartlead target was BettrData's
    Smartlead (amanda@bettrdata.io), so "the only one" would have exported
    Precise Leads mailboxes into BettrData's Smartlead. The target comes from
    ``--third-party-account-id`` or the client's ZAPMAIL_EXPORT_TARGETS entry.
    """
    accounts = _accounts(await z.list_third_party_accounts(app))
    listing = ", ".join(f"{a.get('email')} (id={_account_id(a)})" for a in accounts)
    if not requested:
        if not accounts:
            return None, f"no {app} account registered — run --ensure-account first"
        return None, (f"no export target set for this client. Registered {app} "
                      f"accounts: {listing}. Pass --third-party-account-id or add "
                      "the client to ZAPMAIL_EXPORT_TARGETS")
    if any(_account_id(a) == str(requested) for a in accounts):
        return str(requested), None
    return None, (f"third-party account {requested} is not registered for {app} "
                  f"(registered: {listing or 'none'})")


async def export_domain(
    domain: str,
    *,
    app: str = SMARTLEAD_APP,
    status: str = "ACTIVE",
    third_party_account_id: str | None = None,
    client: str | None = None,
    approve: bool = False,
    poll: bool = True,
    timeout_s: float = 600.0,
    interval_s: float = 15.0,
) -> dict:
    """Export exactly this domain's mailboxes to an app, by mailbox id.

    Dry-run when ``approve`` is False (reads only: resolves the mailboxes and
    target account so the preview shows exactly what would move). When
    ``poll`` and an export id is available, polls ``/v2/exports/status``.
    """
    domain = domain.strip().lower()
    try:
        provider = await domain_provider(domain, client=client)
    except ZapmailError as exc:
        return {"domain": domain, "app": app, "ok": False, "error": str(exc)[:200]}
    if provider is None:
        return {"domain": domain, "app": app, "ok": False,
                "error": "domain not on this client's Zapmail account"}
    async with open_client(client, provider=provider) as z:
        try:
            ids, emails = await _mailbox_ids_for_domain(z, domain, status=status)
            account_id, acc_err = await _pick_account_id(
                z, app, third_party_account_id or export_target_for_client(client))
        except ZapmailError as exc:
            return {"domain": domain, "app": app, "ok": False,
                    "error": str(exc)[:200]}
        if not ids:
            return {"domain": domain, "app": app, "ok": False,
                    "error": " ".join(filter(None, [
                        "no", status, "mailboxes found on exactly", domain]))}
        if acc_err:
            return {"domain": domain, "app": app, "ok": False, "error": acc_err}
        if not approve:
            return {"domain": domain, "app": app, "ok": None, "dry_run": True,
                    "selection": {"mailboxes": emails,
                                  "third_party_account_id": account_id}}
        try:
            resp = await z.export_mailboxes(
                [app], ids=ids, third_party_account_id=account_id, approve=True)
        except ZapmailError as exc:
            return {"domain": domain, "app": app, "ok": False,
                    "error": str(exc)[:200]}

        export_id = _extract_export_id(resp)
        if not (poll and export_id):
            return {"domain": domain, "app": app, "ok": True,
                    "export_id": export_id, "raw": resp,
                    "detail": ("triggered; status not polled"
                               + ("" if export_id else " (no exportId in response)"))}

        status_now = {"export_id": export_id, "status": "unknown"}
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                s = await z.get_export_status(export_id)
            except ZapmailError as exc:
                return {"domain": domain, "app": app, "ok": False,
                        "export_id": export_id, "error": str(exc)[:200]}
            status_now = (s or {}).get("data") or status_now
            if str(status_now.get("status", "")).lower() in ("completed", "failed"):
                break
            await asyncio.sleep(interval_s)
        done = str(status_now.get("status", "")).lower() == "completed"
        return {"domain": domain, "app": app, "ok": done,
                "export_id": export_id, "status": status_now}