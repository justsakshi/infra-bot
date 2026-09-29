"""One-step provisioning after a domain is bought: connect → mailboxes → export.

Chains the three manual steps the team does for every new domain, per
domain, stopping at the first step that fails so a half-set-up domain is
reported, never glossed over:

  1. **Connect** — skipped when the domain is already ACTIVE on the client's
     account, on either provider (domains bought on Zapmail usually are).
     Otherwise it joins ``provider`` (GOOGLE or MICROSOFT).
  2. **Mailboxes** — top up to ``per_domain`` mailboxes (Zapmail max 5),
     using real sender names when given; existing usernames are skipped.
  3. **Export** — optional (``export=True``), because the Smartlead export
     path is not yet confirmed live (docs/ZAPMAIL_QUESTIONS_2026-09.md Q7).

WRITE only (no new money), and a dry run unless ``approve=True``. Every call
runs against the client's own Zapmail account (strict routing) — an unmapped
client is refused before anything happens.
"""

from __future__ import annotations

from smartlead.domain_export import export_domain
from smartlead.domain_lifecycle import (
    assign_mailboxes_and_wait, connect_and_wait, connect_status,
)
from smartlead.zapmail import ZapmailError
from smartlead.zapmail_accounts import require_account


async def provision_domain(
    domain: str,
    *,
    client: str | None,
    per_domain: int = 2,
    provider: str = "GOOGLE",
    sender_names: list[str] | None = None,
    export: bool = False,
    third_party_account_id: str | None = None,
    approve: bool = False,
    timeout_s: float | None = None,
    interval_s: float | None = None,
) -> dict:
    """Run the post-purchase steps for one domain. Returns a per-step report.

    ``timeout_s``/``interval_s`` override each step's own defaults (Zapmail's
    recommended 60s connect / 180s mailbox polling) only when given.
    """
    domain = domain.strip().lower()
    poll = {k: v for k, v in (("timeout_s", timeout_s), ("interval_s", interval_s))
            if v is not None}
    account = require_account(client)
    report: dict = {"domain": domain, "account": account.name,
                    "dry_run": not approve, "steps": {}, "ok": False}

    # 1. connect (only if not already live)
    try:
        state = await connect_status(domain, client=client)
    except ZapmailError as exc:
        report["steps"]["connect"] = {"ok": False, "error": str(exc)[:200]}
        return report
    if state.get("connected"):
        report["steps"]["connect"] = {"ok": True, "status": state.get("status"),
                                      "skipped": "already connected"}
    else:
        res = (await connect_and_wait(
            [domain], approve=approve, client=client, provider=provider,
            **poll)).get(domain, {})
        report["steps"]["connect"] = res
        if not approve:
            report["steps"]["mailboxes"] = {"skipped": "runs after connect"}
            return report
        if not res.get("ok"):
            return report

    # 2. mailboxes
    mb = await assign_mailboxes_and_wait(
        domain, count=per_domain, approve=approve, client=client,
        sender_names=sender_names, **poll)
    report["steps"]["mailboxes"] = mb
    if mb.get("ok") is False:
        return report

    # 3. export (opt-in)
    if export and not approve and mb.get("would_create"):
        # New mailboxes don't exist yet in a dry run, so there is nothing to
        # preview an export of; the real run exports once they are ACTIVE.
        report["steps"]["export"] = {
            "skipped": "runs after the new mailboxes are ACTIVE"}
    elif export:
        if approve and mb.get("pending"):
            report["steps"]["export"] = {
                "ok": False, "error": "mailboxes not ACTIVE yet — re-run later"}
            return report
        report["steps"]["export"] = await export_domain(
            domain, client=client, approve=approve,
            third_party_account_id=third_party_account_id)
        if report["steps"]["export"].get("ok") is False:
            return report
    else:
        report["steps"]["export"] = {"skipped": "pass --export to include"}

    report["ok"] = True if approve else None
    return report
