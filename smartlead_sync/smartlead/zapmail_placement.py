"""Placement-test fleet loop: read the fleet's deliverability, run tests gated.

Read-only:
  * :func:`placement_status` — credits, subscriptions, aggregate report.
  * :func:`eligible_mailboxes` — mailboxes that can be tested.
  * :func:`test_report` — one cart order's detailed result.

Gated (SPEND):
  * :func:`run_test` — create a placement test on selected mailboxes.

``run_test`` is dry-run unless ``approve=True``; a real run also needs the
``ZAPMAIL_ALLOW_SPEND`` kill-switch (checked here and again by the client) and
a strictly-resolved account. This is the automated replacement for the manual
per-inbox testing the team does today.
"""

from __future__ import annotations

from smartlead.zapmail import ZapmailClient, ZapmailSpendBlocked, spend_allowed
from smartlead.zapmail_accounts import open_client, resolve_account_for_client

# Zapmail's placement seeds. "google"/"microsoft365" are the documented values.
DEFAULT_SEEDS: tuple[str, ...] = ("google",)


async def placement_status(*, client: str | None = None) -> dict:
    """Credits + subscriptions + aggregate report. READ-ONLY."""
    async with open_client(client) as z:
        credits = await z.placement_credits()
        subs = await z.placement_subscriptions()
        overall = await z.placement_overall_report()
    return {"credits": credits, "subscriptions": subs, "overall": overall}


async def eligible_mailboxes(*, limit: int = 100, status: str | None = None,
                             client: str | None = None) -> dict:
    """Mailboxes eligible for placement testing. READ-ONLY."""
    async with open_client(client) as z:
        return await z.placement_eligible_mailboxes(page=1, limit=limit,
                                                    status=status)


async def test_report(cart_order_id: str | int, *, client: str | None = None) -> dict:
    """Detailed report for one placement test. READ-ONLY."""
    async with open_client(client) as z:
        return await z.placement_report(cart_order_id)


async def run_test(
    mailbox_ids: list[str],
    *,
    test_name: str = "Fleet placement test",
    seeds: tuple[str, ...] = DEFAULT_SEEDS,
    placement_type: str = "ONE_TIME",
    approve: bool = False,
    client: str | None = None,
) -> dict:
    """Create a placement test. SPEND; dry-run unless approved.

    ``placement_type`` is ``MONTHLY`` (draws on subscription credits) or
    ``ONE_TIME`` ($2/mailbox from wallet). ``mailbox_ids`` must be mailbox ids,
    not emails — resolve them from :func:`eligible_mailboxes`.
    """
    if not mailbox_ids:
        return {"ok": False, "error": "no mailbox ids given"}
    if not approve:
        return {"dry_run": True, "placement_type": placement_type,
                "test_name": test_name, "seeds": list(seeds),
                "mailbox_count": len(mailbox_ids), "mailbox_ids": mailbox_ids,
                "spend_allowed": spend_allowed()}
    if not spend_allowed():
        raise ZapmailSpendBlocked(
            "refusing to run a placement test: ZAPMAIL_ALLOW_SPEND is not 'true'.")
    account = resolve_account_for_client(client, strict=True)
    if account is None:
        raise ZapmailSpendBlocked(
            f"refusing to run a placement test: client={client!r} has no "
            "configured Zapmail account.")
    async with ZapmailClient(api_key=account.api_key,
                             workspace_key=account.workspace_key) as z:
        return await z.purchase_placement_test(
            placement_type=placement_type, test_name=test_name,
            mailbox_ids=mailbox_ids, seed_accounts=list(seeds), approve=True)