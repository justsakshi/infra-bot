"""Discover Zapmail accounts and map clients to them — mirroring Smartlead.

Zapmail is multi-account exactly like Smartlead: some clients have their own
Zapmail account, others share one (Melior may live in the Precise Leads
account, for instance). Availability, purchase, export, and testing for a
client must run against *that client's* account, or the estate logic queries
and bills the wrong tenant.

Conventions (env), matching ``smartlead/accounts.py``:

    ZAPMAIL_API_KEY             -> primary account (fallback for unmapped clients)
    ZAPMAIL_API_KEY_<NAME>      -> named account (Name taken from the suffix)
    ZAPMAIL_WORKSPACE_KEY_<NAME> -> optional per-account workspace scope

The client -> account mapping is config-driven so it can change as accounts
are split or merged without a code edit. Default lives in
:data:`DEFAULT_CLIENT_ACCOUNTS`; override with ``ZAPMAIL_CLIENT_ACCOUNTS``, a
comma-separated ``Client=Account`` list (case-insensitive):

    ZAPMAIL_CLIENT_ACCOUNTS="Belardi Wong=Belardi Wong,Bettrdata=BettrData,Melior=Precise Leads,Precise Leads=Precise Leads"

A client absent from the map resolves to the primary account. Client names are
normalized the same way ``manager_map`` does (case + underscore-insensitive),
so ``BETTRDATA`` and ``Bettrdata`` collide on purpose.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from smartlead.zapmail import ZapmailClient, ZapmailError


class ZapmailAccountMissing(ZapmailError):
    """A client-scoped action has no Zapmail account to run against."""


def _norm(name: str) -> str:
    return name.strip().lower().replace("_", " ")


@dataclass(frozen=True)
class ZapmailAccount:
    name: str
    api_key: str
    workspace_key: str | None = None

    @property
    def key(self) -> str:
        return self.name

    @property
    def masked_key(self) -> str:
        k = self.api_key
        return f"{k[:4]}...{k[-4:]}" if len(k) > 8 else "****"


# Default client -> Zapmail account. Edit to match the real fleet; the env
# override (ZAPMAIL_CLIENT_ACCOUNTS) always wins. Names here must match the
# suffix of a ZAPMAIL_API_KEY_<NAME> (or the primary account's label).
DEFAULT_CLIENT_ACCOUNTS: dict[str, str] = {
    "Belardi Wong": "Belardi Wong",
    # BettrData lives in the Precise Leads Zapmail account (verified live
    # 2026-09-28: askbettrdata.com, bettrdataco.com etc. are there, and its
    # Smartlead export target is BettrData's Smartlead).
    "Better Data": "Precise Leads",
    "Bettrdata": "Precise Leads",
    "Melior": "Precise Leads",
    "Precise Leads": "Precise Leads",
}

# Default client -> Zapmail third-party (Smartlead) account id to export into.
# One Zapmail account can hold several clients, each with its own Smartlead,
# so this is per client. Ids from `zapmail_export.py --accounts --client X`
# (live 2026-09-28). Not secrets — they only name a target inside our own
# Zapmail accounts. ZAPMAIL_EXPORT_TARGETS overrides. Precise Leads and Melior
# have NO Smartlead target registered in Zapmail yet, so their exports are
# refused until one is added (zapmail_export.py --ensure-account).
DEFAULT_EXPORT_TARGETS: dict[str, str] = {
    "Bettrdata": "cc44e5a8-b4c0-4d00-b9f8-6a9a62225782",     # amanda@bettrdata.io
    "Better Data": "cc44e5a8-b4c0-4d00-b9f8-6a9a62225782",
    "Belardi Wong": "e2d96503-c696-4f13-8f6f-1726c33c83aa",  # saml@belardiwong.com
}

# Primary account label when no explicit name is set anywhere. Set
# ZAPMAIL_PRIMARY_ACCOUNT_NAME (e.g. "Precise Leads") so the plain
# ZAPMAIL_API_KEY can be targeted by name from the client map — strict
# (spend) resolution never falls back to it otherwise.
_PRIMARY_NAME = "Primary"


def primary_account_name() -> str:
    return (os.getenv("ZAPMAIL_PRIMARY_ACCOUNT_NAME") or "").strip() or _PRIMARY_NAME

# Env-var suffixes to skip when scanning for named accounts.
_SKIPPED_SUFFIXES = {"CLIENT_NAME"}


def _parse_client_accounts(raw: str) -> dict[str, str]:
    """Parse ``"Client=Account,Client2=Account2"`` into a normalized map."""
    out: dict[str, str] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        client, sep, account = chunk.partition("=")
        client, account = client.strip(), account.strip()
        if not sep or not client or not account:
            continue
        out[_norm(client)] = _norm(account)
    return out


def client_account_map() -> dict[str, str]:
    """Normalized client -> normalized account name, env override applied."""
    merged = {_norm(k): _norm(v) for k, v in DEFAULT_CLIENT_ACCOUNTS.items()}
    merged.update(_parse_client_accounts(os.getenv("ZAPMAIL_CLIENT_ACCOUNTS", "")))
    return merged


def ignored_accounts() -> set[str]:
    """Zapmail accounts the bot must never touch. Belardi Wong's account is
    theirs and they are no longer a client (2026-10-08): no reads, renewals,
    billing or purchases. Override with ZAPMAIL_IGNORE_ACCOUNTS ("" = none)."""
    raw = os.getenv("ZAPMAIL_IGNORE_ACCOUNTS")
    raw = "Belardi Wong" if raw is None else raw
    return {_norm(x) for x in raw.split(",") if x.strip()}


def discover_zapmail_accounts() -> list[ZapmailAccount]:
    """All Zapmail accounts found in the environment (primary first), minus
    ignored ones (see ``ignored_accounts``)."""
    return [a for a in _all_zapmail_accounts() if _norm(a.name) not in ignored_accounts()]


def _all_zapmail_accounts() -> list[ZapmailAccount]:
    accounts: list[ZapmailAccount] = []

    primary_key = os.getenv("ZAPMAIL_API_KEY")
    if primary_key:
        accounts.append(ZapmailAccount(
            name=primary_account_name(),
            api_key=primary_key.strip(),
            workspace_key=os.getenv("ZAPMAIL_WORKSPACE_KEY") or None,
        ))

    for key, value in sorted(os.environ.items()):
        if not key.startswith("ZAPMAIL_API_KEY_") or key in _SKIPPED_SUFFIXES:
            continue
        suffix = key[len("ZAPMAIL_API_KEY_"):]
        if not suffix or not value.strip():
            continue
        accounts.append(ZapmailAccount(
            name=suffix.strip(),
            api_key=value.strip(),
            workspace_key=os.getenv(f"ZAPMAIL_WORKSPACE_KEY_{suffix}") or None,
        ))
    return accounts


def account_by_name(name: str) -> ZapmailAccount | None:
    want = _norm(name)
    for acc in discover_zapmail_accounts():
        if _norm(acc.name) == want:
            return acc
    return None


def resolve_account_for_client(
    client: str | None, *, strict: bool = False,
) -> ZapmailAccount | None:
    """The Zapmail account a client's domain work should run against.

    Resolution order: client -> account name via the map, then account name ->
    key via discovery, then the primary account as a last resort. Returns None
    only when no key is configured at all.

    ``strict=True`` is for anything that spends money: no fallback. A blank
    client means the primary account; a named client must be in the map AND
    its account's key must be configured, otherwise None. A typo'd client or a
    missing ``ZAPMAIL_API_KEY_<NAME>`` must never bill the primary wallet.
    """
    wanted: str | None = None
    if client and client.strip():
        wanted = client_account_map().get(_norm(client))
        if strict:
            return account_by_name(wanted) if wanted else None
    elif strict:
        return account_by_name(primary_account_name())
    if wanted:
        acc = account_by_name(wanted)
        if acc:
            return acc
    primary = account_by_name(primary_account_name())
    if primary:
        return primary
    # No primary key but maybe named-only accounts: return the first.
    accounts = discover_zapmail_accounts()
    return accounts[0] if accounts else None


def api_key_for_client(client: str | None) -> str | None:
    """Lenient key lookup — ONLY for account-agnostic reads (domain
    availability, AI name suggestions). Anything that reads or changes a
    client's own domains/mailboxes must use :func:`open_client`."""
    acc = resolve_account_for_client(client)
    return acc.api_key if acc else None


def export_target_for_client(client: str | None) -> str | None:
    """Zapmail's third-party (Smartlead) account id this client exports into.

    From ``ZAPMAIL_EXPORT_TARGETS="Client=<thirdPartyAccountId>,..."``, then
    :data:`DEFAULT_EXPORT_TARGETS`. Each client has its own Smartlead, and one
    Zapmail account can hold several clients, so the export target is per
    client, never per Zapmail account. List the ids with
    ``zapmail_export.py --accounts --client X``.
    """
    if not client:
        return None
    for chunk in os.getenv("ZAPMAIL_EXPORT_TARGETS", "").split(","):
        name, sep, target = chunk.partition("=")
        if sep and _norm(name) == _norm(client) and target.strip():
            return target.strip()  # ids are case-sensitive: never normalise
    for name, target in DEFAULT_EXPORT_TARGETS.items():
        if _norm(name) == _norm(client):
            return target
    return None


def require_account(client: str | None) -> ZapmailAccount:
    """The client's own Zapmail account, strictly — or raise.

    Used for every client-scoped action (connect, mailboxes, export, tags,
    renewals, placement): running those against a fallback account would read
    the wrong fleet or, worse, create a client's domains/mailboxes in another
    client's tenant.
    """
    acc = resolve_account_for_client(client, strict=True)
    if acc is None:
        raise ZapmailAccountMissing(
            f"client={client!r} has no Zapmail account configured — map it in "
            "ZAPMAIL_CLIENT_ACCOUNTS and set the matching ZAPMAIL_API_KEY_<NAME>.")
    return acc


def open_client(
    client: str | None, *, provider: str | None = None, **kwargs,
) -> ZapmailClient:
    """A :class:`ZapmailClient` bound to the client's own account (strict).

    ``provider`` (``GOOGLE``/``MICROSOFT``) scopes every call to that side of
    the account — Zapmail answers for GOOGLE only when it is omitted.
    """
    acc = require_account(client)
    return ZapmailClient(api_key=acc.api_key, workspace_key=acc.workspace_key,
                         service_provider=provider, **kwargs)


def account_name_for_client(client: str | None) -> str | None:
    acc = resolve_account_for_client(client)
    return acc.name if acc else None


def masked_key_for_client(client: str | None) -> str:
    acc = resolve_account_for_client(client)
    return acc.masked_key if acc else "n/a"