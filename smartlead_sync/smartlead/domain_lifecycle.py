"""Domain lifecycle orchestration: connect + create mailboxes, with polling.

Ties together the individual Zapmail calls into the two operations the team
does by hand every time a domain is bought:

  1. **Connect** — point the domain's NS at Zapmail, call ``connect-domain``,
     then poll ``connection-requests`` until it reaches a terminal state.
  2. **Mailboxes** — resolve the domain's id, assign N mailboxes, poll until
     they are ACTIVE.

Both are WRITE operations (no new money, but they mutate the account and consume
purchased mailbox slots), so both refuse unless ``approve=True``. The CLI wraps
them with a **dry-run by default**: without ``--approve`` it prints exactly what
it would do and calls nothing.

The required nameservers are published by Zapmail and hardcoded in their
connect-domain docs; :data:`ZAPMAIL_NAMESERVERS` is the single copy.
"""

from __future__ import annotations

import asyncio
import hashlib
import time

from smartlead.zapmail import PROVIDERS, ZapmailClient, ZapmailError
from smartlead.zapmail_accounts import open_client, require_account

# Zapmail's published nameservers. A domain must have these set at its registrar
# BEFORE connect-domain can succeed.
ZAPMAIL_NAMESERVERS: tuple[str, ...] = (
    "pns61.cloudns.net",
    "pns62.cloudns.com",
    "pns63.cloudns.net",
    "pns64.cloudns.uk",
)

# connect-domain / connection-requests statuses (support, 2026-09-29).
_CONNECT_OK = {"SUCCESS", "DOMAIN_ALREADY_CONNECTED"}
_CONNECT_FAIL = {
    "DOMAIN_NOT_REGISTERED", "WORKSPACE_ALREADY_EXISTS", "BLACKLISTED_DOMAIN",
    "BANNED_DOMAIN", "FAILED", "PERMISSION_REQUIRED",
}
# Final only after 24-48h — NS changes usually land in 5-30 min, so within our
# polling window it means "nameservers not pointed yet", not a failure.
_CONNECT_WAITING_ON_NS = "NS_NOT_CHANGED"

MAILBOX_STATUS_ACTIVE = {"ACTIVE"}
MAILBOX_STATUS_FAILED = {"FAILED"}

# Zapmail's recommended polling (support, 2026-09-29).
CONNECT_POLL_S = 60.0
MAILBOX_POLL_S = 180.0


def required_nameservers() -> tuple[str, ...]:
    return ZAPMAIL_NAMESERVERS


# ── connect ──────────────────────────────────────────────────────────────────

async def _connect_status_map(z: ZapmailClient) -> dict[str, str]:
    """``{domain: status}`` from the pending connection-requests list."""
    resp = await z.list_connection_requests(page=1, limit=200)
    data = (resp or {}).get("data") or {}
    rows = data.get("data") or []
    return {str(r.get("domainName", "")).strip().lower(): str(r.get("status", ""))
            for r in rows if r.get("domainName")}


async def _is_active_domain(z: ZapmailClient, domain: str) -> bool:
    """True when the domain now appears as an ACTIVE domain (connect finished)."""
    resp = await z.list_domains(contains=domain, page=1, limit=50)
    data = (resp or {}).get("data") or {}
    return any(str(d.get("domain", "")).strip().lower() == domain.lower()
               and str(d.get("status", "")).upper() == "ACTIVE"
               for d in data.get("domains") or [])


async def domain_provider(domain: str, *, client: str | None) -> str | None:
    """``GOOGLE``/``MICROSOFT`` — which side of the client's account holds the
    domain, or None. Zapmail only shows a Microsoft domain to a request that
    asks for MICROSOFT, so every domain-level action locates it first."""
    domain = domain.strip().lower()
    for provider in PROVIDERS:
        async with open_client(client, provider=provider) as z:
            if await _domain_row(z, domain) is not None:
                return provider
    return None


async def _domain_row(z: ZapmailClient, domain: str) -> dict | None:
    resp = await z.list_domains(contains=domain, page=1, limit=50)
    for d in ((resp or {}).get("data") or {}).get("domains") or []:
        if str(d.get("domain", "")).strip().lower() == domain.lower():
            return d
    return None


async def connect_and_wait(
    domain_names: list[str],
    *,
    approve: bool = False,
    client: str | None = None,
    provider: str = "GOOGLE",
    timeout_s: float = 1800.0,
    interval_s: float = CONNECT_POLL_S,
) -> dict[str, dict]:
    """Connect domains and poll to terminal state.

    ``provider`` picks the side of the account the domain joins (GOOGLE or
    MICROSOFT mailboxes). Without ``approve`` this is a pure dry-run: it
    returns the intended action and calls nothing. With ``approve`` it calls
    connect-domain, then polls connection-requests (falling back to the
    active-domain list) until every name is OK, failed, or the timeout elapses.
    """
    domains = [d.strip().lower() for d in domain_names if d.strip()]
    provider = provider.upper()
    account = require_account(client)  # fail the dry-run too if unmapped
    if not approve:
        return {d: {"status": "DRY_RUN", "ok": None, "account": account.name,
                    "provider": provider,
                    "nameservers": list(ZAPMAIL_NAMESERVERS)} for d in domains}

    results: dict[str, dict] = {}
    async with open_client(client, provider=provider) as z:
        try:
            await z.connect_domains(domains, approve=True)
        except ZapmailError as exc:
            return {d: {"status": "REQUEST_FAILED", "ok": False, "detail": str(exc)[:200]}
                    for d in domains}

        remaining = set(domains)
        last_seen: dict[str, str] = {}
        deadline = time.monotonic() + timeout_s
        while remaining and time.monotonic() < deadline:
            try:
                pending = await _connect_status_map(z)
            except ZapmailError:
                pending = {}
            for d in list(remaining):
                st = pending.get(d)
                if st:
                    last_seen[d] = st
                if st in _CONNECT_OK:
                    results[d] = {"status": st, "ok": True}
                    remaining.discard(d)
                elif st in _CONNECT_FAIL:
                    results[d] = {"status": st, "ok": False}
                    remaining.discard(d)
                elif st is None:
                    # Left the pending list — confirm it is live before calling it done.
                    try:
                        if await _is_active_domain(z, d):
                            results[d] = {"status": "SUCCESS", "ok": True}
                            remaining.discard(d)
                    except ZapmailError:
                        pass
            if remaining and time.monotonic() < deadline:
                await asyncio.sleep(min(interval_s, max(0.0, deadline - time.monotonic())))  # never overshoot the deadline
        for d in remaining:
            if last_seen.get(d) == _CONNECT_WAITING_ON_NS:
                results[d] = {"status": _CONNECT_WAITING_ON_NS, "ok": False,
                              "detail": "nameservers not pointed at Zapmail yet — "
                                        "can take up to 24-48h; re-run --connect-status later"}
            else:
                results[d] = {"status": "TIMEOUT", "ok": False,
                              "last_status": last_seen.get(d)}
    return results


async def connect_status(domain: str, *, client: str | None = None) -> dict:
    """Read-only: current connect state of one domain, on either provider."""
    domain = domain.strip().lower()
    for provider in PROVIDERS:
        async with open_client(client, provider=provider) as z:
            if await _is_active_domain(z, domain):
                return {"domain": domain, "status": "ACTIVE", "connected": True,
                        "provider": provider}
            try:
                pending = await _connect_status_map(z)
            except ZapmailError:
                pending = {}  # needs x-workspace-key on some accounts
            if domain in pending:
                return {"domain": domain, "status": pending[domain],
                        "connected": pending[domain] in _CONNECT_OK,
                        "provider": provider}
    return {"domain": domain, "status": "NOT_FOUND", "connected": False,
            "provider": None}


# ── mailboxes ────────────────────────────────────────────────────────────────

_FIRST_NAMES = (
    "James", "Olivia", "Ethan", "Ava", "Noah", "Mia", "Liam", "Emma", "Lucas",
    "Sofia", "Mason", "Isla", "Leo", "Nora", "Owen", "Ruby", "Caleb", "Elena",
    "Alan", "Grace", "Victor", "Clara", "Nadia", "Simon", "Iris", "Marcus",
)
_LAST_NAMES = (
    "Mercer", "Hayes", "Dalton", "Reed", "Foster", "Quinn", "Baxter", "Nolan",
    "Vance", "Ellis", "Porter", "Rowan", "Sutton", "Blake", "Hale", "Griffin",
    "Larsen", "Monroe", "Pace", "Reyes", "Sherman", "Tate", "Underwood",
)


def generate_identities(
    domain: str, count: int, *, exclude: set[str] | frozenset[str] = frozenset(),
) -> list[dict]:
    """Deterministic mailbox identities for a domain.

    Same domain + count + ``exclude`` always yields the same names, so a re-run
    after a partial failure does not invent new ones. ``exclude`` holds the
    usernames already on the domain: a top-up skips them instead of colliding.
    Usernames are letters-only (``firstlast`` lowercased) to satisfy Zapmail's
    no-leading/trailing-dot rule without thinking about it.
    """
    seed = int(hashlib.sha1(domain.lower().encode()).hexdigest()[:8], 16)
    taken = {u.lower() for u in exclude}
    out: list[dict] = []
    i = 0
    # 26*23 combinations; the bound only guards against a pathological exclude.
    while len(out) < count and i < len(_FIRST_NAMES) * len(_LAST_NAMES):
        f = _FIRST_NAMES[(seed + i * 7) % len(_FIRST_NAMES)]
        l = _LAST_NAMES[(seed + i * 13 + 3) % len(_LAST_NAMES)]
        i += 1
        username = f"{f}{l}".lower()
        if username in taken:
            continue
        taken.add(username)
        out.append({
            "firstName": f,
            "lastName": l,
            "mailboxUsername": username,
            "domainName": domain,
        })
    return out


def identities_from_names(domain: str, names: list[str]) -> list[dict]:
    """Identities from real sender names (``"First Last"``), e.g. the client's
    actual people. Use this instead of generated personas whenever the client
    has named senders."""
    out: list[dict] = []
    for full in names:
        parts = [p for p in full.strip().split() if p]
        if not parts:
            continue
        first, last = parts[0], " ".join(parts[1:])
        username = "".join(ch for ch in f"{first}{last}".lower() if ch.isalnum())
        if not username:
            continue
        out.append({"firstName": first, "lastName": last,
                    "mailboxUsername": username, "domainName": domain})
    return out


def _username_variants(first: str, last: str) -> list[str]:
    """Address shapes for one person, in the order our fleet already uses them
    (aaron@, aaron.dix@, aarond@ ...). Letters and single dots only, never at
    either end - Zapmail's username rule."""
    f = "".join(ch for ch in first.lower() if ch.isalpha())
    l = "".join(ch for ch in last.lower() if ch.isalpha())
    if not f:
        return []
    shapes = [f]
    if l:
        shapes += [f"{f}.{l}", f"{f}{l[0]}", f"{f}{l}", f"{f[0]}{l}", f"{f[0]}.{l}", f"{l}.{f}"]
    return list(dict.fromkeys(shapes))


def identities_for_senders(
    domain: str, names: list[str], count: int, *, exclude: set[str] | frozenset[str] = frozenset(),
) -> list[dict]:
    """``count`` identities for real senders, spread across them in turn.

    One person can hold several inboxes on a domain (Ryan on three), each with
    its own address; usernames already on the domain are skipped. Returns
    fewer than ``count`` only when every variant is taken.
    """
    people = []
    for full in names:
        parts = [p for p in str(full).strip().split() if p]
        if parts:
            people.append((parts[0], " ".join(parts[1:]), _username_variants(parts[0], " ".join(parts[1:]))))
    taken = {u.lower() for u in exclude}
    out: list[dict] = []
    depth = 0
    while len(out) < count and people and depth < 7:
        for first, last, variants in people:
            if len(out) >= count:
                break
            if depth < len(variants) and variants[depth] not in taken:
                taken.add(variants[depth])
                out.append({"firstName": first, "lastName": last,
                            "mailboxUsername": variants[depth], "domainName": domain})
        depth += 1
    return out


async def _existing_usernames(z: ZapmailClient, domain: str) -> set[str]:
    """Local parts of mailboxes already on ``domain`` (exact domain match)."""
    resp = await z.list_mailboxes(contains=domain, page=1, limit=50)
    data = (resp or {}).get("data") or {}
    out: set[str] = set()
    for d in data.get("domains") or []:
        for mb in d.get("mailboxes") or []:
            local, _, dom = str(mb.get("email", "")).strip().lower().partition("@")
            if dom == domain.lower() and local:
                out.add(local)
    return out


async def _resolve_domain_id(z: ZapmailClient, domain: str) -> tuple[str | None, int]:
    """``(domain_id, assigned_count)`` if assignable, else ``(None, 0)``."""
    resp = await z.list_assignable_domains(contains=domain, page=1, limit=50)
    data = (resp or {}).get("data") or {}
    for d in data.get("domains") or []:
        if str(d.get("domain", "")).strip().lower() == domain.lower():
            return str(d.get("id")), int(d.get("assignedMailboxesCount") or 0)
    return None, 0


async def assign_mailboxes_and_wait(
    domain: str,
    *,
    count: int = 2,
    approve: bool = False,
    client: str | None = None,
    sender_names: list[str] | None = None,
    timeout_s: float = 3600.0,
    interval_s: float = MAILBOX_POLL_S,
) -> dict:
    """Assign ``count`` mailboxes to a connected domain and poll to ACTIVE.

    Dry-run (no ``approve``) reads the domain and returns the intended
    identities without mutating anything. Enforces Zapmail's
    5-mailboxes-per-domain cap by trimming ``count``. ``sender_names``
    (``["First Last", ...]``) uses real sender identities instead of generated
    personas; usernames already on the domain are always skipped.
    """
    domain = domain.strip().lower()
    provider = await domain_provider(domain, client=client)
    if provider is None:
        return {"domain": domain, "ok": False,
                "error": "domain not on this client's Zapmail account (connect it first)"}
    async with open_client(client, provider=provider) as z:
        domain_id, existing = await _resolve_domain_id(z, domain)
        if not domain_id:
            return {"domain": domain, "ok": False, "provider": provider,
                    "error": "domain not assignable yet (still connecting?)"}

        remaining_slots = max(0, 5 - existing)
        want = min(max(0, count), remaining_slots)
        if want <= 0:
            return {"domain": domain, "ok": True, "created": [],
                    "detail": f"already at cap ({existing}/5)"}

        existing_users = await _existing_usernames(z, domain)
        if sender_names:
            identities = identities_for_senders(domain, sender_names, want, exclude=existing_users)
            if not identities:
                return {"domain": domain, "ok": False,
                        "error": "every given sender name already exists on this domain"}
        else:
            identities = generate_identities(domain, want, exclude=existing_users)
        emails = [f"{i['mailboxUsername']}@{domain}" for i in identities]
        if not approve:
            return {"domain": domain, "ok": None, "dry_run": True,
                    "provider": provider, "domain_id": domain_id,
                    "would_create": emails, "existing": existing}

        payload = {domain_id: identities}
        try:
            await z.assign_mailboxes(payload, approve=True)
        except ZapmailError as exc:
            return {"domain": domain, "ok": False, "error": str(exc)[:200]}

        # Poll the mailbox list for these emails until ACTIVE.
        wanted = {e.lower() for e in emails}
        status: dict[str, str] = {e.lower(): "PENDING" for e in emails}
        deadline = time.monotonic() + timeout_s
        while wanted and time.monotonic() < deadline:
            try:
                resp = await z.list_mailboxes(contains=domain, page=1, limit=50)
            except ZapmailError:
                resp = None
            data = (resp or {}).get("data") or {}
            for d in data.get("domains") or []:
                for mb in d.get("mailboxes") or []:
                    em = str(mb.get("email", "")).strip().lower()
                    if em in wanted:
                        st = str(mb.get("status", "")).upper()
                        status[em] = st
                        if st in MAILBOX_STATUS_ACTIVE or st in MAILBOX_STATUS_FAILED:
                            wanted.discard(em)  # final either way; no point waiting
            if wanted and time.monotonic() < deadline:
                await asyncio.sleep(min(interval_s, max(0.0, deadline - time.monotonic())))  # never overshoot the deadline

        failed = sorted(e for e, st in status.items() if st in MAILBOX_STATUS_FAILED)
        out = {
            "domain": domain, "ok": not wanted and not failed, "provider": provider,
            "created": emails,
            "statuses": status,
            "pending": sorted(wanted),
            "failed": failed,
        }
        if failed:
            out["error"] = f"{len(failed)} mailbox(es) FAILED on Zapmail: {', '.join(failed)}"
        elif wanted:
            out["detail"] = ("still IN_PROGRESS — Google is usually ACTIVE within an hour, "
                             "Microsoft a few hours; check later with /zapmail domain "
                             f"{domain} (escalate to Zapmail after 24h)")
        return out