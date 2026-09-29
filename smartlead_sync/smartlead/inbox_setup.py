"""Set up and maintain sending inboxes in Smartlead: name, signature, warmup, client.

Step 1 of docs/INBOX_PIPELINE_PLAN.md. Works on inboxes that are already in
Smartlead, so the team can rename an inbox or change a signature today; the
buying pipeline reuses the same functions for brand-new inboxes.

What it can change:
  * **Sender name** - Smartlead ``from_name`` and, for a rename, Zapmail's
    first/last name too. The ADDRESS never changes: a new username on a
    warmed inbox restarts its reputation from zero.
  * **Signature** - rendered from the client's template in inbox_profiles.json.
  * **Smartlead client** - Melior's inboxes live in the Precise Leads account,
    filed under client 12256.
  * **Warmup, for a NEW inbox only** - the team standard (docs/STANDARD_SETUP_
    TABLE.md): always on, 40/day ramping +5 from 5, reply rate 25%, no
    auto-adjust. After that the daily warmup planner owns warmup, so existing
    inboxes are never touched here.

Every call is a plan first (``plan_changes``: pure, shows old -> new) and only
writes with ``approve=True``. Nothing here spends money.
"""

from __future__ import annotations

import html
import json
import os
import re
import string
from dataclasses import dataclass, field
from pathlib import Path

from smartlead import config

PROFILES_PATH = Path(os.getenv(
    "INBOX_PROFILES_FILE",
    Path(__file__).resolve().parent.parent / "inbox_profiles.json"))

PLACEHOLDERS = frozenset({"first_name", "last_name", "full_name", "title",
                          "company", "website", "email"})

# The team standard for a brand-new inbox (STANDARD_SETUP_TABLE.md, state NEW).
NEW_INBOX_WARMUP = {
    "enabled": True,
    "total_per_day": config.WARMUP_NEW_PER_DAY,
    "daily_rampup": 5,
    "reply_rate": config.WARMUP_REPLY_RATE,
    "auto_adjust": False,
}

_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z' .-]{0,39}$")


# ── profiles ────────────────────────────────────────────────────────────────

def load_profiles() -> dict[str, dict]:
    with open(PROFILES_PATH, encoding="utf-8") as fh:
        return json.load(fh).get("clients") or {}


def profile_for(client: str) -> tuple[str, dict]:
    want = (client or "").strip().lower().replace("_", " ")
    for key, prof in load_profiles().items():
        if key.lower() == want:
            return key, prof
    raise ValueError(f"no inbox profile for {client!r} in {PROFILES_PATH.name}")


def smartlead_key_for(prof: dict) -> tuple[str, str]:
    """``(account_name, api_key)`` of the Smartlead account holding this client."""
    from smartlead.accounts import discover_accounts
    want = str(prof.get("smartlead_account") or "").strip().lower()
    for acc in discover_accounts():
        if acc.name.lower() == want:
            return acc.name, acc.api_key
    raise ValueError(f"no Smartlead API key configured for account {prof.get('smartlead_account')!r}")


# ── pure helpers ────────────────────────────────────────────────────────────

def valid_name(name: str) -> bool:
    return bool(_NAME_RE.match((name or "").strip()))


def split_name(from_name: str) -> tuple[str, str]:
    parts = (from_name or "").strip().split()
    if not parts:
        return "", ""
    return parts[0], " ".join(parts[1:])


def render_signature(template: str, ctx: dict) -> str:
    """Fill a signature template. Values are HTML-escaped; an unknown
    placeholder is an error rather than a silent '{typo}' in every email."""
    names = {f for _, f, _, _ in string.Formatter().parse(template) if f}
    unknown = names - PLACEHOLDERS
    if unknown:
        raise ValueError(f"signature template has unknown placeholder(s): {sorted(unknown)}")
    safe = {k: html.escape(str(ctx.get(k) or "")) for k in PLACEHOLDERS}
    return template.format(**safe)


@dataclass
class InboxChange:
    email: str
    account_id: int | None = None
    fields: dict = field(default_factory=dict)       # Smartlead email-account update
    before: dict = field(default_factory=dict)        # same keys, current values
    warmup: dict | None = None                        # only for new inboxes
    zapmail_rename: dict | None = None                # {firstName, lastName} or None
    error: str | None = None

    @property
    def empty(self) -> bool:
        return not (self.fields or self.warmup or self.zapmail_rename) and not self.error


def plan_changes(
    account: dict | None,
    prof: dict,
    *,
    email: str,
    first: str | None = None,
    last: str | None = None,
    set_signature: bool = False,
    new_inbox: bool = False,
) -> InboxChange:
    """What to change on one Smartlead inbox. Pure: no network, no writes."""
    ch = InboxChange(email=email.lower())
    if not account:
        ch.error = "not in this client's Smartlead account"
        return ch
    ch.account_id = account.get("id")

    renaming = first is not None or last is not None
    cur_first, cur_last = split_name(account.get("from_name") or "")
    first = (first if first is not None else cur_first).strip()
    last = (last if last is not None else cur_last).strip()
    if renaming:
        if not valid_name(first) or (last and not valid_name(last)):
            ch.error = "names must be letters (and ' . -), up to 40 characters"
            return ch
        ch.zapmail_rename = {"firstName": first, "lastName": last}

    want: dict = {}
    full = f"{first} {last}".strip()
    if renaming and full and full != (account.get("from_name") or ""):
        want["from_name"] = full
    if set_signature:
        tmpl = prof.get("signature") or ""
        if not tmpl:
            ch.error = "no signature template for this client yet (inbox_profiles.json)"
            return ch
        sig = render_signature(tmpl, {
            "first_name": first, "last_name": last, "full_name": full,
            "title": prof.get("title"), "company": prof.get("company"),
            "website": prof.get("website"), "email": email.lower()})
        if sig != (account.get("signature") or ""):
            want["signature"] = sig
    cid = prof.get("smartlead_client_id")
    if cid is not None and account.get("client_id") != cid:
        want["client_id"] = cid

    ch.fields = want
    ch.before = {k: account.get(k) for k in want}
    if new_inbox:
        ch.warmup = dict(NEW_INBOX_WARMUP)
    if ch.zapmail_rename and "from_name" not in want and \
            full == (account.get("from_name") or ""):
        ch.zapmail_rename = None   # already has this name everywhere we can see
    return ch


# ── network ─────────────────────────────────────────────────────────────────

async def plan_for(
    client: str,
    *,
    emails: list[str] | None = None,
    domain: str | None = None,
    first: str | None = None,
    last: str | None = None,
    set_signature: bool = False,
    new_inbox: bool = False,
) -> tuple[str, list[InboxChange]]:
    """Plan changes for given inboxes, or every inbox on ``domain``. READ-ONLY."""
    from smartlead.api import SmartleadClient

    _, prof = profile_for(client)
    acc_name, key = smartlead_key_for(prof)
    async with SmartleadClient(key, account_name=acc_name) as sl:
        rows = await sl.list_email_accounts()
    by_email = {str(r.get("from_email") or "").lower(): r for r in rows}
    wanted = [e.strip().lower() for e in (emails or []) if e.strip()]
    if domain:
        d = domain.strip().lower()
        wanted += sorted(e for e in by_email if e.endswith("@" + d))
    if (first is not None or last is not None) and len(wanted) != 1:
        raise ValueError("rename one inbox at a time (give exactly one address)")
    return acc_name, [plan_changes(by_email.get(e), prof, email=e, first=first, last=last,
                                   set_signature=set_signature, new_inbox=new_inbox)
                      for e in dict.fromkeys(wanted)]


async def apply_changes(client: str, changes: list[InboxChange], *, approve: bool = False) -> list[dict]:
    """Write planned changes: Smartlead fields + warmup, then Zapmail names. WRITE."""
    if not approve:
        raise PermissionError("apply_changes writes to Smartlead/Zapmail: pass approve=True "
                              "only after a person has confirmed the plan.")
    from smartlead.api import SmartleadClient

    _, prof = profile_for(client)
    acc_name, key = smartlead_key_for(prof)
    results: list[dict] = []
    renames: list[tuple[InboxChange, dict]] = []
    async with SmartleadClient(key, account_name=acc_name) as sl:
        for ch in changes:
            res = {"email": ch.email, "ok": True, "done": []}
            if ch.error or ch.empty:
                res.update(ok=not ch.error, error=ch.error, done=[])
                results.append(res)
                continue
            try:
                if ch.fields:
                    await sl.update_email_account(str(ch.account_id), ch.fields)
                    res["done"] += sorted(ch.fields)
                if ch.warmup:
                    w = ch.warmup
                    await sl.set_warmup(str(ch.account_id), w["enabled"], w["total_per_day"],
                                        w["daily_rampup"], w["reply_rate"], w["auto_adjust"])
                    res["done"].append("warmup")
            except Exception as exc:  # noqa: BLE001 - report per inbox, keep going
                res.update(ok=False, error=str(exc)[:200])
            if ch.zapmail_rename and res["ok"]:
                renames.append((ch, res))
            results.append(res)

    if renames:
        await _rename_in_zapmail(client, renames)
    return results


async def _rename_in_zapmail(client: str, renames: list[tuple[InboxChange, dict]]) -> None:
    """Mirror a rename in Zapmail (same address, new first/last name)."""
    from smartlead.zapmail_accounts import open_client

    for provider in ("GOOGLE", "MICROSOFT"):
        pending = [(c, r) for c, r in renames if "zapmail" not in r["done"] and not r.get("zapmail_error")]
        if not pending:
            return
        async with open_client(client, provider=provider) as z:
            for ch, res in pending:
                listing = await z.list_mailboxes(contains=ch.email, page=1, limit=10)
                rows = [m for d in ((listing or {}).get("data") or {}).get("domains") or []
                        for m in d.get("mailboxes") or []]
                hit = next((m for m in rows if str(m.get("email") or "").lower() == ch.email), None)
                if not hit:
                    continue
                try:
                    await z.update_mailbox_names([{
                        "mailboxId": hit["id"], "username": hit["username"],
                        **ch.zapmail_rename}], approve=True)
                    res["done"].append("zapmail")
                except Exception as exc:  # noqa: BLE001
                    res["zapmail_error"] = str(exc)[:200]
    for ch, res in renames:
        if "zapmail" not in res["done"] and not res.get("zapmail_error"):
            res["zapmail_error"] = "not found on this client's Zapmail account"
