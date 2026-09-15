"""Keep the test campaign's copy current with what the client really sends.

Placement tests route through a standing test campaign, so the email under
test is whatever that campaign last held. On 2026-09-14 it held executive-
search copy left over from another client: bettrdatasgroup.com scored
1 inbox / 7 spam at Google on that email and 8 / 0 on real BettrData copy,
same inbox, same day. The domain was fine; the sheet was about to say it
was not.

This module reads the newest active campaign's first step (read-only) and
rewrites the test campaign's first step to match. Live campaigns are never
written to.

Merge fields are the trap. A live campaign fills {{providers_line}} from each
lead; the test campaign has no such lead, so the seed panel would receive the
literal braces, which is itself a spam signal. Custom fields are therefore
resolved from a sample lead before the write, and a refresh that cannot
resolve one is skipped rather than sent degraded.
"""
from __future__ import annotations

import hashlib
import json
import re

_MERGE = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")
# Smartlead fills these itself for every recipient, seed inboxes included.
_STANDARD_FIELDS = {"first_name", "last_name", "email", "company_name",
                    "phone_number", "website", "linkedin_profile", "location"}


def resolve_merge_fields(text: str, sample_fields: dict,
                         signature: str) -> tuple[str, list[str]]:
    """Substitute custom {{fields}} and %signature% with concrete values.

    Standard fields are left for Smartlead. Returns the text and the names of
    any custom fields that had no sample value.
    """
    unresolved: list[str] = []

    def _sub(match: re.Match) -> str:
        name = match.group(1)
        if name in _STANDARD_FIELDS:
            return match.group(0)
        value = sample_fields.get(name)
        if value in (None, ""):
            unresolved.append(name)
            return match.group(0)
        return str(value)

    out = _MERGE.sub(_sub, text or "")
    out = out.replace("%signature%", signature or "")
    return out, unresolved


def pick_source_campaign(campaigns: list[dict]) -> dict | None:
    """Newest ACTIVE campaign. None when there is nothing to copy from."""
    active = [c for c in campaigns if str(c.get("status", "")).upper() == "ACTIVE"]
    if not active:
        return None
    active.sort(key=lambda c: str(c.get("created_at", "")), reverse=True)
    return active[0]


def step_variants(step: dict) -> list[dict]:
    """A step's variants under either key. The read endpoint returns
    `sequence_variants` and the write payload uses `seq_variants` — Smartlead
    support called this a known inconsistency (2026-09-15) and advised handling
    both, since either could appear depending on the path the data came from."""
    return step.get("sequence_variants") or step.get("seq_variants") or []


def build_variants(step: dict, sample_fields: dict,
                   signature: str) -> tuple[list[dict], list[str]]:
    """Resolved variants for one step, plus every reason not to write them."""
    variants: list[dict] = []
    problems: list[str] = []
    for v in step_variants(step):
        label = v.get("variant_label", "A")
        subject, u1 = resolve_merge_fields(v.get("subject", ""), sample_fields, signature)
        body, u2 = resolve_merge_fields(v.get("email_body", ""), sample_fields, signature)
        if not subject.strip() or not body.strip():
            problems.append(f"variant {label} is empty")
            continue
        problems.extend(f"variant {label}: unresolved {{{{{n}}}}}" for n in u1 + u2)
        variants.append({"subject": subject, "email_body": body, "variant_label": label})
    if not variants:
        problems.append("no usable variants")
    return variants, problems


def copy_hash(variants: list[dict]) -> str:
    raw = json.dumps([(v["subject"], v["email_body"]) for v in variants], sort_keys=True)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def sample_custom_fields(leads: list[dict]) -> dict:
    """Custom-field values from the first lead that has any."""
    for entry in leads:
        lead = entry.get("lead") or entry
        fields = lead.get("custom_fields") or {}
        if any(v not in (None, "") for v in fields.values()):
            return dict(fields)
    return {}


def first_signature(accounts: list[dict]) -> str:
    for a in accounts:
        sig = (a.get("signature") or "").strip()
        if sig:
            return sig
    return ""


async def refresh_test_campaign(acc, test_campaign_id: int, step_id: int,
                                store, dry_run: bool = False) -> dict:
    """Rewrite the test campaign's step from the newest active campaign.

    Returns {"written": bool, "source": campaign id or None, "hash": str,
    "reason": str}. Never raises: a refresh that cannot be done safely is
    skipped and the previous copy stays in place.
    """
    from smartlead.api import SmartleadClient

    async with SmartleadClient(acc.api_key, acc.name) as c:
        source = pick_source_campaign(await c.list_campaigns())
        if not source:
            return {"written": False, "source": None, "hash": "", "reason": "no active campaign"}
        sid = source["id"]
        seq = await c._get(f"/campaigns/{sid}/sequences")
        steps = seq if isinstance(seq, list) else seq.get("sequences", [])
        if not steps:
            return {"written": False, "source": sid, "hash": "", "reason": "source has no steps"}
        leads = await c.get_campaign_leads(str(sid))
        senders = await c.get_campaign_email_accounts(str(sid))
        variants, problems = build_variants(
            steps[0], sample_custom_fields(leads), first_signature(senders))
        if problems:
            return {"written": False, "source": sid, "hash": "",
                    "reason": "; ".join(problems)}
        digest = copy_hash(variants)
        if dry_run:
            return {"written": False, "source": sid, "hash": digest,
                    "reason": f"dry-run: would write {len(variants)} variant(s)"}
        current = await c._get(f"/campaigns/{test_campaign_id}/sequences")
        store.save_copy_snapshot(acc.name, test_campaign_id, current)
        await c.save_campaign_sequence_variants(str(test_campaign_id), step_id, variants)
    return {"written": True, "source": sid, "hash": digest,
            "reason": f"{len(variants)} variant(s) from '{source.get('name', '')[:40]}'"}
