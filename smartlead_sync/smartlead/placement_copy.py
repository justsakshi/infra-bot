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

import asyncio
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


_ANY = object()
# A campaign whose name says it is a test is never a source: copying a test
# campaign into another test campaign tests nothing real.
_TEST_WORDS = ("test", "deliverab", "placement")


def pick_source_campaign(campaigns: list[dict], client_id=_ANY,
                         exclude_ids: tuple = ()) -> dict | None:
    """Newest ACTIVE campaign. None when there is nothing to copy from.

    `client_id` narrows the choice to one Smartlead client (None means the
    account's own, un-clientised campaigns) so a test for one client is never
    fed another client's copy; omitted, any client qualifies.
    """
    active = []
    for c in campaigns:
        if str(c.get("status", "")).upper() != "ACTIVE":
            continue
        if c.get("id") in exclude_ids:
            continue
        if any(w in str(c.get("name", "")).lower() for w in _TEST_WORDS):
            continue
        if client_id is not _ANY and c.get("client_id") != client_id:
            continue
        active.append(c)
    if not active:
        return None
    active.sort(key=lambda c: str(c.get("created_at", "")), reverse=True)
    return active[0]


def eligible_sources(campaigns: list[dict], client_id=_ANY, exclude_ids: tuple = (),
                     skip_words: tuple = ()) -> list[dict]:
    """This client's real campaigns that could be copied: not a test, not excluded,
    and ACTIVE, PAUSED or COMPLETED (a draft has never sent anything).
    ``skip_words`` drops names mentioning another client (old campaigns on a
    shared account that carry no client id)."""
    out = []
    for c in campaigns:
        if str(c.get("status", "")).upper() not in ("ACTIVE", "PAUSED", "COMPLETED"):
            continue
        if c.get("id") in exclude_ids:
            continue
        name = str(c.get("name", "")).lower()
        if any(w in name for w in _TEST_WORDS) or name.startswith("dt "):
            continue  # "DT ..." are the placement-test campaigns themselves
        if any(w in name for w in skip_words):
            continue
        if client_id is not _ANY and c.get("client_id") != client_id:
            continue
        out.append(c)
    return out


def is_current(campaign: dict, leads_left: dict) -> bool:
    """Still sending next week: ACTIVE with leads not yet finished."""
    return (str(campaign.get("status", "")).upper() == "ACTIVE"
            and int(leads_left.get(campaign["id"]) or 0) > 0)


def choose_most_active(candidates: list[dict], this_week: dict, last_week: dict | None = None,
                       leads_left: dict | None = None) -> tuple[dict | None, str]:
    """The sequence that sent the most emails, recent first (team rule, 2026-10-08):

    1. most sent in the last 7 days among CURRENT campaigns (active, leads left);
    2. most sent in the last 7 days among any campaign;
    3. most sent in the 7 days before that, current campaigns first;
    4. otherwise none — the test keeps its current copy.
    This week always beats last week; a campaign still sending beats one that
    has finished or been paused.
    """
    leads_left = leads_left or {}
    newest = lambda cs: sorted(cs, key=lambda c: str(c.get("created_at", "")), reverse=True)
    for sent, when in ((this_week, "this week"), (last_week or {}, "last week")):
        sending = [c for c in candidates if int(sent.get(c["id"]) or 0) > 0]
        current = [c for c in sending if is_current(c, leads_left)]
        for pool in (current, sending):
            if pool:
                best = max(newest(pool), key=lambda c: int(sent.get(c["id"]) or 0))
                state = (f"running, {int(leads_left.get(best['id']) or 0)} leads left"
                         if is_current(best, leads_left)
                         else "active, no leads left"
                         if str(best.get("status", "")).upper() == "ACTIVE"
                         else f"{str(best.get('status', '')).lower()} now")
                return best, f"most sent {when}: {int(sent[best['id']])} emails ({state})"
    return None, "no campaign sent anything in the last 14 days"


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
                                store, dry_run: bool = False,
                                source_client_id=_ANY, skip_words: tuple = ()) -> dict:
    """Rewrite the test campaign's step from the client's campaign that sent the
    most emails this week, else last week (``choose_most_active``).

    Returns {"written": bool, "source": campaign id or None, "hash": str,
    "reason": str}. Never raises: a refresh that cannot be done safely is
    skipped and the previous copy stays in place.
    """
    from smartlead.api import SmartleadClient

    from datetime import date, timedelta

    async with SmartleadClient(acc.api_key, acc.name) as c:
        candidates = eligible_sources(await c.list_campaigns(), client_id=source_client_id,
                                      exclude_ids=(int(test_campaign_id),), skip_words=skip_words)
        today = date.today()
        unread: list = []

        async def sent(cs, since, until):
            out = {}
            for camp in cs:
                an = None
                for wait in (0, 5, 20):  # Smartlead throttles hard; retry before giving up
                    if wait:
                        await asyncio.sleep(wait)
                    try:
                        an = await c.get_analytics_by_date(str(camp["id"]), since.isoformat(),
                                                           until.isoformat())
                        break
                    except Exception:  # noqa: BLE001 - retried, then reported below
                        an = None
                if an is None:
                    unread.append(camp["id"])
                else:
                    out[camp["id"]] = int(an.get("sent_count") or 0)
            return out

        # Only campaigns that could have sent in the last 14 days: active ones, plus
        # paused / completed ones whose status changed in the last 21 days.
        cutoff = (today - timedelta(days=21)).isoformat()
        pool = [x for x in candidates if str(x.get("status", "")).upper() == "ACTIVE"
                or str(x.get("updated_at") or "9999")[:10] >= cutoff]
        this_week = await sent(pool, today - timedelta(days=7), today)
        last_week = {}
        if not any(this_week.values()):
            last_week = await sent(pool, today - timedelta(days=14), today - timedelta(days=8))
        # Lead progress, only for campaigns that sent: leads not started or mid-sequence.
        leads_left = {}
        for x in pool:
            if str(x.get("status", "")).upper() == "ACTIVE" and (this_week.get(x["id"]) or last_week.get(x["id"])):
                for wait in (0, 5, 20):
                    if wait:
                        await asyncio.sleep(wait)
                    try:
                        st = (await c.get_campaign_analytics(str(x["id"]))).get("campaign_lead_stats") or {}
                        leads_left[x["id"]] = int(st.get("notStarted") or 0) + int(st.get("inprogress") or 0)
                        break
                    except Exception:  # noqa: BLE001 - retried, then reported below
                        pass
                else:
                    unread.append(x["id"])
        if unread:
            # An unread number could change the pick: guessing would copy the wrong one.
            return {"written": False, "source": None, "hash": "",
                    "reason": f"could not read sends / lead progress for campaign(s) {unread}; kept current copy"}
        source, why = choose_most_active(pool, this_week, last_week, leads_left)
        if not source:
            return {"written": False, "source": None, "hash": "", "reason": why}
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
            return {"written": False, "source": sid, "source_name": source.get("name", ""), "hash": digest,
                    "reason": f"dry-run: would write {len(variants)} variant(s) from '{source.get('name', '')[:50]}' — {why}"}
        current = await c._get(f"/campaigns/{test_campaign_id}/sequences")
        store.save_copy_snapshot(acc.name, test_campaign_id, current)
        await c.save_campaign_sequence_variants(str(test_campaign_id), step_id, variants)
    return {"written": True, "source": sid, "source_name": source.get("name", ""), "hash": digest,
            "reason": f"{len(variants)} variant(s) from '{source.get('name', '')[:50]}' — {why}"}
