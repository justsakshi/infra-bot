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

Merge fields are the trap. A live campaign fills {{first_name}} and
{{providers_line}} from each lead; the seed inboxes are not leads, so they would
receive "Hi ," or literal braces, which is itself a spam signal. Every field is
therefore filled from ONE real lead of the source campaign that has all of them
filled, so the test sends one complete, real-looking email. If no lead has them
all, the refresh is skipped rather than sent degraded.

Three kinds of test campaign use this module (copy_sync.py decides which is which):
the weekly INBOX tests ("DT <client> #n") get the fixed neutral note below, never
campaign copy; the standing test campaigns and each client's COPY check
("DT <client> copy check") get the live copy. A copy check can also be loaded from
one named campaign, a draft included, to test an email before it starts
(``refresh_test_campaign(source_campaign_id=...)``, refused for another client's
campaign), and its senders are the client's inboxes that pass the neutral note now
(``pick_copy_check_senders``).
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re

_MERGE = re.compile(r"\{\{\s*([A-Za-z0-9_]+)\s*\}\}")

# The weekly INBOX test ("DT <client> #n" campaigns) sends this fixed plain note, never campaign copy, with open and
# click tracking off. A test should have one thing that can fail it: on 11 Nov 2026 the same Google inbox reached a
# real Gmail inbox with a plain note and went to spam with the campaign email, tracking on or off, so a test that
# sends campaign copy grades the copy and the inbox together. Copy is checked separately.
NEUTRAL_SUBJECT = "Quick note before this week's update"
NEUTRAL_BODY = ("Hi,<br><br>Sharing a short note ahead of this week's update. Let me know if the timing works on "
                "your side and I'll send the details over.<br><br>Thanks")
NEUTRAL_TRACKING = ["DONT_EMAIL_OPEN", "DONT_LINK_CLICK"]          # as Smartlead stores "tracking off"
_TRACKING_OFF_WRITE = ["DONT_TRACK_EMAIL_OPEN", "DONT_TRACK_LINK_CLICK"]   # as its settings call takes it


def _tracking_off(track_settings) -> bool:
    got = {str(t).replace("DONT_TRACK_", "DONT_") for t in (track_settings or [])}
    return set(NEUTRAL_TRACKING) <= got


def is_neutral(step: dict, track_settings) -> bool:
    """True when the step is exactly the neutral note (one variant) and the campaign's tracking is off."""
    variants = step_variants(step)
    return (len(variants) == 1 and str(variants[0].get("subject") or "").strip() == NEUTRAL_SUBJECT
            and str(variants[0].get("email_body") or "").strip() == NEUTRAL_BODY and _tracking_off(track_settings))


async def write_neutral_note(c, account: str, test_campaign_id: int, step_id: int, store, dry_run: bool = False) -> dict:
    """Make an inbox-test campaign's step the neutral note, tracking off. ``c`` is an open Smartlead client.
    Returns {"written", "source": None, "hash", "reason"} like ``refresh_test_campaign``; the old copy is snapshotted."""
    current = await c._get(f"/campaigns/{test_campaign_id}/sequences")
    steps = current if isinstance(current, list) else current.get("sequences", [])
    step = next((s for s in steps if int(s.get("id", 0)) == int(step_id)), None)
    if step is None:
        return {"written": False, "source": None, "hash": "", "reason": "its step was not found; kept current copy"}
    camp = await c._get(f"/campaigns/{test_campaign_id}")
    variants = [{"subject": NEUTRAL_SUBJECT, "email_body": NEUTRAL_BODY, "variant_label": "A"}]
    digest = copy_hash(variants)
    if is_neutral(step, (camp or {}).get("track_settings")):
        return {"written": False, "source": None, "hash": digest, "reason": "already the neutral note, tracking off"}
    if dry_run:
        return {"written": False, "source": None, "hash": digest,
                "reason": "dry-run: would write the neutral note and turn tracking off"}
    store.save_copy_snapshot(account, test_campaign_id, current)
    await c.save_campaign_sequence_variants(str(test_campaign_id), step_id, variants)
    await c.update_campaign_settings(str(test_campaign_id), {"track_settings": list(_TRACKING_OFF_WRITE)})
    return {"written": True, "source": None, "hash": digest, "reason": "neutral note written, tracking off"}


# The COPY check ("DT <client> copy check", one small campaign per client) is the other half: it carries the client's
# real first email and sends it from a few inboxes that just passed the neutral note, so a bad result there is the
# email's, not the inbox's. Its senders are picked here; copy_sync.py keeps the campaign in step every run.
COPY_CHECK_PER_TYPE = 5         # at most this many Google-hosted and this many Outlook-hosted senders
COPY_CHECK_PER_DOMAIN = 2       # at most this many senders of one sending domain
COPY_CHECK_MAX_AGE_DAYS = 10    # the pass must be this recent: last week's test, with room for a late download
_HOSTED = ("GMAIL", "OUTLOOK")  # Smartlead inbox types that are picked; an SMTP inbox never is
_JUDGED = ("PASS", "WATCH", "FAIL")


def _tag_names(tags) -> list[str]:
    return [str((t.get("tag_name") or t.get("name") or "") if isinstance(t, dict) else t) for t in (tags or [])]


def pick_copy_check_senders(inboxes: list[dict], verdicts: list[dict], today) -> list[int]:
    """The inbox ids a client's copy-check campaign should hold. Pure.

    ``inboxes``: that client's Smartlead email accounts (from_email, id, type, is_smtp_success, is_imap_success,
    tags). ``verdicts``: Mongo v2_verdicts rows (email, night, verdict), already the verdict at the inbox's own
    provider. An inbox is picked when it is Google- or Outlook-hosted, connected, carries no tag starting
    "do not use", and its NEWEST judged verdict is PASS and at most COPY_CHECK_MAX_AGE_DAYS old. At most
    COPY_CHECK_PER_TYPE per type and COPY_CHECK_PER_DOMAIN per sending domain, taken by domain then address, so
    the same inboxes are picked run after run while they keep passing."""
    from datetime import date

    newest: dict[str, tuple[str, str]] = {}
    for v in verdicts:
        email, night = str(v.get("email") or "").strip().lower(), str(v.get("night") or "")
        if email and night and v.get("verdict") in _JUDGED and night >= newest.get(email, ("", ""))[0]:
            newest[email] = (night, v["verdict"])
    ready = []
    for a in inboxes:
        email = str(a.get("from_email") or "").strip().lower()
        kind = str(a.get("type") or "").strip().upper()
        night, verdict = newest.get(email, ("", ""))
        if ("@" not in email or a.get("id") is None or kind not in _HOSTED or verdict != "PASS"
                or not (a.get("is_smtp_success") and a.get("is_imap_success"))
                or any(t.strip().lower().startswith("do not use") for t in _tag_names(a.get("tags")))):
            continue
        try:
            age = (today - date.fromisoformat(night)).days
        except ValueError:
            continue
        if age <= COPY_CHECK_MAX_AGE_DAYS:
            ready.append((email.rsplit("@", 1)[1], email, kind, int(a["id"])))
    picked: list[int] = []
    per_type: dict[str, int] = {}
    per_domain: dict[str, int] = {}
    for domain, _, kind, inbox_id in sorted(ready):
        if per_type.get(kind, 0) >= COPY_CHECK_PER_TYPE or per_domain.get(domain, 0) >= COPY_CHECK_PER_DOMAIN:
            continue
        per_type[kind] = per_type.get(kind, 0) + 1
        per_domain[domain] = per_domain.get(domain, 0) + 1
        picked.append(inbox_id)
    return picked


def resolve_merge_fields(text: str, sample_fields: dict,
                         signature: str) -> tuple[str, list[str]]:
    """Substitute every {{field}} and %signature% with concrete values. Returns
    the text and the names of any fields that had no sample value.
    """
    unresolved: list[str] = []

    def _sub(match: re.Match) -> str:
        name = match.group(1)
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


def merge_names(step: dict) -> set[str]:
    """Every {{field}} used in the step's subjects and bodies."""
    names: set[str] = set()
    for v in step_variants(step):
        for text in (v.get("subject"), v.get("email_body")):
            names.update(_MERGE.findall(text or ""))
    return names


def sample_lead_fields(leads: list[dict], names: set[str]) -> dict | None:
    """All fields (standard and custom) of the first lead that has every one of
    `names` filled, so the whole email comes from one real lead. None if no lead does."""
    for entry in leads:
        lead = entry.get("lead") or entry
        fields = {k: v for k, v in lead.items() if isinstance(v, (str, int, float))}
        fields.update(lead.get("custom_fields") or {})
        if all(str(fields.get(n) or "").strip() for n in names):
            return fields
    return None


def first_signature(accounts: list[dict]) -> str:
    for a in accounts:
        sig = (a.get("signature") or "").strip()
        if sig:
            return sig
    return ""


def named_source(campaigns: list[dict], source_id: int, client_id=_ANY, exclude_ids: tuple = (),
                 skip_words: tuple = ()) -> tuple[dict | None, str]:
    """The campaign named with --source, or (None, why it is refused). Any status counts, a draft too: checking
    an email before it starts is the point. Never a test campaign, and never another client's (``client_id`` and
    ``skip_words`` as in ``eligible_sources``): that would grade one client's email under another's name."""
    camp = next((c for c in campaigns if str(c.get("id")) == str(source_id)), None)
    if camp is None:
        return None, f"source campaign {source_id} is not in this account; kept current copy"
    name = str(camp.get("name", "")).lower()
    if int(source_id) in {int(i) for i in exclude_ids} or name.startswith("dt "):
        return None, f"source campaign {source_id} is a test campaign; kept current copy"
    if (client_id is not _ANY and camp.get("client_id") != client_id) or any(w in name for w in skip_words):
        return None, f"source campaign {source_id} belongs to another client; kept current copy"
    return camp, "named with --source"


async def _most_active_source(c, campaigns: list[dict], test_campaign_id: int, source_client_id,
                              skip_words: tuple) -> tuple[dict | None, str]:
    """The client's campaign that sent the most this week, else last week (``choose_most_active``), or
    (None, why none). ``c`` is an open Smartlead client; sends and lead progress are read from it."""
    from datetime import date, timedelta

    candidates = eligible_sources(campaigns, client_id=source_client_id,
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
        return None, f"could not read sends / lead progress for campaign(s) {unread}; kept current copy"
    return choose_most_active(pool, this_week, last_week, leads_left)


async def refresh_test_campaign(acc, test_campaign_id: int, step_id: int,
                                store, dry_run: bool = False,
                                source_client_id=_ANY, skip_words: tuple = (),
                                source_campaign_id: int | None = None) -> dict:
    """Rewrite the test campaign's step from the client's campaign that sent the
    most emails this week, else last week (``choose_most_active``). With
    ``source_campaign_id`` it is rewritten from that one campaign instead (a copy
    check of an email before it starts); a campaign that is not this client's is
    refused (``named_source``).

    Returns {"written": bool, "source": campaign id or None, "hash": str,
    "reason": str}; "hash" is empty whenever the copy was not, and would not be,
    written. Never raises: a refresh that cannot be done safely is
    skipped and the previous copy stays in place.
    """
    from smartlead.api import SmartleadClient

    async with SmartleadClient(acc.api_key, acc.name) as c:
        campaigns = await c.list_campaigns()
        if source_campaign_id is not None:
            source, why = named_source(campaigns, source_campaign_id, source_client_id,
                                       (int(test_campaign_id),), skip_words)
        else:
            source, why = await _most_active_source(c, campaigns, test_campaign_id, source_client_id, skip_words)
        if not source:
            return {"written": False, "source": None, "hash": "", "reason": why}
        sid = source["id"]
        seq = await c._get(f"/campaigns/{sid}/sequences")
        steps = seq if isinstance(seq, list) else seq.get("sequences", [])
        if not steps:
            return {"written": False, "source": sid, "hash": "", "reason": "source has no steps"}
        leads = await c.get_campaign_leads(str(sid))
        senders = await c.get_campaign_email_accounts(str(sid))
        names = merge_names(steps[0])
        sample = sample_lead_fields(leads, names)
        if sample is None:
            tags = ", ".join("{{%s}}" % n for n in sorted(names))
            return {"written": False, "source": sid, "hash": "",
                    "reason": f"no lead in '{source.get('name', '')[:50]}' has all of {tags} filled; kept current copy"}
        variants, problems = build_variants(steps[0], sample, first_signature(senders))
        if problems:
            return {"written": False, "source": sid, "hash": "",
                    "reason": "; ".join(problems)}
        digest = copy_hash(variants)
        if dry_run:
            return {"written": False, "source": sid, "source_name": source.get("name", ""), "hash": digest,
                    "reason": f"dry-run: would write {len(variants)} variant(s) from '{source.get('name', '')[:50]}', "
                              f"filled from lead {sample.get('email', '?')} — {why}"}
        current = await c._get(f"/campaigns/{test_campaign_id}/sequences")
        store.save_copy_snapshot(acc.name, test_campaign_id, current)
        await c.save_campaign_sequence_variants(str(test_campaign_id), step_id, variants)
    return {"written": True, "source": sid, "source_name": source.get("name", ""), "hash": digest,
            "reason": f"{len(variants)} variant(s) from '{source.get('name', '')[:50]}', "
                      f"filled from lead {sample.get('email', '?')} — {why}"}
