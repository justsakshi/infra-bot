"""Company-staggered lead release.

The goal is reach across COMPANIES, not depth within one. Emailing three
people at one company on day one is worse than emailing three companies:

  * a bounce at a company predicts bounces at its colleagues (same mail
    server, same catch-all, same bad record), so depth multiplies the damage;
  * several messages into one org in a day is what trips server-level rate
    limits and gets the whole domain blocked;
  * one reply - positive or negative - settles the whole company, so the
    other two sends were wasted before they left.

So each day fills the sending cap breadth-first: pass 1 gives one lead to
every company nobody has touched yet. Only if the cap is still unfilled does
pass 2 come back for a second person at companies whose first email actually
SENT, then pass 3, and so on. Volume never drops - the cap is always filled
if leads exist - but it is spent on new companies first.

A company stops receiving anything when:

  * a human replied (positive or negative - both settle it). Out-of-office
    does not count; that is handled by the reply classifier, not here.
  * a send bounced or was blocked. Its colleagues are likely to bounce too,
    and burning more sends on them costs reputation for nothing.

This module is pure: it decides WHO to send to next. Talking to Smartlead and
recording what happened belongs to the executor.
"""

from __future__ import annotations

import csv
import io
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum

# ── states ───────────────────────────────────────────────────────────────────


class CompanyState(str, Enum):
    ACTIVE = "ACTIVE"            # still eligible
    PAUSED_REPLY = "PAUSED_REPLY"  # a human replied; stop the whole company
    PAUSED_BOUNCE = "PAUSED_BOUNCE"  # a send bounced/blocked; colleagues likely bad
    EXHAUSTED = "EXHAUSTED"      # every known lead has been sent

    @property
    def eligible(self) -> bool:
        return self is CompanyState.ACTIVE


class LeadState(str, Enum):
    QUEUED = "QUEUED"    # uploaded, not yet pushed to Smartlead
    SENT = "SENT"        # confirmed sent by Smartlead
    BOUNCED = "BOUNCED"
    REPLIED = "REPLIED"
    SKIPPED = "SKIPPED"  # company paused before this lead's turn


# Free-mail and common catch-all domains are not companies. Two leads at
# gmail.com are unrelated people, so keying on the domain would wrongly treat
# the whole of Gmail as one company and release one lead a day from it.
PUBLIC_EMAIL_DOMAINS: frozenset[str] = frozenset({
    "gmail.com", "googlemail.com", "yahoo.com", "yahoo.co.uk", "hotmail.com",
    "outlook.com", "live.com", "msn.com", "aol.com", "icloud.com", "me.com",
    "mac.com", "protonmail.com", "proton.me", "gmx.com", "gmx.de", "mail.com",
    "zoho.com", "yandex.com", "fastmail.com", "hey.com", "qq.com", "163.com",
})

# Canonical lead fields Smartlead accepts directly; everything else a CSV
# carries becomes a custom_field for the sequence's {{variables}}.
STANDARD_FIELDS: tuple[str, ...] = (
    "email", "first_name", "last_name", "phone_number", "company_name",
    "website", "location", "linkedin_profile", "company_url",
)

# Header spellings seen in the team's exports, normalised to the API's names.
_HEADER_ALIASES: dict[str, str] = {
    "email": "email", "emailaddress": "email", "workemail": "email",
    "email_address": "email", "e_mail": "email",
    "firstname": "first_name", "first": "first_name", "fname": "first_name",
    "lastname": "last_name", "last": "last_name", "lname": "last_name",
    "company": "company_name", "companyname": "company_name",
    "organization": "company_name", "organisation": "company_name",
    "account": "company_name", "accountname": "company_name",
    "companywebsite": "website", "companyurl": "company_url",
    "domain": "company_url", "companydomain": "company_url",
    "linkedin": "linkedin_profile", "linkedinurl": "linkedin_profile",
    "linkedinprofile": "linkedin_profile", "profileurl": "linkedin_profile",
    "phone": "phone_number", "phonenumber": "phone_number", "mobile": "phone_number",
    "city": "location", "country": "location", "region": "location",
}

_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s.]+\.[^@\s]+$")


def normalise_header(header: str) -> str:
    """'Company Name' / 'company-name' / 'CompanyName' -> 'company_name'."""
    cleaned = re.sub(r"[^a-z0-9]+", "", (header or "").strip().lower())
    if cleaned in _HEADER_ALIASES:
        return _HEADER_ALIASES[cleaned]
    # Keep unknown headers readable as custom-field keys.
    return re.sub(r"[^a-z0-9]+", "_", (header or "").strip().lower()).strip("_")


def suggest_mapping(headers: list[str]) -> dict[str, str]:
    """CSV header -> lead field. Unrecognised headers map to themselves and
    become custom fields, which is what the sequence variables read."""
    return {h: normalise_header(h) for h in headers if (h or "").strip()}


def company_key(row: dict, *, company_column: str | None = None) -> str:
    """Stable identifier for the company a lead belongs to.

    Prefers an explicit column when the caller names one, then the email
    domain, which is what actually shares a mail server - the thing whose
    rate limits and bounce behaviour we are pacing against. Public mailbox
    providers are never treated as a company: each such lead stands alone,
    keyed by its own address, so a hundred gmail.com leads are a hundred
    companies rather than one.
    """
    if company_column:
        explicit = str(row.get(company_column) or "").strip().lower()
        if explicit:
            return explicit

    email = str(row.get("email") or "").strip().lower()
    domain = email.rsplit("@", 1)[-1] if "@" in email else ""
    if domain and domain not in PUBLIC_EMAIL_DOMAINS:
        return domain

    for key in ("company_url", "website"):
        value = str(row.get(key) or "").strip().lower()
        value = re.sub(r"^https?://", "", value).split("/")[0].removeprefix("www.")
        if value:
            return value

    name = str(row.get("company_name") or "").strip().lower()
    return name or email or "unknown"


# ── parsing ──────────────────────────────────────────────────────────────────

@dataclass
class ParsedLead:
    email: str
    company: str
    fields: dict[str, str] = field(default_factory=dict)

    def to_smartlead(self) -> dict:
        """Split into Smartlead's standard fields plus custom_fields."""
        payload: dict = {"email": self.email}
        custom: dict[str, str] = {}
        for key, value in self.fields.items():
            if key == "email" or value in (None, ""):
                continue
            if key in STANDARD_FIELDS:
                payload[key] = value
            else:
                custom[key] = value
        if custom:
            payload["custom_fields"] = custom
        return payload


@dataclass
class ParseResult:
    leads: list[ParsedLead]
    headers: list[str]
    mapping: dict[str, str]
    skipped_no_email: int = 0
    skipped_duplicate: int = 0

    @property
    def companies(self) -> int:
        return len({lead.company for lead in self.leads})


def parse_leads_csv(data: bytes | str, *, mapping: dict[str, str] | None = None,
                    company_column: str | None = None) -> ParseResult:
    """Parse an uploaded CSV into leads keyed by company.

    Rows without a valid email are dropped, and a repeated email keeps only
    its first occurrence - a duplicate would otherwise consume a send slot
    that Smartlead then rejects.
    """
    text = data.decode("utf-8-sig", errors="replace") if isinstance(data, bytes) else data
    reader = csv.DictReader(io.StringIO(text))
    headers = [h for h in (reader.fieldnames or []) if (h or "").strip()]
    mapping = mapping or suggest_mapping(headers)

    leads: list[ParsedLead] = []
    seen: set[str] = set()
    no_email = dupes = 0

    for row in reader:
        fields = {}
        for header, value in row.items():
            target = mapping.get(header)
            if not target or target == "__ignore__":
                continue
            text_value = str(value).strip() if value is not None else ""
            if text_value:
                fields[target] = text_value

        email = fields.get("email", "").lower()
        if not _EMAIL_RE.match(email):
            no_email += 1
            continue
        if email in seen:
            dupes += 1
            continue
        seen.add(email)
        fields["email"] = email
        leads.append(ParsedLead(email=email, company=company_key(fields, company_column=company_column),
                                fields=fields))

    return ParseResult(leads=leads, headers=headers, mapping=mapping,
                       skipped_no_email=no_email, skipped_duplicate=dupes)


# ── the release decision ─────────────────────────────────────────────────────

@dataclass
class CompanyBook:
    """What we know about one company's leads."""

    key: str
    state: CompanyState = CompanyState.ACTIVE
    #: Emails NOT yet sent, in upload order. Anything already away has been
    #: removed, so ``queued[0]`` is always the next person to contact.
    queued: list[str] = field(default_factory=list)
    #: How many have already gone out. Drives the breadth-first tiering -
    #: companies with 0 are served before companies with 1.
    sent: int = 0

    @property
    def touched(self) -> bool:
        return self.sent > 0


def plan_release(companies: list[CompanyBook], cap: int) -> list[str]:
    """Choose up to ``cap`` lead emails to send next, breadth-first.

    Pass 1 takes one lead from every eligible company with none sent yet;
    pass 2 takes a second from companies that already have one away, and so
    on. The result is that new companies are always served before second
    contacts, while the cap still gets filled from depth when breadth runs
    out.

    Companies paused by a reply or a bounce are skipped entirely, as are
    those with nothing queued.
    """
    if cap <= 0:
        return []

    available = [c for c in companies if c.state.eligible and c.queued]
    if not available:
        return []

    # Group by how many have already gone out, so untouched companies come
    # first and the order within a tier follows upload order.
    tiers: dict[int, list[CompanyBook]] = defaultdict(list)
    for company in available:
        tiers[company.sent].append(company)

    taken: dict[str, int] = defaultdict(int)
    chosen: list[str] = []

    for depth in sorted(tiers):
        for company in tiers[depth]:
            if len(chosen) >= cap:
                return chosen
            index = taken[company.key]
            if index < len(company.queued):
                chosen.append(company.queued[index])
                taken[company.key] += 1

    # Breadth exhausted below the cap: go deeper, still one company at a time
    # so the extra sends stay spread rather than piling onto one org.
    while len(chosen) < cap:
        progressed = False
        for depth in sorted(tiers):
            for company in tiers[depth]:
                if len(chosen) >= cap:
                    return chosen
                index = taken[company.key]
                if index < len(company.queued):
                    chosen.append(company.queued[index])
                    taken[company.key] += 1
                    progressed = True
        if not progressed:
            break

    return chosen


def state_after_event(current: CompanyState, event: str) -> CompanyState:
    """Fold a Smartlead event into a company's state.

    ``event`` is one of: reply, bounce, blocked, auto_reply, sent. An
    auto-reply is explicitly NOT a reply - an out-of-office says nothing
    about interest, and treating it as one would silently drop companies.
    Terminal states never reopen.
    """
    if current in (CompanyState.PAUSED_REPLY, CompanyState.PAUSED_BOUNCE):
        return current
    if event == "reply":
        return CompanyState.PAUSED_REPLY
    if event in ("bounce", "blocked"):
        return CompanyState.PAUSED_BOUNCE
    return current
