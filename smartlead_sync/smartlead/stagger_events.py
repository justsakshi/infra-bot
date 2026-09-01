"""Turn Smartlead's per-lead statistics into company-level stagger events.

Smartlead already classifies replies, so nothing here guesses. A row from
``GET /campaigns/{id}/statistics`` carries:

  ``lead_category``  Smartlead's own label - "Out Of Office", "Do Not
                     Contact", "Interested", "Sender Originated Bounce", ...
  ``ignore_reply``   True when Smartlead judged the reply an auto-responder
  ``reply_time``     set when a reply arrived at all
  ``is_bounced``     True for a hard bounce

Not every reply means the same thing. "Do Not Contact" and "Interested" both
settle the COMPANY - one told us to stop, the other opened a thread a human
should run. "Not Interested" and "Wrong Person" settle only the PERSON: they
are wrong for that role, and a colleague may still be a real prospect. Wrong
Person is literally an instruction to ask someone else. An out-of-office
settles nothing. A bounce or block takes the company out, because its
colleagues share a mail server and are likely to bounce too.

Every field arrives as a STRING, empties included ("None", ""), so the
parsing here is deliberately defensive - reading "False" as truthy would
pause every company in a batch.
"""

from __future__ import annotations

from dataclasses import dataclass

from smartlead.stagger import CompanyState

# A reply can settle the COMPANY or only the PERSON, and the difference
# decides whether colleagues are still worth contacting. Counts below are
# from a live sweep of every campaign on all five accounts (2026-09-01).

# The answer binds the whole organisation. Either they told us to stop, or a
# conversation is open and a second cold email from the same sender would cut
# across it.
COMPANY_STOP_CATEGORIES: frozenset[str] = frozenset({
    "do not contact",       # 70 - explicit opt-out, applies to the org
    "unsubscribed",         #      same, via the unsubscribe link
    "interested",           # 23 - a thread is open; let the human run it
    "meeting request",      # 11
    "meeting booked",
    "information request",  # 25 - they asked us something; answer, do not blast
})

# One person said no for themselves. A colleague in a different role may
# still be a real prospect, so the company stays in rotation - this is the
# whole reason we hold several leads per company.
PERSON_ONLY_CATEGORIES: frozenset[str] = frozenset({
    "not interested",       # 73 - wrong for THEM, not necessarily the company
    "wrong person",         # 27 - literally "ask someone else here"
    "senior not interested",
})

# Kept for callers that only need "did a human answer at all".
HUMAN_REPLY_CATEGORIES: frozenset[str] = COMPANY_STOP_CATEGORIES | PERSON_ONLY_CATEGORIES

# Categories that mean the mail never reached a person.
BOUNCE_CATEGORIES: frozenset[str] = frozenset({
    "sender originated bounce", "bounced", "hard bounce", "invalid email",
})

# Explicitly NOT a reply: an out-of-office says nothing about interest, and
# pausing on it would silently drop companies that never actually answered.
AUTO_REPLY_CATEGORIES: frozenset[str] = frozenset({
    "out of office", "auto reply", "automated reply", "vacation",
})


def _text(value) -> str:
    """Smartlead sends everything as a string, including "None" for empty."""
    text = str(value).strip().lower() if value is not None else ""
    return "" if text in ("none", "null", "") else text


def _flag(value) -> bool:
    return _text(value) in ("true", "1", "yes")


@dataclass(frozen=True)
class LeadEvent:
    """What one statistics row means for its lead and company."""

    email: str
    kind: str          # reply | bounce | auto_reply | sent
    category: str      # Smartlead's own label, for the audit trail
    detail: str = ""

    @property
    def pauses_company(self) -> bool:
        return self.kind in ("reply_company", "bounce")

    @property
    def pauses_lead_only(self) -> bool:
        """A no from one person. Stop contacting THEM, keep the company."""
        return self.kind == "reply_person"

    @property
    def company_state(self) -> CompanyState | None:
        if self.kind == "reply_company":
            return CompanyState.PAUSED_REPLY
        if self.kind == "bounce":
            return CompanyState.PAUSED_BOUNCE
        return None


def classify_row(row: dict) -> LeadEvent | None:
    """Classify one statistics row. Returns None when the row has no email.

    Order matters. A bounce is checked first because a bounced row can also
    carry a category, and the bounce is the more consequential fact. An
    auto-reply is checked before a generic reply so that an out-of-office
    with a reply_time is never treated as a human answer.
    """
    email = _text(row.get("lead_email"))
    if not email:
        return None

    category = _text(row.get("lead_category"))

    if _flag(row.get("is_bounced")) or category in BOUNCE_CATEGORIES:
        return LeadEvent(email=email, kind="bounce", category=category or "bounced",
                         detail="hard bounce" if _flag(row.get("is_bounced")) else category)

    if category in AUTO_REPLY_CATEGORIES or _flag(row.get("ignore_reply")):
        return LeadEvent(email=email, kind="auto_reply", category=category or "auto reply",
                         detail="auto-responder; company stays in rotation")

    if category in COMPANY_STOP_CATEGORIES:
        return LeadEvent(email=email, kind="reply_company", category=category,
                         detail=f"{category} - stops the whole company")

    if category in PERSON_ONLY_CATEGORIES:
        return LeadEvent(email=email, kind="reply_person", category=category,
                         detail=f"{category} - this person only; colleagues stay in rotation")

    # A reply we have no category for is still a human until proven otherwise.
    # Erring towards pausing costs one company; erring the other way keeps
    # emailing someone who already answered.
    if _text(row.get("reply_time")):
        # Uncategorised reply. Stopping the company is the cautious read: a
        # reply we cannot classify may be an opt-out, and re-emailing someone
        # who asked us to stop is the worse error.
        return LeadEvent(email=email, kind="reply_company", category=category or "replied",
                         detail="uncategorised reply - stopping the company to be safe")

    return LeadEvent(email=email, kind="sent", category=category)


def classify_rows(rows: list[dict]) -> dict[str, LeadEvent]:
    """Collapse many statistics rows (one per sequence step) to one event per
    lead, keeping the most consequential:
    bounce > reply_company > reply_person > auto_reply > sent."""
    rank = {"bounce": 4, "reply_company": 3, "reply_person": 2,
            "auto_reply": 1, "sent": 0}
    best: dict[str, LeadEvent] = {}
    for row in rows:
        event = classify_row(row)
        if event is None:
            continue
        current = best.get(event.email)
        if current is None or rank[event.kind] > rank[current.kind]:
            best[event.email] = event
    return best
