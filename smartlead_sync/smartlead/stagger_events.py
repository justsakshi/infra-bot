"""Turn Smartlead's per-lead statistics into company-level stagger events.

Smartlead already classifies replies, so nothing here guesses. A row from
``GET /campaigns/{id}/statistics`` carries:

  ``lead_category``  Smartlead's own label - "Out Of Office", "Do Not
                     Contact", "Interested", "Sender Originated Bounce", ...
  ``ignore_reply``   True when Smartlead judged the reply an auto-responder
  ``reply_time``     set when a reply arrived at all
  ``is_bounced``     True for a hard bounce

The rule the team asked for maps onto those directly: a human reply of any
sentiment settles the company, an out-of-office does not, and a bounce or a
block takes the company out because its colleagues are likely to bounce too.

Every field arrives as a STRING, empties included ("None", ""), so the
parsing here is deliberately defensive - reading "False" as truthy would
pause every company in a batch.
"""

from __future__ import annotations

from dataclasses import dataclass

from smartlead.stagger import CompanyState

# Categories that mean a person answered. Sentiment is irrelevant: a "not
# interested" settles the company as firmly as an "interested" one.
HUMAN_REPLY_CATEGORIES: frozenset[str] = frozenset({
    "interested", "not interested", "do not contact", "information request",
    "meeting request", "meeting booked", "wrong person", "unsubscribed",
    "senior not interested",
})

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
        return self.kind in ("reply", "bounce")

    @property
    def company_state(self) -> CompanyState | None:
        if self.kind == "reply":
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

    if category in HUMAN_REPLY_CATEGORIES:
        return LeadEvent(email=email, kind="reply", category=category,
                         detail=f"human reply: {category}")

    # A reply we have no category for is still a human until proven otherwise.
    # Erring towards pausing costs one company; erring the other way keeps
    # emailing someone who already answered.
    if _text(row.get("reply_time")):
        return LeadEvent(email=email, kind="reply", category=category or "replied",
                         detail="reply with no category - treated as human")

    return LeadEvent(email=email, kind="sent", category=category)


def classify_rows(rows: list[dict]) -> dict[str, LeadEvent]:
    """Collapse many statistics rows (one per sequence step) to one event per
    lead, keeping the most consequential: bounce > reply > auto_reply > sent."""
    rank = {"bounce": 3, "reply": 2, "auto_reply": 1, "sent": 0}
    best: dict[str, LeadEvent] = {}
    for row in rows:
        event = classify_row(row)
        if event is None:
            continue
        current = best.get(event.email)
        if current is None or rank[event.kind] > rank[current.kind]:
            best[event.email] = event
    return best
