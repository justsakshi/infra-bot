"""Own provider first: one rule for every reader of a placement, spam-test or seed result.

Every campaign is ESP-matched: Google senders mail Google recipients, Outlook senders mail Outlook recipients.
So an inbox is judged on its OWN provider's seeds only: a Google (GMAIL) inbox on Google/Gmail, an Outlook inbox
on Microsoft/Outlook (Manveen, 11 Nov 2026: "google to google and outlook to outlook first"). A result at the
other provider is a signal, never a verdict: it never makes an inbox or its domain spam, FAIL, BUSY, DO NOT USE,
pulled, killed or dropped at renewal, and wherever it is shown it is labelled so (Manveen, 17 Nov 2026, after
standup: "we give the first priority the same provider and if it lands on spam on the other provider it can be
a signal but we can't mark it as spam").

An SMTP inbox, or one whose type is not known, has no own provider: it is judged on both, as before.

Pure. The v2 job's own copy of the rule is smartlead/v2_results.own_verdict; this module shares its table.
"""
from __future__ import annotations

from smartlead.usable import kind_of
from smartlead.v2_results import OWN_PROVIDER

GOOGLE, MICROSOFT = "google", "microsoft"
BOTH = (GOOGLE, MICROSOFT)
NAMES = {GOOGLE: "Google", MICROSOFT: "Microsoft"}
SIGNAL = "a signal, not a verdict"
# A grid cell for a domain's other provider below the pass line (v2_grid, the classic report's grid cells): the
# team still sees where the seeds landed, and no reader takes it for a verdict (sheets.DeliverabilityReader).
SIGNAL_CELL = "spam (other provider: signal only)"

_ALIASES = {
    GOOGLE: ("google", "gmail", "g suite", "gsuite", "g_suite", "google_workspace", "googleworkspace",
             "google workspace"),
    MICROSOFT: ("microsoft", "outlook", "office365", "office 365", "office_365", "microsoft_365", "microsoft365",
                "microsoft 365", "hotmail", "exchange"),
}


def provider_key(name) -> str:
    """Any provider label the code meets ("G Suite", "Office365", "Google", "Outlook", "GOOGLE_WORKSPACE",
    "MICROSOFT_365", "google", "microsoft") -> "google" | "microsoft"; "" when it is neither."""
    s = str(name or "").strip().lower()
    for key, names in _ALIASES.items():
        if s in names:
            return key
    return ""


def own_provider(kind) -> str | None:
    """The provider an inbox is judged on: "google" for a Google inbox, "microsoft" for an Outlook one, None for
    SMTP or an unknown type (judged on both). ``kind`` is Smartlead's type or the sync's provider name."""
    return OWN_PROVIDER.get(kind_of(kind))


def judged_on(kind) -> tuple[str, ...]:
    own = own_provider(kind)
    return (own,) if own else BOTH


def is_own(provider, kind) -> bool:
    """Does a result at ``provider`` (any label) count for an inbox of ``kind``? Always True for SMTP / unknown."""
    return provider_key(provider) in judged_on(kind)


def split(by_provider: dict, kind) -> tuple[dict, dict]:
    """({provider: v} that count for this inbox, {provider: v} that are only a signal). Keys are kept as given.
    A key that is neither Google nor Microsoft counts only for an inbox with no own provider."""
    own = own_provider(kind)
    mine, other = {}, {}
    for p, v in (by_provider or {}).items():
        k = provider_key(p)
        if own is None or k == own:
            mine[p] = v
        else:
            other[p] = v
    return mine, other


def common_kind(kinds) -> str:
    """The one Smartlead type shared by these inboxes ("GMAIL", "OUTLOOK"), else "" (mixed, SMTP or unknown):
    a line or a domain that mixes types is judged on both providers."""
    ks = {kind_of(k) for k in kinds}
    if len(ks) == 1:
        k = ks.pop()
        return k if k in OWN_PROVIDER else ""
    return ""


def signal(provider, what: str) -> str:
    """'Microsoft 0/4 (not its provider: a signal, not a verdict)' style label for a result at the other provider."""
    name = NAMES.get(provider_key(provider)) or str(provider)
    return f"{name} {what} (not its provider: {SIGNAL})"
