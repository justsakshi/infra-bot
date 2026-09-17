"""Placement is a property of the MAILBOX, not the domain.

Test 535093 (2026-09-17, 13-15 seeds per sender) measured three mailboxes on
thebettrdatas.com:

    laurie.d@thebettrdatas.com          12/13  92%   healthy
    laurie@thebettrdatas.com             0/13   0%   dead
    laurie.donnelly@thebettrdatas.com    0/15   0%   dead

_build_inbox_row read `deliverability_map[domain]` BEFORE
`deliverability_map[email]`, so both dead mailboxes inherited their healthy
sibling's "inbox" status. They graded A/96 on the Inboxes tab while delivering
nothing, and the team had no way to see it. Thirteen of the seventeen dead
mailboxes in that test were graded A or B this way.

The domain entry remains the fallback for a mailbox never tested on its own.
"""
from datetime import date

from smartlead.processing import _build_inbox_row
from smartlead.health import compute_health_score

TODAY = date(2026, 9, 17)
FRESH = "2026-09-17"


def ok(c, m):
    print(f"  {'PASS' if c else 'FAIL'}: {m}")
    assert c, m


def row(email, dmap):
    return _build_inbox_row(
        account={"id": 1, "from_name": "X", "type": "GMAIL", "message_per_day": 15,
                 "is_smtp_success": True, "is_imap_success": True},
        email=email, campaign_name="C", campaign_status="ACTIVE",
        load_info={"leads_remaining": 0, "inbox_count": 1, "individual_load": 0},
        deliverability_map=dmap,
    )


# --- the live regression: a dead mailbox beside a healthy sibling ---
dmap = {
    "thebettrdatas.com": {"status": "inbox", "date": FRESH},
    "laurie@thebettrdatas.com": {"status": "fail", "date": FRESH},
}
dead = row("laurie@thebettrdatas.com", dmap)
ok(dead["test_sheet_status"] == "fail",
   "a mailbox's own failing test beats its domain's passing one")

healthy = row("laurie.d@thebettrdatas.com", dmap)
ok(healthy["test_sheet_status"] == "inbox",
   "an untested mailbox still falls back to its domain's result")

# --- and the grade must follow, or the fix is invisible where people look ---
dead_score = compute_health_score(
    {**dead, "warmup_rep_pct": "96%", "connection_ok": True, "bounce_rate": None},
    TODAY)
ok(dead_score["grade"] == "D",
   f"a mailbox landing 0/13 in spam grades D, not A (got {dead_score['grade']}"
   f"/{dead_score['score']})")

# Before the floor, warmup+bounce+connection still summed to 55 = grade C.
ok(dead_score["score"] <= 25,
   f"failed placement caps the score at 25 (got {dead_score['score']})")

# --- a clean mailbox is untouched by the floor ---
good_score = compute_health_score(
    {**healthy, "warmup_rep_pct": "96%", "connection_ok": True, "bounce_rate": 0.5},
    TODAY)
ok(good_score["grade"] in {"A", "B"},
   f"a passing mailbox still grades well (got {good_score['grade']}"
   f"/{good_score['score']})")

# --- an untested mailbox on an untested domain is neutral, not condemned ---
unknown = row("nobody@newdomain.com", {})
ok(unknown["test_sheet_status"] == "Unknown",
   "no evidence anywhere stays Unknown rather than inheriting a verdict")
u_score = compute_health_score(
    {**unknown, "warmup_rep_pct": "95%", "connection_ok": True, "bounce_rate": None},
    TODAY)
ok(u_score["score"] > 25,
   "never-tested is not the same as failed - it must not hit the fail floor")

print("\nALL PASSED")
