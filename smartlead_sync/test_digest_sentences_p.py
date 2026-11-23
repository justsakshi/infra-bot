"""The seven sentences Manveen approved on 23 Nov 2026 ("approve with your changes": P1-P6, P8; P7 rejected).

Each one built from a made-up day through digest_post, in the words the jobs really write; P5 asks only the approved
questions; P1 never names an inbox its provider removed; a kind with no sentence (P7's) never reaches the post.
Made-up domains and addresses only (the repo is public).
"""
import re
from datetime import timedelta

import pytest

import digest_post as dp
from smartlead import digest_guard as guard
from smartlead import digest_items as items
from smartlead import digest_sentences as ds
from test_digest_sentences import NOW, _removed, _src


def _top(head):
    return [x.strip() for x in "\n".join(head).split("\n") if guard.is_item(x.strip())]


def _status(email, client, *, campaigns=(), busy="", provider="Outlook", account=None, state="free"):
    return {"email": email, "domain": email.split("@")[1], "client": client, "account": account or client.upper(),
            "campaigns": list(campaigns), "busy_reason": busy, "provider": provider, "state": state}


# --- the wording ----------------------------------------------------------------------------------------------------

def test_one_inbox_or_domain_reads_in_the_singular():
    one = {"client": "melior", "k": 1, "domain": "a.test", "provider": "Zapmail", "date": "27 Nov",
           "campaign": "Camp One", "question": "cancel-candidates"}
    got = {s: ds.sentence({"s": s, "f": one}) for s in ("P1", "P3", "P5", "P6", "P8")}
    assert got == {
        "P1": "Melior: Smartlead can't log in to 1 inbox on a.test. Reconnect it (list in thread).",
        "P3": "Melior: 1 domain expires at Zapmail before 27 Nov. Renew it or let it go (list in thread).",
        "P5": "Melior: 1 inbox needs your call: cancel it at its provider, or keep repairing its warm-up? (list in "
              "thread).",
        "P6": "Melior: 1 inbox that may not send is still in Camp One. Take it out (list in thread).",
        "P8": "Melior: 1 removed inbox had a lead reply in the last 30 days. Answer the lead, then delete it (list in "
              "thread)."}
    assert ds.sentence({"s": "P2", "f": {"client": "bd", "provider": "Zapmail", "cost": "$14.25", "date": "14 Nov"}}) \
        == "BettrData: the Zapmail bill of $14.25 on 14 Nov failed. Pay it, or let it go on purpose (steps in thread)."
    assert ds.sentence({"s": "P4", "f": {"client": "pl", "test": "pl-weekly-test-2"}}) == (
        "Precise Leads: the weekly test pl-weekly-test-2 looks broken. Re-run it in Smart Delivery (it uses one test "
        "run).")


@pytest.mark.parametrize("q", list(ds.QUESTIONS))
def test_p5_asks_only_an_approved_question(q):
    many, one = ds.QUESTIONS[q]
    assert ds.sentence({"s": "P5", "f": {"client": "Melior", "k": 4, "question": q}}) == \
        f"Melior: 4 inboxes need your call: {many} (list in thread)."
    assert ds.sentence({"s": "P5", "f": {"client": "Melior", "k": 1, "question": q}}) == \
        f"Melior: 1 inbox needs your call: {one} (list in thread)."
    assert many.endswith("?") and one.endswith("?") and not re.search(r"\d", many + one)     # fixed words, no data


def test_p5_never_takes_a_jobs_words():
    with pytest.raises(KeyError):
        ds.sentence({"s": "P5", "f": {"client": "Melior", "k": 4, "question": "say yes to switch it on?"}})
    with pytest.raises(KeyError):
        ds.sentence({"s": "P5", "f": {"client": "Melior", "k": 4}})
    assert set(ds.QUESTIONS) == {"cancel-candidates", "repair-settings", "bill-check", "act-mode", "volume-step-back"}


# --- a day of each, through digest_post -----------------------------------------------------------------------------

def test_p1_one_item_per_client_from_every_job_that_says_it():
    status = _src("inbox-status", [{"text": "2 inboxes in live campaigns are disconnected in Smartlead, so their share "
                                            "is not being sent.", "step": "Reconnect them.",
                                    "key": "inbox-status:disconnected",
                                    "detail": ["• Melior · Camp: One: a@lock.test, b@lock.test"]}])
    repair = _src("warmup-repair", [{"text": "Smartlead can't log in to 2 inboxes on lock.test, so their warm-up is "
                                             "stopped.", "step": "reconnect each one listed.",
                                     "key": "warmup-repair:login-blocked",
                                     "detail": ["• lock.test (Melior): 2 inboxes, Outlook, warm-up score 40% (good from "
                                                "90%): a@, b@"]}])
    weekly = _src("v2-weekly", [{"text": "3 inboxes sent nothing in this week's test because Smartlead can't log in to "
                                         "them (other.test 3), so they have no result.", "step": "reconnect them.",
                                 "key": "v2-weekly:cant"}])
    ctx = {"status": [_status(f"u{i}@other.test", "Precise Leads") for i in range(3)], "v2": [], "tests": []}
    head, parts = dp.build([status, repair, weekly], [], [], NOW, ctx=ctx)
    assert sorted(x[3:] for x in _top(head)) == [
        "Melior: Smartlead can't log in to 2 inboxes on lock.test. Reconnect them (list in thread).",
        "Precise Leads: Smartlead can't log in to 3 inboxes on other.test. Reconnect them (list in thread)."]
    thread = "\n".join(parts)
    assert "delete" not in thread.lower()                    # Manveen cut "or delete them if they were cancelled"
    assert "filter Disconnected and reconnect each inbox listed" in thread
    assert guard.problems(head, parts) == []


def test_p1_never_names_an_inbox_its_provider_removed():
    """A cancelled order's inboxes show as disconnected too; they belong to the delete reminder (sentences 3/4)."""
    status = _src("inbox-status", [{"text": "2 inboxes in live campaigns are disconnected.", "step": "Reconnect.",
                                    "key": "inbox-status:disconnected",
                                    "detail": ["• BettrData · Camp: gone@cut.test, kept@live.test"]}])
    removed = _removed([{"kind": "delete", "client": "BettrData", "provider": "Zapmail", "k": 1,
                         "lines": ["• BettrData 1, removed by Zapmail:", "– cut.test: gone@"]}])
    removed["asks"][0]["inboxes"] = ["gone@cut.test"]
    head, parts = dp.build([status, removed], [], [], NOW)
    p1 = [x for x in _top(head) if "can't log in" in x]
    assert p1 == [p1[0]] and "1 inbox on live.test" in p1[0]
    assert "gone@cut.test" not in "\n".join(x for x in parts if "can't log in" in x or "Disconnected" in x)
    # a reminder stored before t409 (words only) is never a login item either
    old = _removed(None)
    old["asks"][0].pop("facts")
    head, parts = dp.build([old], [], [], NOW)
    assert not any("can't log in" in x for x in _top(head))


def test_p2_a_failed_bill():
    src = _src("billing-watch", [{"text": "Payment failed (card declined): Zapmail, Workspace 3 for BettrData.",
                                  "step": "Zapmail, Billing, pay the invoice or fix the card on file; if the inboxes "
                                          "should go, let them lapse on purpose.",
                                  "key": "billing-watch:payment:Zapmail:s1:2026-11-20",
                                  "detail": ["Zapmail · Workspace 3 for BettrData: bill date 20 Nov, $42.00, 12 inboxes, "
                                             "status ACTIVE", "Domains (4): a.test, b.test, c.test, d.test"]}])
    head, parts = dp.build([src], [], [], NOW)
    assert _top(head) == ["1. BettrData: the Zapmail bill of $42.00 on 20 Nov failed. Pay it, or let it go on purpose "
                          "(steps in thread)."]
    assert "How: Zapmail, Billing, pay the invoice" in parts[0]
    status = _src("billing-watch", [{"text": "Subscription is now CANCELLED: Zapmail, Workspace 4 for BettrData.",
                                     "step": "Check it.", "key": "billing-watch:status:Zapmail:s2:CANCELLED:",
                                     "detail": ["Zapmail · Workspace 4 for BettrData: status CANCELLED"]}])
    head, _ = dp.build([status], [], [], NOW)
    assert _top(head) == []                                   # a status change alone is not a failed bill: thread


def test_p3_domains_expiring_the_first_date_named():
    src = _src("zapmail-digest", [{"text": "3 client domains expire within 14 days, the first on 27 Nov.",
                                   "step": "In Zapmail (the account named), Domains, check each domain listed is set "
                                           "to renew before its date.", "key": "zapmail-digest:domains-expiring",
                                   "detail": ["one.test: expires 27 Nov, Melior, Zapmail account M1, 3 inboxes",
                                              "two.test: expires 2 Dec, Melior, Zapmail account M1, 3 inboxes",
                                              "three.test: expires 30 Nov, BettrData, Zapmail account B1, 0 inboxes"]}])
    head, parts = dp.build([src], [], [], NOW)
    assert _top(head) == [
        "1. Melior: 2 domains expire at Zapmail before 3 Dec. Renew them or let them go (list in thread).",
        "2. BettrData: 1 domain expires at Zapmail before 1 Dec. Renew it or let it go (list in thread)."]
    assert parts[0].index("one.test: expires 27 Nov") < parts[0].index("two.test: expires 2 Dec")    # soonest first
    assert "How: In Zapmail (the account named), Domains: set each domain listed to renew before its date; the first " \
           "expires on 27 Nov." in parts[0] and "client is gone" not in parts[0]
    assert guard.problems(head, parts) == []


def test_p4_a_broken_test_stored_in_the_jobs_old_words():
    src = _src("v2-weekly", [{"text": "Microsoft results of the test \"melior-weekly-test-4\" (run 1, Sat 21 Nov) were "
                                      "not used: nearly every inbox in it scored below 4 of 4 there.",
                              "step": "re-run it.", "key": "v2-weekly:held"}])
    head, parts = dp.build([src], [], [], NOW)
    assert _top(head) == ["1. Melior: the weekly test melior-weekly-test-4 looks broken. Re-run it in Smart Delivery "
                          "(it uses one test run)."]
    assert "How: Re-run that test in Smart Delivery today" in parts[0]


def test_p5_each_decision_kind_gets_its_question():
    ctx = {"status": [_status(f"r{i}@slow.test", "Melior") for i in range(2)]
           + [_status(f"x{i}@bd.test", "BettrData") for i in range(5)], "v2": [], "tests": []}
    repair = _src("warmup-repair", [
        {"text": "2 inboxes on slow.test still have a low warm-up score after 3 weeks of repair.",
         "step": "tell the bot which of them to cancel, or to keep repairing them.",
         "key": "warmup-repair:cancel-candidates",
         "detail": ["• r0@slow.test: Outlook, warm-up score 50% at the start, 52% now", "• r1@slow.test: Outlook"]},
        {"text": "1 inbox on bd.test with a warm-up score under 90% is still on normal warm-up settings.",
         "step": "say yes and it goes on the repair settings.", "key": "warmup-repair:not-on-repair-settings",
         "detail": ["• x0@bd.test: Google, warm-up score 85% (good from 90%), now 20 warm-up emails a day"]}])
    volume = _src("volume-review", [
        {"text": "1 domain outside the pilot should step back to a lower daily limit.", "step": "Your call: add it.",
         "key": "volume-review:suggested", "detail": ["Melior · slow.test: 10 -> 5 a day on 2 inboxes (bounces)"]},
        {"text": "The volume review could not change the daily limits on 1 pilot domain (bd.test).",
         "step": "check the limits by hand.", "key": "volume-review:not-changed", "detail": ["bd.test"]}])
    weekly = _src("v2-weekly", [{"text": "All four agreed conditions for letting the weekly job act on BettrData now "
                                         "hold.", "step": "say yes to switch it on.", "key": "v2-weekly:d8",
                                 "detail": ["BettrData: 4 of 4 conditions hold"]}])
    bill = _src("renewal-review", [{"text": "Zapmail will charge Bettrdata $9.50 on 30 Nov for 3 Google inboxes on "
                                            "chk.test: check 3 of them.", "step": "decide.",
                                    "key": "renewal-review:Zapmail:s7:2026-11-30",
                                    "detail": ["chk.test: check, 3 inboxes, no campaign in 30 days"]}])
    p = dp.plan([repair, volume, weekly, bill], [], [], NOW, ctx=ctx)
    got = {(it["f"]["client"], it["f"]["question"]): ds.sentence(it) for it in p["items"]}
    assert got == {
        ("Melior", "cancel-candidates"): "Melior: 2 inboxes need your call: cancel them at their provider, or keep "
                                         "repairing their warm-up? (list in thread).",
        ("BettrData", "repair-settings"): "BettrData: 1 inbox needs your call: put it on the warm-up repair settings, "
                                          "which repair it faster? (list in thread).",
        ("Melior", "volume-step-back"): "Melior: 2 inboxes need your call: lower their daily limit by adding their "
                                        "domains to the volume pilot? (list in thread).",
        ("BettrData", "act-mode"): "BettrData: 5 inboxes need your call: let the weekly test take the failing ones "
                                   "out of campaigns, as it does for the other clients? (list in thread).",
        ("BettrData", "bill-check"): "BettrData: 3 inboxes need your call: keep them, or cancel them before their "
                                     "next bill? (list in thread)."}
    # the volume ask with no approved question stays in the thread
    assert [t["what"] for t in p["thread"]] == ["The volume review could not change the daily limits on 1 pilot "
                                                "domain (bd.test)."]


def test_p6_take_out_from_every_job_that_says_it():
    held = _src("inbox-status", [{"text": "3 inboxes that failed the weekly test or are tagged DO NOT USE still send in "
                                          "1 live campaign, so they can pull that campaign into spam.",
                                  "step": "In Smartlead, open each campaign listed and remove those inboxes.",
                                  "key": "inbox-status:held_but_sending",
                                  "detail": ["• Melior · Agencies | Two: a@bad.test, b@bad.test (failed the weekly "
                                             "test); c@bad.test (tagged DO NOT USE)"]}])
    gate = _src("gate-selftest", [{"text": "3 inboxes tagged or failed are still attached to sending campaigns: Melior "
                                           "· Agencies | Two (3), so they can pull those campaigns into spam.",
                                   "step": "In Smartlead, open each campaign named and remove those inboxes.",
                                   "key": "gate-selftest:held_back_not_sending",
                                   "detail": ["bad.test: a@, b@, c@"]}])
    stuck = _src("v2-weekly", [{"text": "Campaign PL | Solo is live with only failed inboxes left (s@solo.test), so all "
                                        "its email risks landing in spam.", "step": "pause it.", "key": "v2-weekly:s"}])
    early = _src("fleet-watch", [{"text": "2 inboxes are in a campaign before their ready date.",
                                  "step": "Take each inbox listed out of its campaign until its ready date.",
                                  "key": "fleet-watch:early_campaign",
                                  "detail": ["e1@early.test (ready 30 Nov)", "e2@early.test (ready 30 Nov)"]}])
    ctx = {"status": [_status("s@solo.test", "Precise Leads", campaigns=["PL | Solo"], busy="failed_v2"),
                      _status("e1@early.test", "BettrData", campaigns=["Retail | One"], busy="no_v2_pass"),
                      _status("e2@early.test", "BettrData", campaigns=["Retail | One"], busy="")],
           "v2": [], "tests": []}
    p = dp.plan([held, gate, stuck, early], [], [], NOW, ctx=ctx)
    got = sorted(ds.sentence(it) for it in p["items"])
    assert got == sorted([
        "Melior: 3 inboxes that may not send are still in Agencies | Two. Take them out (list in thread).",
        "Precise Leads: 1 inbox that may not send is still in PL | Solo. Take it out (list in thread).",
        "BettrData: 1 inbox that may not send is still in Retail | One. Take it out (list in thread)."])
    assert not any("e2@early.test" in x for it in p["items"] for x in it["detail"])   # All Inboxes says it may send
    hows = "\n".join(x for it in p["items"] for x in it["steps"])
    assert "weekly job also takes out" not in hows and "open each campaign named and remove the inboxes listed" in hows


def test_p8_removed_inboxes_a_lead_replied_to():
    src = _removed([{"kind": "replied", "client": "Precise Leads", "provider": "ScaledMail", "k": 2,
                     "lines": ["• Precise Leads 2, a lead replied in the last 30 days:", "– reply.test: r1@, r2@"]}])
    head, parts = dp.build([src], [], [], NOW)
    assert _top(head) == ["1. Precise Leads: 2 removed inboxes had a lead reply in the last 30 days. Answer the lead, "
                          "then delete them (list in thread)."]
    assert "answer each lead's reply (Master Inbox)" in parts[0]


# --- P7 rejected: no unapproved kind reaches the post ---------------------------------------------------------------

def test_no_unapproved_kind_reaches_the_top_post():
    fix = [_src("zapmail-digest", [{"text": "Clientco's Zapmail wallet is at $12 with auto-recharge off.",
                                    "step": "Add money.", "key": "zapmail-digest:low-wallet:acct1"}]),
           _src("fleet-watch", [{"text": "2 inboxes have warm-up switched off.", "step": "Turn it on.",
                                 "key": "fleet-watch:warmup_off", "detail": ["w@off.test", "v@off.test"]}]),
           _src("v2-weekly", [{"text": "2 inboxes sent nothing in this week's test (quiet.test 2), so they have no "
                                       "result.", "step": "check they are connected in Smartlead.",
                               "key": "v2-weekly:dead"}]),
           _src("billing-watch", [{"text": "Bought outside the bot: Zapmail, Workspace 9.", "step": "Check it.",
                                   "key": "billing-watch:new:Zapmail:s9", "detail": ["Zapmail · Workspace 9"]}]),
           _src("brand-new-job", [{"text": "Something new needs a look on new.test.", "step": "Look at it.",
                                   "key": "brand-new-job:x"}])]
    p = dp.plan(fix, [], [], NOW)
    head, parts = dp.compose(p, NOW)
    assert p["items"] == [] and _top(head) == []
    assert head[0] == "Inbox digest · Mon 23 Nov · 5 things to do" and "5 more in the thread." in "\n".join(head)
    assert "\n".join(parts).count("(wording not approved yet)") == 5
    assert all(t["kind"] == "fix" for t in p["thread"])
    # and on a full day, every item in the post is one of the approved sentences
    for it in p["items"]:
        assert it["s"] in ds.TEMPLATES


def test_every_routed_kind_is_an_approved_sentence_or_off_the_post():
    for kind, (dest, _, _) in items.KINDS.items():
        assert dest in ds.TEMPLATES or dest in ("bot", "dev", "info", "sheet") or kind == "fix", kind


def test_a_bracket_the_data_cannot_fill_stays_in_the_thread():
    """A failed bill with no amount in its list: no "[$]" gap in the post."""
    src = _src("billing-watch", [{"text": "Payment failed (card declined): Zapmail, Workspace 3 for BettrData.",
                                  "step": "Pay it.", "key": "billing-watch:payment:Zapmail:s1:2026-11-20",
                                  "detail": ["Zapmail · Workspace 3 for BettrData: status ACTIVE"]}],
               at=NOW - timedelta(hours=1))
    head, parts = dp.build([src], [], [], NOW)
    assert _top(head) == [] and "(wording not approved yet)" in parts[0]


def test_p1_guard_holds_even_when_the_upstream_cut_missed_an_inbox():
    """route() itself leaves out removed inboxes (and a domain every inbox of which was removed), in case a job's line
    is one digest.drop_removed can't read."""
    from smartlead.digest import REMOVED_KEY
    asks = [{"key": REMOVED_KEY, "text": "Removed.", "facts": {"type": "removed", "groups": []},
             "inboxes": ["gone@cut.test"], "domains": ["whole.test"], "detail": []},
            {"key": "inbox-status:disconnected", "source": "inbox-status", "text": "3 inboxes are disconnected.",
             "detail": ["• BettrData · Camp: gone@cut.test, kept@live.test, any@whole.test"]},
            {"key": "v2-weekly:cant", "source": "v2-weekly", "text": "Smartlead can't log in to them (cut.test 2)."}]
    ctx = {"status": [_status("x@cut.test", "BettrData")], "v2": [], "tests": []}
    p = items.route(asks, [], [], ctx, NOW)
    p1 = [it for it in p["items"] if it["s"] == "P1"]
    assert [ds.sentence(it) for it in p1] == ["BettrData: Smartlead can't log in to 1 inbox on live.test. Reconnect it "
                                              "(list in thread)."]
    # counted only, on a domain with a removed inbox: can't tell which, so the thread
    assert any("cut.test 2" in t["what"] for t in p["thread"])
