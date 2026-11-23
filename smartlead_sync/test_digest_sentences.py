"""The digest's approved sentences and where every ask kind goes (t409, 23 Nov 2026).

Manveen approved eight sentences ("sure go aheaf"); the post may hold nothing else. These tests pin the wording
letter for letter, build a day of each sentence type, a day with nothing to do, a day with 9 items (5 in the post, the
rest in the thread, oldest first) and an ask kind no sentence fits, and check the blind test's fact checker. Made-up
domains and addresses only (the repo is public).
"""
from datetime import datetime, timedelta, timezone

import pytest

import digest_post as dp
from smartlead import digest as dg
from smartlead import digest_guard as guard
from smartlead import digest_items as items
from smartlead import digest_sentences as ds

NOW = datetime(2026, 11, 23, 5, 30, tzinfo=timezone.utc)          # Mon 23 Nov 11:00 IST

APPROVED = {   # as Manveen approved them, brackets and all (decision 5)
    "1": "[Client]: [domain] failed its weekly test ([n] of [m] reached the inbox at [provider]). Keep its [k] inboxes "
         "out of campaigns and don't renew the domain.",
    "2": "[Client]: retire [k] inboxes on [domain] in [provider] before [date], or they renew ([$]).",
    "3": "[Client]: delete [k] inboxes in Smartlead that [provider] already removed (list in thread).",
    "4": "[Client]: [k] removed inboxes are still in [campaign]. Take them out, then delete them (list in thread).",
    "5": "[Client]: [inbox] finished warm-up repair. Remove its DO NOT USE tag if you want it back.",
    "6": "[Client]: [domain] is on Spamhaus and in [campaign]. Take its [k] inboxes out.",
    "7": "[Client]: [k] inboxes have no weekly test. Add them to a test in Smart Delivery (steps in thread).",
    "W": "Watching: [domain] replies are low ([a] vs [b] expected); the next weekly test on [date] decides.",
}


def _as_brackets(t: str) -> str:
    names = {"client": "Client", "cost": "$"}
    return t.replace("{", "[").replace("}", "]").replace("[client]", "[Client]").replace("[cost]", "[$]") \
        if names else t


@pytest.mark.parametrize("s", list(APPROVED))
def test_the_wording_is_exactly_what_manveen_approved(s):
    assert _as_brackets(ds.TEMPLATES[s]) == APPROVED[s]


def test_the_approved_mock_renders_word_for_word():
    """The mock Manveen approved for 23 Nov, from made-up data of the same shape."""
    its = [{"s": "2", "f": {"client": "Bettrdata", "k": "3 Google", "domain": "billone.test", "provider": "Zapmail",
                            "date": "27 Nov", "cost": "$14.25"}},
           {"s": "2", "f": {"client": "bettrdata", "k": 3, "domain": "billtwo.test", "provider": "Zapmail",
                            "date": "30 Nov", "cost": None}},
           {"s": "1", "f": {"client": "Melior", "domain": "forum.test", "n": 0, "m": 100, "provider": "Outlook",
                            "k": 25}},
           {"s": "3", "f": {"client": "BettrData", "k": 8, "provider": "Zapmail"}}]
    got = [f"{i}. {ds.sentence(x)}" for i, x in enumerate(its, 1)]
    assert got == [
        "1. BettrData: retire 3 Google inboxes on billone.test in Zapmail before 27 Nov, or they renew ($14.25).",
        "2. BettrData: retire 3 inboxes on billtwo.test in Zapmail before 30 Nov, or they renew.",
        "3. Melior: forum.test failed its weekly test (0 of 100 reached the inbox at Outlook). Keep its 25 inboxes out "
        "of campaigns and don't renew the domain.",
        "4. BettrData: delete 8 inboxes in Smartlead that Zapmail already removed (list in thread)."]
    assert ds.header("Mon 23 Nov", 4) == "Inbox digest · Mon 23 Nov · 4 things to do"
    assert ds.watch_line([{"s": "W", "f": {"domain": "peer.test", "a": 6, "b": 21, "date": "Sat 28 Nov"}}]) == [
        "Watching, nothing to do: peer.test replies are low (6 vs 21 expected); the next weekly test on Sat 28 Nov "
        "decides."]


def test_every_sentence_type_fills_only_its_brackets():
    f = {"client": "pl", "domain": "a.test", "n": 3, "m": 40, "provider": "Gmail", "k": 4, "date": "1 Dec",
         "cost": "$9.00", "campaign": "Camp One", "inbox": "x@a.test", "a": 2, "b": 9}
    out = {s: ds.sentence({"s": s, "f": f}) for s in APPROVED}
    assert out["1"] == ("Precise Leads: a.test failed its weekly test (3 of 40 reached the inbox at Gmail). Keep its 4 "
                        "inboxes out of campaigns and don't renew the domain.")
    assert out["4"] == "Precise Leads: 4 removed inboxes are still in Camp One. Take them out, then delete them (list in thread)."
    assert out["5"] == "Precise Leads: x@a.test finished warm-up repair. Remove its DO NOT USE tag if you want it back."
    assert out["6"] == "Precise Leads: a.test is on Spamhaus and in Camp One. Take its 4 inboxes out."
    assert out["7"] == "Precise Leads: 4 inboxes have no weekly test. Add them to a test in Smart Delivery (steps in thread)."
    assert out["W"] == "Watching: a.test replies are low (2 vs 9 expected); the next weekly test on 1 Dec decides."
    one = {**f, "k": 1}
    assert ds.sentence({"s": "7", "f": one}) == ("Precise Leads: 1 inbox has no weekly test. Add it to a test in Smart "
                                                 "Delivery (steps in thread).")
    assert ds.sentence({"s": "4", "f": one}) == ("Precise Leads: 1 removed inbox is still in Camp One. Take it out, "
                                                 "then delete it (list in thread).")
    with pytest.raises(KeyError):                         # a bracket with no value: never sent with a gap
        ds.sentence({"s": "1", "f": {"client": "Melior", "domain": "a.test"}})


# --- a day of each kind, built through digest_post --------------------------------------------------------------------

def _src(sid, asks, at=NOW):
    return {"_id": sid, "at": at.isoformat(),
            "asks": [dg.Ask(**{k: v for k, v in a.items() if k not in ("since", "inboxes", "domains")}).doc(sid)
                     | {"since": a.get("since", at.isoformat())} for a in asks]}


def _v2(dom, client, n_inboxes=3, inbox=0):
    return [{"email": f"u{i}@{dom}", "domain": dom, "client": client, "night": "2026-11-21", "verdict": "FAIL",
             "type": "OUTLOOK", "weekly": True, "microsoft": {"inbox": inbox, "sent": 4}, "google": {"inbox": 4, "sent": 4}}
            for i in range(n_inboxes)]


def _removed(groups):
    return {"_id": "provider-removed", "at": NOW.isoformat(),
            "asks": [{"text": "Some inboxes were removed by their provider.", "step": "Delete them.",
                      "key": dg.REMOVED_KEY, "kind": "ask", "urgent": False, "since": NOW.isoformat(), "detail": [],
                      "inboxes": [], "domains": [], "more": "The bot never deletes.",
                      "facts": {"type": "removed", "groups": groups}}]}


def _day_of_each(n_bills=1):
    ctx = {"v2": _v2("failed.test", "Melior"), "status": [{"email": "ok@fixed.test", "domain": "fixed.test",
                                                            "client": "BettrData", "state": "do_not_use"}],
           "tests": [{"campaign": "DT Melior #1", "senders": ["a@peer.test"], "next_run": "2026-11-28T02:30:00.000Z"}]}
    bills = [{"text": f"Zapmail will charge Bettrdata $14.25 on {27 + i} Nov for 3 Google inboxes on bill{i}.test that "
                      f"we stopped using on 16 Nov.", "step": "retire them in Zapmail before the bill.",
              "key": f"renewal-review:Zapmail:s{i}:2026-11-{27 + i}",
              "detail": [f"bill{i}.test: drop, 3 inboxes, we stopped using them on 16 Nov"]} for i in range(n_bills)]
    sources = [
        _src("renewal-review", bills),
        _src("fleet-watch", [{"text": "3 inboxes are on the kill list.", "key": "fleet-watch:kill", "step": "Take out.",
                              "detail": [f"u{i}@failed.test (open since 11 Nov)" for i in range(3)]}],
             at=NOW - timedelta(days=1)),
        _removed([{"kind": "delete", "client": "Precise Leads", "provider": "ScaledMail", "k": 4,
                   "lines": ["• Precise Leads 4, removed by ScaledMail:", "– all inboxes on these domains: gone.test (4)"]},
                  {"kind": "campaign", "client": "BettrData", "k": 2, "campaigns": ["Camp Two"],
                   "lines": ["• BettrData 2 in Camp Two (paused):", "– kept.test: c@, d@"]}]),
        _src("warmup-repair", [{"text": "1 inbox on fixed.test finished warm-up repair.", "step": "untag it.",
                                "key": "warmup-repair:ready-to-untag",
                                "detail": ["ok@fixed.test: Google, warm-up score 60% at the start, 93% now"]}]),
        _src("dt-senders", [{"text": "5 Melior inboxes are in no weekly test.", "step": "Add them to a weekly test.",
                             "key": "dt-senders:coverage:Melior",
                             "detail": ["Melior · new.test: 5 inboxes, sending in Camp Three"]}]),
        _src("reply-monitor", [{"text": "peer.test (Melior) gets far fewer replies.", "step": "Look it up.",
                                "key": "reply-monitor:alert",
                                "detail": ["peer.test (Melior): 6 replies on 710 sends in 14 days, 21.3 expected"]}]),
    ]
    return sources, ctx


def test_a_day_with_every_sentence_type():
    sources, ctx = _day_of_each()
    head, parts = dp.build(sources, [], [], NOW, ctx=ctx)
    top, thread = "\n".join(head), "\n".join(parts)
    assert top == (
        "Inbox digest · Mon 23 Nov · 6 things to do\n\n"
        "1. BettrData: retire 3 Google inboxes on bill0.test in Zapmail before 27 Nov, or they renew ($14.25).\n"
        "2. Melior: failed.test failed its weekly test (0 of 12 reached the inbox at Outlook). Keep its 3 inboxes out of "
        "campaigns and don't renew the domain.\n"
        "3. Precise Leads: delete 4 inboxes in Smartlead that ScaledMail already removed (list in thread).\n"
        "4. BettrData: 2 removed inboxes are still in Camp Two. Take them out, then delete them (list in thread).\n"
        "5. Melior: 5 inboxes have no weekly test. Add them to a test in Smart Delivery (steps in thread).\n\n"
        "Watching, nothing to do: peer.test replies are low (6 vs 21 expected); the next weekly test on Sat 28 Nov "
        "decides.\n\n1 more in the thread.")
    # the thread in number order: the lists of 1-5, then 6 with its sentence (t409 readers)
    assert thread.startswith("1 above:\nThe bill: $14.25 on 27 Nov for 3 Google inboxes on bill0.test.")
    assert thread.index("5 above:") < thread.index("6. BettrData: ok@fixed.test finished warm-up repair. Remove its DO "
                                                   "NOT USE tag if you want it back.")
    assert "3 above:\n• Precise Leads 4, removed by ScaledMail:\n– all inboxes on these domains: gone.test (4)\nHow: In " \
           "Precise Leads' Smartlead account, Email Accounts: search each domain listed." in thread
    assert "5 above:\n• Melior · new.test: 5 inboxes, sending in Camp Three\nHow: Add them to a weekly test." in thread
    assert guard.problems(head, parts) == []


def test_a_day_with_nothing_to_do_posts_one_line():
    assert dp.build([], [], [], NOW) == (["Inbox digest · Mon 23 Nov · nothing to do today"], [])


def test_nine_items_show_five_and_the_rest_go_to_the_thread_oldest_first():
    sources, ctx = _day_of_each(n_bills=4)          # 4 bills + kill + delete + campaign + untag + no test = 9
    sources[3]["asks"][0]["since"] = (NOW - timedelta(days=3)).isoformat()      # the untag ask: opened first
    sources[4]["asks"][0]["since"] = (NOW - timedelta(days=2)).isoformat()      # the no-test ask: next
    head, parts = dp.build(sources, [], [], NOW, ctx=ctx)
    numbered = [x for x in "\n".join(head).split("\n") if guard.is_item(x)]
    assert head[0] == "Inbox digest · Mon 23 Nov · 9 things to do" and len(numbered) == 5
    assert "4 more in the thread." in "\n".join(head)
    assert numbered[:4] == [f"{i + 1}. BettrData: retire 3 Google inboxes on bill{i}.test in Zapmail before {27 + i} Nov, "
                            f"or they renew ($14.25)." for i in range(4)]            # dated items first, soonest first
    assert numbered[4].startswith("5. Melior: failed.test failed its weekly test")
    thread = "\n".join(parts)
    starts = [thread.index(x) for x in ("6. BettrData: ok@fixed.test finished warm-up repair",
                                        "7. Melior: 5 inboxes have no weekly test",
                                        "8. Precise Leads: delete 4 inboxes", "9. BettrData: 2 removed inboxes")]
    assert starts == sorted(starts)                       # past the fifth, the one opened first comes first
    assert thread.index("5 above:") < thread.index("6. BettrData")       # number order
    assert guard.problems(head, parts) == []


def test_an_ask_kind_with_no_sentence_goes_to_the_thread_never_the_post():
    wallet = _src("zapmail-digest", [{"text": "Clientco's Zapmail wallet is at $12 with auto-recharge off.",
                                      "step": "Add money to the wallet or switch auto-recharge on.",
                                      "key": "zapmail-digest:low-wallet:acct1"}])
    unknown = _src("brand-new-job", [{"text": "Something new needs a look on new.test.", "step": "Look at it.",
                                      "key": "brand-new-job:x"}])
    head, parts = dp.build([wallet, unknown], [], [], NOW)
    assert head == ["Inbox digest · Mon 23 Nov · 2 things to do", "\n2 more in the thread."]
    assert "2. Clientco's Zapmail wallet is at $12 with auto-recharge off. Add money to the wallet or switch " \
           "auto-recharge on. (wording not approved yet)" in parts[0]
    assert "1. Something new needs a look on new.test. Look at it. (wording not approved yet)" in parts[0]
    assert "wallet" not in "\n".join(head) and "new.test" not in "\n".join(head)


def test_a_lookup_that_cannot_read_its_data_never_guesses():
    """No weekly-test list (Mongo test_coverage not read): a reply alert is not called "no weekly test"; it goes to
    the thread as the job wrote it."""
    sources, ctx = _day_of_each()
    ctx.pop("tests")
    head, parts = dp.build([sources[-1]], [], [], NOW, ctx=ctx)
    assert "no weekly test" not in "\n".join(head + parts)
    assert "peer.test (Melior): 6 replies on 710 sends" in parts[0]


def test_a_reply_alert_on_a_failed_domain_is_the_failed_sentence():
    sources, ctx = _day_of_each()
    ctx["v2"] += _v2("peer.test", "Melior", 2, inbox=1)
    head, _ = dp.build([sources[-1]], [], [], NOW, ctx=ctx)
    assert head[1] == ("\n1. Melior: peer.test failed its weekly test (2 of 8 reached the inbox at Outlook). Keep its 2 "
                       "inboxes out of campaigns and don't renew the domain.")


def test_a_bill_with_some_inboxes_kept_names_only_the_dropped_and_no_amount():
    src = _src("renewal-review", [{"text": "Zapmail will charge Bettrdata $19.50 on 30 Nov for 6 Google inboxes on "
                                           "drop.test, keep.test: we stopped using the 3 on drop.test on 16 Nov; the "
                                           "other 3 stay in use.", "step": "retire the 3 listed as drop.",
                                   "key": "renewal-review:Zapmail:s9:2026-11-30",
                                   "detail": ["drop.test: drop, 3 inboxes, we stopped using them on 16 Nov",
                                              "keep.test: keep, 3 inboxes still in use"]}])
    head, _ = dp.build([src], [], [], NOW)
    assert head[1] == "\n1. BettrData: retire 3 inboxes on drop.test in Zapmail before 30 Nov, or they renew."


def test_every_key_pattern_has_a_destination_and_the_table_lists_it():
    table = items.mapping_table()
    for p, k, _ in items.KEYMAP + items.TEXTMAP:
        assert k in items.KINDS, k
        assert p[:30].replace("|", "\\|")[:10] in table or p[:10] in table
    assert all(dest in ("1", "2", "3", "4", "5", "6", "7", "W", "bot", "dev", "info", "sheet", "thread")
               for dest, _, _ in items.KINDS.values())
    for dest, _, prop in items.KINDS.values():
        assert (dest == "thread") == bool(prop)              # every thread kind has a proposed sentence parked
    assert items.kind_of({"key": "renewal-review:Zapmail:x:2026-11-27"}) == "bill"
    assert items.kind_of({"key": "rr:1", "source": "renewal-review"}) == "bill"
    assert items.kind_of({"key": "key-health:dead:PL", "kind": "ask"}) == "broken"
    assert items.kind_of({"key": "v2-weekly:x", "text": "No result this week from DT A (3 inboxes): no finished run"}) \
        == "no-result"


# --- the blind test's checker ------------------------------------------------------------------------------------------

def _facts():
    sources, ctx = _day_of_each()
    p = dp.plan(sources, [], [], NOW, ctx=ctx)
    head, parts = dp.compose(p, NOW)
    return dp.day_facts(p, NOW, head, parts), head


def test_the_checker_passes_the_template_post():
    f, head = _facts()
    assert ds.check(f["text"].split("--- thread ---")[0], f) == []


def test_the_checker_catches_a_missing_item_an_invented_number_and_a_new_domain():
    f, head = _facts()
    post = f["text"].split("--- thread ---")[0]
    dropped = "\n".join(x for x in post.split("\n") if "failed.test" not in x)
    assert any("missing: failed.test" in x for x in ds.check(dropped, f))
    assert "number not in the facts: 77" in ds.check(post + "\nAbout 77 inboxes.", f)
    assert "name not in the facts: other.test" in ds.check(post + "\nAlso other.test.", f)
    assert any("date not in the facts" in x for x in ds.check(post + "\nBefore 3 Dec.", f))
    six = post + "\n6. a\n7. b"
    assert any("numbered items" in x for x in ds.check(six, f))


def test_a_client_domain_in_another_clients_account_names_both_right():
    """23 Nov 2026: a Melior domain's inboxes sit in the Precise Leads Smartlead account, and All Inboxes files the
    pulled ones under Precise Leads. The sentence names the client whose test it is (Melior); the How names the account
    where the inboxes are (Precise Leads)."""
    sources, ctx = _day_of_each()
    ctx["status"] += [{"email": f"u{i}@failed.test", "domain": "failed.test", "client": "Precise Leads",
                       "account": "PRECISE_LEADS", "state": "pulled"} for i in range(3)]
    head, parts = dp.build([sources[1]], [], [], NOW, ctx=ctx)
    assert head[1].startswith("\n1. Melior: failed.test failed its weekly test")
    assert "How: In the Precise Leads Smartlead account (where Melior's inboxes are), search failed.test and take any " \
           "of its inboxes still in a campaign out" in parts[0]
