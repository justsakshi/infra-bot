"""The daily digest stays readable on a bad day (t386, 23 Nov 2026: "why does it fail to be readable again and again").

Every earlier readability round was judged on one good day. These fixtures copy the SHAPE of the 23 Nov posted digest
(a 429 on one account, 112 removed inboxes, a domain three jobs reported with verdicts that disagree, weekly test asks
stored by older code with a results file name in them) with made-up domains and addresses (the repo is public), plus a
good day, and every one must keep the render guard's limits (smartlead/digest_guard.py). A change that makes any of
them unreadable again fails the build here.
"""
from datetime import datetime, timedelta, timezone

import pytest

import digest_post as dp
from smartlead import digest as dg
from smartlead import digest_guard as guard
from smartlead import digest_render as render
from smartlead import provider_removed as pr

NOW = datetime(2026, 11, 23, 5, 30, tzinfo=timezone.utc)          # Mon 23 Nov 11:00 IST
SAT = datetime(2026, 11, 21, 8, 32, tzinfo=timezone.utc)
KILL_DOM = "forumagency.test"
USERS = ["j-doe", "j.doe", "j_doe", "jdoe-r", "jdoe-rep", "jdoe.r", "jdoe.rep", "jdoe_r"] + \
        [f"rep{i}" for i in range(17)]                                # 25 inboxes, dots, dashes and underscores


def _src(sid, asks, at=NOW, **extra):
    return {"_id": sid, "at": at.isoformat(), **extra,
            "asks": [dg.Ask(**{k: v for k, v in a.items() if k != "since"}).doc(sid)
                     | {"since": a.get("since", at.isoformat())} for a in asks]}


def _removed(n_now=8, n_check=7, n_unchecked=97, errors=None, as_of=None):
    """The delete reminder as provider_removed.source hands it over: BettrData to delete now and check first, and PL
    inboxes whose campaign and reply check did not finish (23 Nov: Smartlead's rate limit on the PL key)."""
    rows = [{"email": f"u{i}@gone{i % 5}.test", "domain": f"gone{i % 5}.test", "client": "BettrData",
             "account": "BETTRDATA", "provider": "Zapmail", "why": pr.ZAP_EMPTY, "live": []} for i in range(n_now)]
    base = {"client": "BettrData", "account": "BETTRDATA", "provider": "Zapmail", "why": pr.ZAP_EMPTY, "live": []}
    check = [{**base, "email": f"c{i}@paused{i % 2}.test", "domain": f"paused{i % 2}.test",
              "campaigns": ["Client push (paused)"], "notes": [], **({"as_of": as_of} if as_of else {})}
             for i in range(n_check)]
    later = [{"email": f"pl{i}@plhost{i % 30}.test", "domain": f"plhost{i % 30}.test", "client": "Precise Leads",
              "account": "PRECISE_LEADS", "provider": "ScaledMail", "why": pr.SM_CANCELLED, "live": [],
              "campaigns": [], "notes": [pr.NO_CAMPAIGNS, pr.NO_REPLY_CHECK]} for i in range(n_unchecked)]
    a = pr.ask(rows, {"gone1.test", "gone2.test"}, check + later, errors)
    doc = a.doc(pr.SOURCE) | {"since": NOW.isoformat(), "where": "", "lines": pr.LIST_LINES,
                              "inboxes": sorted(r["email"] for r in rows + check + later), "domains": [],
                              "facts": {"type": "removed", "groups": pr.groups(rows, check + later,
                                                                              {"gone1.test", "gone2.test"})}}
    return {"_id": pr.SOURCE, "at": NOW.isoformat(), "asks": [doc]}


def _bad_day():
    """The 23 Nov shape: kill list from two jobs, a held weekly test (old wording, a .csv name) on the same domain,
    the gate self-test about the same inboxes, two bills, a reply alert, an untag ask, a not-yet-in test, a short-test
    info item, a 429 broken line, and the delete reminder with 97 unchecked inboxes."""
    kill = [_src("fleet-watch", [{"text": "25 inboxes are on the kill list.", "key": "fleet-watch:kill",
                                  "step": "Take each inbox listed out of its campaigns.",
                                  "detail": [f"{u}@{KILL_DOM} (open since 11 Nov)" for u in USERS]}]),
            _src("kill-report", [{"text": "1 domain is landing in spam on the classic tests.", "key": "kill-report:domains",
                                  "step": "Don't add inboxes on the domains listed.",
                                  "detail": [f"Melior · {KILL_DOM} (25 inboxes): 26% reached the inbox"]}],
                 at=NOW - timedelta(days=5))]
    v2 = _src("v2-weekly", [
        {"text": "Microsoft results in melior-weekly-test-4-run-1.csv (2026-11-21) held: nearly every "
                 "inbox in that test was below 4/4 there, which looks like a test problem, so nothing was pulled for "
                 "it. Re-run that test in Smart Delivery today.", "key": "v2-weekly:held", "since": SAT.isoformat()},
        {"text": "pl-weekly-test-2-run-1.csv: 1 seed result had not come in when it was downloaded, so "
                 "those inboxes were not judged. Download it again later today.", "key": "v2-weekly:pending",
         "since": SAT.isoformat()},
        {"text": "No result this week from DT Melior copy check, DT PL copy check (9 inboxes): no finished v2 run found.",
         "step": "Check their tests ran in Smart Delivery v2.", "key": "v2-weekly:none", "since": SAT.isoformat()}],
        at=SAT)
    gate = _src("gate-selftest", [{"text": "Gate self-test: 25 of 37 inboxes that failed v2 are not marked failed_v2 on "
                                           "the list (it may be a day behind the verdicts).", "kind": "broken",
                                   "key": "gate-selftest:list_marks_failed_v2",
                                   "step": "Look again after tomorrow's 10:00 sync.",
                                   "detail": [f"{KILL_DOM}: " + ", ".join(u + "@" for u in USERS[:8]) + " and 17 more"]}])
    bills = _src("renewal-review", [
        {"text": "Zapmail will charge Clientco $14.25 on 27 Nov for 3 Google inboxes on billone.test that we stopped "
                 "using on 16 Nov.", "step": "retire them in Zapmail (Domains, Before the bill) before 27 Nov, so the "
                                            "bill does not renew them.", "key": "rr:1",
         "detail": ["billone.test: drop, 3 inboxes, we stopped using them on 16 Nov (tagged DO NOT USE in Smartlead)"]},
        {"text": "Zapmail will charge Clientco $19.50 on 30 Nov for 6 Google inboxes on billtwo.test, billthree.test "
                 "that we stopped using on 16 Nov.", "step": "retire the 3 listed as drop in Zapmail before 30 Nov.",
         "key": "rr:2", "detail": ["billtwo.test: drop, 3 inboxes, we stopped using them on 16 Nov"]}])
    reply = _src("reply-monitor", [{"text": "peerhost.test (Melior) gets far fewer replies than its campaigns' other "
                                            "domains over 14 days, a sign its emails may be landing in spam.",
                                    "step": "Look up its latest weekly test result (All Inboxes tab): failed, take its "
                                            "inboxes out of their campaigns; passed, leave it.",
                                    "more": "Nothing was paused. The item goes once its replies recover.",
                                    "key": "reply-monitor:alert",
                                    "detail": ["peerhost.test (Melior): 6 replies on 710 sends in 14 days, 21.3 expected"]}],
                 at=NOW - timedelta(days=1))
    untag = _src("warmup-repair", [{"text": "1 inbox on fixed.test finished warm-up repair (score 90%+), but a DO NOT "
                                             "USE tag still keeps it out of campaigns.",
                                    "step": "if you want it back, remove the DO NOT USE tag in Smartlead (Email Accounts).",
                                    "key": "warmup-repair:ready-to-untag",
                                    "detail": ["• sam.k@fixed.test · google · 61% → 93% (good from 90%) · day 6 · "
                                               "warm-up 20→10/day, reply 20→35%"]}])
    info = _src("inbox-status", [{"text": "24 inboxes in a weekly test have no result in the last 10 days because "
                                          "their last test runs sent too few emails to judge; next run Sat 28 Nov.",
                                  "kind": "info", "key": "inbox-status:sending_test_short",
                                  "detail": [f"Precise Leads · short{i}.test: 3 inboxes, in the weekly test of PL | "
                                             f"Pre-warmed test; last run 19 Nov sent 1 email per provider (4 needed)"
                                             for i in range(8)]}],
                 numbers={"Precise Leads": {"Safe to send": 62, "Not proven yet": 15, "Do not use": 117},
                          "Melior": {"Safe to send": 124, "Recovering": 5}})
    keys = _src("key-health", [{"text": "Smartlead could not be reached with the PRECISE_LEADS key (HTTP 429), so "
                                        "that account may be missing from today's checks.", "kind": "broken",
                                "key": "key-health:unreachable:PRECISE_LEADS"}])
    sources = kill + [v2, gate, bills, reply, untag, info, keys, _removed(errors={"PRECISE_LEADS": "rate limit"})]
    done = [{"_id": f"d{i}", "text": t} for i, t in enumerate([
        "Test copy refreshed for 3 clients from their live campaigns.",
        "Warm-up repair, Melior: 7 recovering, 6 worse. Also out of campaigns for other reasons: 14 waiting for a "
        "weekly test re-run, 100 on a blocklisted domain."])]
    holds = [{"_id": "h1", "domains": ["heldco.test"], "reason": "A teammate handles these.", "owner": "Teammate",
              "until": None}]
    ctx = {"verdicts": {KILL_DOM: "never", "peerhost.test": "send", "fixed.test": "never"},
           # the weekly test of 21 Nov: every inbox on the kill domain 0 of 4 at Outlook (counts stored as text, as
           # older rows are); a passing domain elsewhere
           "v2": [{"email": f"{u}@{KILL_DOM}", "domain": KILL_DOM, "client": "Melior", "night": "2026-11-21",
                   "verdict": "FAIL", "type": "OUTLOOK", "weekly": True,
                   "microsoft": "{'inbox': 0, 'reported': 4, 'sent': 4}", "google": "{'inbox': 4, 'reported': 4, 'sent': 4}"}
                  for u in USERS] +
                  [{"domain": "fine.test", "client": "Melior", "night": "2026-11-21", "verdict": "PASS",
                    "microsoft": "{'inbox': 4, 'reported': 4, 'sent': 4}"}],
           "status": [{"email": "sam.k@fixed.test", "domain": "fixed.test", "client": "Melior", "state": "do_not_use"}],
           "tests": [{"campaign": "DT Melior #1", "senders": ["a@peerhost.test"], "next_run": "2026-11-28T02:30:00.000Z"},
                     {"campaign": "DT Melior copy check", "senders": [], "next_run": "2026-11-28T05:00:00.000Z"},
                     {"campaign": "DT PL copy check", "senders": [], "next_run": "2026-11-28T04:00:00.000Z"}]}
    return sources, done, holds, ctx


def _good_day():
    sources = [_src("renewal-review", [{"text": "Zapmail will charge Clientco $14.25 on 27 Nov for 3 Google inboxes on "
                                                "billone.test that we stopped using on 16 Nov.",
                                        "step": "retire them in Zapmail before 27 Nov.", "key": "rr:1",
                                        "detail": ["billone.test: drop, 3 inboxes"]}]),
               _src("inbox-status", [], numbers={"Melior": {"Safe to send": 124}})]
    return sources, [{"_id": "d", "text": "Test copy refreshed for 3 clients from their live campaigns."}], [], {}


def _build(day):
    sources, done, holds, ctx = day
    return dp.build(sources, done, [], NOW, holds, (), ctx)


DAYS = {"bad day (23 Nov shape)": _bad_day, "good day": _good_day,
        "80 inboxes to delete now": lambda: ([_removed(n_now=80, n_check=0, n_unchecked=0)], [], [], {}),
        "a 429 with an earlier check kept": lambda: ([_removed(n_check=40, n_unchecked=0, as_of="2026-11-20T04:10:41+00:00",
                                                               errors={"BETTRDATA": "rate limit"})], [], [], {})}


@pytest.mark.parametrize("name", DAYS)
def test_every_day_keeps_every_limit(name):
    head, parts = _build(DAYS[name]())
    assert guard.problems(head, parts) == [], name


def test_the_bad_day_reads_as_one_item_per_domain_in_todays_words():
    """t409: on the 23 Nov shape the post is the approved sentences only; the held test is folded into the failed
    domain's item; the bot's own problems are one dev-channel message."""
    sources, done, holds, ctx = _bad_day()
    p = dp.plan(sources, done, [], NOW, holds, (), ctx)
    head, parts = dp.compose(p, NOW)
    top, thread = "\n".join(head), "\n".join(parts)
    assert top == (
        "Inbox digest · Mon 23 Nov · 6 things to do\n\n"
        "1. Clientco: retire 3 Google inboxes on billone.test in Zapmail before 27 Nov, or they renew ($14.25).\n"
        "2. Clientco: retire 3 inboxes on billtwo.test in Zapmail before 30 Nov, or they renew.\n"
        f"3. Melior: {KILL_DOM} failed its weekly test (0 of 100 reached the inbox at Outlook). Keep its 25 inboxes out "
        "of campaigns and don't renew the domain.\n"
        "4. BettrData: delete 8 inboxes in Smartlead that Zapmail already removed (list in thread).\n"
        "5. BettrData: 7 removed inboxes are still in Client push. Take them out, then delete them (list in thread).\n\n"
        "Watching, nothing to do: peerhost.test replies are low (6 vs 21 expected); the next weekly test on Sat 28 Nov "
        "decides.\n\n1 more in the thread.")
    assert "6. Melior: sam.k@fixed.test finished warm-up repair. Remove its DO NOT USE tag if you want it back." in thread
    # one item per domain: the held test is covered by the failed item, and the thread says it was left out
    assert "melior-weekly-test-4" not in thread and "first looked like a broken test" not in thread
    assert f"Left out: an older ask to re-run the weekly test of {KILL_DOM} (it looked like a broken test); All " \
           "Inboxes now marks its inboxes do not use, so no re-run is needed." in thread
    assert "Weekly test of Sat 21 Nov: 25 of 25 inboxes failed; 0 of 100 test emails reached the inbox at Outlook" in thread
    assert "classic" not in thread.split("For info")[0]
    assert f"How: In Melior's Smartlead account, search {KILL_DOM} and take any of its inboxes still in a " \
           "campaign out" in thread
    for leak in (".csv", "run-1", "2026-11-21", "failed_v2", "(last checked", "→10/day"):
        assert leak not in top + thread, leak
    # the delete reminder: unchecked inboxes are never listed address by address
    assert "pl1@plhost1.test" not in thread and "could not check replies" not in thread
    # the bot's own problems: the dev channel, never the digest
    dev = dp.dev_post(p, NOW)
    for x in ("PRECISE_LEADS key (HTTP 429)", "Gate self-test", 'the test "pl-weekly-test-2" was read before all its '
              "results were in (1 missing)", "DT Melior copy check, DT PL copy check (9 inboxes)",
              "did not finish for 97 Precise Leads inboxes"):
        assert x in dev, x
    for x in ("429", "Gate self-test", "Not checked yet"):
        assert x not in top + thread, x
    # the facts the post was made from (blind test): every bracket filled from data
    f = dp.day_facts(p, NOW, head, parts)
    assert f["_id"] == "2026-11-23" and f["n"] == 6 and [i["s"] for i in f["items"]] == ["2", "2", "1", "3", "4", "5"]
    assert f["watch"][0]["f"] == {"domain": "peerhost.test", "a": 6, "b": 21, "date": "Sat 28 Nov"}


def test_a_failed_read_keeps_the_last_good_split():
    """23 Nov: 68 delete now / 44 check first on 20 Nov became 8 / 104 when the PL check hit the rate limit."""
    good = {"read": True, "inboxes": {f"a{i}@x.test": {"campaigns": [], "replied": 0, "campaigns_unread": False,
                                                        "replies_unread": False} for i in range(3)}}
    good["inboxes"]["a2@x.test"]["campaigns"] = [{"name": "Push", "status": "PAUSED"}]
    t20 = NOW - timedelta(days=3)
    old = pr.merge_use(None, good, t20)
    failed = {"read": True, "errors": {"PRECISE_LEADS": "rate limit"},
              "inboxes": {e: {"campaigns": [], "replied": 0, "campaigns_unread": True, "replies_unread": True}
                          for e in good["inboxes"]} | {"new@y.test": {"campaigns": [], "replied": 0,
                                                                      "campaigns_unread": True, "replies_unread": True}}}
    merged = pr.merge_use(old, failed, NOW)
    use = pr.use_fresh({"use": merged, "use_at": NOW.isoformat()}, NOW)
    rows = [{"email": e, "domain": e.split("@")[1], "client": "Precise Leads", "account": "PRECISE_LEADS",
             "provider": "ScaledMail", "why": pr.SM_CANCELLED, "live": []} for e in list(good["inboxes"]) + ["new@y.test"]]
    now_rows, check = pr.split(rows, use, NOW)
    assert sorted(r["email"] for r in now_rows) == ["a0@x.test", "a1@x.test"]      # the 20 Nov split, kept
    assert all(r.get("as_of") for r in now_rows)
    a = pr.ask(now_rows, set(), check, merged["errors"])
    assert a.text.startswith("4 inboxes were removed by their provider and can't send again: delete 2 now (Precise "
                             "Leads 2), check 1 first (Precise Leads 1); 1 not checked yet (Precise Leads 1; "
                             "Smartlead's rate limit).")
    assert a.detail[0] == "• Precise Leads: today's check did not finish (Smartlead's rate limit), so its inboxes are " \
                          "placed by its check of Fri 20 Nov."
    # a check that failed as a whole keeps the old one, and so does an account that could not be read at all
    assert pr.merge_use(old, {"read": False, "why": "HTTPStatusError"}, NOW)["inboxes"] == old["inboxes"]
    lost = pr.merge_use(old, {"read": True, "inboxes": {}, "errors": {"PRECISE_LEADS": "rate limit"}}, NOW)
    assert set(lost["inboxes"]) == set(good["inboxes"]) and all(pr.complete(u) for u in lost["inboxes"].values())


# --- the cause of the 429: fewer calls on the shared key ----------------------------------------------------------------

class Account:
    """A Smartlead account with N campaigns and R replies in the window, counting calls."""

    def __init__(self, campaigns=40, replies=120, members=None):
        recent = (NOW - timedelta(days=3)).isoformat().replace("+00:00", "Z")
        self.camps = [{"id": i, "name": f"Push {i}", "status": "PAUSED"} for i in range(campaigns)]
        self.members = members or {}
        self.stats = {i: [{"lead_email": f"l{i}-{k}@lead.test", "stats_id": f"s{i}-{k}", "reply_time": recent}
                          for k in range(replies // campaigns)] for i in range(campaigns)}
        self.calls = 0

    async def get(self, path, params=None):
        self.calls += 1
        if path == "/campaigns":
            return self.camps
        if path.endswith("/email-accounts"):
            return [{"from_email": e} for e in self.members.get(int(path.split("/")[2]), [])]
        if path.endswith("/statistics"):
            return {"data": self.stats[int(path.split("/")[2])] if not params["offset"] else []}
        if path == "/leads/":
            return {"id": 1}
        if path.endswith("/message-history"):
            return {"history": [{"type": "SENT", "from": "someone@else.test"}]}
        raise AssertionError(path)


def _calls(acc, want, threads=None):
    import asyncio
    asyncio.run(pr.account_use(acc.get, "PL", want, NOW - timedelta(days=30), {}, {}, threads))
    return acc.calls


def test_the_check_makes_far_fewer_calls():
    """Before t386 every removed inbox needed every reply's thread read (2 calls a reply), every morning. Now an
    inbox already in a real campaign needs no reply check, and a reply's thread is read once and kept."""
    want = {f"r{i}@gone.test" for i in range(10)}
    in_camps = {i: sorted(want) for i in range(3)}
    # all in a real campaign: campaigns + their email-accounts only
    assert _calls(Account(members=in_camps), want) == 1 + 40
    # some in no campaign: the first morning reads each reply's thread, the next morning reads none of them again
    threads: dict = {}
    first = _calls(Account(), want, threads)
    second = _calls(Account(), want, threads)
    assert first == 1 + 40 + 40 + 2 * 120 and second == 1 + 40 + 40
    assert pr.PACE_S >= 0.6                         # at most 100 calls a minute: half the key's shared limit


def test_a_429_is_not_an_unreachable_key(monkeypatch):
    import asyncio

    import httpx

    import key_health_monitor as kh

    class Resp:
        status_code, text = 429, "Too Many Requests"

    class Client:
        def __init__(self, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def get(self, url, params=None):
            return Resp()
    monkeypatch.setattr(httpx, "AsyncClient", Client)

    class Acc:
        name, api_key = "PRECISE_LEADS", "k" * 20
    r = asyncio.run(kh._probe(Acc()))
    assert r["state"] == "BUSY" and "works" in r["detail"]


# --- the guard's own rules ----------------------------------------------------------------------------------------------

def test_a_user_name_is_never_a_domain():
    assert dg.items_of({"text": f"{KILL_DOM}: j-doe@, j.doe@, j_doe@ and 21 more"}) == (set(), {KILL_DOM})
    head, parts = _build(_bad_day())
    assert "j.doe" not in "\n".join(head)


def test_clean_removes_internal_names():
    assert guard.clean("Microsoft results in melior-weekly-test-4-run-1.csv (2026-11-21) held") == \
        'Microsoft results in "melior-weekly-test-4" (Sat 21 Nov) held'
    assert guard.clean("25 inboxes not marked failed_v2 (t235) (last checked Sat 21 Nov)") == \
        "25 inboxes not marked as failed"
    assert guard.clean("Gate self-test: 25 of 37 inboxes that failed v2 are not marked failed_v2 on the list (x).") == \
        ('Gate self-test: All Inboxes does not yet give "failed the weekly test" as the reason for 25 of the 37 '
         'inboxes that failed it (they can still be marked do not use for another reason; the sync may be a day '
         'behind) (x).')
    assert guard.clean("<https://docs.example/d/ab_cd|the inbox sheet> j_doe@x.test") == \
        "<https://docs.example/d/ab_cd|the inbox sheet> j_doe@x.test"


def test_names_are_capped_and_a_repeated_note_is_said_once():
    line = "• Check first: " + ", ".join(f"a{i}@h{i}.test (could not check replies)" for i in range(63))
    out = guard.cap_names(guard.group_notes(line))
    assert len(guard.names(out)) == guard.MAX_NAMES and "and 57 more" in out
    assert out.count("could not check replies") == 1


def test_the_guard_holds_a_raw_dump_to_the_limits():
    head = ["Inbox digest", "\n1. " + "x " * 400, "\n2. short", "\nFor info: y.", "\nIn the thread: z."]
    parts = [f"Part {k}\n• " + ", ".join(f"a{i}@h{k}{i}.test (could not check replies)" for i in range(63))
             for k in range(5)]
    h, p = guard.enforce(head, parts)
    assert guard.problems(h, p) == []
    assert len(p) == guard.MAX_REPLIES and all(len(x) <= guard.REPLY_CHARS for x in p)


def test_stored_old_wording_is_rendered_in_todays_words():
    a = {"text": "Microsoft results in melior-weekly-test-4-run-1.csv (2026-11-21) held: nearly every "
                 "inbox in that test was below 4/4 there. Re-run that test in Smart Delivery today.", "step": ""}
    r = render.render(a, [{"domain": "fine.test", "client": "Melior", "night": "2026-11-21",
                           "microsoft": "{'inbox': 1, 'reported': 4, 'sent': 4}"}])
    assert r["text"].startswith('Microsoft results of the test "melior-weekly-test-4" (Sat 21 Nov) '
                                'were not used') and r["step"].startswith("re-run that test")
    assert r["facts"]["domains"] == ["fine.test"] and r["where"] == "fine.test"
    # the current wording reads back to the same facts, so a new ask and an old one say the same
    new = render.facts_of('Microsoft results of the test "melior-weekly-test-4" (run 1, Sat 21 Nov) '
                          'were not used: nearly every inbox in it scored below 4 of 4', "2026-11-21")
    assert {k: new[k] for k in ("type", "providers", "test", "client")} == \
        {k: r["facts"][k] for k in ("type", "providers", "test", "client")}
    # an ask with nothing known keeps its words
    assert render.render({"text": "Something else."}) == {"text": "Something else."}


def test_a_stored_bill_says_which_inboxes_were_dropped_and_that_the_rest_stay():
    """23 Nov: both readers asked what happens to the other 3 inboxes of '6 inboxes on a.test, b.test that we stopped
    using', when the list dropped only the 3 on a.test."""
    a = {"key": "renewal-review:Zapmail:sub_x:2026-11-30",
         "text": "Zapmail will charge Clientco $19.50 on 30 Nov for 6 Google inboxes on billtwo.test, billthree.test "
                 "that we stopped using on 16 Nov.",
         "detail": ["• billtwo.test: drop, 3 inboxes, we stopped using them on 16 Nov"]}
    assert render.render(a)["text"] == ("Zapmail will charge Clientco $19.50 on 30 Nov for 6 Google inboxes on "
                                        "billtwo.test, billthree.test: we stopped using the 3 on billtwo.test on "
                                        "16 Nov; the other 3 stay in use.")
    whole = {**a, "text": a["text"].replace("6 Google", "3 Google").replace(", billthree.test", "")}
    assert render.render(whole)["text"] == whole["text"]          # every inbox dropped: the sentence was right


def test_a_run_of_lines_with_the_same_ending_says_it_once():
    run = [f"• Client · s{i}.test: 3 inboxes, in the weekly test of PL | Test; last run 19 Nov sent 1 email per "
           f"provider (4 needed); next run Sat 28 Nov" for i in range(4)]
    out = guard.shared_tail(["For info above, the list:"] + run + ["• and 5 more"])
    assert out[1] == ("Each: in the weekly test of PL | Test; last run 19 Nov sent 1 email per provider (4 needed); "
                      "next run Sat 28 Nov")
    assert out[2:6] == [f"• Client · s{i}.test: 3 inboxes" for i in range(4)] and out[-1] == "• and 5 more"
    assert guard.shared_tail(run[:2]) == run[:2]                     # two lines: left as they are


def test_a_stored_warm_up_repair_line_is_in_todays_words():
    a = {"key": "warmup-repair:ready-to-untag", "text": "1 inbox finished warm-up repair.",
         "detail": ["• sam.k@fixed.test · google · 61% → 93% (good from 90%) · day 6 · warm-up repaired: only a "
                    "person's DO NOT USE tag keeps it out · warm-up 20→10/day, reply 20→35%"]}
    assert render.render(a)["detail"] == ["• sam.k@fixed.test: Google, warm-up score 61% at the start, 93% now "
                                          "(good from 90%), day 6 of repair"]


def test_a_stored_gate_step_for_manveen_becomes_a_thing_to_do():
    """23 Nov: 'Check the PC is on ...' sat under "What broke". t409: the PC job's set-up is the bot's own problem:
    the dev channel, with the step."""
    gate = _src("gate-selftest", [{"text": "Gate self-test: the v2 job runs in warn mode, so it does not take failed "
                                           "inboxes out.", "kind": "broken", "key": "gate-selftest:v2_job_alive",
                                   "step": "Check the PC is on and the task 'InfraBot v2 weekly' is enabled."}])
    p = dp.plan([gate], [], [], NOW)
    assert dp.compose(p, NOW) == (["Inbox digest · Mon 23 Nov · nothing to do today"], [])
    assert "the task 'InfraBot v2 weekly'" in dp.dev_post(p, NOW)


def test_the_post_never_drops_an_item_the_thread_does_not_have():
    """The composer shows as many items as the post's limits allow; every other item is in the thread in full, so the
    guard never has to cut an item out of the post."""
    many = [_src(f"job{k}", [{"text": f"Thing {k} on host{k}.test " + "needs a careful look today " * 6,
                              "step": f"Look at host{k} " + "closely " * 15, "key": f"job{k}:x"}]) for k in range(6)]
    head, parts = dp.build(many, [], [], NOW)
    top, thread = "\n".join(head), "\n".join(parts)
    for k in range(6):
        assert f"Thing {k} on host{k}.test" in top + thread
    assert guard.problems(head, parts) == []
