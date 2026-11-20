"""Inboxes their provider removed (smartlead/provider_removed.py, t352): called removed only from ScaledMail or Zapmail
data, one delete reminder in the digest, and no other ask names them (20 Nov 2026: the digest asked to reconnect the
login of inboxes on a cancelled ScaledMail order)."""
from datetime import datetime, timedelta, timezone

import digest_post as dp
from smartlead import digest as dg
from smartlead import provider_removed as pr
from smartlead import warmup_repair as wr

NOW = datetime(2026, 11, 20, 5, 30, tzinfo=timezone.utc)        # Fri 20 Nov 11:00 IST


def _sm(plan=None, registered=(), cancelled=()):
    """ScaledMail facts: domains on a live plan, domains registered there with no plan, domains on a cancelled order."""
    orders = [{"id": "o1", "status": "Active"}, {"id": "o2", "status": "Cancelled"}]
    domains = [{"domain": d, "payment_id": "o1"} for d in plan or ["live.example"]]
    domains += [{"domain": d, "payment_id": "o2"} for d in cancelled]
    reg = [{"domain": d} for d in list(registered) + [x["domain"] for x in domains]]
    return pr.scaledmail_facts(orders, domains, reg)


def _zap(domains=(), mailboxes=(), errors=()):
    return pr.zapmail_facts([{"domain": d, "status": s, "assigned_mailboxes": n, "account": "PL"} for d, s, n in domains],
                            [{"email": e} for e in mailboxes], errors)


def _inbox(email, client="Precise Leads", state="idle", campaigns=()):
    return {"email": email, "domain": email.split("@")[1], "client": client, "state": state, "campaigns": list(campaigns)}


def test_scaledmail_no_plan_and_zapmail_empty_domain_are_removed_a_live_inbox_never():
    facts = {"scaledmail": _sm(registered=["desk.example"]),
             "zapmail": _zap([("co-one.example", "ACTIVE", 0), ("zap.example", "ACTIVE", 2)],
                             ["a@zap.example", "b@zap.example"])}
    inboxes = [_inbox(f"{u}@desk.example") for u in ("a", "b", "c", "d")]
    inboxes += [_inbox("x@co-one.example", "BettrData"), _inbox("a@zap.example", "BettrData"),
                _inbox("ok@live.example", "Melior"), _inbox("own@inboxkit.example")]
    rows = pr.removed(facts, inboxes)
    assert [(r["email"], r["provider"]) for r in rows] == [
        ("a@desk.example", "ScaledMail"), ("b@desk.example", "ScaledMail"), ("c@desk.example", "ScaledMail"),
        ("d@desk.example", "ScaledMail"), ("x@co-one.example", "Zapmail")]
    assert pr.whole_domains(rows, inboxes) == {"desk.example", "co-one.example"}


def test_a_cancelled_scaledmail_order_counts_and_a_listed_zapmail_inbox_stays():
    facts = {"scaledmail": _sm(cancelled=["gone.example"]),
             "zapmail": _zap([("zap.example", "ACTIVE", 2)], ["a@zap.example", "b@zap.example"])}
    rows = pr.removed(facts, [_inbox("a@gone.example"), _inbox("a@zap.example")])
    assert [(r["email"], r["why"]) for r in rows] == [("a@gone.example", pr.SM_CANCELLED)]


def test_a_zapmail_mailbox_missing_counts_only_when_zapmail_listed_every_mailbox_on_the_domain():
    inboxes = [_inbox("a@half.example"), _inbox("b@half.example"), _inbox("a@short.example")]
    facts = {"scaledmail": _sm(),
             "zapmail": _zap([("half.example", "ACTIVE", 1), ("short.example", "ACTIVE", 3)], ["a@half.example",
                                                                                              "b@short.example"])}
    rows = pr.removed(facts, inboxes)
    # half.example: Zapmail has 1 mailbox and listed it, so b@ is gone. short.example: it says 3 but listed 1, so the
    # list is not complete and nothing is judged there.
    assert [(r["email"], r["why"]) for r in rows] == [("b@half.example", pr.ZAP_GONE)]


def test_an_expired_zapmail_domain_and_an_old_clients_inbox_are_not_listed():
    """The brief's rule: a Zapmail mailbox missing from a domain we still hold. An expired domain is no evidence; an
    old client's inboxes (OSC) are left alone, as warm-up repair does."""
    facts = {"scaledmail": _sm(), "zapmail": _zap([("lapsed.example", "EXPIRED", 0), ("held.example", "INACTIVE", 0),
                                                   ("old.example", "ACTIVE", 0)])}
    rows = pr.removed(facts, [_inbox("a@lapsed.example"), _inbox("a@held.example"), _inbox("a@old.example", "OSC")])
    assert [r["email"] for r in rows] == ["a@held.example"]
    assert pr.removed(facts, [_inbox("b@held.example", "Bettrdata")])[0]["client"] == "BettrData"


def test_nothing_is_judged_from_a_provider_that_was_not_read_or_answered_empty():
    inboxes = [_inbox("a@desk.example"), _inbox("x@co-one.example")]
    zap_ok = _zap([("co-one.example", "ACTIVE", 0)])
    assert pr.removed({"scaledmail": _sm(registered=["desk.example"]), "zapmail": _zap(errors=["PL/GOOGLE: 500"])},
                      inboxes) == []                     # no Zapmail read: a ScaledMail domain could be hosted there
    empty = pr.scaledmail_facts([{"id": "o1", "status": "Active"}], [], [{"domain": "desk.example"}])
    assert empty["read"] is False                        # an empty domain list while an order is live is no evidence
    assert [r["email"] for r in pr.removed({"scaledmail": empty, "zapmail": zap_ok}, inboxes)] == ["x@co-one.example"]
    assert pr.removed(None, inboxes) == []


def test_a_gone_inbox_and_a_login_failure_alone_are_never_called_removed():
    facts = {"scaledmail": _sm(registered=["desk.example"]), "zapmail": _zap()}
    assert pr.removed(facts, [_inbox("a@desk.example", state="gone")]) == []
    # disconnected in Smartlead, but its ScaledMail plan is live: it stays the reconnect ask, not the delete one
    assert pr.removed(facts, [_inbox("a@live.example", state="disconnected")]) == []


def test_the_ask_groups_by_provider_and_client_and_names_inboxes_only_on_part_domains():
    facts = {"scaledmail": _sm(registered=["desk.example", "calendar.example"]),
             "zapmail": _zap([("half.example", "ACTIVE", 1)], ["a@half.example"])}
    inboxes = ([_inbox(f"{u}@desk.example") for u in ("a", "b", "c", "d")] + [_inbox("a@calendar.example", campaigns=["X"])]
               + [_inbox("a@half.example", "BettrData"), _inbox("b@half.example", "BettrData")])
    rows = pr.removed(facts, inboxes)
    a = pr.ask(rows, pr.whole_domains(rows, inboxes))
    assert a.text == "6 inboxes were removed by their provider; delete them from Smartlead by hand."
    assert a.key == pr.KEY
    assert a.detail == [
        "• Precise Leads 5 at ScaledMail (order cancelled, no inboxes left, 2 domains): calendar.example 1 (1 still in "
        "a sending campaign), desk.example 4",
        "• BettrData 1 at Zapmail (inbox gone from the domain, 1 domain): half.example b@"]
    assert "never deletes" in a.step and "by hand" in a.text
    for word in ("disconnect", "log in", "reconnect"):            # no hold or reader takes it for a login ask
        assert word not in (a.text + a.step).lower()
    assert pr.ask([], set()) is None


class Col:
    def __init__(self, docs=()):
        self.docs = [dict(d) for d in docs]

    def find_one(self, q):
        return next((dict(d) for d in self.docs if d.get("_id") == q.get("_id")), None)

    def replace_one(self, q, doc, upsert=False):
        self.docs = [d for d in self.docs if d.get("_id") != q["_id"]] + [dict(doc)]

    def find(self, q=None, proj=None):
        return [dict(d) for d in self.docs if not (q or {}).get("state") or d.get("state") != q["state"]["$ne"]]


def test_facts_are_saved_kept_per_provider_when_unread_and_joined_with_todays_smartlead_inboxes():
    db = {"provider_inventory": Col(), "inbox_status": Col([_inbox("a@desk.example"), _inbox("a@gone-now.example", state="gone")])}
    facts = {"scaledmail": _sm(registered=["desk.example", "gone-now.example"]), "zapmail": _zap()}
    assert pr.save_facts(db, facts, NOW - timedelta(hours=2))
    assert pr.save_facts(db, {"scaledmail": {"read": False}, "zapmail": _zap()}, NOW - timedelta(hours=1))
    doc = db["provider_inventory"].find_one({"_id": pr.DOC_ID})
    assert doc["scaledmail"]["read"] is True and doc["scaledmail_at"] == (NOW - timedelta(hours=2)).isoformat()
    assert pr.emails(db, NOW) == {"a@desk.example"}              # the one deleted by hand (gone) is not asked again
    s = pr.source(db, NOW)
    (a,) = s["asks"]
    assert a["inboxes"] == ["a@desk.example"] and a["domains"] == ["desk.example"] and a["key"] == dg.REMOVED_KEY
    assert pr.source(db, NOW + timedelta(days=9)) is None        # facts older than 8 days are not used
    assert pr.source({"provider_inventory": Col(), "inbox_status": Col()}, NOW) is None


def test_smartleads_own_list_brings_in_inboxes_in_no_campaign_and_the_sync_gives_client_and_gone():
    """20 Nov 2026: Mongo inbox_status (and inbox_daily) leave out inboxes in no campaign: 96 of PL's 422 Smartlead
    inboxes, among them 21 on Zapmail domains with no mailboxes left. Smartlead's own list is the inbox list."""
    status = [_inbox("a@desk.example", "Melior", campaigns=["Live"]), _inbox("b@desk.example", state="gone")]
    sl = pr.smartlead_facts([{"email": "A@desk.example", "client": "Precise Leads"},
                             {"email": "b@desk.example", "client": "Precise Leads"},
                             {"email": "x@old-client.example", "client": "Precise Leads"}])
    facts = {"smartlead": sl, "scaledmail": _sm(registered=["desk.example"]),
             "zapmail": _zap([("old-client.example", "ACTIVE", 0)])}
    inboxes = pr.inboxes_of(facts, status)
    assert [(i["email"], i["client"], i["campaigns"], i["state"]) for i in inboxes] == [
        ("a@desk.example", "Melior", ["Live"], "idle"), ("x@old-client.example", "Precise Leads", [], "in no campaign")]
    assert [(r["email"], r["provider"]) for r in pr.removed(facts, inboxes)] == [
        ("a@desk.example", "ScaledMail"), ("x@old-client.example", "Zapmail")]
    # an account not read: the sync's inboxes alone, never a guess
    assert pr.smartlead_facts([{"email": "a@x.example"}], ["BettrData: HTTPStatusError"])["read"] is False
    assert [i["email"] for i in pr.inboxes_of({"smartlead": {"read": False}}, status)] == ["a@desk.example"]


def _src(sid, asks):
    return {"_id": sid, "at": NOW.isoformat(), "asks": [dg.Ask(**a).doc(sid) | {"since": NOW.isoformat()} for a in asks]}


def _removed_source():
    facts = {"scaledmail": _sm(registered=["desk.example"]), "zapmail": _zap([("co-one.example", "ACTIVE", 0)])}
    inboxes = [_inbox(f"{u}@desk.example") for u in ("a", "b", "c", "d")] + [_inbox("x@co-one.example", "BettrData"),
                                                                            _inbox("a@live.example")]
    db = {"provider_inventory": Col(), "inbox_status": Col(inboxes)}
    pr.save_facts(db, facts, NOW - timedelta(hours=1))
    return pr.source(db, NOW)


def test_the_digest_shows_removed_inboxes_only_in_the_delete_reminder():
    """The 20 Nov digest: 'reconnect the login' on 3 inboxes of a cancelled ScaledMail order. Now: one delete reminder,
    and the login, weekly-test and kill-list asks leave them out (an ask about nothing else goes)."""
    login = _src("warmup-repair", [{"text": "3 inboxes can't warm up: Smartlead blocked their warm-up because it can't "
                                            "log in to them (credentials).", "step": "Fix the login first.",
                                    "key": "warmup-repair:login-blocked",
                                    "detail": ["desk.example (PL): 3 inboxes, custom mail server: a@, b@, c@"]}])
    mixed = _src("inbox-status", [{"text": "3 inboxes are sending with no weekly test result.", "step": "Add them.",
                                   "key": "inbox-status:sending_unproven",
                                   "detail": ["Precise Leads · desk.example: 2 inboxes", "Precise Leads · live.example: 1 inbox"]}])
    named = _src("fleet-watch", [{"text": "2 inboxes are disconnected in live campaigns.", "step": "Reconnect them.",
                                  "key": "fleet-watch:disc", "detail": ["• Camp: a@desk.example, a@live.example"]}])
    hidden = _src("dt-senders", [{"text": "4 inboxes are not in the weekly test.", "step": "Add them.",
                                  "key": "dt-senders:cover", "detail": ["• a@desk.example, b@desk.example and 2 more"]}])
    head, parts = dp.build([login, mixed, named, hidden, _removed_source()], [], [], NOW)
    text = "\n".join(head + parts)
    assert "can't warm up" not in text and "Fix the login" not in text
    # desk.example is named once in the whole digest: in the delete reminder's list
    assert text.count("desk.example") == 1 and "• Precise Leads 4 at ScaledMail (order cancelled, no inboxes left, " \
                                                 "1 domain): desk.example 4" in text
    lines = [x for x in "\n".join(head).split("\n") if x.strip()]
    assert any(x.endswith("5 inboxes were removed by their provider; delete them from Smartlead by hand: Precise Leads "
                          "4 at ScaledMail, BettrData 1 at Zapmail. Do: Delete them by hand in Smartlead, Email Accounts "
                          "(the client's account): search each domain listed; a domain with a count goes whole, one "
                          "with names only those. Or run the matching CHROME-PROMPT-delete file in Claude in Chrome. "
                          "The bot never deletes; an inbox leaves this list once it is gone.") for x in lines)
    assert "3 inboxes are sending with no weekly test result (some removed by their provider, left out: see the " \
           "delete reminder)" in text and "Precise Leads · live.example: 1 inbox" in text
    assert "2 inboxes are disconnected in live campaigns (1 of them removed by their provider" in text
    assert ": Camp: a@live.example." in text                      # a mixed line loses only the removed address
    assert "4 inboxes are not in the weekly test (2 of them removed" in text and ": 2 more." in text


def test_the_delete_reminder_is_left_alone_by_a_login_hold_and_held_items_never_show_twice():
    """The 18 Nov hold on 5 dead BettrData domains held only "can't log in / disconnected" asks. Its inboxes are now
    in the delete reminder, so the login ask about them goes and the hold holds nothing (nothing listed as held)."""
    hold = {"_id": "h", "domains": ["co-one.example"], "when": "disconnected", "owner": "Manveen", "until": None,
            "reason": "dead BettrData inboxes on the delete list."}
    disc = _src("inbox-status", [{"text": "1 inbox in live campaigns is disconnected in Smartlead.", "step": "Reconnect.",
                                  "key": "inbox-status:disconnected", "detail": ["• BettrData · Camp: x@co-one.example"]}])
    head, parts = dp.build([disc, _removed_source()], [], [], NOW, [hold])
    text = "\n".join(head + parts)
    assert "disconnected in Smartlead" not in text and "Left out on purpose" not in text
    assert "BettrData 1 at Zapmail (no inboxes left on the domain, 1 domain): co-one.example 1" in text


def test_warm_up_repair_leaves_removed_inboxes_out_of_its_asks_and_counts():
    login = "Account blocked: authentication failures persisted >= 3 days - update mailbox credentials"
    docs = [{"email": f"{u}@desk.example", "client": "Precise Leads", "kind": "smtp", "started": "2026-11-17",
             "status": "repairing", "blocked": True, "blocked_reason": login, "latest_rep": 88, "rep_at_start": 88,
             "reasons": ["low_rep", "warmup_blocked"]} for u in ("a", "b", "c")]
    today = datetime(2026, 11, 23).date()                  # a Monday: the weekly line too
    asks, done, _ = wr.digest_items(docs, today)
    assert [a.key for a in asks] == ["warmup-repair:login-blocked"] and done
    asks, done, detail = wr.digest_items(docs, today, removed={d["email"] for d in docs})
    assert asks == [] and done == [] and detail == []
