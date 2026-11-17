"""Own provider first, everywhere (17 Nov 2026, after standup): every campaign is ESP-matched, so a Google inbox is
judged on Google seeds and an Outlook inbox on Microsoft seeds. Spam at the other provider is a signal, never a
verdict: it never makes an inbox or its domain spam, FAIL, BUSY, DO NOT USE, pulled, killed or dropped, and wherever
it is shown it is labelled. Each test below has the Google case and its Outlook mirror. Fake domains only."""
from datetime import date

import pytest

import test_v2_weekly as T
import v2_grid
import v2_weekly as wk
from smartlead import kill_rule as kr
from smartlead import own_provider as op
from smartlead import placement_grid as pg
from smartlead import usable
from smartlead import v2_results as v2
from smartlead.placement_schedule import advance
from smartlead.retest_targets import select_targets
from smartlead.sheets import grid_cell_status, merge_grid_entries, own_test_status
from smartlead.smart_delivery import UNJUDGED, own_judgement

SIGNAL = "not its provider: a signal, not a verdict"
# (Google inbox type, its own seeds, the other seeds) and the Outlook mirror
CASES = [("GMAIL", "google", "microsoft"), ("OUTLOOK", "microsoft", "google")]


# --- the shared helper ---------------------------------------------------------------------------------------------

def test_provider_labels_and_own_provider():
    assert {op.provider_key(x) for x in ("G Suite", "Google", "GOOGLE_WORKSPACE", "gmail", "g suite")} == {"google"}
    assert {op.provider_key(x) for x in ("Office365", "Outlook", "MICROSOFT_365", "microsoft")} == {"microsoft"}
    assert op.own_provider("GMAIL") == "google" and op.own_provider("Gmail") == "google"
    assert op.own_provider("OUTLOOK") == "microsoft" and op.own_provider("Outlook") == "microsoft"
    assert op.own_provider("SMTP") is None and op.own_provider("") is None           # judged on both
    assert op.common_kind(["GMAIL", "Gmail"]) == "GMAIL" and op.common_kind(["GMAIL", "OUTLOOK"]) == ""
    assert op.split({"G Suite": 1, "Office365": 2}, "GMAIL") == ({"G Suite": 1}, {"Office365": 2})
    assert op.split({"G Suite": 1, "Office365": 2}, "SMTP") == ({"G Suite": 1, "Office365": 2}, {})
    assert op.signal("Office365", "0/4") == f"Microsoft 0/4 ({SIGNAL})"


# --- v2 (the weekly job): verdict, domain rule, post wording, Mongo doc ------------------------------------------

@pytest.mark.parametrize("kind,own,other", CASES)
def test_v2_spam_at_the_other_provider_never_fails_the_inbox_or_its_domain(tmp_path, kind, own, other):
    """4/4 at its own provider and 0/4 at the other: the inbox passes, nothing is pulled, the domain does not fail,
    and the post labels the other provider's 0/4 a signal."""
    seeds = (4, 0) if own == "google" else (0, 4)          # (Google inbox of 4, Microsoft inbox of 4)
    inboxes = {f"m{n}@fake-{own}.test": seeds for n in range(3)}
    st = T.planned(tmp_path, inboxes, types={e: kind for e in inboxes}, clients={e: "Precise Leads" for e in inboxes})
    for e, rs in st["results"].items():
        assert v2.own_verdict(rs[-1], kind) == v2.PASS
    d = wk.plan(st)
    assert T.pulls(d) == set() and not any(x.get("domain") for x in d.values())
    assert v2.failed_domains({e: v2.own_verdict(rs[-1], kind) for e, rs in st["results"].items()}) == {}
    got = {"ingested": ["a.csv"], "pending": [], "nights": {rs[-1]["night"] for rs in st["results"].values()}}
    head, parts = wk.build_report(st, got, d, None, T.acted(), False, set())
    text = "\n".join(parts)
    assert "3 pass" in head[1] and "0 fail" in head[1]
    assert "fake-" not in text and "domain failed" not in text and "taken out" not in text   # a pass gets no line


@pytest.mark.parametrize("kind,own,other", CASES)
def test_v2_post_labels_the_other_provider_a_signal(tmp_path, kind, own, other):
    """3/4 at its own provider (watch) and 0/4 at the other: the line puts its own provider first and labels the
    other one a signal."""
    seeds = (3, 0) if own == "google" else (0, 3)
    inboxes = {f"m{n}@fake-{own}.test": seeds for n in range(2)}
    st = T.planned(tmp_path, inboxes, types={e: kind for e in inboxes}, clients={e: "Precise Leads" for e in inboxes})
    d = wk.plan(st)
    assert T.pulls(d) == set()
    got = {"ingested": ["a.csv"], "pending": [], "nights": {rs[-1]["night"] for rs in st["results"].values()}}
    head, parts = wk.build_report(st, got, d, None, T.acted(), False, set())
    text = "\n".join(parts)
    assert "2 watch" in head[1] and "0 fail" in head[1]
    assert f"\u2022 watch \u00b7 fake-{own}.test \u00b7 2 of 2 inboxes \u00b7 {op.NAMES[own]} 3/4, {op.NAMES[other]} 0/4 ({SIGNAL})" in text


@pytest.mark.parametrize("kind,own,other", CASES)
def test_v2_the_mirror_still_fails_at_its_own_provider(tmp_path, kind, own, other):
    """0/4 at its own provider is still a fail (the rule is not 'never fail')."""
    seeds = (0, 4) if own == "google" else (4, 0)
    st = T.planned(tmp_path, {f"x@fake-{own}.test": seeds}, types={f"x@fake-{own}.test": kind},
                   clients={f"x@fake-{own}.test": "Precise Leads"})
    assert v2.own_verdict(st["results"][f"x@fake-{own}.test"][-1], kind) == v2.FAIL
    assert T.pulls(wk.plan(st)) == {f"x@fake-{own}.test"}


def test_v2_the_standup_case_google_short_test_and_microsoft_spam_is_not_judged(tmp_path):
    """16 Nov 2026: Melior Google inboxes got 1 Google email (not judged) and 0/4 at Microsoft. Never a fail, and the
    post's line puts Google first and labels the Microsoft 0/4 a signal."""
    st = T.fresh_state(tmp_path)
    st["types"] = {"a@fake-g.test": "GMAIL"}
    pv = v2.provider_verdict
    st["results"]["a@fake-g.test"] = [{"night": "2026-11-09", "verdict": v2.FAIL, "history": False,
                                        "google": pv(["Inbox"]), "microsoft": pv(["Spam"] * 4)}]
    x = st["results"]["a@fake-g.test"][-1]
    assert v2.own_verdict(x, "GMAIL") == v2.INCOMPLETE
    assert T.pulls(wk.plan(st)) == set()
    assert wk._seeds([("a@fake-g.test", x)], "GMAIL") == f"Google 1/1 (only 1 sent), Microsoft 0/4 ({SIGNAL})"


def test_v2_verdict_docs_carry_the_inbox_type(tmp_path):
    st = T.fresh_state(tmp_path)
    st["types"] = {"a@fake-g.test": "GMAIL"}
    st["results"]["a@fake-g.test"] = [T.result(v2.FAIL, 4, 0)]
    st["holds"] = {"2026-11-09": []}
    docs = wk.verdict_docs(st, T.NOW)
    doc = docs["a@fake-g.test|2026-11-09"]
    assert doc["type"] == "GMAIL" and doc["verdict"] == v2.PASS


# --- v2_grid and the grid writer -----------------------------------------------------------------------------------

@pytest.mark.parametrize("kind,own,other", CASES)
def test_v2_grid_labels_the_other_providers_spam_cell(kind, own, other):
    g, m = ((4, 4), (0, 4)) if own == "google" else ((0, 4), (4, 4))
    row = {"email": "a@fake.test", "domain": "fake.test", "client": "Precise Leads", "night": "2026-11-09",
           "verdict": "PASS", "type": kind, "google": {"inbox": g[0], "reported": g[1]},
           "microsoft": {"inbox": m[0], "reported": m[1]}}
    res = v2_grid.night_results([row])[("Precise Leads", "2026-11-09")]["fake.test"]
    other_col = "Office365" if other == "microsoft" else "G Suite"
    own_col = "G Suite" if own == "google" else "Office365"
    assert res[other_col].get("signal") is True
    # a Google domain's own G suite cell is a signal too (t269); an Outlook domain's Outlook cell is a verdict
    assert res[own_col].get("signal") == (op.GOOGLE_SIGNAL_CELL if own == "google" else None)
    grid = [["", "9 Nov 2026 v2", ""], ["Domains", "G suite", "Outlook"], ["fake.test", "", ""]]
    plan = pg.plan_writes(grid, "fake.test", res, "9 Nov 2026 v2", 80.0)
    cells = {c: v for _, c, v in plan.writes}
    assert cells[1 if other == "google" else 2] == op.SIGNAL_CELL
    assert cells[1 if own == "google" else 2] == "Inbox"


def test_v2_grid_writes_a_google_domains_g_suite_spam_as_a_signal_t269():
    """t269 (Manveen, 17 Nov 2026): Google seed results are report-only for Google inboxes, so a Google-only domain's
    G suite spam is written labelled, the reader takes it for no verdict, and the note says where the seeds landed."""
    row = {"email": "a@fake.test", "domain": "fake.test", "client": "Precise Leads", "night": "2026-11-09",
           "verdict": "FAIL", "type": "GMAIL", "google": {"inbox": 0, "reported": 4},
           "microsoft": {"inbox": 4, "reported": 4}}
    res = v2_grid.night_results([row])[("Precise Leads", "2026-11-09")]["fake.test"]
    grid = [["", "9 Nov 2026 v2", ""], ["Domains", "G suite", "Outlook"], ["fake.test", "", ""]]
    cells = {c: v for _, c, v in pg.plan_writes(grid, "fake.test", res, "9 Nov 2026 v2", 80.0).writes}
    assert cells == {1: op.GOOGLE_SIGNAL_CELL, 2: "Inbox"}
    assert grid_cell_status(op.GOOGLE_SIGNAL_CELL) == "" and grid_cell_status(op.SIGNAL_CELL) == "fail"
    status, _, note = own_test_status(entry(op.GOOGLE_SIGNAL_CELL, ""), "GMAIL")
    assert status == "Unknown" and note == "Google seeds in spam on 2026-11-09 (Google seed result: signal only)"
    # an Outlook domain's own Outlook spam stays a verdict, and the classic report's cells follow the same rule
    assert op.grid_signal("Office365", "OUTLOOK") is None and op.grid_signal("G Suite", "OUTLOOK") is True
    assert op.grid_signal("G Suite", "Google") == op.GOOGLE_SIGNAL_CELL and op.grid_signal("G Suite", "SMTP") is None


def test_v2_grid_keeps_plain_spam_for_mixed_or_unknown_types():
    rows = [{"email": f"{k}@fake.test", "domain": "fake.test", "client": "Precise Leads", "night": "2026-11-09",
             "verdict": "PASS", "type": t, "google": {"inbox": 4, "reported": 4}, "microsoft": {"inbox": 0, "reported": 4}}
            for k, t in (("a", "GMAIL"), ("b", "SMTP"))]
    res = v2_grid.night_results(rows)[("Precise Leads", "2026-11-09")]["fake.test"]
    assert not any(v.get("signal") for v in res.values())


# --- the grid reader -> All Inboxes Test Status / Busy Reason / Notes ---------------------------------------------

def entry(gs, ol, gs_date="2026-11-09", ol_date="2026-11-09"):
    return {"status": "fail" if "spam" in (gs + ol).lower() else "inbox", "date": max(gs_date, ol_date),
            "gsuite": gs.lower(), "gsuite_date": gs_date, "outlook": ol.lower(), "outlook_date": ol_date}


def test_grid_google_inbox_is_judged_on_the_g_suite_column_only():
    status, when, note = own_test_status(entry("Inbox", "spam"), "GMAIL")
    assert (status, when) == ("inbox", "2026-11-09") and note == f"Microsoft seeds in spam on 2026-11-09 ({SIGNAL})"
    assert own_test_status(entry("spam", "Inbox"), "GMAIL")[0] == "fail"


def test_grid_outlook_inbox_is_judged_on_the_outlook_column_only():
    status, _, note = own_test_status(entry("spam", "Inbox"), "OUTLOOK")
    assert status == "inbox" and note.startswith("Google seeds in spam") and SIGNAL in note
    assert own_test_status(entry("Inbox", "spam"), "OUTLOOK")[0] == "fail"


def test_grid_other_column_only_spam_is_untested_not_failed_and_smtp_keeps_both():
    assert own_test_status(entry("", "spam"), "GMAIL")[:2] == ("Unknown", "")
    assert own_test_status(entry("", "Inbox"), "GMAIL")[0] == "inbox"        # a pass still counts, as before
    assert own_test_status(entry("Inbox", "spam"), "SMTP") == ("fail", "2026-11-09", "")
    assert own_test_status({"status": "fail", "date": "2026-11-09"}, "GMAIL") == ("fail", "2026-11-09", "")   # API row
    assert own_test_status(entry("Inbox", op.SIGNAL_CELL), "GMAIL")[0] == "inbox"
    assert grid_cell_status(op.SIGNAL_CELL) == "fail"        # it is spam; only the inbox's own column decides


def test_the_signal_goes_to_the_notes_column_not_the_busy_reasons():
    item = {"email": "a@fake.test", "tags": [], "type": "GMAIL", "warmup_rep_pct": "95%",
            "test_signal": own_test_status(entry("Inbox", "spam"), "GMAIL")[2]}
    reasons = usable.apply(item, {"checked": False}, date(2026, 11, 17), block_listed=False, require_v2_pass=False)
    assert reasons == [] and "Microsoft seeds in spam" in item["use_notes"]


def test_two_tabs_merge_per_column():
    a = entry("Inbox", "", ol_date="")
    b = entry("", "spam", gs_date="")
    m = merge_grid_entries(a, b)
    assert (m["gsuite"], m["outlook"], m["status"]) == ("inbox", "spam", "fail")
    assert own_test_status(m, "GMAIL")[0] == "inbox" and own_test_status(m, "OUTLOOK")[0] == "fail"
    newer = merge_grid_entries(entry("Inbox", "Inbox", "2026-11-01", "2026-11-01"), entry("spam", "Inbox"))
    assert newer["gsuite"] == "spam"                          # a failing cell wins, as the status merge did


# --- the classic kill rule (kill warnings, fleet watch "kill" case, renewal note) ---------------------------------

def ctest(day, kind, g, o):
    """A classic test: g/o = (seeds, spam) at Google / Outlook."""
    return {"date": day, "seeds": g[0] + o[0], "spam": g[1] + o[1], "kind": kind,
            "by": {"Google": list(g), "Outlook": list(o)}}


@pytest.mark.parametrize("kind,own,other", CASES)
def test_kill_rule_grades_on_the_own_provider(kind, own, other):
    clean, dirty = (10, 0), (10, 10)
    g, o = (clean, dirty) if own == "google" else (dirty, clean)
    tests = [ctest("2026-10-12", kind, g, o), ctest("2026-10-05", kind, g, o)]
    if kind == "GMAIL":       # t268/t273: a Google inbox's seed tests never count, so never GOOD/WATCH/KILL either
        assert kr.grade(tests[0]) is None
        assert {x["verdict"] for x in kr.judge_all({"a@fake.test": tests}, date(2026, 10, 13)).values()} == {kr.NO_DATA}
        return
    assert kr.grade(tests[0]) == 100.0
    v = kr.judge_all({"a@fake.test": tests, "b@fake.test": tests}, date(2026, 10, 13))
    assert {x["verdict"] for x in v.values()} == {kr.GOOD}
    assert SIGNAL in v["a@fake.test"]["why"] and "0% inbox" in v["a@fake.test"]["why"]
    dead = [ctest("2026-10-12", kind, dirty if own == "google" else clean, clean if own == "google" else dirty)] * 2
    assert kr.judge_all({"a@fake.test": dead}, date(2026, 10, 13))["a@fake.test"]["verdict"] == kr.KILL


def test_a_google_inbox_is_never_on_the_kill_list_or_swapped_out_t273():
    """Manveen 17 Nov 2026: "make sure this does not bite us". Google seed results (own provider or not, the inbox's
    or its domain's) never make a Google inbox SHOULD KILL / WATCH, and never make it a rotation victim."""
    from smartlead.rotation_planner import _is_victim
    dirty = (10, 10)
    g_dead = [ctest("2026-10-12", "GMAIL", dirty, dirty), ctest("2026-10-05", "GMAIL", dirty, dirty)]
    s_dead = [ctest("2026-10-12", "SMTP", dirty, dirty)] * 3
    v = kr.judge_all({"g@fake.test": g_dead, "s1@fake.test": s_dead, "s2@fake.test": s_dead}, date(2026, 10, 13))
    assert v["g@fake.test"] == {"email": "g@fake.test", "verdict": kr.NO_DATA, "rule": "", "why": kr.GOOGLE_WHY}
    assert v["s1@fake.test"]["verdict"] == kr.KILL                    # an SMTP inbox is still judged as before
    live = {"campaign_status": "ACTIVE", "test_sheet_status": "fail", "busy_reason": "failed_test"}
    assert not _is_victim({**live, "provider": "Gmail"}) and not _is_victim({**live, "type": "GMAIL"})
    assert _is_victim({**live, "provider": "Outlook"}) and _is_victim(live)


def test_kill_rule_unknown_type_keeps_the_worse_provider():
    t = ctest("2026-10-12", "", (10, 0), (10, 10))
    assert kr.grade(t) == 0.0


# --- the classic executors -----------------------------------------------------------------------------------------

REPORT = {"inbox_pct": 50.0, "worst_provider_inbox_pct": 0.0,
          "by_provider": {"G Suite": {"inbox_pct": 100.0, "total": 10}, "Office365": {"inbox_pct": 0.0, "total": 10}}}


def test_classic_single_sender_is_judged_on_its_own_provider():
    g = own_judgement(REPORT, ["GMAIL"], 80.0)
    assert g["status"] == "inbox" and g["pct"] == 100.0 and SIGNAL in g["note"]
    o = own_judgement(REPORT, ["OUTLOOK"], 80.0)                 # the mirror: its own seeds were in spam
    assert o["status"] == "fail" and o["pct"] == 0.0 and o["note"] == ""      # Google was fine: no signal
    assert own_judgement(REPORT, [""], 80.0)["status"] == "fail"           # unknown type: worst provider, as before
    assert own_judgement({"by_provider": {"Office365": {"inbox_pct": 0.0, "total": 4}}}, ["GMAIL"], 80.0)["status"] == UNJUDGED


def test_classic_multi_sender_blend_cannot_fail_on_the_other_provider():
    sender = {"inbox_pct": 50.0}
    assert own_judgement(REPORT, ["GMAIL"], 80.0, sender=sender)["status"] == UNJUDGED
    assert own_judgement(REPORT, ["OUTLOOK"], 80.0, sender=sender)["status"] == "fail"
    assert own_judgement(REPORT, ["GMAIL"], 80.0, sender={"inbox_pct": 90.0})["status"] == "inbox"


def test_unjudged_is_not_a_strike_and_retest_targets_carry_the_type():
    rec = advance({"consecutive_fails": 2}, "a@fake.test", "2026-11-17", UNJUDGED, {})
    assert rec["consecutive_fails"] == 2
    rows = [{"top_problem": "Warm-up mail landing in spam", "email": "a@fake.test", "client": "PL",
             "provider": "Gmail", "campaign_name": "c", "test_date": ""}]
    assert select_targets(rows, 5, set())[0]["provider"] == "Gmail"


# --- the classic weekly report: the Google mirror of test_placement_report's Outlook case --------------------------

def test_classic_report_google_sender_in_spam_at_outlook_is_not_flagged():
    from smartlead.placement_report import build_report, format_slack, inbox_rows
    prov = lambda d: "Google" if d.startswith("g") else "Outlook"            # noqa: E731
    senders = {"a@fake-g.test": [{"seed": "x@g.test", "folder": "Inbox"}, {"seed": "y@g.test", "folder": "Inbox"},
                                 {"seed": "z@o.test", "folder": "Spam"}, {"seed": "w@o.test", "folder": "Spam"}]}
    rep = build_report(senders, {"a@fake-g.test": "GMAIL"}, prov)
    a = rep["inboxes"]["a@fake-g.test"]
    assert a["status"] == "good" and rep["domains"]["fake-g.test"]["verdict"] == "good"
    assert rep["domains"]["fake-g.test"]["own"] == "Google"
    head, parts = format_slack("X", rep, ["t"], "2026-11-17")
    assert "no mailbox flagged" in head[0]
    assert head[-1] == "Other provider, a signal only (campaigns are ESP-matched): Google→Outlook 100% spam"
    assert f"Google → Outlook: 2 of 2 in spam (100%) ({SIGNAL})" in "\n".join(parts)
    assert inbox_rows(rep)[0][9] == "YES"                      # Usable
