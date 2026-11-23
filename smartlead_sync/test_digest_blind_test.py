"""The 7-day blind test of the digest (digest_blind_test.py, t409): its pure parts, no Slack, Mongo or Claude."""
import random
import subprocess

import digest_blind_test as bt

FACTS = {"day": "Mon 23 Nov", "n": 2, "items": [{"s": "3", "f": {"client": "BettrData", "k": 8, "provider": "Zapmail"}}],
         "watch": [], "thread": [],
         "text": "Inbox digest · Mon 23 Nov · 1 thing to do\n\n1. BettrData: delete 8 inboxes in Smartlead that Zapmail "
                 "already removed (list in thread).\n\n--- thread ---\n1 above:\n• x"}


def test_the_dm_shows_both_posts_unlabelled_and_keeps_which_is_ai():
    text, ai_is = bt.build_dm(FACTS, "AI POST", [], 3, rng=random.Random(1))
    assert text.startswith("Inbox digest blind test, day 3 of 7 (Mon 23 Nov).")
    assert "Version 1:" in text and "Version 2:" in text and text.endswith(bt.ASK)
    assert "AI" not in text.replace("AI POST", "") and "template" not in text.lower()
    tpl = bt.template_post(FACTS)
    assert "--- thread ---" not in tpl and tpl.endswith("(list in thread).")
    first = text.split("Version 1:\n\n")[1].split("\n\n----------")[0]
    assert (first == "AI POST") == (ai_is == 1) and (first == tpl) == (ai_is == 2)


def test_both_orders_happen():
    seen = {bt.build_dm(FACTS, "AI", [], 1, rng=random.Random(s))[1] for s in range(20)}
    assert seen == {1, 2}


def test_a_failed_fact_check_is_said_in_the_dm():
    text, ai_is = bt.build_dm(FACTS, "AI", ["number not in the facts: 77"], 1, rng=random.Random(0))
    assert f"Note: Version {ai_is} failed the fact check (number not in the facts: 77)." in text


def test_his_answer_is_the_first_1_or_2_after_the_dm():
    msgs = [{"user": "BOT", "ts": "10.0", "text": "Version 1..."}, {"user": "U1", "ts": "9.0", "text": "2"},
            {"user": "U1", "ts": "11.0", "text": "hmm"}, {"user": "U1", "ts": "12.0", "text": "Version 2."},
            {"user": "U1", "ts": "13.0", "text": "1"}]
    assert bt.answer_of(msgs, "U1", "10.0") == "2"
    assert bt.answer_of(msgs[:3], "U1", "10.0") is None


def test_the_tally_and_the_4_of_7_rule():
    docs = [{"winner": "ai"}] * 4 + [{"winner": "template"}] * 3 + [{"winner": None}]
    t = bt.tally(docs)
    assert t == {"answered": 7, "ai": 4, "template": 3, "ai_wins": True}
    assert bt.summary_text(t).startswith("Inbox digest blind test: done. AI wins: template 3, AI 4.")
    t = bt.tally([{"winner": "ai"}] * 3 + [{"winner": "template"}] * 4)
    assert not t["ai_wins"] and "so the approved sentences stay" in bt.summary_text(t)


def test_the_prompt_carries_the_facts_and_the_rules_and_claude_errors_are_reported():
    p = bt.prompt(FACTS)
    assert '"client": "BettrData"' in p and "Never invent" in p and "at most 5 numbered" in p.replace("\n", " ")
    assert "--- thread ---" not in p                              # the template's own text is not shown to the AI

    def boom(*a, **k):
        raise subprocess.TimeoutExpired("claude", 1)
    assert bt.ai_text(FACTS, run=boom) == ("", f"claude -p did not answer in {bt.CLAUDE_TIMEOUT_S} s")

    def ok(cmd, **k):
        assert cmd[:2] == ["claude", "-p"] and "--model" in cmd and k["input"].startswith("You write one Slack message")
        return subprocess.CompletedProcess(cmd, 0, stdout="  the post  \n", stderr="")
    assert bt.ai_text(FACTS, run=ok) == ("the post", "")


def test_claude_runs_on_windows_or_in_wsl_with_the_long_lived_login(monkeypatch):
    assert bt.claude_cmd() == (["claude", "-p", "--model", bt.MODEL, "--output-format", "text"], None)
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "x")
    monkeypatch.delenv("WSLENV", raising=False)
    cmd, env = bt.claude_cmd("wsl")
    assert cmd[:5] == ["wsl.exe", "-d", "Ubuntu", "--", "bash"] and cmd[-1].startswith("claude -p")
    assert env["WSLENV"] == "CLAUDE_CODE_OAUTH_TOKEN/u"
