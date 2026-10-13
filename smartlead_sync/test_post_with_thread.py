"""Headline in the channel, details in the thread (spec 2026-10-13). No network."""
from smartlead import notify


def _fake(monkeypatch, fail_parent=False, fail_reply=False):
    sent = []
    monkeypatch.setattr(notify, "_REPLY_PAUSE_S", 0)

    def fake(token, channel, text, thread_ts=None):
        sent.append((text, thread_ts))
        if thread_ts is None:
            return None if fail_parent else "111.1"
        return None if fail_reply else f"222.{len(sent)}"
    monkeypatch.setattr(notify, "_post", fake)
    return sent


def test_parent_first_then_replies_in_its_thread(monkeypatch):
    sent = _fake(monkeypatch)
    ts = notify.post_with_thread("t", "C1", ["⚠️ Kill warnings, 13 Oct", "Bettrdata 13"], ["*Bettrdata*\nx", "rule"])
    assert ts == "111.1"
    assert sent[0] == ("⚠️ Kill warnings, 13 Oct\nBettrdata 13", None)
    assert [s[1] for s in sent[1:]] == ["111.1", "111.1"] and sent[1][0] == "*Bettrdata*\nx"


def test_long_parts_split_at_40_lines(monkeypatch):
    sent = _fake(monkeypatch)
    notify.post_with_thread("t", "C1", ["h"], ["\n".join(f"line {i}" for i in range(95))])
    assert [len(s[0].split("\n")) for s in sent[1:]] == [40, 40, 15]


def test_parent_failure_posts_nothing_else(monkeypatch):
    sent = _fake(monkeypatch, fail_parent=True)
    assert notify.post_with_thread("t", "C1", ["h"], ["a", "b"]) is None
    assert len(sent) == 1


def test_reply_failure_keeps_the_parent(monkeypatch):
    sent = _fake(monkeypatch, fail_reply=True)
    assert notify.post_with_thread("t", "C1", ["h"], ["a", "b"]) == "111.1"
    assert len(sent) == 5                     # each failed reply is tried twice


def test_a_rate_limited_reply_is_retried_once(monkeypatch):
    monkeypatch.setattr(notify, "_REPLY_PAUSE_S", 0)
    sent, calls = [], {"n": 0}

    def fake(token, channel, text, thread_ts=None):
        sent.append((text, thread_ts))
        if thread_ts is None:
            return "111.1"
        calls["n"] += 1
        return None if calls["n"] == 1 else "222.2"      # first reply attempt fails
    monkeypatch.setattr(notify, "_post", fake)
    notify.post_with_thread("t", "C1", ["h"], ["only"])
    assert [s[0] for s in sent] == ["h", "only", "only"]


def test_empty_parts_are_skipped(monkeypatch):
    sent = _fake(monkeypatch)
    notify.post_with_thread("t", "C1", ["h"], ["", "  ", "real"])
    assert [s[0] for s in sent[1:]] == ["real"]


def test_keys_and_error_pages_never_reach_slack(monkeypatch):
    bodies = []

    class Resp:
        def json(self):
            return {"ok": True, "ts": "1.1"}

    monkeypatch.setattr(notify.requests, "post", lambda url, headers, json, timeout: bodies.append(json) or Resp())
    notify._post("t", "C1", "failed: 'https://server.smartlead.ai/api/v1/x?api_key=SECRET123&limit=1' "
                            "then 502 on /organizations: <html><head><title>502</title></head><body>nginx</body></html> "
                            "and Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abc")
    text = bodies[0]["text"]
    assert "SECRET123" not in text and "eyJhbGci" not in text and "nginx" not in text and "<html" not in text
    assert "502" in text and "error page" in text
