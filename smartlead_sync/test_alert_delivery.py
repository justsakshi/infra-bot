"""Alerts reach a person (audit 7 Nov 2026): the infra channel is the default, a refused channel is rerouted
loudly, keys never appear in Slack, reply alerts stay off until their rule is fixed. No network."""
import importlib

import pytest

from smartlead import notify


class FakeSlack:
    def __init__(self, dead=()):
        self.dead, self.calls = set(dead), []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(dict(json))
        ok = json["channel"] not in self.dead
        body = {"ok": ok, "ts": f"{len(self.calls)}.0"} if ok else {"ok": False, "error": "is_archived"}
        return type("R", (), {"json": lambda self_: body})()


@pytest.fixture
def slack(monkeypatch):
    def install(dead=()):
        fake = FakeSlack(dead)
        monkeypatch.setattr(notify.requests, "post", fake.post)
        monkeypatch.setattr(notify.time, "sleep", lambda s: None)
        notify._REROUTED.clear()
        return fake
    return install


def test_health_channel_defaults_to_the_infra_channel_never_the_archived_one(monkeypatch):
    import smartlead.config as config
    monkeypatch.setenv("HEALTH_NOTIFY_CHANNEL", "")
    monkeypatch.setenv("REMINDER_CHANNEL", "CARCHIVED")
    try:
        assert importlib.reload(config).HEALTH_NOTIFY_CHANNEL == "C0AGVSUNEFP"
        monkeypatch.setenv("HEALTH_NOTIFY_CHANNEL", "CSET")
        assert importlib.reload(config).HEALTH_NOTIFY_CHANNEL == "CSET"
    finally:
        monkeypatch.delenv("HEALTH_NOTIFY_CHANNEL", raising=False)
        importlib.reload(config)


def test_infra_channel_takes_the_first_env_var_set(monkeypatch):
    monkeypatch.delenv("RENEWAL_REVIEW_CHANNEL", raising=False)
    monkeypatch.setenv("ZAPMAIL_NOTIFY_CHANNEL", " CZAP ")
    assert notify.infra_channel("RENEWAL_REVIEW_CHANNEL", "ZAPMAIL_NOTIFY_CHANNEL") == "CZAP"
    monkeypatch.delenv("ZAPMAIL_NOTIFY_CHANNEL")
    assert notify.infra_channel("RENEWAL_REVIEW_CHANNEL", "ZAPMAIL_NOTIFY_CHANNEL") == "C0AGVSUNEFP"


def test_a_refused_channel_is_rerouted_to_the_infra_channel_with_a_note(slack):
    fake = slack(dead={"CDEAD"})
    ts = notify.post_with_thread("tok", "CDEAD", ["Blocklists, 9 Nov", "3 domains listed"], ["detail line"])
    assert ts
    head = [c for c in fake.calls if c["channel"] == "C0AGVSUNEFP" and "thread_ts" not in c]
    assert len(head) == 1 and "meant for <#CDEAD>" in head[0]["text"] and "is_archived" in head[0]["text"]
    assert head[0]["text"].split("\n")[0].endswith("Blocklists, 9 Nov*")       # title stays first
    reply = [c for c in fake.calls if "thread_ts" in c]
    assert reply and all(c["channel"] == "C0AGVSUNEFP" for c in reply)          # replies follow the headline


def test_a_working_channel_is_used_as_it_is(slack):
    fake = slack()
    assert notify._post("tok", "CGOOD", "hello")
    assert [c["channel"] for c in fake.calls] == ["CGOOD"]


def test_key_alert_never_shows_any_part_of_a_key():
    import key_health_monitor as khm
    head, thread = khm._alert_parts([{"client": "PRECISE_LEADS", "key_tail": "ab12"}], [])
    text = "\n".join(head + thread)
    assert "ab12" not in text and "plan is paid" in text


def test_reply_alerts_have_an_off_switch():
    src = open("reply_monitor.py", encoding="utf-8").read()
    assert 'os.getenv("REPLY_MONITOR_POST", "").strip().lower() == "off"' in src


def test_fleet_watch_posts_to_the_infra_channel_through_notify(monkeypatch, slack):
    import fleet_watch
    fake = slack()
    monkeypatch.setenv("SLACK_BOT_TOKEN", "tok")
    monkeypatch.delenv("FLEET_WATCH_CHANNEL", raising=False)
    assert fleet_watch._post("Fleet watch, 9 Nov\n2 new problems")
    assert fake.calls[0]["channel"] == "C0AGVSUNEFP"
