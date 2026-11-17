"""Infra audit rules (no network).   python3 -m pytest test_infra_audit.py -q"""
from datetime import datetime, timedelta, timezone

from smartlead import infra_audit as ia

NOW = datetime(2026, 10, 8, tzinfo=timezone.utc)


def _box(email, kind="GMAIL", mpd=15, warm_days=40, warm="ACTIVE", wmax=10, sig="<div>Jo</div>", name="Jo Doe"):
    return {"from_email": email, "type": kind, "message_per_day": mpd, "signature": sig, "from_name": name,
            "warmup_details": {"status": warm, "max_email_per_day": wmax,
                               "warmup_created_at": (NOW - timedelta(days=warm_days)).isoformat()}}


def checks(fs):
    return sorted(f["check"] for f in fs)


def test_dns_unknown_is_not_missing():
    assert ia.check_dns("a.com", "X", None, None) == []
    assert checks(ia.check_dns("a.com", "X", [], ["v=DMARC1; p=none"])) == ["dns_dmarc_policy", "dns_mx"]
    assert ia.check_dns("a.com", "X", ["mx"], ["v=DMARC1; p=reject"]) == []


def test_ns_cluster_flags_cloudflare_harder():
    ns = {f"d{i}.com": ["lola.ns.cloudflare.com.", "x.ns.cloudflare.com."] for i in range(13)}
    ns.update({f"z{i}.com": ["ns-a.google."] for i in range(5)})
    out = ia.check_ns_clusters(ns, {d: "Precise Leads" for d in ns})
    assert len(out) == 1 and out[0]["severity"] == "P1" and "Cloudflare" in out[0]["detail"]


def test_redirect_rules():
    m = "preciseleads.in"
    assert ia.check_redirect("a.com", "PL", m, {"final_url": "https://www.preciseleads.in/", "status": 200}) == []
    assert ia.check_redirect("a.com", "PL", m, {"final_url": "http://a.com/", "status": 200, "page": "Precise Leads home"}) == []
    assert ia.check_redirect("a.com", "PL", m, {"final_url": "http://a.com/", "status": 200, "page": "Domain parked"})[0]["severity"] == "P2"
    assert "redirects to" in ia.check_redirect("a.com", "PL", m, {"final_url": "https://other.com", "status": 200})[0]["detail"]
    assert ia.check_redirect("a.com", "PL", m, {"error": "ConnectError"})[0]["severity"] == "P1"


def test_main_domain_is_p0():
    assert ia.check_main_domain("belardiwong.com", "BW", "belardiwong.com")[0]["severity"] == "P0"
    assert ia.check_main_domain("findbelardiwong.com", "BW", "belardiwong.com") == []


def test_mailbox_rules():
    boxes = [_box(f"u{i}@a.com", mpd=30) for i in range(4)]                     # 4 Gmail, 120/day
    boxes += [_box("new@b.com", warm_days=10), _box("off@c.com", warm="PAUSED"),
              _box("low@d.com", mpd=20, wmax=5), _box("nosig@e.com", sig=""), _box("idle@f.com", warm_days=3)]
    active = {e: ["Camp"] for e in ("new@b.com", "off@c.com", "low@d.com", "nosig@e.com")}
    out = ia.check_mailboxes(boxes, {b["from_email"]: "X" for b in boxes}, active, NOW)
    got = checks(out)
    assert got.count("mailboxes_per_domain") == 1 and got.count("sends_per_domain") == 1
    assert got.count("sends_per_mailbox") == 4                       # 30 > 25 on the four a.com boxes
    assert "young_inbox_in_campaign" in got and "warmup_off" in got
    assert "warmup_below_cold" in got and "signature_missing" in got
    assert not [f for f in out if f["subject"] == "idle@f.com"]      # young but not sending: fine


def test_outlook_domain_not_capped_by_count():
    boxes = [_box(f"u{i}@o.com", kind="OUTLOOK", mpd=5) for i in range(25)]
    assert ia.check_mailboxes(boxes, {}, {}, NOW) == []


def test_provider_mix_and_esp():
    boxes = [_box(f"u{i}@a{i}.com") for i in range(12)]
    assert checks(ia.check_provider_mix(boxes, {b["from_email"]: "PL" for b in boxes})) == ["provider_mix"]
    assert ia.check_provider_mix(boxes[:5], {}) == []
    assert checks(ia.check_esp_matching([{"id": 1, "enable_ai_esp_matching": False}, {"id": 2, "enable_ai_esp_matching": True}], {})) == ["esp_matching"]


def test_smtp_ip_listing_is_p0():
    out = ia.check_smtp_ips({"smtp.x.com": {"1.2.3.4": ["Spamhaus ZEN"], "1.2.3.5": []}}, {"smtp.x.com": "BW"})
    assert len(out) == 1 and out[0]["severity"] == "P0"


def test_summary_worst_first():
    fs = [ia.finding("signature_missing", "P2", "A", "x@a.com", "no signature", "fix"),
          ia.finding("main_domain", "P0", "B", "b.com", "main", "fix")]
    txt = ia.summarize(fs, {"domains": 2, "inboxes": 2})
    assert txt.index("Sending from the main domain") < txt.index("No signature") and "P0 1" in txt
    assert "no problems" in ia.summarize([], {})


def test_inbox_job_refuses_main_domain():
    import pytest
    from smartlead.inbox_jobs import new_job
    with pytest.raises(ValueError, match="real website"):
        new_job(client="Precise Leads", kind="owned", provider="GOOGLE", domains=["preciseleads.in"])
    assert new_job(client="Precise Leads", kind="owned", provider="GOOGLE", domains=["pipelinecalendar.com"])["domains"]


def test_digest_billing_lines_flag_failed_payment():
    from datetime import date
    from zapmail_digest import billing_lines
    rows = [{"bills_on": "2026-10-08", "mailboxes": 15, "provider": "MICROSOFT", "kind": "inboxes", "price": 48.75,
             "clients": ["Bettrdata"], "payment_failure": "No payment method on file"},
            {"bills_on": "2026-10-10", "mailboxes": 21, "provider": "GOOGLE", "kind": "inboxes", "price": 68.25, "clients": ["Belardi Wong"]},
            {"bills_on": "2026-11-08", "mailboxes": 21, "provider": "GOOGLE", "kind": "inboxes", "price": 68.25, "clients": []}]
    lines = billing_lines(rows, today=date(2026, 10, 8))
    assert len(lines) == 2 and "payment failed" in lines[0] and "Outlook" in lines[0] and "Google" in lines[1]


def test_young_google_inbox_counts_from_its_own_days():
    from datetime import datetime, timezone
    from smartlead import infra_audit as ia
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    g = {"from_email": "a@g.com", "type": "GMAIL", "message_per_day": 15, "tags": [],
         "warmup_details": {"status": "ACTIVE", "max_email_per_day": 25, "warmup_created_at": "2026-10-13T10:00:00Z"}}
    o = {**g, "from_email": "b@o.com", "type": "OUTLOOK", "message_per_day": 10,
         "warmup_details": {**g["warmup_details"], "max_email_per_day": 20}}
    found = ia.check_mailboxes([g, o], {"a@g.com": "Melior", "b@o.com": "Melior"},
                               {"a@g.com": ["Exec"], "b@o.com": ["Exec"]}, now)
    young = [f for f in found if f["check"] == "young_inbox_in_campaign"]
    assert [f["subject"] for f in young] == ["b@o.com"]           # Google ready at 14 days, Outlook not until 21


def test_setup_drift_only_for_set_up_inboxes():
    from datetime import datetime, timezone
    from smartlead import infra_audit as ia
    now = datetime(2026, 10, 30, tzinfo=timezone.utc)
    base = {"type": "GMAIL", "message_per_day": 20, "custom_tracking_domain": None,
            "warmup_details": {"status": "ACTIVE", "max_email_per_day": 25, "reply_rate": 30,
                               "warmup_created_at": "2026-09-01T00:00:00Z"}}
    done = {**base, "from_email": "a@x.com", "tags": [{"tag_name": "Bot-Setup"}]}
    other = {**base, "from_email": "b@x.com", "tags": []}
    found = ia.check_mailboxes([done, other], {"a@x.com": "Melior", "b@x.com": "Melior"}, {}, now)
    drift = [f for f in found if f["check"] == "setup_drift"]
    assert [f["subject"] for f in drift] == ["a@x.com"] and "daily limit 20, want 15" in drift[0]["detail"]


# 17 Nov 2026: the 10:20 audit exited 1 on one Smartlead 429 ("200 requests in 1 min") while reading a campaign's
# inboxes; the 10:00 sync was sharing the same account limit. A 429 is waited out, not the end of the run.
class _Resp:
    def __init__(self, status, body=None, retry_after=None):
        self.status_code, self._body, self.text = status, body, "" if status == 200 else "rate limit"
        self.headers = {"Retry-After": retry_after} if retry_after else {}
        self.reason_phrase = "x"
        self.request = __import__("httpx").Request("GET", "https://server.smartlead.ai/api/v1/campaigns?api_key=SECRET")

    def json(self):
        return self._body


def _fake_get(monkeypatch, statuses):
    import smartlead.api as api
    seq = list(statuses)
    calls = []

    def get(url, params=None, headers=None, timeout=None):
        calls.append(url)
        s = seq.pop(0)
        return _Resp(s[0], s[1] if len(s) > 1 else None, s[2] if len(s) > 2 else None) if isinstance(s, tuple) else _Resp(s)
    monkeypatch.setattr(api.httpx, "get", get)
    return calls


def test_get_json_waits_out_429_then_returns(monkeypatch):
    from smartlead.api import get_json
    calls = _fake_get(monkeypatch, [429, (429, None, "30"), 502, (200, [{"id": 1}])])
    waits = []
    assert get_json("https://x/campaigns", "SECRET", sleep=waits.append) == [{"id": 1}]
    assert len(calls) == 4 and len(waits) == 3
    assert 30 <= waits[1] <= 45                   # Retry-After honoured (plus a little jitter)
    assert all(w > 0 for w in waits)


def test_get_json_gives_up_without_the_key_in_the_error(monkeypatch):
    import httpx
    import pytest
    from smartlead.api import get_json
    _fake_get(monkeypatch, [429] * 10)
    waits = []
    with pytest.raises(httpx.HTTPStatusError) as e:
        get_json("https://x/campaigns", "SECRET", tries=3, sleep=waits.append)
    assert len(waits) == 2 and "SECRET" not in str(e.value) and "429" in str(e.value)


def test_get_json_does_not_retry_a_client_error(monkeypatch):
    import httpx
    import pytest
    from smartlead.api import get_json
    calls = _fake_get(monkeypatch, [404, 200])
    with pytest.raises(httpx.HTTPStatusError):
        get_json("https://x/campaigns", "SECRET", sleep=lambda s: None)
    assert len(calls) == 1


def test_audit_and_attachment_reads_use_the_retrying_get(monkeypatch):
    import infra_audit
    import placement_report
    calls = _fake_get(monkeypatch, [429, (200, [])])
    monkeypatch.setattr("smartlead.api.time.sleep", lambda s: None)
    assert infra_audit._get("/campaigns", "SECRET") == []
    calls.clear()
    _fake_get(monkeypatch, [429, (200, [])])
    assert placement_report._get("https://x/campaigns", "SECRET") == []
