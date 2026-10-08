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
    assert txt.index("Sending from the main domain") < txt.index("No signature") and "*1 P0*" in txt
    assert "All checks passed" in ia.summarize([], {})


def test_inbox_job_refuses_main_domain():
    import pytest
    from smartlead.inbox_jobs import new_job
    with pytest.raises(ValueError, match="real website"):
        new_job(client="Precise Leads", kind="owned", provider="GOOGLE", domains=["preciseleads.in"])
    assert new_job(client="Precise Leads", kind="owned", provider="GOOGLE", domains=["pipelinecalendar.com"])["domains"]
