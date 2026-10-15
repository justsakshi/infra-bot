"""The team's setup standard per inbox type (spec 2026-10-13 inbox intake). No network."""
from datetime import date

from smartlead import inbox_standard as st


def acct(**kw):
    a = {"id": 1, "from_email": "a@x.com", "type": "GMAIL", "smtp_host": None, "message_per_day": 15, "minTimeToWaitInMins": 20,
         "custom_tracking_domain": None, "signature": "<div>A</div>", "client_id": None,
         "warmup_details": {"status": "ACTIVE", "max_email_per_day": 25, "reply_rate": 30,
                            "warmup_created_at": "2026-10-13T10:49:25.235Z", "warmup_reputation": "100%"}}
    a.update(kw)
    return a


def test_kind_from_type_host_and_density():
    assert st.inbox_kind(acct(type="GMAIL"), 3) == "google"
    assert st.inbox_kind(acct(type="SMTP", smtp_host="smtp.gmail.com"), 3) == "google"      # Zapmail Google added by SMTP
    assert st.inbox_kind(acct(type="OUTLOOK"), 3) == "outlook"
    assert st.inbox_kind(acct(type="OUTLOOK"), 25) == "outlook_dense"
    assert st.inbox_kind(acct(type="SMTP", smtp_host="smtp.office365.com"), 25) == "outlook_dense"
    assert st.inbox_kind(acct(type="SMTP", smtp_host="mail.accountsworks.com"), 4) == "smtp"


def test_standard_values_match_the_spec():
    g, o, d, s = (st.standard_for(k) for k in ("google", "outlook", "outlook_dense", "smtp"))
    assert (g["max_email_per_day"], g["total_warmup_per_day"], g["daily_rampup"], g["reply_rate"], g["ready_after_days"]) == (15, 25, 5, 30, 14)
    assert (o["max_email_per_day"], o["total_warmup_per_day"], o["daily_rampup"], o["reply_rate"], o["ready_after_days"]) == (5, 20, 5, 30, 21)  # ScaledMail rule, 15 Oct
    assert (d["max_email_per_day"], d["total_warmup_per_day"], d["daily_rampup"], d["reply_rate"], d["ready_after_days"]) == (5, 5, 0, 30, 21)
    assert (s["max_email_per_day"], s["total_warmup_per_day"], s["daily_rampup"], s["reply_rate"], s["ready_after_days"]) == (7, 20, 5, 30, 21)  # Smartlead support 15 Oct: ~3 weeks (weekdays-only warmup)
    assert all(st.standard_for(k)["weekdays_only"] and st.standard_for(k)["auto_adjust"] for k in ("google", "smtp"))


def test_account_fields_and_warmup_args():
    assert st.account_fields("google", "scaledmail") == {"max_email_per_day": 15, "time_to_wait_in_mins": 20, "custom_tracking_url": ""}
    assert st.account_fields("google", "zapmail") == {"max_email_per_day": 15, "time_to_wait_in_mins": 20}
    assert st.warmup_args("outlook_dense") == {"enabled": True, "total_per_day": 5, "daily_rampup": 0,
                                               "reply_rate": 30, "auto_adjust": True, "weekdays_only": True}


def test_readback_matches_and_mismatches():
    assert st.readback_mismatches(acct(), "google", "scaledmail", {"max_email_per_day": 15}) == []
    bad = acct(message_per_day=10, warmup_details={"status": "ACTIVE", "max_email_per_day": 30, "reply_rate": 65,
                                                    "warmup_created_at": None, "warmup_reputation": "100%"},
               custom_tracking_domain="track.x.com")
    miss = st.readback_mismatches(bad, "google", "scaledmail", {"signature": "<div>B</div>", "client_id": 12256})
    assert "daily limit 10, want 15" in miss and "warmup 30/day, want 25" in miss and "reply rate 65%, want 30%" in miss
    assert "tracking domain track.x.com, want none" in miss and "signature differs" in miss and "client None, want 12256" in miss
    assert st.readback_mismatches(acct(warmup_details={"status": "INACTIVE"}), "google", "zapmail", {})[0] == "warmup off"


def test_drift_ignores_signature_and_client():
    assert st.drift(acct(message_per_day=20), "google", "zapmail") == ["daily limit 20, want 15"]


def test_ready_dates_and_reputation():
    a = acct()
    assert st.warmup_started(a) == date(2026, 10, 13)
    assert st.ready_date(a, "google") == date(2026, 10, 27) and st.ready_date(a, "smtp") == date(2026, 11, 3)
    assert st.readiness(a, "google", date(2026, 10, 26)) == "warming"
    assert st.readiness(a, "google", date(2026, 10, 27)) == "ready"
    low = acct(warmup_details={**a["warmup_details"], "warmup_reputation": 82})
    assert st.readiness(low, "google", date(2026, 10, 27)) == "low_reputation"
    assert st.readiness(acct(warmup_details={}), "google", date(2026, 10, 27)) == "unknown"


def test_set_warmup_sends_the_standard(monkeypatch):
    import asyncio
    from smartlead.api import SmartleadClient

    sent = []

    class Resp:
        status_code = 200
        def raise_for_status(self): pass
        def json(self): return {"ok": True}

    class FakeHttp:
        async def post(self, url, params, json):
            sent.append(json)
            return Resp()

    c = SmartleadClient("k", account_name="X")
    c._client = FakeHttp()
    asyncio.run(c.set_warmup("1", **st.warmup_args("google")))
    asyncio.run(c.set_warmup("2", **st.warmup_args("outlook_dense")))
    assert sent[0] == {"warmup_enabled": "true", "total_warmup_per_day": 25, "reply_rate_percentage": 30,
                       "daily_rampup": 5, "is_rampup_enabled": True, "auto_adjust_warmup": True,
                       "send_warmups_only_on_weekdays": True}
    assert sent[1]["is_rampup_enabled"] is False and "daily_rampup" not in sent[1]


def test_warmup_start_from_the_single_inbox_read():
    # GET /email-accounts/{id} names it warmup_details.created_at; the list read says warmup_created_at (live 2026-10-13)
    one = acct(warmup_details={"status": "ACTIVE", "created_at": "2026-10-13T10:49:25.235Z", "warmup_reputation": 100})
    assert st.warmup_started(one) == date(2026, 10, 13) and st.ready_date(one, "google") == date(2026, 10, 27)


def test_minimum_gap_is_part_of_the_standard():
    # 15 Oct: new inboxes came in with "Minimum time gap" empty; Smartlead support: at least 15 minutes.
    assert all(st.standard_for(k)["time_to_wait_in_mins"] == 20 for k in ("google", "outlook", "outlook_dense", "smtp"))
    assert st.account_fields("google", "zapmail") == {"max_email_per_day": 15, "time_to_wait_in_mins": 20}
    assert st.drift(acct(minTimeToWaitInMins=None), "google", "zapmail") == ["minimum gap empty, want 20 min"]
    assert st.drift(acct(minTimeToWaitInMins=10), "google", "zapmail") == ["minimum gap 10 min, want 20 min"]
    assert st.drift(acct(minTimeToWaitInMins=20), "google", "zapmail") == []


def test_smtp_ready_after_three_weeks_per_smartlead_support():
    # Smartlead support 15 Oct: SMTP 14-30 days; weekdays-only warmup, so "closer to 3 weeks".
    assert st.standard_for("smtp")["ready_after_days"] == 21


def test_weekly_volume_steps_follow_scaledmail():
    # 15 Oct (ScaledMail support, adopted by Manveen): Google 15 +2/week max 20; Outlook 5 +1/week max 8;
    # ScaledMail Outlook (25 per domain) fixed at 5; SMTP held at 7 until ScaledMail answers.
    steps = {k: (st.standard_for(k)["max_email_per_day"], st.standard_for(k)["weekly_step"], st.standard_for(k)["max_daily"])
             for k in ("google", "outlook", "outlook_dense", "smtp")}
    assert steps == {"google": (15, 2, 20), "outlook": (5, 1, 8), "outlook_dense": (5, 0, 5), "smtp": (7, 0, 7)}
