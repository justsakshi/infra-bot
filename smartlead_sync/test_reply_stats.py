from smartlead.reply_stats import aggregate_domain_stats, evaluate_alerts


def test_aggregate_sums_per_domain():
    rows = [
        {"email": "a@dom1.com", "sent_count": 100, "reply_count": 5},
        {"email": "b@dom1.com", "sent_count": 50, "reply_count": 1},
        {"email": "c@dom2.com", "sent_count": 30, "reply_count": 2},
    ]
    out = aggregate_domain_stats(rows)
    assert out["dom1.com"] == {"sent": 150, "replies": 6}
    assert out["dom2.com"] == {"sent": 30, "replies": 2}


def test_alert_on_drop_vs_own_average():
    # prior week: 5% reply rate; current week: 1.2% -> >30% drop -> alert
    history = [{"sent": 100, "replies": 5} for _ in range(7)]
    current = {"sent": 250, "replies": 3}
    alerts = evaluate_alerts("dom1.com", current, history)
    assert any("drop" in a for a in alerts)


def test_alert_on_one_percent_rule():
    current = {"sent": 250, "replies": 1}   # 0.4% after 200+ sends
    alerts = evaluate_alerts("dom1.com", current, [])
    assert any("1% rule" in a for a in alerts)


def test_no_alert_when_healthy():
    history = [{"sent": 100, "replies": 5} for _ in range(7)]
    current = {"sent": 100, "replies": 5}
    assert evaluate_alerts("dom1.com", current, history) == []


def test_no_drop_alert_on_thin_data():
    # under the send floor (REPLY_ALERT_MIN_SENT), drop-vs-average must not
    # fire (noise) — value kept below the floor, not equal to it
    history = [{"sent": 100, "replies": 5} for _ in range(7)]
    current = {"sent": 5, "replies": 0}
    alerts = evaluate_alerts("dom1.com", current, history)
    assert not any("drop" in a for a in alerts)
