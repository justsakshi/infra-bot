from smartlead.capacity import compute_client_capacity


def _inbox(rep="99%", state="on", test="inbox", conn=True, mpd=30, status="ACTIVE"):
    return {"warmup_rep_pct": rep, "warmup_state": state, "test_sheet_status": test,
            "connection_ok": conn, "message_per_day": mpd, "campaign_status": status,
            "email": "e@x.com", "client": "C"}


def test_healthy_fleet_no_order():
    # 10 healthy active inboxes (cap 30 -> 300/day), demand 100/day, bench 5
    rows = [_inbox() for _ in range(10)] + \
           [_inbox(status="", state="on") for _ in range(5)]  # idle bench
    out = compute_client_capacity(rows, demand_per_day=100, churn_per_month=0)
    assert out["safe_capacity"] == 300
    assert out["bench"] == 5
    assert out["bench_target"] == 5  # max(5, 25% of 10 active = 2.5 -> 3) -> 5
    assert out["order_inboxes"] == 0
    assert out["status"] == "OK"


def test_capacity_shortfall_orders_inboxes_and_domains():
    # 2 healthy active inboxes = 60/day capacity, demand 200/day
    rows = [_inbox() for _ in range(2)]
    out = compute_client_capacity(rows, demand_per_day=200, churn_per_month=0)
    # need 200*1.2=240 -> shortfall 180 -> ceil(180/30)=6 inboxes + bench deficit 5
    assert out["order_inboxes"] == 11
    assert out["order_domains"] == 6  # ceil(11/2)
    assert out["status"] == "ORDER NOW"


def test_unhealthy_inboxes_do_not_count():
    rows = [
        _inbox(rep="70%"),            # low rep
        _inbox(test="fail"),          # failed placement
        _inbox(conn=False),           # disconnected
        _inbox(state="blocked"),      # warmup blocked
    ]
    out = compute_client_capacity(rows, demand_per_day=0, churn_per_month=0)
    assert out["sendable_inboxes"] == 0
    assert out["safe_capacity"] == 0


def test_churn_added_to_order():
    rows = [_inbox() for _ in range(2)]
    base = compute_client_capacity(rows, demand_per_day=200, churn_per_month=0)
    churned = compute_client_capacity(rows, demand_per_day=200, churn_per_month=4)
    assert churned["order_inboxes"] == base["order_inboxes"] + 4
