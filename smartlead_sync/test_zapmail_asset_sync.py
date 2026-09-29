"""Zapmail → /infra tracker sync rules (pure planning; no Mongo, no network).

    .venv\\Scripts\\python.exe -m pytest test_zapmail_asset_sync.py -q
"""
from datetime import datetime, timezone

from smartlead.zapmail_asset_sync import plan_sync
from smartlead.zapmail_clients import client_from_tracker, infer_client

TODAY = "2026-09-29"


def _dom(name, account="PRECISE_LEADS", status="ACTIVE", expire="2027-06-01",
         registered="2026-06-01", provider="GOOGLE"):
    return {"domain": name, "account": account, "status": status, "expire_on": expire,
            "registered_on": registered, "provider": provider}


def _mb(email, status="ACTIVE", expire="2026-10-20", account="PRECISE_LEADS",
        provider="GOOGLE"):
    return {"email": email, "domain": email.split("@")[1], "status": status,
            "expire_on": expire, "assigned_on": "2026-06-01", "provider": provider,
            "account": account}


def _dt(day):
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def _ops(plan, action=None):
    return {o["name"]: o for o in plan["ops"] if action in (None, o["action"])}


# ── client inference ─────────────────────────────────────────────────────────

def test_infer_client():
    assert infer_client("askbettrdata.com", "PRECISE_LEADS") == "Bettrdata"
    assert infer_client("dataingesthq.com", "PRECISE_LEADS") == "Bettrdata"
    assert infer_client("gomelior.com", "PRECISE_LEADS") == "Melior"
    assert infer_client("usepreciseleads.com", "PRECISE_LEADS") == "Precise Leads"
    assert infer_client("anything.com", "Belardi Wong") == "Belardi Wong"
    assert infer_client("reachbw.com", "PRECISE_LEADS") == "Belardi Wong"
    assert infer_client("b2bworldsummit.com", "PRECISE_LEADS") is None   # "bw" inside, not a BW domain
    assert infer_client("kombinatorfunds.com", "PRECISE_LEADS") is None
    # tracker wins; a past-client tracker row is never re-assigned
    assert infer_client("x.com", "PRECISE_LEADS", "Belardiwong") == "Belardi Wong"
    assert infer_client("gomelior.com", "PRECISE_LEADS", "OSC - Srivatsan") is None


def test_client_from_tracker_spellings():
    assert client_from_tracker("Preciseleads") == "Precise Leads"
    assert client_from_tracker("Belardiwong") == "Belardi Wong"
    assert client_from_tracker("Darlean") is None


# ── domains ──────────────────────────────────────────────────────────────────

def test_new_current_client_domain_is_inserted_without_owner():
    plan = plan_sync([_dom("gomelior.com")], [], {}, today=TODAY)
    doc = _ops(plan, "insert")["gomelior.com"]["doc"]
    assert doc["client"] == "Melior" and doc["provider"] == "Zapmail"
    assert doc["workspace"] == "Google" and doc["status"] == "Active"
    assert doc["expiryDate"] == _dt("2027-06-01")
    assert "primaryOwner" not in doc and "visibilityChannel" not in doc  # no reminder spam


def test_past_client_and_lapsed_are_not_inserted():
    plan = plan_sync([_dom("kombinatorfunds.com"),
                      _dom("joinmelior.com", expire="2026-07-03")], [], {}, today=TODAY)
    assert _ops(plan, "insert") == {}
    whys = {s["name"]: s["why"] for s in plan["skipped"]}
    assert whys == {"kombinatorfunds.com": "not a current client",
                    "joinmelior.com": "lapsed/inactive"}


def test_expiry_follows_zapmail_with_one_day_tolerance():
    tracker = {
        "a.com": {"name": "a.com", "client": "Melior", "status": "Active",
                  "workspace": "Google", "purchaseDate": _dt("2026-01-01"),
                  "expiryDate": _dt("2027-05-31")},            # 1 day off: leave it
        "b.com": {"name": "b.com", "client": "Melior", "status": "Active",
                  "workspace": "Google", "purchaseDate": _dt("2026-01-01"),
                  "expiryDate": _dt("2026-10-01")},            # renewed in Zapmail
    }
    plan = plan_sync([_dom("a.com"), _dom("b.com")], [], tracker, today=TODAY)
    ups = _ops(plan, "update")
    assert "a.com" not in ups
    assert ups["b.com"]["set"] == {"expiryDate": _dt("2027-06-01")}


def test_status_only_downgrades_never_reactivates():
    tracker = {
        "lapsed.com": {"name": "lapsed.com", "client": "Melior", "status": "Active",
                       "workspace": "Google", "purchaseDate": _dt("2025-01-01"),
                       "expiryDate": _dt("2026-01-01")},
        "parked.com": {"name": "parked.com", "client": "Melior", "status": "Inactive",
                       "workspace": "Google", "purchaseDate": _dt("2026-01-01"),
                       "expiryDate": _dt("2027-06-01")},
    }
    plan = plan_sync([_dom("lapsed.com", expire="2026-01-01"), _dom("parked.com")],
                     [], tracker, today=TODAY)
    ups = _ops(plan, "update")
    assert ups["lapsed.com"]["set"] == {"status": "Inactive"}
    assert "parked.com" not in ups            # team's Inactive stays Inactive


def test_team_fields_never_overwritten_but_blanks_filled():
    tracker = {"a.com": {"name": "a.com", "client": "Melior", "provider": "Scaled Mail",
                         "status": "Active", "workspace": None, "purchaseDate": None,
                         "expiryDate": _dt("2027-06-01"), "notes": "keep"}}
    plan = plan_sync([_dom("a.com", provider="MICROSOFT")], [], tracker, today=TODAY)
    s = _ops(plan, "update")["a.com"]["set"]
    assert s == {"workspace": "Outlook", "purchaseDate": _dt("2026-06-01")}


# ── inboxes ──────────────────────────────────────────────────────────────────

def test_inbox_insert_uses_domain_client_and_skips_in_progress():
    plan = plan_sync([_dom("gomelior.com")],
                     [_mb("ann@gomelior.com"), _mb("bob@gomelior.com", status="IN_PROGRESS"),
                      _mb("x@kombinatorfunds.com")], {}, today=TODAY)
    ins = _ops(plan, "insert")
    assert ins["ann@gomelior.com"]["doc"]["client"] == "Melior"
    assert ins["ann@gomelior.com"]["doc"]["domain"] == "gomelior.com"
    assert "bob@gomelior.com" not in ins
    assert "x@kombinatorfunds.com" not in ins


def test_inbox_monthly_expiry_and_failed_status():
    tracker = {
        "ann@a.com": {"name": "ann@a.com", "client": "Melior", "status": "Active",
                      "workspace": "Google", "purchaseDate": _dt("2026-06-01"),
                      "domain": "a.com", "expiryDate": _dt("2026-09-20")},
        "bob@a.com": {"name": "bob@a.com", "client": "Melior", "status": "Active",
                      "workspace": "Google", "purchaseDate": _dt("2026-06-01"),
                      "domain": "a.com", "expiryDate": _dt("2026-10-20")},
    }
    plan = plan_sync([], [_mb("ann@a.com"), _mb("bob@a.com", status="FAILED")],
                     tracker, today=TODAY)
    ups = _ops(plan, "update")
    assert ups["ann@a.com"]["set"] == {"expiryDate": _dt("2026-10-20")}
    assert ups["bob@a.com"]["set"] == {"status": "Inactive"}


def test_counts():
    plan = plan_sync([_dom("gomelior.com"), _dom("kombinatorfunds.com")],
                     [_mb("ann@gomelior.com")], {}, today=TODAY)
    assert plan["counts"] == {"domain_insert": 1, "inbox_insert": 1, "domain_skipped": 1}
