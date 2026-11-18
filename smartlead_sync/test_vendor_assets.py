"""Asset tracker rows for inboxes bought by hand from a vendor infra-bot cannot read (t248, 18 Nov 2026). Pure; synthetic."""
from datetime import date

from smartlead import vendor_assets as va

ACCTS = [{"from_email": f"{u}@one.test", "type": "GMAIL"} for u in ("a", "b", "c")] + \
        [{"from_email": "x@two.test", "type": "OUTLOOK"}]
ARGS = dict(vendor="Inboxkit", client="New Client", bought=date(2026, 11, 17), received=date(2026, 11, 18),
            bill_day=date(2026, 12, 17), kind="Prewarmed", why="t248")


def test_one_domain_row_and_one_inbox_row_each_insert_only():
    plan = va.plan_rows(ACCTS, {}, **ARGS)
    ins = {(o["type"], o["name"]) for o in plan["ops"]}
    assert ins == {("DOMAIN", "one.test"), ("DOMAIN", "two.test"), ("INBOX", "a@one.test"), ("INBOX", "b@one.test"),
                   ("INBOX", "c@one.test"), ("INBOX", "x@two.test")}
    assert all(o["action"] == "insert" for o in plan["ops"])
    d = next(o["doc"] for o in plan["ops"] if o["name"] == "one.test")
    assert (d["provider"], d["workspace"], d["brandedPrewarmed"], d["status"], d["client"]) == (
        "Inboxkit", "Google", "Prewarmed", "Active", "New Client")
    assert d["purchaseDate"].date() == date(2026, 11, 17) and d["expiryDate"] is None   # unknown: shown as estimated
    i = next(o["doc"] for o in plan["ops"] if o["name"] == "x@two.test")
    assert (i["domain"], i["workspace"], i["expiryDate"].date()) == ("two.test", "Outlook", date(2026, 12, 17))
    assert "received 18 Nov 2026" in i["notes"] and "estimated" in i["notes"]


def test_rows_the_tracker_already_has_are_left_alone():
    plan = va.plan_rows(ACCTS, {"one.test": {"name": "one.test"}, "a@one.test": {"name": "a@one.test"}}, **ARGS)
    names = {o["name"] for o in plan["ops"]}
    assert "one.test" not in names and "a@one.test" not in names and "b@one.test" in names
    assert plan["counts"] == {"domain_insert": 1, "inbox_insert": 3, "already": 2}
