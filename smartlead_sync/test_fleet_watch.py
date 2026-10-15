"""fleet_watch.py: building each day's record and keeping it. No network."""
from datetime import date

import fleet_watch as fw

TODAY = date(2026, 10, 20)


def acct(**over):
    a = {"id": 1, "from_email": "A@d.com", "type": "GMAIL", "smtp_host": None, "message_per_day": 15, "client_id": None,
         "is_smtp_success": True, "is_imap_success": True,
         "tags": [{"tag_name": "Bot-Setup"}, {"tag_name": "ScaledMail-Google"}],
         "warmup_details": {"status": "ACTIVE", "warmup_reputation": "98%", "warmup_created_at": "2026-10-13T10:00:00Z"}}
    a.update(over)
    return a


FACTS = {"a@d.com": {"client": "Precise Leads", "connected": True, "campaigns": ["PL 1"], "sending": ["PL 1"],
                     "tested_on": "2026-10-13", "pct": 95.0}}


def test_record_holds_the_inbox_state_for_the_day():
    r = fw.record("PRECISE_LEADS", acct(), FACTS, verdicts={"a@d.com": "GOOD"}, per_domain={"d.com": 3},
                  approved={"a@d.com": (15, "2026-10-14")}, health={"a@d.com": (120, 1)}, today=TODAY)
    assert r == {"email": "a@d.com", "domain": "d.com", "date": "2026-10-20", "account": "PRECISE_LEADS",
                 "client": "Precise Leads", "kind": "google", "vendor": "ScaledMail", "setup": True, "limit": 15,
                 "approved_limit": 15, "warmup_on": True, "rep": 98, "connected": True, "campaigns": 1, "sending": True,
                 "ready_on": "2026-10-27", "warmup_started": "2026-10-13", "test_date": "2026-10-13", "test_pct": 95.0,
                 "verdict": "GOOD", "sent_14d": 120, "bounced_14d": 1}


def test_old_inboxes_get_no_ready_date_and_missing_facts_are_empty():
    r = fw.record("BETTRDATA", acct(tags=[], is_imap_success=False, warmup_details={"status": "INACTIVE"}), {}, {},
                  {"d.com": 1}, {}, {}, TODAY)
    assert r["ready_on"] is None and r["setup"] is False and r["warmup_on"] is False and r["connected"] is False
    assert r["client"] == "Bettrdata" and r["campaigns"] == 0 and r["test_date"] is None and r["vendor"] is None


class FakeCol:
    def __init__(self): self.docs = []
    def _match(self, d, q):
        for k, v in q.items():
            if isinstance(v, dict) and "$gte" in v:
                if not d.get(k) or d[k] < v["$gte"]:
                    return False
            elif isinstance(v, dict) and "$in" in v:
                if d.get(k) not in v["$in"]:
                    return False
            elif d.get(k) != v:
                return False
        return True
    def update_one(self, q, upd, upsert=False):
        for d in self.docs:
            if self._match(d, q):
                d.update(upd["$set"]); return
        self.docs.append({**q, **upd["$set"]})
    def find(self, q, proj=None):
        return [dict(d) for d in self.docs if self._match(d, q)]


def test_store_upserts_by_inbox_and_day_and_returns_history_oldest_first():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_state([{"email": "a@d.com", "date": "2026-10-19", "rep": 90}])
    s.save_metrics({"a@d.com": {"2026-10-13": {"sent": 15, "bounced": 0}}})
    s.save_metrics({"a@d.com": {"2026-10-13": {"sent": 14, "bounced": 1}}})           # a later read overwrites
    s.save_state([{"email": "a@d.com", "date": "2026-10-20", "rep": 80}])
    h = s.history(["a@d.com"], since="2026-10-14")
    assert [d["rep"] for d in h["a@d.com"]] == [90, 80]
    assert [d for d in s.daily.docs if d["date"] == "2026-10-13"][0]["sent"] == 14


def test_store_keeps_cases_by_key():
    s = fw.FleetStore(FakeCol(), FakeCol())
    s.save_cases([{"key": "warmup_off:a@d.com", "kind": "warmup_off", "closed_on": None}])
    assert list(s.open_cases()) == ["warmup_off:a@d.com"]
    s.save_cases([{"key": "warmup_off:a@d.com", "kind": "warmup_off", "closed_on": "2026-10-21"}])
    assert s.open_cases() == {} and len(s.all_cases()) == 1
