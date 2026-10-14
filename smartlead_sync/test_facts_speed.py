"""The Smartlead read behind Before the bill / Kill warnings: same answer, done
side by side (2026-10-14: 123 s, all calls one after another). No network."""
import json
import time
from types import SimpleNamespace

import placement_report as pr
import renewal_review as rr

DELAY = 0.2
ACC = SimpleNamespace(name="PRECISE_LEADS", api_key="k")
TESTS = [{"spam_test_id": i, "status": "COMPLETED", "created_at": f"2026-10-{10 + i:02d}T00:00:00Z"} for i in range(8)]


def _fake(monkeypatch, tmp_path):
    seen_mx = []

    def get(url, key, **params):
        if url.endswith("/email-accounts/"):
            return [{"from_email": "a@x.com", "client_id": None, "is_smtp_success": True, "is_imap_success": True,
                     "warmup_details": {"warmup_reputation": "100%"}}]
        if "sender-account-wise" in url:
            time.sleep(DELAY)
            i = int(url.split("/report/")[1].split("/")[0])
            spam = i % 3
            return [{"email": "A@x.com", "details": [{"email": f"s{n}@gmail.com" if n % 2 else f"s{n}@outlook.com",
                                                       "reply": {"mail_folder": "spam" if n < spam else "inbox"}}
                                                      for n in range(10)]}]
        raise AssertionError(url)

    class Resp:
        def json(self): return TESTS

    monkeypatch.setattr(rr, "_get", get)
    monkeypatch.setattr(rr.httpx, "post", lambda *a, **k: Resp())
    monkeypatch.setattr(rr, "discover_accounts", lambda: [ACC])
    monkeypatch.setattr(pr, "attachments", lambda key, sending_days=None: {})

    def dns(url, params, timeout):
        seen_mx.append(params["name"])
        time.sleep(DELAY)
        mx = "aspmx.l.google.com" if "gmail" in params["name"] else "x.mail.protection.outlook.com"
        return SimpleNamespace(json=lambda: {"Answer": [{"data": mx}]})
    monkeypatch.setattr(rr.httpx, "get", dns)
    monkeypatch.setattr(rr, "_SEED_MX", {})
    monkeypatch.setattr(rr, "SEED_MX_FILE", tmp_path / "seed_mx.json")
    return seen_mx


def test_tests_read_side_by_side_same_answer(monkeypatch, tmp_path):
    _fake(monkeypatch, tmp_path)
    t = time.time()
    facts = rr.smartlead_facts([])
    took = time.time() - t
    tests = facts["a@x.com"]["tests"]
    assert [x["test_id"] for x in tests] == [7, 6, 5, 4, 3, 2, 1, 0]          # newest first, as before
    assert [x["spam"] for x in tests] == [1, 0, 2, 1, 0, 2, 1, 0] and all(x["seeds"] == 10 for x in tests)
    assert tests[0]["by"] == {"Outlook": [5, 1], "Google": [5, 0]}
    assert facts["a@x.com"]["tested_on"] == "2026-10-17" and facts["a@x.com"]["pct"] == 90.0
    assert took < 8 * DELAY * 0.6, f"{took:.2f}s: still one at a time"      # 8 reports one by one = 1.6 s


def test_seed_providers_are_remembered_between_runs(monkeypatch, tmp_path):
    seen = _fake(monkeypatch, tmp_path)
    rr.smartlead_facts([])
    first = len(seen)
    assert first == 2 and json.loads((tmp_path / "seed_mx.json").read_text()) == {"gmail.com": "Google", "outlook.com": "Outlook"}
    monkeypatch.setattr(rr, "_SEED_MX", {})                                  # a new run: memory empty, file kept
    rr.smartlead_facts([])
    assert len(seen) == first                                                # no lookups the second time


def test_live_campaign_reads_side_by_side_same_order(monkeypatch):
    camps = [{"id": i, "name": f"C{i}", "status": "ACTIVE"} for i in range(6)]

    def get(url, key, **params):
        if url.endswith("/campaigns"):
            return camps
        time.sleep(DELAY)
        cid = int(url.split("/campaigns/")[1].split("/")[0])
        if url.endswith("/analytics"):
            return {"status": "ACTIVE", "campaign_lead_stats": {"notStarted": 1, "inprogress": 0}}
        if url.endswith("/analytics-by-date"):
            return {"sent_count": str(cid % 2)}
        return [{"from_email": "a@x.com"}]
    monkeypatch.setattr(pr, "_get", get)
    t = time.time()
    out = pr.attachments("k", sending_days=7)
    took = time.time() - t
    assert out == {"a@x.com": {"campaigns": [f"C{i}" for i in range(6)], "sending": ["C1", "C3", "C5"]}}
    assert took < 6 * 3 * DELAY * 0.5, f"{took:.2f}s: still one at a time"  # 18 calls one by one = 3.6 s
