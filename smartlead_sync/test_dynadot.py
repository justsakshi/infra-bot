"""Dynadot: gates, price parsing, the buy run (target, ceilings, sale wait,
lost answers, never buying twice), and the key never reaching a message.
No network: an httpx MockTransport plays Dynadot's api3."""
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from smartlead import dynadot_orders as orders
from smartlead.dynadot import (DynadotBlocked, DynadotClient, DynadotOutcomeUnknown, DynadotRefused,
                               parse_price)

KEY = "k3y-should-never-show-123"


class FakeDynadot:
    """Enough of api3.json for search / register / get_account_balance / domain_info."""

    def __init__(self, balance=300.0, price=2.60, taken=(), premium=(), funds_error="insufficient_funds"):
        self.balance = balance
        self.price = price
        self.taken = set(taken)
        self.premium = set(premium)
        self.owned = set()
        self.registers = []
        self.drop_register_answer = set()   # register goes through, but the answer is lost
        self.fail_transport = False
        self.funds_error = funds_error

    def __call__(self, request: httpx.Request) -> httpx.Response:
        if self.fail_transport:
            raise httpx.ConnectError("boom", request=request)
        q = dict(request.url.params)
        assert q["key"] == KEY
        cmd = q["command"]
        if cmd == "get_account_balance":
            return httpx.Response(200, json={"GetAccountBalanceResponse": {
                "ResponseCode": 0, "Status": "success",
                "BalanceList": [{"Currency": "USD", "Amount": f"{self.balance:.2f}"}]}})
        if cmd == "search":
            d = q["domain0"]
            avail = d not in self.taken and d not in self.owned
            price = (f"{self.price:.2f} in USD and domain is "
                     f"{'premium' if d in self.premium else 'not premium'}") if avail else ""
            return httpx.Response(200, json={"SearchResponse": {"ResponseCode": "0", "SearchResults": [
                {"DomainName": d, "Available": "yes" if avail else "no", "Price": price}]}})
        if cmd == "domain_info":
            d = q["domain"]
            if d in self.owned:
                return httpx.Response(200, json={"DomainInfoResponse": {
                    "ResponseCode": 0, "Status": "success", "DomainInfo": {"Name": d, "Expiration": "1"}}})
            return httpx.Response(200, json={"DomainInfoResponse": {
                "ResponseCode": -1, "Status": "error", "Error": "could not find domain"}})
        if cmd == "register":
            d = q["domain"]
            self.registers.append(d)
            if self.balance < self.price:
                return httpx.Response(200, json={"RegisterResponse": {
                    "ResponseCode": -1, "Status": "error", "Error": self.funds_error}})
            if d in self.owned or d in self.taken:
                return httpx.Response(200, json={"RegisterResponse": {
                    "ResponseCode": -1, "Status": "error", "Error": "not_available"}})
            self.owned.add(d)
            self.balance -= self.price
            if d in self.drop_register_answer:
                raise httpx.ReadTimeout("lost", request=request)
            return httpx.Response(200, json={"RegisterResponse": {
                "ResponseCode": 0, "Status": "success", "DomainName": d, "Expiration": 1790000000000}})
        return httpx.Response(200, json={"Response": {"ResponseCode": -1, "Error": "unknown command"}})


def client(fake, sandbox=False):
    return DynadotClient(KEY, sandbox=sandbox, transport=httpx.MockTransport(fake), sleep=lambda s: None)


class Clock:
    def __init__(self):
        self.t = datetime(2026, 11, 27, 0, 5, tzinfo=timezone.utc)

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += timedelta(seconds=s)


def run(fake, names, tmp_path, *, target, sandbox=False, monkeypatch=None, **kw):
    clock = kw.pop("clock", Clock())
    log = kw.pop("log", lambda s: None)
    with client(fake, sandbox=sandbox) as dd:
        return orders.buy(dd, names, target=target, ledger=orders.Ledger(tmp_path / "ledger.json"),
                          approve=True, sleep=clock.sleep, clock=clock, log=log, **kw)


@pytest.fixture(autouse=True)
def allow_spend(monkeypatch):
    monkeypatch.setenv("DYNADOT_ALLOW_SPEND", "true")


NAMES = [f"name{i}example.co" for i in range(10)]


def test_parse_price():
    assert parse_price("2.60 in USD and domain is not premium") == (2.6, "USD", False)
    assert parse_price("1,200.00 in USD and domain is premium") == (1200.0, "USD", True)
    assert parse_price("") == (None, None, False)
    live = ("Registration Price: 88.00 in USD and Renewal price: 16.00 in USD and "
            "Domain is not a Premium Domain")                     # sandbox, 24 Nov 2026
    assert parse_price(live) == (88.0, "USD", False)
    assert parse_price("Registration Price: 950.00 in USD and Renewal price: 16.00 in USD and "
                       "Domain is a Premium Domain")[2] is True


def test_register_gates(monkeypatch):
    fake = FakeDynadot()
    with client(fake) as dd:
        with pytest.raises(DynadotBlocked):
            dd.register("a.co", approve=False)
        monkeypatch.setenv("DYNADOT_ALLOW_SPEND", "")
        with pytest.raises(DynadotBlocked):
            dd.register("a.co", approve=True)
    with client(fake, sandbox=True) as dd:          # test account: no real money, approve is enough
        assert dd.register("a.co", approve=True)["status"] == "success"
    assert fake.registers == ["a.co"]


def test_buy_refuses_without_spend_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DYNADOT_ALLOW_SPEND", "")
    with pytest.raises(DynadotBlocked):
        run(FakeDynadot(), NAMES, tmp_path, target=2)


def test_buys_to_target_and_skips_taken_and_premium(tmp_path):
    fake = FakeDynadot(taken={NAMES[0]}, premium={NAMES[1]})
    s = run(fake, NAMES, tmp_path, target=3)
    assert s["bought"] == 3 and s["stopped"] == "target reached"
    assert fake.registers == NAMES[2:5]
    assert s["committed_usd"] == pytest.approx(7.80)
    led = orders.Ledger(tmp_path / "ledger.json")
    assert led.domains[NAMES[0]]["status"] == "skipped"
    assert led.domains[NAMES[1]]["reason"] == "premium"


def test_skips_are_logged(tmp_path):
    """buy.log is what a person reads on Friday: every skipped name says why."""
    lines = []
    run(FakeDynadot(taken={NAMES[0]}, premium={NAMES[1]}), NAMES, tmp_path, target=1, log=lines.append)
    assert f"skipped {NAMES[0]}: not available" in lines
    assert f"skipped {NAMES[1]}: premium" in lines


def test_second_run_never_buys_twice(tmp_path):
    fake = FakeDynadot()
    run(fake, NAMES, tmp_path, target=3)
    s = run(fake, NAMES, tmp_path, target=5)
    assert s["bought"] == 5
    assert fake.registers == NAMES[:5]              # each name ordered exactly once


def test_waits_for_sale_then_buys(tmp_path):
    fake = FakeDynadot(price=15.50)
    clock = Clock()
    real_sleep = clock.sleep

    def sleep(s):                                   # the sale goes live after the first wait
        real_sleep(s)
        fake.price = 2.60
    clock.sleep = sleep
    s = run(fake, NAMES, tmp_path, target=2, clock=clock,
            wait_until=clock() + timedelta(minutes=30), poll_seconds=120)
    assert s["bought"] == 2 and fake.registers == NAMES[:2]


def test_never_buys_above_ceiling(tmp_path):
    fake = FakeDynadot(price=15.50)
    clock = Clock()
    s = run(fake, NAMES, tmp_path, target=2, clock=clock, wait_until=clock() + timedelta(minutes=10),
            poll_seconds=120)
    assert s["bought"] == 0 and fake.registers == []
    assert "above the $2.60 ceiling" in s["stopped"]


def test_total_ceiling_stops(tmp_path):
    fake = FakeDynadot()
    s = run(fake, NAMES, tmp_path, target=10, total_ceiling=8.0)
    assert s["bought"] == 3 and "total ceiling" in s["stopped"]


def test_balance_runs_out(tmp_path):
    fake = FakeDynadot(balance=5.5)
    s = run(fake, NAMES, tmp_path, target=5)
    assert s["bought"] == 2 and "balance ran out" in s["stopped"]
    assert len(fake.registers) == 3                 # stops at the first refusal for money


def test_balance_below_one_domain_buys_nothing(tmp_path):
    fake = FakeDynadot(balance=1.0)
    s = run(fake, NAMES, tmp_path, target=5)
    assert s["bought"] == 0 and fake.registers == []


def test_lost_answer_is_looked_up_not_reordered(tmp_path):
    fake = FakeDynadot()
    fake.drop_register_answer = {NAMES[1]}
    s = run(fake, NAMES, tmp_path, target=3)
    assert s["bought"] == 3
    assert fake.registers == NAMES[:3]
    led = orders.Ledger(tmp_path / "ledger.json")
    assert "found in the account" in led.domains[NAMES[1]]["note"]


def test_unknown_resolved_on_next_run(tmp_path):
    led = orders.Ledger(tmp_path / "ledger.json")
    led.set(NAMES[0], status="unknown", price_usd=2.6)
    fake = FakeDynadot()
    fake.owned.add(NAMES[0])                        # it did go through last time
    s = run(fake, NAMES, tmp_path, target=2)
    assert s["bought"] == 2 and fake.registers == [NAMES[1]]


def test_bad_names_refused_before_anything(tmp_path):
    fake = FakeDynadot()
    with pytest.raises(Exception, match="bad names"):
        run(fake, ["fine.co", "wrongtld.com"], tmp_path, target=1)
    assert fake.registers == []


def test_key_never_in_error_messages():
    fake = FakeDynadot()
    fake.fail_transport = True
    with client(fake) as dd:
        with pytest.raises(DynadotOutcomeUnknown) as e:
            dd.balance()
    assert KEY not in str(e.value) and "api.dynadot.com" not in str(e.value)

    def five_hundred(request):
        return httpx.Response(500, text=f"oops {request.url}")
    with DynadotClient(KEY, transport=httpx.MockTransport(five_hundred), sleep=lambda s: None) as dd:
        with pytest.raises(DynadotOutcomeUnknown) as e:
            dd.balance()
    assert KEY not in str(e.value)

    def refused(request):
        return httpx.Response(403, text=f"denied {request.url}")
    with DynadotClient(KEY, transport=httpx.MockTransport(refused), sleep=lambda s: None) as dd:
        with pytest.raises(DynadotRefused) as e:
            dd.balance()
    assert KEY not in str(e.value)


def test_summary_text():
    s = {"target": 100, "bought": 98, "committed_usd": 257.4, "stopped": "ran out of names",
         "balance_after": 12.6, "skipped": 30, "failed": 0, "unknown": 1, "sandbox": False}
    text = orders.summary_text(s, "C:/x/ledger.json")
    assert text.startswith("Dynadot buy: 98 of 100 bought, $257.40 spent.")
    assert "\n\n" in text and "never buys one twice" in text


def test_cli_live_buy_needs_a_sandbox_pass(tmp_path, monkeypatch):
    import dynadot_cli
    sent = []
    monkeypatch.setattr(dynadot_cli, "_dm", lambda text, log: sent.append(text) or True)
    monkeypatch.setenv("DYNADOT_API_KEY", KEY)
    monkeypatch.setenv("DYNADOT_DIR", str(tmp_path))      # the CLI writes it; restored after the test
    (tmp_path / "names.txt").write_text("name0example.co\n")
    argv = ["--env-file", str(tmp_path / "none.env"), "--dir", str(tmp_path), "buy", "--names",
            str(tmp_path / "names.txt"), "--count", "1", "--approve", "--allow-spend",
            "--require-sandbox-pass", "--notify"]
    assert dynadot_cli.main(argv) == 2
    assert "no sandbox test" in sent[0]
    assert "no sandbox test" in (tmp_path / "buy.log").read_text()


def test_cli_missing_key_says_so(tmp_path, monkeypatch):
    import dynadot_cli
    sent = []
    monkeypatch.setattr(dynadot_cli, "_dm", lambda text, log: sent.append(text) or True)
    monkeypatch.delenv("DYNADOT_API_KEY", raising=False)
    monkeypatch.setenv("DYNADOT_DIR", str(tmp_path))      # the CLI writes it; restored after the test
    (tmp_path / "names.txt").write_text("name0example.co\n")
    argv = ["--env-file", str(tmp_path / "none.env"), "--dir", str(tmp_path), "buy", "--names",
            str(tmp_path / "names.txt"), "--count", "1", "--approve", "--notify"]
    assert dynadot_cli.main(argv) == 2
    assert "DYNADOT_API_KEY is not set" in sent[0]
