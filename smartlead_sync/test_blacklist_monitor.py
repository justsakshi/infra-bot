"""Blocklist monitor (audit 7 Nov 2026: BLOCKLIST-1/3/4/8/9/10/12; review BL-R1/R2). Fakes only, no network.

    .venv\\Scripts\\python.exe -m pytest test_blacklist_monitor.py -q
"""
import asyncio
import socket
import struct
from datetime import date as _date

import pytest

import blacklist_monitor as bm
from smartlead.config import INFRA_CHANNEL, AccountConfig

TODAY = "2026-11-09"
PL, BD = "PRECISE_LEADS", "BettrData"


# ── fakes ────────────────────────────────────────────────────────────────────

class FakeCol:
    """Enough of a Mongo collection for Store: equality, array membership, $in; $set/$setOnInsert upserts."""

    def __init__(self, rows=()):
        self.rows = [dict(r) for r in rows]

    def find(self, flt, proj=None):
        out = []
        for r in self.rows:
            ok = True
            for k, v in flt.items():
                if isinstance(v, dict) and "$in" in v:
                    ok = ok and r.get(k) in v["$in"]
                elif isinstance(r.get(k), list):
                    ok = ok and v in r[k]
                else:
                    ok = ok and r.get(k) == v
            if ok:
                out.append(dict(r))
        return out

    def create_index(self, *a, **k):
        pass

    def bulk_write(self, ops, ordered=False):
        for flt, upd in ops:
            row = next((r for r in self.rows if all(r.get(k) == v for k, v in flt.items())), None)
            if row is None:
                row = dict(flt)
                row.update(upd.get("$setOnInsert", {}))
                self.rows.append(row)
            row.update(upd.get("$set", {}))


def _row(domain, date, listed=(), clients=(PL,), errors=(), inconclusive=(), **extra):
    return {"domain": domain, "date": date, "listed_on": list(listed), "clients": list(clients),
            "lookup_errors": list(errors), "inconclusive_zones": list(inconclusive), **extra}


def _result(domain, listed=(), errors=(), inconclusive=()):
    return {"domain": domain, "listed_on": list(listed), "lookup_errors": list(errors),
            "inconclusive_zones": list(inconclusive)}


def _inbox(email, client_id=None, tags=()):
    return {"from_email": email, "client_id": client_id, "tags": list(tags)}


@pytest.fixture(autouse=True)
def _no_pause(monkeypatch):
    monkeypatch.setattr(bm, "_RETRY_PAUSE_S", 0)
    monkeypatch.setattr(bm, "UpdateOne", lambda flt, upd, upsert=False: (flt, upd))


# ── lookups: error codes are UNKNOWN, never clean or listed (BLOCKLIST-12) ──

class _Resp:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        pass

    def json(self):
        return self._data


class _DoH:
    def __init__(self, *answers):
        self.answers = list(answers)
        self.calls = 0

    async def get(self, url, params=None):
        self.calls += 1
        a = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(a, Exception):
            raise a
        if a == "NX":
            return _Resp({"Status": 3})
        if isinstance(a, int):                                  # a bare DNS status: 2 SERVFAIL, 5 REFUSED
            return _Resp({"Status": a})
        return _Resp({"Status": 0, "Answer": [{"type": 1, "data": ip} for ip in a]})


@pytest.mark.parametrize("ip", ["127.255.255.252", "127.255.255.254", "127.255.255.255", "127.0.0.1", "127.0.1.255"])
def test_error_answers_are_lookup_errors(ip):
    assert asyncio.run(bm._listed_ips(_DoH([ip]), "x.com", "dbl.spamhaus.org")) is None


def test_listing_clean_and_failure():
    assert asyncio.run(bm._listed_ips(_DoH(["127.0.1.2"]), "x.com", "dbl.spamhaus.org")) == ["127.0.1.2"]
    assert asyncio.run(bm._listed_ips(_DoH("NX"), "x.com", "dbl.spamhaus.org")) == []
    assert asyncio.run(bm._listed_ips(_DoH(RuntimeError("timeout")), "x.com", "dbl.spamhaus.org")) is None


@pytest.mark.parametrize("status", [2, 5])
def test_doh_error_status_is_unknown_not_clean(status):
    """BL-R1: Google DoH SERVFAIL / REFUSED came back as [] (clean)."""
    assert asyncio.run(bm._listed_ips(_DoH(status), "x.com", "dbl.spamhaus.org")) is None


def _dns_reply(query: bytes, rcode=0, ips=(), tc=False) -> bytes:
    """A DNS reply to `query`: same id and question, compressed-name A answers."""
    flags = 0x8180 | rcode | (0x0200 if tc else 0)
    head = query[:2] + struct.pack(">HHHHH", flags, 1, len(ips), 0, 0)
    answers = b"".join(struct.pack(">HHHIH", 0xC00C, 1, 1, 60, 4) + bytes(int(x) for x in ip.split("."))
                       for ip in ips)
    return head + query[12:] + answers


def _udp(monkeypatch, **reply):
    class Sock:
        def __init__(self, *a):
            self.q = b""

        def settimeout(self, t):
            pass

        def sendto(self, pkt, addr):
            self.q = pkt

        def recvfrom(self, n):
            return _dns_reply(self.q, **reply), ("1.2.3.4", 53)

        def close(self):
            pass
    monkeypatch.setattr(socket, "socket", Sock)
    return bm._udp_query_a("x.com.dbl.spamhaus.org", "1.2.3.4")


def test_udp_reply_codes(monkeypatch):
    """BL-R1: the list's own server answering SERVFAIL or REFUSED is unknown, never clean."""
    assert _udp(monkeypatch, rcode=0, ips=["127.0.1.2"]) == ["127.0.1.2"]
    assert _udp(monkeypatch, rcode=3) == []                     # NXDOMAIN: not listed
    assert _udp(monkeypatch, rcode=0) == []                     # no A record: not listed
    assert _udp(monkeypatch, rcode=2) is None                   # SERVFAIL
    assert _udp(monkeypatch, rcode=5) is None                   # REFUSED
    assert _udp(monkeypatch, rcode=0, tc=True) is None          # truncated


def test_authoritative_error_answer_is_unknown(monkeypatch):
    monkeypatch.setattr(bm, "_udp_query_a", lambda name, ns: ["127.255.255.254"])
    assert asyncio.run(bm._listed_ips_auth("1.2.3.4", "x.com", "dbl.spamhaus.org")) is None
    monkeypatch.setattr(bm, "_udp_query_a", lambda name, ns: ["127.0.1.2"])
    assert asyncio.run(bm._listed_ips_auth("1.2.3.4", "x.com", "dbl.spamhaus.org")) == ["127.0.1.2"]


def _zone_only(monkeypatch, zone="dbl.spamhaus.org"):
    monkeypatch.setattr(bm, "BLACKLIST_ZONES", {zone: bm.BLACKLIST_ZONES[zone]})


def test_check_domain_retries_once_then_reports_unknown(monkeypatch):
    _zone_only(monkeypatch)
    strat = {"dbl.spamhaus.org": {"mode": "doh", "ns_ip": None}}
    sem = asyncio.Semaphore(1)
    flaky = _DoH(RuntimeError("timeout"), "NX")
    r = asyncio.run(bm._check_domain(flaky, sem, "x.com", strat))
    assert r["lookup_errors"] == [] and r["listed_on"] == [] and flaky.calls == 2
    dead = _DoH(["127.255.255.254"])
    r = asyncio.run(bm._check_domain(dead, sem, "x.com", strat))
    assert r["lookup_errors"] == ["Spamhaus DBL"] and r["listed_on"] == [] and dead.calls == 2


def _strategy(monkeypatch, *, ns_ip, auth_answer, doh_answers):
    _zone_only(monkeypatch)

    async def zone_ns(client, zone):
        return ns_ip
    monkeypatch.setattr(bm, "_zone_ns_ip", zone_ns)
    monkeypatch.setattr(bm, "_udp_query_a", lambda name, ns: auth_answer)
    doh = _DoH(*doh_answers)
    return asyncio.run(bm._pick_strategies(doh))["dbl.spamhaus.org"], doh


def test_strategy_prefers_the_lists_own_name_server(monkeypatch):
    """7 Nov 2026: Google DoH missed Spamhaus's own test listing on 2 of 8 tries, so DoH is a fallback only."""
    got, doh = _strategy(monkeypatch, ns_ip="1.2.3.4", auth_answer=["127.0.1.2"], doh_answers=[["127.0.1.2"]])
    assert got == {"mode": "auth", "ns_ip": "1.2.3.4"} and doh.calls == 0


def test_strategy_doh_fallback_counts_listings_only(monkeypatch):
    """BL-R1: DoH drops about a quarter of Spamhaus answers, so passing probes never make its NXDOMAIN clean."""
    got, _ = _strategy(monkeypatch, ns_ip="1.2.3.4", auth_answer=None, doh_answers=[["127.0.1.2"]])
    assert got["mode"] == "hits_only"
    got, _ = _strategy(monkeypatch, ns_ip=None, auth_answer=None, doh_answers=["NX", ["127.0.1.2"]])
    assert got["mode"] == "hits_only"                                             # seen once is enough
    got, doh = _strategy(monkeypatch, ns_ip=None, auth_answer=None, doh_answers=["NX"])
    assert got["mode"] == "broken" and doh.calls == bm._CONTROL_PROBES            # never seen
    got, _ = _strategy(monkeypatch, ns_ip="1.2.3.4", auth_answer=["127.255.255.254"], doh_answers=["NX"])
    assert got["mode"] == "broken"                                                # refused everywhere


def test_hits_only_zone_counts_listings_and_nothing_else(monkeypatch):
    _zone_only(monkeypatch)
    strat = {"dbl.spamhaus.org": {"mode": "hits_only", "ns_ip": None}}
    sem = asyncio.Semaphore(1)
    r = asyncio.run(bm._check_domain(_DoH(["127.0.1.2"]), sem, "x.com", strat))
    assert r["listed_on"] == ["Spamhaus DBL (127.0.1.2)"] and r["inconclusive_zones"] == []
    r = asyncio.run(bm._check_domain(_DoH("NX"), sem, "y.com", strat))
    assert r["listed_on"] == [] and r["inconclusive_zones"] == ["Spamhaus DBL"]  # not seen is not clean
    r = asyncio.run(bm._check_domain(_DoH(2), sem, "z.com", strat))
    assert r["inconclusive_zones"] == ["Spamhaus DBL"] and r["lookup_errors"] == []


def test_broken_zone_is_inconclusive(monkeypatch):
    _zone_only(monkeypatch)
    r = asyncio.run(bm._check_domain(_DoH("NX"), asyncio.Semaphore(1), "x.com",
                                     {"dbl.spamhaus.org": {"mode": "broken", "ns_ip": None}}))
    assert r["inconclusive_zones"] == ["Spamhaus DBL"] and r["listed_on"] == []


# ── which domains: real clients, and unreadable accounts still checked (BLOCKLIST-3/8) ──

ACCOUNTS = [AccountConfig(name=PL, api_key="k1"), AccountConfig(name=BD, api_key="k2")]


def _lister(by_account):
    async def fake(acc):
        got = by_account[acc.name]
        if isinstance(got, Exception):
            raise got
        return got
    return fake


def test_collect_names_the_client_not_the_account(monkeypatch):
    monkeypatch.setattr(bm, "_list_inboxes", _lister({
        PL: [_inbox("a@m1.com", 12256), _inbox("b@m1.com", 12256), _inbox("c@p1.com"),
             _inbox("d@osc1.com", 145916, tags=[{"tag_name": "OSC"}])],
        BD: RuntimeError("401 on /api/v1/email-accounts: plan expired api_key=SECRET123"),
    }))
    inv, per, failed = asyncio.run(bm._collect_domains(ACCOUNTS))
    assert inv["m1.com"]["clients"] == {"Melior"} and inv["m1.com"]["accounts"] == {PL}
    assert len(inv["m1.com"]["emails"]) == 2
    assert inv["p1.com"]["clients"] == {"Precise Leads"}
    assert "osc1.com" not in inv                                  # excluded client dropped
    assert per == {PL: 4}                                         # inboxes returned, excluded ones too
    assert failed[0]["account"] == BD and "SECRET123" not in failed[0]["error"]


def test_unreadable_account_checked_from_its_last_weekly_list():
    col = FakeCol([
        _row("b1.com", "2026-10-26", clients=[BD]),
        _row("b1.com", "2026-11-02", clients=[BD], client_names=["Bettrdata"]),
        _row("b2.com", "2026-11-02", clients=[BD], client_names=["Bettrdata"]),
        _row("b3.com", "2026-11-07", clients=["adhoc"]),             # a check by hand is not a list
        _row("b4.com", "2026-11-08", clients=[BD], run="adhoc"),
    ])
    inv = {"m1.com": bm._entry()}
    skipped = bm._fill_from_last_lists(inv, {PL: 1}, [{"account": BD, "error": "401"}], bm.Store(col))
    assert skipped == [{"account": BD, "error": "401", "list_date": "2026-11-02", "domains": 2, "mongo_ok": True}]
    assert set(inv) == {"m1.com", "b1.com", "b2.com"}
    assert inv["b1.com"]["stale"] and inv["b1.com"]["clients"] == {"Bettrdata"} and inv["b1.com"]["accounts"] == {BD}


def test_account_with_no_inboxes_counts_as_skipped_only_if_it_had_some():
    col = FakeCol([_row("b1.com", "2026-11-02", clients=[BD])])
    inv = {}
    skipped = bm._fill_from_last_lists(inv, {BD: 0, "NEWONE": 0, PL: 3}, [], bm.Store(col))
    assert [s["account"] for s in skipped] == [BD] and "no inboxes" in skipped[0]["error"]
    assert "b1.com" in inv


def test_no_mongo_says_mongo_not_no_saved_list():
    """BL-R2: with Mongo down the list exists but can't be read; and an empty account can't be ruled out."""
    skipped = bm._fill_from_last_lists({}, {BD: 0}, [{"account": "MELIOR", "error": "401"}], None)
    assert [s["account"] for s in skipped] == ["MELIOR", BD]
    assert skipped[0] == {"account": "MELIOR", "error": "401", "list_date": None, "domains": 0, "mongo_ok": False}

    class Down(FakeCol):
        def find(self, *a, **k):
            raise RuntimeError("connection reset")
    skipped = bm._fill_from_last_lists({}, {}, [{"account": BD, "error": "401"}], bm.Store(Down()))
    assert skipped[0]["mongo_ok"] is False
    found = bm.analyse([_result("m1.com")], {}, {}, TODAY)
    head, thread = bm.summary(found, TODAY, skipped=skipped, accounts=2)
    assert head[1] == "Read 1 of 2 Smartlead accounts · skipped BettrData: Mongo unreadable, so its domains were not checked"
    assert "• BettrData: 401. Mongo unreadable, so its domains were not checked." in "\n".join(thread)
    assert "no saved list" not in "\n".join(head + thread)


def test_last_list_skips_weeks_the_account_was_not_read():
    """BL-R2: a fallback week's rows are not the account's list; a shared domain read from another account isn't either."""
    col = FakeCol([
        _row("b1.com", "2026-11-02", clients=[BD], source="smartlead", read_from=[BD]),
        _row("s1.com", "2026-11-02", clients=[BD, PL], source="smartlead", read_from=[BD, PL]),
        _row("b1.com", "2026-11-09", clients=[BD], source="last list", read_from=[]),
        _row("s1.com", "2026-11-09", clients=[BD, PL], source="smartlead", read_from=[PL]),
        _row("b9.com", "2026-11-09", clients=[BD], source="last list"),            # no read_from: source decides
    ])
    when, rows = bm.Store(col).last_list(BD)
    assert when == "2026-11-02" and [r["domain"] for r in rows] == ["b1.com", "s1.com"]
    assert bm.Store(col).last_list(PL)[0] == "2026-11-09"


# ── history: new vs ongoing, first seen ──────────────────────────────────────

def test_listing_age_new_ongoing_and_first_seen():
    dbl = "Spamhaus DBL (127.0.1.2)"
    rows = [_row("x.com", "2026-10-19", [dbl]), _row("x.com", "2026-10-26", [dbl]),
            _row("x.com", "2026-11-02", [dbl])]
    assert bm.listing_age(rows, bm.DBL, TODAY) == {"new": False, "since": "2026-10-19", "clean_on": None}
    rows = [_row("x.com", "2026-10-19", [dbl]), _row("x.com", "2026-10-26", []), _row("x.com", "2026-11-02", [])]
    assert bm.listing_age(rows, bm.DBL, TODAY) == {"new": True, "since": TODAY, "clean_on": "2026-11-02"}
    assert bm.listing_age([], bm.DBL, TODAY) == {"new": True, "since": TODAY, "clean_on": None}


def test_listing_age_skips_unknown_weeks_and_by_hand_rows_for_new():
    dbl = "Spamhaus DBL (127.0.1.2)"
    rows = [_row("x.com", "2026-10-26", []),
            _row("x.com", "2026-11-02", [], inconclusive=["Spamhaus DBL"]),     # unknown week: look past it
            _row("x.com", "2026-11-07", [dbl], clients=["adhoc"])]               # seen by hand, never reported
    assert bm.listing_age(rows, bm.DBL, TODAY) == {"new": True, "since": "2026-11-07", "clean_on": "2026-10-26"}
    rows.append(_row("x.com", TODAY, []))                                       # today's own row is ignored
    assert bm.listing_age(rows, bm.DBL, TODAY)["new"] is True


# ── the summary (BLOCKLIST-4/8/9) ────────────────────────────────────────────

def _headline_ok(head):
    assert 1 <= len(head) <= 4
    assert not any("@" in line for line in head)


def _inv(domain, clients, n, account=PL, stale=False):
    e = bm._entry()
    e["accounts"], e["clients"], e["stale"] = {account}, set(clients), stale
    e["emails"] = {} if stale else {f"u{i}@{domain}": account for i in range(n)}
    return e


def test_clean_week_still_posts_one_line():
    results = [_result(d) for d in ("a.com", "b.com", "c.com")]
    found = bm.analyse(results, {}, {}, TODAY)
    head, thread = bm.summary(found, TODAY, accounts=2)
    assert head == ["✅ Blocklists 9 Nov: checked 3 of 3 domains · Spamhaus DBL 0 (0 new) · URIBL 0 · "
                    "SURBL new 0 · unknown 0"]
    assert thread == []


def test_hits_name_client_inboxes_sending_and_age_live_first():
    dbl, surbl = "Spamhaus DBL (127.0.1.2)", "SURBL (ABUSE)"
    inv = {"old1.com": _inv("old1.com", ["Melior"], 3), "live1.com": _inv("live1.com", ["Melior"], 25),
           "pl1.com": _inv("pl1.com", ["Precise Leads"], 2), "bd1x.com": _inv("bd1x.com", ["Bettrdata"], 4, BD),
           "bd2.com": _inv("bd2.com", ["Bettrdata"], 4, BD), "ok1.com": _inv("ok1.com", ["Melior"], 2)}
    results = [_result("old1.com", [dbl]), _result("live1.com", [dbl, surbl]), _result("pl1.com", ["URIBL (127.0.0.2)"]),
               _result("bd1x.com", [surbl]), _result("bd2.com", [surbl]), _result("ok1.com")]
    history = {"old1.com": [_row("old1.com", "2026-10-19", [dbl]), _row("old1.com", "2026-11-02", [dbl])],
               "live1.com": [_row("live1.com", "2026-11-02", [surbl])],
               "pl1.com": [_row("pl1.com", "2026-11-02", [])],
               "bd1x.com": [_row("bd1x.com", "2026-09-07", [surbl], clients=[BD]),
                          _row("bd1x.com", "2026-11-02", [surbl], clients=[BD])],
               "bd2.com": [_row("bd2.com", "2026-11-02", [], clients=[BD])]}
    found = bm.analyse(results, inv, history, TODAY)
    att = {PL: {**{f"u{i}@live1.com": {"campaigns": ["Roundtable", "DT Melior #1"], "sending": ["Roundtable"]}
                   for i in range(25)},
                "u0@pl1.com": {"campaigns": ["Agencies"], "sending": []}},
           BD: {}}
    for h in found["hits"] + found["surbl"]:
        h.update(bm._status(inv[h["domain"]], att))
    head, thread = bm.summary(found, TODAY, accounts=2)
    _headline_ok(head)
    assert head[0] == ("🚨 Blocklists 9 Nov: checked 6 of 6 domains · Spamhaus DBL 2 (1 new) · URIBL 1 · "
                       "SURBL new 1 · unknown 0")
    assert head[1] == "Listed and still sending: live1.com (Melior)"
    hits = thread[0].split("\n")
    assert hits[0] == "On Spamhaus DBL or URIBL (3), live senders first:"
    assert hits[1] == ("• *live1.com* · Melior · Spamhaus DBL · 25 inboxes · sending in 1 live campaign · "
                       "new (clean on 2 Nov)")
    assert hits[2] == "• *pl1.com* · Precise Leads · URIBL · 2 inboxes · in 1 live campaign, nothing sent in 7 days · " \
                      "new (clean on 2 Nov)"
    assert hits[3] == "• *old1.com* · Melior · Spamhaus DBL · 3 inboxes · in no live campaign · ongoing, first seen 19 Oct"
    body = "\n".join(thread)
    assert body.count("decide with the client lead") == 1 and "decide with the client lead" not in "\n".join(head)
    assert "removal request" in body and "file a delisting request" not in body          # no blind removal
    assert "expire" in body
    # SURBL: only the domain new to it this week; the long-running one stays out
    assert "bd2.com" in body and "*bd1x.com*" not in body
    assert "New on SURBL since the last check (1)" in body
    assert "ok1.com" not in body


def test_unknown_is_never_clean():
    results = [_result("a.com", inconclusive=["Spamhaus DBL"]), _result("b.com", inconclusive=["Spamhaus DBL"]),
               _result("c.com", errors=["URIBL"])]
    found = bm.analyse(results, {}, {}, TODAY)
    head, thread = bm.summary(found, TODAY, accounts=2)
    assert head[0].startswith("⚠️ Blocklists 9 Nov: checked 0 of 3 domains") and head[0].endswith("unknown 3")
    body = "\n".join(thread)
    assert "Unknown (3): a list could not be read fully for these, so they don't count as clean:" in body
    assert "Spamhaus DBL: its test domain was not seen" in body
    assert "• c.com: URIBL lookup failed" in body


def test_hits_only_zone_says_why_it_is_unknown():
    results = [_result("a.com", ["Spamhaus DBL (127.0.1.2)"]), _result("b.com", inconclusive=["Spamhaus DBL"])]
    found = bm.analyse(results, {}, {}, TODAY, modes={"Spamhaus DBL": "hits_only", "URIBL": "auth"})
    head, thread = bm.summary(found, TODAY, accounts=2)
    assert "checked 1 of 2 domains · Spamhaus DBL 1 (1 new)" in head[0] and head[0].endswith("unknown 1")
    body = "\n".join(thread)
    assert ("• Spamhaus DBL: its own name server could not be used, and Google DNS misses some of its listings, "
            "so a domain not seen listed there is unknown on it, not clean") in body
    assert "test domain was not seen" not in body


def test_skipped_account_named_in_headline():
    inv = {"m1.com": _inv("m1.com", ["Melior"], 2), "b1.com": _inv("b1.com", ["Bettrdata"], 0, BD, stale=True)}
    found = bm.analyse([_result("m1.com"), _result("b1.com", ["Spamhaus DBL (127.0.1.2)"])], inv, {}, TODAY)
    skipped = [{"account": BD, "error": "401 plan expired", "list_date": "2026-11-02", "domains": 1}]
    head, thread = bm.summary(found, TODAY, skipped=skipped, accounts=2)
    _headline_ok(head)
    assert head[1] == "Read 1 of 2 Smartlead accounts · skipped BettrData: its 1 domain came from the 2 Nov check"
    assert "• *b1.com* · Bettrdata · Spamhaus DBL · sending unknown · new (first check)" in thread[0]
    assert ("• BettrData: 401 plan expired. Its 1 domain from the 2 Nov check was checked; inbox counts, "
            "sending and any domain added since are missing.") in "\n".join(thread)
    skipped = [{"account": BD, "error": "401", "list_date": None, "domains": 0}]
    head, thread = bm.summary(bm.analyse([_result("m1.com")], inv, {}, TODAY), TODAY, skipped=skipped, accounts=2)
    assert head[0].startswith("⚠️") and "no saved list, so its domains were not checked" in head[1]
    assert "• BettrData: 401. No saved list, so its domains were not checked." in "\n".join(thread)


def test_no_history_says_so_instead_of_guessing():
    found = bm.analyse([_result("a.com", ["Spamhaus DBL (127.0.1.2)", "SURBL (ABUSE)"])], {}, None, TODAY)
    head, thread = bm.summary(found, TODAY, accounts=1)
    assert "Spamhaus DBL 1 (new unknown)" in head[0] and "SURBL new unknown" in head[0]
    assert any("Mongo history could not be read" in t for t in thread)


def test_status_ignores_test_campaigns_and_marks_unread_accounts():
    e = _inv("x.com", ["Melior"], 2)
    assert bm._status(e, {PL: {"u0@x.com": {"campaigns": ["DT Melior #2"], "sending": ["DT Melior #2"]}}}) == \
        {"status": "none", "campaigns": 0, "sending": 0}
    assert bm._status(e, {PL: None})["status"] == "unknown"
    assert bm._status(_inv("x.com", [], 0, stale=True), {})["status"] == "unknown"
    assert bm._status(bm._entry(), {})["status"] is None                     # by hand: not shown


# ── the whole run ────────────────────────────────────────────────────────────

class _Run:
    """Patch every outside call; record what was posted and saved."""

    def __init__(self, monkeypatch, *, inboxes, results, col=None, post_ok=True, att=None):
        self.col = col if col is not None else FakeCol()
        self.posts, self.attach_calls = [], []
        monkeypatch.setattr(bm, "discover_accounts", lambda: list(ACCOUNTS))
        monkeypatch.setattr(bm, "_list_inboxes", _lister(inboxes))
        monkeypatch.setattr(bm, "_open_store", lambda: bm.Store(self.col) if self.col is not False else None)

        async def check_all(domains):
            return [dict(results.get(d) or _result(d), domain=d) for d in domains], {}
        monkeypatch.setattr(bm, "_check_all", check_all)

        def attach(key):
            self.attach_calls.append(key)
            return att or {}
        monkeypatch.setattr(bm, "_attachments", attach)

        def post(head, thread):
            self.posts.append((head, thread))
            return post_ok
        monkeypatch.setattr(bm, "_post", post)


def test_weekly_run_clean_posts_saves_and_exits_0(monkeypatch, capsys):
    run = _Run(monkeypatch, inboxes={PL: [_inbox("a@m1.com", 12256), _inbox("b@p1.com")], BD: [_inbox("c@b1.com")]},
               results={})
    assert asyncio.run(bm.main([])) == 0
    assert len(run.posts) == 1 and run.posts[0][0][0].startswith("✅ Blocklists")
    assert run.attach_calls == []                     # nothing listed: no campaign reads
    saved = {r["domain"]: r for r in run.col.rows}
    assert saved["m1.com"]["clients"] == [PL] and saved["m1.com"]["client_names"] == ["Melior"]
    assert saved["b1.com"]["clients"] == [BD] and saved["b1.com"]["inboxes"] == 1
    assert saved["m1.com"]["unknown"] is False and saved["m1.com"]["run"] == "weekly"
    assert saved["m1.com"]["source"] == "smartlead" and saved["m1.com"]["read_from"] == [PL]
    assert "checked 3 of 3 domains" in capsys.readouterr().out


def test_weekly_run_with_unreadable_account_still_checks_and_exits_1(monkeypatch):
    col = FakeCol([_row("b1.com", "2026-11-02", clients=[BD], client_names=["Bettrdata"]),
                   _row("b2.com", "2026-11-02", clients=[BD], client_names=["Bettrdata"])])
    run = _Run(monkeypatch, inboxes={PL: [_inbox("a@m1.com", 12256)], BD: RuntimeError("401 plan expired")},
               results={"b2.com": _result("b2.com", ["Spamhaus DBL (127.0.1.2)"])}, col=col)
    assert asyncio.run(bm.main([])) == 1
    head, thread = run.posts[0]
    assert "checked 3 of 3 domains" in head[0] and "Spamhaus DBL 1 (1 new)" in head[0]
    assert "skipped BettrData" in head[1]
    assert run.attach_calls == []                     # the listed domain's account can't be read: sending unknown
    assert "sending unknown" in thread[0]
    b2 = next(r for r in col.rows if r["domain"] == "b2.com" and r["date"] != "2026-11-02")
    assert b2["source"] == "last list" and b2["clients"] == [BD] and b2["inboxes"] is None


class _Today:
    def __init__(self, iso):
        self.iso = iso

    def today(self):
        return _date.fromisoformat(self.iso)


def _week(monkeypatch, iso, col, inboxes):
    monkeypatch.setattr(bm, "date", _Today(iso))
    run = _Run(monkeypatch, inboxes=inboxes, results={}, col=col)
    return asyncio.run(bm.main([])), run.posts[0]


@pytest.mark.parametrize("bd_later", [RuntimeError("401 plan expired"), []], ids=["unreadable", "no inboxes"])
def test_two_skipped_weeks_still_name_the_last_real_list(monkeypatch, bd_later):
    """BL-R2: on 16 Nov the post said 'from the 9 Nov check' (last week's fallback), hiding domains added after 2 Nov."""
    col = FakeCol()
    pl = [_inbox("a@m1.com", 12256), _inbox("s@s1.com")]
    code, _ = _week(monkeypatch, "2026-11-02", col,
                    {PL: pl, BD: [_inbox("c@b1.com"), _inbox("d@b2.com"), _inbox("e@s1.com")]})
    assert code == 0
    for iso in ("2026-11-09", "2026-11-16"):
        code, (head, thread) = _week(monkeypatch, iso, col, {PL: pl, BD: bd_later})
        assert code == 1
        assert head[0].startswith("⚠️ Blocklists") and "checked 4 of 4 domains" in head[0]
        assert head[1] == "Read 1 of 2 Smartlead accounts · skipped BettrData: its 3 domains came from the 2 Nov check"
        assert "Its 3 domains from the 2 Nov check were checked" in "\n".join(thread)
    s1 = next(r for r in col.rows if r["domain"] == "s1.com" and r["date"] == "2026-11-09")
    assert s1["clients"] == [BD, PL] and s1["read_from"] == [PL] and s1["source"] == "smartlead"
    b1 = next(r for r in col.rows if r["domain"] == "b1.com" and r["date"] == "2026-11-16")
    assert b1["source"] == "last list" and b1["read_from"] == [] and b1["clients"] == [BD]


def test_weekly_run_reads_campaigns_only_for_listed_domains_account(monkeypatch):
    run = _Run(monkeypatch, inboxes={PL: [_inbox("a@m1.com", 12256)], BD: [_inbox("c@b1.com")]},
               results={"m1.com": _result("m1.com", ["Spamhaus DBL (127.0.1.2)"])},
               att={"a@m1.com": {"campaigns": ["Roundtable"], "sending": ["Roundtable"]}})
    assert asyncio.run(bm.main([])) == 0
    assert run.attach_calls == ["k1"]
    head, thread = run.posts[0]
    assert head[0].startswith("🚨") and head[1] == "Listed and still sending: m1.com (Melior)"
    assert "• *m1.com* · Melior · Spamhaus DBL · 1 inbox · sending in 1 live campaign · new (first check)" in thread[0]


def test_weekly_run_failed_post_or_unknown_exits_1(monkeypatch):
    _Run(monkeypatch, inboxes={PL: [_inbox("a@m1.com")], BD: []}, results={}, post_ok=False)
    assert asyncio.run(bm.main([])) == 1
    _Run(monkeypatch, inboxes={PL: [_inbox("a@m1.com")], BD: []},
         results={"m1.com": _result("m1.com", errors=["URIBL"])})
    assert asyncio.run(bm.main([])) == 1


def test_dry_run_saves_and_posts_nothing(monkeypatch):
    run = _Run(monkeypatch, inboxes={PL: [_inbox("a@m1.com")], BD: []}, results={})
    assert asyncio.run(bm.main(["--dry-run"])) == 0
    assert run.posts == [] and run.col.rows == []


def test_by_hand_prints_only_and_takes_several_domains(monkeypatch, capsys):
    run = _Run(monkeypatch, inboxes={}, results={"x.com": _result("x.com", ["SURBL (ABUSE)"])})
    assert asyncio.run(bm.main(["X.com", "https://y.com/page", "x.com"])) == 0
    assert run.posts == [] and run.col.rows == []
    out = capsys.readouterr().out
    assert "Blocklists" in out and "(by hand): checked 2 of 2 domains" in out
    assert "SURBL 1 (1 new)" in out and "*x.com*" in out           # by hand shows every SURBL listing


def test_by_hand_save_never_overwrites_a_weekly_rows_clients(monkeypatch):
    from datetime import date
    today = date.today().isoformat()
    col = FakeCol([_row("x.com", today, clients=[PL], client_names=["Melior"], run="weekly", inboxes=5)])
    run = _Run(monkeypatch, inboxes={}, col=col,
               results={"x.com": _result("x.com", ["Spamhaus DBL (127.0.1.2)"])})
    assert asyncio.run(bm.main(["x.com", "z.com", "--save", "--post"])) == 0
    assert len(run.posts) == 1 and "(by hand)" in run.posts[0][0][0]
    x = next(r for r in col.rows if r["domain"] == "x.com")
    assert x["clients"] == [PL] and x["client_names"] == ["Melior"] and x["inboxes"] == 5 and x["run"] == "weekly"
    assert x["listed_on"] == ["Spamhaus DBL (127.0.1.2)"]
    z = next(r for r in col.rows if r["domain"] == "z.com")
    assert z["clients"] == ["adhoc"] and z["run"] == "adhoc" and z["unknown"] is False


def test_post_goes_to_infra_channel_unless_overridden(monkeypatch):
    sent = []
    import smartlead.notify as notify
    monkeypatch.setattr(notify, "post_with_thread", lambda token, ch, head, thread: sent.append(ch) or "1.0")
    monkeypatch.setenv("SLACK_BOT_TOKEN", "xoxb-test")
    monkeypatch.delenv("BLACKLIST_CHANNEL", raising=False)
    assert bm._post(["h"], []) is True and sent == [INFRA_CHANNEL]
    monkeypatch.setenv("BLACKLIST_CHANNEL", "C123")
    assert bm._post(["h"], []) is True and sent[-1] == "C123"
    monkeypatch.delenv("SLACK_BOT_TOKEN")
    assert bm._post(["h"], []) is False
