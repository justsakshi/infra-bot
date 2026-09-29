"""Unit tests for the Zapmail integration's pure logic (no network, no spend).

Covers parsing, batching, identity generation, and account mapping — the parts
that decide correctness before any live call. Nothing here touches the API.
"""
from __future__ import annotations

import os

from smartlead.domain_availability import _parse_bulk_rows
from smartlead.domain_batch import plan_batches
from smartlead.domain_export import _extract_export_id
from smartlead.domain_lifecycle import generate_identities, required_nameservers
from smartlead.zapmail import REGISTRABLE_TLDS, _pruned
from smartlead.zapmail_accounts import client_account_map


# ── availability parsing ─────────────────────────────────────────────────────

def test_parse_bulk_rows_reads_status_and_price():
    data = {"domains": [
        {"domainName": "a.com", "status": "AVAILABLE",
         "domainPrice": "$12.99", "renewPrice": "$18.99", "isPremiumDomain": False},
        {"domainName": "b.com", "status": "UNAVAILABLE",
         "domainPrice": "", "renewPrice": ""},
    ]}
    out = _parse_bulk_rows(data)
    assert out["a.com"] == (True, 12.99)
    assert out["b.com"] == (False, None)


def test_parse_bulk_rows_unknown_status_is_none():
    out = _parse_bulk_rows({"domains": [{"domainName": "c.com", "status": ""}]})
    assert out["c.com"] == (None, None)


def test_registrable_tlds_excludes_io_co():
    assert "com" in REGISTRABLE_TLDS
    assert "io" not in REGISTRABLE_TLDS
    assert "co" not in REGISTRABLE_TLDS


# ── batching ─────────────────────────────────────────────────────────────────

def test_plan_batches_groups_and_offsets():
    domains = [f"d{i}.com" for i in range(7)]
    batches = plan_batches(domains, registrars=("Zapmail",),
                           per_batch=3, day_gap=2)
    assert [b.day_offset for b in batches] == [0, 2, 4]
    assert [len(b.domains) for b in batches] == [3, 3, 1]
    assert all(b.registrar == "Zapmail" for b in batches)


def test_batch_id_is_stable_regardless_of_order():
    a = plan_batches(["x.com", "y.com"], registrars=("Zapmail",), per_batch=2)
    b = plan_batches(["y.com", "x.com"], registrars=("Zapmail",), per_batch=2)
    assert a[0].batch_id == b[0].batch_id


# ── mailbox identities ───────────────────────────────────────────────────────

def test_generate_identities_count_and_validity():
    ids = generate_identities("example.com", 2)
    assert len(ids) == 2
    for i in ids:
        u = i["mailboxUsername"]
        assert u.isalpha() and u.islower()
        assert i["domainName"] == "example.com"
        assert i["firstName"] and i["lastName"]


def test_generate_identities_deterministic():
    assert generate_identities("example.com", 3) == generate_identities("example.com", 3)


# ── export id extraction ─────────────────────────────────────────────────────

def test_extract_export_id_variants():
    assert _extract_export_id({"data": {"exportId": 5}}) == 5
    assert _extract_export_id({"export_id": 7}) == 7
    assert _extract_export_id({"data": {"id": 9}}) == 9
    assert _extract_export_id({"data": None}) is None


# ── nameservers ──────────────────────────────────────────────────────────────

def test_required_nameservers_present():
    ns = required_nameservers()
    assert len(ns) == 4
    assert "pns61.cloudns.net" in ns


# ── account mapping ──────────────────────────────────────────────────────────

def test_client_account_map_env_override(monkeypatch):
    monkeypatch.setenv("ZAPMAIL_CLIENT_ACCOUNTS", "Foo=Bar, Hello World=Baz")
    m = client_account_map()
    assert m["foo"] == "bar"
    assert m["hello world"] == "baz"


def test_pruned_drops_none():
    assert _pruned({"a": 1, "b": None, "c": ""}) == {"a": 1, "c": ""}