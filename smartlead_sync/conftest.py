"""Shared test setup. The daily digest writes to Mongo, and several modules load the real .env when imported, so
every test runs with the digest off unless it turns it on itself (16 Nov 2026: a test wrote a real digest doc)."""
import pytest


@pytest.fixture(autouse=True)
def _digest_off(monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "off")
