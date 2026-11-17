"""Shared test setup. The daily digest writes to Mongo, and several modules load the real .env when imported, so
every test runs with the digest off unless it turns it on itself (16 Nov 2026: a test wrote a real digest doc)."""
import pytest


@pytest.fixture(autouse=True)
def _digest_off(monkeypatch):
    monkeypatch.setenv("DIGEST_MODE", "off")


@pytest.fixture(autouse=True)
def _no_live_mongo(monkeypatch):
    """Modules load the real .env on import, so a test that reached Mongo would read or write the live database.
    Every test runs with MONGO_URI blank (Mongo-backed code then sees no database); tests use fakes (17 Nov 2026)."""
    monkeypatch.setenv("MONGO_URI", "")
