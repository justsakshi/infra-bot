"""Cross-check between Zapmail and the /infra asset tracker.

Zapmail knows the authoritative domain list and expiry dates; the /infra
asset tracker is where the team records purchases and renewals by hand. The
two drift — a domain bought in the Zapmail UI but never ``/infra add``-ed, a
tracked domain that is not on any Zapmail account, or two different expiry
dates. This module compares the tracker against EVERY domain on every Zapmail
account (both providers), all READ-ONLY (Zapmail fetch + Mongo read).

    * ``only_in_zapmail``    — live on a Zapmail account, absent from the
                               tracker. Likely bought via Zapmail, never recorded.
    * ``expired_in_zapmail`` — listed by Zapmail but past its expiry (lapsed),
                               absent from the tracker. Usually nothing to do.
    * ``only_in_tracker``    — tracked, but on no connected Zapmail account:
                               another provider, a dropped domain, or an
                               account whose key is not configured.
    * ``no_tracker_expiry``  — in both, but the tracker has no expiry date.
    * ``date_mismatch``      — both have dates and they differ by >1 day.

If any Zapmail account could not be read, ``zapmail_errors`` says so and
``only_in_tracker`` must not be trusted (it would include that account's
domains). Nothing here spends money or writes back to either system.
"""

from __future__ import annotations

from datetime import datetime, timezone

from smartlead.zapmail_fleet import all_domains

try:
    from pymongo import MongoClient
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB
from smartlead.domain_estate import ASSETS_COLLECTION


def _mongo_db():
    uri = __import__("os").getenv("MONGO_URI", "")
    if not uri or MongoClient is None:
        return None
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        default_db = client.get_default_database()
        return default_db if default_db is not None else client[HEALTH_HISTORY_DB]
    except Exception:  # noqa: BLE001
        return None


def _tracker_expiry_map() -> dict[str, str]:
    """``{domain: expiry_date}`` from the asset tracker's DOMAIN rows.

    ``expiryDate`` wins; when absent, purchaseDate + 365 days is used as the
    tracker's own renewal assumption (mirrors index.js `computeDaysLeft`).
    """
    db = _mongo_db()
    if db is None:
        return {}
    out: dict[str, str] = {}
    try:
        for row in db[ASSETS_COLLECTION].find(
            {"type": "DOMAIN"}, {"name": 1, "expiryDate": 1, "purchaseDate": 1},
        ):
            name = str(row.get("name", "")).strip().lower().split("//")[-1].split("/")[0]
            if not name or "." not in name:
                continue
            expiry = row.get("expiryDate")
            if not expiry and row.get("purchaseDate"):
                pd = row.get("purchaseDate")
                if hasattr(pd, "year"):
                    pd = datetime(pd.year, pd.month, pd.day).replace(
                        tzinfo=timezone.utc)
                from datetime import timedelta
                expiry = pd + timedelta(days=365)
            if not expiry:
                out[name] = ""  # tracked, but no expiry recorded
                continue
            date_str = expiry.strftime("%Y-%m-%d") if hasattr(expiry, "strftime") else str(expiry)[:10]
            out[name] = date_str
    except Exception:  # noqa: BLE001
        pass
    return out


def _days_apart(a: str, b: str) -> int:
    try:
        return abs((datetime.strptime(a, "%Y-%m-%d")
                    - datetime.strptime(b, "%Y-%m-%d")).days)
    except ValueError:
        return 999 if a != b else 0


async def renewals_cross_check() -> dict:
    """Three-way drift between all Zapmail domains and the asset tracker.

    Returns ``{only_in_zapmail, only_in_tracker, date_mismatch, zapmail_ok,
    tracker_ok, zapmail_errors, zapmail_domains, tracker_domains}``.
    """
    errors: list[str] = []
    zap_rows = await all_domains(errors)
    zap_map: dict[str, dict] = {r["domain"]: r for r in zap_rows}
    tracker_map = _tracker_expiry_map()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    only_zap: list[dict] = []
    expired_zap: list[dict] = []
    no_tracker_date: list[dict] = []
    date_mismatch: list[dict] = []
    from smartlead.zapmail_clients import infer_client
    for d, row in zap_map.items():
        # client=None marks a past client's domain: views leave those out
        # (they are being left to lapse, not tracked).
        item = {"domain": d, "expire_on": row["expire_on"], "account": row["account"],
                "client": infer_client(d, row["account"], None)}
        if d not in tracker_map:
            # A past expiry means the domain lapsed; Zapmail still lists it.
            (expired_zap if row["expire_on"] and row["expire_on"] < today
             else only_zap).append(item)
        elif not tracker_map[d]:
            no_tracker_date.append(item)
        elif row["expire_on"] and _days_apart(tracker_map[d], row["expire_on"]) > 1:
            # <=1 day apart is a UTC-vs-local rendering difference, not drift.
            date_mismatch.append({
                "domain": d,
                "zapmail_expire": row["expire_on"],
                "tracker_expire": tracker_map[d],
                "account": row["account"],
            })

    only_tracker: list[dict] = [
        {"domain": d, "tracker_expire": exp}
        for d, exp in tracker_map.items()
        if d not in zap_map
    ]

    return {
        "only_in_zapmail": sorted(only_zap, key=lambda x: x["expire_on"] or ""),
        "expired_in_zapmail": sorted(expired_zap, key=lambda x: x["expire_on"]),
        "only_in_tracker": sorted(only_tracker, key=lambda x: x["domain"]),
        "no_tracker_expiry": sorted(no_tracker_date, key=lambda x: x["domain"]),
        "date_mismatch": date_mismatch,
        "zapmail_ok": bool(zap_rows) and not errors,
        "tracker_ok": bool(tracker_map),
        "zapmail_errors": errors,
        "zapmail_domains": len(zap_map),
        "tracker_domains": len(tracker_map),
    }