"""Inbox intake: set every new Smartlead inbox up once, prove it, then only watch.

Spec: docs/superpowers/specs/2026-10-13-inbox-intake-design.md. Per current-
client Smartlead account: inboxes created on/after INTAKE_SINCE and not done
in the ledger get the type's standard (inbox_standard), the client's signature
and Smartlead client, and tags; then each is read back. Only a matching
read-back counts as done (ledger + Bot-Setup tag). A ScaledMail order or a
Zapmail job waits until all its inboxes are in Smartlead. approve=False
writes nothing.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from smartlead import inbox_standard as st

COLLECTION = "inbox_intake"
MAX_TRIES = 3
TAG_BATCH = 25
CURRENT_ACCOUNTS = ("PRECISE_LEADS", "BETTRDATA")
CLIENT_TAG = {"Precise Leads": "PreciseLeads", "Melior": "Melior", "Bettrdata": "BettrData"}
VENDOR_TAG = {("scaledmail", "google"): "ScaledMail-Google", ("scaledmail", "outlook"): "ScaledMail-Microsoft",
              ("scaledmail", "outlook_dense"): "ScaledMail-Microsoft", ("scaledmail", "smtp"): "ScaledMail-SMTP",
              ("zapmail", "google"): "Zapmail-Google", ("zapmail", "outlook"): "Zapmail-Microsoft"}
CURRENT_CLIENTS = ("Precise Leads", "Melior", "Bettrdata")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class IntakeStore:
    """One Mongo document per inbox: tries, ok, the values written, history."""

    def __init__(self, collection=None) -> None:
        if collection is None:
            from smartlead.zapmail_asset_sync import _db
            db = _db()
            collection = db[COLLECTION] if db is not None else None
        if collection is None:
            raise RuntimeError("intake ledger unavailable (Mongo); nothing was written")
        self.col = collection

    def _doc(self, email: str) -> dict:
        return self.col.find_one({"email": email}, {"_id": 0}) or {}

    def done(self, email: str) -> bool:
        return bool(self._doc(email).get("ok"))

    def tries(self, email: str) -> int:
        return int(self._doc(email).get("tries") or 0)

    def record(self, email: str, ok: bool, **fields) -> None:
        self.col.update_one({"email": email},
                            {"$set": {"ok": ok, "updated_at": _now(), **fields}, "$inc": {"tries": 1},
                             "$push": {"history": {"at": _now(), "ok": ok, **fields}}}, upsert=True)


def deliveries(plans: list[dict], jobs: list[dict]) -> dict[str, dict]:
    """domain -> the ScaledMail order or Zapmail job that brought it."""
    out: dict[str, dict] = {}
    for p in plans:
        if p.get("status") == "placed":
            d = {"source": "scaledmail", "id": p["plan_id"], "client": p.get("client"),
                 "domains": {x.lower() for x in p.get("domains") or []}, "expected": int(p.get("mailboxes") or 0)}
            out.update({dom: d for dom in d["domains"]})
    for j in jobs:
        if j.get("status") not in ("cancelled",) and j.get("inboxes"):
            d = {"source": "zapmail", "id": j["job_id"], "client": j.get("client"),
                 "domains": {x.lower() for x in j.get("domains") or []}, "expected": len(j["inboxes"])}
            out.update({dom: d for dom in d["domains"]})
    return out


def _created(a: dict) -> date | None:
    v = a.get("created_at")
    return date.fromisoformat(str(v)[:10]) if v else None


def _has_tag(a: dict, name: str) -> bool:
    return any((t.get("tag_name") or t.get("name")) == name for t in a.get("tags") or [])


def _client(a: dict, account_name: str, d: dict | None, tracker_row: dict, melior_id) -> str | None:
    if d and d.get("client") in CURRENT_CLIENTS:
        return d["client"]
    if account_name.upper() == "BETTRDATA":
        return "Bettrdata"
    if melior_id is not None and a.get("client_id") == melior_id:
        return "Melior"
    c = (tracker_row or {}).get("client")
    return c if c in CURRENT_CLIENTS else None


def _vendor(d: dict | None, tracker_row: dict) -> str:
    if d:
        return d["source"]
    p = str((tracker_row or {}).get("provider") or "").lower().replace(" ", "")
    return "scaledmail" if "scaledmail" in p else "zapmail" if "zapmail" in p else "unknown"


async def run_account(sl, account_name: str, *, approve: bool, since: date, today: date,
                      by_domain: dict, tracker: dict, store, profile) -> dict:
    from smartlead.inbox_setup import plan_changes
    from smartlead.notify import redact
    out: dict = {"set_up": [], "would": [], "waiting": [], "problems": [], "complete": []}
    accounts = await sl.list_email_accounts()
    tags = {t["name"]: t["id"] for t in await sl.list_tags()}
    per_domain: dict[str, int] = {}
    for a in accounts:
        dom = str(a.get("from_email", "")).split("@")[-1].lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
    melior_id = (profile("Melior") or {}).get("smartlead_client_id")
    new = [a for a in accounts
           if (_created(a) or date.min) >= since and not _has_tag(a, st.SETUP_TAG)
           and not store.done(str(a["from_email"]).lower())]
    seen_waiting: set = set()
    ok_by_tags: dict[tuple, list[int]] = {}
    attempted: dict[str, list[bool]] = {}
    for a in sorted(new, key=lambda x: x.get("id") or 0):
        email = str(a["from_email"]).lower()
        dom = email.split("@")[-1]
        d = by_domain.get(dom)
        if d:
            found = sum(per_domain.get(x, 0) for x in d["domains"])
            if found < d["expected"]:
                if d["id"] not in seen_waiting:
                    seen_waiting.add(d["id"])
                    what = "ScaledMail order" if d["source"] == "scaledmail" else "Zapmail job"
                    out["waiting"].append(f"⏳ {found} of {d['expected']} inboxes in Smartlead for {what} {d['id']} "
                                          f"({', '.join(sorted(d['domains']))}), waiting for the rest")
                continue
        if store.tries(email) >= MAX_TRIES:
            out["problems"].append(f"`{email}`: stuck after {MAX_TRIES} tries, set it up by hand in Smartlead")
            continue
        client = _client(a, account_name, d, tracker.get(dom), melior_id)
        if not client:
            out["problems"].append(f"`{email}`: no client known for {dom} (no order, job or tracker row), not set up")
            continue
        kind = st.inbox_kind(a, per_domain.get(dom, 1))
        vendor = _vendor(d, tracker.get(dom))
        written = {**plan_changes(a, profile(client), email=email, set_signature=True).fields,
                   **st.account_fields(kind, vendor)}
        row = {"email": email, "client": client, "kind": kind, "vendor": vendor, "domain": dom,
               "ready_on": st.ready_date(a, kind)}
        if not approve:
            out["would"].append(row)
            continue
        back = a
        try:
            await sl.update_email_account(str(a["id"]), written)
            await sl.set_warmup(str(a["id"]), **st.warmup_args(kind))
            back = await sl.get_email_account(str(a["id"]))
            miss = st.readback_mismatches(back, kind, vendor, written)
            row["ready_on"] = st.ready_date(back, kind)
        except Exception as exc:  # noqa: BLE001 - one inbox failing must not stop the rest
            miss = [f"Smartlead error: {redact(str(exc))[:160]}"]
        store.record(email, ok=not miss, kind=kind, vendor=vendor, client=client, account=account_name,
                     written=written, mismatches=miss, warmup_started=str(st.warmup_started(back) or ""))
        if d:
            attempted.setdefault(d["id"], []).append(not miss)
        if miss:
            out["problems"].append(f"`{email}`: {'; '.join(miss)} (try {store.tries(email)} of {MAX_TRIES})")
            continue
        out["set_up"].append(row)
        names = (st.SETUP_TAG, CLIENT_TAG.get(client), VENDOR_TAG.get((vendor, kind)))
        ok_by_tags.setdefault(tuple(sorted(tags[n] for n in names if n and n in tags)), []).append(int(a["id"]))
    for tag_ids, ids in ok_by_tags.items():
        for i in range(0, len(ids), TAG_BATCH):
            if tag_ids:
                await sl.assign_tags(ids[i:i + TAG_BATCH], list(tag_ids))
    for name in {VENDOR_TAG.get((r["vendor"], r["kind"])) for r in out["set_up"]} | {st.SETUP_TAG}:
        if name and name not in tags:
            out["problems"].append(f"Smartlead has no tag `{name}` in {account_name}: create it so new inboxes get it")
    for did, results in attempted.items():
        d = next(v for v in by_domain.values() if v["id"] == did)
        if len(results) >= d["expected"] and all(results):
            out["complete"].append(did)
    return out
