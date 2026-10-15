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
POSTED_KEY = "__posted_problems__"   # ledger doc: problems already posted to the channel
REVIEW_KEY = "__volume_review__:"    # ledger doc per weekly volume review (its decisions and inputs)
PILOT_KEY = "__volume_pilot__"       # ledger doc: domains the volume review may change
CONTROL_KEY = "__volume_control__"   # ledger doc: comparison domains, kept flat
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

    def record(self, email: str, ok: bool, *, count_try: bool = True, **fields) -> None:
        """count_try=False for Smartlead outages and tag failures: only a
        read-back that differs uses up one of the MAX_TRIES."""
        upd = {"$set": {"ok": ok, "updated_at": _now(), **fields},
               "$push": {"history": {"at": _now(), "ok": ok, **fields}}}
        upd["$inc"] = {"tries": 1 if count_try else 0}
        self.col.update_one({"email": email}, upd, upsert=True)

    # Manveen's answer to "whose are these?" for a domain: a client name or "skip".
    def answer(self, domain: str) -> str | None:
        return self._doc("__answer__:" + domain.lower()).get("client")

    def set_answer(self, domain: str, client: str, by: str = "") -> None:
        self.col.update_one({"email": "__answer__:" + domain.lower()},
                            {"$set": {"client": client, "by": by, "at": _now()}}, upsert=True)

    def asked(self, domain: str) -> bool:
        return bool(self._doc("__asked__:" + domain.lower()))

    def mark_asked(self, domain: str) -> None:
        self.col.update_one({"email": "__asked__:" + domain.lower()}, {"$set": {"at": _now()}}, upsert=True)

    def client_of(self, email: str) -> str | None:
        return self._doc(email).get("client")

    def announced(self, key: str) -> bool:
        return bool(self._doc("__announced__:" + key))

    def mark_announced(self, key: str) -> None:
        self.col.update_one({"email": "__announced__:" + key}, {"$set": {"at": _now()}}, upsert=True)

    # Weekly volume review: each inbox's approved daily limit, and the reviews.
    def approved_limits(self) -> dict[str, tuple[int, str]]:
        """{email: (approved limit, approved on)}; drift compares against these."""
        return {d["email"]: (int(d["approved_limit"]), str(d.get("approved_on") or ""))
                for d in self.col.find({"approved_limit": {"$exists": True}}, {"_id": 0})}

    def approve_limit(self, email: str, limit: int, *, by: str, action: str, on: date) -> None:
        self.col.update_one({"email": email}, {
            "$set": {"approved_limit": int(limit), "approved_on": on.isoformat(), "updated_at": _now()},
            "$push": {"history": {"at": _now(), "ok": True, "approved_limit": int(limit), "by": by, "action": action}}},
            upsert=True)

    def save_review(self, review_id: str, rows: list[dict]) -> None:
        """Each week's decisions with their inputs: the record the rule is graded on."""
        self.col.update_one({"email": REVIEW_KEY + review_id}, {"$set": {"rows": rows, "created_at": _now()}},
                            upsert=True)

    def pilot_domains(self) -> set[str]:
        """Domains whose limits the weekly review changes ("*" = every judged domain)."""
        return set(self._doc(PILOT_KEY).get("domains") or [])

    def control_domains(self) -> set[str]:
        """Comparison domains: judged and reported like the pilot, never changed."""
        return set(self._doc(CONTROL_KEY).get("domains") or [])

    def set_control(self, domains: set[str], by: str = "") -> None:
        self.col.update_one({"email": CONTROL_KEY}, {"$set": {"domains": sorted({d.strip().lower() for d in domains}),
                                                              "by": by, "at": _now()}}, upsert=True)

    def set_pilot(self, domains: set[str], by: str = "") -> None:
        self.col.update_one({"email": PILOT_KEY}, {"$set": {"domains": sorted({d.strip().lower() for d in domains}),
                                                            "by": by, "at": _now()}}, upsert=True)

    def posted_problems(self) -> list[str]:
        return list(self._doc(POSTED_KEY).get("problems") or [])

    def save_posted_problems(self, problems: list[str]) -> None:
        self.col.update_one({"email": POSTED_KEY}, {"$set": {"problems": list(problems), "updated_at": _now()}},
                            upsert=True)


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
                      by_domain: dict, tracker: dict, store, profile, only: str | None = None) -> dict:
    """One Smartlead account. ``only`` (an inbox or a domain) limits the run,
    for trying a few inboxes before the rest."""
    from smartlead.inbox_setup import plan_changes
    from smartlead.notify import redact
    out: dict = {"set_up": [], "would": [], "waiting": [], "problems": [], "complete": [], "ready": [], "ask": []}
    accounts = await sl.list_email_accounts()
    tags = {t["name"]: t["id"] for t in await sl.list_tags()}
    email_of = lambda a: str(a.get("from_email", "")).lower()          # noqa: E731
    dom_of = lambda a: email_of(a).split("@")[-1]                       # noqa: E731
    fresh = lambda a: (_created(a) or date.min) >= since                # noqa: E731
    per_domain: dict[str, int] = {}
    fresh_on: dict[str, list[dict]] = {}
    for a in accounts:
        per_domain[dom_of(a)] = per_domain.get(dom_of(a), 0) + 1
        if fresh(a):
            fresh_on.setdefault(dom_of(a), []).append(a)
    melior_id = (profile("Melior") or {}).get("smartlead_client_id")
    approved = {e: lim for e, (lim, _) in store.approved_limits().items()}
    out["problems"] += _drift_sweep(accounts, per_domain, approved)
    # Set-up domains that just became ready (or reached their date still warming), with whose they are.
    owners: dict[str, str] = {}
    for a in accounts:
        if _has_tag(a, st.SETUP_TAG):
            dom = dom_of(a)
            owners.setdefault(dom, store.client_of(email_of(a))
                              or _client(a, account_name, by_domain.get(dom), tracker.get(dom), melior_id) or "")
    out["ready"] = ready_announcements(accounts, today, store, owners)
    new = [a for a in accounts if fresh(a) and not _has_tag(a, st.SETUP_TAG) and not store.done(email_of(a))]
    if only:
        new = [a for a in new if only.lower() in (email_of(a), dom_of(a))]
    no_setup_tag = bool(new) and st.SETUP_TAG not in tags
    if no_setup_tag:
        out["problems"].append(f"Smartlead has no tag `{st.SETUP_TAG}` in {account_name}: create it in Smartlead "
                               "(Email Accounts, tags), nothing was set up")
        if approve:
            return out
    seen_waiting: set = set()
    touched: set = set()
    written_ok: list[dict] = []
    for a in sorted(new, key=lambda x: x.get("id") or 0):
        email, dom = email_of(a), dom_of(a)
        d = by_domain.get(dom)
        if d:
            found = sum(len(fresh_on.get(x, [])) for x in d["domains"])     # this delivery's inboxes only
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
        answer = store.answer(dom)
        if answer == "skip":
            continue                                   # Manveen said these are not ours
        client = _client(a, account_name, d, tracker.get(dom), melior_id) or (
            answer if answer in CURRENT_CLIENTS else None)
        if not client:
            # Precise Leads' account holds Precise Leads and Melior: ask Manveen once per domain.
            if not store.asked(dom) and dom not in {x["domain"] for x in out["ask"]}:
                out["ask"].append({"domain": dom, "account": account_name, "kind": st.standard_for(
                    st.inbox_kind(a, per_domain.get(dom, 1)))["label"],
                    "inboxes": sum(1 for x in new if dom_of(x) == dom)})
            continue
        kind = st.inbox_kind(a, per_domain.get(dom, 1))
        vendor = _vendor(d, tracker.get(dom))
        written = {**plan_changes(a, profile(client), email=email, set_signature=True).fields,
                   **st.account_fields(kind, vendor)}
        w = a.get("warmup_details") or {}
        before = {"message_per_day": a.get("message_per_day"),
                  "warmup_per_day": w.get("max_email_per_day") or w.get("warmup_max_count"),
                  "reply_rate": w.get("reply_rate"), "tracking": a.get("custom_tracking_domain")}
        row = {"email": email, "client": client, "kind": kind, "vendor": vendor, "domain": dom,
               "ready_on": st.ready_date(a, kind)}
        if not approve:
            out["would"].append(row)
            continue
        if d:
            touched.add(d["id"])
        rec = dict(kind=kind, vendor=vendor, client=client, account=account_name, written=written, before=before)
        try:
            await sl.update_email_account(str(a["id"]), written)
            await sl.set_warmup(str(a["id"]), **st.warmup_args(kind))
            back = await sl.get_email_account(str(a["id"]))
        except Exception as exc:  # noqa: BLE001 - an outage: report, retry next run, no try used
            err = f"Smartlead error: {redact(str(exc))[:160]}"
            store.record(email, ok=False, count_try=False, mismatches=[err], **rec)
            out["problems"].append(f"`{email}`: {err} (will retry)")
            continue
        miss = st.readback_mismatches(back, kind, vendor, written)
        row["ready_on"] = st.ready_date(back, kind)
        rec["warmup_started"] = str(st.warmup_started(back) or "")
        if miss:
            store.record(email, ok=False, mismatches=miss, **rec)
            out["problems"].append(f"`{email}`: {'; '.join(miss)} (try {store.tries(email)} of {MAX_TRIES})")
            continue
        names = (st.SETUP_TAG, CLIENT_TAG.get(client), VENDOR_TAG.get((vendor, kind)))
        written_ok.append({"id": int(a["id"]), "row": row, "rec": rec,
                           "tag_ids": tuple(sorted(tags[n] for n in names if n and n in tags))})
    # The single-inbox read has no "Minimum time gap": confirm it from one list read.
    if written_ok:
        listed = {email_of(a): a for a in await sl.list_email_accounts()}
        kept = []
        for x in written_ok:
            email = x["row"]["email"]
            gap = st.gap_problem(listed.get(email) or {"minTimeToWaitInMins": None}, x["rec"]["kind"])
            if gap:
                store.record(email, ok=False, mismatches=[gap], **x["rec"])
                out["problems"].append(f"`{email}`: {gap} (try {store.tries(email)} of {MAX_TRIES})")
            else:
                kept.append(x)
        written_ok = kept
    # Tags last; an inbox counts as done only once its tags are on.
    untagged: set = set()
    groups: dict[tuple, list[int]] = {}
    for x in written_ok:
        groups.setdefault(x["tag_ids"], []).append(x["id"])
    for tag_ids, ids in groups.items():
        for i in range(0, len(ids), TAG_BATCH):
            batch = ids[i:i + TAG_BATCH]
            try:
                await sl.assign_tags(batch, list(tag_ids))
            except Exception as exc:  # noqa: BLE001
                untagged.update(batch)
                out["problems"].append(f"tags not added on {len(batch)} inbox(es) in {account_name} "
                                       f"({redact(str(exc))[:120]}); they will be retried")
    for x in written_ok:
        if x["id"] in untagged:
            store.record(x["row"]["email"], ok=False, count_try=False, mismatches=["tags not added"], **x["rec"])
            continue
        store.record(x["row"]["email"], ok=True, mismatches=[], **x["rec"])
        out["set_up"].append(x["row"])
    for name in sorted(({VENDOR_TAG.get((r["vendor"], r["kind"])) for r in out["set_up"]}
                        | {CLIENT_TAG.get(r["client"]) for r in out["set_up"]}) - {None}):
        if name not in tags:
            out["problems"].append(f"Smartlead has no tag `{name}` in {account_name}: create it so new inboxes get it")
    # An order is complete when every one of its new inboxes is done, whichever run did it.
    for did in sorted(touched):
        d = next(v for v in by_domain.values() if v["id"] == did)
        boxes = [a for dom in d["domains"] for a in fresh_on.get(dom, [])]
        if len(boxes) >= d["expected"] and all(_has_tag(a, st.SETUP_TAG) or store.done(email_of(a)) for a in boxes):
            out["complete"].append(did)
    return out


def _drift_sweep(accounts: list[dict], per_domain: dict[str, int], approved: dict[str, int] | None = None) -> list[str]:
    """Inboxes intake set up whose settings no longer match the standard (or a
    setting that was never set, like the empty "Minimum time gap" on 15 Oct),
    one line per kind of problem. Read-only; posted only when new (CLI)."""
    groups: dict[str, list[str]] = {}
    for a in accounts:
        if not _has_tag(a, st.SETUP_TAG):
            continue
        email = str(a.get("from_email", "")).lower()
        dom = email.split("@")[-1]
        names = [str(t.get("tag_name") or t.get("name") or "") for t in a.get("tags") or []]
        vendor = "scaledmail" if any(n.startswith("ScaledMail") for n in names) else "zapmail"
        issues = st.drift(a, st.inbox_kind(a, per_domain.get(dom, 1)), vendor, want_limit=(approved or {}).get(email))
        if issues:
            groups.setdefault("; ".join(issues), []).append(email)
    lines = []
    for issue, emails in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        n = len(emails)
        where = emails[0] if n == 1 else ", ".join(sorted({e.split("@")[-1] for e in emails}))
        lines.append(f"Changed since setup on {n} inbox{'es' if n != 1 else ''} ({where}): {issue}")
    return lines


def ready_announcements(accounts: list[dict], today: date, store, clients: dict[str, str]) -> list[dict]:
    """Domains whose set-up inboxes have ALL reached their ready date: "ready"
    when every one has its type's healthy warmup reputation (90%+, SMTP 70%+), else "low" (keep warming).
    Each (state, domain) is announced once; a domain announced low can later
    be announced ready. Returns [{key, domain, state, line}]. Pure apart from
    reading the store."""
    per_domain: dict[str, int] = {}
    for a in accounts:
        dom = str(a.get("from_email", "")).split("@")[-1].lower()
        per_domain[dom] = per_domain.get(dom, 0) + 1
    by_dom: dict[str, list[dict]] = {}
    for a in accounts:
        if _has_tag(a, st.SETUP_TAG):
            by_dom.setdefault(str(a["from_email"]).split("@")[-1].lower(), []).append(a)
    out = []
    for dom, boxes in sorted(by_dom.items()):
        kind = st.inbox_kind(boxes[0], per_domain[dom])
        states = [st.readiness(a, kind, today) for a in boxes]
        if any(s in ("warming", "unknown") for s in states):
            continue
        state = "ready" if all(s == "ready" for s in states) else "low"
        key = f"{state}:{dom}"
        if store.announced(key):
            continue
        reps = [st._int((a.get("warmup_details") or {}).get("warmup_reputation")) for a in boxes]
        reps = [r for r in reps if r is not None]
        client = clients.get(dom) or NO_CLIENT_LABEL
        if state == "ready":
            days = (today - st.warmup_started(boxes[0])).days
            label = st.standard_for(kind)["label"]
            n = len(boxes)
            line = (f"• {dom} · {n} {label} inbox{'es' if n != 1 else ''} · {client} · warmed {days} days"
                    f" · reputation {min(reps)}%")
        else:
            line = f"• {dom} · {client} · warmup reputation {min(reps)}% (needs {st.good_reputation(kind)}%), keep warming"
        out.append({"key": key, "domain": dom, "state": state, "line": line})
    return sorted(out, key=lambda a: (a["state"] != "ready", a["domain"]))


NO_CLIENT_LABEL = "client unknown"
