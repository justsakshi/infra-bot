# Inbox Health Workbook (Phase 1 + 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Score every inbox's deliverability health daily (0–100), store history in MongoDB, write a "Inbox Health" workbook Google-Sheets tab (score + trend + top problem + exact next step + owner + manager), and post a daily Slack digest grouped by client manager. (Phase 3 auto-retest is a separate plan.)

**Architecture:** A pure `health` module (score + action + trend, no I/O) feeds a Mongo store and a Sheets writer, wired into the existing daily `run.py` after the master-inbox write, all inside try/except so health never breaks the core sync. A `notify` module posts one shared-channel Slack digest via the existing bot token.

**Tech Stack:** Python 3.10+, httpx/asyncio (existing), gspread (existing), pymongo 4.6 (installed), slack via `requests` POST to `chat.postMessage`. Tests are plain runnable scripts (`python3 <file>.py`), like `test_sync_wiring.py` — NOT pytest. Run everything with `python3`. Project IS a git repo (`infra-bot`), branch `main`. Working dir for commands: `smartlead_sync/`.

## Global Constraints

- Run scripts with `python3`, never `python`.
- All new work is **read-only** externally (no Smartlead writes, no credit spend) — Phase 3 handles that separately.
- Health/notify code MUST be wrapped so a failure logs and is skipped; the core sync still succeeds.
- Env vars (already set): `MONGO_URI`, `SLACK_BOT_TOKEN`, `REMINDER_CHANNEL`.
- Real inbox-row snapshot keys (verified live): `email, provider, client, warmup_rep_pct (str like "100%"), warmup_state, connection_ok (bool), test_sheet_status, test_date (YYYY-MM-DD|""), busy_reason (comma str), campaign_name, max_per_day, sent_today, true_load, available_capacity, account_id`. NOTE: `bounced`/`sent` are `None` on inbox rows (bounce is campaign-level) → bounce scores **neutral** when absent.
- `warmup_rep_pct` is a **string with `%`** — parse with the existing `_parse_number` in sheets.py pattern (strip `%`, float).
- Grades: 90–100 A, 70–89 B, 50–69 C, 0–49 D/F. Missing signal → **neutral (half credit)**, not zero.
- This workbook **supersedes** the Deliverability Queue: remove `write_deliverability_queue` call from `run.py` once the workbook writes (keep the builder function; it is reused conceptually but the tab is replaced).

## File Structure

- `smartlead/health.py` (NEW) — pure: `compute_health_score`, `resolve_action`, `compute_trend`, constants.
- `smartlead/health_store.py` (NEW) — pymongo read/write for `inbox_health_history`.
- `smartlead/manager_map.py` (NEW) — `MANAGER_MAP` + `resolve_manager`.
- `smartlead/notify.py` (NEW) — build + post daily Slack digest.
- `smartlead/sheets.py` (MODIFY) — `write_inbox_health` + labels/widths/coloring.
- `smartlead/config.py` (MODIFY) — tab name, channel, weight/threshold constants.
- `run.py` (MODIFY) — wire scoring→store→workbook→notify; drop queue tab write.
- Tests: `test_health.py`, `test_manager_map.py`, `test_notify.py` (NEW).

---

### Task 1: Config constants

**Files:** Modify `smartlead/config.py`

**Interfaces:**
- Produces: `INBOX_HEALTH_TAB_NAME`, `HEALTH_NOTIFY_CHANNEL`, `HEALTH_HISTORY_DB`, `HEALTH_HISTORY_COLLECTION`, weight/threshold constants.

- [ ] **Step 1:** Append to `smartlead/config.py` after the `CAMPAIGN_METRICS_*` block:

```python
# ── Inbox Health workbook ────────────────────────────────────────────────────
INBOX_HEALTH_TAB_NAME: str = os.getenv("INBOX_HEALTH_TAB_NAME", "Inbox Health")
HEALTH_NOTIFY_CHANNEL: str = os.getenv("HEALTH_NOTIFY_CHANNEL", os.getenv("REMINDER_CHANNEL", ""))
HEALTH_HISTORY_DB: str = os.getenv("HEALTH_HISTORY_DB", "infrabot")
HEALTH_HISTORY_COLLECTION: str = os.getenv("HEALTH_HISTORY_COLLECTION", "inbox_health_history")

# Health score weights (sum = 100) and thresholds. Tune after seeing real data.
HEALTH_WEIGHTS: dict[str, int] = {"placement": 40, "warmup": 25, "bounce": 20, "connection": 15}
HEALTH_TEST_STALE_DAYS: int = 14   # test older than this starts decaying placement credit
HEALTH_TEST_DEAD_DAYS: int = 28    # test older than this treated as untested (neutral)
HEALTH_WARMUP_FULL: float = 99.0   # rep >= this -> full warmup credit
HEALTH_WARMUP_ZERO: float = 90.0   # rep <  this -> zero warmup credit
HEALTH_TREND_DROP: int = 8         # score drop >= this over 7d -> "declining" early warning
```

- [ ] **Step 2:** Verify — `python3 -c "from smartlead.config import INBOX_HEALTH_TAB_NAME, HEALTH_WEIGHTS, HEALTH_NOTIFY_CHANNEL; print(sum(HEALTH_WEIGHTS.values()))"` → prints `100`.

- [ ] **Step 3:** Commit — `git add smartlead/config.py && git commit -m "feat(health): config constants for inbox health workbook"`

---

### Task 2: Health scoring + action resolution (pure logic)

**Files:** Create `smartlead/health.py`; Test `test_health.py`

**Interfaces:**
- Produces:
  - `compute_health_score(snapshot: dict, today: date) -> dict` → `{"score": int, "grade": str, "drivers": {"placement": int, "warmup": int, "bounce": int, "connection": int}}`
  - `resolve_action(snapshot: dict, score: int) -> dict` → `{"priority": str, "top_problem": str, "what_to_do": str, "how_long": str, "owner": str, "owner_skill": str, "status": str}`
  - `compute_trend(today_score: int, prior_score: int | None) -> dict` → `{"delta_7d": int|None, "arrow": str, "declining": bool}`
- Consumes: snapshot keys listed in Global Constraints; constants from Task 1.

- [ ] **Step 1: Write the failing test** — `test_health.py`:

```python
"""Unit tests for health scoring, action resolution, trend."""
from datetime import date
from smartlead.health import compute_health_score, resolve_action, compute_trend

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

TODAY = date(2026, 7, 2)

def snap(**kw):
    base = dict(email="a@x.com", client="DARLEAN", provider="Gmail",
                warmup_rep_pct="100%", warmup_state="active", connection_ok=True,
                test_sheet_status="inbox", test_date="2026-07-01", busy_reason="",
                campaign_name="C1", max_per_day=30, sent_today=5, true_load=10)
    base.update(kw); return base

# --- scoring ---
s = compute_health_score(snap(), TODAY)
ok(s["score"] == 100, f"all-healthy == 100 (got {s['score']})")
ok(s["grade"] == "A", "grade A")
ok(s["drivers"]["placement"] == 40, "full placement credit")

f = compute_health_score(snap(test_sheet_status="fail", busy_reason="failed_test"), TODAY)
ok(f["drivers"]["placement"] == 0, "failed test -> 0 placement")
ok(f["score"] == 60, f"fail test drops 40 -> 60 (got {f['score']})")

w = compute_health_score(snap(warmup_rep_pct="90%"), TODAY)
ok(w["drivers"]["warmup"] == 0, "rep 90% -> 0 warmup credit")

wm = compute_health_score(snap(warmup_rep_pct="99%"), TODAY)
ok(wm["drivers"]["warmup"] == 25, "rep 99% -> full warmup credit")

d = compute_health_score(snap(connection_ok=False, busy_reason="disconnected"), TODAY)
ok(d["drivers"]["connection"] == 0, "disconnected -> 0 connection")

# stale test decays, dead test -> neutral half
st = compute_health_score(snap(test_sheet_status="inbox", test_date="2026-06-10"), TODAY)  # 22d old
ok(0 < st["drivers"]["placement"] < 40, f"stale test decays placement (got {st['drivers']['placement']})")
dead = compute_health_score(snap(test_sheet_status="", test_date=""), TODAY)
ok(dead["drivers"]["placement"] == 20, f"untested -> neutral 20/40 (got {dead['drivers']['placement']})")

# missing bounce data -> neutral (half of 20 = 10)
ok(compute_health_score(snap(), TODAY)["drivers"]["bounce"] == 20, "no bounce data but healthy load -> full? see impl")

# --- action resolution ---
a = resolve_action(snap(test_sheet_status="fail", busy_reason="failed_test"), 60)
ok(a["priority"] == "P0", "failed -> P0")
ok(a["owner"] == "human", "failed -> human owner")
ok("SPF" in a["what_to_do"] or "retest" in a["what_to_do"].lower(), "failed action mentions fix")

a2 = resolve_action(snap(test_sheet_status="stale", busy_reason="stale_test"), 80)
ok(a2["priority"] == "P1" and a2["owner"] == "auto", "stale -> P1 auto")

a3 = resolve_action(snap(), 100)
ok(a3["priority"] == "" and a3["status"] == "healthy", "healthy -> no priority")

# --- trend ---
t = compute_trend(70, 85)
ok(t["delta_7d"] == -15 and t["arrow"] == "↓" and t["declining"] is True, "declining trend")
ok(compute_trend(90, None)["arrow"] == "—", "no prior -> flat/unknown")
ok(compute_trend(90, 88)["arrow"] == "↑", "improving -> up arrow")

print("\nALL PASSED")
```

- [ ] **Step 2: Run, verify fail** — `python3 test_health.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/health.py`:

```python
"""Pure inbox-health scoring, action resolution, and trend (no I/O)."""
from __future__ import annotations

from datetime import date, datetime

from smartlead.config import (
    HEALTH_WEIGHTS, HEALTH_TEST_STALE_DAYS, HEALTH_TEST_DEAD_DAYS,
    HEALTH_WARMUP_FULL, HEALTH_WARMUP_ZERO, HEALTH_TREND_DROP,
)

_FAIL = {"fail", "spam"}


def _num(v) -> float:
    try:
        return float(str(v).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def _reasons(snapshot: dict) -> set[str]:
    return {r.strip() for r in str(snapshot.get("busy_reason", "")).split(",") if r.strip()}


def _test_age_days(test_date: str, today: date) -> int | None:
    if not test_date:
        return None
    try:
        d = datetime.fromisoformat(str(test_date)[:10]).date()
    except ValueError:
        return None
    return (today - d).days


def _placement_points(snapshot: dict, today: date) -> int:
    """40 max. inbox+fresh=40; fail=0; stale decays toward half; dead/untested=neutral 20."""
    maxp = HEALTH_WEIGHTS["placement"]
    status = str(snapshot.get("test_sheet_status", "")).strip().lower()
    if status in _FAIL:
        return 0
    age = _test_age_days(str(snapshot.get("test_date", "")), today)
    if status not in {"inbox", "stale"} or age is None:
        return maxp // 2  # untested -> neutral
    if age >= HEALTH_TEST_DEAD_DAYS:
        return maxp // 2  # too old -> neutral (treat as untested)
    if age <= HEALTH_TEST_STALE_DAYS:
        return maxp        # fresh inbox
    # linear decay between stale and dead: maxp -> maxp/2
    span = HEALTH_TEST_DEAD_DAYS - HEALTH_TEST_STALE_DAYS
    frac = (age - HEALTH_TEST_STALE_DAYS) / span
    return round(maxp - frac * (maxp / 2))


def _warmup_points(snapshot: dict) -> int:
    maxp = HEALTH_WEIGHTS["warmup"]
    rep = _num(snapshot.get("warmup_rep_pct"))
    if rep <= 0:
        return maxp // 2  # unknown -> neutral
    if rep >= HEALTH_WARMUP_FULL:
        return maxp
    if rep < HEALTH_WARMUP_ZERO:
        return 0
    frac = (rep - HEALTH_WARMUP_ZERO) / (HEALTH_WARMUP_FULL - HEALTH_WARMUP_ZERO)
    return round(frac * maxp)


def _bounce_points(snapshot: dict) -> int:
    """20 max. Bounce is campaign-level; inbox rows lack it -> neutral full-safe.
    If a bounce rate is present (%), <1%=full, >5%=0, linear between."""
    maxp = HEALTH_WEIGHTS["bounce"]
    raw = snapshot.get("bounce_rate")
    if raw in (None, ""):
        return maxp  # no per-inbox bounce signal -> do not penalize
    br = _num(raw)
    if br < 1.0:
        return maxp
    if br > 5.0:
        return 0
    frac = 1 - (br - 1.0) / 4.0
    return round(frac * maxp)


def _connection_points(snapshot: dict) -> int:
    maxp = HEALTH_WEIGHTS["connection"]
    if not snapshot.get("connection_ok", True):
        return 0
    return maxp  # connected; reply-rate rule needs campaign data -> full when connected


def _grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 70:
        return "B"
    if score >= 50:
        return "C"
    return "D"


def compute_health_score(snapshot: dict, today: date) -> dict:
    drivers = {
        "placement": _placement_points(snapshot, today),
        "warmup": _warmup_points(snapshot),
        "bounce": _bounce_points(snapshot),
        "connection": _connection_points(snapshot),
    }
    score = int(sum(drivers.values()))
    return {"score": score, "grade": _grade(score), "drivers": drivers}


def resolve_action(snapshot: dict, score: int) -> dict:
    """First-match-wins problem -> fix/owner/priority. Same order as the queue."""
    reasons = _reasons(snapshot)
    status = str(snapshot.get("test_sheet_status", "")).strip().lower()

    def item(priority, problem, what, how, owner, skill):
        state = "broken" if priority == "P0" else ("needs_action" if priority else "healthy")
        return {"priority": priority, "top_problem": problem, "what_to_do": what,
                "how_long": how, "owner": owner, "owner_skill": skill, "status": state}

    if "failed_test" in reasons or status in _FAIL:
        return item("P0", "Failed placement test",
                    "Pause inbox/domain; check SPF/DKIM/DMARC + copy + list; retest.",
                    "1-7 days", "human", "deliverability-incident-response")
    if "warmup_blocked" in reasons:
        return item("P0", "Warmup blocked",
                    "Investigate block reason; pause or retire inbox if it does not recover.",
                    "1-7 days", "human", "smartlead-inbox-manager")
    if "disconnected" in reasons or not snapshot.get("connection_ok", True):
        return item("P1", "SMTP/IMAP disconnected",
                    "Reconnect the inbox before any campaign assignment.",
                    "minutes", "human", "smartlead-inbox-manager")
    if "low_rep" in reasons:
        return item("P1", "Warmup reputation below threshold",
                    "Keep out of campaigns; continue/adjust warmup; retest after recovery.",
                    "3-14 days", "human", "smartlead-inbox-manager")
    if "untested" in reasons or status in {"", "unknown"}:
        return item("P1", "No placement test on record",
                    "Auto-run initial GSuite + Outlook placement test.",
                    "auto", "auto", "deliverability-test-public")
    if "stale_test" in reasons or status == "stale":
        return item("P1", "Placement test is stale",
                    "Auto-run fresh placement test.",
                    "auto", "auto", "deliverability-test-public")
    if int(_num(snapshot.get("max_per_day"))) >= 35 and status != "inbox":
        return item("P2", "High-volume inbox needs routine retest",
                    "Auto-retest before scaling volume.",
                    "auto", "auto", "deliverability-test-public")
    return item("", "", "", "", "", "")


def compute_trend(today_score: int, prior_score: int | None) -> dict:
    if prior_score is None:
        return {"delta_7d": None, "arrow": "—", "declining": False}
    delta = today_score - prior_score
    arrow = "↑" if delta > 0 else ("↓" if delta < 0 else "→")
    return {"delta_7d": delta, "arrow": arrow, "declining": delta <= -HEALTH_TREND_DROP}
```

- [ ] **Step 4: Run, verify pass** — `python3 test_health.py` → `ALL PASSED`. (If the "no bounce data" assertion mismatches, note: bounce with no data = full 20 by design — do not penalize; adjust the test comment, not the impl.)

- [ ] **Step 5: Commit** — `git add smartlead/health.py test_health.py && git commit -m "feat(health): scoring + action resolution + trend (pure)"`

---

### Task 3: Mongo history store

**Files:** Create `smartlead/health_store.py`

**Interfaces:**
- Produces: `HealthStore` with `save_daily(records: list[dict]) -> int`, `prior_score(client, email, days_ago, today) -> int|None`, `close()`. Constructor reads `MONGO_URI`; if unset/unreachable, methods no-op (return 0 / None) so the sync never breaks.
- Record shape written: `{client, email, domain, date (YYYY-MM-DD), score, grade, drivers{...}, placement_status, placement_date, warmup_rep_pct, connection_ok, campaigns}`.

- [ ] **Step 1: Implement** (no offline unit test — it is thin DB glue; validated live in Task 8). `smartlead/health_store.py`:

```python
"""MongoDB history for daily inbox health scores. No-ops if Mongo is unavailable."""
from __future__ import annotations

import os
from datetime import date, timedelta

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except ImportError:  # pragma: no cover
    MongoClient = None

from smartlead.config import HEALTH_HISTORY_DB, HEALTH_HISTORY_COLLECTION


class HealthStore:
    def __init__(self) -> None:
        self._col = None
        uri = os.getenv("MONGO_URI", "")
        if not uri or MongoClient is None:
            print("  [Health] Mongo unavailable (no MONGO_URI) - history disabled.")
            return
        try:
            client = MongoClient(uri, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            self._col = client[HEALTH_HISTORY_DB][HEALTH_HISTORY_COLLECTION]
            self._col.create_index([("client", 1), ("email", 1), ("date", 1)], unique=True)
        except Exception as exc:  # noqa: BLE001
            print(f"  [Health] Mongo connect failed ({exc}) - history disabled.")
            self._col = None

    def save_daily(self, records: list[dict]) -> int:
        if self._col is None or not records:
            return 0
        ops = [
            UpdateOne(
                {"client": r["client"], "email": r["email"], "date": r["date"]},
                {"$set": r}, upsert=True,
            )
            for r in records
        ]
        try:
            res = self._col.bulk_write(ops, ordered=False)
            return (res.upserted_count or 0) + (res.modified_count or 0)
        except PyMongoError as exc:
            print(f"  [Health] Mongo write failed: {exc}")
            return 0

    def prior_score(self, client: str, email: str, days_ago: int, today: date) -> int | None:
        if self._col is None:
            return None
        target = (today - timedelta(days=days_ago)).strftime("%Y-%m-%d")
        try:
            # nearest record on-or-before the target date (within a 3-day window)
            floor = (today - timedelta(days=days_ago + 3)).strftime("%Y-%m-%d")
            doc = self._col.find_one(
                {"client": client, "email": email, "date": {"$lte": target, "$gte": floor}},
                sort=[("date", -1)],
            )
            return int(doc["score"]) if doc and "score" in doc else None
        except PyMongoError:
            return None

    def close(self) -> None:
        pass
```

- [ ] **Step 2: Import check** — `python3 -c "from smartlead.health_store import HealthStore; HealthStore().save_daily([]); print('ok')"` → prints Mongo status line then `ok` (works even if Mongo down).

- [ ] **Step 3: Commit** — `git add smartlead/health_store.py && git commit -m "feat(health): MongoDB history store (fail-safe)"`

---

### Task 4: Manager map

**Files:** Create `smartlead/manager_map.py`; Test `test_manager_map.py`

**Interfaces:**
- Produces: `MANAGER_MAP: dict[str, dict]` (`{client: {"name", "slack"}}`), `resolve_manager(client: str) -> dict` (unknown → `{"name": "Unassigned", "slack": ""}`).

- [ ] **Step 1: Failing test** — `test_manager_map.py`:

```python
from smartlead.manager_map import resolve_manager, MANAGER_MAP

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

ok(resolve_manager("DARLEAN")["name"] != "Unassigned", "DARLEAN mapped")
# multi-client-under-one-account: Melior/Precise Leads/Better Data distinct
ok("Melior" in MANAGER_MAP, "Melior present")
ok("Bettrdata" in MANAGER_MAP, "Better Data (Bettrdata) present")
ok(resolve_manager("Melior") is not resolve_manager("Precise Leads"), "distinct entries")
ok(resolve_manager("NoSuchClient") == {"name": "Unassigned", "slack": ""}, "unknown -> Unassigned")
print("\nALL PASSED")
```

- [ ] **Step 2: Run, verify fail** — `python3 test_manager_map.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/manager_map.py` (placeholder Slack handles; user fills real ones at rollout — this is config, not a code placeholder):

```python
"""Per-client owner map. Ownership is per client (not per Smartlead account):
one account (e.g. PRECISE_LEADS) holds several clients, each with its own manager.
Fill slack handles at rollout (Slack member IDs like 'U01234' or '@name')."""
from __future__ import annotations

MANAGER_MAP: dict[str, dict] = {
    "DARLEAN":       {"name": "Unassigned", "slack": ""},
    "Melior":        {"name": "Unassigned", "slack": ""},
    "Precise Leads": {"name": "Unassigned", "slack": ""},
    "Bettrdata":     {"name": "Unassigned", "slack": ""},  # Better Data
    "Mythic":        {"name": "Unassigned", "slack": ""},
    "Belardi Wong":  {"name": "Unassigned", "slack": ""},
    "Avench":        {"name": "Unassigned", "slack": ""},
    "OSC":           {"name": "Unassigned", "slack": ""},
    "StaffAI":       {"name": "Unassigned", "slack": ""},
}


def resolve_manager(client: str) -> dict:
    return MANAGER_MAP.get(client, {"name": "Unassigned", "slack": ""})
```

- [ ] **Step 4: Run, verify pass** — `python3 test_manager_map.py` → `ALL PASSED`.

- [ ] **Step 5: Commit** — `git add smartlead/manager_map.py test_manager_map.py && git commit -m "feat(health): per-client manager map"`

---

### Task 5: Health rows builder (join scoring + trend + action + manager)

**Files:** Modify `smartlead/health.py` (add `build_health_rows`)

**Interfaces:**
- Produces: `build_health_rows(inbox_rows: list[dict], today: date, store, resolve_manager) -> list[dict]` → deduped one row per (client,email) with all workbook columns + a parallel `history_records` via `health_records_for_store(rows, today)`.
- Consumes: `compute_health_score`, `resolve_action`, `compute_trend` (Task 2); `HealthStore.prior_score` (Task 3); `resolve_manager` (Task 4); `_dedupe_inbox_rows` from sheets.py.

- [ ] **Step 1: Add to `smartlead/health.py`:**

```python
def _domain(email: str) -> str:
    return str(email).split("@", 1)[1].strip().lower() if "@" in str(email) else ""


def build_health_rows(inbox_rows: list[dict], today, store, resolve_manager) -> list[dict]:
    """Score+action+trend+manager for each unique inbox. `today` is a date."""
    from smartlead.sheets import _dedupe_inbox_rows  # reuse existing dedup
    rows: list[dict] = []
    for snap in _dedupe_inbox_rows(inbox_rows):
        client = snap.get("client", "")
        email = str(snap.get("email", "")).strip()
        if not email:
            continue
        hs = compute_health_score(snap, today)
        act = resolve_action(snap, hs["score"])
        prior = store.prior_score(client, email, 7, today) if store else None
        tr = compute_trend(hs["score"], prior)
        mgr = resolve_manager(client)
        drivers = hs["drivers"]
        rows.append({
            "priority": act["priority"],
            "client": client,
            "email": email,
            "domain": _domain(email),
            "provider": snap.get("provider", ""),
            "score": hs["score"],
            "grade": hs["grade"],
            "trend": (f"{tr['arrow']} {tr['delta_7d']:+d}" if tr["delta_7d"] is not None else tr["arrow"]),
            "status": act["status"],
            "top_problem": act["top_problem"],
            "what_to_do": act["what_to_do"],
            "owner": "🤖 Auto" if act["owner"] == "auto" else ("👤 You" if act["owner"] == "human" else ""),
            "how_long": act["how_long"],
            "manager": mgr["name"],
            "drivers": f"test {drivers['placement']}/40 · warmup {drivers['warmup']}/25 · bounce {drivers['bounce']}/20 · conn {drivers['connection']}/15",
            "warmup_rep_pct": snap.get("warmup_rep_pct", ""),
            "test_sheet_status": snap.get("test_sheet_status", ""),
            "test_date": snap.get("test_date", ""),
            "campaigns": snap.get("campaigns", 0),
            "owner_skill": act["owner_skill"],
            "_declining": tr["declining"],
        })
    priority_order = {"P0": 0, "P1": 1, "P2": 2, "": 9}
    rows.sort(key=lambda r: (priority_order.get(r["priority"], 9), r["client"], r["score"]))
    return rows


def health_records_for_store(rows: list[dict], today) -> list[dict]:
    """Project workbook rows into Mongo history records."""
    ds = today.strftime("%Y-%m-%d")
    out = []
    for r in rows:
        out.append({
            "client": r["client"], "email": r["email"], "domain": r["domain"],
            "date": ds, "score": r["score"], "grade": r["grade"],
            "placement_status": r["test_sheet_status"], "placement_date": r["test_date"],
            "warmup_rep_pct": r["warmup_rep_pct"], "campaigns": r["campaigns"],
        })
    return out
```

- [ ] **Step 2: Extend `test_health.py`** — append:

```python
# --- build_health_rows integration (no Mongo, no manager file needed) ---
class _NoStore:
    def prior_score(self, *a, **k): return None
from smartlead.health import build_health_rows, health_records_for_store
inbox = [snap(email="a@d1.com", client="DARLEAN"),
         snap(email="a@d1.com", client="DARLEAN", campaign_name="C2"),  # dup -> merged
         snap(email="b@d2.com", client="DARLEAN", test_sheet_status="fail", busy_reason="failed_test")]
rows = build_health_rows(inbox, TODAY, _NoStore(), lambda c: {"name": "Dmitrii", "slack": "@d"})
ok(len(rows) == 2, f"deduped to 2 inboxes (got {len(rows)})")
ok(rows[0]["priority"] == "P0", "P0 sorts first")
ok(rows[0]["manager"] == "Dmitrii", "manager attached")
recs = health_records_for_store(rows, TODAY)
ok(recs[0]["date"] == "2026-07-02" and "score" in recs[0], "history record shape")
print("\nALL PASSED (with build_health_rows)")
```

- [ ] **Step 3: Run** — `python3 test_health.py` → `ALL PASSED (with build_health_rows)`.

- [ ] **Step 4: Commit** — `git add smartlead/health.py test_health.py && git commit -m "feat(health): build_health_rows + history projection"`

---

### Task 6: Sheets writer for the workbook tab

**Files:** Modify `smartlead/sheets.py`

**Interfaces:**
- Produces: `SheetsWriter.write_inbox_health(rows: list[dict]) -> None`; `INBOX_HEALTH_COLUMNS`.
- Consumes: `INBOX_HEALTH_TAB_NAME` (config).

- [ ] **Step 1:** Add to `_HEADER_LABELS` (after the "Deliverability Queue" entry):

```python
    "Inbox Health": {
        "priority": "Priority", "client": "Client", "email": "Email", "domain": "Domain",
        "provider": "Provider", "score": "Health Score", "grade": "Grade", "trend": "Trend (7d)",
        "status": "Status", "top_problem": "Top Problem", "what_to_do": "What To Do",
        "owner": "Owner", "how_long": "How Long", "manager": "Manager", "drivers": "Score Drivers",
        "warmup_rep_pct": "Warmup Rep %", "test_sheet_status": "Test Status", "test_date": "Test Date",
        "campaigns": "# Campaigns", "owner_skill": "Owner Skill",
    },
```
and to `_COL_WIDTHS`:
```python
    "Inbox Health": {"priority": 80, "client": 130, "email": 250, "score": 90, "grade": 70,
                     "trend": 100, "status": 130, "top_problem": 220, "what_to_do": 420,
                     "owner": 90, "manager": 130, "drivers": 320},
```

- [ ] **Step 2:** Add the method after `write_deliverability_queue`:

```python
    INBOX_HEALTH_COLUMNS = [
        "priority", "client", "email", "domain", "provider", "score", "grade", "trend",
        "status", "top_problem", "what_to_do", "owner", "how_long", "manager", "drivers",
        "warmup_rep_pct", "test_sheet_status", "test_date", "campaigns", "owner_skill",
    ]

    def write_inbox_health(self, rows: list[dict]) -> None:
        """Write the 'Inbox Health' workbook tab."""
        from smartlead.config import INBOX_HEALTH_TAB_NAME
        if not rows:
            print("  [Sheets] No inbox health rows - skipping.")
            return
        projected = [{c: r.get(c, "") for c in self.INBOX_HEALTH_COLUMNS} for r in rows]
        self._write_tab(INBOX_HEALTH_TAB_NAME, projected)
        print(f"  [Sheets] Inbox Health tab written: {len(rows)} inboxes")
```

- [ ] **Step 3:** Grade coloring — in `_status_colors`, add near the other `if tab_key ==` blocks:

```python
        from smartlead.config import INBOX_HEALTH_TAB_NAME
        if tab_key == INBOX_HEALTH_TAB_NAME and "grade" in columns:
            col_idx = columns.index("grade")
            palette = {"A": (_COLORS["green_bg"], _COLORS["green_text"]),
                       "B": (_COLORS["green_bg"], _COLORS["green_text"]),
                       "C": (_COLORS["yellow_bg"], _COLORS["orange_text"]),
                       "D": (_COLORS["red_bg"], _COLORS["red_text"])}
            for ri, row in enumerate(data, start=1):
                bg, fg = palette.get(str(row.get("grade", "")).upper(), (None, None))
                if bg:
                    reqs.append(_format_range(sheet_id, ri, ri + 1, col_idx, col_idx + 1,
                                              bg=bg, fg=fg, bold=True, h_align="CENTER"))
```

- [ ] **Step 4: Verify** — `python3 -m py_compile smartlead/sheets.py` and `python3 -c "from smartlead.sheets import SheetsWriter; print('write_inbox_health' in dir(SheetsWriter))"` → `True`. Run `python3 test_sync_wiring.py` → `ALL PASSED` (regression).

- [ ] **Step 5: Commit** — `git add smartlead/sheets.py && git commit -m "feat(health): sheets writer for Inbox Health workbook"`

---

### Task 7: Slack daily digest

**Files:** Create `smartlead/notify.py`; Test `test_notify.py`

**Interfaces:**
- Produces: `build_digest(rows: list[dict], sheet_url: str) -> str` (Slack markdown; groups action items by client, P0 first, @-mentions manager slack handle); `post_digest(text: str) -> bool` (POST to Slack `chat.postMessage`, returns ok).
- Consumes: `HEALTH_NOTIFY_CHANNEL`, `SLACK_BOT_TOKEN` (env).

- [ ] **Step 1: Failing test** — `test_notify.py` (tests only the pure `build_digest`):

```python
from smartlead.notify import build_digest

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

rows = [
    {"priority": "P0", "client": "DARLEAN", "email": "a@x.com", "top_problem": "Failed placement test",
     "owner": "👤 You", "manager": "Dmitrii", "_mgr_slack": "@dmitrii"},
    {"priority": "P1", "client": "Melior", "email": "b@y.com", "top_problem": "Stale test",
     "owner": "🤖 Auto", "manager": "Sam", "_mgr_slack": ""},
    {"priority": "", "client": "DARLEAN", "email": "ok@x.com", "top_problem": "", "owner": "", "manager": "Dmitrii"},
]
msg = build_digest(rows, "https://sheet")
ok("DARLEAN" in msg and "Melior" in msg, "clients present")
ok("a@x.com" in msg, "P0 inbox listed")
ok("ok@x.com" not in msg, "healthy inbox excluded from digest")
ok("@dmitrii" in msg, "manager mentioned")
ok("https://sheet" in msg, "workbook link present")
ok(msg.index("DARLEAN") < msg.index("Melior") or "P0" in msg, "P0 client surfaced")
print("\nALL PASSED")
```

- [ ] **Step 2: Run, verify fail** — `python3 test_notify.py` → ModuleNotFoundError.

- [ ] **Step 3: Implement** — `smartlead/notify.py`:

```python
"""Daily Slack digest of inbox-health action items, grouped by client manager."""
from __future__ import annotations

import os

import requests

from smartlead.config import HEALTH_NOTIFY_CHANNEL

_PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}


def build_digest(rows: list[dict], sheet_url: str) -> str:
    """Markdown digest: only rows needing action (priority set), grouped by client."""
    actionable = [r for r in rows if r.get("priority")]
    if not actionable:
        return f"✅ Inbox Health: all inboxes healthy today. Workbook: {sheet_url}"

    by_client: dict[str, list[dict]] = {}
    for r in actionable:
        by_client.setdefault(r.get("client", "Unknown"), []).append(r)

    # clients with a P0 first, then by count
    def client_rank(items):
        return (0 if any(i["priority"] == "P0" for i in items) else 1, -len(items))

    lines = [f"*🩺 Inbox Health — {len(actionable)} inbox(es) need attention*",
             f"<{sheet_url}|Open the workbook>", ""]
    for client, items in sorted(by_client.items(), key=lambda kv: client_rank(kv[1])):
        items.sort(key=lambda r: _PRIORITY_ORDER.get(r["priority"], 9))
        mgr_slack = next((i.get("_mgr_slack") for i in items if i.get("_mgr_slack")), "")
        mgr_name = items[0].get("manager", "Unassigned")
        who = f"<{mgr_slack}>" if mgr_slack.startswith("U") else (mgr_slack or mgr_name)
        p0 = sum(1 for i in items if i["priority"] == "P0")
        lines.append(f"*{client}* — {who} · {len(items)} item(s){f', {p0} 🔴 P0' if p0 else ''}")
        for i in items[:8]:
            auto = " _(auto-fixing)_" if i.get("owner", "").startswith("🤖") else ""
            lines.append(f"   • `{i['priority']}` {i['email']} — {i['top_problem']}{auto}")
        if len(items) > 8:
            lines.append(f"   • …and {len(items) - 8} more")
        lines.append("")
    return "\n".join(lines).strip()


def post_digest(text: str) -> bool:
    token = os.getenv("SLACK_BOT_TOKEN", "")
    channel = HEALTH_NOTIFY_CHANNEL
    if not token or not channel:
        print("  [Notify] SLACK_BOT_TOKEN/HEALTH_NOTIFY_CHANNEL missing - skipping post.")
        return False
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": text, "unfurl_links": False},
            timeout=15,
        )
        ok = resp.json().get("ok", False)
        if not ok:
            print(f"  [Notify] Slack error: {resp.json().get('error')}")
        return ok
    except requests.RequestException as exc:
        print(f"  [Notify] Slack post failed: {exc}")
        return False
```

- [ ] **Step 4: Run, verify pass** — `python3 test_notify.py` → `ALL PASSED`.

- [ ] **Step 5: Commit** — `git add smartlead/notify.py test_notify.py && git commit -m "feat(health): Slack daily digest builder + poster"`

---

### Task 8: Wire into run.py + drop the queue tab

**Files:** Modify `run.py`

**Interfaces:** consumes everything above.

- [ ] **Step 1: Imports** — add near the other `smartlead.*` imports in `run.py`:

```python
from datetime import date as _date
from smartlead.config import INBOX_HEALTH_TAB_NAME, HEALTH_NOTIFY_CHANNEL
from smartlead.health import build_health_rows, health_records_for_store
from smartlead.health_store import HealthStore
from smartlead.manager_map import resolve_manager
from smartlead import notify as _notify
```

- [ ] **Step 2: Replace the master-tab loop** so it also writes health + collects rows. Find:

```python
            writer = SheetsWriter(sheet_id)
            writer.write_master_inboxes(rows)
            writer.write_deliverability_queue(rows)
```
Replace with:
```python
            writer = SheetsWriter(sheet_id)
            writer.write_master_inboxes(rows)
```
(Removes the queue-tab write; the workbook supersedes it.)

- [ ] **Step 3: Add the health block** after the master-tab loop (before Campaign Metrics):

```python
    # ── Inbox Health workbook (score + trend + action + manager) ─────────────
    health_rows_by_sheet: dict[str, list[dict]] = {}
    try:
        today = _date.today()
        store = HealthStore()
        all_health_rows: list[dict] = []
        for sheet_id, rows in rows_by_sheet.items():
            hrows = build_health_rows(rows, today, store, resolve_manager)
            # attach manager slack for notify
            from smartlead.manager_map import resolve_manager as _rm
            for hr in hrows:
                hr["_mgr_slack"] = _rm(hr["client"]).get("slack", "")
            health_rows_by_sheet[sheet_id] = hrows
            all_health_rows.extend(hrows)
            try:
                SheetsWriter(sheet_id).write_inbox_health(hrows)
            except Exception as exc:
                print(f"[!] Inbox Health tab failed for sheet {sheet_id}: {exc}")
        # history
        saved = store.save_daily(
            [rec for hrows in health_rows_by_sheet.values()
             for rec in health_records_for_store(hrows, today)]
        )
        print(f"[*] Inbox Health: {len(all_health_rows)} inboxes scored, {saved} history records saved")
        # Slack digest (one shared channel; uses the primary sheet's url)
        try:
            primary = accounts[0].sheet_id
            sheet_url = f"https://docs.google.com/spreadsheets/d/{primary}"
            digest = _notify.build_digest(all_health_rows, sheet_url)
            if _notify.post_digest(digest):
                print("[*] Inbox Health Slack digest posted")
        except Exception as exc:
            print(f"[!] Inbox Health notify failed: {exc}")
    except Exception as exc:
        print(f"[!] Inbox Health workbook failed: {exc}")
```

- [ ] **Step 4: Compile + mock** — `python3 -m py_compile run.py` then `python3 run.py --mock` → mock output, no traceback.

- [ ] **Step 5: Live validation (read-only score, no sheet write)** — create `_health_probe.py`, run, then delete:
```python
import asyncio
from datetime import date
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
from smartlead.accounts import discover_accounts
from smartlead.api import SmartleadClient
from smartlead.sheets import DeliverabilityReader
from smartlead.config import ACCOUNT_DELIVERABILITY_TABS
from smartlead.processing import fetch_account_data
from smartlead.health import build_health_rows
from smartlead.manager_map import resolve_manager

class _NoStore:
    def prior_score(self, *a, **k): return None

async def main():
    acc = next(a for a in discover_accounts() if a.name == "DARLEAN")
    dmap = {}
    for t in ACCOUNT_DELIVERABILITY_TABS.get("DARLEAN", []):
        dmap.update(await DeliverabilityReader(tab_name=t).fetch())
    async with SmartleadClient(acc.api_key, acc.name) as c:
        inbox, _, _ = await fetch_account_data(c, dmap, active_only=False)
    for r in inbox:
        r.setdefault("client", "DARLEAN")
    rows = build_health_rows(inbox, date.today(), _NoStore(), resolve_manager)
    from collections import Counter
    print("inboxes:", len(rows))
    print("grades:", dict(Counter(r["grade"] for r in rows)))
    print("priorities:", dict(Counter(r["priority"] or "healthy" for r in rows)))
    for r in rows[:5]:
        print(f"  {r['grade']} {r['score']:3} {r['priority'] or '  ':3} {r['email']:34} {r['top_problem']}")
asyncio.run(main())
```
Run `python3 _health_probe.py` → sensible grade/priority distribution. Then `rm _health_probe.py`.

- [ ] **Step 6: Commit** — `git add run.py && git commit -m "feat(health): wire Inbox Health workbook + history + Slack digest into daily sync"`

---

### Task 9: Full live sync + cleanup

- [ ] **Step 1: Full sync** — `python3 run.py 2>&1 | grep -E "Inbox Health|digest|\[\!\]" | tail -10`. Expected: `Inbox Health: N inboxes scored, M history records saved` and (if Slack env set) digest posted.
- [ ] **Step 2: Verify tab** — open the sheet; confirm "Inbox Health" tab exists, grade-colored, sorted P0-first, "What To Do" populated. Confirm old "Deliverability Queue" tab is no longer being updated (may delete it manually).
- [ ] **Step 3: Verify history** — `python3 -c "import os; from pymongo import MongoClient; c=MongoClient(os.getenv('MONGO_URI')); print(c['infrabot']['inbox_health_history'].count_documents({}))"` → nonzero.

---

## Self-Review Notes

- **Spec coverage:** score (T2), history/Mongo (T3), workbook tab (T5/T6), manager map (T4), Slack notify (T7), track-all-inboxes (T5 uses `active_only=False` inbox rows incl. no-campaign inboxes), trend/early-warning (T2 `compute_trend` + `_declining`), supersede queue (T8 drops the write), 3-phase rollout (this plan = P1+P2; P3 auto-retest deferred). Bounce is campaign-level → neutral (documented in T2).
- **Placeholder scan:** Manager Map handles are config values the user fills at rollout (spec Open Item), not code placeholders. No TBDs.
- **Type consistency:** `build_health_rows(inbox_rows, today, store, resolve_manager)` signature matches T8 call; row dict keys match `INBOX_HEALTH_COLUMNS` (T6) and `build_digest` reads (`priority, client, email, top_problem, owner, manager, _mgr_slack`); `HealthStore.prior_score(client, email, days_ago, today)` matches T5 call.
- **Deferred to P3 plan:** `smart_delivery.py`, `retest_executor.py`, API↔manual merge rule, separate cron, daily test cap.
