#!/usr/bin/env python3
"""Weekly Smart Delivery v2 job: read the result CSVs, take failing inboxes out of campaigns,
bring them back once they pass twice, and post what happened.

Why it runs on Manveen's PC: v2 has no API, webhook or email (Smartlead support, 5-6 Nov 2026),
so the results only exist as the CSVs he downloads. A Windows scheduled task runs this every hour;
it reads new v2 CSVs in his Downloads folder (by content hash, so a file is read once).

What it does (rules in smartlead/v2_results.py):
  * every inbox gets PASS / WATCH / FAIL per provider (4 of 4 seeds in the inbox / 2-3 / 3+ in spam);
    a seed still pending or BOUNCED, or a test that sent fewer than 4, is not judged and never acted on;
  * own provider (Manveen 11 Nov 2026): a GMAIL inbox is judged on its Google result only, an OUTLOOK
    inbox on its Microsoft result only; an SMTP inbox, or one of unknown type, on both as before;
  * a domain fails when half or more of its inboxes (all the job knows on it, tested or not) fail at
    their own provider: every other inbox on that domain is taken out the same way, and none of them
    comes back while it still fails;
  * FAIL, or WATCH in two Monday tests 6+ days apart: removed from every live campaign that is not a
    test campaign, its leads moved to the campaign's other senders (stalled leads resumed), and
    tagged "DO NOT USE - failed v2 (auto)" so gtm presend refuses it. Warm-up stays on, and it
    stays in its DT test campaign, so next Monday tests it again;
  * Melior is report-only (REPORT_ONLY_CLIENTS, until Ryan agrees; --act-clients Melior to act): its
    inboxes are listed as "would be taken out" and nothing in Smartlead changes for them;
  * a campaign left with no good sender is not touched (sending or scheduled: the post asks a person);
  * an inbox the job pulled comes back (same campaigns, tag removed) after two passes 6+ days apart;
  * if nearly every inbox in a night, or in one test file, is below 4/4 at one provider, that provider is
    held (test problem);
  * it acts on a night only once its files stopped arriving for 90 minutes;
  * state is saved after every change, half-done moves are retried, a crash posts once a day;
  * from Tuesday 12:00 IST it posts one reminder naming each DT test whose results are not in;
  * after every saved run it writes a heartbeat and the verdicts to Mongo (v2_heartbeat, v2_verdicts),
    so Render can see the job ran and what v2 said; a Mongo problem never stops the run.

Modes:
    python v2_weekly.py                         # dry run: print the plan; no state, no Smartlead, no Slack
    python v2_weekly.py --warn --post           # save state and post the plan; no Smartlead changes
    python v2_weekly.py --apply --post          # act
    python v2_weekly.py --apply --post --act-from 2026-11-16   # --warn until that date, then act
    python v2_weekly.py --history-only          # file the CSVs as history: kept, never acted on
    python v2_weekly.py --replan --apply --post # act on stored results without new files
    python v2_weekly.py --self-check            # Slack channel, both Smartlead keys, Downloads, Mongo; posts nothing

Exit codes: 0 fine, 1 a Smartlead step failed (see the post), 2 crashed, 3 a Slack post failed
("UNPOSTED" in the log; the report is queued and posted again next run). --self-check: 1 if a check failed.

Install (Windows, once): see docs/V2_WEEKLY.md. State and a copy of every CSV live in --state-dir
(default ..\\..\\v2-weekly, outside the repo).
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import traceback
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

if sys.platform == "win32" and sys.stdout is not None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

HERE = Path(__file__).resolve().parent


def _env_dir() -> Path:
    """The smartlead_sync folder whose .env holds the keys (the main checkout when this runs from a
    worktree). Read before any smartlead import: smartlead.config reads the environment on import."""
    if "--env-dir" in sys.argv[:-1]:
        return Path(sys.argv[sys.argv.index("--env-dir") + 1])
    return Path(os.getenv("INFRABOT_ENV_DIR") or HERE)


ENV_DIR = _env_dir()
try:
    from dotenv import load_dotenv
    load_dotenv(ENV_DIR / ".." / ".env")
    load_dotenv(ENV_DIR / ".env")
except ImportError:  # pragma: no cover
    pass

sys.path.insert(0, str(HERE))
from smartlead import v2_results as v2  # noqa: E402

TAG = "DO NOT USE - failed v2 (auto)"
DNU = "do not use"                       # gtm presend refuses any tag starting with this
ACCOUNTS = ("PRECISE_LEADS", "BETTRDATA")
SENDING = {"ACTIVE", "SCHEDULED"}        # a campaign that sends, or will, without anyone touching it
STUCK = {"PAUSED", "PENDING", "BLOCKED"}
MAX_AGE_DAYS = 21
MAX_BYTES = 10_000_000
SETTLE_MINUTES = 90
UNTESTED_AFTER_DAYS = 8
REPAIR_DAYS = 14
DEFAULT_FOLDER = Path(os.getenv("V2_CSV_FOLDER") or Path.home() / "Downloads")
# #domain-inboxes-expiry-reminder, where the renewal checks and classic placement posts go. Not HEALTH_NOTIFY_CHANNEL:
# on the PC that falls back to REMINDER_CHANNEL, an archived channel, so every post failed (found 7 Nov 2026).
INFRA_CHANNEL = "C0AGVSUNEFP"
DEFAULT_STATE = Path(os.getenv("V2_STATE_DIR") or HERE.parents[1] / "v2-weekly")
# Clients whose inboxes the job only reports, never changes in Smartlead (7 Nov 2026: Melior pulls go through
# Ryan's path until he agrees to automatic ones). --act-clients overrides this for a run.
REPORT_ONLY_CLIENTS = {"Melior"}
REPORT_ONLY_WHY = {"Melior": "until Ryan agrees"}
REPORT_ONLY_OWNER = {"Melior": "Ryan"}   # who decides a report-only client's stuck campaign
# Mongo (db smartlead.config.HEALTH_HISTORY_DB), read on Render: is the PC job alive, and what did v2 say.
HEARTBEAT_COLLECTION = "v2_heartbeat"
VERDICTS_COLLECTION = "v2_verdicts"
VERDICT_DAYS = 42                        # nights older than this are no longer re-sent to Mongo


class FleetIncomplete(RuntimeError):
    """An account could not be read in full: acting on part of the fleet could undo good state."""


def now_ist() -> datetime:
    return datetime.now(v2.IST)


def other_dnu(tags: list[str]) -> list[str]:
    return [t for t in tags if t != TAG and t.strip().lower().startswith(DNU)]


# --- state ---------------------------------------------------------------------------

def load_state(d: Path) -> dict:
    p = d / "state.json"
    s = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    for k, empty in (("files", {}), ("results", {}), ("holds", {}), ("pulled", {}), ("returned", []),
                     ("reminded", []), ("log", []), ("overrides", {}), ("repair", []), ("file_holds", {}),
                     ("clients", {}), ("types", {}), ("dt_tests", {}), ("synced", {})):
        s.setdefault(k, empty)
    s.setdefault("needs_action", False)
    s.setdefault("since", None)
    s.setdefault("failed_on", None)
    s.setdefault("repair_day", None)
    s.setdefault("unposted", [])
    s.setdefault("last_post_ok", None)
    s.setdefault("rules", None)
    return s


def save_state(d: Path, s: dict) -> None:
    d.mkdir(parents=True, exist_ok=True)
    s["log"] = s["log"][-200:]
    tmp = d / "state.json.tmp"
    tmp.write_text(json.dumps(s, indent=1, sort_keys=True, default=list), encoding="utf-8")
    os.replace(tmp, d / "state.json")


# --- files ---------------------------------------------------------------------------

def scan(folder: Path, state: dict, now: datetime, unrecognised: list | None = None) -> list[tuple[Path, str, str]]:
    """New v2 CSVs in the folder: [(path, sha256, text)]. A CSV with a Mailbox column but another header
    (Smartlead changed the download?) goes to ``unrecognised`` as (path, sha256), once per file."""
    out = []
    cutoff = now.timestamp() - MAX_AGE_DAYS * 86400
    for p in sorted(folder.glob("*.csv")):
        try:
            st = p.stat()
            if st.st_mtime < cutoff or st.st_size > MAX_BYTES:
                continue
            raw = p.read_bytes()
        except OSError:
            continue
        sha = hashlib.sha256(raw).hexdigest()
        known = state["files"].get(sha)
        if known and known.get("status") != "unrecognised":
            continue
        text = raw.decode("utf-8-sig", errors="replace")
        if v2.is_v2_csv(text):
            out.append((p, sha, text))
        elif not known and unrecognised is not None and v2.looks_like_v2(text):
            unrecognised.append((p, sha))
    return out


def ingest(files, state: dict, state_dir: Path, history: bool, now: datetime, persist: bool) -> dict:
    """Read new files into state. Returns {ingested: [...], pending: [...], nights: set of nights to act on}.
    A test from before the job's first day (state["since"]) is filed as history whatever the flags say."""
    got = {"ingested": [], "pending": [], "nights": set(), "unrecognised": []}
    for path, sha, text in files:
        rows = v2.parse(text)
        night = v2.night_of(rows)
        info = {"name": path.name, "night": night, "rows": len(rows), "seen": now.isoformat(timespec="minutes")}
        if not v2.file_ready(rows) or not night:
            state["files"][sha] = {**info, "status": "pending"}
            got["pending"].append(path.name)
            continue
        summary = v2.summarise(rows)
        sent_last = v2.last_sent(rows)
        info["per_provider"] = v2.emails_per_provider(rows)
        info["by_provider"] = v2.emails_by_provider(rows)
        info["pending_seeds"] = v2.pending_seeds(rows)
        old = history or bool(state.get("since") and night < state["since"])
        for mb, s in summary.items():
            res = {"night": night, "verdict": s["verdict"], "google": s["google"], "microsoft": s["microsoft"],
                   "sent_last": sent_last, "file": sha[:12], "history": old}
            state["results"][mb] = v2.merge_results(state["results"].get(mb, []), [res])
        state["files"][sha] = {**info, "status": "history" if old else "ingested", "inboxes": len(summary)}
        if persist:
            keep = state_dir / "runs"
            keep.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, keep / f"{night}__{sha[:8]}__{path.name}")
        got["ingested"].append(path.name)
        if not old:
            got["nights"].add(night)
            state["needs_action"] = True
    return got


def unsettled(state: dict, now: datetime) -> list[str]:
    """Nights whose newest file arrived less than SETTLE_MINUTES ago (more may still be coming)."""
    newest: dict[str, str] = {}
    for f in state["files"].values():
        if f.get("status") == "ingested" and f.get("night"):
            newest[f["night"]] = max(newest.get(f["night"], ""), f.get("seen") or "")
    cut = (now - timedelta(minutes=SETTLE_MINUTES)).isoformat(timespec="minutes")
    return sorted(n for n, seen in newest.items() if seen > cut)


def rejudge(state: dict, state_dir: Path) -> int:
    """Results stored under older rules are judged again from the kept copy of their file, so a pending
    or BOUNCED seed recorded before the 7 Nov rules is not left standing as a WATCH or FAIL (it would
    reach Render through v2_verdicts). Once per rules version. Returns how many verdicts changed."""
    if state.get("rules") == v2.RULES:
        return 0
    changed = 0
    for sha, f in state["files"].items():
        if f.get("status") not in ("ingested", "history") or not f.get("night") or not f.get("name"):
            continue
        try:
            raw = (state_dir / "runs" / f"{f['night']}__{sha[:8]}__{f['name']}").read_bytes()
        except OSError:
            continue
        for mb, s in v2.summarise(v2.parse(raw.decode("utf-8-sig", errors="replace"))).items():
            for x in state["results"].get(mb, []):
                if x.get("file") == sha[:12] and x["night"] == f["night"]:
                    changed += x["verdict"] != s["verdict"]
                    x.update(verdict=s["verdict"], google=s["google"], microsoft=s["microsoft"])
    state["rules"] = v2.RULES
    return changed


def compute_holds(state: dict, fleet: dict | None) -> None:
    """Held providers per night and per test file, leaving out inboxes already known bad that night
    (pulled before it, or carrying a person's DO NOT USE tag). Per file too, so one broken test among a
    night's good ones is held on its own (audit PLACEMENT-10)."""
    tags = {e: i["tags"] for f in (fleet or {}).values() for e, i in f["inboxes"].items()}
    nights: dict[str, dict] = defaultdict(dict)
    files: dict[str, dict] = defaultdict(dict)
    for mb, rs in state["results"].items():
        for x in rs:
            nights[x["night"]][mb] = x
            if x.get("file"):
                files[x["file"]][mb] = x

    def known_bad(results: dict) -> set[str]:
        return {mb for mb, x in results.items()
                if str((state["pulled"].get(mb) or {}).get("on") or "9999") < x["night"] or other_dnu(tags.get(mb, []))}

    for night, results in nights.items():
        state["holds"][night] = sorted(v2.held_providers(results, exclude=known_bad(results)))
    state["file_holds"] = {}
    for fid, results in files.items():
        held = v2.held_providers(results, exclude=known_bad(results))
        if held:
            state["file_holds"][fid] = sorted(held)


def held_for(state: dict, x: dict) -> set[str]:
    """Providers set aside for one result: held for its night, or for its test file."""
    return set(state["holds"].get(x["night"]) or []) | set(state.get("file_holds", {}).get(x.get("file") or "") or [])


def effective(results: list[dict], state: dict, inbox_type: str | None = None) -> list[dict]:
    """Results as the job judges them: held providers set aside, then the verdict at the inbox's own provider
    (GMAIL: Google, OUTLOOK: Microsoft; any other type, or none: both providers)."""
    return [{**x, "verdict": v2.own_verdict(x, inbox_type, held_for(state, x))} for x in results]


def judged_nights(state: dict, mb: str) -> list[str]:
    """Nights on which an inbox got a verdict at its own provider (holds not applied), oldest first."""
    kind = (state.get("types") or {}).get(mb)
    return [x["night"] for x in state["results"].get(mb, []) if v2.own_verdict(x, kind) in v2.JUDGED]


def remember_fleet(state: dict, fleet: dict) -> None:
    """What the hourly runs need without reading Smartlead: each inbox's client (for v2_verdicts) and type
    (GMAIL / OUTLOOK / SMTP, for the own-provider verdict), and each DT test campaign's inboxes (for the
    Tuesday reminder)."""
    state["clients"] = {e: i["client"] for f in fleet.values() for e, i in f["inboxes"].items()}
    state["types"] = {e: str(i.get("type") or "") for f in fleet.values() for e, i in f["inboxes"].items()}
    dt = {}
    for f in fleet.values():
        email_of = {i["id"]: e for e, i in f["inboxes"].items()}
        for c in f.get("dt", {}).values():
            if c["senders"]:
                dt[c["name"]] = sorted(email_of[i] for i in c["senders"] if i in email_of)
    state["dt_tests"] = dt


def report_only_clients(args) -> set[str]:
    acting = {c.strip().lower() for c in str(getattr(args, "act_clients", "") or "").split(",") if c.strip()}
    return {c for c in REPORT_ONLY_CLIENTS if c.lower() not in acting}


def plan(state: dict, nights=None) -> dict[str, dict]:
    """{mailbox: decision} for every inbox with results, each judged at its own provider, plus the other
    inboxes of a failed domain. An inbox whose tag a person removed is not pulled again for a result they
    already overrode (state overrides: {email: night}).

    Failed domains: per client and sending domain, the inboxes whose new (not history) result in ``nights``
    (the nights being acted on; by default the newest night that has one) failed at their own provider are
    counted against every inbox the job knows on the domain (state clients: the fleet, tested or not). Half
    or more fails the domain. Every other inbox on it is then pulled with the reason "its domain failed (N
    of M inboxes in spam)", and none of its inboxes returns while it still fails. The decision carries
    ``domain`` {name, failed, inboxes}."""
    types, clients = state.get("types") or {}, state.get("clients") or {}
    if nights is None:
        new = [x["night"] for rs in state["results"].values() for x in rs if not x.get("history")]
        nights = {max(new)} if new else set()
    out, shown = {}, {}
    for mb, rs in state["results"].items():
        eff = effective(rs, state, types.get(mb))
        d = v2.decide(eff, state["pulled"].get(mb))
        judged = judged_nights(state, mb)
        if d["action"] == v2.PULL and judged and judged[-1] <= str(state["overrides"].get(mb) or ""):
            d = {"action": None, "why": "override"}
        out[mb] = d
        mine = [x for x in eff if x["night"] in nights and not x.get("history")]
        if mine:        # the result the job acts on: the newest judged one of these nights, else the newest
            shown[mb] = next((x for x in reversed(mine) if x["verdict"] in v2.JUDGED), mine[-1])

    def key(mb: str) -> tuple:
        return clients.get(mb), mb.rsplit("@", 1)[-1]

    sizes = Counter(key(mb) for mb in clients)           # every inbox the job knows on the domain, tested or not
    failed = v2.failed_domains({mb: x["verdict"] for mb, x in shown.items()}, clients, sizes)
    newest: dict[tuple, str] = {}
    for mb, x in shown.items():
        newest[key(mb)] = max(newest.get(key(mb), ""), x["night"])
    for mb in sorted(set(clients) | set(state["results"])) if failed else ():
        if key(mb) not in failed or (shown.get(mb) or {}).get("verdict") == v2.FAIL:
            continue                                     # another domain, or it failed itself (decided above)
        n, m = failed[key(mb)]
        dom = {"name": key(mb)[1], "failed": n, "inboxes": m}
        why = f"its domain failed ({n} of {_inboxes(m)} in spam)"
        d = out.get(mb) or {"action": None, "why": "no result"}
        if d["action"] == v2.RETURN:
            out[mb] = {"action": None, "why": f"{d['why']}, but {why}", "domain": dom, "waits": True}
        elif d["action"] == v2.PULL:
            continue                                     # pulled for its own results (two weeks below 4/4)
        elif mb in state["pulled"]:
            out[mb] = {**d, "domain": dom}               # already out; the report names it with its domain
        elif newest[key(mb)] <= str(state["overrides"].get(mb) or ""):
            out[mb] = {"action": None, "why": "override"}
        else:
            out[mb] = {"action": v2.PULL, "why": why, "domain": dom}
    return out


def reconcile(state: dict, fleet: dict, today: str) -> list[str]:
    """The tag is the truth: a pulled inbox whose tag a person removed is theirs again."""
    notes = []
    tags = {e: i["tags"] for f in fleet.values() for e, i in f["inboxes"].items()}
    for e in list(state["pulled"]):
        if e in tags and TAG not in tags[e]:
            rs = judged_nights(state, e)
            state["overrides"][e] = rs[-1] if rs else today
            state["pulled"].pop(e)
            notes.append(f"{e}: a person removed its DO NOT USE tag, so the job leaves it alone until its next result.")
        elif e not in tags:
            state["pulled"].pop(e)
            notes.append(f"{e}: no longer in Smartlead, dropped from the job's list.")
    return notes


def still_attached(state: dict, fleet: dict) -> dict[str, list[str]]:
    """Pulled, still tagged, yet in a live non-test campaign (one it was the only sender of, or one
    it was added to again): {account: [emails]} to pull again."""
    out = defaultdict(list)
    for acc, f in fleet.items():
        live = {i for c in f["camps"].values() for i in c["senders"]}
        for e, inf in f["inboxes"].items():
            if e in state["pulled"] and TAG in inf["tags"] and inf["id"] in live:
                out[acc].append(e)
    return out


# --- Smartlead -----------------------------------------------------------------------

def _client(account: str, key: str):
    from smartlead.api import SmartleadClient
    return SmartleadClient(key, account)


async def load_fleet() -> dict:
    """Inboxes, live non-test campaigns and DT test campaigns of the client accounts. Raises
    FleetIncomplete if an account's key is missing, so state is never reconciled against half a fleet."""
    from smartlead.accounts import discover_accounts
    from smartlead.report_sheet import client_label
    found = {a.name.upper(): a for a in discover_accounts() if a.name.upper() in ACCOUNTS}
    missing = [a for a in ACCOUNTS if a not in found]
    if missing:
        raise FleetIncomplete(f"no Smartlead key loaded for {', '.join(missing)} (check --env-dir)")
    fleet = {}
    for name, acc in found.items():
        async with _client(acc.name, acc.api_key) as c:
            inboxes = {}
            for a in await c.list_email_accounts():
                e = str(a.get("from_email") or "").strip().lower()
                if e and a.get("id") is not None:
                    tags = [str(t.get("tag_name") or t.get("name") or "") for t in (a.get("tags") or []) if isinstance(t, dict)]
                    inboxes[e] = {"id": int(a["id"]), "client": client_label(acc.name, a.get("client_id")), "tags": tags,
                                  "type": str(a.get("type") or "").strip().upper()}
            if not inboxes:
                raise FleetIncomplete(f"{name}: Smartlead returned no inboxes")
            camps, dt = {}, {}
            for cp in await c.list_campaigns() or []:
                if not v2.is_live(cp.get("status")):
                    continue
                ea = await c.get_campaign_email_accounts(str(cp["id"])) or []
                row = {"name": str(cp.get("name") or cp["id"]), "status": str(cp.get("status")).upper(),
                       "senders": {int(a["id"]) for a in ea if a.get("id") is not None}}
                if str(cp.get("name") or "").strip().upper().startswith("DT "):
                    dt[int(cp["id"])] = row
                elif not v2.is_test_campaign(cp.get("name")):
                    camps[int(cp["id"])] = row
        fleet[acc.name] = {"key": acc.api_key, "inboxes": inboxes, "camps": camps, "dt": dt}
    return fleet


async def _lead_status(c, cid: int) -> dict[int, str]:
    out = {}
    for r in await c.get_campaign_leads(str(cid)):
        lid = (r.get("lead") or {}).get("id")
        if lid:
            out[int(lid)] = str(r.get("status", "")).upper()
    return out


async def _tag_id(c, create: bool) -> int | None:
    tags = {t.get("name"): t.get("id") for t in await c.list_tags()}
    if TAG not in tags and create:
        await c._request_json("POST", "/tags", {"name": TAG, "color": "#E53935"})   # as pull_batch.py, 4 Nov
        tags = {t.get("name"): t.get("id") for t in await c.list_tags()}
    return tags.get(TAG)


async def finish_move(c, job: dict, status: str, rep: dict) -> bool:
    """After an inbox left a campaign: move its leads, resume the ones the removal stalled.
    True when done; False to retry later (error, or the campaign is not sending yet)."""
    try:
        if not job.get("reallocated"):
            re = await c._request_json("POST", f"/campaigns/{job['cid']}/reallocate-mailboxes",
                                       {"from_email_account_ids": job["from"]})
            rep["leads"]["moved"] += int((re or {}).get("leads_reallocated") or 0)
            job["reallocated"] = True
        after = await _lead_status(c, job["cid"])
        if "stalled" not in job:             # first look after the removal: these are the leads it stalled
            before = {int(k): v for k, v in job.pop("before", {}).items()}
            job["stalled"] = [lid for lid, s in after.items() if s in STUCK and before.get(lid) not in STUCK]
        newly = [lid for lid in job["stalled"] if after.get(lid) in STUCK]   # never a lead paused later by others
        if not newly:
            return True
        if status != "ACTIVE":
            job["waiting"] = len(newly)          # resumed once the campaign is active again
            return False
        failed = 0
        for lid in newly:
            try:
                await c.resume_lead(str(job["cid"]), lid)
                rep["leads"]["resumed"] += 1
            except Exception:  # noqa: BLE001
                failed += 1
        if failed:
            rep["errors"].append(f"{job['name']}: {failed} lead(s) could not be resumed; retried next run")
        return not failed
    except Exception as exc:  # noqa: BLE001
        rep["errors"].append(f"{job['name']}: moving leads failed ({type(exc).__name__}); retried next run")
        return False


def _new_rep() -> dict:
    return {"removed": defaultdict(list), "blocked": defaultdict(list), "parked": defaultdict(list),
            "tagged": [], "failed": [], "added": defaultdict(list), "skipped": [], "errors": [], "notes": [],
            "leads": Counter(), "report_only": {}}


async def do_pulls(account: str, f: dict, emails: list[str], write: bool, today: str, state: dict, save) -> dict:
    rep = _new_rep()
    ids = {f["inboxes"][e]["id"]: e for e in emails}
    bad = {i["id"] for i in f["inboxes"].values() if TAG in i["tags"] or other_dnu(i["tags"])} - set(ids)
    p = v2.pull_plan(set(ids), f["camps"], also_bad=bad)
    for cid, mine in p["blocked"].items():
        for i in mine:
            rep["blocked" if f["camps"][cid]["status"] in SENDING else "parked"][ids[i]].append(cid)
    if not write:
        for cid, mine in p["remove"].items():
            for i in mine:
                rep["removed"][ids[i]].append(cid)
        rep["tagged"] = [e for e in emails if e not in rep["removed"]]
        return rep
    async with _client(account, f["key"]) as c:
        tid = await _tag_id(c, create=True)
        if not tid:
            rep["errors"].append(f"{account}: could not create the DO NOT USE tag, so nothing was pulled")
            return rep
        id_list = sorted(ids)
        for k in range(0, len(id_list), 25):
            await c.assign_tags(id_list[k:k + 25], [tid])
        for i, e in ids.items():
            rec = state["pulled"].get(e) or {"on": today, "account": account, "id": i, "campaigns": []}
            rec["blocked"] = rep["blocked"].get(e, []) + rep["parked"].get(e, [])
            state["pulled"][e] = rec
            if TAG not in f["inboxes"][e]["tags"]:
                f["inboxes"][e]["tags"].append(TAG)
        save()
        for cid, mine in p["remove"].items():
            camp = f["camps"][cid]
            try:
                before = await _lead_status(c, cid)
            except Exception as exc:  # noqa: BLE001
                rep["errors"].append(f"{camp['name']}: could not read its leads ({type(exc).__name__}); nothing removed, retried next run")
                rep["failed"] += [ids[i] for i in mine]
                continue
            try:
                await c.remove_campaign_email_accounts(str(cid), mine)
            except Exception as exc:  # noqa: BLE001
                rep["errors"].append(f"{camp['name']}: the removal call failed ({type(exc).__name__}); checked below")
            try:
                attached = {int(a["id"]) for a in await c.get_campaign_email_accounts(str(cid))}
            except Exception as exc:  # noqa: BLE001
                rep["errors"].append(f"{camp['name']}: could not check its senders after the removal "
                                     f"({type(exc).__name__}); check this campaign by hand")
                rep["failed"] += [ids[i] for i in mine]
                continue
            still = [i for i in mine if i in attached]
            if still:
                rep["errors"].append(f"{camp['name']}: {len(still)} inbox(es) still attached after removal; retried next run")
                rep["failed"] += [ids[i] for i in still]
            mine = [i for i in mine if i not in attached]
            if not mine:
                continue
            for i in mine:
                rec = state["pulled"][ids[i]]
                rec["campaigns"] = sorted(set(rec["campaigns"]) | {cid})
                rep["removed"][ids[i]].append(cid)
            camp["senders"] -= set(mine)
            job = {"account": account, "cid": cid, "name": camp["name"], "from": mine, "on": today,
                   "before": {str(k): v for k, v in before.items()}, "reallocated": False}
            state["repair"].append(job)
            save()
            if await finish_move(c, job, camp["status"], rep):
                state["repair"].remove(job)
            save()
    planned = {ids[i] for mine in p["remove"].values() for i in mine}
    rep["tagged"] = [e for e in emails if e not in planned]
    return rep


async def run_repairs(fleet: dict, state: dict, today: str, save) -> dict:
    """Retry lead moves left half-done (error, or the campaign was paused at the time)."""
    rep = _new_rep()
    for job in list(state["repair"]):
        f = fleet.get(job["account"])
        camp = (f or {}).get("camps", {}).get(int(job["cid"]))
        age = (date.fromisoformat(today) - date.fromisoformat(job.get("on") or today)).days
        if f is None or camp is None or age > REPAIR_DAYS:
            state["repair"].remove(job)
            why = "the campaign is no longer live" if camp is None else f"{REPAIR_DAYS} days passed"
            rep["notes"].append(f"{job['name']}: stopped retrying the lead move ({why}); "
                                f"{job.get('waiting', 'some')} lead(s) may still be paused there.")
            save()
            continue
        async with _client(job["account"], f["key"]) as c:
            if await finish_move(c, job, camp["status"], rep):
                state["repair"].remove(job)
        save()
    return rep


async def do_returns(account: str, f: dict, emails: list[str], write: bool, today: str, state: dict, save) -> dict:
    rep = _new_rep()
    ok = []
    for e in emails:
        others = other_dnu(f["inboxes"][e]["tags"])
        if others:
            rep["skipped"].append(f"{e} passed twice but also has the tag \"{others[0]}\", so the job left it out.")
            continue
        ok.append(e)
        rep["added"][e] = [int(c) for c in state["pulled"][e].get("campaigns") or [] if int(c) in f["camps"]]
    if not write:
        return rep
    async with _client(account, f["key"]) as c:
        tid = await _tag_id(c, create=False)
        for e in ok:
            i = f["inboxes"][e]["id"]
            try:
                for cid in rep["added"].get(e, []):
                    if i not in f["camps"][cid]["senders"]:
                        await c.add_campaign_email_accounts(str(cid), [i])
                        f["camps"][cid]["senders"].add(i)
                if tid:
                    await c._request_json("DELETE", "/email-accounts/tag-mapping",
                                          {"email_account_ids": [i], "tag_ids": [int(tid)]})
            except Exception as exc:  # noqa: BLE001
                rep["errors"].append(f"{e}: return failed ({type(exc).__name__}); retried next run")
                rep["added"].pop(e, None)
                continue
            state["returned"].append({"email": e, "on": today, "campaigns": rep["added"].get(e, [])})
            state["pulled"].pop(e, None)
            save()
    return rep


# --- report --------------------------------------------------------------------------

def _range(vals: list[int]) -> str:
    lo, hi = min(vals), max(vals)
    return f"{lo}" if lo == hi else f"{lo}-{hi}"


def _seeds(items: list[tuple[str, dict]]) -> str:
    g = [x["google"].get("inbox", 0) for _, x in items if (x.get("google") or {}).get("reported")]
    m = [x["microsoft"].get("inbox", 0) for _, x in items if (x.get("microsoft") or {}).get("reported")]
    return ", ".join(p for p in (f"Google {_range(g)}/4" if g else "", f"Microsoft {_range(m)}/4" if m else "") if p)


def _names(cids, names: dict) -> str:
    return ", ".join(names.get(int(c), str(c)) for c in cids)


def status_of(mb: str, x: dict, eff: str, decision: dict, acted: dict, pulled_before: set, in_fleet: bool) -> str:
    """What happened to this inbox in this run, for the report."""
    if mb in acted.get("report_only", {}):
        return "report_only_" + _ro_kind(acted["report_only"][mb])
    if mb in acted["failed"] and mb not in acted["removed"]:
        return "pull_failed"
    if mb in acted["removed"]:
        return "pulled"
    if mb in acted["blocked"]:
        return "blocked"
    if mb in acted["parked"]:
        return "parked"
    if mb in acted["tagged"]:
        return "tagged"
    if mb in pulled_before and x["verdict"] in (v2.FAIL, v2.WATCH):
        return "out_before"
    if not in_fleet and x["verdict"] in (v2.FAIL, v2.WATCH, v2.NOT_SENT):
        return "missing"
    if eff != x["verdict"] and x["verdict"] in (v2.FAIL, v2.WATCH):
        return "held"
    if decision.get("why") == "override":
        return "override"
    return {v2.FAIL: "fail_idle", v2.WATCH: "watch", v2.NOT_SENT: "not_sent", v2.INCOMPLETE: "incomplete"}.get(x["verdict"], "")


def _ro_kind(rec: dict) -> str:
    """What the job would do to one report-only inbox, in the order the acting path reports it: take it out
    of a campaign, leave it as the last good sender of a live one, leave it in a paused/draft one, or only tag it."""
    return next((k for k in ("removed", "blocked", "parked") if rec.get(k)), "tagged")


def _report_only_tail(kind: str, client: str, items: list[dict], names: dict) -> str:
    """What the job would have done to a group of a report-only client's inboxes, all of one kind (review V2-S2:
    one mixed group said '2 of 2 inboxes ... would stay there' for an inbox in no campaign). Nothing was done."""
    cids = _names(sorted({c for r in items for c in r.get(kind, [])}), names)
    tail = {"removed": f"would be taken out of {cids}",
            "blocked": f"last good sender of live {cids}, so it would stay there",
            "parked": f"only failed senders left in paused/draft {cids}, so it would stay there; add a good sender "
                      f"before resuming it",
            "tagged": "in no campaign; would be tagged DO NOT USE"}[kind]
    why = REPORT_ONLY_WHY.get(client, "")
    return f"{tail} ({client}: report-only{' ' + why if why else ''}); nothing changed in Smartlead"


def _inboxes(n: int) -> str:
    return f"{n} inbox{'es' if n != 1 else ''}"


def _and(bits: list[str]) -> str:
    return bits[0] if len(bits) == 1 else ", ".join(bits[:-1]) + " and " + bits[-1]


def _ro_would(kinds: Counter) -> str:
    """'take 1 inbox out of campaigns and tag 1 inbox that is in no campaign': counted by what would happen."""
    bits = []
    if kinds["removed"]:
        bits.append(f"take {_inboxes(kinds['removed'])} out of campaigns")
    n = kinds["blocked"]
    if n:
        bits.append(f"leave {_inboxes(n)} in {'a live campaign where it is the last good sender' if n == 1 else 'live campaigns where they are the last good senders'}")
    n = kinds["parked"]
    if n:
        bits.append(f"leave {_inboxes(n)} in {'a paused/draft campaign that has' if n == 1 else 'paused/draft campaigns that have'} no good sender")
    n = kinds["tagged"]
    if n:
        bits.append(f"tag {_inboxes(n)} that {'is' if n == 1 else 'are'} in no campaign")
    return _and(bits)


def _domain_tail(client: str, es: list[str], acted: dict, names: dict, write: bool) -> str:
    """What happened to the other inboxes of a failed domain: '2 more inboxes taken out of <campaigns>; tagged
    DO NOT USE'. Counted by kind when they differ (out of a campaign, last good sender, paused/draft, in no
    campaign, removal failed). A report-only client's inboxes read 'would be' and end as its other lines do."""
    report_only = acted.get("report_only") or {}
    ro = any(e in report_only for e in es)
    kinds: dict[str, list[str]] = {}
    for e in es:
        if e in report_only:
            k = _ro_kind(report_only[e])
        elif e in acted["failed"] and e not in acted["removed"]:
            k = "failed"
        else:
            k = next((k for k in ("removed", "blocked", "parked") if e in acted[k]), "tagged")
        kinds.setdefault(k, []).append(e)

    def camps(k: str) -> str:
        ids = set()
        for e in kinds[k]:
            ids |= set((report_only[e].get(k) if e in report_only else acted[k].get(e)) or [])
        return _names(sorted(ids), names)

    bits = []
    for k in ("failed", "removed", "blocked", "parked", "tagged"):
        if k not in kinds:
            continue
        stay = f", so {'it' if len(kinds[k]) == 1 else 'they'} would stay there" if ro else ""
        if k == "failed":
            bit = "removal failed (see Needs you), retried next run"
        elif k == "removed":
            bit = f"{'taken' if write and not ro else 'would be taken'} out of {camps(k)}"
        elif k == "blocked":
            bit = f"last good sender of live {camps(k)}{stay or ': not removed (see Needs you)'}"
        elif k == "parked":
            bit = f"left in paused/draft {camps(k)}{stay}"
        else:
            bit = "in no campaign"
        bits.append(bit if len(kinds) == 1 else f"{len(kinds[k])} {bit}")
    more = f"{len(es)} more inbox{'es' if len(es) != 1 else ''}"
    what = f"{more} {bits[0]}" if len(bits) == 1 else f"{more}: {', '.join(bits)}"
    if ro:
        why = REPORT_ONLY_WHY.get(client, "")
        return f"{what} ({client}: report-only{' ' + why if why else ''}); nothing changed in Smartlead"
    return f"{what}; {'' if write else 'would be '}tagged DO NOT USE"


def _unjudged_why(x: dict) -> list[str]:
    """Why a result that was not judged needs the test fixed: 'bounced' (a seed bounced), or the provider with
    no seed rows at all. A pending seed or a short test is reported by its own Needs-you line."""
    out = []
    provs = [(p, x.get(p) or {}) for p in ("google", "microsoft")]
    if any(int(d.get("bounced") or 0) and d.get("verdict") == v2.INCOMPLETE for _, d in provs):
        out.append("bounced")
    out += [p for p, d in provs if not int(d.get("sent") or 0) and d.get("verdict") != v2.NOT_SENT]
    return out


def _incomplete_tail(items: list[tuple[str, dict]]) -> str:
    """Why a result was not judged, from its seed counts."""
    provs = [x.get(p) or {} for _, x in items for p in ("google", "microsoft")]
    odd = sorted({int(p.get("sent") or 0) for p in provs if 0 < int(p.get("sent") or 0) != v2.PASS_SEEDS})
    if odd and odd[0] < v2.PASS_SEEDS:
        n = odd[0]
        return f"the test sent {n} email{'s' if n != 1 else ''} per provider, too few to judge; nothing done"
    if odd:
        return f"the test sent {odd[0]} emails per provider; the job judges only 4-email tests; nothing done"
    bounced = any(int(p.get("bounced") or 0) for p in provs)
    pending = any(int(p.get("sent") or 0) - int(p.get("reported") or 0) - int(p.get("bounced") or 0) > 0 for p in provs)
    why = [w for w, on in (("a seed bounced (a seed-side problem)", bounced),
                           ("a seed had not answered when the file was downloaded", pending)) if on]
    return f"{' and '.join(why) or 'a provider has no result'}, so it was not judged; nothing done"


def _shown(rs: list[dict], nights=None) -> dict:
    """The result the report shows for an inbox: its newest (among ``nights`` when given). When that one was
    not judged (a short test, a pending seed) and an earlier one was, the judged one is shown: it is the result
    the job acts on, so the line and the headline say 'fail', not 'incomplete'."""
    mine = [x for x in rs if nights is None or x["night"] in nights] or rs
    last = mine[-1]
    if last["verdict"] == v2.INCOMPLETE:
        judged = [x for x in mine if x["verdict"] in v2.JUDGED]
        if judged:
            return judged[-1]
    return last


def build_report(state: dict, got: dict, decisions: dict, fleet: dict | None, acted: dict, write: bool,
                 pulled_before: set, act_from: str | None = None, behind: int | None = None,
                 report_only_clients: set | None = None) -> tuple[list[str], list[str]]:
    nights = sorted(got["nights"])
    types = state.get("types") or {}

    def own(mb: str) -> list[dict]:
        """An inbox's results with the verdict at its own provider (holds not applied)."""
        return [{**x, "verdict": v2.own_verdict(x, types.get(mb))} for x in state["results"][mb]]

    latest = {mb: _shown(own(mb), got["nights"]) for mb, rs in state["results"].items()
              if rs and rs[-1]["night"] in got["nights"]}
    count = Counter(x["verdict"] for x in latest.values())
    tonight = dict(latest)
    report_only = acted.get("report_only") or {}
    touched = (set(acted["removed"]) | set(acted["blocked"]) | set(acted["parked"]) | set(acted["tagged"])
               | set(acted["failed"]) | set(report_only))
    # Inboxes out because their domain failed, not for a result of their own: one line per domain, below.
    dom_pulled = {e: d["domain"] for e, d in decisions.items() if e in touched and d.get("domain")}
    for e in touched - set(dom_pulled):
        if e not in latest and state["results"].get(e):
            latest[e] = _shown(own(e))                      # acted on from an older result: still named
    fleet = fleet or {}
    client_of = {e: i["client"] for f in fleet.values() for e, i in f["inboxes"].items()}
    names = {cid: c["name"] for f in fleet.values() for cid, c in {**f["camps"], **f.get("dt", {})}.items()}
    would = "" if write else "would be "
    per_dom = Counter(e.split("@")[1] for e in latest)

    groups = defaultdict(list)
    for mb, x in sorted(latest.items()):
        if mb in dom_pulled:
            continue
        eff = v2.own_verdict(x, types.get(mb), held_for(state, x))
        st = status_of(mb, x, eff, decisions.get(mb, {}), acted, pulled_before, mb in client_of)
        if st:
            groups[(client_of.get(mb, "Not in Smartlead"), st, x["verdict"], mb.split("@")[1])].append((mb, x))

    n_out = len(acted["removed"])
    camp_ids = {c for cs in acted["removed"].values() for c in cs}
    n_camps = len(camp_ids)
    # "live" only when every one of them sends: on 10 Nov 2026 the line said "12 live campaigns" for 3 sending
    # and 9 paused ones (an inbox is taken out of paused campaigns too, so it cannot come back on a resume).
    status = {str(cid): c.get("status") for f in (fleet or {}).values() for cid, c in (f.get("camps") or {}).items()}
    n_send = sum(1 for c in camp_ids if status.get(str(c)) in SENDING)
    if n_camps and n_send < n_camps and status:
        camps_text = (f"{n_camps} campaign{'s' if n_camps != 1 else ''} ({n_send} sending, "
                      f"{n_camps - n_send} paused or draft)")
    else:
        camps_text = f"{n_camps} live campaign{'s' if n_camps != 1 else ''}"
    n_tag = len([e for e in acted["tagged"] if e not in acted["blocked"] and e not in acted["parked"]])
    n_park = len([e for e in acted["parked"] if e not in acted["removed"]])
    no_full = count[v2.INCOMPLETE] + count[v2.NOT_SENT]
    when = " and ".join(f"{date.fromisoformat(n):%a} {date.fromisoformat(n).day} {date.fromisoformat(n):%b}" for n in nights)
    head = [f"v2 placement results, tests of {when or '-'}",
            f"{len(tonight)} inboxes · {count[v2.PASS]} pass · {count[v2.WATCH]} watch · {count[v2.FAIL]} fail"
            + (f" · {no_full} no full result" if no_full else ""),
            f"{'Took' if write else 'Would take'} {n_out} inbox{'es' if n_out != 1 else ''} out of {camps_text} "
            f"· {'tagged' if write else 'would tag'} "
            + (f"{n_park} left in paused/draft campaigns and " if n_park else "")
            + f"{n_tag} in no campaign · {'returned' if write else 'would return'} {len(acted['added'])}"]
    if not write and act_from:
        ro_note = f" ({', '.join(sorted(report_only_clients))} stays report-only)" if report_only_clients else ""
        head.append(f"Watch week: nothing is changed in Smartlead until {act_from}, then the job acts on its own{ro_note}.")

    needs, info = [], list(acted["notes"]) + list(acted["skipped"])
    for name in got.get("unrecognised") or []:
        needs.append(f"unrecognised v2 CSV format: {name}. It has a Mailbox column but not the header the job reads "
                     f"({', '.join(v2.HEADER)}), so nothing was read from it.")
    for night in nights:
        if state["holds"].get(night):
            needs.append(f"{' and '.join(p.title() for p in state['holds'][night])} results for {night} held: nearly every "
                         f"inbox was below 4/4 there, which looks like a test problem, so nothing was pulled for it. "
                         f"Re-run that test in Smart Delivery today.")
    file_of = {sha[:12]: f for sha, f in state["files"].items()}
    for fid, provs in sorted(state.get("file_holds", {}).items()):
        f = file_of.get(fid) or {}
        extra = [p for p in provs if p not in (state["holds"].get(f.get("night")) or [])]
        if f.get("night") in got["nights"] and extra:
            needs.append(f"{' and '.join(p.title() for p in extra)} results in {f.get('name', fid)} ({f['night']}) held: "
                         f"nearly every inbox in that test was below 4/4 there, which looks like a test problem, so "
                         f"nothing was pulled for it. Re-run that test in Smart Delivery today.")
    ro_kinds: dict[str, Counter] = defaultdict(Counter)
    for e, rec in report_only.items():
        ro_kinds[client_of.get(e, "Not in Smartlead")][_ro_kind(rec)] += 1
    for client, kinds in sorted(ro_kinds.items()):
        why = REPORT_ONLY_WHY.get(client, "")
        needs.append(f"{client} is report-only{' ' + why if why else ''}, so nothing changed in Smartlead for it. "
                     f"The job would {_ro_would(kinds)}. List under *{client}* below.")
    stuck, ro_stuck = defaultdict(list), defaultdict(list)
    for e, cids in acted["blocked"].items():
        for cid in cids:
            stuck[cid].append(e)
    for e, rec in report_only.items():
        for cid in rec.get("blocked", []):
            ro_stuck[cid].append(e)
    for cid, es in sorted(stuck.items()):
        needs.append(f"{names.get(cid, cid)} ({cid}) is live with only failed inboxes left ({', '.join(sorted(es))}). "
                     f"Not changed: pause it or add a good sender.")
    for cid, es in sorted(ro_stuck.items()):
        client = client_of.get(sorted(es)[0], "Not in Smartlead")
        why, owner = REPORT_ONLY_WHY.get(client, ""), REPORT_ONLY_OWNER.get(client)
        needs.append(f"{names.get(cid, cid)} ({cid}) is live with only failed inboxes left ({', '.join(sorted(es))}). "
                     f"Not changed, as {client} is report-only{' ' + why if why else ''}: "
                     f"{'ask ' + owner + ' to ' if owner else ''}pause it or add a good sender.")
    needs += [f"Error: {err}" for err in acted["errors"]]
    if got["pending"]:
        needs.append(f"Downloaded before the results were in (download again later): {', '.join(got['pending'])}")
    dead = Counter()
    for k, items in groups.items():
        if k[1] == "not_sent":
            dead[k[3]] += len(items)
    for dom, n in sorted(dead.items()):
        needs.append(f"{dom}: {n} inbox{'es' if n != 1 else ''} did not send in the test. Check "
                     f"{'they are' if n != 1 else 'it is'} connected in Smartlead.")
    thin = sorted((f["name"], f["per_provider"], f.get("by_provider") or {}) for f in state["files"].values()
                  if f.get("night") in got["nights"] and f.get("status") == "ingested"
                  and 0 < int(f.get("per_provider") or v2.PASS_SEEDS) < v2.PASS_SEEDS)
    for name, n, by in thin:
        if len(set(by.values())) > 1:       # the two providers got different counts: name both
            sends = " and ".join(f"{c} {p.title()}" for p, c in sorted(by.items())) + " emails per inbox"
            needs.append(f"{name}: this test sends {sends}, so the job can't judge its inboxes (it needs 4 of 4 at "
                         f"each). In Smart Delivery, set the test to 4 emails per provider and check its seed list.")
            continue
        needs.append(f"{name}: this test sends {n} email{'s' if n != 1 else ''} per provider, so the job can't judge its "
                     f"inboxes (it needs 4 of 4). In Smart Delivery, set the test to 4 emails per provider.")
    current = {x.get("file") for rs in state["results"].values() for x in rs}
    partial = sorted((f["name"], f["pending_seeds"]) for sha, f in state["files"].items()
                     if f.get("night") in got["nights"] and f.get("status") == "ingested" and f.get("pending_seeds")
                     and sha[:12] in current)
    for name, n in partial:
        needs.append(f"{name}: {n} seed result{'s' if n != 1 else ''} had not come in when it was downloaded, so "
                     f"those inboxes were not judged. Download it again later today.")
    # A bounced seed, or a provider with no seed rows, leaves inboxes not judged at all: no fail is pulled and no
    # pulled inbox comes back until the test is fixed, so it always needs a person (review V2-S1).
    unjudged = defaultdict(set)
    for mb, rs in state["results"].items():
        for x in rs:
            if (x["night"] in got["nights"] and not x.get("history")
                    and v2.own_verdict(x, types.get(mb)) == v2.INCOMPLETE):
                for why in _unjudged_why(x):
                    unjudged[(x.get("file") or "", why)].add(mb)
    for (fid, why), es in sorted(unjudged.items()):
        name, n = (file_of.get(fid) or {}).get("name") or "a test file", len(es)
        what = (f"a seed bounced for {_inboxes(n)}" if why == "bounced"
                else f"no {why.title()} seed results for {_inboxes(n)}")
        needs.append(f"{name}: {what}, so {'it was' if n == 1 else 'they were'} not judged. Check the test's seed list "
                     f"in Smart Delivery{'' if why == 'bounced' else ' (it needs 4 Google and 4 Microsoft seeds)'} "
                     f"and re-run it.")
    weekly = any(v2.is_weekly_night(n) for n in nights)
    if nights and not weekly:
        info.append("Not a weekly test (those start on a Saturday, Sunday or Monday), so the weekly checks "
                    "(missing tests, untested senders) were skipped.")
    tested = set(tonight)
    no_result, waiting, in_dt = [], set(), set()
    for acc, f in fleet.items():
        for c in f.get("dt", {}).values():
            in_dt |= {(acc, i) for i in c["senders"]}
            if c["senders"] and not any(e in tested for e, i in f["inboxes"].items() if i["id"] in c["senders"]):
                no_result.append(c["name"])
                waiting |= {(acc, i) for i in c["senders"]}
    if no_result and weekly:
        needs.append(f"No result this week from {', '.join(sorted(no_result))} ({len(waiting)} inbox{'es' if len(waiting) != 1 else ''}): "
                     f"download {'their CSVs' if len(no_result) != 1 else 'its CSV'} into Downloads.")
    untested, missed = defaultdict(list), defaultdict(list)
    if weekly:
        newest = date.fromisoformat(max(nights))
        for acc, f in fleet.items():
            live = {i for c in f["camps"].values() if c["status"] in SENDING for i in c["senders"]}
            for e, inf in f["inboxes"].items():
                if inf["id"] not in live or TAG in inf["tags"] or other_dnu(inf["tags"]) or (acc, inf["id"]) in waiting:
                    continue
                rs = state["results"].get(e) or []
                last = rs[-1]["night"] if rs else None
                if not last or (newest - date.fromisoformat(last)).days > UNTESTED_AFTER_DAYS:
                    (missed if (acc, inf["id"]) in in_dt else untested)[(inf["client"], e.split("@")[1])].append(last or "never tested")
    if missed:
        n = sum(len(v) for v in missed.values())
        needs.append(f"{n} inbox{'es' if n != 1 else ''} sit in a DT campaign but were missing from its v2 test results: "
                     f"the test may hold an old list of inboxes, so re-create that test from the campaign. List below.")
    if untested:
        n = sum(len(v) for v in untested.values())
        d = len(untested)
        needs.append(f"{n} inbox{'es' if n != 1 else ''} on {d} domain{'s' if d != 1 else ''} send in live campaigns but "
                     f"are in no DT test: add {'the domains' if d != 1 else 'the domain'} to the deliverability sheet "
                     f"(dt_senders fills the DT tests from it). List below.")
    head.append("\n" + (f"*Needs you: {len(needs)}*" if needs else "Nothing needs you."))

    parts = []
    if needs:
        parts.append("*Needs you*\n\n" + "\n".join(f"• {x}" for x in needs))
    by_client = defaultdict(list)
    by_domain = defaultdict(list)
    for e, dom in sorted(dom_pulled.items()):
        by_domain[(client_of.get(e, "Not in Smartlead"), dom["name"])].append(e)
    for (client, name), es in sorted(by_domain.items()):
        dom = dom_pulled[es[0]]
        by_client[client].append(f"• domain failed · {name} · {dom['failed']} of {_inboxes(dom['inboxes'])} in spam → "
                                 + _domain_tail(client, es, acted, names, write))
    waits = defaultdict(list)
    for e, d in decisions.items():
        if d.get("waits"):
            waits[(d["domain"]["name"], d["domain"]["failed"], d["domain"]["inboxes"])].append(e)
    for (name, n, m), es in sorted(waits.items()):
        info.append(f"{name} still fails ({n} of {_inboxes(m)} in spam), so {_inboxes(len(es))} that passed twice "
                    f"stay{'s' if len(es) == 1 else ''} out of campaigns until it no longer does.")
    out_before = defaultdict(lambda: [0, set()])
    order = {"pull_failed": -1, "pulled": 0, "report_only_removed": 0.5, "blocked": 1, "report_only_blocked": 1.5,
             "parked": 2, "report_only_parked": 2.5, "tagged": 3, "report_only_tagged": 3.5, "held": 4,
             "override": 5, "fail_idle": 6, "watch": 7, "not_sent": 8, "incomplete": 9, "missing": 10}
    for (client, st, verdict, dom), items in sorted(groups.items(), key=lambda kv: (kv[0][0], order.get(kv[0][1], 99), kv[0][3])):
        es = [e for e, _ in items]
        if st == "out_before":
            out_before[client][0] += len(items)
            out_before[client][1].add(dom)
            continue
        if st.startswith("report_only_"):
            pre = "below 4/4 two weeks running: " if verdict == v2.WATCH else ""
            tail = pre + _report_only_tail(st[len("report_only_"):], client, [report_only[e] for e in es], names)
        elif st == "pulled":
            cids = sorted({c for e in es for c in acted["removed"].get(e, [])})
            pre = "below 4/4 two weeks running: " if verdict == v2.WATCH else ""
            tail = f"{pre}{'taken' if write else 'would be taken'} out of {_names(cids, names)}; {would}tagged DO NOT USE"
        elif st == "pull_failed":
            tail = "removal failed (see Needs you), retried next run; tagged DO NOT USE"
        elif st == "blocked":
            cids = sorted({c for e in es for c in acted["blocked"][e]})
            tail = f"last good sender of live {_names(cids, names)}: not removed (see Needs you); {would}tagged DO NOT USE"
        elif st == "parked":
            cids = sorted({c for e in es for c in acted["parked"][e]})
            tail = (f"only failed senders left in paused/draft {_names(cids, names)}: left there, sends nothing while "
                    f"paused; add a good sender before resuming it; {would}tagged DO NOT USE")
        elif st == "tagged":
            tail = f"in no campaign; {would}tagged DO NOT USE so it is not used"
        elif st == "held":
            tail = "not pulled: that provider's results are held as a test problem; still in its campaigns"
        elif st == "override":
            tail = "not pulled: a person removed its DO NOT USE tag; still in its campaigns"
        elif st == "fail_idle":
            tail = "not pulled (no action decided)"
        elif st == "watch":
            if all(v2.is_weekly_night(x["night"]) for _, x in items):
                tail = "2-3 of 4 seeds in the inbox. Stays in campaigns; if next week's test is below 4/4 too, it counts as failed"
            else:
                tail = "2-3 of 4 seeds in the inbox. Stays in campaigns; a one-off test never counts toward two weeks running"
        elif st == "not_sent":
            tail = "did not send in the test (see Needs you)"
        elif st == "incomplete":
            tail = _incomplete_tail(items)
        else:
            tail = "not in Smartlead"
        who = items[0][0] if len(items) == 1 else f"{len(items)} of {per_dom[dom]} inboxes"
        label = verdict.lower().replace("_", " ")
        by_client[client].append(" · ".join(x for x in (f"• {label}", dom, who, _seeds(items)) if x) + f" → {tail}")
    for client in sorted(set(by_client) | set(out_before)):
        lines = list(by_client.get(client, []))
        if client in out_before:
            n, doms = out_before[client]
            lines.append(f"• Still failing, already out from an earlier run: {n} inbox{'es' if n != 1 else ''} "
                         f"on {len(doms)} domain{'s' if len(doms) != 1 else ''}")
        parts.append(f"*{client}*\n\n" + "\n".join(lines))
    # Inboxes acted on with no stored v2 result (pulled by hand, e.g. after a one-off test) are not in any
    # group above, so name them here: every inbox the headline counts must be listed.
    no_result = sorted(e for e in (set(acted["removed"]) | set(acted["blocked"]) | set(acted["parked"]) | set(acted["failed"]))
                       if e not in latest and e not in dom_pulled)
    if no_result:
        lines = []
        for e in no_result:
            if e in acted["removed"]:
                what = f"{'taken' if write else 'would be taken'} out of {_names(acted['removed'][e], names)}"
            elif e in acted["blocked"]:
                what = f"last good sender of live {_names(acted['blocked'][e], names)}: not removed (see Needs you)"
            elif e in acted["parked"]:
                what = f"only failed senders left in paused/draft {_names(acted['parked'][e], names)}: left there"
            else:
                what = "removal failed (see Needs you), retried next run"
            lines.append(f"• {e} ({client_of.get(e, 'not in Smartlead')}) → {what}")
        parts.append("*Pulled earlier and still in campaigns* (tagged DO NOT USE, no v2 result stored)\n\n"
                     + "\n".join(lines))
    if acted["added"]:
        parts.append("*Back in campaigns* (passed twice, 6+ days apart)\n\n"
                     + "\n".join(f"• {e} → {_names(c, names) or 'no live campaign left'}" for e, c in sorted(acted["added"].items())))
    if missed:
        parts.append("*In a DT campaign but missing from its v2 test*\n\n" + "\n".join(
            f"• {dom} ({client}): {len(v)} inbox{'es' if len(v) != 1 else ''}, last v2 {max(v)}"
            for (client, dom), v in sorted(missed.items())))
    if untested:
        parts.append("*Sending but in no DT test*\n\n" + "\n".join(
            f"• {dom} ({client}): {len(v)} inbox{'es' if len(v) != 1 else ''}, last v2 {max(v)}"
            for (client, dom), v in sorted(untested.items())))
    if behind:
        info.append(f"The job's code on the PC is {behind} commit{'s' if behind != 1 else ''} behind main (as last "
                    f"fetched). To ship it, see 'Ship a code change' in docs/V2_WEEKLY.md.")
    if info:
        parts.append("*For info*\n\n" + "\n".join(f"• {x}" for x in info))
    return head, parts


def _post_noted(state: dict, ctx: dict | None, head: list[str], parts: list[str]) -> bool:
    """Post, and remember whether Slack took it: a refused post writes UNPOSTED to the log and makes the
    run exit non-zero (audit PLACEMENT-1: an empty channel setting kept every post silent for a week)."""
    ok = bool(post(head, parts))
    state["last_post_ok"] = ok
    if not ok:
        print(f"UNPOSTED: {head[0] if head else '(no headline)'}")
        if ctx is not None:
            ctx["post_failed"] = True
    return ok


def post_or_queue(state: dict, head: list[str], parts: list[str], ctx: dict | None = None) -> bool:
    """Post; if Slack refuses, keep it in state so the next run tries again (the job is never silent)."""
    if _post_noted(state, ctx, head, parts):
        return True
    state["unposted"] = (state["unposted"] + [{"head": head, "parts": parts}])[-5:]
    print("  queued: the next run tries to post it again")
    return False


def _channel() -> str:
    return (os.getenv("V2_WEEKLY_CHANNEL") or "").strip() or INFRA_CHANNEL


def post(head: list[str], parts: list[str]) -> bool:
    from smartlead.notify import post_with_thread
    token = os.getenv("SLACK_BOT_TOKEN", "")
    if not token:
        print("  SLACK_BOT_TOKEN missing: not posted")
        return False
    return bool(post_with_thread(token, _channel(), head, parts))


def reminder_lines(night: str, missing: list[str], some_in: bool) -> list[str]:
    """The Tuesday reminder: which DT tests have no result for the week of that Monday."""
    d = date.fromisoformat(night)
    day = f"the week of Mon {d.day} {d:%b}"
    if some_in:
        n = len(missing)
        return [f"v2 results for {day}: {n} DT test{'s' if n != 1 else ''} not in yet",
                f"\nMissing: {', '.join(missing)}.",
                f"\nDownload {'their CSVs' if n != 1 else 'its CSV'} from Smart Delivery v2 into Downloads on Manveen's "
                f"PC. The job reads {'them' if n != 1 else 'it'} within the hour; {'their' if n != 1 else 'its'} inboxes get "
                f"no verdict until then."]
    return ([f"v2 results for {day} are not in yet"]
            + ([f"\nMissing: {', '.join(missing)} ({len(missing)} test{'s' if len(missing) != 1 else ''})."]
               if missing else [])
            + ["\nDownload each test's CSV from Smart Delivery v2 into Downloads on Manveen's PC. "
               "The job reads them within the hour. Until then nothing is pulled or returned."])


def unrecognised_lines(names: list[str]) -> list[str]:
    many = len(names) != 1
    return [f"unrecognised v2 CSV format: {', '.join(names)}",
            f"\n{'These files have' if many else 'The file has'} a Mailbox column but not the header the job reads "
            f"({', '.join(v2.HEADER)}), so nothing was read from {'them' if many else 'it'}. If Smart Delivery changed "
            f"its download, the job needs a code change before it can read the results."]


def _mark_told(state: dict, names: list[str]) -> None:
    for f in state["files"].values():
        if f.get("status") == "unrecognised" and f.get("name") in names:
            f["told"] = True


# --- Mongo (read on Render), git, self-check -------------------------------------------

def _short(exc: BaseException, n: int = 160) -> str:
    """An error for the log, Slack or Mongo: no keys, tokens or connection strings."""
    from smartlead.notify import redact
    text = re.sub(r"mongodb(\+srv)?://\S+", "mongodb://<hidden>", redact(f"{type(exc).__name__}: {exc}"))
    return " ".join(text.split())[:n]


def _mongo_db():
    """The shared Mongo database (MONGO_URI in the .env, db HEALTH_HISTORY_DB), as the other jobs reach it."""
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client[HEALTH_HISTORY_DB]


def _close(db) -> None:
    try:
        getattr(db, "client", None) and db.client.close()
    except Exception:  # noqa: BLE001
        pass


def _counts(p: dict | None) -> dict:
    return {k: int((p or {}).get(k) or 0) for k in ("inbox", "reported", "sent")}


def _fingerprint(doc: dict) -> str:
    return hashlib.sha1(json.dumps({k: v for k, v in doc.items() if k != "updated_at"}, sort_keys=True,
                                   default=list).encode()).hexdigest()[:12]


def verdict_docs(state: dict, now: datetime) -> dict[str, dict]:
    """v2_verdicts docs (shared contract), one per inbox per night: nights from the last VERDICT_DAYS days
    whose holds an acting run has worked out and whose files stopped arriving. The verdict is the one the job
    acts on: at the inbox's own provider, held providers set aside (``held`` names them). The readers of
    v2_verdicts (usable list, gate self-test, renewal review, grid) therefore agree with what was pulled."""
    cut = (now.date() - timedelta(days=VERDICT_DAYS)).isoformat()
    skip = set(unsettled(state, now))
    at = now.astimezone(timezone.utc).isoformat(timespec="seconds")
    out = {}
    for mb, rs in state["results"].items():
        pulled_on = (state["pulled"].get(mb) or {}).get("on")
        for x in rs:
            n = x["night"]
            if n < cut or n in skip or n not in state["holds"]:
                continue
            held = held_for(state, x)
            doc = {"_id": f"{mb}|{n}", "email": mb, "domain": mb.split("@", 1)[-1], "client": state["clients"].get(mb),
                   "night": n, "verdict": v2.own_verdict(x, (state.get("types") or {}).get(mb), held),
                   "google": _counts(x.get("google")), "microsoft": _counts(x.get("microsoft")),
                   "history": bool(x.get("history")), "weekly": v2.is_weekly_night(n), "pulled_on": pulled_on,
                   "held": sorted(held), "updated_at": at}
            out[doc["_id"]] = doc
    return out


def write_verdicts(col, docs: list[dict]) -> None:
    from pymongo import ReplaceOne
    for k in range(0, len(docs), 500):
        col.bulk_write([ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in docs[k:k + 500]], ordered=False)


def heartbeat(state: dict, now: datetime, ctx: dict, errors: list[str]) -> dict:
    """The v2_heartbeat doc (shared contract): Render alarms when it goes stale."""
    read = [f for f in state["files"].values() if f.get("status") in ("ingested", "history")]
    nights = [f["night"] for f in read if f.get("night")]
    commit, behind = ctx.get("git") or (None, None)
    return {"_id": "pc", "last_run": now.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "newest_night": max(nights) if nights else None, "files_read": len(read), "mode": ctx.get("mode") or "warn",
            "last_post_ok": state.get("last_post_ok"), "commit": commit, "behind_main": behind,
            "errors": [str(e)[:200] for e in errors][:10]}


def record(state_dir: Path, ctx: dict) -> None:
    """After a saved run: the verdicts that changed, then the heartbeat. Never raises; a Mongo problem is
    printed, and put in the heartbeat's errors when the heartbeat itself can still be written."""
    db = None
    try:
        state = load_state(state_dir)
        now = now_ist()
        errors = list(ctx["errors"])
        if state["unposted"]:
            errors.append(f"{len(state['unposted'])} Slack post(s) refused and queued; tried again next run")
        try:
            db = _mongo_db()
        except Exception as exc:  # noqa: BLE001
            print(f"[v2-weekly] Mongo not reached ({_short(exc)}): heartbeat and verdicts not written")
            return
        try:
            docs = verdict_docs(state, now)
            fresh = [d for i, d in docs.items() if state["synced"].get(i) != _fingerprint(d)]
            if fresh:
                write_verdicts(db[VERDICTS_COLLECTION], fresh)
                print(f"[v2-weekly] Mongo: {len(fresh)} verdict(s) written")
            state["synced"] = {i: _fingerprint(d) for i, d in docs.items()}
        except Exception as exc:  # noqa: BLE001
            errors.append(f"v2_verdicts not written: {_short(exc)}")
            print(f"[v2-weekly] {errors[-1]}")
        try:
            db[HEARTBEAT_COLLECTION].replace_one({"_id": "pc"}, heartbeat(state, now, ctx, errors), upsert=True)
        except Exception as exc:  # noqa: BLE001
            print(f"[v2-weekly] v2_heartbeat not written: {_short(exc)}")
        save_state(state_dir, state)
    except Exception as exc:  # noqa: BLE001
        print(f"[v2-weekly] Mongo record failed: {_short(exc)}")
    finally:
        _close(db)


def git_facts() -> tuple[str | None, int | None]:
    """(commit, commits behind origin/main as last fetched) of the checkout the job runs from.
    (None, None) when git is missing or fails: never a reason to stop."""
    exe = shutil.which("git") or shutil.which("git.exe")
    if not exe:
        return None, None
    kw = {"cwd": str(HERE.parent), "capture_output": True, "text": True, "timeout": 20, "stdin": subprocess.DEVNULL}
    if sys.platform == "win32":
        kw["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0)     # pythonw: no console flash

    def git(*a: str) -> str | None:
        try:
            r = subprocess.run([exe, *a], **kw)
            return r.stdout.strip() if r.returncode == 0 else None
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
    commit = git("rev-parse", "--short", "HEAD")
    behind = git("rev-list", "--count", "HEAD..origin/main")
    return commit, int(behind) if behind and behind.isdigit() else None


def _slack_api(method: str, token: str, **params) -> dict:
    import requests
    resp = requests.get(f"https://slack.com/api/{method}", headers={"Authorization": f"Bearer {token}"},
                        params=params, timeout=15)
    return resp.json()


async def self_check(args) -> int:
    """Everything the hourly run needs, checked without posting or changing anything (audit PLACEMENT-1:
    a blank channel setting kept every post silent and nothing noticed). Exit 1 if any check fails."""
    rows: list[tuple[bool, str, str]] = []
    token, channel = os.getenv("SLACK_BOT_TOKEN", ""), _channel()
    if not token:
        rows.append((False, "Slack", "SLACK_BOT_TOKEN is not set"))
    else:
        try:
            data = _slack_api("conversations.info", token, channel=channel)
            ch = data.get("channel") or {}
            name = f"#{ch.get('name') or channel}"
            if data.get("error") == "missing_scope":
                rows.append((False, f"Slack channel {channel}", f"the bot token lacks the {data.get('needed') or 'channels:read'} "
                             f"scope, so the channel can't be checked without posting"))
            elif not data.get("ok"):
                rows.append((False, f"Slack channel {channel}", f"Slack says {data.get('error')}"))
            elif ch.get("is_archived"):
                rows.append((False, f"Slack channel {channel}", f"{name} is archived"))
            elif not ch.get("is_member"):
                rows.append((False, f"Slack channel {channel}", f"the bot is not a member of {name}: invite it"))
            else:
                rows.append((True, f"Slack channel {channel}", f"{name}, the bot is a member"))
        except Exception as exc:  # noqa: BLE001
            rows.append((False, f"Slack channel {channel}", _short(exc)))
    from smartlead.accounts import discover_accounts
    found = {a.name.upper(): a for a in discover_accounts() if a.name.upper() in ACCOUNTS}
    for name in ACCOUNTS:
        acc = found.get(name)
        if not acc:
            rows.append((False, f"Smartlead {name}", "no key loaded (check --env-dir)"))
            continue
        try:
            async with _client(acc.name, acc.api_key) as c:
                await c._get("/email-accounts", {"offset": 0, "limit": 1})
            rows.append((True, f"Smartlead {name}", "key works"))
        except Exception as exc:  # noqa: BLE001
            rows.append((False, f"Smartlead {name}", _short(exc)))
    folder = Path(args.folder)
    try:
        n = sum(1 for x in os.listdir(folder) if x.lower().endswith(".csv"))
        rows.append((True, f"Downloads folder {folder}", f"readable, {n} CSV file{'s' if n != 1 else ''}"))
    except OSError as exc:
        rows.append((False, f"Downloads folder {folder}", _short(exc)))
    db = None
    try:
        db = _mongo_db()
        rows.append((True, "Mongo", f"reachable, database {getattr(db, 'name', '?')}"))
    except Exception as exc:  # noqa: BLE001
        rows.append((False, "Mongo", _short(exc)))
    finally:
        _close(db)
    for ok, what, detail in rows:
        print(f"{'PASS' if ok else 'FAIL'}  {what}: {detail}")
    bad = sum(1 for ok, _, _ in rows if not ok)
    print(f"[v2-weekly] self-check: {len(rows) - bad} passed, {bad} failed")
    return 1 if bad else 0


# --- main ----------------------------------------------------------------------------

def _merge(into: dict, rep: dict) -> None:
    for k in ("removed", "blocked", "parked", "added"):
        for e, v in rep[k].items():
            into[k][e] = into[k].get(e, []) + list(v)
    for k in ("tagged", "failed", "skipped", "errors", "notes"):
        into[k] += rep[k]
    into["leads"].update(rep["leads"])
    into["report_only"].update(rep.get("report_only") or {})


def _writes(args, today: str) -> bool:
    """--apply, and not before --act-from."""
    act_from = getattr(args, "act_from", None)
    return bool(getattr(args, "apply", False)) and not (act_from and today < act_from)


def _crash_alert(state_dir: Path, exc: BaseException, ctx: dict) -> None:
    """One post a day while the job keeps failing; a refused post is tried again next run."""
    try:
        state = load_state(state_dir)
        today = now_ist().date().isoformat()
        if state.get("failed_on") != today:
            if _post_noted(state, ctx, ["v2 weekly job failed", "\n" + _short(exc, 200),
                                        f"\nIt tries again every hour and says so here once a day while it keeps "
                                        f"failing. Log: {state_dir / 'task.log'}"], []):
                state["failed_on"] = today
            save_state(state_dir, state)
    except Exception:  # noqa: BLE001
        traceback.print_exc()


async def run(args) -> int:
    state_dir = Path(args.state_dir)
    persist = bool(args.apply or args.warn or args.history_only)
    lock = state_dir / "run.lock"
    if lock.exists() and datetime.now().timestamp() - lock.stat().st_mtime < 50 * 60:
        print("[v2-weekly] another run is in progress; skipping")
        return 0
    if persist:
        state_dir.mkdir(parents=True, exist_ok=True)
        lock.write_text(datetime.now().isoformat(), encoding="utf-8")
    ctx = {"errors": [], "post_failed": False, "git": (None, None),
           "mode": ("act" if _writes(args, now_ist().date().isoformat()) else "warn") if persist else "dry"}
    try:
        ctx["git"] = git_facts()
        try:
            rc = await _run(args, state_dir, persist, ctx)
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            ctx["errors"].append(f"crashed: {_short(exc)}")
            rc = 2
            if persist and getattr(args, "post", False):
                _crash_alert(state_dir, exc, ctx)
        if ctx["post_failed"] and rc == 0:
            rc = 3
        if persist:
            record(state_dir, ctx)
        return rc
    finally:
        if persist and lock.exists():
            lock.unlink()


async def _run(args, state_dir: Path, persist: bool, ctx: dict | None = None) -> int:
    ctx = ctx if ctx is not None else {"errors": [], "post_failed": False, "git": (None, None)}
    state = load_state(state_dir)
    now = now_ist()
    today = now.date().isoformat()
    state["since"] = state["since"] or today
    write = _writes(args, today)
    can_post = bool(args.post) and persist
    behind = (ctx.get("git") or (None, None))[1]

    def save():
        if persist:
            save_state(state_dir, state)

    if persist:
        n = rejudge(state, state_dir)
        if n:
            print(f"[v2-weekly] {n} stored result(s) judged again under the {v2.RULES} rules")
    if can_post and state["unposted"]:
        for item in list(state["unposted"]):
            if _post_noted(state, ctx, item["head"], item["parts"]):
                state["unposted"].remove(item)
        save()
    odd: list = []
    got = ingest(scan(Path(args.folder), state, now, odd), state, state_dir, args.history_only, now, persist)
    if persist:
        for p, sha in odd:
            state["files"][sha] = {"name": p.name, "status": "unrecognised", "seen": now.isoformat(timespec="minutes"),
                                   "told": False}
        got["unrecognised"] = sorted({f["name"] for f in state["files"].values()
                                      if f.get("status") == "unrecognised" and not f.get("told")})
    else:
        got["unrecognised"] = sorted(p.name for p, _ in odd)
    print(f"[v2-weekly] {now:%Y-%m-%d %H:%M} IST ({'act' if write else 'warn' if persist else 'dry run'}): "
          f"{len(got['ingested'])} new file(s), {len(got['pending'])} not ready"
          + (f", {len(odd)} unrecognised" if odd else "")
          + (f": {', '.join(got['ingested'])}" if got["ingested"] else "")
          + (f"; code {behind} commit(s) behind origin/main" if behind else ""))
    save()
    if args.history_only:
        print(f"[v2-weekly] filed as history: {', '.join(got['ingested']) or '-'}; no action")
        return 0

    acting = state["needs_action"] or args.replan or (got["ingested"] and not persist)
    if acting and persist and not args.replan and unsettled(state, now):
        print(f"[v2-weekly] waiting: files for {', '.join(unsettled(state, now))} arrived in the last "
              f"{SETTLE_MINUTES} minutes; acting once no more come")
        return 0
    if not acting:
        nights_in = {n for n in (i.get("night") for i in state["files"].values()
                                 if i.get("status") in ("ingested", "history")) if n}
        mon = v2.week_start(v2.last_monday(now.date())).isoformat()      # the week's tests may start Sat or Sun
        tested = {e for e, rs in state["results"].items() if any(x["night"] >= mon for x in rs)}
        missing = v2.dt_missing(state["dt_tests"], tested)
        due = v2.reminder_due((now.date(), now.hour), nights_in, set(state["reminded"]), state["since"], missing)
        if due and can_post and _post_noted(state, ctx, reminder_lines(due, missing, any(n >= due for n in nights_in)), []):
            state["reminded"].append(due)
        if got["pending"] and can_post:
            _post_noted(state, ctx, [f"v2 CSVs downloaded before the results were in: {', '.join(got['pending'])}",
                                     "\nDownload them again in a few hours. Nothing was read from them."], [])
        if got["unrecognised"] and can_post and _post_noted(state, ctx, unrecognised_lines(got["unrecognised"]), []):
            _mark_told(state, got["unrecognised"])
        if state["repair"] and write and state["repair_day"] != today:
            fleet = await load_fleet()
            remember_fleet(state, fleet)
            rep = await run_repairs(fleet, state, today, save)
            state["repair_day"] = today
            ctx["errors"] += rep["errors"]
            print(f"[v2-weekly] repairs: {dict(rep['leads'])}, notes {rep['notes']}, errors {rep['errors']}")
            if (rep["notes"] or rep["errors"]) and can_post:
                post_or_queue(state, ["v2 job: leads in campaigns it pulled inboxes from",
                                      "\n" + "\n".join(f"• {x}" for x in rep["errors"] + rep["notes"])], [], ctx)
        save()
        return 0

    if not persist and not args.replan and not got["nights"]:
        print("[v2-weekly] dry run: only history files (tests from before the job's first day); nothing to act on")
        return 0
    nights_now = set(got["nights"])
    if not nights_now:
        known = {f["night"] for f in state["files"].values() if f.get("status") == "ingested" and f.get("night")}
        nights_now = {max(known)} if known else set()
    got["nights"] = nights_now

    fleet = await load_fleet()
    remember_fleet(state, fleet)
    acted = _new_rep()
    if write and state["repair"]:
        _merge(acted, await run_repairs(fleet, state, today, save))
    if write:
        acted["notes"] += reconcile(state, fleet, today)
    pulled_before = set(state["pulled"])
    compute_holds(state, fleet)
    decisions = plan(state, got["nights"])
    where = {e: acc for acc, f in fleet.items() for e in f["inboxes"]}
    pulls, returns = defaultdict(list), defaultdict(list)
    for mb, d in sorted(decisions.items()):
        if d["action"] == v2.PULL and mb in where:
            pulls[where[mb]].append(mb)
        elif d["action"] == v2.RETURN and mb in where:
            returns[where[mb]].append(mb)
    for acc, emails in still_attached(state, fleet).items():
        pulls[acc] += [e for e in emails if e not in pulls[acc] and decisions.get(e, {}).get("action") != v2.RETURN]

    # Report-only clients: planned like any other inbox, but nothing in Smartlead changes for them.
    ro, client_of = report_only_clients(args), state["clients"]
    ro_pulls, ro_back = defaultdict(list), []
    for acc in list(pulls):
        ro_pulls[acc] = [e for e in pulls[acc] if client_of.get(e) in ro]
        pulls[acc] = [e for e in pulls[acc] if client_of.get(e) not in ro]
    for acc in list(returns):
        ro_back += [e for e in returns[acc] if client_of.get(e) in ro]
        returns[acc] = [e for e in returns[acc] if client_of.get(e) not in ro]

    for acc, emails in pulls.items():
        if emails:
            _merge(acted, await do_pulls(acc, fleet[acc], emails, write, today, state, save))
    for acc, emails in ro_pulls.items():
        if emails:
            rep = await do_pulls(acc, fleet[acc], emails, False, today, state, save)
            for e in emails:
                acted["report_only"][e] = {k: list(rep[k].get(e, [])) for k in ("removed", "blocked", "parked")}
    for acc, emails in returns.items():
        if emails:
            _merge(acted, await do_returns(acc, fleet[acc], emails, write, today, state, save))
    for e in ro_back:
        acted["notes"].append(f"{e} passed twice and would go back into its campaigns, but {client_of[e]} is "
                              f"report-only, so nothing was changed.")
    if acted["leads"]:
        print(f"  leads moved {acted['leads']['moved']}, resumed {acted['leads']['resumed']}")
    ctx["errors"] += acted["errors"]

    head, parts = build_report(state, got, decisions, fleet, acted, write, pulled_before, args.act_from, behind, ro)
    text = "\n".join(head) + "\n\n" + "\n\n".join(parts)
    print(text)
    state["log"].append({"at": now.isoformat(timespec="minutes"), "write": write, "nights": sorted(got["nights"]),
                         "pulled": dict(acted["removed"]), "tagged": acted["tagged"], "returned": dict(acted["added"]),
                         "blocked": dict(acted["blocked"]), "report_only": sorted(acted["report_only"]),
                         "errors": acted["errors"]})
    state["needs_action"] = False
    if persist:
        reports = state_dir / "reports"
        reports.mkdir(parents=True, exist_ok=True)
        (reports / f"{now:%Y-%m-%d_%H%M}{'' if write else '_warn'}.md").write_text(text, encoding="utf-8")
        if can_post:
            post_or_queue(state, head, parts, ctx)            # queued if refused: it counts as told
            _mark_told(state, got["unrecognised"])
        save()
    else:
        print("[v2-weekly] dry run: nothing changed in Smartlead, state not saved, nothing posted")
    return 1 if acted["errors"] else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--folder", default=str(DEFAULT_FOLDER))
    ap.add_argument("--state-dir", default=str(DEFAULT_STATE))
    ap.add_argument("--apply", action="store_true", help="change Smartlead and save state (default: dry run)")
    ap.add_argument("--warn", action="store_true", help="save state and post the plan, change nothing in Smartlead")
    ap.add_argument("--act-from", help="with --apply: behave like --warn before this date (YYYY-MM-DD)")
    ap.add_argument("--act-clients", default="",
                    help=f"comma-separated clients to act on although they are report-only by default "
                         f"({', '.join(sorted(REPORT_ONLY_CLIENTS))})")
    ap.add_argument("--post", action="store_true", help="post to Slack (with --apply or --warn)")
    ap.add_argument("--history-only", action="store_true", help="file new CSVs as history, act on nothing")
    ap.add_argument("--replan", action="store_true", help="decide again from stored results without new files")
    ap.add_argument("--self-check", action="store_true",
                    help="check the Slack channel, both Smartlead keys, the Downloads folder and Mongo; posts nothing")
    ap.add_argument("--env-dir", help="smartlead_sync folder holding the .env (read before anything else)")
    ap.add_argument("--log", help="append all output to this file (the scheduled task runs windowless)")
    args = ap.parse_args(argv)
    if args.act_from:
        date.fromisoformat(args.act_from)
    if args.log or sys.stdout is None:
        log = Path(args.log or Path(args.state_dir) / "task.log")
        log.parent.mkdir(parents=True, exist_ok=True)
        sys.stdout = sys.stderr = open(log, "a", encoding="utf-8", buffering=1)
    unknown = [c.strip() for c in args.act_clients.split(",")
               if c.strip() and c.strip().lower() not in {x.lower() for x in REPORT_ONLY_CLIENTS}]
    if unknown:
        print(f"[v2-weekly] --act-clients: {', '.join(unknown)} is not report-only; ignored")
    if args.self_check:
        return asyncio.run(self_check(args))
    return asyncio.run(run(args))


if __name__ == "__main__":
    sys.exit(main())
