"""The gate self-test's comparisons (pure). ``gate_selftest.py`` reads the sources and posts.

Every check returns {"name", "ok", "checked", "bad": [...], "says", "note"}:
``checked`` = how many inboxes (or campaigns) the check looked at, ``bad`` = the ones that disagree,
``says`` = one plain line for the post, ``note`` = something to know that is not a failure.
A check that could not run because its source was not read is NOT ok: silence must never read as agreement.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

from smartlead import usable
from smartlead.inbox_status import AUTO_TAG, REPORT_ONLY
from smartlead.zapmail_clients import client_from_tracker

HEARTBEAT_MAX_HOURS = 26      # the PC job runs hourly; a PC off overnight still reports within a day


def _reasons(row: dict) -> list[str]:
    return [x.strip() for x in str(row.get("Busy Reason") or "").split(",") if x.strip()]


def _n(k: int, one: str = "inbox", many: str = "inboxes") -> str:
    return f"{k} {one if k == 1 else many}"


def _fresh_pass(v2: dict | None, today: date) -> bool:
    if not v2 or str(v2.get("verdict") or "").upper() != "PASS":
        return False
    try:
        return (today - date.fromisoformat(str(v2.get("night") or "")[:10])).days <= usable.V2_FRESH_DAYS
    except ValueError:
        return False


def checks(inboxes: list[dict], campaigns: list[dict], rows: list[dict], v2: dict[str, dict],
           heartbeat: dict | None, now: datetime, require_pass: bool, errors: list[str] | None = None,
           tracker: set[str] | None = None) -> list[dict]:
    """``inboxes`` [{email, tags, connected, client}] from Smartlead's list call; ``campaigns`` [{client, campaign,
    emails}] the ACTIVE ones; ``rows`` the published tab; ``v2`` {email: {verdict, night}} newest judged;
    ``heartbeat`` the v2 job's doc; ``tracker`` the names in the asset tracker (None = that check is not asked for).
    ``errors`` name sources that were not read (their checks fail)."""
    errors = errors or []
    unread = lambda *names: [e for e in errors if e.startswith(names)]   # noqa: E731
    today = now.date()
    by_email = {str(r.get("Email") or "").strip().lower(): r for r in rows if str(r.get("Email") or "").strip()}
    tagged = {i["email"] for i in inboxes if i["email"] and usable.is_do_not_use(i.get("tags"))}
    auto = {i["email"] for i in inboxes if any(str(t).strip().lower() == AUTO_TAG for t in i.get("tags") or [])}
    failed = {e for e, d in v2.items() if str(d.get("verdict") or "").upper() == "FAIL"}
    out = []

    def add(name, checked, bad, good, wrong, blocked=(), note=""):
        ok = not bad and not blocked
        says = (f"not checked: {'; '.join(blocked)}" if blocked else good if not bad else wrong)
        out.append({"name": name, "ok": ok, "checked": checked, "bad": sorted(bad), "says": says, "note": note})

    # 1. the list marks every tagged inbox (only inboxes the list carries: past clients are not on it)
    on_list = sorted(e for e in tagged if e in by_email)
    bad = [e for e in on_list if str(by_email[e].get("Availability") or "").upper() != "BUSY"
           or "do_not_use" not in _reasons(by_email[e])]
    add("list_marks_tagged", len(on_list), bad,
        f"all {_n(len(on_list))} tagged DO NOT USE are BUSY on the list with the reason do_not_use",
        f"{len(bad)} of {_n(len(on_list))} tagged DO NOT USE are not marked do_not_use on the list, so Campaign Desk "
        "and gtm would not hold them back for the tag",
        blocked=unread("Smartlead", "the list"))

    # 2. the list marks every failed v2 verdict
    on_list = sorted(e for e in failed if e in by_email)
    bad = [e for e in on_list if str(by_email[e].get("Availability") or "").upper() != "BUSY"
           or "failed_v2" not in _reasons(by_email[e])]
    add("list_marks_failed_v2", len(on_list), bad,
        f"all {_n(len(on_list))} whose newest v2 test failed are BUSY on the list with the reason failed_v2",
        f"{len(bad)} of {_n(len(on_list))} that failed v2 are not marked failed_v2 on the list (it may be a day behind "
        "the verdicts)",
        blocked=unread("v2 verdicts", "the list"))

    # 3. FREE means proven, once that rule is on
    free = sorted(e for e, r in by_email.items() if str(r.get("Availability") or "").upper() == "FREE")
    if require_pass:
        bad = [e for e in free if not _fresh_pass(v2.get(e), today)]
        add("free_means_v2_pass", len(free), bad,
            f"all {_n(len(free), 'FREE inbox', 'FREE inboxes')} have a v2 pass in the last {usable.V2_FRESH_DAYS} days",
            f"{len(bad)} of {_n(len(free), 'FREE inbox', 'FREE inboxes')} have no v2 pass in the last "
            f"{usable.V2_FRESH_DAYS} days",
            blocked=unread("v2 verdicts", "the list"))
    else:
        proven = sum(1 for e in free if _fresh_pass(v2.get(e), today))
        add("free_means_v2_pass", len(free), [], f"a v2 pass is not required for FREE yet ({proven} of {len(free)} FREE "
            "inboxes have one)", "", blocked=unread("the list"))

    # 4. nothing held back is still sending
    held = tagged | failed
    acting, reported = [], []
    for c in campaigns:
        hit = sorted(set(c.get("emails") or []) & held)
        if hit:
            (reported if str(c.get("client") or "").lower() in REPORT_ONLY else acting).append((c, hit))
    bad = sorted({e for _, hit in acting for e in hit})
    where = "; ".join(f"{c['client']} · {c['campaign']} ({len(hit)})" for c, hit in acting[:6])
    note = ""
    if reported:
        k = len({e for _, hit in reported for e in hit})
        note = (f"{_n(k)} held back still {'sends' if k == 1 else 'send'} in {_n(len(reported), 'campaign', 'campaigns')} "
                "of a report-only client ("
                + "; ".join(f"{c['client']} · {c['campaign']} ({len(hit)})" for c, hit in reported[:6]) + ")")
    add("held_back_not_sending", len(campaigns), bad,
        f"no tagged or failed inbox is attached to a sending campaign ({len(campaigns)} checked)",
        f"{_n(len(bad))} tagged or failed {'is' if len(bad) == 1 else 'are'} still attached to sending campaigns: {where}",
        blocked=unread("Smartlead", "v2 verdicts"), note=note)

    # 5. the v2 job is alive and acting
    age = None
    try:
        last = datetime.fromisoformat(str((heartbeat or {}).get("last_run") or ""))
        age = (now.astimezone(timezone.utc) - last.astimezone(timezone.utc)).total_seconds() / 3600
    except ValueError:
        pass
    mode = str((heartbeat or {}).get("mode") or "")
    stale = age is None or age > HEARTBEAT_MAX_HOURS
    wrong = ("the v2 job on the PC has never reported" if age is None
             else f"the v2 job on the PC last ran {age:.0f} hours ago" if stale
             else f"the v2 job runs in {mode or 'an unknown'} mode, so it does not take failed inboxes out")
    add("v2_job_alive", 1, ["v2 job"] if stale or mode != "act" else [],
        f"the v2 job on the PC ran {0 if age is None else age:.0f} hours ago in act mode"
        + (f" ({_n(len(auto))} {'carries' if len(auto) == 1 else 'carry'} its tag)" if auto else ""), wrong,
        blocked=unread("v2 heartbeat"))

    # 6. the asset tracker knows every working inbox of a current client (what bills and expires is read from it)
    if tracker is not None:
        working = sorted(i["email"] for i in inboxes if i["email"] and i.get("connected")
                         and client_from_tracker(i.get("client")))
        bad = [e for e in working if e not in tracker]
        add("tracker_has_every_inbox", len(working), bad,
            f"all {_n(len(working), 'connected inbox', 'connected inboxes')} of current clients have a row in the "
            "asset tracker",
            f"{len(bad)} of {_n(len(working), 'connected inbox', 'connected inboxes')} of current clients have no row "
            "in the asset tracker, so no expiry reminder or bill review covers them",
            blocked=unread("Smartlead", "the tracker"))
    return out


def format_post(results: list[dict], today: date, errors: list[str] | None = None) -> tuple[list[str], list[str]]:
    """(headline lines, thread parts)."""
    bad = [r for r in results if not r["ok"]]
    day = f"{today.day} {today.strftime('%b')}"
    if bad:
        head = [f"⚠️ Gate self-test {day}: {len(bad)} of {len(results)} checks disagree"]
        head += [f"• {r['says']}" for r in bad]
    else:
        head = [f"✅ Gate self-test {day}: all {len(results)} checks agree"]
    head += [f"• Note: {r['note']}" for r in results if r.get("note")]
    thread = ["\n".join(["*Every check*"] + [f"{'✅' if r['ok'] else '❌'} {r['says']}" for r in results])]
    for r in bad:
        names = [b for b in r["bad"] if "@" in b]
        if names:
            more = len(names) - 30
            thread.append(f"*{r['name']}*\n" + ", ".join(names[:30]) + (f" and {more} more" if more > 0 else ""))
    if errors:
        thread.append("*Could not read*\n" + "\n".join(f"• {e}" for e in errors))
    return head, thread
