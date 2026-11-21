"""Warm-up repair tracker (t16, 17 Nov 2026). Manveen, 16 Nov: "make sure we are of track of improving them and
change warmup's stuff and trak thye imporve also work on simmilar inboxes in all clients".

Every inbox of PL, Melior or BettrData whose warm-up reputation is under the All Inboxes cutoff
(config.MIN_WARMUP_REP_PCT, 90, the inbox list's low_rep cutoff; one cutoff for every inbox type, parent 17 Nov) has one
document in Mongo ``warmup_repair`` until it recovers or leaves. Fleet watch's daily reading (inbox_daily ``rep``) updates it:

  * recovered: the latest reading is at or above the cutoff and warm-up is not blocked;
  * worse: the latest reading is 3+ points below the reputation at the start (a flag, not a status);
  * no_improvement: 21+ days in repair and the latest reading not above the start + 3. These are cancel candidates
    in the digest; cancelling is Manveen's call;
  * gone: the inbox left the watched inboxes (same evidence fleet watch uses to close a case).

Report-only. Nothing here changes a warm-up setting, cancels, deletes or calls Smartlead: the lead changed the warm-up
of the first ones by hand, with Manveen's approval, and seeded them from local files (warmup_repair_cli.py). Inboxes the
bot enrols by itself carry settings_after None. A recovered inbox is freed by the inbox list on its own (low_rep
clears; processing.py + usable.py), nothing here holds it back.

Every bad inbox, not only low warm-up (t325, 19 Nov 2026; Manveen: "make sure inboxes that are bad are recovering").
A connected inbox of the three clients is enrolled for any reason the gate holds it back that someone can repair
(``reasons``, the same codes as the All Inboxes tab and usable.py): low warm-up, warm-up blocked, a person's DO NOT USE
tag, the v2 job's pull tag, a v2 FAIL that holds it back, a sending domain on Spamhaus DBL / URIBL. Each doc keeps
``reasons`` (today's) and ``reasons_at_start``; PATHS says what brings each one back. It is "recovered" only when no
reason is left. Warm-up repaired (90+, not blocked) while only a person's tag still holds it = "ready": the digest asks
Manveen, who alone removes the tag (the bot never untags). Under 90 and not on the repair settings (<= 10/day, 30-40%
reply, Smartlead's guide api.smartlead.ai/guides/email-warmup, used 16 Nov) is a digest ask too. Warm-up switched off
and bad inboxes in live campaigns are fleet.py's daily asks (warmup_off, dnu_live, listed_live); one active weekly v2
test per inbox is dt_senders' coverage rule. Docs from before t325 (no ``reasons``) keep the warm-up-only rule.
Disconnected and DNS-dead inboxes (the tab's disconnected / dns_error, read from the sync's Mongo inbox_status) are
never enrolled: nothing to repair.
"""
from __future__ import annotations

from collections import Counter
from datetime import date, timedelta

from smartlead import config
from smartlead import inbox_standard as st

WORSE_BY = 3
IMPROVE_BY = 3
NO_IMPROVEMENT_DAYS = 21
# inbox_daily's client label -> the name the digest uses. Other labels (OSC, client_id 145916) are never enrolled.
CLIENTS = {"Precise Leads": "PL", "Melior": "Melior", "Bettrdata": "BettrData"}
_SEED_CLIENT = {"PL": "Precise Leads", "PRECISE LEADS": "Precise Leads", "MELIOR": "Melior", "BETTRDATA": "Bettrdata"}
SETTING_KEYS = ("status", "warmup_min_count", "warmup_max_count", "max_email_per_day", "reply_rate",
                "daily_reply_limit", "is_rampup_enabled", "rampup_value", "auto_adjust_warmup",
                "send_warmups_only_on_weekdays", "warmup_key_id")
IN_REPAIR = ("repairing", "no_improvement", "ready")
# Why a bad inbox is out, and what brings it back (t325). Codes as in the All Inboxes tab, except v2_pulled: the v2
# job's own tag (usable.V2_AUTO_TAG), which the v2 job takes off itself; do_not_use here is a person's tag only.
PATHS = {"low_rep": "its warm-up is back to 90",
         "warmup_blocked": "its warm-up is unblocked (credentials fixed first)",
         "v2_pulled": "the v2 job returns it after two v2 passes 6+ days apart",
         "failed_v2": "it passes v2 in its weekly test",
         "blocklisted": "its domain is off Spamhaus DBL / URIBL",
         "do_not_use": "a person removes the DO NOT USE tag"}
WARMUP_REASONS = ("low_rep", "warmup_blocked")
REPAIR_PER_DAY, REPAIR_REPLY = 10, (30, 40)      # the 16 Nov repair settings: 10/day, ramp 5, 35% reply


def level(kind: str | None = None) -> int:
    """The reputation counted as recovered: the All Inboxes cutoff (90) for every inbox type. inbox_standard's
    per-type good_reputation (SMTP 70) is not used here (parent, 17 Nov: one recovery cutoff)."""
    return int(config.MIN_WARMUP_REP_PCT)


def _rep(x) -> int | None:
    try:
        return int(round(float(str(x).replace("%", ""))))
    except (TypeError, ValueError):
        return None


def _kind(t: str | None) -> str:
    return st.inbox_kind({"type": t}, 1)


def _client(c: str | None) -> str:
    c = str(c or "")
    return c if c in CLIENTS else _SEED_CLIENT.get(c.upper(), c)


def _why_blocked(reason: str | None) -> str:
    r = str(reason or "").lower()
    if not r or "authentic" in r or "credential" in r:
        return "warm-up blocked: credentials must be fixed first"
    return f"warm-up blocked: {reason}"


def new_doc(email: str, account: str, client: str, typ: str | None, kind: str, started: str, rep: int | None, *,
            before: dict | None, after: dict | None, why: str | None, blocked: bool, by: str,
            reasons: list[str] | None = None) -> dict:
    d = {"email": email, "account": account, "client": client, "type": typ, "kind": kind, "started": started,
         "rep_at_start": rep, "settings_before": before, "settings_after": after, "why_not_changed": why,
         "blocked": bool(blocked), "status": "repairing", "enrolled_by": by, "recovered_on": None,
         "latest_rep": rep, "latest_on": None, "gone_on": None}
    if reasons is not None:
        d["reasons"], d["reasons_at_start"] = list(reasons), list(reasons)
    return d


def reasons(r: dict, v2: dict | None = None, listed: set[str] | None = None, prev=()) -> list[str]:
    """Why this inbox is out, from today's reading ``r`` (fleet_watch.record). ``v2`` = {email: newest judged v2
    verdict} (usable.newest_judged), ``listed`` = listed sending domains (usable.listed_domains); None = not read today,
    and the inbox's last answer (``prev``) stands. An inbox a vendor warms (``warmup_by``, t248) is never judged on its
    Smartlead warm-up. Pure."""
    from smartlead import usable
    out = []
    ext = bool(r.get("warmup_by"))
    rep = _rep(r.get("rep"))
    if not ext and rep is not None and rep < level(r.get("kind")):
        out.append("low_rep")
    if not ext and r.get("warmup_blocked"):
        out.append("warmup_blocked")
    tags = [str(t).strip().lower() for t in r.get("tags") or []]
    if any(t.startswith(usable.DO_NOT_USE) and t != usable.V2_AUTO_TAG for t in tags):
        out.append("do_not_use")
    if usable.V2_AUTO_TAG in tags:
        out.append("v2_pulled")
    if v2 is None:
        out += [x for x in ("failed_v2",) if x in prev]
    elif usable.holds_back(v2.get(str(r.get("email") or "").lower()), r.get("kind") or ""):
        out.append("failed_v2")
    dom = str(r.get("domain") or str(r.get("email") or "").split("@")[-1]).lower()
    if listed is None:
        out += [x for x in ("blocklisted",) if x in prev]
    elif dom in listed:
        out.append("blocklisted")
    return out


def off_repair_settings(d: dict) -> bool:
    """Under 90 on warm-up, warm-up on, and not on the repair settings (more than 10/day, or reply outside 30-40%).
    Settings not read = no claim. Pure."""
    if "low_rep" not in (d.get("reasons") or ()) or d.get("warmup_on") is False:
        return False
    per_day, reply = d.get("warmup_per_day"), d.get("reply_rate")
    if per_day is None or reply is None:
        return False
    return per_day > REPAIR_PER_DAY or not REPAIR_REPLY[0] <= reply <= REPAIR_REPLY[1]


def login_blocked(d: dict) -> bool:
    """Warm-up blocked because Smartlead can't log in (Smartlead's blocked_reason names authentication failures or
    credentials; a doc without a reason read falls back on why_not_changed). Another block (hard bounces ...) is not
    a login step. Pure."""
    if not d.get("blocked"):
        return False
    r = str(d.get("blocked_reason") or "").lower()
    if r:
        return "authentic" in r or "credential" in r
    return "credentials" in str(d.get("why_not_changed") or "")


def repair_would_help(d: dict) -> bool:
    """Would the repair settings help this inbox now? Not while its warm-up is blocked (Smartlead can't log in, so no
    warm-up runs: the login comes first), nor while its domain is on Spamhaus DBL / URIBL (the listing keeps it out
    whatever its warm-up; fleet.py asks about listed inboxes in live campaigns). 20 Nov 2026: 3 of the 4 inboxes
    the digest asked to put on the repair settings were warm-up blocked, the 4th on a listed domain. Pure."""
    return not d.get("blocked") and "blocklisted" not in (d.get("reasons") or ())


KIND_WORDS = {"google": "Google", "outlook": "Outlook", "outlook_dense": "Outlook", "smtp": "custom mail server"}


def kind_word(kind: str | None) -> str:
    """The inbox type in plain words ('outlook_dense' -> 'Outlook')."""
    return KIND_WORDS.get(str(kind or ""), str(kind or "inbox type not known"))


def _score(d: dict) -> str:
    rep = d.get("latest_rep")
    return f"warm-up score {rep}%" if rep is not None else "warm-up score not read"


def _blocked_lines(docs: list[dict]) -> list[str]:
    """One line per domain: client, how many, type, score, the inboxes. Plain words, no codes."""
    groups: dict[tuple, list[dict]] = {}
    for d in docs:
        groups.setdefault((d["email"].split("@")[-1], CLIENTS.get(d.get("client"), d.get("client") or "")), []).append(d)
    out = []
    for (dom, client), ds in sorted(groups.items()):
        reps = sorted(d["latest_rep"] for d in ds if d.get("latest_rep") is not None)
        score = (f"warm-up score {reps[0]}%" if reps and reps[0] == reps[-1] else
                 f"warm-up score {reps[0]}-{reps[-1]}%" if reps else "warm-up score not read")
        kinds = ", ".join(sorted({kind_word(d.get("kind")) for d in ds}))
        users = ", ".join(sorted(d["email"].split("@")[0] + "@" for d in ds))
        out.append(f"• {dom} ({client}): {_n(len(ds))}, {kinds}, {score} (good from {level()}%): {users}")
    return out


def _settings_line(d: dict) -> str:
    """'a@x.com: Outlook, warm-up score 85% (good from 90%), now 20 warm-up emails a day at 30% reply'."""
    live = d.get("live")
    return (f"• {d['email']}: {kind_word(d.get('kind'))}, {_score(d)} (good from {level(d.get('kind'))}%), now "
            f"{d.get('warmup_per_day')} warm-up emails a day at {d.get('reply_rate')}% reply"
            + (f", in {live} live campaign{'s' if live != 1 else ''}" if live else ""))


def seed_docs(changes: dict, before: dict, today: date) -> list[dict]:
    """One doc per inbox in the lead's before-snapshot (``before['inboxes']``), with the settings change from
    ``changes`` ({email: {on, before, after, ok}}) or why the settings were left alone. Pure."""
    by_email = {str(k).lower(): v for k, v in (changes or {}).items()}
    out = []
    for x in (before or {}).get("inboxes") or []:
        email = str(x.get("email") or x.get("from_email") or "").lower()
        if not email:
            continue
        w = x.get("warmup_details") or {}
        c = by_email.get(email)
        blocked = bool(w.get("is_warmup_blocked") or w.get("blocked_reason"))
        current = {k: w[k] for k in SETTING_KEYS if k in w}
        if c and c.get("ok"):
            settings_before, after, why = c.get("before") or current, c.get("after"), None
        elif c:
            settings_before, after, why = c.get("before") or current, None, \
                "the settings change did not apply; settings left as they were"
        elif blocked:
            settings_before, after, why = current, None, _why_blocked(w.get("blocked_reason"))
        else:
            settings_before, after = current, None
            why = (f"already at {w.get('warmup_max_count')}/day, {w.get('reply_rate')}% reply; "
                   "settings left as they were")
        started = str((c or {}).get("on") or today.isoformat())[:10]
        out.append(new_doc(email, str(x.get("account") or ""), _client(x.get("client")), x.get("type"),
                           _kind(x.get("type")), started, _rep(x.get("warmup_reputation") or (c or {}).get("rep_before")),
                           before=settings_before, after=after, why=why, blocked=blocked, by="seed"))
    return out


def worse(d: dict) -> bool:
    rep, start = d.get("latest_rep"), d.get("rep_at_start")
    return rep is not None and start is not None and rep <= start - WORSE_BY


def update(d: dict, reading: dict | None, today: date, why: list[str] | None = None) -> dict:
    """The doc after today's reading (None = no reading today: the last one stands). ``why`` = today's ``reasons``
    (None = not judged today: the doc's last ones stand). A doc without ``reasons`` (before t325) recovers on warm-up
    alone. Pure."""
    d = dict(d)
    if reading:
        if reading.get("rep") is not None:
            d["latest_rep"], d["latest_on"] = _rep(reading["rep"]), reading.get("date") or today.isoformat()
        d["kind"] = reading.get("kind") or d.get("kind")
        if reading.get("warmup_blocked") is not None:
            d["blocked"] = bool(reading["warmup_blocked"])
            d["blocked_reason"] = str(reading.get("blocked_reason") or "")[:200] if d["blocked"] else None
        for k in ("warmup_on", "warmup_per_day", "reply_rate"):
            if k in reading:
                d[k] = reading[k]
        if "live_campaigns" in reading:
            d["live"] = len(reading.get("live_campaigns") or [])
        d["gone_on"] = None
    if why is not None:
        if "reasons_at_start" not in d:                 # a doc from before t325 was enrolled for low warm-up
            d["reasons_at_start"] = (["low_rep"] if "reasons" not in d and "low_rep" not in why else []) + list(why)
        d["reasons"] = list(why)
    rep, start = d.get("latest_rep"), d.get("rep_at_start")
    held = d.get("reasons")
    if held is None:                                   # before t325: warm-up only
        warm_ok = rep is not None and rep >= level(d.get("kind")) and not d.get("blocked")
        other = []
    else:
        warm_ok = not any(x in WARMUP_REASONS for x in held)
        other = [x for x in held if x not in WARMUP_REASONS]
    if warm_ok and not other:
        d["status"] = "recovered"
        d["recovered_on"] = d.get("recovered_on") or today.isoformat()
        return d
    d["recovered_on"] = None
    if warm_ok:                                         # warm-up fine: something else holds it (PATHS)
        repaired = "low_rep" in (d.get("reasons_at_start") or ["low_rep"])
        d["status"] = "ready" if other == ["do_not_use"] and repaired else "repairing"
        return d
    days = (today - date.fromisoformat(str(d["started"])[:10])).days
    stuck = rep is None or start is None or rep <= start + IMPROVE_BY
    d["status"] = "no_improvement" if days >= NO_IMPROVEMENT_DAYS and stuck and not d.get("blocked") else "repairing"
    return d


def _left(email: str, roster: dict | None) -> bool:
    from smartlead import fleet as fl
    return fl.left({"domain": email.split("@")[-1], "scope": "inbox", "target": email}, roster)


def run(docs: list[dict], records: list[dict], today: date, *, read: set[str], roster: dict | None,
        v2: dict | None = None, listed: set[str] | None = None, dead: set[str] | None = None
        ) -> tuple[list[dict], list[dict]]:
    """(every doc after today's readings, the docs enrolled today). A doc missing from today's records turns
    'gone' only when its account was read in full and the roster shows the inbox left. ``v2``/``listed``: see
    ``reasons``. ``dead`` = inboxes the All Inboxes gate calls disconnected or dns_error: never enrolled (nothing to
    repair; Smartlead may still call a DNS-broken inbox connected). Pure."""
    by_email = {r["email"]: r for r in records}
    out = []
    for d in docs:
        e = d["email"]
        if e in by_email:
            out.append(update(d, by_email[e], today, reasons(by_email[e], v2, listed, d.get("reasons") or ())))
        elif d.get("status") == "gone":
            out.append(d)
        elif d.get("account") in read and _left(e, roster):
            out.append({**d, "status": "gone", "gone_on": today.isoformat(), "recovered_on": None})
        else:
            out.append(update(d, None, today))
    known = {d["email"] for d in docs}
    new = []
    for r in records:
        rep = _rep(r.get("rep"))
        why = reasons(r, v2, listed)                     # warmup_by: its vendor warms it, no warm-up reason (t248)
        if (r["email"] in known or r.get("client") not in CLIENTS or r.get("connected") is not True or not why
                or r["email"] in (dead or ())):
            continue
        known.add(r["email"])
        new.append(update(new_doc(r["email"], r.get("account") or "", r["client"], None, r.get("kind") or "google",
                                  today.isoformat(), rep, before=None, after=None,
                                  why="report-only: enrolled by the bot, warm-up settings unchanged",
                                  blocked=bool(r.get("warmup_blocked")), by="auto", reasons=why), r, today, why))
    return out + new, new


# --- the digest -----------------------------------------------------------------------------------------------------

def _day(d) -> str:
    d = date.fromisoformat(str(d)[:10])
    return f"{d.day} {d.strftime('%b')}"


def _this_week(d: str | None, today: date) -> bool:
    return bool(d) and today - timedelta(days=6) <= date.fromisoformat(str(d)[:10]) <= today


def _n(k: int) -> str:
    return f"{k} inbox{'es' if k != 1 else ''}"


def _on(docs: list[dict]) -> str:
    """' on a.com' / ' on a.com and b.com' for an ask's sentence; '' for three or more (the list names them)."""
    doms = sorted({str(d["email"]).split("@")[-1] for d in docs})
    return f" on {' and '.join(doms)}" if 0 < len(doms) <= 2 else ""


SHORT = {"v2_pulled": "v2 retest (the v2 job pulled them)", "failed_v2": "v2 fail", "do_not_use": "a person's DO NOT USE tag",
         "blocklisted": "listed domain"}
OTHER_ORDER = ("v2_pulled", "failed_v2", "do_not_use", "blocklisted")


def warm_path(d: dict) -> bool:
    """Is this doc in warm-up repair (under 90 or blocked)? Docs from before t325 always are."""
    held = d.get("reasons")
    return held is None or any(x in WARMUP_REASONS for x in held)


def other_path(d: dict) -> str | None:
    """The first reason other than warm-up that keeps it out (OTHER_ORDER), or None."""
    return next((x for x in OTHER_ORDER if x in (d.get("reasons") or ())), None)


def week_line(client: str, docs: list[dict], today: date) -> str | None:
    """'Warm-up repair, BettrData: 30 recovering, 4 recovered this week, 2 worse.' plus, since t325, the bad inboxes
    out for another reason ('Other recovery: 25 listed domain.') and those waiting on Manveen's untag. None when there
    is nothing."""
    rep = [d for d in docs if d.get("status") == "repairing" and warm_path(d)]
    other = [d for d in docs if d.get("status") == "repairing" and not warm_path(d)]
    ready = [d for d in docs if d.get("status") == "ready"]
    stuck = [d for d in docs if d.get("status") == "no_improvement"]
    rec = [d for d in docs if d.get("status") == "recovered" and _this_week(d.get("recovered_on"), today)]
    bad = [d for d in docs if d.get("status") in IN_REPAIR and warm_path(d) and worse(d)]
    gone = [d for d in docs if d.get("status") == "gone" and _this_week(d.get("gone_on"), today)]
    blocked = sum(1 for d in rep if d.get("blocked"))
    if not (rep or stuck or rec or gone or other or ready):
        return None
    parts = [f"{len(rep)} recovering" + (f" ({blocked} warm-up blocked)" if blocked else "")]
    if stuck:
        parts.append(f"{len(stuck)} not improving after 3 weeks")
    if rec:
        parts.append(f"{len(rec)} recovered this week")
    if bad:
        parts.append(f"{len(bad)} worse")
    if gone:
        parts.append(f"{len(gone)} gone")
    line = f"Warm-up repair, {CLIENTS.get(client, client)}: {', '.join(parts)}."
    if other:
        by = Counter(other_path(d) for d in other)
        line += " Other recovery: " + ", ".join(f"{by[k]} {SHORT[k]}" for k in OTHER_ORDER if by[k]) + "."
    if ready:
        line += f" {_n(len(ready))} repaired, waiting on your untag."
    return line


def _detail_line(d: dict, today: date) -> str:
    days = (today - date.fromisoformat(str(d["started"])[:10])).days
    rep = d.get("latest_rep")
    flags = [{"repairing": "recovering", "no_improvement": "no improvement in 3 weeks (cancel candidate)",
              "recovered": f"recovered {_day(d['recovered_on'])}" if d.get("recovered_on") else "recovered",
              "ready": "warm-up repaired: only a person's DO NOT USE tag keeps it out",
              "gone": "gone"}.get(d.get("status"), str(d.get("status")))]
    if d.get("status") in IN_REPAIR and worse(d):
        flags.append("worse")
    if d.get("blocked"):
        flags.append("warm-up blocked")
    also = [SHORT[x] for x in OTHER_ORDER if x in (d.get("reasons") or ())]
    if also and d.get("status") != "ready":
        flags.append("also out for: " + ", ".join(also))
    if off_repair_settings(d):
        flags.append(f"not on repair settings ({d.get('warmup_per_day')}/day, {d.get('reply_rate')}% reply)")
    if d.get("live"):
        flags.append(f"in {d['live']} live campaign{'s' if d['live'] != 1 else ''}")
    b, a = d.get("settings_before") or {}, d.get("settings_after")
    setting = (f"warm-up {b.get('warmup_max_count')}→{a.get('warmup_max_count')}/day, reply "
               f"{b.get('reply_rate')}→{a.get('reply_rate')}%" if a else (d.get("why_not_changed") or "settings unchanged"))
    return (f"• {d['email']} · {d.get('kind')} · {d.get('rep_at_start')}% → {rep if rep is not None else '?'}% "
            f"(good from {level(d.get('kind'))}%) · day {days} · {', '.join(flags)} · {setting}")


def _other_lines(docs: list[dict]) -> list[str]:
    """Inboxes out for a reason other than warm-up, one line per domain and reason (a domain is one decision)."""
    groups: dict[tuple, list[dict]] = {}
    for d in docs:
        groups.setdefault((d["email"].split("@")[-1], d.get("kind"), other_path(d)), []).append(d)
    out = []
    for (dom, kind, why), ds in sorted(groups.items(), key=lambda kv: (OTHER_ORDER.index(kv[0][2]), kv[0][0])):
        live = sum(1 for d in ds if d.get("live"))
        since = min(str(d.get("started")) for d in ds)
        out.append(f"• {dom} · {_n(len(ds))} {kind} · {SHORT[why]} · back when {PATHS[why]} · since {_day(since)}"
                   + (f" · {live} still in live campaigns" if live else ""))
    return out


def digest_items(docs: list[dict], today: date, weekly: bool | None = None,
                 removed: set[str] | None = None) -> tuple[list, list, list[str]]:
    """(asks, done, detail) for the digest. The weekly line per client is a Done line on Mondays (``weekly``
    overrides the day). Asks: inboxes 3 weeks in repair without improving (cancel candidates); since t325, inboxes
    whose warm-up is repaired and only a person's tag keeps out (untag is Manveen's call), and inboxes under 90 not
    on the repair settings. Since 20 Nov 2026 inboxes whose warm-up is blocked (a login problem) are their own ask
    ("fix the login first"), and the repair-settings ask lists only inboxes it would help (``repair_would_help``).
    ``removed``: inboxes their provider removed (provider_removed.emails, t352): left out of every ask and count here,
    as there is nothing to repair; the digest's delete reminder lists them."""
    from smartlead import digest as dg
    if removed:
        docs = [d for d in docs if str(d.get("email") or "").lower() not in removed]
    weekly = today.weekday() == 0 if weekly is None else weekly
    by_client: dict[str, list[dict]] = {}
    for d in docs:
        by_client.setdefault(d.get("client"), []).append(d)
    done = []
    if weekly:
        for client in list(CLIENTS) + sorted(c for c in by_client if c not in CLIENTS):
            line = week_line(client, by_client.get(client) or [], today)
            if line:
                done.append(dg.Done(line, key=f"warmup-repair:week:{CLIENTS.get(client, client)}:{today.isoformat()}"))
    stuck = [d for d in docs if d.get("status") == "no_improvement"]
    ready = [d for d in docs if d.get("status") == "ready"]
    blocked = [d for d in docs if d.get("status") in IN_REPAIR and login_blocked(d)]
    off = [d for d in docs if d.get("status") in IN_REPAIR and off_repair_settings(d) and repair_would_help(d)]
    # 21 Nov 2026 (Manveen: "i think this still is not readable"): each ask is what is wrong and why it matters in one
    # short sentence, naming the domains when there are one or two, then one step. The list names every inbox.
    asks = []
    if stuck:
        n = len(stuck)
        it = "it" if n == 1 else "them"
        asks.append(dg.Ask(f"{_n(n)}{_on(stuck)} still {'has' if n == 1 else 'have'} a low warm-up score after 3 weeks of "
                           f"repair, so {'it is' if n == 1 else 'they are'} unlikely to recover and cost money each "
                           "month.",
                           f"tell the bot which of {it} to cancel, or to keep repairing {it}; the bot never cancels or "
                           "deletes an inbox.",
                           key="warmup-repair:cancel-candidates", detail=[_detail_line(d, today) for d in stuck]))
    if ready:
        n = len(ready)
        it = "it" if n == 1 else "them"
        asks.append(dg.Ask(f"{_n(n)}{_on(ready)} finished warm-up repair (score {level()}%+), but a DO NOT USE tag "
                           f"still keeps {it} out of campaigns.",
                           f"if you want {it} back, remove the DO NOT USE tag in Smartlead (Email Accounts); the bot "
                           "never removes a tag.",
                           key="warmup-repair:ready-to-untag", detail=[_detail_line(d, today) for d in ready]))
    if blocked:
        k = len(blocked)
        it = "it" if k == 1 else "them"
        asks.append(dg.Ask(f"Smartlead can't log in to {_n(k)}{_on(blocked)}, so {'its' if k == 1 else 'their'} "
                           f"warm-up is stopped and {'it' if k == 1 else 'they'} can't recover.",
                           f"in Smartlead, Email Accounts, reconnect {'it' if k == 1 else 'each one listed'} with "
                           "its current password (or app password); if one was cancelled, say so and it leaves this "
                           "list.",
                           key="warmup-repair:login-blocked", detail=_blocked_lines(blocked)))
    if off:
        k = len(off)
        it = "it" if k == 1 else "them"
        asks.append(dg.Ask(f"{_n(k)}{_on(off)} with a warm-up score under {level()}% {'is' if k == 1 else 'are'} "
                           f"still on normal warm-up settings, which repair {it} more slowly.",
                           f"say yes and {it} {'goes' if k == 1 else 'go'} on the repair settings ({REPAIR_PER_DAY} "
                           f"warm-up emails a day, never higher than now, {REPAIR_REPLY[0]}-{REPAIR_REPLY[1]}% reply); the "
                           "bot changes "
                           "no warm-up setting by itself.",
                           key="warmup-repair:not-on-repair-settings", detail=[_settings_line(d) for d in off]))
    live = [d for d in docs if d.get("status") in IN_REPAIR
            or (d.get("status") == "recovered" and _this_week(d.get("recovered_on"), today))
            or (d.get("status") == "gone" and _this_week(d.get("gone_on"), today))]
    detail = []
    if live and (weekly or asks):
        counts = Counter(d.get("status") for d in live)
        head = (f"Warm-up repair, {_day(today)}: {counts['repairing'] + counts['no_improvement']} recovering, "
                f"{counts['recovered']} recovered this week (report-only: the bot changes no warm-up setting)")
        order = {"no_improvement": 0, "ready": 1, "repairing": 2, "recovered": 3, "gone": 4}
        lines = [head]
        for client in sorted({d.get("client") for d in live}, key=lambda c: list(CLIENTS).index(c) if c in CLIENTS else 9):
            mine = [d for d in live if d.get("client") == client]
            warm = sorted((d for d in mine if warm_path(d) or d.get("status") != "repairing"),
                          key=lambda d: (order.get(d.get("status"), 9), d.get("latest_rep") or 0, d["email"]))
            rest = [d for d in mine if not warm_path(d) and d.get("status") == "repairing"]
            text = "\n".join(_detail_line(d, today) for d in warm)
            if rest:
                text += ("\n" if text else "") + "Out for another reason (the All Inboxes gate decides who sends):\n" \
                        + "\n".join(_other_lines(rest))
            lines.append(f"\n{CLIENTS.get(client, client)}:\n" + text)
        detail = ["\n".join(lines)]
    return asks, done, detail


# --- Mongo ----------------------------------------------------------------------------------------------------------

class RepairStore:
    """warmup_repair: one document per inbox, by email."""

    def __init__(self, col) -> None:
        self.col = col

    def all(self) -> list[dict]:
        return [d for d in self.col.find({}, {"_id": 0})]

    def save(self, docs: list[dict]) -> None:
        for d in docs:
            self.col.update_one({"email": d["email"]}, {"$set": d}, upsert=True)

    def seed(self, docs: list[dict]) -> int:
        """Write the seed docs; an inbox already seeded keeps its doc, one the bot enrolled by itself is replaced
        (the seed knows the real start and the settings change). Returns how many were written."""
        have = {d["email"]: d for d in self.all()}
        todo = [d for d in docs if d["email"] not in have or have[d["email"]].get("enrolled_by") == "auto"]
        self.save(todo)
        return len(todo)
