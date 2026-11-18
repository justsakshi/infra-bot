"""Which inboxes may be handed to a campaign: the evidence gate on top of the capacity rule.

The "All Inboxes" tab says FREE or BUSY for every inbox, and every tool that picks inboxes reads it
(precise-automator's inbox recommender, people, other agents). Until 10 Nov 2026 FREE meant: connected,
capacity left, warm-up reputation 90%+, a fresh "inbox" in the hand-kept test grid, DNS fine. It did not
look at the three things that showed the Google batch was in spam:

  * a tag starting "DO NOT USE" in Smartlead (a person's, or the v2 job's);
  * the Smart Delivery v2 verdict (Mongo ``v2_verdicts``, written by v2_weekly.py);
  * a Spamhaus DBL or URIBL listing of the sending domain (Mongo ``blacklist_checks``).

That day 88 inboxes were FREE: 49 on listed domains, 83 with no v2 result. The 60 tagged ones were BUSY only
because their grid rows happened to be stale.

Always BUSY: tagged DO NOT USE (``do_not_use``), newest judged v2 verdict FAIL (``failed_v2``), unless that
fail is set aside (``fail_set_aside``): the inbox is of a type whose v2 test is not trusted (Google inboxes since
12 Nov 2026, the same rule as v2_weekly.REPORT_ONLY_TYPES), or a person removed the v2 job's tag after it (the
verdict doc's ``overridden``). A set-aside fail is a note, and the inbox still has no v2 pass.
BUSY when the switch is on, a note until then:
  AVAILABILITY_BLOCK_LISTED=true     listed sending domain -> ``blocklisted``
  AVAILABILITY_REQUIRE_V2_PASS=true  no v2 pass in the last 10 days -> ``no_v2_pass``
  (on by date as well: AVAILABILITY_REQUIRE_V2_PASS_FROM, 13 Nov 2026 unless set to "off")
The notes go in the Notes column, so the warning is on the sheet before it blocks anything.

High bounce (``high_bounce``, set by processing.py) is set aside by a v2 pass in the last 10 days: the inbox is FREE
as far as bounces go, with the note BOUNCE_NOTE. Manveen, 16 Nov 2026: high bounce on its own does not mean do not
use, "so just say so" (Outlook inboxes that passed v2 again with warm-up 91-100% were BUSY only for bounces from past
campaigns' lead lists). It is a note in the sheet, never a digest ask.

Google inboxes have their own rule (Manveen, 17 Nov 2026, t268: "yes i agree with this"). A Google (GMAIL) inbox is
usable when it is connected, has no DO NOT USE tag, is not "quiet", and EITHER it reached an inbox in the last 28 days
(the weekly real-send evidence, inbox_evidence.py: a real reply, out-of-office or wrong-person reply, at its own
recipient provider, Gmail) OR its warm-up reputation is MIN_WARMUP_REP_PCT (90) or more. Low reputation with real
replies is "repairing but usable" (a note). The v2 test and the seed / classic placement grid stay report-only for
Google: shown in the notes, never a reason (``google_gate``). Missing evidence is "not enough data", never quiet; a
quiet or reached label at another recipient provider is a signal only (own-provider rule, smartlead/own_provider.py).
Accepted risk: "reached" counts OOO and wrong-person replies, so reputation 90+ is the only guard against an inbox
whose mail mostly lands in spam. Capacity, warm-up blocked, DNS and bounce checks (processing.py) are unchanged.

``gate`` is pure. ``load_gate_data`` reads Mongo and never raises: without it the tag rule still holds
(tags come from Smartlead) and every row gets a note saying the rest was not read.
"""
from __future__ import annotations

import os
from datetime import date

DO_NOT_USE = "do not use"            # tag prefix, any case (same as smartlead/fleet.py)
V2_FRESH_DAYS = 10                   # a weekly test plus three days: results are downloaded by hand, a day
                                     # late must not empty the FREE list (the renewal review keeps 8 for money)
JUDGED = ("PASS", "WATCH", "FAIL")   # INCOMPLETE and anything else is not a verdict
SENDER_LISTS = ("spamhaus", "uribl")  # lists that judge the sending domain (SURBL lists links in the copy)
NOT_READ = "v2 results and blocklists not read today"
BOUNCE_NOTE = "bounces come from the lead list"
# Inbox types whose v2 FAIL does not hold them back (12 Nov 2026): in the same campaigns, Gmail recipients answered
# Google-sent mail 1.5-2.5 times as often as Outlook-sent mail while v2 failed nearly every Google inbox. One rule for
# every reader: the v2 job (REPORT_ONLY_TYPES), the list, the daily fleet post and the gate self-test.
TYPE_NAMES = {"GMAIL": "Google inboxes", "OUTLOOK": "Outlook inboxes", "SMTP": "SMTP inboxes"}
DEFAULT_NOT_TRUSTED = {"GMAIL": "Google inboxes"}
# Shared by every reader and refreshed IN PLACE by refresh_trust() (v2_weekly.REPORT_ONLY_TYPES is this same dict).
# Since 13 Nov 2026 the weekly real-recipient check decides it from real replies (real_recipients.trust_decision) and
# keeps it in Mongo v2_trust; the default above holds until it has, or when Mongo cannot be read.
V2_NOT_TRUSTED = dict(DEFAULT_NOT_TRUSTED)
TRUST_COLLECTION = "v2_trust"
# Inbox types judged on real sends and warm-up instead of seed tests (t268, 17 Nov 2026), whatever v2_trust says, and
# the recipient provider (inbox_evidence.py's MX label) each one is judged at.
REAL_SEND_RULE = {"GMAIL": "Google inboxes"}
OWN_RECIPIENT = {"GMAIL": "Gmail", "OUTLOOK": "Microsoft"}
REACHED, NOT_ENOUGH = "reached the inbox", "not enough data"


def trust_from(env_value: str | None, doc: dict | None) -> tuple[dict[str, str], str]:
    """({type: name}, where it came from). Env V2_UNTRUSTED_TYPES ("GMAIL,OUTLOOK", or "none") wins over the Mongo
    doc, which wins over the default. Pure."""
    env_value = str(env_value or "").strip()
    if env_value:
        if env_value.lower() in ("none", "off", "-"):
            return {}, "env V2_UNTRUSTED_TYPES"
        kinds = [kind_of(x) for x in env_value.split(",") if x.strip()]
        return {k: TYPE_NAMES.get(k, k) for k in kinds}, "env V2_UNTRUSTED_TYPES"
    if isinstance(doc, dict) and isinstance(doc.get("untrusted"), dict):
        return ({kind_of(k): TYPE_NAMES.get(kind_of(k), k) for k in doc["untrusted"]},
                f"real-recipient check of {doc.get('decided') or 'an earlier week'}")
    return dict(DEFAULT_NOT_TRUSTED), "default"


def refresh_trust(db=None) -> str:
    """Set V2_NOT_TRUSTED in place from the env, Mongo, or the default. Never raises; returns where it came from,
    "unread" when Mongo could not be read (the default is then in place; a caller that acts may prefer its own last
    copy, as v2_weekly does)."""
    doc = None
    if not os.getenv("V2_UNTRUSTED_TYPES", "").strip():
        try:
            doc = (db if db is not None else _db())[TRUST_COLLECTION].find_one({"_id": "current"})
        except Exception as exc:  # noqa: BLE001 - the default holds
            print(f"  [Usable] v2 trust not read ({type(exc).__name__}); using the default")
            V2_NOT_TRUSTED.clear()
            V2_NOT_TRUSTED.update(DEFAULT_NOT_TRUSTED)
            return "unread"
    kinds, source = trust_from(os.getenv("V2_UNTRUSTED_TYPES"), doc)
    V2_NOT_TRUSTED.clear()
    V2_NOT_TRUSTED.update(kinds)
    return source


def kind_of(type_or_provider) -> str:
    """Smartlead's account type ("GMAIL", "OUTLOOK", "SMTP") or the sync's provider ("Gmail", "Outlook") -> the
    Smartlead type, upper case; "" when unknown."""
    s = str(type_or_provider or "").strip().upper()
    if "GMAIL" in s or "GOOGLE" in s:
        return "GMAIL"
    if any(k in s for k in ("OUTLOOK", "OFFICE", "MICROSOFT", "EXCHANGE")):
        return "OUTLOOK"
    return s


def fail_set_aside(v2: dict | None, kind="") -> str:
    """Why a FAIL verdict does not hold this inbox back ("" = it does, or it is not a FAIL). Pure."""
    if str((v2 or {}).get("verdict") or "").upper() != "FAIL":
        return ""
    if (v2 or {}).get("overridden"):
        return "a person put it back after this result"
    name = V2_NOT_TRUSTED.get(kind_of(kind)) or REAL_SEND_RULE.get(kind_of(kind))
    return f"the v2 test is not used for {name}" if name else ""


def real_send_rule(kind) -> bool:
    """Is this inbox judged on real sends and warm-up (the Google rule, t268) rather than on seed tests?"""
    return kind_of(kind) in REAL_SEND_RULE


def rep_pct(value) -> float:
    """'95%' / '95' / 95 -> 95.0; anything unreadable ('N/A', '') -> 0.0, as processing.py reads it."""
    try:
        return float(str(value).replace("%", "").strip())
    except (TypeError, ValueError):
        return 0.0


def google_gate(rep: float, own_label: str, test_status: str = "", test_date: str = "",
                other: dict | None = None, warmed_by: str | None = None) -> tuple[list[str], list[str]]:
    """(busy reasons, notes) of the Google rule (module note, t268). Pure.

    ``rep`` = warm-up reputation %; ``own_label`` = the week's real-send label at the inbox's own recipient provider
    ("reached the inbox", "quiet (0 of ~N expected)", "not enough data"; "" = none = not enough data);
    ``test_status``/``test_date`` = the seed grid's own-column result (report only); ``other`` = {recipient provider:
    label} elsewhere (a signal only); ``warmed_by`` = the vendor that runs its warm-up outside Smartlead
    (smartlead/external_warmup.py, t248): its Smartlead warm-up reputation is then no reason."""
    from smartlead.config import MIN_WARMUP_REP_PCT
    from smartlead.own_provider import SIGNAL
    label = str(own_label or "").strip() or NOT_ENOUGH
    reached, quiet = label == REACHED, label.startswith("quiet")
    reasons, notes = [], []
    if quiet:
        reasons.append("quiet")
    if warmed_by:                       # the vendor warms it (t248): Smartlead's reputation says nothing about it
        pass
    elif rep < MIN_WARMUP_REP_PCT:
        if reached:
            notes.append(f"repairing but usable: warm-up {rep:g}%, reached the inbox at Gmail in 28 days")
        else:
            reasons.append("low_rep")
    t = str(test_status or "").strip().lower()
    if t in ("fail", "spam", "stale"):
        when = f" {_day(test_date)}" if test_date else ""
        notes.append(f"seed test {'spam' if t != 'stale' else 'stale'}{when}: report only for Google inboxes")
    for rp, lab in sorted((other or {}).items()):
        lab = str(lab or "")
        if lab == REACHED or lab.startswith("quiet"):
            notes.append(f"{lab} at {rp} (not its provider: {SIGNAL})")
    return reasons, notes


def warmed_by(email: str, data: dict | None) -> str | None:
    """The vendor that runs this inbox's warm-up outside Smartlead (gate data's "external" marks), or None."""
    from smartlead import external_warmup as xw
    return xw.vendor(email, (data or {}).get("external"))


def holds_back(v2: dict | None, kind="") -> bool:
    """Does this verdict keep the inbox out of campaigns? FAIL, and not set aside. Pure."""
    return str((v2 or {}).get("verdict") or "").upper() == "FAIL" and not fail_set_aside(v2, kind)


def require_v2_pass(today: date) -> bool:
    """Is a v2 pass required for FREE today? The switch, or the start date having come."""
    from smartlead.config import AVAILABILITY_REQUIRE_V2_PASS, AVAILABILITY_REQUIRE_V2_PASS_FROM
    if AVAILABILITY_REQUIRE_V2_PASS:
        return True
    start = str(AVAILABILITY_REQUIRE_V2_PASS_FROM or "").strip().lower()
    if start in ("", "off", "false", "never"):
        return False
    try:
        return today >= date.fromisoformat(start)
    except ValueError:
        return False


def fresh_pass(v2: dict | None, today: date) -> bool:
    """Is this newest judged verdict a v2 PASS from the last V2_FRESH_DAYS days? Pure."""
    if str((v2 or {}).get("verdict") or "").upper() != "PASS":
        return False
    try:
        return (today - date.fromisoformat(str((v2 or {}).get("night") or "")[:10])).days <= V2_FRESH_DAYS
    except ValueError:
        return False


def set_aside_bounce(item: dict, reasons: list[str], today: date) -> list[str]:
    """``reasons`` without ``high_bounce`` when the inbox passed v2 lately (``item`` already filled by ``apply``), the
    note BOUNCE_NOTE added to the row instead. Otherwise unchanged. See the module note, 16 Nov 2026."""
    if "high_bounce" not in reasons or not fresh_pass({"verdict": item.get("v2_status"), "night": item.get("v2_date")},
                                                       today):
        return reasons
    item["use_notes"] = "; ".join(x for x in (item.get("use_notes"), BOUNCE_NOTE) if x)
    return [r for r in reasons if r != "high_bounce"]


def tag_names(account: dict) -> list[str]:
    """Tag names of a Smartlead email account (dicts with tag_name / name; plain strings kept)."""
    out = []
    for t in (account or {}).get("tags") or []:
        name = str(t.get("tag_name") or t.get("name") or "") if isinstance(t, dict) else str(t or "")
        if name.strip():
            out.append(name.strip())
    return out


def carry_tags(listed, details) -> list[dict]:
    """``details`` with each account's tags taken from the list call when the detail has none.

    Smartlead's GET /email-accounts (the list) returns ``tags``; GET /email-accounts/{id} (the detail the
    daily sync builds its rows from) does not. So from 10 to 11 Nov 2026 the tag rule never fired in the
    sheet: 63 inboxes were tagged DO NOT USE and none carried ``do_not_use`` (all were BUSY only because
    they had also failed v2). Found by comparing the tags with the published tab. Pure; never raises."""
    by_id = {str(a.get("id")): a.get("tags") for a in listed or [] if isinstance(a, dict) and a.get("tags")}
    out = []
    for d in details or []:
        if isinstance(d, dict) and not d.get("tags") and str(d.get("id")) in by_id:
            d = {**d, "tags": by_id[str(d.get("id"))]}
        out.append(d)
    return out


def is_do_not_use(tags) -> bool:
    return any(str(t).strip().lower().startswith(DO_NOT_USE) for t in tags or [])


V2_AUTO_TAG = "do not use - failed v2 (auto)"     # v2_weekly.TAG, lower case: the v2 job's own pull, not repair
RECOVERING = "recovering"
RECOVERING_NOTE = "recovering: out of campaigns while its warm-up repairs (tag stays DO NOT USE)"


def recovering(tags, rep) -> bool:
    """Taken out of campaigns for warm-up repair: a person's DO NOT USE tag (not the v2 job's auto tag) on an inbox whose
    warm-up reputation is readable and under the cutoff. Shown as "recovering", never "pulled" (Manveen 17 Nov 2026:
    "instead of pulled make it recovering"). The tag name itself stays DO NOT USE (gtm presend matches on it). Pure."""
    from smartlead.config import MIN_WARMUP_REP_PCT
    hand = any(str(t).strip().lower().startswith(DO_NOT_USE) and str(t).strip().lower() != V2_AUTO_TAG
               for t in tags or [])
    try:
        value = float(str(rep).replace("%", "").strip())
    except (TypeError, ValueError):
        return False
    return hand and value < MIN_WARMUP_REP_PCT


def _day(iso: str) -> str:
    try:
        d = date.fromisoformat(str(iso)[:10])
    except ValueError:
        return str(iso)
    return f"{d.day} {d.strftime('%b')}"


def gate(tags, v2: dict | None, listed: bool | None, today: date, *, checked: bool = True,
         block_listed: bool = False, require_v2_pass: bool = False, kind: str = "") -> tuple[list[str], list[str]]:
    """(busy reasons, notes) for one inbox.

    ``v2`` = its newest judged verdict {"verdict", "night"} or None; ``listed`` = its domain is on
    Spamhaus DBL / URIBL (None = no blocklist row for it). ``checked`` False = Mongo was not read:
    only the tag rule applies and nothing is claimed about v2 or blocklists. ``kind`` = the inbox's type or
    provider (see ``fail_set_aside``).
    """
    reasons, notes = [], []
    if is_do_not_use(tags):
        reasons.append("do_not_use")
    if not checked:
        return reasons, notes
    verdict = str((v2 or {}).get("verdict") or "").upper()
    night = str((v2 or {}).get("night") or "")
    aside = fail_set_aside(v2, kind)
    if verdict == "FAIL" and not aside:
        reasons.append("failed_v2")
    else:
        if not fresh_pass(v2, today):
            why = (f"failed v2 on {_day(night)}, not counted: {aside}" if aside
                   else f"v2 watch on {_day(night)}" if verdict == "WATCH"
                   else f"last v2 pass {_day(night)}, older than {V2_FRESH_DAYS} days" if verdict == "PASS"
                   else "no v2 result")
            if require_v2_pass and not real_send_rule(kind):      # Google: v2 is report-only (t268)
                reasons.append("no_v2_pass")
            notes.append(why)
    if listed:
        if block_listed:
            reasons.append("blocklisted")
        notes.append("domain on Spamhaus DBL or URIBL")
    return reasons, notes


def newest_judged(rows) -> dict[str, dict]:
    """{email: {"verdict", "night", "overridden"}}: each inbox's newest v2 row that is a verdict. Pure."""
    out: dict[str, dict] = {}
    for d in rows or []:
        email, night = str(d.get("email") or "").lower(), str(d.get("night") or "")
        verdict = str(d.get("verdict") or "").upper()
        if not email or verdict not in JUDGED:
            continue
        if email not in out or night > out[email]["night"]:
            out[email] = {"verdict": verdict, "night": night, "overridden": bool(d.get("overridden"))}
    return out


def listed_domains(rows) -> set[str]:
    """Domains whose NEWEST blocklist row lists them on Spamhaus DBL or URIBL. Pure."""
    newest: dict[str, dict] = {}
    for d in rows or []:
        dom, day = str(d.get("domain") or "").lower(), str(d.get("date") or "")
        if dom and (dom not in newest or day > str(newest[dom].get("date") or "")):
            newest[dom] = d
    return {dom for dom, d in newest.items()
            if any(k in str(z).lower() for z in d.get("listed_on") or [] for k in SENDER_LISTS)}


def _db():
    from pymongo import MongoClient
    from smartlead.config import HEALTH_HISTORY_DB
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        raise RuntimeError("MONGO_URI is not set")
    return MongoClient(uri, serverSelectionTimeoutMS=8000)[HEALTH_HISTORY_DB]


def load_evidence(db) -> dict:
    """The newest weekly real-send evidence (inbox_evidence.py, Mongo inbox_evidence): {"evidence": {email: {evidence,
    label, providers: {recipient provider: label}}}, "evidence_week": day}, or {} when there is none or it can't be
    read. Report only for Outlook; the Google rule reads it (``google_gate``, docs/INBOX_EVIDENCE.md)."""
    try:
        doc = next(iter(db["inbox_evidence"].find({}, {"_id": 1, "inboxes": 1}).sort("_id", -1).limit(1)), None)
    except Exception:  # noqa: BLE001 - the columns stay blank; nothing else depends on them
        return {}
    if not doc:
        return {}
    return {"evidence": {str(k).lower(): {"evidence": v.get("evidence", ""), "label": v.get("label", ""),
                                          "providers": {str(rp): str((c or {}).get("label") or "")
                                                        for rp, c in (v.get("providers") or {}).items()}}
                         for k, v in (doc.get("inboxes") or {}).items()}, "evidence_week": str(doc["_id"])}


def own_evidence(email: str, kind, data: dict | None) -> tuple[str, dict]:
    """(label at the inbox's own recipient provider, {other recipient provider: label}) from the week's evidence.
    No evidence, no row or no own-provider cell = "not enough data" (never quiet). Pure."""
    row = ((data or {}).get("evidence") or {}).get(str(email or "").lower()) or {}
    provs = dict(row.get("providers") or {})
    own = OWN_RECIPIENT.get(kind_of(kind), "")
    return provs.pop(own, "") or NOT_ENOUGH, provs


def _evidence(item: dict, data: dict | None) -> None:
    ev = (data or {}).get("evidence")
    if ev is None:
        item["evidence"] = item["evidence_label"] = ""
        return
    row = ev.get(str(item.get("email") or "").lower())
    if not row:
        item["evidence"], item["evidence_label"] = "", "no real sends in the window"
        return
    d = date.fromisoformat(str(data.get("evidence_week"))[:10])
    item["evidence"] = row["evidence"]
    item["evidence_label"] = f"{row['label']} (week of {d.day} {d:%b})"


def load_gate_data() -> dict:
    """{"v2": {email: verdict row}, "listed": {domain}, "checked": bool, "error": str}. Never raises."""
    from smartlead.config import BLACKLIST_COLLECTION
    try:
        db = _db()
        refresh_trust(db)
        v2 = newest_judged(db["v2_verdicts"].find({}, {"_id": 0, "email": 1, "night": 1, "verdict": 1, "overridden": 1}))
        listed = listed_domains(db[BLACKLIST_COLLECTION].find({}, {"_id": 0, "domain": 1, "date": 1, "listed_on": 1}))
        from smartlead import external_warmup as xw
        try:
            marks_col = db[xw.COLLECTION]
        except Exception:  # noqa: BLE001 - a test db without it
            marks_col = None
        return {"v2": v2, "listed": listed, "checked": True, "error": "", **load_evidence(db),
                "external": xw.load(marks_col)}
    except Exception as exc:  # noqa: BLE001 - the tag rule still holds; the sheet says the rest was not read
        print(f"  [Usable] v2 verdicts and blocklists not read: {str(exc)[:150]}")
        return {"v2": {}, "listed": set(), "checked": False, "error": str(exc)[:150]}


def apply(item: dict, data: dict | None, today: date, *, block_listed: bool, require_v2_pass: bool) -> list[str]:
    """Fill the inbox row's v2 and note fields and return its extra busy reasons.
    ``data`` None (callers that never loaded it) = the tag rule only, no notes; a Google inbox is still judged by
    the Google rule (``google_gate``), its real-send evidence then "not enough data". ``item["warmup_rep_pct"]`` must
    be filled first (processing.py does)."""
    email = str(item.get("email") or "").lower()
    checked = bool(data and data.get("checked"))
    v2 = (data or {}).get("v2", {}).get(email)
    listed = email.split("@")[-1] in (data or {}).get("listed", set()) if checked else None
    kind = item.get("type") or item.get("provider") or ""
    reasons, notes = gate(item.get("tags"), v2, listed, today, checked=checked,
                          block_listed=block_listed, require_v2_pass=require_v2_pass, kind=kind)
    vendor = warmed_by(email, data)
    if real_send_rule(kind):            # the Google rule (t268): real sends + warm-up decide, seed tests are notes
        own_label, other = own_evidence(email, kind, data)
        g_reasons, g_notes = google_gate(rep_pct(item.get("warmup_rep_pct")), own_label,
                                         item.get("test_sheet_status", ""), item.get("test_date", ""), other,
                                         warmed_by=vendor)
        reasons += [r for r in g_reasons if r not in reasons]
        notes = g_notes + notes
    if vendor:
        from smartlead import external_warmup as xw
        notes.insert(0, xw.note(vendor))
    elif recovering(item.get("tags"), item.get("warmup_rep_pct")):
        notes.insert(0, RECOVERING_NOTE)
    if data is not None and not checked:
        notes.append(NOT_READ)
    if item.get("test_signal"):         # the grid's other-provider spam (sheets.own_test_status): shown, never a reason
        notes.append(item["test_signal"])
    item["v2_status"] = (v2 or {}).get("verdict", "")
    item["v2_set_aside"] = fail_set_aside(v2, item.get("type") or item.get("provider") or "") if checked else ""
    item["v2_date"] = (v2 or {}).get("night", "")
    item["use_notes"] = "; ".join(notes)
    _evidence(item, data)
    return reasons
