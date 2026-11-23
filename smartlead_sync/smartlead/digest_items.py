"""Every ask kind, routed to one place (t409, 23 Nov 2026).

Manveen's daily post holds only what a person must do, each item one approved sentence (smartlead/digest_sentences.py).
This module decides, for every ask a job hands the digest, where it goes:

  * a sentence (1-7) in the post (the first MAX_ITEMS; the rest in the thread, oldest first), its list in the thread;
  * "W": a "Watching" line under the items (nothing to do yet);
  * the bot itself: a read-only lookup (the domain's latest weekly test verdict, Mongo v2_verdicts; the domain's next
    weekly test, Mongo test_coverage) that turns the ask into a sentence, a Watching line, a dev line or nothing;
  * dev: a bot or job problem, posted to the bot-health channel (BOT_HEALTH_CHANNEL, else the infra channel) as its own
    message, never in Manveen's digest;
  * info / sheet: a fact with no step, or a count All Inboxes already shows: never in the post;
  * thread: a person-only ask no approved sentence fits. It goes to the thread in the closest sentence's shape and
    its sentence waits for Manveen's OK (the "proposal" column of KINDS; each proposal is a parked tracker task).

Never: a test run started, an inbox tagged, untagged, pulled or deleted, a limit changed. The lookups only read.

    route(asks, broken, infos, ctx, now) -> Plan     (dict: items, watch, thread, dev, info, dropped)
    KINDS                                            the routing table (the PR's mapping table is printed from it)
    kind_of(ask) -> str                              which row of KINDS an ask is
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone

from smartlead import digest_sentences as ds

IST = timezone(timedelta(hours=5, minutes=30))
OWN = {"OUTLOOK": "microsoft", "GMAIL": "google", "GOOGLE": "google"}
PROVIDER_WORD = {"microsoft": "Outlook", "google": "Gmail"}
PRIORITY = {"1": 0, "6": 1, "2": 2, "3": 3, "4": 4, "7": 5, "5": 6}      # after the dated items (bills), in this order

# Proposed sentences for person-only kinds with no approved one (each a parked tracker task, t409). The thread uses
# them meanwhile, marked "(wording not approved yet)".
PROPOSALS = {
    "P1": "[Client]: Smartlead can't log in to [k] inboxes on [domain]. Reconnect them, or delete them if they were "
          "cancelled (list in thread).",
    "P2": "[Client]: the [provider] bill of [$] on [date] failed. Pay it, or let it go on purpose (steps in thread).",
    "P3": "[Client]: [k] domains expire at [provider] before [date]. Renew them or let them go (list in thread).",
    "P4": "[Client]: the weekly test [test] looks broken. Re-run it in Smart Delivery (it uses one test run).",
    "P5": "[Client]: [k] inboxes need your call: [question] (list in thread).",
    "P6": "[Client]: [k] inboxes that may not send are still in [campaign]. Take them out (list in thread).",
    "P7": "[Client]: [what]. [step] (steps in thread).",
    "P8": "[Client]: [k] removed inboxes had a lead reply in the last 30 days. Answer the lead, then delete them "
          "(list in thread).",
}

# kind -> (destination, what it is, proposal). Destination: "1".."7" | "W" | "bot" | "dev" | "info" | "sheet" |
# "thread" | "done". The PR's mapping table is this table (mapping_table()).
KINDS: dict[str, tuple[str, str, str]] = {
    # sentences
    "kill-list": ("1", "kill list (fleet watch, kill report): the domain's weekly test verdict is read; FAIL -> "
                       "sentence 1; no weekly fail -> thread (P6)", ""),
    "v2-domain-fail": ("1", "weekly test: one domain failed at its own provider", ""),
    "bill-drop": ("2", "renewal review: a bill renews inboxes we stopped using", ""),
    "removed": ("3", "the delete reminder, split by its groups: delete now -> 3, still in a real campaign -> 4, "
                     "a lead replied -> thread (P8), check not finished -> dev", ""),
    "bill": ("2", "a renewal-review bill: its drop inboxes -> 2, its keep-or-cancel ones -> thread (P5)", ""),
    "removed-delete": ("3", "provider removed the inboxes, in no real campaign: delete now", ""),
    "removed-in-campaign": ("4", "provider removed the inboxes, still in a real campaign", ""),
    "ready-to-untag": ("5", "warm-up repair finished, DO NOT USE tag left", ""),
    "no-weekly-test": ("7", "inboxes sending or held with no active weekly test (test-campaign job, inbox list)", ""),
    # the bot looks it up itself
    "reply-alert": ("bot", "replies low on a domain: latest weekly verdict read; FAIL -> sentence 1, PASS or none -> "
                           "Watching with the next test date; no weekly test -> sentence 7", ""),
    "no-result": ("bot", "no weekly result for a test campaign: its weekly test is looked up; scheduled -> dev line; "
                         "none -> sentence 7", ""),
    "seeds-pending": ("dev", "a test was read before all results were in: the weekly job reads it again itself", ""),
    "removed-unchecked": ("dev", "removed inboxes whose campaign and reply check did not finish", ""),
    # person-only, no approved sentence yet: thread, closest shape, proposal parked
    "test-held": ("thread", "a weekly test looks broken (nearly every inbox below 4 of 4): re-run it", "P4"),
    "disconnected": ("thread", "Smartlead can't log in to inboxes (inbox list, fleet watch, warm-up repair, weekly "
                               "test): reconnect or delete", "P1"),
    "removed-replied": ("thread", "removed inboxes a lead replied to in the last 30 days", "P8"),
    "bill-check": ("thread", "renewal review: inboxes on a bill need a keep-or-cancel call", "P5"),
    "bill-failed": ("thread", "billing watch: a payment failed, a subscription not renewed or cancelled", "P2"),
    "domains-expiring": ("thread", "Zapmail: client domains expire soon", "P3"),
    "decide": ("thread", "a call only Manveen makes (cancel candidates, repair settings, act mode, volume pilot, "
                         "real-send trust)", "P5"),
    "take-out": ("thread", "inboxes that may not send are in live campaigns (gate self-test, weekly test stuck "
                           "campaign, fleet watch sending-while-held)", "P6"),
    "fix": ("thread", "a set-up fix by hand (untracked domains, no room on a test campaign, two tests with one name, "
                      "test copy, new-inbox problems, Zapmail wallet, purchase batches, ScaledMail orders)", "P7"),
    # off the post
    "broken": ("dev", "a job or check that failed, could not read, or never finished (bot health, every job's "
                      "errors, the digest's own checks: PC job, Render behind, sync not finished)", ""),
    "info": ("info", "a fact with no step (\"For info\")", ""),
    "counts": ("sheet", "inbox counts per client: the All Inboxes tab", ""),
}

# Every key a job hands over (inventory of 23 Nov 2026, 123 variants), checked in order: (key pattern, kind, what it
# is). The first that matches wins. Broken asks are "broken" and infos "info" whatever their key.
KEYMAP = [
    (r"^digest:kill-list$", "kill-list", "kill list, merged at post time"),
    (r"^kill-report:(sending|idle|domains)$", "kill-list", "kill report: kill-list inboxes sending or idle, domains"),
    (r"^fleet-watch:kill$", "kill-list", "fleet watch: inboxes on the kill list"),
    (r"^provider-removed:delete$", "removed", "inboxes their provider removed"),
    (r"^warmup-repair:ready-to-untag$", "ready-to-untag", "warm-up repaired, DO NOT USE tag left"),
    (r"^warmup-repair:login-blocked$", "disconnected", "warm-up repair: Smartlead can't log in"),
    (r"^warmup-repair:(cancel-candidates|not-on-repair-settings)$", "decide", "warm-up repair: cancel candidates, "
     "repair settings"),
    (r"^reply-monitor:alert$", "reply-alert", "reply check: replies low on a domain"),
    (r"^renewal-review:", "bill", "renewal review: a bill with inboxes to drop or check"),
    (r"^billing-watch:(payment|not_renewed|status):", "bill-failed", "billing watch: payment failed, not renewed, "
     "status not active"),
    (r"^billing-watch:(new|gone|bill_day):", "fix", "billing watch: bought outside the bot, no longer listed, card "
     "bill today"),
    (r"^dt-senders:coverage:", "no-weekly-test", "test-campaign job: a client's inboxes in no weekly test"),
    (r"^inbox-status:(sending_unproven|sending_not_in_test|test_campaign_no_weekly_test)$", "no-weekly-test",
     "inbox list: sending or on a test campaign with no weekly test"),
    (r"^inbox-status:disconnected$", "disconnected", "inbox list: inboxes in live campaigns disconnected"),
    (r"^inbox-status:(held_but_sending|held_but_sending_report_only)$", "take-out", "inbox list: held-back inboxes "
     "still in live campaigns"),
    (r"^inbox-status:campaign_short$", "fix", "inbox list: a live campaign lost half its inboxes (urgent)"),
    (r"^gate-selftest:held_back_not_sending$", "take-out", "gate self-test: held-back inboxes still sending"),
    (r"^fleet-watch:early_campaign$", "take-out", "fleet watch: inboxes in a campaign before their ready date"),
    (r"^fleet-watch:(warmup_off|bounce_high|smtp_ip|over_limit|hand_limit)$", "fix", "fleet watch: warm-up off, "
     "bounces 4%+, SMTP reputation, limits over or set by hand"),
    (r"^zapmail-digest:domains-expiring$", "domains-expiring", "Zapmail: client domains expire within 14 days"),
    (r"^zapmail-digest:(low-wallet|batches-due|batches-reconcile|sm-late|sm-live|sm-stuck|sm-no-client)",
     "fix", "Zapmail digest: wallet low, purchase batches, ScaledMail orders and domains"),
    (r"^volume-review:(not-changed|suggested)$", "decide", "volume review: pilot limits, domains to step back"),
    (r"^(copy-sync|inbox-intake|dt-senders):", "fix", "test copy, new-inbox set-up, test-campaign upkeep "
     "(two tests with one name, inboxes in two tests, untracked domains, no room)"),
    (r"^key-health:dead:", "broken", "API keys: Smartlead rejects an account's key (a bot setting)"),
    (r"^(v2-note-|v2-reminder$|v2-weekly-crash$)", "broken", "weekly test job notes: results read early or "
     "unreadable, runs that sent nothing, its own errors"),
]
# The weekly test job's "Needs you" lines have no key of their own (their key is made from their words): by text.
TEXTMAP = [
    (r"failed the weekly test .* at their own provider", "v2-domain-fail", "weekly test: one domain failed"),
    (r"results? of the test .* were not used|results in [\w.-]+\.csv \(.*\) held|results of the .* tests .* "
     r"held|so (it was|they were) not judged", "test-held", "weekly test: a test looks broken or a seed bounced"),
    (r"seed results? had not come in|had not come in when it was read|read before (all its results|they) were in",
     "seeds-pending", "weekly test: read before all results were in"),
    (r"^No (weekly test )?result this week", "no-result", "weekly test: no result for a test campaign"),
    (r"sent nothing in this week's test|can't log in|cannot log in|reconnect", "disconnected",
     "weekly test: inboxes sent nothing, Smartlead can't log in"),
    (r"is live with only failed inboxes", "take-out", "weekly test: a live campaign has only failed inboxes (urgent)"),
    (r"missing from its (v2|weekly) test results|send in live campaigns but", "no-weekly-test",
     "weekly test: inboxes left out of their test, sending with no test"),
    (r"agreed conditions .* now hold", "decide", "weekly test: a report-only client may be switched on"),
    (r"failed the copy check", "fix", "copy check: an email failed"),
    (r"^Error: |format the job can't read", "broken", "weekly test job: an error, a file it can't read"),
    (r"far fewer replies", "reply-alert", "reply check"),
]
_KEYS = [(re.compile(p), k) for p, k, _ in KEYMAP]
_TEXT = [(re.compile(p, re.I), k) for p, k, _ in TEXTMAP]


def kind_of(a: dict) -> str:
    """The row of KINDS an ask is (before any lookup). Broken asks are "broken", infos "info"."""
    if a.get("kind") == "broken":
        return "broken"
    if a.get("kind") == "info":
        return "info"
    t = (a.get("facts") or {}).get("type")
    if t == "test-held":
        return "test-held"
    if t == "seeds-pending":
        return "seeds-pending"
    if t == "no-result":
        return "no-result"
    key = str(a.get("key") or "")
    src = str(a.get("source") or "")
    for rx, k in _KEYS:
        # a key a job set without its own prefix ("rr:1") is read under its source too ("renewal-review:rr:1")
        if rx.search(key) or (src and not key.startswith(src + ":") and rx.search(f"{src}:{key}")):
            return k
    text = str(a.get("text") or "")
    for rx, k in _TEXT:
        if rx.search(text):
            return k
    return "fix"


def mapping_table() -> str:
    """Every ask kind and where it goes, as markdown (the PR and the report print it): one row per key pattern
    (KEYMAP) and per weekly-test wording (TEXTMAP), then the kinds every job shares."""
    where = {"bot": "bot looks it up itself", "dev": "dev channel", "info": "off the post (thread, For info)",
             "sheet": "All Inboxes sheet", "thread": "thread, closest shape (sentence parked)", "W": "Watching line"}

    def dest(kind: str) -> str:
        d, _, prop = KINDS[kind]
        return (f"sentence {d}" if d.isdigit() else where.get(d, d)) + (f" ({prop})" if prop else "")
    rows = ["| Ask (key or wording) | What it is | Goes to |", "|---|---|---|"]
    for p, k, what in KEYMAP:
        rows.append(f"| `{p}` | {what} | {dest(k)} |")
    for p, k, what in TEXTMAP:
        rows.append(f"| weekly test words: `{p[:60]}` | {what} | {dest(k)} |")
    rows += [f"| any key, kind broken | {KINDS['broken'][1]} | dev channel |",
             f"| any key, kind info | {KINDS['info'][1]} | {where['info']} |",
             "| inbox counts (inbox-status numbers) | where the inboxes stand | All Inboxes sheet (not posted) |",
             "| anything else | a person-only ask with no row here | thread, closest shape (P7) |"]
    rows += ["", "| Kind | Goes to | How |", "|---|---|---|"]
    rows += [f"| {k} | {dest(k)} | {what} |" for k, (d, what, prop) in KINDS.items()]
    rows += ["", "Proposed sentences (parked for Manveen's OK; the thread uses them meanwhile):", ""]
    rows += [f"- {k}: \"{v}\"" for k, v in PROPOSALS.items()]
    return "\n".join(rows)


# --- lookups (read-only, from what the digest already loaded) -----------------------------------------------------------

def _day(d) -> str:
    if isinstance(d, str):
        try:
            d = date.fromisoformat(d[:10])
        except ValueError:
            return ""
    if isinstance(d, datetime):
        d = d.astimezone(IST).date()
    return f"{d:%a} {d.day} {d:%b}" if d else ""


def _short(d: date) -> str:
    return f"{d.day} {d:%b}"


class Ctx:
    """What the lookups read: Mongo v2_verdicts rows (``v2``), inbox_status rows (``status``), the weekly tests
    (``tests``, inbox_status.weekly_tests), today (IST)."""

    def __init__(self, ctx: dict | None, now: datetime):
        ctx = ctx or {}
        self.today = now.astimezone(IST).date()
        self.v2 = list(ctx.get("v2") or [])
        self.status = [r for r in ctx.get("status") or [] if r.get("state") != "gone"]
        self.tests = list(ctx.get("tests") or [])
        self.tests_read = ctx.get("tests") is not None      # not read: no lookup may conclude "no weekly test"
        self.by_dom: dict[str, list[dict]] = defaultdict(list)
        self.by_email: dict[str, dict] = {}
        for r in self.status:
            e = str(r.get("email") or "").lower()
            d = str(r.get("domain") or e.rsplit("@", 1)[-1]).lower()
            self.by_dom[d].append(r)
            if e:
                self.by_email[e] = r

    def client(self, dom: str = "", email: str = "") -> str:
        """The client a domain (or inbox) belongs to: its weekly test's client (Mongo v2_verdicts) first, then All
        Inboxes. All Inboxes files an inbox pulled out of its campaigns under its Smartlead account's client (23 Nov
        2026: a Melior domain in the PL account read "Precise Leads"); the test is the client's own."""
        dom = (dom or email.rsplit("@", 1)[-1]).lower()
        v = Counter(ds.client_name(r.get("client")) for r in self.v2 if str(r.get("domain") or "").lower() == dom
                    and r.get("client"))
        if v:
            return v.most_common(1)[0][0]
        r = self.by_email.get(email.lower()) if email else None
        if r and r.get("client"):
            return ds.client_name(r["client"])
        c = Counter(ds.client_name(r.get("client")) for r in self.by_dom.get(dom, []) if r.get("client"))
        return c.most_common(1)[0][0] if c else ""

    def account(self, dom: str, client: str = "") -> str:
        """The Smartlead account that holds the domain's inboxes (All Inboxes), in words; when it is another client's,
        it says whose inboxes sit there."""
        acc = Counter(str(r.get("account") or "") for r in self.by_dom.get(dom.lower(), []) if r.get("account"))
        words = _account([acc.most_common(1)[0][0]] if acc else [], client)
        held = ds.client_name(str(acc.most_common(1)[0][0]).replace("_", " ")) if acc else ""
        return words + (f" (where {client}'s inboxes are)" if client and held and held != client else "")

    def inboxes(self, dom: str) -> int:
        return len(self.by_dom.get(dom.lower(), []))

    def latest(self, dom: str) -> dict | None:
        """The domain's latest weekly test: {verdict, night, provider, n, m, failed, judged} (n of m test emails in
        the inbox at the failing inboxes' own provider, as the weekly job counts a domain fail), or None."""
        rows = [r for r in self.v2 if str(r.get("domain") or "").lower() == dom.lower() and r.get("weekly") is not False
                and not r.get("history")]
        if not rows:
            return None
        night = max(str(r.get("night")) for r in rows)
        rows = [r for r in rows if str(r.get("night")) == night]
        failed = [r for r in rows if str(r.get("verdict")) == "FAIL"]
        out = {"night": night, "judged": len(rows), "failed": len(failed),
               "verdict": "FAIL" if failed and len(failed) * 2 >= len(rows) else
               ("PASS" if all(str(r.get("verdict")) == "PASS" for r in rows) else "MIXED")}
        if failed:
            own = Counter(OWN.get(str(r.get("type") or "").upper(), "microsoft") for r in failed)
            at = own.most_common(1)[0][0]
            out["provider"] = PROVIDER_WORD.get(at, at.title())
            from smartlead.digest_render import _counts
            out["n"] = sum(int(_counts(r.get(at)).get("inbox") or 0) for r in failed)
            out["m"] = sum(int(_counts(r.get(at)).get("sent") or 0) for r in failed)
        return out

    def next_test(self, dom: str = "", campaign: str = "") -> date | None:
        """The next run (IST date after today) of a weekly test that sends from an inbox on ``dom``, or whose test
        campaign is ``campaign``; None when no active weekly test covers it."""
        days = []
        for t in self.tests:
            senders = {str(s).lower() for s in t.get("senders") or []}
            hit = (dom and any(s.endswith("@" + dom.lower()) for s in senders)) or \
                  (campaign and str(t.get("campaign") or "").strip().lower() == campaign.strip().lower())
            if not hit:
                continue
            try:
                d = datetime.fromisoformat(str(t.get("next_run")).replace("Z", "+00:00")).astimezone(IST).date()
            except (TypeError, ValueError):
                continue
            if d > self.today:
                days.append(d)
        return min(days) if days else None


# --- parsing the asks' own words (stored asks carry text and a list, not facts) -----------------------------------------

_EMAIL = re.compile(r"[a-z0-9._%+'-]+@[a-z0-9-]+(?:\.[a-z0-9-]+)+", re.I)
_DOM = re.compile(r"(?<![\w@.-])(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z]{2,24}(?![\w.-]*\w)(?!@)", re.I)
_FILES = ("csv", "json", "py", "txt", "md", "png", "js", "log", "html", "pdf", "xlsx")
_MONTHS = {m: i for i, m in enumerate(("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov",
                                       "Dec"), 1)}


def _doms(text: str) -> list[str]:
    t = _EMAIL.sub(" ", str(text or ""))
    return list(dict.fromkeys(d.lower() for d in _DOM.findall(t) if d.rsplit(".", 1)[-1].lower() not in _FILES))


def _lines(a: dict) -> list[str]:
    from smartlead.digest import bullets, plain
    return [plain(x) for x in bullets(a.get("detail"))]


def _date_of(day_month: str, today: date) -> date | None:
    m = re.match(r"(\d{1,2}) (\w{3})", str(day_month or ""))
    if not m or m.group(2) not in _MONTHS:
        return None
    d = date(today.year, _MONTHS[m.group(2)], int(m.group(1)))
    if (d - today).days < -180:
        d = date(today.year + 1, d.month, d.day)
    return d


def _join(xs: list[str], n: int = ds.MAX_NAMED) -> str:
    xs = list(dict.fromkeys(xs))
    return ", ".join(xs[:n]) + (f" and {len(xs) - n} more" if len(xs) > n else "")


def _item(s: str, f: dict, a: dict, *, by: date | None = None, detail=None, steps=None, kind: str = "") -> dict:
    out = {"s": s, "f": f, "by": by.isoformat() if by else None, "since": str(a.get("since") or ""),
           "detail": list(detail if detail is not None else _lines(a)), "steps": [x for x in (steps or []) if x],
           "kind": kind, "key": a.get("key")}
    if a.get("lines"):
        out["cap"] = int(a["lines"])          # a working list (the delete reminder): its own length in the thread
    return out


def _thread(kind: str, a: dict, client: str = "", what: str = "", step: str | None = None, detail=None) -> dict:
    from smartlead.digest import plain
    return {"s": KINDS[kind][2], "kind": kind, "f": {"client": client}, "what": plain(what or a.get("text") or ""),
            "step": plain(a.get("step") or "") if step is None else step, "since": str(a.get("since") or ""),
            "detail": list(detail if detail is not None else _lines(a)), "steps": [plain(a.get("more") or "")]
            if a.get("more") else [], "key": a.get("key")}


def _dev(a: dict, text: str | None = None) -> str:
    from smartlead.digest import plain
    t = plain(text or a.get("text") or "").strip()
    step = plain(a.get("step") or "").strip()
    return t + (f" {step}" if step and step not in t else "")


# --- per kind -----------------------------------------------------------------------------------------------------------

FAILED_HOW = ("In {account}, search {domain} and take any of its inboxes still in a campaign out "
              "(the campaign's Email Accounts tab); All Inboxes already marks them do not use. At the domain's next bill, "
              "let it lapse.")
UNTAG_HOW = ("In {account}, Email Accounts: search the inbox and remove its DO NOT USE tag. Its warm-up score is back "
             "above 90%; the bot never removes a tag.")
WATCH_HOW = "If that test fails, this becomes a \"failed its weekly test\" item; if it passes, nothing to do."
REMOVED_HOW = {
    "delete": "In {account}, Email Accounts: search each domain listed. Under \"all inboxes on "
              "these domains\" delete every inbox of the domain; otherwise only the inboxes named. {provider} "
              "already removed them, so they can't send again. Deleting is permanent.",
    "campaign": "In {account}: open each campaign named, remove these inboxes from it (its Email "
                "Accounts tab), then delete them in Email Accounts. {provider} already removed them. Deleting is "
                "permanent.",
}

_KILL_LINE = re.compile(r"^•\s*(?:(?P<client>[A-Z][\w ]*?) · )?(?P<dom>[a-z0-9.-]+\.[a-z]{2,24})\b(?:[: ]|$)", re.I)


def _kill(a: dict, c: Ctx, plan: dict) -> None:
    """One item per domain on the kill list. Sentence 1 when the domain's latest weekly test failed (its numbers);
    otherwise the thread (P6: the kill list came from an older or classic test)."""
    lines = _lines(a)
    doms: dict[str, list[str]] = {}
    for x in lines:
        m = _KILL_LINE.match(x)
        ds_ = [m.group("dom").lower()] if m else (_doms(x) or [e.rsplit("@", 1)[1].lower() for e in _EMAIL.findall(x)])
        for d in ds_[:1]:
            doms.setdefault(d, []).append(x)
    for e in _EMAIL.findall(str(a.get("text") or "")):
        doms.setdefault(e.rsplit("@", 1)[1].lower(), [])
    named = {}
    for x in lines:
        m = _KILL_LINE.match(x)
        if m and m.group("client"):
            named[m.group("dom").lower()] = ds.client_name(m.group("client"))
    for dom, dl in doms.items():
        v = c.latest(dom)
        client = named.get(dom) or c.client(dom)
        k = c.inboxes(dom) or _count(dl) or len(dl)
        if v and v["verdict"] == "FAIL" and v.get("m"):
            dl = [_test_line(v)] + [re.sub(r";\s*domain landing in spam on the classic tests", "", x) for x in dl]
            plan["items"].append(_item("1", {"client": client, "domain": dom, "n": v["n"], "m": v["m"],
                                             "provider": v["provider"], "k": k}, a, detail=dl,
                                       steps=[FAILED_HOW.format(account=c.account(dom, client), domain=dom)], kind="kill-list"))
        else:
            plan["thread"].append(_thread("take-out", a, client, f"{_n(k)} on {dom} {'is' if k == 1 else 'are'} on "
                                          "the kill list from older placement tests, with no failed weekly test",
                                          detail=dl))


def _test_line(v: dict) -> str:
    return (f"Weekly test of {_day(v['night'])}: {v['failed']} of {v['judged']} inboxes failed; {v['n']} of {v['m']} test "
            f"emails reached the inbox at {v['provider']}, their own provider.")


def _account(accounts, client: str = "") -> str:
    """Where to do it: "the Precise Leads Smartlead account" (the account that holds the inboxes, from All Inboxes;
    23 Nov 2026: Melior's domains sit in the Precise Leads account), else the client's."""
    names = sorted({ds.client_name(str(a).replace("_", " ")) for a in accounts or [] if a})
    if names:
        return f"the {' and '.join(names)} Smartlead account{'s' if len(names) > 1 else ''}"
    return f"{client}'s Smartlead account".replace("s's ", "s' ") if client else "the client's Smartlead account"


def _n(k: int) -> str:
    return f"{k} inbox" if k == 1 else f"{k} inboxes"


def _count(lines: list[str]) -> int:
    n = 0
    for x in lines:
        m = re.search(r"\((\d+) inbox", x) or re.search(r": (\d+) inbox", x)
        n += int(m.group(1)) if m else len(_EMAIL.findall(x))
    return n


_V2FAIL = re.compile(r"(\d+) of (\d+) inbox(?:es)? on ([a-z0-9.-]+\.[a-z]{2,24}) failed the weekly test.*?\((\w+): (\d+) of "
                     r"(\d+) test emails in the inbox\)", re.I)


def _v2_fail(a: dict, c: Ctx, plan: dict) -> None:
    m = _V2FAIL.search(str(a.get("text") or ""))
    if not m:
        plan["thread"].append(_thread("take-out", a))
        return
    dom = m.group(3).lower()
    prov = PROVIDER_WORD.get(m.group(4).lower(), m.group(4).title())
    plan["items"].append(_item("1", {"client": c.client(dom), "domain": dom, "n": int(m.group(5)),
                                     "m": int(m.group(6)), "provider": prov, "k": c.inboxes(dom) or int(m.group(2))},
                               a, steps=[FAILED_HOW.format(account=c.account(dom, c.client(dom)), domain=dom)],
                               kind="v2-domain-fail"))


_BILL = re.compile(r"^(?P<prov>[A-Z][\w]*) will charge (?P<who>.*?)(?P<money>\$[\d,]+(?:\.\d\d)?) on (?P<day>\d{1,2} "
                   r"\w{3}) for (?P<label>\d+ [^:]*?inbox(?:es)?)")
_DROP = re.compile(r"^•?\s*(?P<dom>[\w.-]+\.[a-z]{2,24}): drop, (?P<k>\d+) inbox")
_DROP1 = re.compile(r"^•?\s*(?P<e>[\w.%+'-]+@[\w.-]+): drop\b")
_CHECK = re.compile(r": check, ")


def _bill(a: dict, c: Ctx, plan: dict) -> None:
    """A renewal-review bill: sentence 2 for the inboxes it lists as drop; the keep-or-cancel ones to the thread."""
    from smartlead.digest import plain
    text = plain(a.get("text") or "")
    m = _BILL.match(text)
    lines = _lines(a)
    drops: dict[str, int] = {}
    for x in lines:
        if d := _DROP.match(x):
            drops[d.group("dom").lower()] = drops.get(d.group("dom").lower(), 0) + int(d.group("k"))
        elif d := _DROP1.match(x):
            dom = d.group("e").rsplit("@", 1)[1].lower()
            drops[dom] = drops.get(dom, 0) + 1
    checks = [x for x in lines if _CHECK.search(x)]
    if not m:
        plan["thread"].append(_thread("bill-check" if checks or not drops else "bill-check", a))
        return
    by = _date_of(m.group("day"), c.today)
    who = ds.client_name(m.group("who").strip().rstrip(","))
    label = m.group("label")
    n_bill = int(label.split()[0])
    kind = re.match(r"\d+ (Google|Outlook|Microsoft|SMTP) inbox", label)
    if drops:
        k = sum(drops.values())
        client = who or c.client(next(iter(drops)))
        whole = k == n_bill
        f = {"client": client, "k": f"{k} {kind.group(1)}" if whole and kind else k, "domain": _join(list(drops)),
             "provider": m.group("prov"), "date": m.group("day"), "cost": m.group("money") if whole else None}
        on = re.search(r" on (?P<doms>[^:]+?)(?::| that we stopped|$)", text[m.end():])
        bill = (f"The bill: {m.group('money')} on {m.group('day')} for {label}"
                + (f" on {on.group('doms')}" if on else "") + ".")
        if not whole:
            bill += f" Only these {k} are dropped; the other {n_bill - k} stay in use, so the bill itself goes on."
        plan["items"].append(_item("2", f, a, by=by, detail=[bill] + [x for x in lines if not _CHECK.search(x)],
                                   steps=[plain(a.get("step") or "")], kind="bill-drop"))
    if checks:
        k = sum(int(x) for x in re.findall(r"(\d+) inbox", " ".join(checks))) or len(checks)
        plan["thread"].append(_thread("bill-check", a, who, f"decide keep or cancel for {_n(k)} on the "
                                      f"{m.group('prov')} bill of {m.group('day')}", detail=checks))


def _removed(a: dict, c: Ctx, plan: dict) -> None:
    """The delete reminder (provider_removed): sentence 3 per client and provider for "delete now", sentence 4 per
    client for "check first" inboxes still in a real campaign, the thread for those a lead replied to (P8), a dev line
    for those whose check did not finish."""
    f = a.get("facts") or {}
    groups = f.get("groups")
    if groups is None:          # stored before t409: words only
        plan["thread"].append(_thread("disconnected", a, what=a.get("text")))
        return
    for g in groups:
        prov = g.get("provider") or " and ".join(g.get("providers") or []) or "Their provider"
        how = REMOVED_HOW.get(g["kind"], "").format(account=_account(g.get("accounts"), g["client"]), provider=prov)
        if g["kind"] == "delete":
            plan["items"].append(_item("3", {"client": g["client"], "k": g["k"], "provider": g["provider"]}, a,
                                       detail=g["lines"], steps=[how], kind="removed-delete"))
        elif g["kind"] == "campaign":
            camps = g["campaigns"]
            where = camps[0] if len(camps) == 1 else f"{len(camps)} campaigns"
            plan["items"].append(_item("4", {"client": g["client"], "k": g["k"], "campaign": where}, a,
                                       detail=g["lines"], steps=[how], kind="removed-in-campaign"))
        elif g["kind"] == "replied":
            plan["thread"].append(_thread("removed-replied", a, g["client"],
                                          f"{_n(g['k'])} their provider removed had a lead reply in the last 30 days",
                                          step="answer the lead, then delete them in Smartlead", detail=g["lines"]))
        elif g["kind"] == "unchecked":
            plan["dev"].append(f"Removed inboxes: the campaign and reply check did not finish for {g['k']} "
                               f"{g['client']} inboxes, so they are on no delete list yet; the next morning's check "
                               f"places them.")


_REPAIRED = re.compile(r"^•\s*(?P<e>[\w.%+'-]+@[\w.-]+)")


def _untag(a: dict, c: Ctx, plan: dict) -> None:
    """Sentence 5 per inbox (a client with more than MAX_NAMED: one item, "N inboxes, list in thread")."""
    by: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for x in _lines(a):
        m = _REPAIRED.match(x)
        if m:
            e = m.group("e").lower()
            by[c.client(email=e)].append((e, x))
    if not by:
        plan["thread"].append(_thread("fix", a))
        return
    for client, rows in by.items():
        if len(rows) <= ds.MAX_NAMED:
            for e, x in rows:
                plan["items"].append(_item("5", {"client": client, "inbox": e}, a, detail=[x],
                                           steps=[UNTAG_HOW.format(account=c.account(e.rsplit("@", 1)[1], client))],
                                           kind="ready-to-untag"))
        else:
            plan["items"].append(_item("5", {"client": client, "inbox": f"{len(rows)} inboxes (list in thread)"}, a,
                                       detail=[x for _, x in rows],
                                       steps=[UNTAG_HOW.format(account=c.account(rows[0][0].rsplit("@", 1)[1], client))],
                                       kind="ready-to-untag"))


_REPLY = re.compile(r"^•\s*(?P<dom>[a-z0-9.-]+\.[a-z]{2,24})(?: \((?P<client>[^)]*)\))?: (?P<a>\d+) repl(?:y|ies) on "
                    r"(?P<sends>[\d,]+) sends?.*?(?P<b>\d+(?:\.\d+)?) expected", re.I)


def _reply(a: dict, c: Ctx, plan: dict) -> None:
    """Replies low on a domain. The bot reads the domain's latest weekly verdict: FAIL -> sentence 1; otherwise
    Watching until the next weekly test; no weekly test covers it -> sentence 7. A line it can't read goes to the
    thread as the job wrote it (never lost)."""
    unread = []
    for x in _lines(a) or [str(a.get("text") or "")]:
        m = _REPLY.match(x)
        if not m:
            unread.append(x)
            continue
        dom = m.group("dom").lower()
        client = ds.client_name(m.group("client")) if m.group("client") else c.client(dom)
        v = c.latest(dom)
        if v and v["verdict"] == "FAIL" and v.get("m"):
            plan["items"].append(_item("1", {"client": client, "domain": dom, "n": v["n"], "m": v["m"],
                                             "provider": v["provider"], "k": c.inboxes(dom) or v["judged"]}, a,
                                       detail=[x], steps=[FAILED_HOW.format(account=c.account(dom, client), domain=dom)],
                                       kind="reply-alert"))
            continue
        nxt = c.next_test(dom=dom)
        if not c.tests_read:
            unread.append(x)
        elif nxt:
            plan["watch"].append({"s": "W", "f": {"domain": dom, "a": int(m.group("a")),
                                                  "b": int(round(float(m.group("b")))), "date": _day(nxt)},
                                  "since": str(a.get("since") or ""), "detail": [x, WATCH_HOW], "kind": "reply-alert",
                                  "key": a.get("key")})
        else:
            plan["items"].append(_item("7", {"client": client, "k": c.inboxes(dom) or 1}, a, detail=[x],
                                       steps=[f"Replies on {dom} are low and no weekly test sends from it: add its "
                                              "inboxes to the client's weekly test in Smart Delivery."],
                                       kind="reply-alert"))
    if unread:
        doms = _doms(" ".join(unread))
        plan["thread"].append(_thread("fix", a, c.client(doms[0]) if doms else "", detail=unread))


def _no_result(a: dict, c: Ctx, plan: dict) -> None:
    """No weekly result for test campaigns: the bot looks their weekly tests up. A test is scheduled: a dev line (the
    run went missing; nothing a person sets up). None: sentence 7."""
    f = a.get("facts") or {}
    names = f.get("names") or re.findall(r"DT [\w]+ [\w# ]+?(?=,| \(|$)", str(a.get("text") or ""))
    n = int(f.get("n") or 0)
    if not c.tests_read or not names:
        plan["thread"].append(_thread("fix", a))
        return
    missing = [nm for nm in names if not c.next_test(campaign=nm)]
    if not missing:
        days = sorted({_day(c.next_test(campaign=nm)) for nm in names})
        plan["dev"].append(f"Weekly test: no finished run last week for the test campaign{'s' if len(names) != 1 else ''} "
                           f"{', '.join(names)} ({n} inboxes); {'their tests are' if len(names) != 1 else 'its test is'} "
                           f"scheduled, next run {', '.join(days)}.")
        return
    for nm in missing:
        client = ds.client_name(nm.split()[1]) if len(nm.split()) > 1 else ""
        plan["items"].append(_item("7", {"client": client, "k": n if len(missing) == 1 else len(missing)}, a,
                                   detail=[f"Test campaign {nm}: no active weekly test in Smart Delivery"],
                                   steps=["In Smart Delivery, create the weekly test for this test campaign (its "
                                          "senders are the campaign's inboxes)."], kind="no-result"))


_COVER = re.compile(r"^•?\s*(?:(?P<client>[A-Z][\w ]*?) · )?(?P<dom>[a-z0-9.-]+\.[a-z]{2,24})\b", re.I)


def _no_test(a: dict, c: Ctx, plan: dict) -> None:
    """Sentence 7, one per client, from the coverage or inbox-list ask's list."""
    from smartlead.digest import plain
    lines = _lines(a)
    by: dict[str, list[str]] = defaultdict(list)
    n_by: Counter = Counter()
    for x in lines:
        m = _COVER.match(x)
        dom = m.group("dom").lower() if m else (_doms(x) or [""])[0]
        emails = _EMAIL.findall(x)
        client = ds.client_name(m.group("client")) if m and m.group("client") else \
            (c.client(email=emails[0]) if emails else c.client(dom))
        by[client].append(x)
        k = re.search(r"(\d+) inbox", x)
        n_by[client] += int(k.group(1)) if k else max(len(emails), 1)
    if not by:
        k = re.search(r"(\d+) inbox", str(a.get("text") or ""))
        by[""] = []
        n_by[""] = int(k.group(1)) if k else 0
    for client, xs in by.items():
        if not n_by[client]:
            plan["thread"].append(_thread("fix", a, client))
            continue
        plan["items"].append(_item("7", {"client": client or c.client(), "k": n_by[client]}, a, detail=xs,
                                   steps=[plain(a.get("step") or ""), plain(a.get("more") or "")],
                                   kind="no-weekly-test"))


def _seeds(a: dict, c: Ctx, plan: dict) -> None:
    f = a.get("facts") or {}
    test = f.get("test") or "a weekly test"
    plan["dev"].append(f"Weekly test: the test \"{test}\" was read before all its results were in "
                       f"({f.get('n', '?')} missing); the weekly job reads an API run again by itself, a downloaded "
                       f"file needs downloading again.")


def route(asks: list[dict], broken: list[dict], infos: list[dict], ctx: dict | None, now: datetime) -> dict:
    """Where every ask goes (see the module doc). ``asks`` are the person asks after holds, the delete reminder's
    split and the folding of older items (digest_post.build), ``broken`` the job problems, ``infos`` the facts."""
    c = Ctx(ctx, now)
    plan = {"items": [], "watch": [], "thread": [], "dev": [], "info": list(infos), "kinds": Counter()}
    for b in broken:
        plan["dev"].append(_dev(b))
        plan["kinds"]["broken"] += 1
    for a in asks:
        k = kind_of(a)
        plan["kinds"][k] += 1
        try:
            if k == "kill-list":
                _kill(a, c, plan)
            elif k == "v2-domain-fail":
                _v2_fail(a, c, plan)
            elif k == "bill":
                _bill(a, c, plan)
            elif k == "removed":
                _removed(a, c, plan)
            elif k == "ready-to-untag":
                _untag(a, c, plan)
            elif k == "reply-alert":
                _reply(a, c, plan)
            elif k == "no-result":
                _no_result(a, c, plan)
            elif k == "no-weekly-test":
                _no_test(a, c, plan)
            elif k == "seeds-pending":
                _seeds(a, c, plan)
            elif k == "broken":
                plan["dev"].append(_dev(a))
            elif k == "info":
                plan["info"].append(a)
            else:
                doms = sorted({d for d in _doms(" ".join([str(a.get("text") or "")] + _lines(a)))})
                client = c.client(doms[0]) if doms else ""
                plan["thread"].append(_thread(k if KINDS.get(k, ("",))[0] == "thread" else "fix", a, client))
        except Exception as exc:  # noqa: BLE001 - an ask is never lost to its routing: it goes to the thread as it is
            plan["dev"].append(f"Digest: an ask could not be routed ({type(exc).__name__}); it is in the thread as "
                               f"the job wrote it.")
            plan["thread"].append(_thread("fix", a))
    _dedupe(plan)
    _order(plan)
    return plan


def _dedupe(plan: dict) -> None:
    """One item per sentence and domain (or inbox): the kill list and a reply alert on the same failed domain are one
    item; their lists go together."""
    seen: dict[tuple, dict] = {}
    out = []
    for it in plan["items"]:
        f = it["f"]
        key = (it["s"], str(f.get("domain") or f.get("inbox") or ""), str(f.get("client") or ""),
               "" if (f.get("domain") or f.get("inbox")) else str(f.get("provider") or f.get("campaign") or it["key"]))
        if key in seen:
            seen[key]["detail"] += [x for x in it["detail"] if x not in seen[key]["detail"]]
            seen[key]["steps"] += [x for x in it["steps"] if x not in seen[key]["steps"]]
            continue
        seen[key] = it
        out.append(it)
    plan["items"] = out
    doms = {str(it["f"].get("domain")) for it in out if it["s"] == "1"}
    plan["watch"] = [w for w in plan["watch"] if w["f"]["domain"] not in doms]


def _order(plan: dict) -> None:
    """Dated items first (soonest), then by PRIORITY, then oldest first. The post shows the first MAX_ITEMS; the
    thread takes the rest in the order they were opened (oldest first), then the person-only asks with no sentence."""
    far = "9999-12-31"
    plan["items"].sort(key=lambda it: (it.get("by") or far, PRIORITY.get(it["s"], 9), it.get("since") or far))
    head, rest = plan["items"][:ds.MAX_ITEMS], plan["items"][ds.MAX_ITEMS:]
    rest.sort(key=lambda it: it.get("since") or far)
    plan["items"] = head + rest
    plan["thread"].sort(key=lambda it: it.get("since") or far)
    plan["watch"].sort(key=lambda w: -(w["f"]["b"] - w["f"]["a"]))
