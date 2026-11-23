"""The digest's one renderer, and one item per domain (t386, 23 Nov 2026).

Two causes of the 23 Nov digest that the render guard (digest_guard) can only hide, fixed here:

  * Stored wording. A job stores its asks in Mongo digest_asks with the words of the code it ran. The weekly test job
    runs on the PC once a week, so on 23 Nov its 21 Nov asks still said "Microsoft results in
    melior-weekly-test-4-run-1.csv (2026-11-21) held", two days after PR #113 had reworded them. Now
    an ask can carry its facts ({"type": ..., ...}) and the digest words it at post time with the renderer of its type;
    an older ask without facts is read back into facts (``facts_of``, one pattern per wording a job has used), so every
    item, stored before or after a wording change, is said in today's words.
  * Contradicting items. Each job writes its own ask, so on 23 Nov forum.test was item 1 (kill list) and
    item 4 ("held: looks like a test problem, re-run"). The digest now judges each domain from one place, the All
    Inboxes verdict (usable.send_verdict on Mongo inbox_status, which the sync writes from the tab every agent trusts):
    an item that says a domain's inboxes are fine while All Inboxes says they may never send is folded into the
    stronger item on that domain as one short clause, or left out when no item about the domain remains.

    render(ask, v2_rows) -> ask             today's words for one stored ask
    verdicts(inbox_status_rows) -> {domain: worst verdict}
    resolve(asks, verdicts) -> (asks, superseded)
"""
from __future__ import annotations

import ast
import re
from datetime import date

SEVERITY = ("never", "cannot", "hold", "send")       # usable.send_verdict, worst first
WEAK = ("test-held",)                                  # fact types that say "the inboxes are fine, the test broke"
CLIENT_PREFIX = {"pl": "Precise Leads", "melior": "Melior", "bettrdata": "Bettrdata", "bd": "Bettrdata"}


def _day(iso: str | None) -> str:
    try:
        d = date.fromisoformat(str(iso)[:10])
    except (TypeError, ValueError):
        return ""
    return f"{d:%a} {d.day} {d:%b}"


def test_name(file_name: str) -> str:
    """'melior-weekly-test-4-run-1.csv' -> 'melior-weekly-test-4': the test's name as
    Smart Delivery shows it (its download is named after it), so the reader can search for it there."""
    m = re.match(r"\"?(.+?)(?:-run-\d+)?(?:\.csv)?\"?$", str(file_name or "").strip())
    return m.group(1) if m else str(file_name or "")


def client_of(test: str) -> str:
    """The client a test belongs to, from its name's first word ('pl-weekly-...' -> 'Precise Leads'), or ''."""
    return CLIENT_PREFIX.get(re.split(r"[-_ ]", str(test or "").lower(), maxsplit=1)[0], "")


# --- old and new wordings -> facts --------------------------------------------------------------------------------------

_PROV = r"(?P<prov>(?:Google|Microsoft|Outlook)(?: and (?:Google|Microsoft|Outlook))?)"
PATTERNS = [
    # PC job before PR #113 (stored 21 Nov 2026): 'Microsoft results in melior-...-run-1.csv (2026-11-21) held: ...'
    ("test-held", re.compile(_PROV + r" results in (?P<file>[\w.-]+\.csv) \((?P<night>\d{4}-\d{2}-\d{2})\) held", re.I)),
    # since PR #113: 'Microsoft results of the test "melior-..." (run 1, Sat 21 Nov) were not used: nearly every inbox'
    ("test-held", re.compile(_PROV + r" results of the test \"(?P<file>[^\"]+)\"(?: \((?:run \d+, )?(?P<day>[^)]*)\))? "
                             r"were not used", re.I)),
    # before PR #113: 'pl-weekly-...-run-1.csv: 1 seed result had not come in when it was downloaded'
    ("seeds-pending", re.compile(r"^(?P<file>[\w.-]+\.csv): (?P<n>\d+) seed results? had not come in", re.I)),
    # since PR #113: '1 result of the test "pl-weekly-..." (run 1) had not come in when it was read'
    ("seeds-pending", re.compile(r"^(?P<n>\d+) results? of the test \"(?P<file>[^\"]+)\"(?: \([^)]*\))? had not come in "
                                 r"when it was read, so its inboxes are not judged", re.I)),
    # 'No result this week from DT A copy check, DT B copy check (9 inboxes): no finished v2 run found.' (before #113)
    # and 'No result this week from DT A (3 inboxes): no finished run of its weekly test was found' (since #113)
    ("no-result", re.compile(r"^No result this week from (?P<names>.+?) \((?P<n>\d+) inbox(?:es)?\): no finished "
                             r"(?:v2 run|weekly test run|run of)", re.I)),
]


def facts_of(text: str, night: str | None = None) -> dict:
    """The facts of an ask's text, from any wording the weekly test job has used ({} when none matches). ``night``:
    the run's date when the text gives only a day name."""
    for kind, rx in PATTERNS:
        m = rx.search(str(text or ""))
        if not m:
            continue
        g = m.groupdict()
        if kind == "no-result":
            return {"type": kind, "names": [x.strip() for x in g["names"].split(", ") if x.strip()], "n": int(g["n"])}
        test = test_name(g["file"])
        if kind == "test-held":
            provs = [p.strip().title().replace("Outlook", "Microsoft") for p in g["prov"].split(" and ")]
            return {"type": kind, "providers": provs, "test": test, "client": client_of(test),
                    "night": g.get("night") or night or ""}
        return {"type": kind, "n": int(g["n"]), "test": test, "night": night or ""}
    return {}


# --- facts -> today's words ---------------------------------------------------------------------------------------------

def _test_words(f: dict) -> str:
    day = _day(f.get("night"))
    return f"the test \"{f['test']}\"" + (f" ({day})" if day else "")


def _held(f: dict) -> tuple[str, str]:
    provs = " and ".join(f.get("providers") or ["Some"])
    return (f"{provs} results of {_test_words(f)} were not used: nearly every inbox in it scored below 4 of 4 there, "
            f"which points to a broken test, so no inbox was taken out.",
            "re-run that test in Smart Delivery today (it uses one of the plan's test runs).")


def _pending(f: dict) -> tuple[str, str]:
    n = int(f.get("n") or 0)
    day = _day(f.get("night"))
    return (f"The test \"{f['test']}\"{f' ({day})' if day else ''} was read before all its results were in ({n} "
            f"missing), so its inboxes have no verdict yet.",
            "download its results again in Smart Delivery later today, so the weekly test job can judge them.")


def _no_result(f: dict) -> tuple[str, str]:
    names = [f'"{x}"' for x in f.get("names") or []]
    k, n = len(names), int(f.get("n") or 0)
    which = names[0] if k == 1 else ", ".join(names[:-1]) + " and " + names[-1] if names else "some test campaigns"
    return (f"No weekly test result this week for the test campaign{'s' if k != 1 else ''} {which} ({n} "
            f"inbox{'es' if n != 1 else ''}): no finished test run was found, so those inboxes stay held back until a "
            f"test passes.",
            f"check in Smart Delivery that {'their tests' if k != 1 else 'its test'} ran; if not, run "
            f"{'them' if k != 1 else 'it'} again.")


# The words of an ask's step and its thread note now, by ask key: a stored ask gets today's step whenever it was
# stored (23 Nov 2026: the 22 Nov reply alert still carried a 190-character step a later change had cut).
REPLY_ALERT_STEP = ("Look up each domain's latest weekly test result (All Inboxes tab): failed, take its inboxes out of "
                    "their campaigns; passed, leave it.")
REPLY_ALERT_MORE = "The bot paused nothing; the item stays until the domain's replies recover."


def _reply_alert(a: dict) -> dict:
    from smartlead.digest import reach
    doms = reach(a)
    if not doms:
        return a                           # names no domain: left as it is (the digest leaves out an ask with no list)
    step = REPLY_ALERT_STEP.replace("each domain's", "its") if len(doms) == 1 else REPLY_ALERT_STEP
    return {**a, "step": step, "more": REPLY_ALERT_MORE}


_BILL = re.compile(r"^(?P<head>.+? for (?P<n>\d+) .+? on (?P<doms>[^:]+?)) that we stopped using(?P<when> on [^.]+)?\.$")
_DROP = re.compile(r"^[•\s]*(?P<dom>[\w.-]+): drop, (?P<k>\d+) inbox")


def _bill(a: dict) -> dict:
    """A stored bill whose sentence said every inbox on it was dropped when its list drops only some (renewal_review
    before t386: '6 inboxes on a.com, b.com that we stopped using', 3 of them on a.com): today's words."""
    m = _BILL.match(str(a.get("text") or ""))
    drops = [d for d in (_DROP.match(str(x)) for x in a.get("detail") or []) if d]
    k = sum(int(d.group("k")) for d in drops)
    if not m or not drops or k >= int(m.group("n")):
        return a
    gone = ", ".join(sorted({d.group("dom") for d in drops}))
    return {**a, "text": f"{m.group('head')}: we stopped using the {k} on {gone}{m.group('when') or ''}; the other "
                         f"{int(m.group('n')) - k} stay in use."}


_REPAIR = re.compile(r"^•\s*(?P<e>\S+@\S+) · (?P<k>\w+) · (?P<a>\d+)% → (?P<b>\S+?)%? \(good from (?P<g>\d+)%\) · day "
                     r"(?P<d>\d+)(?P<rest>.*)$")
_KIND = {"google": "Google", "outlook": "Outlook", "smtp": "SMTP"}


def _repair(a: dict) -> dict:
    """A stored warm-up repair list line in the words of before t386 ('• a@x.com · google · 61% → 93% (good from
    90%) · day 6 · warm-up repaired: ... · warm-up 20→10/day, reply 20→35%'): today's words."""
    out = []
    for x in a.get("detail") or []:
        m = _REPAIR.match(str(x))
        if not m:
            out.append(x)
            continue
        flags = [f.strip() for f in m.group("rest").split(" · ") if f.strip() and "→" not in f
                 and not f.strip().startswith("warm-up repaired:")]
        out.append(f"• {m.group('e')}: {_KIND.get(m.group('k').lower(), m.group('k'))}, warm-up score "
                   f"{m.group('a')}% at the start, {m.group('b')}% now (good from {m.group('g')}%), day {m.group('d')} "
                   f"of repair" + (f", {', '.join(flags)}" if flags else ""))
    return {**a, "detail": out}


RENDERERS = {"test-held": _held, "seeds-pending": _pending, "no-result": _no_result}
STEPS_NOW = {"reply-monitor:alert": _reply_alert}
def _gate(a: dict) -> dict:
    """A stored gate self-test item with today's step (gate_selftest.STEPS): its old steps said "check the PC" with no
    "Manveen:", so the digest listed his task under "What broke" instead of counting it (t386). A check that could
    not run keeps its own step."""
    from smartlead.gate_selftest import NOT_READ_STEP, STEPS
    name = str(a.get("key") or "").split(":", 1)[-1]
    if name not in STEPS or a.get("step") == NOT_READ_STEP:
        return a
    return {**a, "step": STEPS[name], **({"detail": []} if name == "v2_job_alive" else {})}   # its list was "v2 job"


PREFIX_NOW = {"renewal-review:": _bill, "warmup-repair:": _repair, "gate-selftest:": _gate}


def _counts(x) -> dict:
    if isinstance(x, dict):
        return x
    try:
        v = ast.literal_eval(str(x))
        return v if isinstance(v, dict) else {}
    except (ValueError, SyntaxError):
        return {}


def held_domains(f: dict, v2_rows) -> list[str]:
    """The domains a held test was about, from the weekly test verdicts (Mongo v2_verdicts) of that night and client:
    the inboxes below 4 of 4 at the held provider ("nearly every inbox in it") that did not pass (a passing inbox with
    3 of 4 there was in another test of that night)."""
    want = {p.lower() for p in f.get("providers") or []}
    out = set()
    for r in v2_rows or []:
        if str(r.get("night")) != str(f.get("night")) or (f.get("client") and str(r.get("client") or "").lower()
                                                          != f["client"].lower()) or str(r.get("verdict")) == "PASS":
            continue
        for p in want:
            c = _counts(r.get(p))
            if c.get("sent") and int(c.get("inbox") or 0) < int(c.get("sent") or 0):
                out.add(str(r.get("domain") or "").lower())
    return sorted(d for d in out if d)


def render(a: dict, v2_rows=()) -> dict:
    """The ask in today's words: its facts (stored, or read back from its text) through the renderer of their type.
    An ask with no known facts keeps its text (the render guard cleans it). Never raises."""
    try:
        if a.get("key") in STEPS_NOW:
            a = STEPS_NOW[a["key"]](a)
        for prefix, fn in PREFIX_NOW.items():
            if str(a.get("key") or "").startswith(prefix):
                a = fn(a)
        f = dict(a.get("facts") or {}) or facts_of(a.get("text"), str(a.get("since") or "")[:10] or None)
        if f.get("type") not in RENDERERS:
            return a
        text, step = RENDERERS[f["type"]](f)
        if f["type"] == "test-held" and not f.get("domains"):
            f["domains"] = held_domains(f, v2_rows)
        out = {**a, "text": text, "step": step, "facts": f}
        doms = f.get("domains") or []
        if doms:
            out["where"] = ", ".join(doms[:3]) + (f" and {len(doms) - 3} more" if len(doms) > 3 else "")
        return out
    except Exception:  # noqa: BLE001 - an ask is never lost to its wording
        return a


# --- one item per domain, judged by All Inboxes -------------------------------------------------------------------------

def verdicts(rows) -> dict[str, str]:
    """{domain: the worst All Inboxes verdict of its inboxes} (usable.send_verdict on Mongo inbox_status rows: email,
    domain, busy_reason, provider). Inboxes the sync calls gone are left out."""
    from smartlead.usable import send_verdict
    out: dict[str, str] = {}
    for r in rows or []:
        if r.get("state") == "gone":
            continue
        dom = str(r.get("domain") or str(r.get("email") or "").rsplit("@", 1)[-1]).lower()
        if not dom:
            continue
        v, _ = send_verdict(r.get("busy_reason"), r.get("provider"))
        if dom not in out or SEVERITY.index(v) < SEVERITY.index(out[dom]):
            out[dom] = v
    return out


def domains_of(a: dict) -> set[str]:
    from smartlead.digest import reach
    return reach(a) | {d.lower() for d in (a.get("facts") or {}).get("domains") or []}


def resolve(asks: list[dict], verdict: dict[str, str]) -> tuple[list[dict], list[dict]]:
    """(asks, superseded): an item that says its inboxes are fine (a held test, WEAK) about domains All Inboxes says
    may never send is folded into the item on those domains that agrees with All Inboxes (as one short "also" clause),
    or, with no such item, left out (superseded: the thread says so in one line). Every other item is kept."""
    out, gone = [], []
    for a in asks:
        f = a.get("facts") or {}
        doms = domains_of(a)
        if f.get("type") not in WEAK or not doms or not all(verdict.get(d) == "never" for d in doms):
            out.append(a)
            continue
        host = next((h for h in asks if h is not a and (h.get("facts") or {}).get("type") not in WEAK
                     and domains_of(h) & doms), None)
        clause = (f"no re-run of the test \"{f.get('test')}\" ({_day(f.get('night'))}) is needed: it first looked like "
                  f"a broken test, but All Inboxes now marks the inboxes of {', '.join(sorted(doms))} \"do not use\"")
        if host is not None:
            host.setdefault("clauses", []).append(clause)
        gone.append({**a, "_domains": sorted(doms), "_folded": host is not None})
    return out, gone
