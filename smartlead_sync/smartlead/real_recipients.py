"""What real recipients say, by sending inbox type and recipient provider (pure parts).

Why (12-13 Nov 2026): Smart Delivery v2, Zapmail's seed test and real mailboxes disagreed about Google inboxes. Real
replies were the one signal everybody trusts (Avinash, monthly review 12 Nov: "the only thing we can look at is the
response rate"), and they showed Google-sent mail reaching Gmail recipients 1.5-2.5x as often as Outlook-sent mail in
the same campaigns, while v2 failed nearly every Google inbox. Until then that study ran only when someone asked.
Manveen, 13 Nov 2026: make it weekly.

An event is a lead who replied with a conversation or sent an out-of-office (provider_watch.lead_events). Gmail and
Exchange send no automatic reply for mail they filed as spam or junk, so an out-of-office shows the mail reached the
inbox. A bounce notice is not an event.

Smartlead's lead and statistics rows do not say which inbox emailed a lead; only a lead's message history does. So
every lead with an event is looked up, and for the other leads a sample of up to SAMPLE per (campaign, recipient
provider) gives the sender mix, scaled to that cell's lead count. Lead counts are estimates; event counts are exact.

The fair comparison is inside a campaign: Google-sent against Outlook-sent mail to the same recipient provider, in
campaigns that used both (same email, list and dates).
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import date

DAYS = 28
SAMPLE = 25                 # quiet leads looked up per (campaign, recipient provider)
FEW = 5                     # fewer events than this on either side: the ratio is shown as too few to read
MIN_SAMPLED = 20            # fewer leads looked up than this on either side: its lead count is a guess, no ratio
                            # (first live run, 13 Nov 2026: 7 looked-up leads turned a 1.5x into a 0.5x)
RECIPIENT = {"google": "Gmail", "microsoft": "Microsoft", "proofpoint": "Proofpoint"}   # anything else: "other"
SENDER = {"GMAIL": "Google", "OUTLOOK": "Outlook", "SMTP": "SMTP"}
BOUNCE = re.compile(r"address not found|message blocked|not delivered|couldn'?t be delivered|could not be delivered|"
                    r"may not exist|undeliverable|delivery (status|has failed|failure)|mail delivery", re.I)


def recipient_of(provider: str) -> str:
    return RECIPIENT.get(str(provider or "").lower(), "other")


def sender_of(kind: str) -> str:
    return SENDER.get(str(kind or "").upper(), "unknown")


def read_history(hist) -> tuple[str | None, str]:
    """A lead's message history (list, or {"history": [...]}) -> (the inbox that sent the newest email, the first
    reply's text, tags stripped, 400 characters). Pure."""
    rows = (hist.get("history") if isinstance(hist, dict) else hist) or []
    sent = [m for m in rows if str(m.get("type")).upper() == "SENT"]
    reps = [m for m in rows if str(m.get("type")).upper() == "REPLY"]
    sender = str(sent[-1].get("from") or "").strip().lower() if sent else None
    body = re.sub(r"<[^>]+>", " ", str(reps[0].get("email_body") or ""))[:400] if reps else ""
    return sender or None, body


def is_bounce(body: str) -> bool:
    return bool(body) and bool(BOUNCE.search(body))


STRATUM_MIN = 5             # looked-up leads a side, below which one campaign's lead split is a guess and it is left
                            # out of a comparison (review 13 Nov 2026: a 3-lead side passed inside a pooled total)


def _known(c: dict) -> dict:
    return {k: v for k, v in (c.get("mix") or {}).items() if k != "unknown"}


def sampled_ok(c: dict) -> bool:
    """A cell whose quiet leads exist but none of whose looked-up senders is known cannot be split by sender: counting
    its events alone would read as about 1,000 per 1,000 (review 13 Nov 2026). Pure."""
    return not c.get("quiet") or sum(_known(c).values()) > 0


def _split(c: dict, st: str) -> dict:
    known = _known(c)
    n = sum(known.values())
    ev = int((c.get("events_by_sender") or {}).get(st, 0))
    return {"leads": (c.get("quiet", 0) * known.get(st, 0) / n if n else 0.0) + ev, "events": ev,
            "sampled": known.get(st, 0)}


def estimates(cells: list[dict]) -> dict[tuple[str, str, str], dict]:
    """{(client, sender, recipient): {"leads", "events", "sampled"}} from the cells. A cell is one campaign and
    recipient provider: {"client", "recipient", "quiet", "mix": {sender: n sampled}, "events_by_sender": {sender: n}}
    with senders already named ("Google", "Outlook", "SMTP", "unknown"). Cells that cannot be split are left out
    (sampled_ok). Pure."""
    out: dict[tuple, dict] = defaultdict(lambda: {"leads": 0.0, "events": 0, "sampled": 0})
    for c in cells:
        if not sampled_ok(c):
            continue
        evs = {k for k, v in (c.get("events_by_sender") or {}).items() if k != "unknown" and v}
        for st in set(_known(c)) | evs:
            o, s = out[(c["client"], st, c["recipient"])], _split(c, st)
            for k in o:
                o[k] += s[k]
    return dict(out)


def compare(cells: list[dict], recipient: str, a: str = "Google", b: str = "Outlook", client: str | None = None) -> dict:
    """a-sent against b-sent mail at one recipient provider, campaign by campaign (one client, or all of them).
    {a: totals, b: totals, "ratio": the Mantel-Haenszel rate ratio over the campaigns or None, "strata": n}.
    A campaign counts only if both types emailed that provider there with STRATUM_MIN+ looked-up leads each, so a
    campaign with a better list cannot pass its rate to the type that happened to send more of it (review 13 Nov
    2026: two campaigns at 1.1-1.2x each pooled to 3.0x when summed). Pure."""
    tot = {a: {"leads": 0.0, "events": 0, "sampled": 0}, b: {"leads": 0.0, "events": 0, "sampled": 0}}
    num = den = 0.0
    strata = 0
    for c in cells:
        if c["recipient"] != recipient or (client is not None and c["client"] != client) or not sampled_ok(c):
            continue
        x, y = _split(c, a), _split(c, b)
        if min(x["sampled"], y["sampled"]) < STRATUM_MIN or not x["leads"] or not y["leads"]:
            continue
        strata += 1
        for st, s in ((a, x), (b, y)):
            for k in tot[st]:
                tot[st][k] += s[k]
        T = x["leads"] + y["leads"]
        num += x["events"] * y["leads"] / T
        den += y["events"] * x["leads"] / T
    return {**tot, "ratio": (num / den) if den else None, "strata": strata}


def paired(cells: list[dict], a: str = "Google", b: str = "Outlook") -> dict[tuple[str, str], dict]:
    """{(client, recipient): compare(...)} wherever at least one campaign compares the two types. Pure."""
    out = {}
    for cl, rp in sorted({(c["client"], c["recipient"]) for c in cells}):
        r = compare(cells, rp, a, b, client=cl)
        if r["strata"]:
            out[(cl, rp)] = r
    return out


def rate(d: dict) -> float | None:
    """Events per 1,000 leads; None without leads."""
    return 1000 * d["events"] / d["leads"] if d.get("leads") else None


def _ratio(x: dict, y: dict, r: float | None = None) -> str:
    """" (2.1x)" or why there is no ratio. ``r`` = a campaign-by-campaign ratio; without it, the plain one."""
    if rate(x) is None or rate(y) is None:
        return ""
    if min(x["events"], y["events"]) < FEW:
        return " (too few replies to compare)"
    if min(x.get("sampled", MIN_SAMPLED), y.get("sampled", MIN_SAMPLED)) < MIN_SAMPLED:
        return " (too few leads looked up to compare)"
    if r is None:
        r = rate(x) / rate(y) if rate(y) else None
    return f" ({r:.1f}x)" if r is not None else ""


def _per(d: dict) -> str:
    r = rate(d)
    return "none sent" if r is None else f"{r:.0f}"


# ── does Smartlead's test agree with real recipients? (13 Nov 2026, "make sure our live pipeline is dynamic") ──
# Each inbox type is judged by v2 at its own provider (a Google inbox at Google seeds, an Outlook inbox at Microsoft
# seeds), so it is checked against real recipients at that provider, campaign by campaign (compare).
OWN = {"GMAIL": ("Gmail", "Google", "Outlook"), "OUTLOOK": ("Microsoft", "Outlook", "Google")}
MIN_JUDGED = 10             # inboxes of the type with a recent v2 verdict, below which v2's view is not read
UNTRUST_SHARE = 0.6         # v2 fails at least this share of the type: it says "this type is in spam"
TRUST_SHARE = 0.4           # v2 fails at most this share: it is not in the way, trust it
AGREE_BELOW = 0.7           # real replies at most this ratio against the other type: they agree it does worse
DISAGREE_FROM = 1.0         # at least this: real recipients answer it as well as the other type, so v2 is wrong
                            # between the bands on either number: no change (one noisy week cannot flip it)


TRUST_VERDICT_DAYS = 21     # only verdicts this recent count toward v2's fail share (old docs are never deleted)
HISTORY_ERRORS_MAX = 0.05   # share of message-history lookups that may fail before the decision is not saved


def complete_read(read: int, total: int, errors: dict, lookups: int) -> tuple[bool, str]:
    """May this run's trust decision be saved? Only when every account, campaign list and campaign read worked and
    few lead lookups failed: a decision from part of the data could flip a type on one client's numbers (review 13
    Nov 2026). (ok, why not). Pure."""
    if not total or read < total:
        return False, f"{total - read} of {total} Smartlead accounts not read"
    hard = sum(v for k, v in errors.items() if k.startswith(("account ", "campaign statistics", "campaign leads")))
    if hard:
        return False, f"{hard} campaign read(s) failed"
    soft = sum(v for k, v in errors.items() if k.startswith("message history"))
    if soft > max(10, HISTORY_ERRORS_MAX * lookups):
        return False, f"{soft} of {lookups} lead lookups failed"
    return True, ""


def recent_share(v2: dict[str, dict], kinds: dict[str, str], today: date) -> dict[str, tuple[int, int]]:
    """{type: (failing, judged)} over inboxes in the current accounts whose newest verdict is from the last
    TRUST_VERDICT_DAYS days. Pure."""
    from datetime import timedelta
    cut = (today - timedelta(days=TRUST_VERDICT_DAYS)).isoformat()
    out: dict[str, list] = defaultdict(lambda: [0, 0])
    for e, kind in kinds.items():
        v = v2.get(e)
        if v and str(v.get("night") or "") >= cut:
            out[kind][1] += 1
            out[kind][0] += str(v.get("verdict") or "").upper() == "FAIL"
    return {k: (a, b) for k, (a, b) in out.items()}


def trust_decision(cells: list[dict], v2_share: dict[str, tuple[int, int]], current: set[str]
                   ) -> tuple[set[str], list[str], list[str]]:
    """(types whose v2 fail should not hold an inbox back, what changed, one line per type). ``v2_share``:
    {type: (inboxes failing v2, inboxes with a recent verdict)}; ``current``: the types not trusted now (the saved
    decision, never an env override). A type changes only on enough evidence; otherwise it stays as it is. Pure."""
    new, changes, lines = set(current), [], []
    for kind, (rp, mine, other) in OWN.items():
        name = TYPE_NAMES_SHORT[kind]
        fails, judged = v2_share.get(kind, (0, 0))
        cmp_ = compare(cells, rp, mine, other)
        x, y, ratio = cmp_[mine], cmp_[other], cmp_["ratio"]
        enough = (judged >= MIN_JUDGED and min(x["events"], y["events"]) >= FEW
                  and min(x["sampled"], y["sampled"]) >= MIN_SAMPLED and ratio is not None)
        share = fails / judged if judged else 0.0
        stays = "not trusted" if kind in current else "trusted"
        seen = (f"Smartlead's test fails {fails} of {judged} {name} inboxes at {rp}; real {rp} recipients, campaign by "
                f"campaign ({cmp_['strata']}): {mine} inboxes {_per(x)}, {other} inboxes {_per(y)} per 1,000")
        if not enough:
            lines.append(f"{seen}. Not enough to judge, so it stays {stays}.")
            continue
        if share >= UNTRUST_SHARE and ratio >= DISAGREE_FROM:
            new.add(kind)
            verdict = "not trusted: real recipients answer them as well as the other inboxes"
        elif share <= TRUST_SHARE:
            new.discard(kind)
            verdict = "trusted: it fails few of them"
        elif ratio <= AGREE_BELOW:
            new.discard(kind)
            verdict = "trusted: real recipients agree they do worse"
        else:
            verdict = f"in between, so it stays {stays}"
        lines.append(f"{seen} ({ratio:.1f}x). Smartlead's test for {name} inboxes is {verdict}.")
        if (kind in new) != (kind in current):
            changes.append(f"Smartlead's test for {name} inboxes is now {'NOT trusted' if kind in new else 'trusted'}: "
                           + ("a failed test no longer takes them out of campaigns; the weekly job only reports it."
                              if kind in new else "the weekly job takes failing ones out of campaigns again."))
    return new, changes, lines


TYPE_NAMES_SHORT = {"GMAIL": "Google", "OUTLOOK": "Outlook"}
QUIET_MIN_LEADS = 200       # a campaign this big with almost no replies from ANY provider
QUIET_MAX_RATE = 3.0        # per 1,000 at every provider together: the offer or the list, not the inboxes


def quiet_campaigns(cells: list[dict]) -> list[str]:
    """Campaigns that reached QUIET_MIN_LEADS+ leads with replies under QUIET_MAX_RATE per 1,000 at every provider
    together. Pure."""
    by: dict[tuple, list] = defaultdict(lambda: [0, 0])
    for c in cells:
        k = (c["client"], c.get("name") or c.get("campaign"))
        by[k][0] += c.get("quiet", 0) + sum((c.get("events_by_sender") or {}).values())
        by[k][1] += sum((c.get("events_by_sender") or {}).values())
    return [f"• {cl} · {name}: {ev} from {n} leads" for (cl, name), (n, ev) in sorted(by.items())
            if n >= QUIET_MIN_LEADS and 1000 * ev / n < QUIET_MAX_RATE]


def format_post(cells: list[dict], today: date, days: int = DAYS, errors: dict | None = None,
                trust: tuple[list[str], list[str]] | None = None) -> tuple[list[str], list[str]]:
    """(headline lines, thread parts) for the weekly post. ``trust`` = (changes, lines) from trust_decision. Pure."""
    day = f"{today.day} {today.strftime('%b')}"
    head = [f"📬 Real replies, {days} days to {day}: replies and out-of-office messages per 1,000 leads, bounces left out"]
    pairs = paired(cells)
    est = estimates(cells)
    clients = sorted({c["client"] for c in cells})
    for cl in clients:
        bits = []
        for rp in ("Gmail", "Microsoft"):
            p = pairs.get((cl, rp))
            if p and p["Google"]["leads"] >= 1 and p["Outlook"]["leads"] >= 1:
                bits.append(f"{rp} recipients: Google inboxes {_per(p['Google'])}, Outlook inboxes {_per(p['Outlook'])}"
                            + _ratio(p["Google"], p["Outlook"], p["ratio"]))
            else:
                one = [(st, est[(cl, st, rp)]) for st in ("Google", "Outlook", "SMTP") if est.get((cl, st, rp), {}).get("leads", 0) >= 1]
                if one:
                    bits.append(f"{rp} recipients: " + ", ".join(f"{st} inboxes {_per(d)}" for st, d in one)
                                + " (no campaign used both types, so no fair comparison)")
        if bits:
            head.append(f"{cl}. " + ". ".join(bits) + ".")
    gaps = []
    for cl in clients:
        for st in ("Google", "Outlook"):
            g, m = est.get((cl, st, "Gmail")), est.get((cl, st, "Microsoft"))
            if g and m and g["leads"] >= 1 and m["leads"] >= 1 and rate(g):
                r = rate(m) / rate(g)
                gaps.append(f"• {cl}, {st} inboxes: Microsoft recipients {_per(m)} vs Gmail {_per(g)}"
                            + (_ratio(m, g) or f" ({r:.1f}x)"))
    parts = []
    changes, tlines = trust or ([], [])
    head += changes
    quiet = quiet_campaigns(cells)
    if quiet:
        head.append(f"{len(quiet)} campaign{'s' if len(quiet) != 1 else ''} got almost no replies from any provider: "
                    "that points at the offer or the list, not the inboxes (list in the thread).")
        parts.append("\n".join(["*Almost no replies from any provider*"] + quiet))
    if tlines:
        parts.append("\n".join(["*Does Smartlead's test agree with real recipients?*"] + [f"• {x}" for x in tlines]))
    if gaps:
        parts.append("\n".join(["*Microsoft against Gmail recipients, same sending inbox type*"] + gaps))
    lines = ["*Every cell* (client · sending inbox · recipient: leads emailed, estimated · replies and out-of-office · "
             "per 1,000 · leads looked up for the sender mix)"]
    for (cl, st, rp), d in sorted(est.items()):
        if d["leads"] >= 1:
            lines.append(f"• {cl} · {st} · {rp}: {round(d['leads'])} · {d['events']} · {_per(d)} · {d['sampled']}")
    parts.append("\n".join(lines))
    skipped = sum(1 for c in cells if not sampled_ok(c))
    note = ("How it is counted: every lead that replied was looked up in Smartlead to find the inbox that emailed it; "
            f"for the rest, up to {SAMPLE} leads per campaign and recipient provider were looked up and the mix scaled "
            "up, so lead counts are estimates and reply counts exact. Gmail and Microsoft send no out-of-office for "
            "mail they put in spam, so an out-of-office means the email reached the inbox.")
    note += (" Comparisons between inbox types are made campaign by campaign, so a campaign with a better list does "
             "not favour the type that sent more of it.")
    if skipped:
        note += f" Left out: {skipped} campaign and provider cell{'s' if skipped != 1 else ''} where no sender could be found."
    if errors:
        note += " Not read: " + ", ".join(f"{k} {v}" for k, v in sorted(errors.items())) + "."
    parts.append(note)
    return head, parts
