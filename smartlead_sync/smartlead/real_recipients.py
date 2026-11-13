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


def estimates(cells: list[dict]) -> dict[tuple[str, str, str], dict]:
    """{(client, sender, recipient): {"leads", "events", "sampled"}} from the cells. A cell is one campaign and
    recipient provider: {"client", "recipient", "quiet", "mix": {sender: n sampled}, "events_by_sender": {sender: n}}
    with senders already named ("Google", "Outlook", "SMTP", "unknown"). Pure."""
    out: dict[tuple, dict] = defaultdict(lambda: {"leads": 0.0, "events": 0, "sampled": 0})
    for c in cells:
        cl, rp = c["client"], c["recipient"]
        known = {k: v for k, v in (c.get("mix") or {}).items() if k != "unknown"}
        n = sum(known.values())
        evs = Counter({k: v for k, v in (c.get("events_by_sender") or {}).items() if k != "unknown"})
        for st in set(known) | set(evs):
            o = out[(cl, st, rp)]
            o["leads"] += (c.get("quiet", 0) * known.get(st, 0) / n if n else 0.0) + evs[st]
            o["events"] += evs[st]
            o["sampled"] += known.get(st, 0)
    return dict(out)


def paired(cells: list[dict], a: str = "Google", b: str = "Outlook") -> dict[tuple[str, str], dict]:
    """{(client, recipient): {a: {"leads", "events"}, b: {...}}} over campaigns where both sender types emailed that
    recipient provider (both in the sampled mix or both with events). Pure."""
    out: dict[tuple, dict] = defaultdict(lambda: {a: {"leads": 0.0, "events": 0}, b: {"leads": 0.0, "events": 0}})
    for c in cells:
        known = {k: v for k, v in (c.get("mix") or {}).items() if k != "unknown"}
        evs = c.get("events_by_sender") or {}
        if not ((known.get(a) or evs.get(a)) and (known.get(b) or evs.get(b))):
            continue
        n = sum(known.values())
        for st in (a, b):
            o = out[(c["client"], c["recipient"])][st]
            o["leads"] += (c.get("quiet", 0) * known.get(st, 0) / n if n else 0.0) + evs.get(st, 0)
            o["events"] += evs.get(st, 0)
    return dict(out)


def rate(d: dict) -> float | None:
    """Events per 1,000 leads; None without leads."""
    return 1000 * d["events"] / d["leads"] if d.get("leads") else None


def _ratio(x: dict, y: dict) -> str:
    rx, ry = rate(x), rate(y)
    if rx is None or ry is None:
        return ""
    if min(x["events"], y["events"]) < FEW:
        return " (too few replies to compare)"
    return f" ({rx / ry:.1f}x)" if ry else ""


def _per(d: dict) -> str:
    r = rate(d)
    return "none sent" if r is None else f"{r:.0f}"


def format_post(cells: list[dict], today: date, days: int = DAYS, errors: dict | None = None
                ) -> tuple[list[str], list[str]]:
    """(headline lines, thread parts) for the weekly post. Pure."""
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
                            + _ratio(p["Google"], p["Outlook"]))
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
                few = min(g["events"], m["events"]) < FEW
                gaps.append(f"• {cl}, {st} inboxes: Microsoft recipients {_per(m)} vs Gmail {_per(g)}"
                            + (" (too few replies to compare)" if few else f" ({r:.1f}x)"))
    parts = []
    if gaps:
        parts.append("\n".join(["*Microsoft against Gmail recipients, same sending inbox type*"] + gaps))
    lines = ["*Every cell* (client · sending inbox · recipient: leads emailed, estimated · replies and out-of-office · "
             "per 1,000 · leads looked up for the sender mix)"]
    for (cl, st, rp), d in sorted(est.items()):
        if d["leads"] >= 1:
            lines.append(f"• {cl} · {st} · {rp}: {round(d['leads'])} · {d['events']} · {_per(d)} · {d['sampled']}")
    parts.append("\n".join(lines))
    note = ("How it is counted: every lead that replied was looked up in Smartlead to find the inbox that emailed it; "
            f"for the rest, up to {SAMPLE} leads per campaign and recipient provider were looked up and the mix scaled "
            "up, so lead counts are estimates and reply counts exact. Gmail and Microsoft send no out-of-office for "
            "mail they put in spam, so an out-of-office means the email reached the inbox.")
    if errors:
        note += " Not read: " + ", ".join(f"{k} {v}" for k, v in sorted(errors.items())) + "."
    parts.append(note)
    return head, parts
