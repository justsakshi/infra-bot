"""Per-inbox evidence from real sends (t17, 17 Nov 2026). Manveen, 16 Nov 2026, on judging inboxes by replies: "can't
really trust that you can see all results replies everything", then "so complete your data we can't take chances we
should do everything we can do".

Every lead emailed in the window is read, not a sample (real_recipients.py samples quiet leads). A lead's message
history names the inbox that sent each email (``stats_id`` -> ``from``), so every send, reply, out-of-office and bounce
is counted for the exact inbox that sent that email, per recipient provider (the lead's MX). Rules, written for any
client: docs/INBOX_EVIDENCE.md.

  * reply: the statistics row has a reply that is not out-of-office, wrong person or a bounce notice;
  * out-of-office (OOO): the lead's category says so. It proves the email reached the inbox: Gmail and Exchange send
    none for mail in spam or junk;
  * bounce: the row is marked bounced, or the "reply" is a delivery-failure notice. Kept apart: a bounce says the
    address is bad (the lead list), not where the email landed.

A label is evidence, never a clean bill: an inbox with too few sends for a reply to be expected is "not enough data",
never "fine". Pure; the job is inbox_evidence.py.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta

from smartlead import provider_watch as pw
from smartlead import real_recipients as rr

DAYS = 28                 # the window the labels are judged on
WEEK = 7                  # the weekly counts kept beside it
EXPECT_MIN = 3.0          # replies + OOO expected below this: "not enough data" (P(0 | 3 expected) = 5%)
MISS_NO_REPLY = "Smartlead shows no reply from this lead"
MISS_NOT_READ = "lead not in any campaign read in the window (another account, an older send, or a deleted campaign)"
MISS_NO_SENDER = "reply found but the sending inbox is unknown"


@dataclass
class Lead:
    rows: list[dict]                       # GET /campaigns/{id}/statistics rows of this lead
    history: object = None                 # GET .../leads/{id}/message-history (None: not read)
    extra: dict = field(default_factory=dict)


def sent_by(history) -> tuple[dict[str, str], str]:
    """(stats_id -> sending inbox, the first reply's text) from a message history. Pure."""
    h = (history.get("history") if isinstance(history, dict) else history) or []
    out = {str(m.get("stats_id")): str(m.get("from") or "").strip().lower()
           for m in h if str(m.get("type")).upper() == "SENT" and m.get("stats_id") and m.get("from")}
    reps = [m for m in h if str(m.get("type")).upper() == "REPLY"]
    body = re.sub(r"<[^>]+>", " ", str(reps[0].get("email_body") or ""))[:400] if reps else ""
    return out, body


def kind(r: dict, body: str) -> str | None:
    """'bounce', 'ooo', 'reply' or None for one statistics row. Pure."""
    cat = str(r.get("lead_category") or "").strip().lower()
    if r.get("is_bounced") or cat == "sender originated bounce":
        return "bounce"
    if cat == "out of office":
        return "ooo"
    if r.get("reply_time"):
        if rr.is_bounce(body):
            return "bounce"
        return None if cat in pw.NOT_A_CONVERSATION else "reply"
    return None


def _day(ts) -> date | None:
    try:
        return date.fromisoformat(str(ts)[:10])
    except ValueError:
        return None


def _blank() -> dict:
    return {k: 0 for k in ("sent", "replies", "ooo", "bounces", "sent7", "replies7", "ooo7", "bounces7", "leads")}


def tally(leads: dict[str, Lead], provider_of, today: date, days: int = DAYS) -> tuple[dict, int]:
    """({(inbox, recipient provider): counts}, sends whose inbox is unknown). ``provider_of(domain)`` -> MX provider.
    Each email counts for the inbox that sent it; a reply on a step counts for that step's inbox. Pure."""
    cells: dict = defaultdict(_blank)
    unknown = 0
    start, week = today - timedelta(days=days), today - timedelta(days=WEEK)
    for email, lead in leads.items():
        by_sid, body = sent_by(lead.history)
        rp = rr.recipient_of(provider_of(email.split("@", 1)[-1]))
        seen = set()
        for r in lead.rows:
            d = _day(r.get("sent_time"))
            if not d or not (start < d <= today):
                continue
            inbox = by_sid.get(str(r.get("stats_id")))
            if not inbox:
                unknown += 1
                continue
            c = cells[(inbox, rp)]
            recent = d > week
            c["sent"] += 1
            c["sent7"] += recent
            if inbox not in seen:
                c["leads"] += 1
                seen.add(inbox)
            k = kind(r, body)
            if k:
                key = {"reply": "replies", "ooo": "ooo", "bounce": "bounces"}[k]
                c[key] += 1
                c[key + "7"] += recent
    return dict(cells), unknown


def base_rates(cells: dict) -> dict[str, float]:
    """{recipient provider: (replies + OOO) / sent} over every inbox given (one client). Pure."""
    s, r = defaultdict(int), defaultdict(int)
    for (_, rp), c in cells.items():
        s[rp] += c.get("sent", 0)
        r[rp] += c.get("replies", 0) + c.get("ooo", 0)
    return {rp: r[rp] / s[rp] for rp in s if s[rp] and r[rp]}


def label(c: dict, rp: str, rates: dict[str, float]) -> str:
    """'reached the inbox' (a reply or OOO seen), 'quiet (0 of ~N expected)' (at the client's own rate at that
    provider, 3+ were expected and none came), else 'not enough data'. Never 'fine'. Pure."""
    if c.get("replies", 0) + c.get("ooo", 0) > 0:
        return "reached the inbox"
    exp = c.get("sent", 0) * rates.get(rp, 0.0)
    if exp >= EXPECT_MIN:
        return f"quiet (0 of ~{exp:.1f} expected)"
    return "not enough data"


def inbox_rows(cells: dict, rates: dict[str, float]) -> dict[str, dict]:
    """{inbox: {"evidence": text, "label": text, "providers": {rp: counts + label}, sent, sent7}}. Pure."""
    by: dict[str, dict] = defaultdict(dict)
    for (inbox, rp), c in cells.items():
        by[inbox][rp] = {**c, "label": label(c, rp, rates)}
    out = {}
    for inbox, provs in sorted(by.items()):
        order = sorted(provs, key=lambda p: (-provs[p]["sent"], p))
        parts = [f"{p} {provs[p]['sent']} sent, {provs[p]['replies']} replies, {provs[p]['ooo']} OOO" for p in order]
        b = sum(provs[p]["bounces"] for p in order)
        s7 = sum(provs[p].get("sent7", 0) for p in order)
        evidence = f"{DAYS}d: " + "; ".join(parts) + (f"; bounces {b}" if b else "") + f" · 7d: {s7} sent"
        quiet = [f"quiet at {p} {provs[p]['label'][len('quiet '):]}" for p in order if provs[p]["label"].startswith("quiet")]
        seen = [p for p in order if provs[p]["label"] == "reached the inbox"]
        lab = "; ".join(quiet + ([f"reached the inbox at {', '.join(seen)}"] if seen else []))
        out[inbox] = {"evidence": evidence, "label": lab or "not enough data", "providers": provs,
                      "sent": sum(provs[p]["sent"] for p in order), "sent7": s7}
    return out


def crosscheck(logged: list[dict], replied: dict[str, str | None], emailed: set[str], today: date,
               days: int = DAYS) -> dict:
    """Check the team's trusted reply log against what this run found. ``logged`` = [{email, date}] from the log;
    ``replied`` = {lead email: sending inbox or None} for leads Smartlead shows a reply or OOO from; ``emailed`` =
    lead emails sent in the window in the campaigns read. Logged replies in the window, one per lead. Pure."""
    start = today - timedelta(days=days)
    want = sorted({str(x.get("email") or "").strip().lower() for x in logged
                   if "@" in str(x.get("email") or "") and (d := _day(x.get("date"))) and start < d <= today})
    misses, matched = {}, 0
    for e in want:
        if e in replied and replied[e]:
            matched += 1
        elif e in replied:
            misses[e] = MISS_NO_SENDER
        elif e in emailed:
            misses[e] = MISS_NO_REPLY
        else:
            misses[e] = MISS_NOT_READ
    return {"logged": len(want), "matched": matched, "pct": round(100 * matched / len(want)) if want else None,
            "misses": misses}


def digest_line(client: str, s: dict, cc: dict | None) -> str:
    """One line per client for the daily digest (a Done line, weekly). Pure."""
    log = (f"team reply log matched {cc['matched']} of {cc['logged']} ({cc['pct']}%)" if cc and cc.get("logged")
           else "team reply log had no replies in the window" if cc else "team reply log not read")
    return (f"Inbox evidence, {client} ({DAYS} days): {s['inboxes']} inboxes, {s['sent']:,} emails, sender known for "
            f"{s['known_pct']}%; {s['replies']} replies, {s['ooo']} out-of-office; {log}; {s['quiet']} quiet, "
            f"{s['not_enough']} not enough data.")
