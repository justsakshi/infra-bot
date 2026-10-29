"""Text of the daily campaign stats post (campaign_stats_post.py).

Asked for at the 29 Oct 2026 standup: the email and LinkedIn numbers in the
group before the call, so nobody opens Smartlead or Expandi on the call to
read them out. The headline gives each client's totals since the last post;
the thread lists every campaign with its change and its running total.
"""
from __future__ import annotations

from datetime import date

from smartlead.notify import day as _day

# Order the clients appear in; anything else follows alphabetically.
_CLIENT_ORDER = ["BettrData", "Melior", "Precise Leads"]
# Fields summed per channel. Email replies are broken down by Smartlead's
# category (Manveen, 29 Oct 2026: "positive/neutral and not interested and
# wrong person and how many emails went out"); LinkedIn platforms tag only
# "interested".
_FIELDS = {
    "Email": ["sent", "replies", "positive", "not_interested", "wrong_person", "bounced"],
    "LinkedIn": ["invites", "accepted", "replies", "positive"],
}
_REPLY_KINDS = [("positive", "positive/neutral"), ("not_interested", "not interested"),
                ("wrong_person", "wrong person")]
_LINKEDIN = [("invites", "invite", "invites"), ("accepted", "accepted", "accepted"),
             ("replies", "reply", "replies"), ("positive", "interested", "interested")]


def weekday_day(iso: str) -> str:
    """'2026-10-30' -> 'Fri 30 Oct'."""
    d = date.fromisoformat(iso)
    return f"{d:%a} {_day(iso)}"


def _n(n: int, one: str, many: str) -> str:
    return f"{n:,} {one if n == 1 else many}"


def _counts(values: dict, channel: str, *, skip_zero: bool = False) -> str:
    """'165 sent, 3 replies (2 positive/neutral, 1 not interested, 0 wrong
    person), 0 bounced' for email; '12 invites, 0 accepted, ...' for LinkedIn.
    skip_zero (the running totals) leaves out the zeros."""
    def shown(field):
        n = values.get(field)
        return n is not None and not (skip_zero and n == 0)

    parts = []
    if channel == "LinkedIn":
        parts = [_n(values[f], one, many) for f, one, many in _LINKEDIN if shown(f)]
        return ", ".join(parts) or "nothing yet"
    if shown("sent"):
        parts.append(f"{values['sent']:,} sent")
    if shown("replies"):
        replies = values["replies"]
        kinds = [(values.get(f), label) for f, label in _REPLY_KINDS]
        detail = [f"{n:,} {label}" for n, label in kinds
                  if n is not None and not (skip_zero and n == 0)]
        # Do Not Contact, Uncategorizable and custom categories.
        other = replies - sum(n for n, _ in kinds if n)
        if other > 0:
            detail.append(f"{other:,} other")
        parts.append(_n(replies, "reply", "replies")
                     + (f" ({', '.join(detail)})" if replies and detail else ""))
    if shown("bounced"):
        parts.append(f"{values['bounced']:,} bounced")
    return ", ".join(parts) or "nothing yet"


def _client_rank(client: str):
    return (_CLIENT_ORDER.index(client) if client in _CLIENT_ORDER else len(_CLIENT_ORDER),
            client)


def _sum(rows: list[dict], key: str, channel: str) -> dict:
    total: dict = {}
    for r in rows:
        vals = r[key] or {}
        for field in _FIELDS[channel]:
            v = vals.get(field)
            if v is not None:
                total[field] = total.get(field, 0) + v
    return total


def build_post(changes: list[dict], today: str, base: str | None,
               sheet_url: str, not_counted: list[str]) -> tuple[str, list[str]]:
    """(headline, thread parts) for notify.post_with_thread([headline], parts).

    The headline is one string: post_with_thread drops empty items from its
    headline list, which would take out the blank line between clients.
    """
    first = base is None
    head = [f"📊 Campaign stats, {weekday_day(today)}", ""]
    if first:
        head.append("Totals to date. Campaign History starts today, so from the "
                    "next post these are changes since the post before.")
    else:
        head.append(f"Since {weekday_day(base)}, 9:00 IST. Each campaign is in the thread.")
    thread: list[str] = []

    by_client: dict[str, list[dict]] = {}
    for c in changes:
        by_client.setdefault(c["client"], []).append(c)

    for client in sorted(by_client, key=_client_rank):
        rows = by_client[client]
        head += ["", f"*{client}*"]
        detail: list[str] = []
        for channel in ("Email", "LinkedIn"):
            ch_rows = [r for r in rows if r["channel"] == channel]
            if not ch_rows:
                continue
            key = "totals" if first else "change"
            known = [r for r in ch_rows if r[key] is not None]
            head.append(f"{channel}: {_counts(_sum(known, key, channel), channel)}" if known
                        else f"{channel}: no earlier numbers to compare (see the thread)")
            lines = []
            for r in sorted(ch_rows, key=lambda r: (not r["running"], r["campaign"].lower())):
                moved = r["change"] and any(v for v in r["change"].values())
                if not first and not r["running"] and not moved:
                    continue
                to_date = _counts(r["totals"], channel, skip_zero=True)
                if first:
                    lines.append(f"• {r['campaign']}: {to_date}")
                elif r["change"] is None:
                    lines.append(f"• {r['campaign']}: to date {to_date} "
                                 f"(not in the {_day(base)} numbers, so no change shown)")
                else:
                    lines.append(f"• {r['campaign']}: {_counts(r['change'], channel)}"
                                 f" · to date {to_date}")
            if lines:
                detail += [f"*{client} · {channel}*"] + lines + [""]
        if detail:
            thread.append("\n".join(detail).strip())

    if not by_client:
        head += ["", "No active campaigns in today's numbers."]
    if not_counted:
        head += ["", "Not counted: " + "; ".join(not_counted) + "."]
    head += ["", f"<{sheet_url}|Campaign History sheet>"]
    return "\n".join(head), thread


def not_ready_text(today: str, latest: str | None) -> str:
    """The post when the 9:00 run left no numbers for today."""
    seen = f"The latest numbers are from {weekday_day(latest)}." if latest else \
        "Campaign History is empty."
    return (f"⚠️ Campaign stats, {weekday_day(today)}: not ready\n\n"
            f"The 9:00 stats run has not written today's numbers to Campaign History. "
            f"{seen} Its log is on Render (search for [campaign-metrics]).")
