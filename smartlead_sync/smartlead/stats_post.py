"""Text of the daily campaign stats post (campaign_stats_post.py).

Asked for at the 29 Oct 2026 standup: the email and LinkedIn numbers in the
group before the call, so nobody opens Smartlead or Expandi on the call to
read them out. The headline gives each client's totals since the last post;
the thread lists every campaign with its change and its running total.
"""
from __future__ import annotations

import json
from datetime import date

from smartlead.notify import day as _day
from smartlead.reply_view import post_window, short_names

# Order the clients appear in; anything else follows alphabetically.
_CLIENT_ORDER = ["BettrData", "Melior", "Precise Leads"]
# Fields summed per channel. Email replies are broken down by Smartlead's
# category (Manveen, 29 Oct 2026: "positive/neutral and not interested and
# wrong person and how many emails went out"); LinkedIn platforms tag only
# "interested".
_FIELDS = {
    "Email": ["sent", "reached", "replies", "positive", "not_interested", "wrong_person",
              "out_of_office", "bounced"],
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


def _counts(values: dict, channel: str, *, skip_zero: bool = False, sep: str = ", ") -> str:
    """'165 sent, 3 replies (2 positive/neutral, 1 not interested, 0 wrong
    person), 0 bounced' for email; '12 invites, 0 accepted, ...' for LinkedIn.
    skip_zero (the running totals) leaves out the zeros."""
    def shown(field):
        n = values.get(field)
        return n is not None and not (skip_zero and n == 0)

    parts = []
    if channel == "LinkedIn":
        parts = [_n(values[f], one, many) for f, one, many in _LINKEDIN if shown(f)]
        return sep.join(parts) or "nothing yet"
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
    return sep.join(parts) or "nothing yet"


def _replies(v: dict) -> str:
    """'4 replies (3 positive/neutral, 1 other)'; zeros left out of the brackets."""
    replies = v.get("replies") or 0
    kinds = [(v.get(f), label) for f, label in _REPLY_KINDS]
    detail = [f"{n:,} {label}" for n, label in kinds if n]
    other = replies - sum(n for n, _ in kinds if n)     # Do Not Contact, custom...
    if other > 0:
        detail.append(f"{other:,} other")
    return _n(replies, "reply", "replies") + (f" ({', '.join(detail)})" if replies and detail else "")


def email_new(v: dict) -> str:
    """Since the last post, in Avinash's terms (standup 30 Oct 2026): new
    people reached, emails sent, replies, out-of-office, bounces if any."""
    parts = []
    if v.get("reached") is not None:
        parts.append(_n(v["reached"], "new person reached", "new people reached"))
    if v.get("sent"):
        parts.append(_n(v["sent"], "email sent", "emails sent"))
    if v.get("replies") is not None:
        parts.append(_replies(v))
    if v.get("out_of_office"):
        parts.append(f"{v['out_of_office']:,} out-of-office")
    if v.get("bounced"):
        parts.append(f"{v['bounced']:,} bounced")
    return " · ".join(parts) or "no activity"


def email_total(t: dict, open_tracking: str) -> str:
    """To date: people reached, open rate (tracked campaigns only), reply rate."""
    reached, replies = t.get("reached"), t.get("replies")
    parts = []
    if reached:
        parts.append(_n(reached, "person reached", "people reached"))
    elif t.get("sent"):
        parts.append(_n(t["sent"], "email sent", "emails sent"))
    if open_tracking == "on" and reached and t.get("opened") is not None:
        parts.append(f"{_pct(t['opened'], reached)} opened")
    if reached and replies is not None:
        pos = f", {t['positive']:,} positive/neutral" if t.get("positive") else ""
        parts.append(f"{_pct(replies, reached)} replied ({replies:,} of {reached:,}{pos})")
    elif replies:
        parts.append(_replies(t))
    return " · ".join(parts) or "nothing yet"


def _pct(part, whole) -> str | None:
    if not whole or part is None:
        return None
    return f"{round(100 * part / whole)}%"


def _rate(totals: dict, channel: str, open_tracking: str) -> str:
    """' · 80% opened' (email with open tracking) or ' · 4% accepted' (LinkedIn)."""
    if channel == "Email":
        p = _pct(totals.get("opened"), totals.get("reached")) if open_tracking == "on" else None
        return f" · {p} opened" if p else ""
    p = _pct(totals.get("accepted"), totals.get("invites"))
    return f" · {p} accepted" if p else ""


def _open_rate(rows: list[dict]) -> str:
    """The client's open rate over the campaigns that track opens."""
    tracked = [r for r in rows if r.get("open_tracking") == "on" and r["totals"].get("reached")]
    reached = sum(r["totals"]["reached"] for r in tracked)
    opened = sum(r["totals"].get("opened") or 0 for r in tracked)
    p = _pct(opened, reached)
    if not p:
        return ""
    return (f" · {p} opened to date ({len(tracked)} of {len(rows)} campaigns track opens)"
            if len(tracked) < len(rows) else f" · {p} opened to date")


_MAX_SECTIONS = 45   # Slack allows 50 blocks a message


def _button(r: dict, title: str, window: tuple, replies: int) -> dict:
    value = {"client": r["client"], "channel": r["channel"], "platform": r.get("platform", ""),
             "id": str(r.get("campaign_id") or ""), "name": r["campaign"], "title": title,
             "from": window[0], "to": window[1], "n": replies}
    return {"type": "button", "action_id": "show_replies",
            "text": {"type": "plain_text", "text": f"Show {_n(replies, 'reply', 'replies')}"},
            "value": json.dumps(value, ensure_ascii=False)}   # Slack's cap is 2,000


def _thread_part(client: str, channel: str, rows: list[dict], first: bool,
                 base: str | None, window: tuple) -> list[dict]:
    """One client·channel as Slack messages: a short block per campaign that
    moved, a Show replies button where it got replies, the rest on one line."""
    names = short_names([r["campaign"] for r in rows])
    sections, quiet = [], []
    for r in sorted(rows, key=lambda r: (not r["running"], r["campaign"].lower())):
        title = names[r["campaign"]]
        moved = bool(r["change"]) and any(r["change"].get(f) for f in _FIELDS[channel])
        if channel == "Email":
            to_date, rate = email_total(r["totals"], r.get("open_tracking", "")), ""
        else:
            to_date = _counts(r["totals"], channel, skip_zero=True, sep=" · ")
            rate = _rate(r["totals"], channel, "")
        if first:
            text = f"*{title}*\nTotal: {to_date}{rate}"
        elif r["change"] is None and not r["running"]:
            # Paused / finished and missing from the last snapshot (29 Oct:
            # Melior's 26 HeyReach campaigns on their first day): nothing to
            # report. The sheet still has the row.
            continue
        elif r["change"] is None:
            text = (f"*{title}*\nTotal: {to_date}{rate}\n_Not in the {_day(base)} "
                    f"numbers, so no change shown_")
        elif not moved:
            if r["running"]:
                quiet.append(title)
            continue
        else:
            new = (email_new(r["change"]) if channel == "Email"
                   else _counts(r["change"], channel, skip_zero=True, sep=" · "))
            text = f"*{title}*\nNew: {new}\nTotal: {to_date}{rate}"
        block = {"type": "section", "text": {"type": "mrkdwn", "text": text}}
        replies = (r["change"] or {}).get("replies") or 0
        if replies and r.get("campaign_id") and not first:
            block["accessory"] = _button(r, title, window, replies)
        sections.append(block)
    if not sections and not quiet:
        return []
    head = {"type": "header", "text": {"type": "plain_text", "text": f"{client} · {channel}"}}
    tail = ([{"type": "context", "elements": [{"type": "mrkdwn",
                                                "text": _quiet_line(quiet)}]}]
            if quiet else [])
    parts = []
    for i in range(0, max(len(sections), 1), _MAX_SECTIONS):
        chunk = sections[i:i + _MAX_SECTIONS]
        last = i + _MAX_SECTIONS >= len(sections)
        blocks = ([head] if i == 0 else []) + chunk + (tail if last else [])
        parts.append({"text": f"{client} · {channel}", "blocks": blocks})
    return parts


def _quiet_line(names: list[str], cap: int = 40) -> str:
    more = f" and {len(names) - cap} more" if len(names) > cap else ""
    return "No change: " + ", ".join(names[:cap]) + more


def part_as_text(part) -> str:
    """A thread part as plain text, for --dry-run and the logs."""
    if isinstance(part, str):
        return part
    lines = []
    for b in part.get("blocks", []):
        if b["type"] in ("header", "section"):
            lines.append(b["text"]["text"])
            if b.get("accessory"):
                lines.append(f"   [{b['accessory']['text']['text']}]")
        elif b["type"] == "context":
            lines.append(b["elements"][0]["text"])
        lines.append("")
    return "\n".join(lines).strip()


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
               sheet_url: str, not_counted: list[str]) -> tuple[str, list[dict]]:
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
        head.append(f"Since {weekday_day(base)}, 9:00 IST. Each campaign is in the thread, "
                    "with a button to read its new replies.")
    thread: list[dict] = []

    by_client: dict[str, list[dict]] = {}
    for c in changes:
        by_client.setdefault(c["client"], []).append(c)

    window = post_window(base, today)
    for client in sorted(by_client, key=_client_rank):
        rows = by_client[client]
        head += ["", f"*{client}*"]
        for channel in ("Email", "LinkedIn"):
            ch_rows = [r for r in rows if r["channel"] == channel]
            if not ch_rows:
                continue
            key = "totals" if first else "change"
            known = [r for r in ch_rows if r[key] is not None]
            rate = _open_rate(ch_rows) if channel == "Email" else ""
            if not known:
                head.append(f"{channel}: no earlier numbers to compare (see the thread)")
            elif channel == "Email":
                summed = _sum(known, key, channel)
                head.append(f"Email: {email_total(summed, '') if first else email_new(summed)}{rate}")
            else:
                head.append(f"LinkedIn: {_counts(_sum(known, key, channel), channel)}")
            thread += _thread_part(client, channel, ch_rows, first, base, window)

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
