"""Text of the 10:15 stats post (campaign_stats_post.py).

Asked for at the 29 Oct 2026 standup: the email and LinkedIn numbers in the
group before the call, so nobody opens Smartlead or Expandi on the call to
read them out. Since 2 Nov (Manveen) it reports the last working day only,
plus the week on Fridays and last month on the month's first working day;
the numbers come from smartlead.daily_stats.

The headline has each client's numbers; the thread lists the day's campaigns,
each with a button that shows the replies behind its number.
"""
from __future__ import annotations

import json
from datetime import date

from smartlead.daily_stats import Period, month_name
from smartlead.reply_view import short_names

# Order the clients appear in; anything else follows alphabetically.
_CLIENT_ORDER = ["BettrData", "Melior", "Precise Leads"]
_REPLY_KINDS = [("positive", "positive/neutral"), ("not_interested", "not interested"),
                ("wrong_person", "wrong person")]
_MAX_SECTIONS = 45   # Slack allows 50 blocks a message


def weekday_day(d: date | str) -> str:
    """date(2026, 10, 30) -> 'Fri 30 Oct'."""
    d = date.fromisoformat(d) if isinstance(d, str) else d
    return f"{d:%a} {d.day} {d:%b}"


def _n(n: int, one: str, many: str) -> str:
    return f"{n:,} {one if n == 1 else many}"


def _people_replied(n: int) -> str:
    return f"{n:,} {'person' if n == 1 else 'people'} replied"


def email_line(c: dict) -> str:
    """Avinash's terms (standup 30 Oct 2026): new people reached, emails
    sent, replies by kind, out-of-office, bounces."""
    if not any(c.get(f) for f in c):
        return "nothing sent, no replies"
    parts = []
    if c.get("reached"):
        parts.append(_n(c["reached"], "new person reached", "new people reached"))
    if c.get("sent") or not c.get("replies"):
        parts.append(_n(c.get("sent") or 0, "email sent", "emails sent"))
    replies = c.get("replies") or 0
    kinds = [f"{c[f]:,} {label}" for f, label in _REPLY_KINDS if c.get(f)]
    other = replies - sum(c.get(f) or 0 for f, _ in _REPLY_KINDS)
    if other > 0:
        kinds.append(f"{other:,} other")
    parts.append(_people_replied(replies) + (f" ({', '.join(kinds)})" if replies and kinds else ""))
    if c.get("out_of_office"):
        parts.append(f"{c['out_of_office']:,} out-of-office")
    if c.get("bounced"):
        parts.append(f"{c['bounced']:,} bounced")
    return " · ".join(parts)


def linkedin_line(c: dict) -> str:
    if not any(c.get(f) for f in c):
        return "no activity"
    parts = []
    if c.get("invites") is not None:
        parts.append(_n(c["invites"], "invite", "invites"))
    if c.get("accepted") is not None:
        parts.append(f"{c['accepted']:,} accepted")
    replies = c.get("replies") or 0
    pos = f" ({c['positive']:,} interested)" if replies and c.get("positive") else ""
    parts.append(_people_replied(replies) + pos)
    return " · ".join(parts)


def _line(channel: str, c: dict) -> str:
    return email_line(c) if channel == "Email" else linkedin_line(c)


def _sum(rows: list[dict], key: str) -> dict:
    """Field totals over campaigns; a field no campaign has (Waalaxy invites)
    stays None so it is left out rather than shown as 0."""
    total: dict = {}
    for r in rows:
        for f, v in r["counts"][key].items():
            if v is None:
                total.setdefault(f, None)
            else:
                total[f] = (total.get(f) or 0) + v
    return total


def _client_rank(client: str):
    return (_CLIENT_ORDER.index(client) if client in _CLIENT_ORDER else len(_CLIENT_ORDER),
            client)


def _period_title(p: Period) -> str:
    if p.key == "week":
        return f"This week, {weekday_day(p.start)} – {weekday_day(p.end)}"
    if p.key == "month":
        return month_name(p)
    return weekday_day(p.start)


def _button(r: dict, title: str, p: Period, replies: int) -> dict:
    lo, hi = p.window
    value = {"client": r["client"], "channel": r["channel"], "platform": r.get("platform", ""),
             "id": str(r.get("campaign_id") or ""), "name": r["campaign"], "title": title,
             "from": lo.isoformat(), "to": hi.isoformat(), "n": replies}
    return {"type": "button", "action_id": "show_replies",
            "text": {"type": "plain_text", "text": f"Show {_n(replies, 'reply', 'replies')}"},
            "value": json.dumps(value, ensure_ascii=False)}   # Slack's cap is 2,000


def _thread_part(client: str, channel: str, rows: list[dict], day: Period) -> list[dict]:
    """The day's campaigns for one client·channel, a button where people replied."""
    moved = [r for r in rows if any(r["counts"][day.key].values())]
    if not moved:
        return []
    names = short_names([r["campaign"] for r in moved])
    sections = []
    for r in sorted(moved, key=lambda r: names[r["campaign"]].lower()):
        c = r["counts"][day.key]
        title = names[r["campaign"]]
        block = {"type": "section",
                 "text": {"type": "mrkdwn", "text": f"*{title}*\n{_line(channel, c)}"}}
        if c.get("replies") and r.get("campaign_id"):
            block["accessory"] = _button(r, title, day, c["replies"])
        sections.append(block)
    head = {"type": "header", "text": {"type": "plain_text", "text": f"{client} · {channel}"}}
    parts = []
    for i in range(0, len(sections), _MAX_SECTIONS):
        blocks = ([head] if i == 0 else []) + sections[i:i + _MAX_SECTIONS]
        parts.append({"text": f"{client} · {channel}", "blocks": blocks})
    return parts


def build_post(rows: list[dict], periods: list[Period], clients: list[str],
               not_counted: list[str] = (), failed: list[str] = (),
               linkedin_clients: tuple = (),
               linkedin_from: dict | None = None) -> tuple[str, list[dict]]:
    """(headline, thread parts) for notify.post_with_thread([headline], parts).

    `clients` are the active clients, each listed even on a quiet day so a
    missing line never reads as "nothing happened". `linkedin_clients` get a
    LinkedIn line whether or not a campaign moved. `linkedin_from` (client ->
    first day tracked) marks a period that starts before it as incomplete
    (PL's Waalaxy replies are logged from 30 Oct 2026).
    """
    day = next(p for p in periods if p.key == "day")
    head = [f"📊 Campaign stats for {weekday_day(day.start)}", "",
            "A day runs 05:30 to 05:30 IST, as in Smartlead. New people reached = first "
            "emails; emails sent includes follow-ups. Each campaign is in the thread, "
            "with a button to read its replies."]
    thread: list[dict] = []
    order = sorted(set(clients) | {r["client"] for r in rows}, key=_client_rank)

    def lines(p: Period, prefix: bool) -> list[str]:
        out = []
        for client in order:
            mine = [r for r in rows if r["client"] == client]
            chans = ["Email"] + (["LinkedIn"] if client in linkedin_clients
                                 or any(r["channel"] == "LinkedIn" for r in mine) else [])
            if not prefix:
                out += ["", f"*{client}*"]
            for ch in chans:
                ch_rows = [r for r in mine if r["channel"] == ch]
                text = _line(ch, _sum(ch_rows, p.key)) if ch_rows else _line(ch, {})
                if ch == "LinkedIn" and any(r.get("partial") for r in ch_rows):
                    text += " · invites and accepts may be low (Expandi's lead history is still loading)"
                since = (linkedin_from or {}).get(client) if ch == "LinkedIn" else None
                if since and p.end < since:
                    text = f"not tracked before {weekday_day(since)}"
                elif since and p.start <= since:      # logging began during that day
                    text += f" (tracked from {weekday_day(since)})"
                out.append(f"{client} {ch.lower() if ch == 'Email' else ch}: {text}"
                           if prefix else f"{ch}: {text}")
            if not prefix:
                for ch in chans:
                    thread.extend(_thread_part(client, ch, [r for r in mine if r["channel"] == ch], day))
        return out

    head += lines(day, prefix=False)
    for p in periods:
        if p.key != "day":
            head += ["", f"*{_period_title(p)}*"] + lines(p, prefix=True)
    if not_counted:
        head += ["", "Not counted: " + "; ".join(not_counted) + "."]
    if failed:
        head += ["", "⚠️ Not fully read, so these numbers may be off: " + "; ".join(failed) + "."]
    return "\n".join(head), thread


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
