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


def email_line(c: dict, quiet_zero: bool = False) -> str:
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
    if replies or not quiet_zero:
        parts.append(_people_replied(replies) + (f" ({', '.join(kinds)})" if replies and kinds else ""))
    if c.get("out_of_office"):
        parts.append(f"{c['out_of_office']:,} out-of-office")
    if c.get("bounced"):
        parts.append(f"{c['bounced']:,} bounced")
    return " · ".join(parts)


def linkedin_line(c: dict, quiet_zero: bool = False) -> str:
    if not any(c.get(f) for f in c):
        return "no activity"
    parts = []
    if c.get("invites") is not None:
        parts.append(_n(c["invites"], "invite", "invites"))
    if c.get("accepted") is not None:
        parts.append(f"{c['accepted']:,} accepted")
    replies = c.get("replies") or 0
    pos = f" ({c['positive']:,} interested)" if replies and c.get("positive") else ""
    if replies or not quiet_zero:
        parts.append(_people_replied(replies) + pos)
    return " · ".join(parts)


def _line(channel: str, c: dict, quiet_zero: bool = False) -> str:
    """quiet_zero leaves out "0 people replied" (the Campaign summary, where
    most campaigns had none)."""
    return email_line(c, quiet_zero) if channel == "Email" else linkedin_line(c, quiet_zero)


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


def button_label(c: dict, channel: str = "Email") -> str:
    """What the replies were, e.g. '1 interested, 1 not interested · 1 on 2nd+
    touch' (Manveen, 3 Nov 2026). LinkedIn tags only "interested", so the rest
    are just replies. Slack caps a button at 75 characters."""
    replies = c.get("replies") or 0
    if channel == "LinkedIn":
        pos = c.get("positive") or 0
        text = (f"{pos} interested" + (f", {replies - pos} more" if replies > pos else "")
                if pos else _n(replies, "reply", "replies"))
    else:
        kinds = [(c.get("positive") or 0, "interested"), (c.get("not_interested") or 0, "not interested"),
                 (c.get("wrong_person") or 0, "wrong person")]
        other = replies - sum(n for n, _ in kinds)
        parts = [f"{n} {label}" for n, label in kinds + [(other, "other")] if n > 0]
        text = ", ".join(parts) or _n(replies, "reply", "replies")
    if c.get("later_touch"):
        text += f" · {c['later_touch']} on 2nd+ touch"
    return text if len(text) <= 75 else text[:74] + "…"


def _button(r: dict, title: str, p: Period, c: dict) -> dict:
    lo, hi = p.window
    value = {"client": r["client"], "channel": r["channel"], "platform": r.get("platform", ""),
             "id": str(r.get("campaign_id") or ""), "name": r["campaign"], "title": title,
             "from": lo.isoformat(), "to": hi.isoformat(), "n": c["replies"]}
    return {"type": "button", "action_id": "show_replies",
            "text": {"type": "plain_text", "text": button_label(c, r["channel"])},
            "value": json.dumps(value, ensure_ascii=False)}   # Slack's cap is 2,000


def _thread_part(client: str, channel: str, rows: list[dict], day: Period) -> list[dict]:
    """The day's campaigns that got replies, for one client·channel, each with
    its button. Campaigns that only sent are left out (Manveen, 3 Nov 2026:
    "just put campaign with replies"); the headline still counts them."""
    moved = [r for r in rows if r["counts"][day.key].get("replies")]
    if not moved:
        return []
    names = short_names([r["campaign"] for r in moved])
    sections = []
    for r in sorted(moved, key=lambda r: names[r["campaign"]].lower()):
        c = r["counts"][day.key]
        title = names[r["campaign"]]
        block = {"type": "section", "text": {"type": "mrkdwn", "text": f"*{title}*"}}
        if c.get("replies") and r.get("campaign_id"):
            block["accessory"] = _button(r, title, day, c)
        sections.append(block)
    head = {"type": "header", "text": {"type": "plain_text", "text": f"{client} · {channel}"}}
    parts = []
    for i in range(0, len(sections), _MAX_SECTIONS):
        blocks = ([head] if i == 0 else []) + sections[i:i + _MAX_SECTIONS]
        parts.append({"text": f"{client} · {channel}", "blocks": blocks})
    return parts


SUMMARY_ACTION = "show_campaign_summary"


def summary_messages(rows: list[dict], day: Period) -> list[dict]:
    """Every campaign that moved on the day, per client and channel: the
    Campaign summary button's answer (Manveen, 3 Nov 2026: "how many emails
    were sent from each campaign and also replies and everything"). Saved with
    the post, shown only to whoever clicks."""
    sections: list[dict] = []
    for client in sorted({r["client"] for r in rows}, key=_client_rank):
        for channel in ("Email", "LinkedIn"):
            mine = [r for r in rows if r["client"] == client and r["channel"] == channel
                    and any(r["counts"][day.key].values())]
            if not mine:
                continue
            names = short_names([r["campaign"] for r in mine])
            sections.append({"type": "header", "text": {"type": "plain_text",
                                                        "text": f"{client} · {channel}"}})
            for r in sorted(mine, key=lambda r: names[r["campaign"]].lower()):
                sections.append({"type": "section", "text": {"type": "mrkdwn", "text":
                                 f"*{names[r['campaign']]}*\n{_line(channel, r['counts'][day.key], True)}"}})
    if not sections:
        sections = [{"type": "section", "text": {"type": "mrkdwn",
                                                 "text": "No campaign moved this day."}}]
    title = {"type": "section", "text": {"type": "mrkdwn",
                                         "text": f"*Campaign summary, {weekday_day(day.start)}*"}}
    out = []
    for i in range(0, len(sections), _MAX_SECTIONS):
        out.append({"text": f"Campaign summary, {weekday_day(day.start)}",
                    "blocks": ([title] if i == 0 else []) + sections[i:i + _MAX_SECTIONS]})
    return out


def summary_part(day: Period) -> dict:
    """The thread's last message: the Campaign summary button."""
    return {"text": "Campaign summary",
            "blocks": [{"type": "section",
                        "text": {"type": "mrkdwn", "text": "Every campaign's numbers for the day:"},
                        "accessory": {"type": "button", "action_id": SUMMARY_ACTION,
                                      "text": {"type": "plain_text", "text": "Campaign summary"},
                                      "value": json.dumps({"day": day.start.isoformat()})}}]}


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
            "Day = 05:30 to 05:30 IST (Smartlead's day). New people reached = first emails; "
            "emails sent includes follow-ups.",
            "In the thread: the campaigns that got replies (click to read them), then "
            "*Campaign summary* for every campaign's numbers."]
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
    thread.append(summary_part(day))
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
