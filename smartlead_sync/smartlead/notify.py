"""Daily Slack digest of inbox-health action items, grouped by client manager."""
from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta, timezone

import requests

from smartlead.config import HEALTH_NOTIFY_CHANNEL

_PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}


def _group_key(r: dict) -> str:
    """Group by the real client (Melior/Bettrdata/OSC inside the PL agency
    account), falling back to the Smartlead account name."""
    return r.get("sub_client") or r.get("client", "Unknown")


def build_digest(rows: list[dict], sheet_url: str) -> str:
    """Markdown digest: only rows needing action (priority set), grouped by client."""
    actionable = [r for r in rows if r.get("priority")]
    if not actionable:
        return f"✅ *Inbox Health: all inboxes healthy today*\n\n<{sheet_url}|Open the workbook>"

    by_client: dict[str, list[dict]] = {}
    for r in actionable:
        by_client.setdefault(_group_key(r), []).append(r)

    # clients with a P0 first, then by count
    def client_rank(items):
        return (0 if any(i["priority"] == "P0" for i in items) else 1, -len(items))

    n = len(actionable)
    lines = [f"🩺 *Inbox Health: {n} {'inbox needs' if n == 1 else 'inboxes need'} attention*",
             "", f"<{sheet_url}|Open the workbook>", ""]
    for client, items in sorted(by_client.items(), key=lambda kv: client_rank(kv[1])):
        items.sort(key=lambda r: _PRIORITY_ORDER.get(r["priority"], 9))
        mgr_slack = next((i.get("_mgr_slack") for i in items if i.get("_mgr_slack")), "")
        mgr_name = items[0].get("manager") or "Unassigned"
        # Slack member mentions need the <@U…> form; a bare <U…> renders literally
        who = f"<@{mgr_slack}>" if mgr_slack.startswith("U") else (mgr_slack or mgr_name)
        p0 = sum(1 for i in items if i["priority"] == "P0")
        k = len(items)
        lines.append(" · ".join([f"*{client}*"] + ([who] if who else [])
                                + [f"{k} item{'s' if k != 1 else ''}" + (f", {p0} 🔴 P0" if p0 else "")]))
        for i in items[:8]:
            auto = " _(auto-fixing)_" if i.get("owner", "").startswith("🤖") else ""
            lines.append(f"• `{i['priority']}` {i['email']} · {i['top_problem']}{auto}")
        if len(items) > 8:
            lines.append(f"• …and {len(items) - 8} more (full list in the thread)")
        lines.append("")
    return "\n".join(lines).strip()


def build_full_lists(rows: list[dict]) -> dict[str, list[str]]:
    """Complete per-client line lists (no truncation) for thread replies."""
    actionable = [r for r in rows if r.get("priority")]
    out: dict[str, list[str]] = {}
    for r in sorted(actionable, key=lambda r: (_PRIORITY_ORDER.get(r["priority"], 9), r.get("email", ""))):
        out.setdefault(_group_key(r), []).append(
            f"`{r['priority']}` {r['email']} · {r['top_problem']}")
    return out


# Layout (Manveen, 21 Oct 2026: "right spacing ... so it looks natural") ------

_LEAD_EMOJI = re.compile(r"^((?:(?::[\w+-]+:)|[^\x00-\x7F\w\s])+\s+)?(.*\S)\s*$", re.S)


def title(line: str) -> str:
    """'⚠️ Kill warnings, 13 Oct' -> '⚠️ *Kill warnings, 13 Oct*': the title in
    bold after its emoji. A line that already has bold (or a quote / list
    marker) is left as it is."""
    if (line or "").lstrip()[:1] in (">", "•", "`", "<"):
        return line
    m = _LEAD_EMOJI.match(line or "")
    if not m or "*" in m.group(2) or m.group(2)[:1] in (">", "•", "`"):
        return line
    return f"{m.group(1) or ''}*{m.group(2)}*"


def tidy(text: str, *, headline: bool = True) -> str:
    """What every post looks like: no trailing spaces, never two blank lines in
    a row, no blank lines at the ends; for a channel post (headline=True) the
    first line is a bold title with one blank line under it."""
    lines = [l.rstrip() for l in str(text or "").replace("\r\n", "\n").split("\n")]
    out: list[str] = []
    for l in lines:
        if not l and (not out or not out[-1]):
            continue
        out.append(l)
    while out and not out[-1]:
        out.pop()
    if headline and out:
        out[0] = title(out[0])
        if len(out) > 1 and out[1]:
            out.insert(1, "")
    return "\n".join(out)


IST = timezone(timedelta(hours=5, minutes=30))


def sync_failure_text(failed_tabs: list[dict], total: int, when: datetime | None = None) -> str:
    """The daily sheet sync's alert when a tab was not written (run.py)."""
    when = (when or datetime.now(IST)).astimezone(IST)
    n = len(failed_tabs)
    lines = [f"⚠️ *Sheet sync: {n} of {total} tab{'s' if total != 1 else ''} not written*", ""]
    lines += [f"• `{e['tab']}`: {str(e.get('error') or '')[:160]}" for e in failed_tabs]
    lines += ["", f"Details on the Last Sync tab · {when.day} {when:%b, %H:%M} IST"]
    return "\n".join(lines)


def day(iso) -> str:
    """'2026-11-08' (or a datetime string) -> '8 Nov'; anything else unchanged."""
    from datetime import date
    s = str(iso or "")
    try:
        d = date.fromisoformat(s[:10])
    except ValueError:
        return s
    return f"{d.day} {d:%b}"


_SECRETS = [
    (re.compile(r"((?:api_?key|token|key|secret|password)=)[^&\s'\"]+", re.I), r"\1<hidden>"),
    (re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+"), "Bearer <hidden>"),
    (re.compile(r"eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?"), "<hidden>"),
    (re.compile(r"xox[abposr]-[A-Za-z0-9-]+"), "<hidden>"),
    (re.compile(r"<(?:!doctype|html)\b.*?(?:</html>|$)", re.I | re.S), "(error page)"),
]


def redact(text: str) -> str:
    """Keys, tokens and raw provider error pages out of anything sent to Slack."""
    for pattern, repl in _SECRETS:
        text = pattern.sub(repl, text)
    return text


def _post(token: str, channel: str, text: str, thread_ts: str | None = None) -> str | None:
    """Post one message; returns its ts on success, None on failure. A channel
    post gets the title layout (tidy); a thread reply only the spacing."""
    body = {"channel": channel, "text": redact(tidy(text, headline=not thread_ts)), "unfurl_links": False}
    if thread_ts:
        body["thread_ts"] = thread_ts
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json=body, timeout=15,
        )
        data = resp.json()
        if not data.get("ok"):
            print(f"  [Notify] Slack error: {data.get('error')}")
            return None
        return data.get("ts")
    except requests.RequestException as exc:
        print(f"  [Notify] Slack post failed: {exc}")
        return None


_THREAD_CHUNK = 40  # lines per thread reply — stays well inside Slack's msg limit


_REPLY_PAUSE_S = 1.1  # Slack allows about one post per second per channel


def post_with_thread(token: str, channel: str, headline_lines: list[str],
                     thread_parts: list[str]) -> str | None:
    """Headline in the channel, details in its thread (spec 2026-10-13).

    The parent is the headline (a few short lines); each thread part is a
    reply, split at _THREAD_CHUNK lines. Slack folds the replies under
    "N replies", which works as the "show details" button. Returns the parent
    ts; nothing else is posted if the parent fails."""
    ts = _post(token, channel, "\n".join(l for l in headline_lines if l))
    if not ts:
        return None
    for part in thread_parts:
        lines = str(part or "").strip("\n").split("\n")
        if not "".join(lines).strip():
            continue
        for i in range(0, len(lines), _THREAD_CHUNK):
            chunk = "\n".join(lines[i:i + _THREAD_CHUNK])
            time.sleep(_REPLY_PAUSE_S)
            if not _post(token, channel, chunk, thread_ts=ts):
                time.sleep(max(_REPLY_PAUSE_S * 2, 0))        # rate limit: wait, try once more
                if not _post(token, channel, chunk, thread_ts=ts):
                    print("  [Notify] a thread reply failed twice; the headline stays.")
    return ts


def post_digest(text: str, full_lists: dict[str, list[str]] | None = None) -> bool:
    """Post the digest; if full_lists given, post the COMPLETE per-client lists
    as thread replies so '…and N more' is always expandable in-channel."""
    token = os.getenv("SLACK_BOT_TOKEN", "")
    channel = HEALTH_NOTIFY_CHANNEL
    if not token or not channel:
        print("  [Notify] SLACK_BOT_TOKEN/HEALTH_NOTIFY_CHANNEL missing - skipping post.")
        return False
    ts = _post(token, channel, text)
    if not ts:
        return False
    for client, lines in (full_lists or {}).items():
        if len(lines) <= 8:
            continue  # main message already shows everything for this client
        for i in range(0, len(lines), _THREAD_CHUNK):
            chunk = lines[i:i + _THREAD_CHUNK]
            head = f"*{client}: full list ({len(lines)} items)*\n" if i == 0 else ""
            _post(token, channel, head + "\n".join(chunk), thread_ts=ts)
    return True
