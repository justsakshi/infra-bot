"""Daily Slack digest of inbox-health action items, grouped by client manager."""
from __future__ import annotations

import os

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
        return f"✅ Inbox Health: all inboxes healthy today. Workbook: {sheet_url}"

    by_client: dict[str, list[dict]] = {}
    for r in actionable:
        by_client.setdefault(_group_key(r), []).append(r)

    # clients with a P0 first, then by count
    def client_rank(items):
        return (0 if any(i["priority"] == "P0" for i in items) else 1, -len(items))

    lines = [f"*🩺 Inbox Health — {len(actionable)} inbox(es) need attention*",
             f"<{sheet_url}|Open the workbook>", ""]
    for client, items in sorted(by_client.items(), key=lambda kv: client_rank(kv[1])):
        items.sort(key=lambda r: _PRIORITY_ORDER.get(r["priority"], 9))
        mgr_slack = next((i.get("_mgr_slack") for i in items if i.get("_mgr_slack")), "")
        mgr_name = items[0].get("manager", "Unassigned")
        # Slack member mentions need the <@U…> form; a bare <U…> renders literally
        who = f"<@{mgr_slack}>" if mgr_slack.startswith("U") else (mgr_slack or mgr_name)
        p0 = sum(1 for i in items if i["priority"] == "P0")
        lines.append(f"*{client}* — {who} · {len(items)} item(s){f', {p0} 🔴 P0' if p0 else ''}")
        for i in items[:8]:
            auto = " _(auto-fixing)_" if i.get("owner", "").startswith("🤖") else ""
            lines.append(f"   • `{i['priority']}` {i['email']} — {i['top_problem']}{auto}")
        if len(items) > 8:
            lines.append(f"   • …and {len(items) - 8} more")
        lines.append("")
    return "\n".join(lines).strip()


def build_full_lists(rows: list[dict]) -> dict[str, list[str]]:
    """Complete per-client line lists (no truncation) for thread replies."""
    actionable = [r for r in rows if r.get("priority")]
    out: dict[str, list[str]] = {}
    for r in sorted(actionable, key=lambda r: (_PRIORITY_ORDER.get(r["priority"], 9), r.get("email", ""))):
        out.setdefault(_group_key(r), []).append(
            f"`{r['priority']}` {r['email']} — {r['top_problem']}")
    return out


def _post(token: str, channel: str, text: str, thread_ts: str | None = None) -> str | None:
    """Post one message; returns its ts on success, None on failure."""
    body = {"channel": channel, "text": text, "unfurl_links": False}
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
            head = f"*{client} — full list ({len(lines)} items)*\n" if i == 0 else ""
            _post(token, channel, head + "\n".join(chunk), thread_ts=ts)
    return True
