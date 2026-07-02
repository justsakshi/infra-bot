"""Daily Slack digest of inbox-health action items, grouped by client manager."""
from __future__ import annotations

import os

import requests

from smartlead.config import HEALTH_NOTIFY_CHANNEL

_PRIORITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}


def build_digest(rows: list[dict], sheet_url: str) -> str:
    """Markdown digest: only rows needing action (priority set), grouped by client."""
    actionable = [r for r in rows if r.get("priority")]
    if not actionable:
        return f"✅ Inbox Health: all inboxes healthy today. Workbook: {sheet_url}"

    by_client: dict[str, list[dict]] = {}
    for r in actionable:
        by_client.setdefault(r.get("client", "Unknown"), []).append(r)

    # clients with a P0 first, then by count
    def client_rank(items):
        return (0 if any(i["priority"] == "P0" for i in items) else 1, -len(items))

    lines = [f"*🩺 Inbox Health — {len(actionable)} inbox(es) need attention*",
             f"<{sheet_url}|Open the workbook>", ""]
    for client, items in sorted(by_client.items(), key=lambda kv: client_rank(kv[1])):
        items.sort(key=lambda r: _PRIORITY_ORDER.get(r["priority"], 9))
        mgr_slack = next((i.get("_mgr_slack") for i in items if i.get("_mgr_slack")), "")
        mgr_name = items[0].get("manager", "Unassigned")
        who = f"<{mgr_slack}>" if mgr_slack.startswith("U") else (mgr_slack or mgr_name)
        p0 = sum(1 for i in items if i["priority"] == "P0")
        lines.append(f"*{client}* — {who} · {len(items)} item(s){f', {p0} 🔴 P0' if p0 else ''}")
        for i in items[:8]:
            auto = " _(auto-fixing)_" if i.get("owner", "").startswith("🤖") else ""
            lines.append(f"   • `{i['priority']}` {i['email']} — {i['top_problem']}{auto}")
        if len(items) > 8:
            lines.append(f"   • …and {len(items) - 8} more")
        lines.append("")
    return "\n".join(lines).strip()


def post_digest(text: str) -> bool:
    token = os.getenv("SLACK_BOT_TOKEN", "")
    channel = HEALTH_NOTIFY_CHANNEL
    if not token or not channel:
        print("  [Notify] SLACK_BOT_TOKEN/HEALTH_NOTIFY_CHANNEL missing - skipping post.")
        return False
    try:
        resp = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {token}"},
            json={"channel": channel, "text": text, "unfurl_links": False},
            timeout=15,
        )
        ok = resp.json().get("ok", False)
        if not ok:
            print(f"  [Notify] Slack error: {resp.json().get('error')}")
        return ok
    except requests.RequestException as exc:
        print(f"  [Notify] Slack post failed: {exc}")
        return False
