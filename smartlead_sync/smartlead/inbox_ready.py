"""ScaledMail inboxes that reached Smartlead -> ready to send.

ScaledMail's support uploads an order's inboxes to Smartlead days after the
order. When every inbox of a placed order (scaledmail_order_plans) is in the
client's Smartlead account, this:
  - sets the client's signature and Smartlead client (inbox_setup.plan_changes;
    an inbox that already has the same signature is left alone),
  - adds the vendor tag (ScaledMail-Google / -Microsoft / -SMTP) and the client
    tag (tags are only ever added),
  - flags a sender name that is not on the order (never renames),
and logs `in_smartlead` + `ready` in the order's history. Warmup and sending
caps are Smartlead's (decision 2026-10-08). Only inboxes on the order's own
domains are touched. Writes need approve=True (INBOX_READY_ENABLED on Render).
Live 2026-10-13: order 77129b's 20 SMTP inboxes arrived with no signature/tags.
"""

from __future__ import annotations

from smartlead.inbox_setup import plan_changes

VENDOR_TAG = {"google": "ScaledMail-Google", "outlook": "ScaledMail-Microsoft", "smtp": "ScaledMail-SMTP"}
CLIENT_TAG = {"Precise Leads": "PreciseLeads", "Melior": "Melior"}
TAG_BATCH = 25   # Smartlead: at most 25 inboxes per tag-mapping call


def _senders(plan: dict) -> set[str]:
    rows = next(iter((plan.get("payload") or {}).values()), {}).get("domains") or []
    return {f"{r.get('first_name', '')} {r.get('last_name', '')}".strip() for r in rows}


def plan_ready(plan: dict, accounts: list[dict], prof: dict, tags: dict[str, int]) -> dict:
    """Pure: what making this order's inboxes ready would change."""
    domains = {d.lower() for d in plan.get("domains") or []}
    mine = sorted((a for a in accounts
                   if str(a.get("from_email", "")).split("@")[-1].lower() in domains),
                  key=lambda a: a.get("id") or 0)
    notes: list[str] = []
    tag_ids: list[int] = []
    for name in (VENDOR_TAG.get(plan.get("provider", "")), CLIENT_TAG.get(plan.get("client", ""))):
        if name and name in tags:
            tag_ids.append(tags[name])
    if plan.get("client") not in CLIENT_TAG or CLIENT_TAG.get(plan.get("client")) not in tags:
        notes.append(f"no Smartlead tag for client {plan.get('client')}")
    senders = _senders(plan)
    changes = [plan_changes(a, prof, email=str(a["from_email"]).lower(), set_signature=True) for a in mine]
    return {
        "expected": int(plan.get("mailboxes") or 0),
        "found": [str(a["from_email"]).lower() for a in mine],
        "complete": bool(mine) and len(mine) >= int(plan.get("mailboxes") or 0),
        "account_ids": [a["id"] for a in mine],
        "changes": changes,
        "tag_ids": tag_ids,
        "name_mismatch": [f"{a['from_email']} ({a.get('from_name')})" for a in mine
                          if senders and (a.get("from_name") or "").strip() not in senders],
        "notes": notes,
    }


def _done(plan: dict) -> bool:
    return any(e.get("status") == "ready" for e in plan.get("history") or [])


def _label(plan: dict) -> str:
    return (f"{plan.get('mailboxes')} {VENDOR_TAG.get(plan.get('provider', ''), plan.get('provider', ''))} "
            f"inboxes ({len(plan.get('domains') or [])} domains, plan {plan.get('plan_id')})")


async def run_ready(store, *, approve: bool, sl_for, profile) -> dict:
    """Check every placed, not-yet-ready order. ``sl_for(client)`` gives a
    Smartlead client (or an async context manager of one); ``profile(client)``
    the inbox profile. Returns {"lines": [...], "ready": [plan ids]}."""
    lines: list[str] = []
    ready: list[str] = []
    todo = [p for p in store.all() if p.get("status") == "placed" and not _done(p)]
    by_client: dict[str, list[dict]] = {}
    for p in todo:
        by_client.setdefault(p.get("client", ""), []).append(p)
    for client, plans in by_client.items():
        handle = sl_for(client)
        if hasattr(handle, "__aenter__"):
            async with handle as sl:
                await _client_pass(sl, client, plans, profile(client), approve, store, lines, ready)
        else:
            await _client_pass(handle, client, plans, profile(client), approve, store, lines, ready)
    return {"lines": lines, "ready": ready}


async def _client_pass(sl, client, plans, prof, approve, store, lines, ready) -> None:
    accounts = await sl.list_email_accounts()
    tags = {t["name"]: t["id"] for t in await sl.list_tags()}
    for plan in plans:
        r = plan_ready(plan, accounts, prof, tags)
        if not r["found"]:
            continue                                     # not uploaded yet: the setup watcher speaks
        if not r["complete"]:
            lines.append(f"⏳ {len(r['found'])} of {r['expected']} inboxes in Smartlead for {_label(plan)} "
                         "- waiting for the rest.")
            continue
        edits = [c for c in r["changes"] if c.fields and not c.error]
        tag_names = [n for n, i in tags.items() if i in r["tag_ids"]]
        what = (f"signature/client on {len(edits)}, tags {' + '.join(tag_names) or 'none'}")
        warn = ("  ⚠️ sender not on the order: " + ", ".join(r["name_mismatch"])) if r["name_mismatch"] else ""
        note = ("  (" + "; ".join(r["notes"]) + ")") if r["notes"] else ""
        if not approve:
            lines.append(f"Would make {_label(plan)} ready: {what}.{warn}{note}")
            continue
        for c in edits:
            await sl.update_email_account(str(c.account_id), c.fields)
        if r["tag_ids"]:
            ids = r["account_ids"]
            for i in range(0, len(ids), TAG_BATCH):
                await sl.assign_tags(ids[i:i + TAG_BATCH], r["tag_ids"])
        store.log(plan["plan_id"], "in_smartlead", found=len(r["found"]), expected=r["expected"])
        store.log(plan["plan_id"], "ready", signatures=len(edits), tags=tag_names,
                  name_mismatch=r["name_mismatch"], notes=r["notes"])
        ready.append(plan["plan_id"])
        lines.append(f"✅ Ready: {_label(plan)} - {what}.{warn}{note}")
