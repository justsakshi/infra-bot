#!/usr/bin/env python3
"""Daily Zapmail digest — read-only. What needs a human today.

Covers every configured Zapmail account:
  * wallet balance, auto-recharge, mailbox usage, placement credits
  * low wallet (auto-recharge off and balance under ZAPMAIL_WALLET_MIN)
  * domains expiring within ZAPMAIL_EXPIRY_ALERT_DAYS (default 14)
  * purchase batches due today / overdue, and batches blocked on reconcile

Posts to Slack only when ZAPMAIL_NOTIFY_CHANNEL is set (off by default —
same rule as the inbox-health digest); otherwise prints. Spends nothing and
changes nothing, so it is safe to run on a cron.

    python zapmail_digest.py            # print (and post if channel set)
    python zapmail_digest.py --json     # machine-readable
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, timedelta

if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

BLOCKED_STATUSES = ("unknown", "in_progress", "partial")


def _money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "$?"


def format_digest(
    status: dict,
    renewals: list[dict],
    batches: list[dict],
    *,
    today: date,
    ledger_ok: bool,
    renewal_errors: list[str] | tuple = (),
    wallet_min: float = 30.0,
    expiry_days: int = 14,
) -> tuple[str, int]:
    """``(slack_text, action_count)``. Pure — no I/O, so it is unit-tested."""
    actions = 0
    # Say what this covers: on 2026-09-29 the team saw "0 expiring" here and
    # "5 expire today" in the Daily Renewal Check - those 5 were on Inboxkit,
    # which Zapmail cannot see.
    lines = [f"*📬 Domains & inboxes daily — {today.isoformat()}*",
             "_Zapmail and ScaledMail (sections below). Inboxkit is in the Daily Renewal Check._",
             ""]

    lines.append("*Accounts*")
    for name, s in sorted(status.items()):
        if not s.get("ok"):
            actions += 1
            lines.append(f"• *{name}* — :warning: unreachable ({s.get('error', '')[:80]})")
            continue
        wallet = s.get("wallet_balance")
        recharge = "auto-recharge on" if s.get("auto_recharge") else "auto-recharge OFF"
        credits = s.get("placement_credits")
        act = s.get("active_mailboxes") or {}
        boxes = (f"{act.get('GOOGLE', '?')} Google + {act.get('MICROSOFT', '?')} Outlook mailboxes"
                 if act else f"{s.get('assigned_mailboxes')} mailboxes")
        lines.append(
            f"• *{name}* — wallet {_money(wallet)} ({recharge}) · {boxes}"
            + (f" · {credits} placement credits" if credits is not None else ""))
        try:
            low = not s.get("auto_recharge") and float(wallet or 0) < wallet_min
        except (TypeError, ValueError):
            low = False
        if low:
            actions += 1
            lines.append(f"   :warning: wallet under {_money(wallet_min)} with auto-recharge off "
                         "— purchases and renewals will fail")

    cutoff = (today + timedelta(days=expiry_days)).isoformat()
    expiring = sorted((r for r in renewals if r.get("expire_on") and r["expire_on"] <= cutoff),
                      key=lambda r: r["expire_on"])
    # Rows tagged with no current client are past-client domains being left
    # to lapse (standup 2026-09-29): reported as a count, never as an action.
    past = [r for r in expiring if "client" in r and not r["client"]]
    soon = [r for r in expiring if r not in past]
    lines += ["", f"*Current-client domains expiring within {expiry_days} days* ({len(soon)})"]
    for e in renewal_errors:
        actions += 1
        lines.append(f":warning: could not check renewals for {e[:120]}")
    if soon:
        actions += len(soon)
        for r in soon[:15]:
            who = f" · {r['client']}" if r.get("client") else ""
            lines.append(f"• `{r['domain']}` — {r['expire_on']}{who} · {r.get('account')}"
                         f" · {r.get('assigned_mailboxes', 0)} inboxes")
        if len(soon) > 15:
            lines.append(f"• …and {len(soon) - 15} more (`/zapmail renewals`)")
    elif not renewal_errors:
        lines.append("_none_")
    if past:
        lines.append(f"_{len(past)} past-client domain(s) lapsing too — left to expire._")

    today_s = today.isoformat()
    due = [b for b in batches if b.get("status") in ("planned", "failed")
           and (b.get("earliest_date") or "") <= today_s]
    blocked = [b for b in batches if b.get("status") in BLOCKED_STATUSES]
    lines += ["", "*Domain purchases*"]
    if not ledger_ok:
        lines.append(":warning: purchase ledger unreachable (Mongo)")
    if due:
        actions += len(due)
        lines.append(f"Ready to buy ({len(due)}):")
        for b in sorted(due, key=lambda x: x.get("earliest_date", "")):
            lines.append(f"• `{b['batch_id']}` {b.get('client') or 'primary'} — "
                         f"{', '.join(b.get('domains', []))} · est. "
                         f"{_money(b.get('estimated_usd'))} · due {b.get('earliest_date')}")
    if blocked:
        actions += len(blocked)
        lines.append(f"Needs reconcile ({len(blocked)}):")
        for b in blocked:
            lines.append(f"• `{b['batch_id']}` is *{b.get('status')}* — run "
                         f"`zapmail_buy.py --reconcile {b['batch_id']}`")
    if not due and not blocked and ledger_ok:
        lines.append("_nothing due_")

    if actions == 0:
        lines += ["", "✅ Nothing needs action today."]
    return "\n".join(lines), actions


async def collect(today: date) -> dict:
    from smartlead.domain_batch import BatchStore
    from smartlead.zapmail_fleet import fleet_status, renewals_asof

    status = await fleet_status()
    errors: list[str] = []
    renewals = await renewals_asof(errors)
    store = BatchStore()
    return {"status": status, "renewals": renewals, "renewal_errors": errors,
            "batches": store.load_all(), "ledger_ok": store.available}


def billing_lines(rows: list[dict], *, today: date, days: int = 3) -> list[str]:
    """Zapmail inbox subscriptions billing within ``days`` and any failed
    payment. Pure. Inboxes renew with their subscription, never one by one."""
    horizon = (today + timedelta(days=days)).isoformat()
    out = []
    for r in rows:
        due = r.get("bills_on") and r["bills_on"] <= horizon
        if not (due or r.get("payment_failure")):
            continue
        what = (f"{r.get('mailboxes') or '?'} {'Outlook' if r.get('provider') == 'MICROSOFT' else 'Google'} "
                f"{r.get('kind', 'inboxes')}")
        who = ", ".join(r.get("clients") or []) or r.get("account", "")
        line = f"• {r.get('bills_on')}: {what} {_money(r.get('price'))} ({who})"
        if r.get("payment_failure"):
            line = ":x: " + line + f" — *payment failed: {r['payment_failure']}* (inboxes may be suspended)"
            if r.get("invoice_url"):
                line += f" · <{r['invoice_url']}|Pay / see invoice>"
        out.append(line)
    return out


async def extra_sections(today: date) -> tuple[str, int]:
    """Zapmail inbox billing + ScaledMail digest, as one block of text."""
    parts, n = [], 0
    try:
        from smartlead.zapmail_fleet import subscriptions_billing
        lines = billing_lines(await subscriptions_billing(days=3), today=today)
        if lines:
            parts.append("*Inbox subscriptions billing in 3 days (Zapmail)*\n" + "\n".join(lines)
                         + "\n_To stop paying for some inboxes: `/domains domain <name>` → Retire inboxes._")
            n += len(lines)
    except Exception as exc:  # noqa: BLE001 - the domain digest still posts
        parts.append(f":warning: Zapmail billing could not be read: {str(exc)[:120]}")
    try:
        from smartlead.scaledmail import configured
        if configured():
            from scaledmail_cli import digest as sm_digest
            from smartlead.scaledmail import ScaledMailClient
            with ScaledMailClient() as sm:
                d = await asyncio.to_thread(sm_digest, sm)
            if d.get("lines"):
                parts.append("*ScaledMail*\n" + "\n".join("• " + l for l in d["lines"]))
                n += len(d["lines"])
    except Exception as exc:  # noqa: BLE001
        parts.append(f":warning: ScaledMail could not be read: {str(exc)[:120]}")
    return "\n\n".join(parts), n


def post_to_slack(text: str) -> bool:
    from smartlead.notify import _post
    channel = os.getenv("ZAPMAIL_NOTIFY_CHANNEL", "").strip()
    token = os.getenv("SLACK_BOT_TOKEN") or os.getenv("DOMAINS_SLACK_BOT_TOKEN") or ""
    if not channel or not token:
        print("  [Zapmail digest] ZAPMAIL_NOTIFY_CHANNEL or Slack token not set — "
              "printed only, not posted.")
        return False
    return _post(token, channel, text) is not None


async def main() -> int:
    ap = argparse.ArgumentParser(description="Daily Zapmail digest (read-only)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-post", action="store_true", help="Never post to Slack")
    args = ap.parse_args()

    today = date.today()
    data = await collect(today)
    text, actions = format_digest(
        data["status"], data["renewals"], data["batches"], today=today,
        ledger_ok=data["ledger_ok"], renewal_errors=data["renewal_errors"],
        wallet_min=float(os.getenv("ZAPMAIL_WALLET_MIN", "30")),
        expiry_days=int(os.getenv("ZAPMAIL_EXPIRY_ALERT_DAYS", "14")))
    # One daily message for both providers (2026-10-08): inbox subscriptions
    # billing soon (and failed payments) + ScaledMail's own items.
    extra, n_extra = await extra_sections(today)
    if extra:
        text += "\n\n" + extra
        actions += n_extra
    if args.json:
        print(json.dumps({"text": text, "actions": actions}, default=str))
    else:
        print(text)
    if not args.no_post:
        post_to_slack(text)
    return 0


def _run() -> int:
    from smartlead.zapmail import ZapmailError
    try:
        return asyncio.run(main())
    except ZapmailError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_run())
