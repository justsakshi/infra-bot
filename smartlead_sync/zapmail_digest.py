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


def _plural(n: int, word: str, many: str | None = None) -> str:
    return f"{n} {word if n == 1 else (many or word + 's')}"


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
    zap_billing: list[dict] | tuple = (),
    scaledmail: dict | None = None,
    errors: list[str] | tuple = (),
    readiness: dict | None = None,
) -> tuple[list[str], list[str], int]:
    """``(headline_lines, thread_parts, action_count)``. Pure - no I/O.

    Headline (spec 2026-10-13): ✅/⚠️ + date, then at most 3 short lines: what
    bills soon, what is being set up / became ready, what needs a look. Every
    list (accounts, domains, batches, bills, past-client domains) is in the
    thread; the boilerplate is said once, in the last reply."""
    actions = 0
    look: list[str] = []          # "needs a look" counts for the headline

    # ── Zapmail ──
    zap = ["*Zapmail*", "*Accounts*"]
    unreachable = low_wallets = 0
    for name, s in sorted(status.items()):
        if not s.get("ok"):
            actions += 1
            unreachable += 1
            zap.append(f"• *{name}* — :warning: unreachable ({s.get('error', '')[:80]})")
            continue
        wallet = s.get("wallet_balance")
        recharge = "auto-recharge on" if s.get("auto_recharge") else "auto-recharge OFF"
        credits = s.get("placement_credits")
        act = s.get("active_mailboxes") or {}
        boxes = (f"{s.get('active_mailboxes_total')} mailboxes" if s.get("active_mailboxes_total") is not None
                 else f"{act.get('GOOGLE', '?')} Google + {act.get('MICROSOFT', '?')} Outlook mailboxes"
                 if act else f"{s.get('assigned_mailboxes')} mailboxes")
        zap.append(f"• *{name}* — wallet {_money(wallet)} ({recharge}) · {boxes}"
                   + (f" · {credits} placement credits" if credits is not None else ""))
        try:
            low = not s.get("auto_recharge") and float(wallet or 0) < wallet_min
        except (TypeError, ValueError):
            low = False
        if low:
            actions += 1
            low_wallets += 1
            zap.append(f"   :warning: wallet under {_money(wallet_min)} with auto-recharge off "
                       "— purchases and renewals will fail")
    if unreachable:
        look.append(f"{_plural(unreachable, 'Zapmail account')} unreachable")
    if low_wallets:
        look.append(f"{_plural(low_wallets, 'wallet')} low")

    cutoff = (today + timedelta(days=expiry_days)).isoformat()
    expiring = sorted((r for r in renewals if r.get("expire_on") and r["expire_on"] <= cutoff),
                      key=lambda r: r["expire_on"])
    # Rows tagged with no current client are past-client domains being left
    # to lapse (standup 2026-09-29): one thread line, never an action.
    past = [r for r in expiring if "client" in r and not r["client"]]
    soon = [r for r in expiring if r not in past]
    zap += ["", f"*Current-client domains expiring within {expiry_days} days* ({len(soon)})"]
    for e in renewal_errors:
        actions += 1
        zap.append(f":warning: could not check renewals for {e[:120]}")
    if renewal_errors:
        look.append(f"renewals not checked on {_plural(len(renewal_errors), 'account')}")
    if soon:
        actions += len(soon)
        look.append(f"{_plural(len(soon), 'client domain')} expiring")
        for r in soon:
            who = f" · {r['client']}" if r.get("client") else ""
            zap.append(f"• `{r['domain']}` — {r['expire_on']}{who} · {r.get('account')}"
                       f" · {r.get('assigned_mailboxes', 0)} inboxes")
    elif not renewal_errors:
        zap.append("_none_")

    today_s = today.isoformat()
    due = [b for b in batches if b.get("status") in ("planned", "failed")
           and (b.get("earliest_date") or "") <= today_s]
    blocked = [b for b in batches if b.get("status") in BLOCKED_STATUSES]
    zap += ["", "*Domain purchases*"]
    if not ledger_ok:
        zap.append(":warning: purchase ledger unreachable (Mongo)")
        look.append("purchase ledger unreachable")
    if due:
        actions += len(due)
        look.append(f"{_plural(len(due), 'purchase batch', 'purchase batches')} due")
        zap.append(f"Ready to buy ({len(due)}):")
        for b in sorted(due, key=lambda x: x.get("earliest_date", "")):
            zap.append(f"• `{b['batch_id']}` {b.get('client') or 'primary'} — "
                       f"{', '.join(b.get('domains', []))} · est. "
                       f"{_money(b.get('estimated_usd'))} · due {b.get('earliest_date')}")
    if blocked:
        actions += len(blocked)
        look.append(f"{_plural(len(blocked), 'batch', 'batches')} to reconcile")
        zap.append(f"Needs reconcile ({len(blocked)}):")
        for b in blocked:
            zap.append(f"• `{b['batch_id']}` is *{b.get('status')}* — run "
                       f"`zapmail_buy.py --reconcile {b['batch_id']}`")
    if not due and not blocked and ledger_ok:
        zap.append("_nothing due_")

    bill_lines = billing_lines(list(zap_billing), today=today)
    if bill_lines:
        actions += len(bill_lines)
        zap += ["", "*Inbox subscriptions billing in 3 days*"] + bill_lines
    horizon = (today + timedelta(days=3)).isoformat()
    bills = [float(r.get("price") or 0) for r in zap_billing
             if r.get("bills_on") and r["bills_on"] <= horizon]
    failed = sum(1 for r in zap_billing if r.get("payment_failure"))
    if failed:
        look.append(f"{_plural(failed, 'payment')} failed")

    # ── ScaledMail ──
    sm = scaledmail or {}
    sm_part = ""
    setup_bits: list[str] = []
    if scaledmail is not None:
        sm_lines = list(sm.get("lines") or [])
        actions += len(sm_lines)
        sm_part = "\n".join(["*ScaledMail*"] + (["• " + l for l in sm_lines] or ["_nothing to act on_"]))
        bills += [float(o.get("amount") or 0) for o in sm.get("billing") or []]
        for key, word in (("setting_up", "setting up"), ("live", "live"),
                          ("ready", "ready in Smartlead")):
            if sm.get(key):
                setup_bits.append(f"{sm[key]} {word}")
        if sm.get("late"):
            look.append(f"{_plural(sm['late'], 'ScaledMail order')} late")
        if sm.get("live"):
            look.append(f"{_plural(sm['live'], 'live order')} to send to support")
        if sm.get("unassigned"):
            look.append(f"{_plural(len(sm['unassigned']), 'domain')} with no client")
        if sm.get("flagged"):
            look.append(f"{_plural(sm['flagged'], 'domain')} under 80% in ScaledMail's placement test")
        if sm.get("stuck"):
            look.append(f"{_plural(sm['stuck'], 'ScaledMail domain')} stuck in setup")
    if errors:
        look.append(f"{_plural(len(errors), 'report')} could not be read")

    rd = readiness or {}
    if rd.get("low"):
        look.append(f"{_plural(len(rd['low']), 'domain')} past their warmup days but under 90% reputation")

    # ── headline ──
    attention = bool(look)
    head = [f"{'⚠️' if attention else '✅'} Domains & inboxes, {today.day} {today:%b}"]
    if bills:
        head.append(f"Bills in 3 days: {_money(sum(bills))} across {_plural(len(bills), 'bill')}")
    if setup_bits:
        head.append("ScaledMail orders: " + ", ".join(setup_bits))
    if rd.get("ready_today"):
        head.append("Ready to use today: " + "; ".join(rd["ready_today"]))
    head.append(("Needs a look: " + ", ".join(look)) if attention else "Nothing needs action today.")

    # ── thread ──
    parts = ["\n".join(zap)]
    if sm_part:
        parts.append(sm_part)
    if rd.get("ready_today") or rd.get("low"):
        parts.append("\n".join(["*Ready to send*"] + ["• " + x for x in rd.get("ready_today", [])]
                               + ["• ⚠️ " + x for x in rd.get("low", [])]))
    if past:
        parts.append(f"_{_plural(len(past), 'past-client domain')} lapsing (left to expire, no action): "
                     + ", ".join(r["domain"] for r in past) + "_")
    if errors:
        parts.append("\n".join(f":warning: {e[:160]}" for e in errors))
    tail = ["_Covers Zapmail and ScaledMail. Inboxkit is in the Daily Renewal Check._"]
    if bills:
        tail.append("_To stop paying for some inboxes: `/domains domain <name>` → Retire inboxes._")
    parts.append("\n".join(tail))
    return head, parts, actions


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


async def extra_sections(today: date) -> dict:
    """Zapmail inbox billing rows + the ScaledMail digest, for format_digest.
    A read failure becomes an error line; the digest still posts."""
    out: dict = {"zap_billing": [], "scaledmail": None, "errors": []}
    try:
        from smartlead.zapmail_fleet import subscriptions_billing
        out["zap_billing"] = await subscriptions_billing(days=3)
    except Exception as exc:  # noqa: BLE001 - the domain digest still posts
        out["errors"].append(f"Zapmail billing could not be read: {str(exc)[:120]}")
    try:
        from smartlead.scaledmail import configured
        if configured():
            from scaledmail_cli import digest as sm_digest
            from smartlead.scaledmail import ScaledMailClient
            with ScaledMailClient() as sm:
                out["scaledmail"] = await asyncio.to_thread(sm_digest, sm)
    except Exception as exc:  # noqa: BLE001
        out["errors"].append(f"ScaledMail could not be read: {str(exc)[:120]}")
    try:
        from smartlead.accounts import discover_accounts
        from smartlead.api import SmartleadClient
        from smartlead.inbox_intake import CURRENT_ACCOUNTS, readiness_lines
        boxes = []
        for acc in discover_accounts():
            if acc.name.upper() in CURRENT_ACCOUNTS:
                async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
                    boxes += await sl.list_email_accounts()
        out["readiness"] = readiness_lines(boxes, today)
    except Exception as exc:  # noqa: BLE001 - the digest still posts
        out["errors"].append(f"Ready-to-send check could not run: {str(exc)[:80]}")
    return out


def post_to_slack(headline_lines: list[str], thread_parts: list[str]) -> bool:
    from smartlead.notify import post_with_thread
    channel = os.getenv("ZAPMAIL_NOTIFY_CHANNEL", "").strip()
    token = os.getenv("SLACK_BOT_TOKEN") or os.getenv("DOMAINS_SLACK_BOT_TOKEN") or ""
    if not channel or not token:
        print("  [Zapmail digest] ZAPMAIL_NOTIFY_CHANNEL or Slack token not set — "
              "printed only, not posted.")
        return False
    return post_with_thread(token, channel, headline_lines, thread_parts) is not None


def as_text(headline_lines: list[str], thread_parts: list[str]) -> str:
    """Both parts as the terminal shows them."""
    return "\n".join(headline_lines) + "".join(
        f"\n\n--- thread reply {i} ---\n{p}" for i, p in enumerate(thread_parts, 1))


async def main() -> int:
    ap = argparse.ArgumentParser(description="Daily Zapmail digest (read-only)")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--no-post", action="store_true", help="Never post to Slack")
    args = ap.parse_args()

    today = date.today()
    data = await collect(today)
    # One daily message for both providers (2026-10-08): inbox subscriptions
    # billing soon (and failed payments) + ScaledMail's own items.
    extra = await extra_sections(today)
    head, parts, actions = format_digest(
        data["status"], data["renewals"], data["batches"], today=today,
        ledger_ok=data["ledger_ok"], renewal_errors=data["renewal_errors"],
        wallet_min=float(os.getenv("ZAPMAIL_WALLET_MIN", "30")),
        expiry_days=int(os.getenv("ZAPMAIL_EXPIRY_ALERT_DAYS", "14")), **extra)
    if args.json:
        print(json.dumps({"headline": head, "thread": parts, "text": as_text(head, parts),
                          "actions": actions}, default=str))
    else:
        print(as_text(head, parts))
    if not args.no_post:
        post_to_slack(head, parts)
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
