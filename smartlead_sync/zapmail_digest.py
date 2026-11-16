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

Daily digest (16 Nov 2026, smartlead/digest.py): with the digest on, nothing is posted here. Each to-do (low
wallet, client domains expiring, batches due or to reconcile, ScaledMail orders late / live / stuck, domains
with no client) is an ask with its step; an account or report that could not be read is a "broken" ask; the
status lines are silent and the full digest goes in the digest's thread. Failed payments are left to the
billing watch, which asks about them already. If the digest can't be written, the old post goes out.
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
SOURCE = "zapmail-digest"


def _money(v) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return "$?"


def _day(iso) -> str:
    from smartlead.notify import day
    return day(iso)


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
) -> tuple[list[str], list[str], int]:
    """``(headline_lines, thread_parts, action_count)``. Pure - no I/O.

    Headline (spec 2026-10-13): ✅/⚠️ + date, then at most 3 short lines: what
    bills soon, what is being set up / became ready, what needs a look. Every
    list (accounts, domains, batches, bills, past-client domains) is in the
    thread; the boilerplate is said once, in the last reply."""
    actions = 0
    look: list[str] = []          # "needs a look" counts for the headline

    # ── Zapmail ──
    zap = ["*Zapmail*"]
    unreachable = low_wallets = 0
    for name, s in sorted(status.items()):
        if not s.get("ok"):
            actions += 1
            unreachable += 1
            zap.append(f"• *{name}* · :warning: unreachable ({s.get('error', '')[:80]})")
            continue
        wallet = s.get("wallet_balance")
        recharge = "auto-recharge on" if s.get("auto_recharge") else "auto-recharge OFF"
        credits = s.get("placement_credits")
        act = s.get("active_mailboxes") or {}
        boxes = (f"{s.get('active_mailboxes_total')} mailboxes" if s.get("active_mailboxes_total") is not None
                 else f"{act.get('GOOGLE', '?')} Google + {act.get('MICROSOFT', '?')} Outlook mailboxes"
                 if act else f"{s.get('assigned_mailboxes')} mailboxes")
        zap.append(f"• *{name}* · wallet {_money(wallet)} ({recharge}) · {boxes}"
                   + (f" · {credits} placement credits" if credits is not None else ""))
        try:
            low = not s.get("auto_recharge") and float(wallet or 0) < wallet_min
        except (TypeError, ValueError):
            low = False
        if low:
            actions += 1
            low_wallets += 1
            zap.append(f":warning: {name}'s wallet is under {_money(wallet_min)} with auto-recharge off, "
                       "so purchases and renewals will fail")
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
    zap += ["", f"*Current-client domains expiring within {expiry_days} days ({len(soon)})*"]
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
            zap.append(f"• `{r['domain']}` · {_day(r['expire_on'])}{who} · {r.get('account')}"
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
            zap.append(f"• `{b['batch_id']}` {b.get('client') or 'primary'} · "
                       f"{', '.join(b.get('domains', []))} · est. "
                       f"{_money(b.get('estimated_usd'))} · due {_day(b.get('earliest_date'))}")
    if blocked:
        actions += len(blocked)
        look.append(f"{_plural(len(blocked), 'batch', 'batches')} to reconcile")
        zap.append(f"Needs reconcile ({len(blocked)}):")
        for b in blocked:
            zap.append(f"• `{b['batch_id']}` is *{b.get('status')}*: run "
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

    # ── headline ──
    attention = bool(look)
    head = [f"{'⚠️' if attention else '✅'} Domains & inboxes, {today.day} {today:%b}"]
    if bills:
        head.append(f"Bills in 3 days: {_money(sum(bills))} across {_plural(len(bills), 'bill')}")
    if setup_bits:
        head.append("ScaledMail orders: " + ", ".join(setup_bits))
    head.append(("Needs a look: " + ", ".join(look)) if attention else "Nothing needs action today.")

    # ── thread ──
    parts = ["\n".join(zap)]
    if sm_part:
        parts.append(sm_part)
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


def _ids(xs: list[str], k: int = 3) -> str:
    return ", ".join(xs[:k]) + (f" and {len(xs) - k} more" if len(xs) > k else "")


def digest_asks(
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
) -> list:
    """What format_digest's "Needs a look" asks of a person, as digest asks (same inputs). Lists stay in the
    thread. Bills coming up are status (the renewal review asks about them); failed payments are the billing
    watch's. Pure."""
    from smartlead import digest as dg
    asks = []
    for name, s in sorted(status.items()):
        if not s.get("ok"):
            asks.append(dg.Ask(f"Zapmail account {name} could not be read, so its wallet and domains were not checked.",
                               "It is read again tomorrow; if it stays, check that account's Zapmail key on Render "
                               "(ZAPMAIL_API_KEY, or ZAPMAIL_API_KEY_<account> for the others).",
                               key=f"{SOURCE}:unreachable:{name}", kind="broken"))
            continue
        try:
            low = not s.get("auto_recharge") and float(s.get("wallet_balance") or 0) < wallet_min
        except (TypeError, ValueError):
            low = False
        if low:
            asks.append(dg.Ask(f"{name}'s Zapmail wallet is at {_money(s.get('wallet_balance'))} with auto-recharge off, "
                               "so purchases and renewals will fail.",
                               f"Zapmail ({name} account), Wallet: add funds or turn auto-recharge on.",
                               key=f"{SOURCE}:low-wallet:{name}"))
    if renewal_errors:
        asks.append(dg.Ask(f"Zapmail domain renewals could not be checked on {_plural(len(renewal_errors), 'account')}.",
                           "It is checked again tomorrow; the error is in the thread.",
                           key=f"{SOURCE}:renewals-unread", kind="broken"))
    cutoff = (today + timedelta(days=expiry_days)).isoformat()
    soon = sorted((r for r in renewals if r.get("expire_on") and r["expire_on"] <= cutoff
                   and not ("client" in r and not r["client"])), key=lambda r: r["expire_on"])
    if soon:
        asks.append(dg.Ask(f"{_plural(len(soon), 'client domain')} expire{'s' if len(soon) == 1 else ''} within "
                           f"{expiry_days} days, the first on "
                           f"{_day(soon[0]['expire_on'])}.",
                           "In Zapmail, Domains, check each domain in the thread is set to renew; let one lapse only "
                           "if its client is gone.", key=f"{SOURCE}:domains-expiring"))
    if not ledger_ok:
        asks.append(dg.Ask("The domain purchase ledger (Mongo) could not be read, so due purchases are not listed.",
                           "It is read again tomorrow; if it stays, check MONGO_URI on Render.",
                           key=f"{SOURCE}:ledger", kind="broken"))
    today_s = today.isoformat()
    due = [b for b in batches if b.get("status") in ("planned", "failed") and (b.get("earliest_date") or "") <= today_s]
    blocked = [b for b in batches if b.get("status") in BLOCKED_STATUSES]
    if due:
        cost = sum(float(b.get("estimated_usd") or 0) for b in due)
        asks.append(dg.Ask(f"{_plural(len(due), 'domain purchase batch', 'domain purchase batches')} "
                           f"{'is' if len(due) == 1 else 'are'} due to buy (about {_money(cost)}).",
                           "Decide on each batch in the thread; to buy one, run `zapmail_buy.py --execute <batch_id> "
                           f"--approve` with ZAPMAIL_ALLOW_SPEND=true ({_ids([b['batch_id'] for b in due])}).",
                           key=f"{SOURCE}:batches-due"))
    if blocked:
        asks.append(dg.Ask(f"{_plural(len(blocked), 'purchase batch', 'purchase batches')} stopped half way and "
                           f"need{'s' if len(blocked) == 1 else ''} a reconcile.",
                           "Run `zapmail_buy.py --reconcile <batch_id>` for each: "
                           f"{_ids([b['batch_id'] for b in blocked])}.", key=f"{SOURCE}:batches-reconcile"))
    sm = scaledmail or {}
    if sm.get("late"):
        asks.append(dg.Ask(f"{_plural(sm['late'], 'ScaledMail order')} {'is' if sm['late'] == 1 else 'are'} late in "
                           "setup.", "Ask ScaledMail support about the orders marked Late in the thread.",
                           key=f"{SOURCE}:sm-late"))
    if sm.get("live"):
        asks.append(dg.Ask(f"{_plural(sm['live'], 'ScaledMail order')} {'is' if sm['live'] == 1 else 'are'} live and "
                           "waiting for its inboxes in Smartlead.",
                           "Send ScaledMail support the message on each LIVE line in the thread so they upload the "
                           "inboxes to Smartlead.", key=f"{SOURCE}:sm-live"))
    if sm.get("stuck"):
        asks.append(dg.Ask(f"{_plural(sm['stuck'], 'ScaledMail domain')} {'is' if sm['stuck'] == 1 else 'are'} stuck in "
                           "setup.", "Ask ScaledMail support about the domains under Still being set up in the thread.",
                           key=f"{SOURCE}:sm-stuck"))
    if sm.get("unassigned"):
        k = len(sm["unassigned"])
        asks.append(dg.Ask(f"{_plural(k, 'ScaledMail domain')} {'has' if k == 1 else 'have'} no client.",
                           f"In ScaledMail, set the client tag on the order of {_ids(list(sm['unassigned']))}.",
                           key=f"{SOURCE}:sm-no-client"))
    if errors:
        asks.append(dg.Ask(f"{_plural(len(errors), 'report')} could not be read for the domains and inboxes digest "
                           f"({'; '.join(e.split(':', 1)[0] for e in errors)}).",
                           "It is read again tomorrow; the error is in the thread.",
                           key=f"{SOURCE}:read", kind="broken"))
    return asks


def to_digest(asks: list, head: list[str], parts: list[str]) -> bool:
    """Hand the digest this run's asks and the full post for its thread. False = post the old way."""
    from smartlead import digest as dg
    return dg.on() and dg.report(SOURCE, asks, [], ["\n".join(head)] + list(parts))


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
        what = (f"{str(r.get('mailboxes')) + ' ' if r.get('mailboxes') else ''}{'Outlook' if r.get('provider') == 'MICROSOFT' else 'Google'} "
                f"{r.get('kind', 'inboxes')}")
        who = ", ".join(r.get("clients") or []) or r.get("account", "")
        line = f"{_day(r.get('bills_on'))}: {what} {_money(r.get('price'))}" + (f" ({who})" if who else "")
        if r.get("payment_failure"):
            line = ":x: " + line + f" · *payment failed: {r['payment_failure']}* (inboxes may be suspended)"
            if r.get("invoice_url"):
                line += f" · <{r['invoice_url']}|Pay / see invoice>"
        out.append("• " + line)
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
    inputs = dict(today=today, ledger_ok=data["ledger_ok"], renewal_errors=data["renewal_errors"],
                  wallet_min=float(os.getenv("ZAPMAIL_WALLET_MIN", "30")),
                  expiry_days=int(os.getenv("ZAPMAIL_EXPIRY_ALERT_DAYS", "14")), **extra)
    head, parts, actions = format_digest(data["status"], data["renewals"], data["batches"], **inputs)
    if args.json:
        print(json.dumps({"headline": head, "thread": parts, "text": as_text(head, parts),
                          "actions": actions}, default=str))
    else:
        print(as_text(head, parts))
    if not args.no_post:
        asks = digest_asks(data["status"], data["renewals"], data["batches"], **inputs)
        if not to_digest(asks, head, parts):
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
