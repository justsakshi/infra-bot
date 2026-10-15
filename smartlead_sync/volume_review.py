#!/usr/bin/env python3
"""Weekly volume review: raise, hold or step back each set-up inbox's daily
campaign limit. The rule is smartlead/volume_rule.py; spec
docs/superpowers/specs/2026-10-15-weekly-volume-review-design.md.

REPORT ONLY by default (Manveen, 15 Oct): the post says what the rule would do
and changes nothing. Apply buttons and --apply-client work only with
VOLUME_REVIEW_APPLY=true, and then only when an approver presses Apply.

    python volume_review.py                  # dry run: print this week's review
    python volume_review.py --post           # save it (the shadow record) and post it
                                             #   (VOLUME_REVIEW_CHANNEL, else KILL_REPORT_CHANNEL, else C0AGVSUNEFP)
    python volume_review.py --apply-client "Precise Leads" --review 2026-11-03 --by U123 --json

Runs Tuesday 13:45 IST, after Kill warnings, when VOLUME_REVIEW_ENABLED=true.
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
    except Exception:
        pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".env"))
except Exception:
    pass

from smartlead import inbox_standard as st
from smartlead import volume_rule as vr

DEFAULT_CHANNEL = "C0AGVSUNEFP"
HEALTH_URL = "https://server.smartlead.ai/api/v1/analytics/mailbox/name-wise-health-metrics"
HEALTH_DAYS = 14
REPUTATION_WINDOW_DAYS = 7     # Smartlead: reputation is a 7-day window; early numbers are noise


def build_rows(per_account: dict[str, list[dict]], facts: dict, health: dict, approved: dict,
               verdicts: dict, today: date) -> tuple[list[dict], dict]:
    """Rows to decide on, and what was not reviewed (and why). Pure.
    health = {email: (sent, bounced)}; approved = {email: (limit, approved on)}."""
    from smartlead.report_sheet import client_label
    rows: list[dict] = []
    waiting: dict = {"warming": 0, "first_ready": None, "no_campaign": 0, "by_hand": [], "skipped": [], "rotate_ip": []}
    low_ip: dict[str, int] = {}
    for account, accounts in per_account.items():
        per_domain: dict[str, int] = {}
        for a in accounts:
            dom = str(a.get("from_email", "")).lower().split("@")[-1]
            per_domain[dom] = per_domain.get(dom, 0) + 1
        for a in accounts:
            email = str(a.get("from_email", "")).lower()
            dom = email.split("@")[-1]
            kind = st.inbox_kind(a, per_domain[dom])
            f = facts.get(email) or {}
            lim, on = approved.get(email, (None, None))
            reason = vr.eligible(a, kind, today, sending=bool(f.get("sending")), approved=lim)
            if reason == vr.NOT_OURS:
                continue
            rep = (a.get("warmup_details") or {}).get("warmup_reputation")
            started = st.warmup_started(a)
            if st.needs_ip_rotation(kind, rep) and started and (today - started).days >= REPUTATION_WINDOW_DAYS:
                low_ip[dom] = min(low_ip.get(dom, 101), st._int(rep))
            if reason:
                if reason.startswith("warming"):
                    waiting["warming"] += 1
                    ready = st.ready_date(a, kind).isoformat()
                    waiting["first_ready"] = min(filter(None, [waiting["first_ready"], ready]))
                elif reason.startswith("no campaign"):
                    waiting["no_campaign"] += 1
                elif reason.startswith("changed by hand"):
                    waiting["by_hand"].append(f"{email} {reason}")
                else:
                    waiting["skipped"].append(f"{email}: {reason}")
                continue
            sent, bounced = health.get(email, (0, 0))
            sig = vr.signals(f.get("tests") or [], sent, bounced, rep, today, kind)
            d = vr.decide(kind, st._int(a.get("message_per_day")), sig,
                          last_change=date.fromisoformat(on) if on else None, today=today, verdict=verdicts.get(email))
            if d["action"] == vr.SKIP:
                waiting["skipped"].append(f"{email}: {d['why']}")
                continue
            client = f.get("client") or client_label(account, a.get("client_id"))
            rows.append({**d, "client": client, "account": account, "id": a["id"], "email": email})
    waiting["rotate_ip"] = [f"{dom} ({rep}%)" for dom, rep in sorted(low_ip.items())]
    return rows, waiting


def apply_enabled() -> bool:
    """Report only until Manveen switches Apply on (15 Oct: "right now I just want you to report")."""
    return os.getenv("VOLUME_REVIEW_APPLY", "").strip().lower() == "true"


def apply_text(r: dict) -> str:
    if r.get("error"):
        return ":x: " + r["error"]
    n = len(r["changed"])
    lines = [f":white_check_mark: {r['client']}: changed the daily limit on {n} inbox{'es' if n != 1 else ''}"
             + (" (read back from Smartlead)." if n else ".")]
    lines += [f"• {c}" for c in r["changed"][:15]] + ([f"• and {n - 15} more"] if n > 15 else [])
    if r["skipped"]:
        lines.append("Skipped (changed since the review): " + "; ".join(r["skipped"][:10]))
    if r["failed"]:
        lines.append(":warning: Not changed: " + "; ".join(r["failed"][:10]))
    return "\n".join(lines)


def _health(key: str, today: date) -> dict:
    import httpx
    r = httpx.get(HEALTH_URL, params={"api_key": key, "start_date": (today - timedelta(days=HEALTH_DAYS)).isoformat(),
                                      "end_date": today.isoformat(), "timezone": "Asia/Kolkata"}, timeout=120)
    r.raise_for_status()
    rows = ((r.json() or {}).get("data") or {}).get("email_health_metrics") or []
    return {str(x.get("from_email", "")).lower(): (int(x.get("sent") or 0), int(x.get("bounced") or 0)) for x in rows}


def _clients() -> dict:
    from smartlead.accounts import discover_accounts
    from smartlead.inbox_intake import CURRENT_ACCOUNTS
    return {a.name: a for a in discover_accounts() if a.name.upper() in CURRENT_ACCOUNTS}


def gather(today: date, store, errors: list[str]) -> tuple[list[dict], dict]:
    from renewal_review import smartlead_facts
    from smartlead import kill_rule as kr
    from smartlead.api import SmartleadClient
    from smartlead.notify import redact
    facts = smartlead_facts(errors)
    verdicts = {e: v["verdict"] for e, v in kr.judge_all({e: f.get("tests") or [] for e, f in facts.items()}, today).items()}
    per_account: dict[str, list[dict]] = {}
    health: dict = {}

    async def read(acc):
        async with SmartleadClient(acc.api_key, account_name=acc.name) as sl:
            return await sl.list_email_accounts()

    for name, acc in _clients().items():
        try:
            per_account[name] = asyncio.run(read(acc))
            health.update(_health(acc.api_key, today))
        except Exception as exc:  # noqa: BLE001 - say so, keep the other account
            errors.append(f"Smartlead {name}: {redact(str(exc))[:150]}")
    return build_rows(per_account, facts, health, store.approved_limits(), verdicts, today)


def _post(text: str, blocks: list[dict]) -> bool:
    import requests
    from smartlead.notify import redact
    token = os.getenv("SLACK_BOT_TOKEN", "").strip()
    channel = (os.getenv("VOLUME_REVIEW_CHANNEL") or os.getenv("KILL_REPORT_CHANNEL")
               or os.getenv("PLACEMENT_REPORT_CHANNEL") or DEFAULT_CHANNEL)
    if not token:
        return False
    r = requests.post("https://slack.com/api/chat.postMessage", headers={"Authorization": f"Bearer {token}"},
                      json={"channel": channel, "text": redact(text)[:3000], "blocks": blocks}, timeout=20).json()
    if not r.get("ok"):
        print(f"  [volume-review] post failed: {r.get('error')}", file=sys.stderr)
    return bool(r.get("ok"))


def main() -> int:
    ap = argparse.ArgumentParser(description="Weekly volume review (raise / hold / step back daily limits)")
    ap.add_argument("--post", action="store_true")
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--apply-client", default=None)
    ap.add_argument("--review", default=None, help="review id (its date, YYYY-MM-DD)")
    ap.add_argument("--by", default="")
    args = ap.parse_args()
    from smartlead.notify import redact
    today = date.today()
    if args.apply_client and not apply_enabled():
        r = {"error": "Applying volume changes is switched off (report only until the rule is proven; "
                      "VOLUME_REVIEW_APPLY=true turns it on)."}
        print(json.dumps({**r, "text": apply_text(r)}))
        return 0
    from smartlead.inbox_intake import IntakeStore
    try:
        store = IntakeStore()
    except Exception as exc:  # noqa: BLE001
        print(json.dumps({"error": redact(str(exc))}))
        return 1

    if args.apply_client:
        from smartlead.api import SmartleadClient

        async def run():
            sls = {}
            try:
                for name, acc in _clients().items():
                    sls[name] = await SmartleadClient(acc.api_key, account_name=acc.name).__aenter__()
                return await vr.apply_review(sls, store, args.review or today.isoformat(), args.apply_client,
                                             by=args.by, today=today)
            finally:
                for sl in sls.values():
                    await sl.__aexit__(None, None, None)
        try:
            r = asyncio.run(run())
        except Exception as exc:  # noqa: BLE001
            r = {"error": redact(f"{type(exc).__name__}: {exc}")[:200]}
        print(json.dumps({**r, "text": apply_text(r)}))
        return 0

    errors: list[str] = []
    rows, waiting = gather(today, store, errors)
    if errors:
        waiting["skipped"] = waiting.get("skipped", []) + [f"could not read {e}" for e in errors]
    text = vr.review_text(rows, waiting, today, apply_enabled())
    out = {"review": today.isoformat(), "rows": len(rows), "text": text}
    if args.post:
        store.save_review(today.isoformat(), rows)
        out["posted"] = _post(text, vr.review_blocks(rows, waiting, today, today.isoformat(), apply_enabled()))
    print(json.dumps(out, default=str) if args.json else text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
