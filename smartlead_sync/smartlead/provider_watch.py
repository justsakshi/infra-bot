"""Do Microsoft and Proofpoint recipients still get our mail? Judged on real sends, not seed tests. No network.

Why (measured 2026-11-01 on BettrData, 12,217 sent campaign x lead pairs): seed placement said Office365 7/7
inbox, yet inside the same campaigns Microsoft recipients replied 3 times against 26.9 expected at the Google
rate (0.11x) and sent 9 out-of-office replies against 60.5 (0.15x), with normal bounces. Exchange sends no
automatic reply for mail it filed in Junk, so out-of-office replies work as a junk sensor that seeds miss.

Rule (Karpathy review, 2026-11-01):
  * One event = a lead who replied (any reply that is a conversation) or sent an out-of-office.
  * Each campaign is its own control: a provider's expected events = its sends x that campaign's Google
    event rate, summed over campaigns with >= MIN_GOOGLE Google sends and >= MIN_TARGET sends to it.
  * Rolling WINDOW_DAYS of sends, so a week's thin numbers are pooled.
  * ALERT: expected >= MIN_EXPECTED and Poisson P(X <= observed) <= ALERT_P (10 expected, 3 seen fires).
  * WATCH: expected >= MIN_EXPECTED and observed/expected < WATCH_RATIO, not yet an alert.
  * OK: expected >= MIN_EXPECTED and the ratio is fine. TOO FEW: under MIN_EXPECTED expected events.
REPORT ONLY: nothing here pauses, removes or re-routes anything; a person decides.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import date, datetime, timedelta

WINDOW_DAYS = 28
MIN_GOOGLE = 30
MIN_TARGET = 10
MIN_EXPECTED = 10.0
ALERT_P = 0.011
WATCH_RATIO = 0.7
TARGETS = ("microsoft", "proofpoint")
NOT_A_CONVERSATION = {"out of office", "wrong person", "sender originated bounce"}

ALERT, WATCH, OK, TOO_FEW = "ALERT", "WATCH", "OK", "TOO FEW"

GOOGLE_MX = ("google.com", "googlemail.com")
MICROSOFT_MX = ("mail.protection.outlook.com", "outlook.com", "office365.us", "hotmail.com")
PROOFPOINT_MX = ("pphosted.com", "ppe-hosted.com")


def provider_from_mx(hosts: list[str]) -> str:
    """google / microsoft / proofpoint / other / no_mx from a domain's MX hosts."""
    hosts = [h.lower().rstrip(".") for h in hosts if h]
    if not hosts:
        return "no_mx"
    for markers, name in ((PROOFPOINT_MX, "proofpoint"), (GOOGLE_MX, "google"), (MICROSOFT_MX, "microsoft")):
        if any(h == m or h.endswith("." + m) for h in hosts for m in markers):
            return name
    return "other"


def _day(ts: str) -> date | None:
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).date()
    except ValueError:
        return None


def lead_events(stats_rows: list[dict], today: date, window_days: int = WINDOW_DAYS) -> dict[str, dict]:
    """One campaign's statistics rows (GET /campaigns/{id}/statistics) -> {email: {"event": bool}} for leads
    sent at least one email in the window. A bounced lead is dropped: it never reached a mailbox."""
    start = today - timedelta(days=window_days)
    rows = [(str(r.get("lead_email") or "").strip().lower(), r) for r in stats_rows]
    rows = [(e, r) for e, r in rows if "@" in e]
    bounced = {e for e, r in rows if r.get("is_bounced")}
    in_window = {e for e, r in rows if (d := _day(r.get("sent_time") or "")) and start < d <= today}
    out = {e: {"event": False} for e in in_window - bounced}
    for e, r in rows:   # a reply can sit on an earlier step's row, so read every row of a counted lead
        if e in out:
            cat = str(r.get("lead_category") or "").strip().lower()
            if cat == "out of office" or (r.get("reply_time") and cat not in NOT_A_CONVERSATION):
                out[e]["event"] = True
    return out


def poisson_le(k: int, lam: float) -> float:
    """P(X <= k) for X ~ Poisson(lam)."""
    if lam <= 0:
        return 1.0
    term = total = math.exp(-lam)
    for i in range(1, k + 1):
        term *= lam / i
        total += term
    return min(1.0, total)


def judge(campaigns: dict[str, dict[str, dict]], provider_of) -> dict[str, dict]:
    """campaigns: {campaign id: {email: {"event": bool}}}; provider_of(domain) -> provider name.
    Returns {target: {"verdict", "observed", "expected", "ratio", "p", "sent", "campaigns"}} plus "google"
    totals ({"sent", "events"}) for context."""
    google = {"sent": 0, "events": 0}
    result = {t: {"observed": 0, "expected": 0.0, "sent": 0, "campaigns": 0} for t in TARGETS}
    for leads in campaigns.values():
        counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
        for email, lead in leads.items():
            c = counts[provider_of(email.split("@", 1)[1])]
            c[0] += 1
            c[1] += bool(lead["event"])
        g_n, g_k = counts["google"]
        google["sent"] += g_n
        google["events"] += g_k
        if g_n < MIN_GOOGLE:
            continue
        for t in TARGETS:
            t_n, t_k = counts[t]
            if t_n < MIN_TARGET:
                continue
            r = result[t]
            r["campaigns"] += 1
            r["sent"] += t_n
            r["observed"] += t_k
            r["expected"] += t_n * g_k / g_n
    for r in result.values():
        exp, obs = r["expected"], r["observed"]
        r["ratio"] = round(obs / exp, 2) if exp else None
        r["p"] = poisson_le(obs, exp)
        if exp < MIN_EXPECTED:
            r["verdict"] = TOO_FEW
        elif r["p"] <= ALERT_P:
            r["verdict"] = ALERT
        elif obs / exp < WATCH_RATIO:
            r["verdict"] = WATCH
        else:
            r["verdict"] = OK
        r["expected"] = round(exp, 1)
    result["google"] = google
    return result


LABEL = {"microsoft": "Microsoft", "proofpoint": "Proofpoint"}


def client_lines(client: str, result: dict | None, error: str = "") -> list[str]:
    """Plain-text lines for one client."""
    if error:
        return [f"*{client}*: not read ({error})"]
    g = result["google"]
    rate = f"{100 * g['events'] / g['sent']:.1f}%" if g["sent"] else "-"
    lines = [f"*{client}*: Google recipients {g['sent']} sent, {g['events']} replied or out-of-office ({rate})"]
    for t in TARGETS:
        r = result[t]
        if not r["campaigns"]:
            lines.append(f"• {LABEL[t]}: no campaign with enough sends to compare")
            continue
        lines.append(f"• {LABEL[t]}: {r['verdict']}. {r['observed']} replied or out-of-office vs {r['expected']} "
                     f"expected at the Google rate ({r['ratio']}x) on {r['sent']} sent in {r['campaigns']} "
                     f"campaign{'s' if r['campaigns'] != 1 else ''}")
    return lines


def report_text(blocks: list[list[str]], today: date, window_days: int = WINDOW_DAYS) -> str:
    head = (f"Mail reaching Microsoft and Proofpoint recipients, last {window_days} days to {today.isoformat()} "
            "(real sends, each campaign compared with its own Google recipients)")
    alert = any(f": {ALERT}." in line for b in blocks for line in b)
    tail = ("ALERT means far fewer replies and out-of-office messages than Google recipients in the same "
            "campaigns: the mail is most likely going to junk, whatever seed tests say. Report only; nothing "
            "was paused or changed.") if alert else "Report only; nothing was paused or changed."
    return "\n\n".join([head] + ["\n".join(b) for b in blocks] + [tail])
