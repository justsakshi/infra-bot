"""Dynadot buy run: walk a names file, buy until the target, never overspend.

The only path that spends money on Dynadot. Built for t406 (Black Friday .co
buy, 27 Nov 2026) to run unattended from Task Scheduler at sale start.

For each name, in file order, until ``target`` domains are bought:

  ledger says bought      → counted, skipped
  ledger says unknown     → look it up in our account (``domain_info``):
                            ours → bought; not ours → treated as new
  search (free)           → not available / premium / not USD → skipped
                          → price above the ceiling: the sale is not live
                            yet. Before ``wait_until``: sleep and search
                            again. After it: stop, nothing bought above
                            the ceiling, ever.
  spend check             → spent so far (bought + unknown, at their price)
                            + this price must stay within ``total_ceiling``
  register                → success: bought
                            refused "not enough balance": stop
                            refused otherwise: failed, next name
                            (3 refusals in a row: stop)
                            no answer: look it up once; still unclear →
                            unknown (counted as spent), next name

The ledger (JSON, outside the repo: the repo is public and must never hold
domain names) is written after every domain, so a crash loses nothing and a
second run picks up where the first stopped.
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

from smartlead.dynadot import (DynadotBlocked, DynadotError, DynadotOutcomeUnknown, DynadotRefused,
                               funds_problem, spend_allowed)

SAFE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9-]{1,63})+$")
DEFAULT_DIR = Path(os.getenv("DYNADOT_DIR") or Path.home() / ".infrabot" / "dynadot")
ALLOWED_TLDS = tuple(t.strip().lower() for t in os.getenv("DYNADOT_TLDS", ".co").split(",") if t.strip())
PRICE_CEILING = float(os.getenv("DYNADOT_PRICE_CEILING", "2.60"))
TOTAL_CEILING = float(os.getenv("DYNADOT_PLAN_CEILING", "260"))
MAX_REFUSALS_IN_A_ROW = 3


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def read_names(path: str | Path) -> list[str]:
    """One domain per line; blanks and # comments ignored; order kept, duplicates dropped."""
    out, seen = [], set()
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        d = line.split("#", 1)[0].strip().lower()
        if d and d not in seen:
            seen.add(d)
            out.append(d)
    return out


def name_problems(names: list[str], tlds: tuple[str, ...] = ALLOWED_TLDS) -> dict[str, str]:
    bad = {}
    for d in names:
        if not SAFE.match(d):
            bad[d] = "not a domain"
        elif not d.endswith(tlds):
            bad[d] = f"not {'/'.join(tlds)}"
    return bad


class Ledger:
    """{"domains": {name: {status, price_usd, at, ...}}, "runs": [...]} in one JSON file."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.data = {"domains": {}, "runs": []}
        if self.path.exists():
            self.data = json.loads(self.path.read_text(encoding="utf-8"))
            self.data.setdefault("domains", {})
            self.data.setdefault("runs", [])

    @property
    def domains(self) -> dict[str, dict]:
        return self.data["domains"]

    def set(self, domain: str, **row) -> None:
        self.domains[domain] = {**self.domains.get(domain, {}), **row, "at": now_utc().isoformat()}
        self.save()

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True), encoding="utf-8")
        os.replace(tmp, self.path)

    def count(self, status: str) -> int:
        return sum(1 for r in self.domains.values() if r.get("status") == status)

    def committed_usd(self) -> float:
        """Money that is or may be gone: bought plus unknown, at the price we searched."""
        return round(sum(float(r.get("price_usd") or 0) for r in self.domains.values()
                         if r.get("status") in ("bought", "unknown")), 2)


def buy(dd, names: list[str], *, target: int, ledger: Ledger, approve: bool,
        price_ceiling: float = PRICE_CEILING, total_ceiling: float = TOTAL_CEILING,
        wait_until: datetime | None = None, poll_seconds: int = 120,
        sleep=time.sleep, clock=now_utc, log=print) -> dict:
    """SPEND. Returns a summary; the ledger holds the per-domain detail."""
    if not approve:
        raise DynadotBlocked("buying spends the balance: pass approve=True after a human confirmed it")
    if not dd.sandbox and not spend_allowed():
        raise DynadotBlocked("DYNADOT_ALLOW_SPEND is not 'true': buying is switched off")
    bad = name_problems(names)
    if bad:
        raise DynadotError(f"{len(bad)} bad names, nothing bought: " +
                           ", ".join(f"{d} ({why})" for d, why in list(bad.items())[:5]))
    started = clock()
    stop = ""
    refusals = 0
    try:
        bal = dd.balance().get("USD")
    except DynadotError as exc:
        bal = None
        log(f"balance unreadable: {exc}")
    log(f"start: target {target}, bought so far {ledger.count('bought')}, balance "
        f"{'unknown' if bal is None else f'${bal:.2f}'}, ceilings ${price_ceiling:.2f}/domain "
        f"${total_ceiling:.2f} total, sandbox={dd.sandbox}")
    if bal is not None and bal < price_ceiling:
        stop = f"balance ${bal:.2f} is below one domain"

    for domain in names:
        if stop or ledger.count("bought") >= target:
            break
        prev = ledger.domains.get(domain) or {}
        if prev.get("status") in ("bought", "skipped"):
            continue
        if prev.get("status") == "unknown":
            try:
                info = dd.owned(domain)
            except DynadotOutcomeUnknown:
                continue                    # still can't tell: leave it, never buy it twice
            if info:
                ledger.set(domain, status="bought", note="found in the account on a later run",
                           expiration_ms=info.get("Expiration"))
                continue
        # price gate (waits for the sale to go live)
        while True:
            try:
                s = dd.search(domain)
            except DynadotOutcomeUnknown as exc:
                stop = f"search failed: {exc}"
                break
            except DynadotRefused as exc:
                s = {"available": False, "error": str(exc)}
            if not s.get("available") or s.get("premium") or s.get("price_usd") is None:
                why = "premium" if s.get("premium") else s.get("error") or (
                    "not available" if not s.get("available") else f"price not in USD: {s.get('price_text')}")
                ledger.set(domain, status="skipped", reason=str(why)[:200])
                log(f"skipped {domain}: {str(why)[:120]}")
                s = None
                break
            if s["price_usd"] <= price_ceiling:
                break
            if wait_until and clock() < wait_until:
                log(f"{domain}: ${s['price_usd']:.2f} is above ${price_ceiling:.2f}; sale not live yet, "
                    f"checking again in {poll_seconds}s")
                sleep(poll_seconds)
                continue
            stop = f"price ${s['price_usd']:.2f} above the ${price_ceiling:.2f} ceiling (sale not live?)"
            break
        if stop or not s:
            continue
        price = s["price_usd"]
        if ledger.committed_usd() + price > total_ceiling + 1e-9:
            stop = f"total ceiling ${total_ceiling:.2f} reached"
            break
        ledger.set(domain, status="unknown", price_usd=price, error="interrupted mid-purchase")
        try:
            r = dd.register(domain, approve=True)
            ledger.set(domain, status="bought", price_usd=price, error=None,
                       expiration_ms=r.get("expiration_ms"))
            refusals = 0
            log(f"bought {domain} ${price:.2f} ({ledger.count('bought')}/{target})")
        except DynadotRefused as exc:
            ledger.set(domain, status="failed", price_usd=price, error=str(exc)[:300])
            log(f"refused {domain}: {exc}")
            if funds_problem(str(exc)):
                stop = f"balance ran out: {exc}"
            refusals += 1
            if refusals >= MAX_REFUSALS_IN_A_ROW and not stop:
                stop = f"{refusals} refusals in a row, last: {exc}"
        except DynadotOutcomeUnknown as exc:
            sleep(10)
            try:
                info = dd.owned(domain)
            except DynadotOutcomeUnknown:
                info = None
            if info:
                ledger.set(domain, status="bought", price_usd=price, error=None,
                           note="no answer to register; found in the account")
            else:
                ledger.set(domain, status="unknown", price_usd=price, error=str(exc)[:300])
                log(f"unknown {domain}: {exc} (counted as spent; the next run looks it up)")

    bought = ledger.count("bought")
    if not stop and bought < target:
        stop = f"ran out of names ({len(names)} in the file)"
    summary = {"target": target, "bought": bought, "unknown": ledger.count("unknown"),
               "failed": ledger.count("failed"), "skipped": ledger.count("skipped"),
               "committed_usd": ledger.committed_usd(), "stopped": stop or "target reached",
               "sandbox": dd.sandbox, "started": started.isoformat(), "ended": clock().isoformat()}
    try:
        summary["balance_after"] = dd.balance().get("USD")
    except DynadotError:
        summary["balance_after"] = None
    ledger.data["runs"].append(summary)
    ledger.save()
    log(f"done: {summary}")
    return summary


def summary_text(s: dict, ledger_path: str) -> str:
    """The Slack DM after a run (plain, short)."""
    head = "Dynadot buy" + (" (SANDBOX, no real money)" if s.get("sandbox") else "")
    bal = s.get("balance_after")
    lines = [f"{head}: {s['bought']} of {s['target']} bought, ${s['committed_usd']:.2f} spent.",
             "",
             f"Stopped because: {s['stopped']}.",
             f"Balance left: {'unknown' if bal is None else f'${bal:.2f}'}. "
             f"Skipped (taken or premium): {s['skipped']}. Refused: {s['failed']}. Unclear: {s['unknown']}."]
    if s.get("unknown"):
        lines += ["", "Unclear ones are counted as spent. Run the same command again: it looks them up "
                      "in the account first and never buys one twice."]
    lines += ["", f"Ledger: {ledger_path}"]
    return "\n".join(lines)
