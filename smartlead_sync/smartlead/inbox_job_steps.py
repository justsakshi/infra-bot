"""The real steps behind inbox_jobs.advance - Zapmail, Smartlead, tracker.

Every step is safe to run again: it first looks at what already exists
(bought domains, existing inboxes, inboxes already in Smartlead) and only does
what is missing. Tests swap this class for a fake; see test_inbox_jobs.py.
"""

from __future__ import annotations

from datetime import date

from smartlead.inbox_jobs import StepOutcome
from smartlead.zapmail import ZapmailHTTPError

GOOGLE_SMTP = ("smtp.gmail.com", 587, "imap.gmail.com", 993)
MICROSOFT_SMTP = ("smtp.office365.com", 587, "outlook.office365.com", 993)


class RealSteps:
    def __init__(self, store) -> None:
        self.store = store

    def save(self, job: dict) -> None:
        self.store.save(job)

    async def cost(self, job: dict) -> dict:
        """Live inputs for inbox_jobs.estimate: prices, free slots, plan. READ-ONLY."""
        from smartlead.domain_availability import check_availability_bulk
        from smartlead.inbox_jobs import estimate
        from smartlead.zapmail_accounts import api_key_for_client, open_client, require_account

        # Nobody named: the client's default senders (inbox_profiles.json), so
        # the person approving sees whose inboxes these will be.
        if not job.get("names") and job["kind"] != "prewarmed":
            from smartlead.inbox_setup import profile_for
            job["names"] = list(profile_for(job["client"])[1].get("default_senders") or [])

        prices: dict[str, float] = {}
        if job["kind"] == "new":
            fresh = await check_availability_bulk(
                job["domains"], api_key=api_key_for_client(job["client"]), use_cache=False,
                max_calls=-(-len(job["domains"]) // 20))
            problems = []
            for d in job["domains"]:
                available, price = fresh.get(d, (None, None))
                if available is not True or price is None:
                    problems.append(f"{d} is {'taken' if available is False else 'not confirmed available'}")
                else:
                    prices[d] = price
            if problems:
                raise ValueError("; ".join(problems))
        needed = None
        if job["kind"] == "owned":
            # The domain decides Google vs Outlook, not the request: an inbox
            # can only be created on the provider the domain lives on.
            from smartlead.zapmail_fleet import locate_domain
            acc = require_account(job["client"]).name
            problems, existing = [], {}
            for d in job["domains"]:
                hits = [h for h in await locate_domain(d) if h.get("account") == acc and not h.get("error")]
                if not hits:
                    problems.append(f"{d} is not on {job['client']}'s Zapmail account")
                    continue
                if hits[0].get("provider") != job["provider"]:
                    want = "Outlook" if hits[0].get("provider") == "MICROSOFT" else "Google"
                    problems.append(f"{d} is {'an' if want == 'Outlook' else 'a'} {want} domain - choose {want}")
                existing[d] = [m["email"] for m in hits[0].get("mailboxes") or []]
                if len(existing[d]) >= 5:
                    problems.append(f"{d} already has 5 inboxes (the most Zapmail allows)")
            if problems:
                raise ValueError("; ".join(problems))
            needed = sum(self._to_create(job, existing).values())
        async with open_client(job["client"], provider=job["provider"]) as z:
            quota = ((await z.list_mailboxes(page=1, limit=1)) or {}).get("data") or {}
            user = ((await z.get_user()) or {}).get("data") or {}
            if job["kind"] == "prewarmed":
                # The inventory price of exactly this domain (still unsold?).
                sale = ((await z.prewarmed_domains(page=1, limit=50, contains=job["domains"][0]))
                        or {}).get("data") or {}
                row = next((x for x in sale.get("domains") or []
                            if str(x.get("id")) == str(job["prewarmed_domain_id"])), None)
                if not row or row.get("isSold"):
                    raise ValueError(f"{job['domains'][0]} is no longer for sale")
                from smartlead.domain_availability import parse_price
                price = parse_price(row.get("price"))
                if price is None:
                    raise ValueError(f"{job['domains'][0]}: Zapmail gave no price")
                job["prewarmed_price"] = price
        return estimate(job, domain_prices=prices,
                        free_slots=int(quota.get("availableMailboxes") or 0),
                        plan=str(user.get("activePlan") or "growth"), needed=needed)

    # ── domains ─────────────────────────────────────────────────────────

    async def buy_domains(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.domain_batch import BatchStore, execute_one, stage_batches

        from smartlead.domain_batch import HOLDING_STATUSES
        from smartlead.zapmail import ZapmailHTTPError
        from smartlead.zapmail_accounts import _norm

        ledger = BatchStore()
        ids = step["data"].get("batch_ids")
        if not ids:
            # A crash after planning but before saving the ids must not look
            # like "already planned by someone else": reuse this client's own
            # batches when they cover every domain of the job.
            wanted = set(job["domains"])
            own = [b for b in ledger.load_all()
                   if b.get("status") in HOLDING_STATUSES
                   and _norm(b.get("client") or "") == _norm(job["client"])
                   and set(b.get("domains") or []) & wanted]
            if own and wanted <= {d for b in own for d in b.get("domains") or []}:
                ids = [b["batch_id"] for b in own]
                step["data"]["batch_ids"] = ids
                self.save(job)
        if not ids:
            res = await stage_batches(job["domains"], client=job["client"], store=ledger,
                                      provider=job["provider"])
            problems = ([f"{d} is taken" for d in res.get("unavailable") or []]
                        + [f"{d}: availability unknown" for d in res.get("unknown") or []]
                        + [f"{d} costs more than ${res.get('price_ceiling')}" for d in res.get("over_ceiling") or []]
                        + [f"{d} is already in another purchase plan" for d in res.get("already_planned") or []])
            if problems:
                return StepOutcome("failed", "; ".join(str(p) for p in problems))
            ids = [b["batch_id"] for b in res.get("batches") or []]
            if not ids:
                return StepOutcome("failed", "nothing could be planned for purchase")
            step["data"]["batch_ids"] = ids
            self.save(job)
        waiting: list[str] = []
        for bid in ids:
            b = ledger.get(bid) or {}
            st = b.get("status")
            if st == "purchased":
                continue
            if st in ("unknown", "partial", "in_progress"):
                return StepOutcome("needs_check", f"purchase {bid} is '{st}': run "
                                   f"zapmail_buy.py --reconcile {bid} before anything else")
            if st == "failed":
                # Zapmail definitively refused it (nothing charged). Say why;
                # a person decides whether to try again, not a loop.
                err = ((b.get("result") or {}).get("error") or "refused by Zapmail")
                return StepOutcome("failed", f"buying {', '.join(b.get('domains') or [])} "
                                   f"failed: {str(err)[:160]} - nothing was charged")
            if st == "planned":
                if b.get("earliest_date") and date.fromisoformat(b["earliest_date"]) > date.today():
                    waiting.append(f"{', '.join(b.get('domains') or [])} on {b['earliest_date']}")
                    continue
                try:
                    await execute_one(bid, approve=True, client=job["client"], store=ledger)
                except ZapmailHTTPError as exc:
                    return StepOutcome("failed", f"Zapmail refused the purchase: {str(exc)[:160]} "
                                       "- nothing was charged")
                continue
            return StepOutcome("needs_check", f"purchase {bid} has unexpected status {st!r}")
        if waiting:
            return StepOutcome("waiting", "staggered buying: " + "; ".join(waiting))
        return StepOutcome("done", f"bought {', '.join(job['domains'])}")

    async def domains_active(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.zapmail_fleet import locate_domain

        pending, wrong = [], []
        for d in job["domains"]:
            hits = [h for h in await locate_domain(d) if not h.get("error")]
            if not hits:
                pending.append(f"{d}: not in Zapmail yet")
                continue
            h = hits[0]
            if h.get("provider") != job["provider"]:
                wrong.append(f"{d} is on {h.get('provider')}, the job asked for {job['provider']}")
            elif str(h.get("status") or "").upper() != "ACTIVE":
                pending.append(f"{d}: {h.get('status')}")
        if wrong:
            return StepOutcome("failed", "; ".join(wrong))
        if pending:
            return StepOutcome("waiting", "registration in progress (usually a few hours): "
                               + "; ".join(pending))
        return StepOutcome("done")

    # ── inboxes ─────────────────────────────────────────────────────────

    async def _existing(self, job: dict) -> dict[str, list[str]]:
        from smartlead.zapmail_fleet import locate_domain
        out: dict[str, list[str]] = {}
        for d in job["domains"]:
            hits = [h for h in await locate_domain(d) if not h.get("error")]
            out[d] = [m["email"] for m in (hits[0].get("mailboxes") if hits else []) or []]
        return out

    def _to_create(self, job: dict, existing: dict[str, list[str]]) -> dict[str, int]:
        mine = {i["email"] for i in job.get("inboxes") or []}
        need = {}
        for d in job["domains"]:
            have_for_job = len([e for e in existing.get(d, []) if e in mine])
            room = max(0, 5 - len(existing.get(d, [])))
            need[d] = min(room, max(0, job["inboxes_per_domain"] - have_for_job))
        return need

    async def inbox_slots(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.zapmail_accounts import open_client

        needed = sum(self._to_create(job, await self._existing(job)).values())
        async with open_client(job["client"], provider=job["provider"]) as z:
            quota = ((await z.list_mailboxes(page=1, limit=1)) or {}).get("data") or {}
            free = int(quota.get("availableMailboxes") or 0)
            if free >= needed:
                return StepOutcome("done", f"{free} free slot(s), {needed} needed", {"free": free})
            if step["data"].get("bought_qty"):
                return StepOutcome("waiting", f"bought {step['data']['bought_qty']} slot(s) from "
                                   f"the wallet; {free} free so far (Zapmail usually adds them within minutes)")
            if step["data"].get("attempted"):
                return StepOutcome("needs_check", "a slot purchase was started but its result "
                                   "was not recorded - check Zapmail billing before retrying")
            qty = needed - free
            # Slots are paid from the wallet only (Zapmail, 2026-09-30): refuse
            # before buying when it cannot cover them, with the amount to add.
            from smartlead.domain_purchase import require_wallet_covers
            from smartlead.inbox_jobs import ADDON_PRICE
            from smartlead.zapmail_accounts import require_account
            plan = str((((await z.get_user()) or {}).get("data") or {}).get("activePlan") or "growth").lower()
            await require_wallet_covers(z, round(qty * ADDON_PRICE.get(plan, ADDON_PRICE["growth"]), 2),
                                        require_account(job["client"]).name)
            step["data"]["attempted"] = True
            self.save(job)
            try:
                resp = await z.buy_addon_mailboxes(qty, approve=True)
            except ZapmailHTTPError:
                step["data"]["attempted"] = False   # definitive refusal: nothing charged
                self.save(job)
                raise
        link = (resp or {}).get("paymentLink")
        return StepOutcome("waiting", f"bought {qty} slot(s) from the wallet; waiting for them to appear",
                           {"bought_qty": qty, "payment_link": link})

    async def create_inboxes(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.domain_lifecycle import assign_mailboxes_and_wait

        existing = await self._existing(job)
        need = self._to_create(job, existing)
        made: list[str] = []
        for d, count in need.items():
            if count <= 0:
                continue
            r = await assign_mailboxes_and_wait(
                d, count=count, approve=True, client=job["client"],
                sender_names=job.get("names") or None, timeout_s=1)
            if r.get("ok") is False and not r.get("created"):
                return StepOutcome("failed", f"{d}: {r.get('error')}")
            for e in r.get("created") or []:
                job["inboxes"].append({"email": e.lower()})
                made.append(e)
            self.save(job)
        return StepOutcome("done", f"requested {len(made)} inbox(es)" if made else "already created")

    async def inboxes_active(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.zapmail_fleet import locate_domain

        wanted = {i["email"] for i in job.get("inboxes") or []}
        if not wanted:
            return StepOutcome("failed", "the job has no inboxes to wait for")
        status: dict[str, str] = {}
        for d in job["domains"]:
            for h in await locate_domain(d):
                for m in h.get("mailboxes") or []:
                    if m["email"] in wanted:
                        status[m["email"]] = str(m.get("status") or "").upper()
        failed = sorted(e for e, s in status.items() if s == "FAILED")
        if failed:
            return StepOutcome("failed", "Zapmail could not create: " + ", ".join(failed)
                               + " - retry them in Zapmail, then resume the job")
        waiting = sorted(e for e in wanted if status.get(e) != "ACTIVE")
        if waiting:
            return StepOutcome("waiting", f"{len(wanted) - len(waiting)}/{len(wanted)} ready "
                               "(Google usually within an hour, Outlook a few hours)")
        return StepOutcome("done", f"{len(wanted)} inbox(es) active")

    # ── pre-warmed ──────────────────────────────────────────────────────

    async def assign_prewarmed(self, job: dict, step: dict) -> StepOutcome:
        """Order the chosen pre-warmed domain (Zapmail, 2026-09-30: no plan needed).

        PAID: Zapmail charges the wallet, or the card on file when the wallet is
        short - so the wallet must cover the price first (no card surprises).
        Called at most once: afterwards we only look for the domain.
        """
        from smartlead.domain_purchase import require_wallet_covers
        from smartlead.zapmail_accounts import open_client, require_account
        from smartlead.zapmail_fleet import locate_domain

        d = job["domains"][0]
        acc = require_account(job["client"]).name
        mine = [h for h in await locate_domain(d) if h.get("account") == acc and not h.get("error")]
        if mine:
            job["inboxes"] = [{"email": m["email"]} for m in mine[0].get("mailboxes") or []]
            if job["inboxes"]:
                return StepOutcome("done", f"{d} with {len(job['inboxes'])} warmed inbox(es)")
            return StepOutcome("waiting", f"{d} is ours; waiting for its inboxes to show")
        if step["data"].get("attempted"):
            return StepOutcome("waiting", f"ordered {d}; waiting for it to appear on {acc}'s account "
                               "(if this lasts over an hour, check the Zapmail app)")
        async with open_client(job["client"], provider=job["provider"]) as z:
            await require_wallet_covers(z, float(job.get("prewarmed_price") or 0), acc)
            step["data"]["attempted"] = True
            self.save(job)
            try:
                resp = await z.assign_prewarmed([job["prewarmed_domain_id"]], approve=True)
            except ZapmailHTTPError:
                step["data"]["attempted"] = False   # definitive refusal: nothing charged
                self.save(job)
                raise
        rows = (resp or {}).get("data") or []
        job["inboxes"] = [{"email": f"{r['username']}@{r['domain']}".lower(),
                           "first": r.get("firstName"), "last": r.get("lastName")}
                          for r in rows if r.get("username") and r.get("domain")]
        if not job["inboxes"]:
            return StepOutcome("waiting", f"ordered {d}; waiting for its inboxes to show")
        return StepOutcome("done", f"{d} with {len(job['inboxes'])} warmed inbox(es)")

    # ── Smartlead ───────────────────────────────────────────────────────

    async def into_smartlead(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.zapmail_accounts import export_target_for_client

        if export_target_for_client(job["client"]):
            from smartlead.domain_export import export_domain
            done = step["data"].setdefault("exported", {})
            for d in job["domains"]:
                if d in done:
                    continue
                r = await export_domain(d, client=job["client"], approve=True, poll=False)
                if not r.get("ok"):
                    return StepOutcome("failed", f"export {d}: {r.get('error')}")
                done[d] = r.get("export_id") or True
                self.save(job)
            return StepOutcome("done", "exported by Zapmail")
        return await self._add_directly(job)

    async def _add_directly(self, job: dict) -> StepOutcome:
        """Option 2: Zapmail cannot export to this client's Smartlead, so add each
        inbox with its own login. Passwords are read and used, never stored."""
        from smartlead.api import SmartleadClient
        from smartlead.inbox_setup import profile_for, smartlead_key_for
        from smartlead.zapmail_accounts import open_client

        _, prof = profile_for(job["client"])
        acc_name, key = smartlead_key_for(prof)
        host = GOOGLE_SMTP if job["provider"] == "GOOGLE" else MICROSOFT_SMTP
        wanted = {i["email"] for i in job.get("inboxes") or []}
        async with SmartleadClient(key, account_name=acc_name) as sl:
            present = {str(a.get("from_email") or "").lower() for a in await sl.list_email_accounts()}
            missing = sorted(wanted - present)
            if not missing:
                return StepOutcome("done", "already in Smartlead")
            async with open_client(job["client"], provider=job["provider"]) as z:
                creds: dict[str, dict] = {}
                for d in job["domains"]:
                    listing = await z.list_mailboxes(contains=d, page=1, limit=50)
                    for dom in ((listing or {}).get("data") or {}).get("domains") or []:
                        for m in dom.get("mailboxes") or []:
                            creds[str(m.get("email") or "").lower()] = m
            failed = []
            for email in missing:
                m = creds.get(email)
                secret = (m or {}).get("appPassword") if job["provider"] == "GOOGLE" else (m or {}).get("password")
                if not m or not secret:
                    failed.append(f"{email}: Zapmail gave no {'app password' if job['provider'] == 'GOOGLE' else 'password'}")
                    continue
                try:
                    await sl.save_email_account({
                        "from_name": f"{m.get('firstName', '')} {m.get('lastName', '')}".strip() or email,
                        "from_email": email, "user_name": email,
                        "password": str(secret).replace(" ", ""),
                        "smtp_host": host[0], "smtp_port": host[1],
                        "imap_host": host[2], "imap_port": host[3],
                        "warmup_enabled": True, "max_email_per_day": 20})
                except Exception as exc:  # noqa: BLE001
                    failed.append(f"{email}: {str(exc)[:120]}")
        if failed:
            hint = (" - Outlook often blocks this sign-in; add these in Smartlead with "
                    "'Connect Microsoft' instead") if job["provider"] == "MICROSOFT" else ""
            return StepOutcome("failed", "; ".join(failed) + hint)
        return StepOutcome("done", f"added {len(missing)} inbox(es) to Smartlead")

    async def smartlead_setup(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.inbox_setup import apply_changes, plan_for, profile_for

        emails = [i["email"] for i in job.get("inboxes") or []]
        _, prof = profile_for(job["client"])
        use_sig = bool(job.get("signature")) and bool(prof.get("signature"))
        _, changes = await plan_for(job["client"], emails=emails, set_signature=use_sig, new_inbox=True)
        absent = [c.email for c in changes if c.error and "not in this client's Smartlead" in c.error]
        if absent:
            return StepOutcome("waiting", f"waiting for Smartlead to show {len(absent)} inbox(es)")
        bad = [f"{c.email}: {c.error}" for c in changes if c.error]
        if bad:
            return StepOutcome("failed", "; ".join(bad))
        results = await apply_changes(job["client"], changes, approve=True)
        errs = [f"{r['email']}: {r.get('error')}" for r in results if not r.get("ok")]
        if errs:
            return StepOutcome("failed", "; ".join(errs))
        note = "" if use_sig or not job.get("signature") else " (no signature template yet - add one and use Name & signature)"
        return StepOutcome("done", f"{len(emails)} inbox(es) set up" + note)

    async def tracker(self, job: dict, step: dict) -> StepOutcome:
        from smartlead.zapmail_asset_sync import sync
        written = 0
        for d in job["domains"]:
            r = await sync(apply_changes=True, domain=d)
            written += int(r.get("applied") or 0)
        return StepOutcome("done", f"{written} tracker row(s) written")
