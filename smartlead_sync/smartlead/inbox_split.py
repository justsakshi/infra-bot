"""Per-client split of branded vs pre-warmed inboxes (2026-10-14). READ-ONLY.

Pre-warmed = a Zapmail domain flagged ``isWarmedUp`` (live 14 Oct: the flagged
domains hold exactly our 27 pre-warmed slots). ScaledMail inboxes are branded.
Whose: the usual client rules (zapmail_clients.infer_client, ScaledMail's
order client), else the Smartlead account the domain's inboxes sit in
(BettrData's account -> Bettrdata; Precise Leads' account -> Melior when filed
under Melior's Smartlead client, else Precise Leads). Unknown stays "no client".
"""
from __future__ import annotations

from collections import defaultdict

NO_CLIENT = "no client"


def client_of_account(account_name: str, client_id, melior_id) -> str:
    if account_name.upper() == "BETTRDATA":
        return "Bettrdata"
    if melior_id is not None and client_id == melior_id:
        return "Melior"
    return "Precise Leads"


def split(zap_rows: list[tuple[str, dict]], sm_domains: list[dict], where: dict[str, str], infer) -> dict:
    """{client: {branded, prewarmed}} from Zapmail (account, domain row) pairs,
    ScaledMail domains ({domain, client, mailboxes}) and domain -> client by
    Smartlead account. ``infer(domain, account)`` = zapmail_clients.infer_client. Pure."""
    out: dict[str, dict] = defaultdict(lambda: {"branded": 0, "prewarmed": 0, "domains": 0})
    for account, r in zap_rows:
        n = int(r.get("assignedMailboxesCount") or 0)
        if not n:
            continue
        dom = str(r.get("domain") or "").lower()
        client = infer(dom, account) or where.get(dom) or NO_CLIENT
        out[client]["prewarmed" if r.get("isWarmedUp") else "branded"] += n
        out[client]["domains"] += 1
    for d in sm_domains:
        n = int(d.get("mailboxes") or 0)
        if n:
            out[d.get("client") or NO_CLIENT]["branded"] += n
            out[d.get("client") or NO_CLIENT]["domains"] += 1
    return dict(out)


def split_line(counts: dict) -> str:
    """'By client: Melior 155 branded on 15 domains · Bettrdata 45 branded + 27 pre-warmed on 24 domains · …',
    biggest first, 'no client' last. Domains = ones with inboxes."""
    def part(client, c):
        bits = ([f"{c['branded']} branded"] if c["branded"] else []) + \
               ([f"{c['prewarmed']} pre-warmed"] if c["prewarmed"] else [])
        doms = c.get("domains") or 0
        return f"{client} {' + '.join(bits)}" + (f" on {doms} domain{'s' if doms != 1 else ''}" if doms else "")
    order = sorted(counts.items(), key=lambda kv: (kv[0] == NO_CLIENT, -(kv[1]["branded"] + kv[1]["prewarmed"]), kv[0]))
    return "By client: " + " · ".join(part(cl, c) for cl, c in order if c["branded"] or c["prewarmed"])


async def collect(errors: list[str]) -> dict:
    """Live counts: Zapmail domains (each once), ScaledMail domains, and the
    Smartlead account of any Zapmail domain the client rules can't place."""
    import httpx
    from smartlead import zapmail_fleet as zf
    from smartlead.accounts import discover_accounts
    from smartlead.inbox_setup import profile_for
    from smartlead.zapmail_clients import infer_client

    zap: list[tuple[str, dict]] = []
    for acc in zf.discover_zapmail_accounts():
        seen: set = set()
        try:
            async with zf._client(acc, "GOOGLE") as z:   # the list ignores the provider header
                page = 1
                while page <= 50:
                    d = ((await z.list_domains(page=page, limit=100)) or {}).get("data") or {}
                    rows = d.get("domains") or []
                    zap += [(acc.name, r) for r in rows if r.get("id") not in seen and not seen.add(r.get("id"))]
                    if len(rows) < 100:
                        break
                    page += 1
        except zf.ZapmailError as exc:
            errors.append(f"Zapmail {acc.name}: {str(exc)[:120]}")
    sm: list[dict] = []
    try:
        from smartlead.scaledmail import ScaledMailClient, configured
        if configured():
            from smartlead.scaledmail_fleet import snapshot
            with ScaledMailClient() as c:
                sm = [{"domain": d["domain"], "client": d.get("client"), "mailboxes": d.get("mailboxes")}
                      for d in snapshot(c)["domains"]]
    except Exception as exc:  # noqa: BLE001
        errors.append(f"ScaledMail: {str(exc)[:120]}")
    unplaced = {str(r.get("domain") or "").lower() for a, r in zap
                if int(r.get("assignedMailboxesCount") or 0) and not infer_client(str(r.get("domain") or "").lower(), a)}
    where: dict[str, str] = {}
    if unplaced:
        melior_id = profile_for("Melior")[1].get("smartlead_client_id")
        for acc in discover_accounts():
            if acc.name.upper() not in ("PRECISE_LEADS", "BETTRDATA"):
                continue
            off = 0
            try:
                while True:
                    page = httpx.get("https://server.smartlead.ai/api/v1/email-accounts/", timeout=60,
                                     params={"api_key": acc.api_key, "offset": off, "limit": 100}).json()
                    for a in page:
                        dom = str(a.get("from_email") or "").lower().split("@")[-1]
                        if dom in unplaced:
                            where[dom] = client_of_account(acc.name, a.get("client_id"), melior_id)
                    if len(page) < 100:
                        break
                    off += 100
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Smartlead {acc.name}: {type(exc).__name__}")
    return split(zap, sm, where, infer_client)
