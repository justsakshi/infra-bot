"""Weekly placement report: who lands in spam, from which provider to which,
and whether those inboxes are attached to a campaign that is really sending.

Pure functions only, so the numbers can be tested without Smartlead.

Receiving provider comes from the SEED's MX record. SmartDelivery only gives
the Google/Microsoft split for a whole test, never per sender; resolving each
seed domain's MX reproduces that split exactly (verified 2026-10-06 on test
547669: 126 inbox / 54 spam at Google, 381 / 47 at Microsoft).
"""
from __future__ import annotations

import subprocess
from collections import defaultdict

SPAM_FOLDERS = {"spam", "junk"}
GOOD_AT = 80.0     # a mailbox at or above this inbox rate lands in the inbox; below it, spam
BAD_BELOW = 50.0   # below this the mailbox is "bad" (worst of the spam group)

_PROVIDER = {"GMAIL": "Google", "OUTLOOK": "Outlook", "SMTP": "SMTP"}


def mx_provider(domain: str) -> str:
    """Google / Outlook / other, from the domain's MX records."""
    try:
        out = subprocess.run(["nslookup", "-type=MX", domain], capture_output=True,
                             text=True, timeout=20).stdout.lower()
    except Exception:
        return "other"
    if "protection.outlook.com" in out:
        return "Outlook"
    if "google" in out or "googlemail" in out:
        return "Google"
    return "other"


def sender_provider(account_type: str) -> str:
    return _PROVIDER.get(str(account_type or "").upper(), "other")


def status_for(pct: float | None) -> str:
    if pct is None:
        return "untested"
    if pct >= GOOD_AT:
        return "good"
    if pct >= BAD_BELOW:
        return "watch"
    return "bad"


def active_campaign(analytics: dict) -> bool:
    """A campaign is 'active' when it is running and still has leads to send:
    status ACTIVE and Leads Not Started + Leads In Progress > 0."""
    if str(analytics.get("status", "")).upper() != "ACTIVE":
        return False
    st = analytics.get("campaign_lead_stats") or {}
    return int(st.get("notStarted") or 0) + int(st.get("inprogress") or 0) > 0


def domain_verdict(n_mailboxes: int, n_spam: int) -> str:
    """The team's rule (Anjali, 2026-10-06), per domain:

    - no mailbox in spam                      -> good
    - fewer than half the mailboxes in spam   -> avoid_new: still counts as inbox,
      but keep it out of NEW campaigns (its reputation drops within 2-3 days)
    - half or more in spam                    -> spam (close to 50% is spam)
    """
    if n_spam == 0:
        return "good"
    return "avoid_new" if n_spam * 2 < n_mailboxes else "spam"


def build_report(senders: dict[str, list[dict]], type_by_email: dict[str, str],
                 seed_provider, attached: dict[str, list[str]] | None = None) -> dict:
    """senders: {email: [{"seed": address, "folder": str}, ...]}.

    seed_provider(domain) -> 'Google' | 'Outlook' | 'other'.
    attached: {email: [active campaign names]}.
    """
    attached = attached or {}
    inboxes: dict[str, dict] = {}
    pair = defaultdict(lambda: {"seeds": 0, "spam": 0, "inboxes_hit": set()})
    for email, rows in senders.items():
        email = email.lower()
        sp = sender_provider(type_by_email.get(email))
        got = spam = 0
        recv = defaultdict(lambda: [0, 0])      # receiver provider -> [seeds, spam]
        for r in rows:
            folder = str(r.get("folder", "")).strip().lower()
            if not folder:
                continue                          # not classified yet: unmeasured, not failing
            is_spam = folder in SPAM_FOLDERS
            rp = seed_provider(r["seed"].split("@")[1].lower())
            got += 1
            spam += is_spam
            recv[rp][0] += 1
            recv[rp][1] += is_spam
            p = pair[(sp, rp)]
            p["seeds"] += 1
            p["spam"] += is_spam
            if is_spam:
                p["inboxes_hit"].add(email)
        pct = round(100.0 * (got - spam) / got, 1) if got else None
        inboxes[email] = {"email": email, "domain": email.split("@")[1], "sender": sp,
                          "seeds": got, "spam": spam, "pct": pct, "status": status_for(pct),
                          "campaigns": attached.get(email, []),
                          "by_receiver": {k: {"seeds": v[0], "spam": v[1]} for k, v in recv.items()}}

    domains: dict[str, dict] = {}
    for i in inboxes.values():
        d = domains.setdefault(i["domain"], {"domain": i["domain"], "sender": i["sender"],
                                              "seeds": 0, "spam": 0, "inboxes": 0,
                                              "mailboxes_spam": 0, "emails": []})
        d["seeds"] += i["seeds"]
        d["spam"] += i["spam"]
        d["inboxes"] += 1
        d["emails"].append(i["email"])
        d["mailboxes_spam"] += i["status"] in ("watch", "bad")
    for d in domains.values():
        d["pct"] = round(100.0 * (d["seeds"] - d["spam"]) / d["seeds"], 1) if d["seeds"] else None
        d["verdict"] = domain_verdict(d["inboxes"], d["mailboxes_spam"])
        d["status"] = {"good": "good", "avoid_new": "watch", "spam": "bad"}[d["verdict"]]
        # The grid has one cell per receiving provider (G suite / Outlook).
        d["cells"] = {}
        for rp in ("Google", "Outlook"):
            per = [inboxes[e]["by_receiver"][rp] for e in d["emails"] if rp in inboxes[e]["by_receiver"]]
            if not per:
                continue
            bad = sum(1 for v in per if v["seeds"] and 100.0 * (v["seeds"] - v["spam"]) / v["seeds"] < GOOD_AT)
            d["cells"][rp] = "spam" if bad * 2 >= len(per) else "Inbox"

    return {"inboxes": inboxes, "domains": domains,
            "pairs": {k: {"seeds": v["seeds"], "spam": v["spam"],
                          "inboxes_hit": len(v["inboxes_hit"])} for k, v in pair.items()}}


def counts(items: dict) -> dict:
    out = {"good": 0, "watch": 0, "bad": 0, "untested": 0}
    for v in items.values():
        out[v["status"]] += 1
    return out


PAIR_ORDER = [("Google", "Google"), ("Google", "Outlook"),
              ("Outlook", "Outlook"), ("Outlook", "Google")]


def format_slack(client: str, report: dict, test_names: list[str]) -> str:
    ib, dm = report["inboxes"], report["domains"]
    ci, cd = counts(ib), counts(dm)
    lines = [f"*Deliverability test - {client}* ({', '.join(test_names)})",
             f"Inboxes tested {len(ib)}: *{ci['good']} in inbox* (80%+), "
             f"*{ci['watch'] + ci['bad']} in spam* (under 80%; {ci['bad']} of them under 50%)" + (f", {ci['untested']} no data" if ci["untested"] else ""),
             f"Domains tested {len(dm)}: *{cd['good']} good*, {cd['watch']} keep out of NEW campaigns "
             f"(some mailboxes in spam), *{cd['bad']} spam* (half or more mailboxes in spam)",
             "", "*Spam by sender -> receiver*"]
    for sp, rp in PAIR_ORDER:
        p = report["pairs"].get((sp, rp))
        if not p:
            continue
        pct = 100.0 * p["spam"] / p["seeds"] if p["seeds"] else 0
        lines.append(f"- {sp} -> {rp}: {p['spam']}/{p['seeds']} in spam ({pct:.0f}%), "
                     f"{p['inboxes_hit']} inbox(es) affected")
    flagged = sorted((i for i in ib.values() if i["status"] in ("watch", "bad")),
                     key=lambda i: (i["pct"] if i["pct"] is not None else 101))
    lines += ["", f"*Flagged inboxes ({len(flagged)})* - :warning: = attached to an active campaign"]
    for i in flagged[:40]:
        mark = f" :warning: {', '.join(i['campaigns'])}" if i["campaigns"] else ""
        lines.append(f"- `{i['status']}` {i['email']} - {i['pct']:.0f}% inbox ({i['sender']}){mark}")
    if len(flagged) > 40:
        lines.append(f"...and {len(flagged) - 40} more")
    live = [i for i in flagged if i["campaigns"]]
    lines += ["", f"*Flagged AND sending now: {len(live)} inbox(es)*" +
              (" - consider pulling them from the campaigns above." if live else " - none.")]
    return "\n".join(lines)


def inbox_rows(report: dict, client_by_email: dict[str, str] | None = None,
               tested_on: str = "") -> list[list]:
    """One row per inbox for the Campaign Desk tab. `Usable` is the column it reads:
    YES (use anywhere), EXISTING (domain has a mailbox in spam: keep out of new
    campaigns), NO (mailbox or domain in spam)."""
    client_by_email = client_by_email or {}
    rows = []
    for i in sorted(report["inboxes"].values(), key=lambda x: (x["domain"], x["email"])):
        d = report["domains"][i["domain"]]
        in_inbox = i["status"] == "good"
        if in_inbox and d["verdict"] == "good":
            usable = "YES"
        elif in_inbox and d["verdict"] == "avoid_new":
            usable = "EXISTING"
        else:
            usable = "NO"
        g = i["by_receiver"].get("Google")
        o = i["by_receiver"].get("Outlook")
        pct = lambda v: round(100.0 * (v["seeds"] - v["spam"]) / v["seeds"]) if v and v["seeds"] else ""
        rows.append([i["email"], i["domain"], client_by_email.get(i["email"], ""), i["sender"],
                     "" if i["pct"] is None else round(i["pct"]), pct(g), pct(o),
                     "Inbox" if in_inbox else "Spam", d["verdict"], usable, tested_on,
                     ", ".join(i["campaigns"])])
    return rows


INBOX_HEADER = ["Email", "Domain", "Client", "Sender type", "Inbox %", "Inbox % at Google",
                "Inbox % at Outlook", "Mailbox verdict", "Domain verdict", "Usable",
                "Tested on", "In active campaign"]
