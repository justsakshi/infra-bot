# DNS request — DMARC reporting + Google Postmaster verification

**Date:** 2026-07-27 · **Scope:** 97 sending domains across Belardi Wong, DARLEAN, MYTHIC, PRECISE_LEADS
**Full per-domain list:** `dmarc_rua_audit.txt` (domain → client → current rua address)

Two changes, both TXT-record only. No MX, SPF, or DKIM changes — sending is unaffected.

---

## Request 1 — point DMARC aggregate reports somewhere we can read

**What's wrong today:** every domain publishes a `rua=` address, so Google and Microsoft
have been generating daily DMARC aggregate reports for months. Nobody receives them.

- **54 of 97 domains** send reports to a mailbox that **does not exist in any Smartlead
  account we operate** — e.g. `heybelardiwong.com` → `zachary.riegle@heybelardiwong.com`,
  and 20+ Darlean domains → `adam.pitts@…` / `dejan.lukic@…`. Those reports bounce or
  vanish.
- The other 43 land in real sending mailboxes that no human or system reads, and
  Smartlead's API exposes only campaign replies, so we cannot read them programmatically.

**What we need:** change the `rua=` value on all 97 domains to a single collector address
(we will provide the exact address — either our DMARC vendor's collector or one mailbox we
control). Everything else in the record stays exactly as-is.

Example, `heybelardiwong.com` — only the `rua=` value changes:

```
before:  v=DMARC1; p=reject; rua=mailto:zachary.riegle@heybelardiwong.com; ruf=mailto:zachary.riegle@heybelardiwong.com; fo=0; pct=100; rf=afrf; ri=604800
after:   v=DMARC1; p=reject; rua=mailto:<COLLECTOR@our-domain>; ruf=mailto:<COLLECTOR@our-domain>; fo=0; pct=100; rf=afrf; ri=604800
```

**Why it matters:** DMARC aggregate reports are the earliest deliverability warning
available — they show, per receiving provider, how much of our mail authenticates and
whether anyone is sending as us. We currently have zero visibility into this. It is also
the only per-provider authentication feedback Microsoft gives us, and we have a confirmed
Microsoft placement problem (see Request 3 context below).

## Request 2 — Google Postmaster Tools verification

Add one TXT record per domain (verification string differs per domain; we will supply the
full list once the Postmaster project is created). This unlocks Google's own view of each
domain: reputation rating, spam rate, and authentication success rates — free, official,
and pollable by API.

If bulk TXT addition is easy on your side, we would like this for all 97 domains. If it is
manual per-domain, start with the 22 Belardi Wong domains and we will prioritise the rest.

---

## Context / questions (no action needed, but useful answers)

1. **Who are `zachary.riegle@`, `zacharyr@`, `adam.pitts@`, `dejan.lukic@`?**
   These addresses appear in DMARC records across dozens of our domains but exist in none
   of our four Smartlead accounts. Were they provisioned and later removed, or are they
   template defaults? If mail is being delivered to them somewhere, we would like access.

2. **SURBL listings.** 99 of our 121 checked domains are listed on SURBL (ABUSE category);
   0 are on Spamhaus DBL. Our domains span Google, GoDaddy and Cloudflare nameservers, so
   this is not a single-registrar artifact. Have you seen fleet-wide SURBL ABUSE listings
   on similar domain sets, and do you handle bulk delisting requests?

3. **Microsoft placement.** We have a domain that inboxes 4/4 on Google seed addresses and
   is spam-foldered 4/4 on Microsoft, with correct SPF/DKIM alignment and `p=reject` in
   place. If you offer any Microsoft-side reputation support (SNDS/JMRP registration on
   the sending infrastructure you control), we would like to discuss it.
