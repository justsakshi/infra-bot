# Placement test findings — 2026-07-27

Four EmailGuard seed-list tests, sent from our REAL production mailboxes through
each inbox's own Smartlead account. 8 seeds per test (4 Google, 4 Microsoft).

## Results

| Domain | Mail hosting | Google seeds | Microsoft seeds | Verdict |
|---|---|---|---|---|
| heybelardiwong.com | Google Workspace | **4/4 inbox** | 0/4 spam | Healthy at Google |
| realbelardiwong.com | Google Workspace | **4/4 inbox** | 0/4 spam | Healthy at Google |
| reachbw.com | Microsoft 365 | 0/4 spam | 0/4 spam | **Dead everywhere** |
| bwdirectmail.com | Microsoft 365 | 0/4 spam | 0/4 spam | **Dead everywhere** |

Google-hosted: 8/8 inbox at Google. Microsoft-hosted: 0/16 anywhere.
Two domains per class, perfectly consistent.

## What this overturns

**The retirement list was backwards.** heybelardiwong.com and bwdirectmail.com
were both slated for retirement based on Anjali's 2026-07-09 manual sweep.
heybelardiwong is in fact one of the healthier domains — it inboxes 4/4 at
Google. bwdirectmail genuinely is dead, but for a reason that has nothing to do
with the domain itself.

**Anjali's July-9 rankings do not describe our production senders.** Those tests
were sent from `zachary.riegle@` / `zacharyr@` mailboxes that exist in none of
our four Smartlead accounts. The variance she measured was between those
senders. Two domains her sweep rated very differently (54% vs 100%) test
identically today from our own mailboxes.

**Provider-switching is not a remedy.** Moving Microsoft-heavy prospects onto
Microsoft-hosted inboxes — the intuitive fix for "we fail at Microsoft" — would
have traded "half the ecosystem works" for "none of it works."

## What does NOT explain it

- **Authentication**: SPF, DKIM and DMARC valid and aligned on all four.
  Google-hosted use `_spf.google.com` + `google` selector; Microsoft-hosted use
  `spf.protection.outlook.com` + `selector1/2`. `p=reject` in place.
- **Blacklists**: 0 of 121 fleet domains on Spamhaus DBL (verified with a
  working checker — see the DBL fix in this repo's history).
- **Warmup**: 100% reputation, zero spam landings fleet-wide.
- **Sending history**: realbelardiwong has 712 sends, 0.56% bounce and a 4.49%
  reply rate — genuinely good engagement — and still scores 0/4 at Microsoft.
  reachbw.com has only 8 sends (our own tests), so it is unproven rather than
  burned, and fails anyway.

## The structural finding

**68 of 97 sending domains resolve to a single shared A-record IP:
`52.15.49.97`** — across all four clients, regardless of mail host. This is the
domain provider's redirect/parking infrastructure. The IP itself is clean on
blacklists, so this is not a listing; it is a fingerprint that lets any filter
recognise these domains as one group.

Combined with the brand-permutation naming pattern (`heybelardiwong`,
`getbelardiwong`, `trybelardiwong`, `reachbw`, `sendbw`…) and 99 of 121 domains
listed on SURBL ABUSE, the fleet is trivially identifiable as bulk cold-email
infrastructure.

## Open questions

1. Why do Microsoft-365-hosted domains score zero at **Google** as well? That is
   the most surprising result and is not explained by anything measured so far.
2. Does the shared A-record IP causally contribute, or is it only correlated?
   Testing a domain with a distinct IP would answer this.
3. Would a domain masking proxy (distinct IP per domain) change placement?

## Method note

Every result above was produced by sending from the actual production mailbox
through its own Smartlead account — no imports, no proxy senders. That is why
these numbers can be trusted where the July-9 sweep cannot.
