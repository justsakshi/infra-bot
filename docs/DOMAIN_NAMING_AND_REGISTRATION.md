# Domain Naming & Registration Playbook (2026)

Companion to `domain_generator.py`. This is the *why*; the script is the *how*.

Research window: sources current as of 2026-08-19 (Spamhaus/SURBL guidance,
Scaled Mail + Email Bison operator commentary, Smartlead/Mailshake/Hunter
secondary-domain guides, r/Emailmarketing + r/SaaS discussion).

---

## 1. What actually changed

The old playbook was: buy 10 domains that are permutations of the client's
brand (`getbrand.com`, `trybrand.com`, `brandhq.com`), all at one registrar,
all on the same afternoon.

That pattern is now a fingerprint. Spamhaus/SURBL began clustering domain
BATCHES that are too similar to one another and registered too close together.
Operator estimates put current impact at roughly **5-10% of senders**, with the
trend tightening. Being in that 5% is not a gradual degradation — it is the
whole batch going dark at once.

Two letters of difference between domains is enough to cluster them.

## 2. Naming rules

**Never send from the main domain.** A blacklisting cascades into support,
renewals, invoicing, and normal sales mail. This is not negotiable and is not
what the rest of this document is about.

**Build names from the client's vocabulary, not the client's brand.**

The inversion that matters: the main domain is used *only to reject*
candidates, never to build them. Instead of permuting `bettrdata`, build from
what the product does and the problem it solves — `dataingest`, `ingestaccuracy`,
`coveragepipeline`. Each reads as a plausible standalone company to a recipient
scanning a From: header, and none of them cluster with the others or with the
main domain.

**Generic nouns inside the brand are still usable.** `bettrdata.io` contains
`data`, but `data` identifies nobody — thousands of senders use it, so a
recipient cannot read it as "same sender". The distinctive half (`bettr`) is
the identity and never reappears. So `dataingest.com` is fine and
`bettringest.com` is not. Same for `preciseleads.in`: `leads` is usable,
`precise` is not. A brand with no generic half (`melior`, `belardiwong`) has
nothing to reuse, so its domains are pure keyword names.

| Rejected | Why |
|---|---|
| `gobettrdata.com` | affix + brand stem — the classic batch shape |
| `bettrdatahq.com` | brand stem + affix, same cluster |
| `bettrdta.com` | near-miss misspelling — reads as typosquatting |
| `bettringest.com` | reuses `bettr`, the distinctive half of the brand |
| `precisesignal.com` (for preciseleads.in) | reuses `precise`, the distinctive half |
| `br4nd.com` | digit-for-letter substitution — phishing shape |
| `data-ingest.com` | hyphens read as phishing/bulk |
| `datablast.com` | bulk-mail vocabulary |
| `dataingest.xyz` | cheap TLD carries a spam prior no warmup undoes |

The stem-fragment rule is the subtle one and the reason a plain
string-similarity check is not enough. `bettringest` scores low against
`bettrdata` on raw similarity, yet re-exposes the brand outright. The
generator matches the distinctive half against the candidate string itself,
so it is caught whether or not anyone typed `bettr` as a vocabulary word, and
it reports which of your words were skipped so you know why.

**TLD: `.com` only.** Recipients and filters both pattern-match on TLD.
`.xyz`, `.click`, `.top`, `.info`, `.buzz` are disproportionately spam.

**Length: 6-20 characters.** Shorter reads as a real brand.

## 3. Price ceiling

Real-word `.com` compounds are frequently premium — `dataquality.com` either
does not exist as an available name or costs four figures. Sending domains are
**consumables**, retired every 12-18 months, so premium pricing never pays back.
The generator's default ceiling is **$25/yr**; anything above that is rejected
automatically.

Expect to generate a large surplus. Most passing names will be taken.

## 4. Registration: spread and stagger

Buying N similar domains at one registrar on one day is the correlation list
operators look for. Remove the correlation without changing what you own:

- **Multiple registrars.** Use different accounts where practical.
  **Caveat:** buying everything inside Zapmail is operationally simplest but
  puts every domain behind one registrar — exactly the correlation this rule
  exists to break. Pass `--registrars Zapmail,Namecheap,Porkbun` once separate
  registrar accounts exist. Domains bought outside Zapmail need their NS
  pointed at Zapmail before connecting.
- **Stagger purchases across days.** Default plan: 3 domains per registrar per
  batch, 2 days between batches.
- `domain_generator.py` prints this schedule. Purchasing stays manual by
  design — a script that both picks and buys turns a naming mistake into a
  billing mistake.

## 5. Check blacklist history BEFORE buying

A domain someone else burned can be re-registered and arrives already listed.
The generator checks every available candidate against Spamhaus DBL, SURBL, and
URIBL before it reaches the purchase plan, reusing the DNSBL logic in
`blacklist_monitor.py` (control-domain probing, authoritative-NS fallback when
public resolvers are blocked).

Note the asymmetry already documented in `blacklist_monitor.py`: a **Spamhaus
DBL** listing genuinely suppresses inbox placement and is rare. **SURBL ABUSE**
hits a large share of outreach domains, reflects URLs inside message bodies
rather than sender reputation, and is not on its own a reason to reject a
domain. The generator surfaces both; treat DBL as disqualifying.

## 6. Redirects and forwarding

Blacklist providers inspect where a domain redirects, which can associate every
outreach domain with the same destination — reconstructing the cluster you just
spent effort avoiding. In order of preference:

1. **Domain masking** — bots resolve a different IP/site than a real visitor.
2. **Cloudflare CAPTCHA layer** in front of the redirect, blocking automated
   scanning of the destination.
3. **No forwarding at all** — safest, least practical.

Registrar concentration is itself visible. Moving DNS to Cloudflare changes the
visible footprint away from the original registrar.

## 7. After purchase

1. Point NS at Zapmail.
2. Wait 15-20 min for DNS propagation.
3. Connect domains on Zapmail; poll until assignable.
4. Create 2 inboxes per domain.
5. **Warm 2-3 weeks before the first cold send.** Non-negotiable.
6. Sending limits: 10-20 cold emails per mailbox per day, spread across a 9-5
   window with randomized intervals. ~3 mailboxes x 20 = 60/day per domain is
   safe; under 200 is acceptable.
7. Warmup ratio: 2 warmup emails per 1 cold email (1:1 also workable).
8. Cap outreach at 2-3 emails per recipient organization per day.

## 8. Ongoing monitoring

Track daily, at account and domain level:

- **Reply rate** — primary signal; a drop indicates degradation.
- **Bounce rate** — rising bounces signal server-level blocking.
- **Warmup score** — treat with skepticism; providers inflate it.

Monthly: inbox placement tests and blacklist checks (`blacklist_monitor.py`
already runs weekly).

**Recovery rule:** put a degraded account on warmup-only for 2-3 weeks. If it
does not recover, retire and replace.

---

## Usage

```bash
python3 domain_generator.py \
    --client "Bettrdata" \
    --main-domain bettrdata.io \
    --value data,ingest,coherence \
    --problem accuracy,coverage,resolve \
    --need 10
```

Add `--no-network` for naming rules only, `--show-rejects` to audit what was
rejected and why.

Availability and pricing come from Zapmail
(`POST /api/v2/domains/available`, returns `domainPrice` + `renewPrice`), so
the whole flow runs on `ZAPMAIL_API_KEY` — no separate registrar API needed.
API access requires Zapmail's Pro plan.

**Rate limit is the binding constraint:** Zapmail allows **10 domain searches
per 30 minutes**, and one search covers one name. That is the real ceiling on
throughput. The tool caches results on disk for 7 days, reports how many calls
a run will cost before spending them, and stops cleanly at the budget — re-run
later and cached results carry over. `--max-calls` overrides the per-run
budget.

---

## Appendix: picking words, per client

Word choice decides the whole result. Three constraints pull against each
other, and the middle ground is narrow:

1. **Not the client's distinctive brand word.** Rejected automatically.
2. **Not a generic category word.** `smart`, `search`, `talent`, `business`,
   `data` on its own — every two-word `.com` built from these was registered
   years ago. Measured 2026-08-20: `smart,talent,search` for Melior returned
   **zero** available names.
3. **Still has to read like a company** in a From: header. `benchslate.com`
   does; `xk4ventures.com` does not.

Two categories reliably land in the middle:

- **Outcome words** — what the client actually delivers: `placed`, `retained`,
  `booked`, `sourced`, `resolved`.
- **Niche words** — who they serve or what they touch: `dental`, `hvac`,
  `catalog`, `jobsite`, `household`.

Both beat activity and category words, which are exhausted.

**Use 5-6 words, not 3.** The generator builds two-word compounds, so *n*
words yield *n × (n−1)* candidates: 3 words → 6 names, 6 words → 30. Most are
taken, so a thin vocabulary produces nothing.

### Starting points

Verified against each client's existing domains. Adjust as you learn what
survives — these are starting points, not answers.

| Client | Main domain | Try |
|---|---|---|
| Bettrdata | `bettrdata.io` | `data,ingest,coherence,accuracy,coverage,resolve` |
| Melior | `melior.com` | `placed,retained,shortlist,slate,bench,sourced` |
| Belardiwong | `belardiwong.com` | `catalog,mailer,response,retention,lifetime,household` |
| Darlean | — | `dispatch,route,technician,jobsite,uptime,fleet` |
| Mythic | — | `steward,continuity,coverage,region,capacity` |

Darlean and Mythic are inferred from campaign material rather than confirmed
with the account owner; correct them before spending a Zapmail budget on
them. Creflo, Incubeta, Innovint and the Srivatsan accounts have no entry yet.

### What the estate looks like today

Every client except OSC was built on the pattern this document warns about —
one stem, many affixes, bought in batches:

```
Bettrdata     run / go / hey / send / reach / use / true / best / real /
              try / one / top / my / live / open  + bettrdata     (15 domains)
Melior        go / smart / launch / join / lead / mail / run / find /
              grow / ask / hub / build / call     + melior        (23 domains)
Belardiwong   connect / dm / go / mail / team / try / meet / base / hq /
              hub / zone / get / join / reach     + belardiwong   (28 domains)
```

OSC is the exception and the model to copy: `contactnets.com`,
`prospectcube.com`, `prospectways.com`, `prospectdock.com` — no brand stem,
industry-adjacent, each reads as its own company.

Darlean and Incubeta already hold some domains in the right shape
(`clientzenapp.co`, `accountpointapp.co`, `reachlylabs.com`, `brandant.co`),
so the shift was already underway before this tool existed.

**This is a hypothesis, not a proven cause.** Batch-permutation domains are
what Spamhaus/SURBL began clustering on, and the estate is full of them, but
nothing here establishes that they caused any specific deliverability drop.
The cheap test is to build the next batch in the OSC shape and compare
placement against the existing domains for the same client.
