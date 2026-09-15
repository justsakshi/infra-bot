# Smartlead Native Capabilities Research — What NOT to Build Ourselves

Researched 2026-09-15 via WebSearch/WebFetch against helpcenter.smartlead.ai, api.smartlead.ai (including llms.txt), smartlead.ai/blog. No changelog/release-notes page was found (api.smartlead.ai/changelog returns 404).

Legend for confidence per claim: **[Documented]** = found in Smartlead's own help center or API reference. **[Marketing]** = found only in smartlead.ai blog/marketing copy or third-party review sites quoting Smartlead. **[Unconfirmed]** = could not verify; explicitly a gap.

---

## 1. Auto-Replace ("Automatically Detect and Replace Burning or Expiring Mailboxes") — PRIORITY

**Status: real feature, but the deep mechanics are NOT publicly documented in detail.** This is the weakest-documented item in this report — flag accordingly.

- **Does it exist natively:** Yes. Referenced across multiple sources as part of "Smart Senders" (Smartlead's DFY domain/mailbox purchasing product line, which also includes InboxKit and Infrainbox as underlying providers — see below). **[Marketing/partially documented]**
- **What it does (as described):** "Auto-Replace keeps an eye on your Smart Sender mailboxes, detects burning or expiring accounts, and handles the replacement workflow before your campaigns are affected." Smartlead also shows an "upcoming-expiry notice" on the Email Accounts page ahead of time. **[Marketing — smartlead.ai blog / third-party summaries; I could not locate this exact sentence on a helpcenter.smartlead.ai page directly, only in search-engine-surfaced summaries attributed to Smartlead]**
- **Criteria for "burning out":** **[Unconfirmed]**. No page found that enumerates the actual signal(s) — whether it's placement-test data, warmup reputation/health score, bounce rate, spam-complaint rate, or a composite. Given Smartlead's mailbox health score is documented elsewhere as "solely based on inbox placement data" (see §9), it's plausible Auto-Replace keys off placement/health-score decay, but this is inference, not documentation.
- **Where replacement domains/mailboxes come from:** The SmartSenders FAQ (helpcenter.smartlead.ai/en/articles/256) states that once existing domains approach the end of their 12-month registration term, you either buy fresh domains/mailboxes yourself or "use the in-product Replace flow to get new mailboxes with similar-looking domains." **[Documented, but shallow]** — it does not name a specific registrar. Related SmartSenders articles reference **InboxKit** (helpcenter.smartlead.ai/en/articles/419, Google/Outlook mailboxes) and **Infrainbox** (helpcenter.smartlead.ai/en/articles/258, Microsoft Outlook mailboxes) as the DFY mailbox providers behind SmartSenders — these appear to be Smartlead's white-labeled/partnered mailbox vendors, not a named domain registrar like Namecheap/Cloudflare. **[Documented that InboxKit/Infrainbox are providers; unconfirmed whether Auto-Replace specifically routes through one or the other, or a third provider]**
- **Naming scheme for new domains:** **[Unconfirmed]**. Only "similar-looking domains" is stated — no algorithm, pattern, or example given anywhere found.
- **Warmup duration before sending on replacements:** **[Unconfirmed]**. Not documented. General Smartlead warmup ramp mechanics are documented (§5) but nothing ties a specific ramp/duration to Auto-Replace-generated mailboxes specifically.
- **API to configure or read Auto-Replace:** **[Unconfirmed]** — I found no `/reference/` page under api.smartlead.ai naming "auto-replace." There IS a documented endpoint called **`Auto-generate-mailboxes`** at `https://api.smartlead.ai/reference/auto-generate-mailboxes`, but when fetched it actually returned the "Get All Campaigns" documentation (likely a caching/routing issue on my fetch, or the reference page redirects) — **this needs a follow-up direct check before relying on it**, since generic search summaries are not a substitute for the live page. Do not assume this endpoint is unusable or usable without re-verifying it directly.
- **Free or paid:** **[Unconfirmed]**. Not stated in any source found. Given it's bundled under SmartSenders (a paid DFY purchasing flow), it is reasonable to assume it is not a free/base-plan feature, but no pricing page confirms this.

**Bottom line for build-vs-buy:** Smartlead appears to have *some* automated detect-and-suggest-replacement workflow, but publicly documented specifics are thin enough that we should not assume it fully replaces a custom burn-detection + replacement pipeline. Recommend: (a) get direct product-side confirmation (support chat, or a paying account's UI) of the actual trigger criteria and whether there's an API hook, before deciding to retire any of our own burn-detection logic; (b) treat "buys domains from X registrar" as unverified.

---

## 2. Auto-rotate / Mailbox Rotation

**Documented natively, at least at a high level.**
- Smartlead rotates sends across all connected mailboxes (SMTP/Gmail/Outlook) automatically per campaign, governed by settings like `min_time_btwn_emails` and `max_leads_per_day` (seen in the Get All Campaigns API response schema). **[Documented — api.smartlead.ai campaign settings fields]**
- Rotation is described as domain-aware — avoiding clustering sends from sibling mailboxes on the same domain into the same minute. **[Marketing/third-party review synthesis — not directly confirmed on a helpcenter page in this pass]**
- No dedicated "rotation algorithm" API reference page was found; rotation is implicit in how campaigns consume the linked email-account pool, not a separately configurable rotation endpoint.

---

## 3. Reallocate Mailboxes

**Documented, UI-triggered, semi-automatic.** Source: helpcenter.smartlead.ai/en/articles/231-what-is-the-reallocate-mailboxes-feature-and-how-to-use-it **[Documented]**
- Trigger: sender accounts becoming disconnected mid-campaign.
- Mechanism: user manually clicks "Reallocate Mailboxes" from a campaign dropdown menu; once triggered, Smartlead automatically reassigns leads that were assigned to disconnected mailboxes onto active/connected mailboxes.
- Not a background/automatic process — requires manual initiation each time.
- No documented configurable settings (e.g., which mailboxes to prefer) beyond "assign to active mailboxes."
- API: the article references api.smartlead.ai/introduction generically but does not document a specific reallocate-mailboxes endpoint. **[Unconfirmed whether an API equivalent exists — worth a direct search of api.smartlead.ai/api-reference for "reallocate" before assuming UI-only]**

---

## 4. SmartDelivery Placement Testing

**Strongly documented, including scheduled/recurring tests via API — this endpoint is real, not something we should assume doesn't exist.**
- Manual test: helpcenter.smartlead.ai/en/articles/236-how-to-run-a-manual-deliverability-test-using-smartdelivery **[Documented]**
- Test with non-connected accounts: helpcenter.smartlead.ai/en/articles/235 **[Documented]**
- Automated/recurring test: helpcenter.smartlead.ai/en/articles/237-how-to-run-an-automatic-deliverability-test-using-smartdelivery — lets you set recurring tests at a chosen frequency; runs automatically with no further manual action. **[Documented]**
- **API — CONFIRMED to exist:**
  - `POST https://smartdelivery.smartlead.ai/api/v1/spam-test/schedule` — Create an Automated Placement Test. Auth via `api_key` query param. Response includes `id`, `test_name`, `description`, `spam_filters`, `link_checker`, `campaign_id`, `sequence_mapping_id`, `status`, `schedule_start_time`, `test_end_date`, `every_days` (recurrence interval in days), `scheduler_cron_value` (cron expression). Error codes 400/401/404/422/429/500/503 documented. **[Documented — api.smartlead.ai/reference/create-an-automated-placement-test]**
  - `GET .../spam-test/report` — List all spam tests, filterable by date/type/status. **[Documented — referenced via api.smartlead.ai/api-reference/smart-delivery/list-tests]**
  - Spam Test Details endpoint (GET) — retrieve full data for a given test. **[Documented — api.smartlead.ai/reference/spam-test-details]**
  - `PUT` Stop an Automated Smart Delivery Test — stops an active automated (parent) test before its end date; only applicable to automated tests. **[Documented — api.smartlead.ai/reference/stop-an-automated-test]**
- Also documented: SmartDelivery monitors domains/IPs against 400+ blacklists continuously and alerts on new listings; tests placement across Gmail/Outlook/Yahoo etc. and reports primary/promotions/spam landing. **[Marketing-level description, consistent with the API fields above but the "400+ blacklists" figure itself is unverified/marketing]**

**Action item:** Given the analyst's stated instruction that we were "already burned once" assuming an endpoint didn't exist — this section confirms `/spam-test/schedule` IS documented and real. Any internal assumption that recurring placement tests require custom scheduling should be revisited; native scheduling appears to cover this.

---

## 5. Warmup

**Well documented.**
- Smart-adjusting warmup algorithm: helpcenter.smartlead.ai/en/articles/63-smartlead-s-smart-adjusting-algorithm-for-mailbox-warmups **[Documented]**
- Daily Ramp-Up feature: helpcenter.smartlead.ai/en/articles/164-understanding-the-daily-ramp-up-feature-in-smartlead-s-email-warm-up — gradually increases daily sending limit during warmup; requires warmup + ramp-up both enabled to function. **[Documented]**
- Auto-ramp adjusts daily volume increase based on the mailbox's health-score trajectory (slows if ramping faster than reputation can absorb, speeds up if behind schedule). **[Documented via helpcenter/API guide synthesis — api.smartlead.ai/guides/email-warmup]**
- Reputation/health score factors named: sending volume (controlled, warmup-safe growth), reply rate, and ESP diversity (spread across Gmail/Outlook/Yahoo etc. to avoid over-reliance on one ESP). **[Documented]**
- Bulk warmup update: helpcenter.smartlead.ai/en/articles/269-how-to-bulk-update-warmup-for-mailboxes **[Documented]**
- Warmup Pool / upgrade eligibility concept exists: helpcenter.smartlead.ai/en/articles/438-understand-your-warmup-pool-and-upgrade-eligibility — implies a shared warmup pool mechanic gating some behavior; details of pool mechanics not fully extracted in this pass. **[Documented existence; details thin]**
- API warmup stats retrieval documented under email-accounts endpoints (per llms.txt summary: "retrieve warmup statistics"). **[Documented]**

---

## 6. Bounce Handling / Auto-Pause on Bounce Thresholds

**Documented, UI-configured, with webhook.**
- Source: helpcenter.smartlead.ai/en/articles/210-bounce-autopause-and-webhook and helpcenter.smartlead.ai/en/articles/274-what-is-high-bounce-rate-auto-protection-in-smartlead **[Documented]**
- Mechanism: "High Bounce Rate Auto Protection" toggle set during campaign SETUP; you specify a threshold percentage (example given: 3%). Formula shown: `(total_bounce_count + latest_bounce) / total_lead_count * 100`; if this exceeds the threshold, campaign auto-pauses.
- Webhook: enabled via Account Settings → Warning Notifications → add bounce-autopause webhook URL. Payload includes campaign name/ID, bounce threshold %, total bounce count, total leads, auto-pause timestamp, and resulting campaign status.
- **Limitations:** No documented minimum/maximum threshold bounds; no documented API endpoint to set this threshold programmatically (appears UI-only for configuration, though webhook consumption is obviously API-adjacent) — **[Unconfirmed whether campaign-settings API exposes this threshold field; worth checking the campaign settings POST schema directly for a bounce-threshold parameter before assuming UI-only]**.

---

## 7. Blacklist Monitoring

**Documented at a basic level; some claims are marketing-only.**
- Domain Blocklists article: helpcenter.smartlead.ai/en/articles/246-domain-blocklists — describes checking URLs embedded in email copy against blocklist providers, explicitly names **Spamhaus**; other blocklist names (SURBL, URIBL, Barracuda, SpamCop) appear in the article's discussion but it is NOT confirmed that Smartlead actively monitors all of them — only Spamhaus is explicitly confirmed as checked. **[Documented for Spamhaus specifically; others unconfirmed]**
- IP Blacklists article: helpcenter.smartlead.ai/en/articles/251-ip-blacklists **[Documented to exist; content not fully extracted this pass]**
- Standalone public tool: smartlead.ai/tools/domain-blacklist-checker — free-standing checker tool, separate from account monitoring. **[Documented as a tool; not necessarily the same as continuous account-level monitoring]**
- The claim that "SmartDelivery monitors domains/IPs against 400+ blacklists continuously and alerts automatically" appears only in marketing/third-party synthesis, not in a helpcenter article I could directly confirm. **[Marketing]**
- API: `api.smartlead.ai/reference/domain-blacklist` exists as a reference page. **[Documented to exist as an endpoint reference; parameters not extracted this pass — worth a direct follow-up read]**

---

## 8. ESP Matching (`enable_ai_esp_matching`)

**Documented as a real API/campaign-settings field.**
- `enable_ai_esp_matching` is a boolean on the campaign settings endpoint `POST https://server.smartlead.ai/api/v1/campaigns/{campaign_id}/settings`, and also appears in the Get All Campaigns response schema. Default is `false`. **[Documented]**
- Function (per synthesis of naming + surrounding docs): when true, Smartlead's AI matches each lead to the sending account best suited by ESP (matching sender ESP to recipient's provider) and historical deliverability performance. **[Documented that the field exists and is toggle-able; the exact matching logic/algorithm is not detailed in any source found — treat the "how" as marketing-level inference]**

---

## 9. Domain/Mailbox Health Scoring / "Smart Agent" Insights

- Per-mailbox color-coded health score: green 90–100, yellow 70–90, red below 70. Overview tab shows daily warmup sends, replies, inbox-vs-spam rate. **[Marketing/third-party review synthesis (warmforge.ai, mailcon.com) — not independently confirmed on a helpcenter page in this pass]**
- Explicitly noted limitation (from a third-party comparison, likely a competitor's framing so treat with caution): "Smartlead's inbox health score is solely based on inbox placement data" — does not factor content quality, domain reputation/DNS, or list health. **[Marketing/competitor-sourced claim — flag as low confidence, possibly biased]**
- "Smart Agent" as a named feature: **not found**. No page or reference located under this exact name. **[Could not find — do not assume it exists under this name]**
- Automated alert "if a sending account starts drifting toward spam" mentioned in third-party synthesis only. **[Marketing]**

---

## 10. Other Deliverability/Fleet-Health Features Found

- **Auto-restart OOO based on email copy:** helpcenter.smartlead.ai/en/articles/209 — AI detects out-of-office autoreplies and auto-resumes the lead's sequence after the OOO period. **[Documented]** — relevant if our own automation currently hand-rolls OOO detection.
- **Pre-warmed mailboxes:** helpcenter.smartlead.ai/en/articles/429-pre-warmed-mailboxes-by-smartlead — Smartlead can sell mailboxes that arrive already warmed, reducing time-to-send. **[Documented to exist; commercial terms not extracted]**
- **SmartSenders (DFY domain+mailbox purchasing):** helpcenter.smartlead.ai/en/articles/255 (walkthrough) and /256 (FAQ) — an in-product flow to buy domains and mailboxes, provisioned via partners InboxKit (Google/Outlook) and Infrainbox (Outlook). This is the umbrella product Auto-Replace lives under. **[Documented]**
- **Bulk mailbox upload / SMTP 2FA approach:** helpcenter.smartlead.ai/en/articles/6 **[Documented]**
- **Bulk unblock warmup mailboxes:** helpcenter.smartlead.ai/en/articles/290 — implies mailboxes can get "blocked" from warmup pool participation and need manual/bulk unblocking; relevant to our own fleet ops. **[Documented to exist; mechanics not fully extracted]**
- **Signature-missing check, campaign diagnostics, sending-volume audit, mailbox provider-wise performance, mailbox domain-wise / name-wise health metrics:** these all appear as named MCP/API tool surfaces in the Smartlead integration available in this environment (e.g., `check_signature_missing`, `get_campaign_diagnostics`, `get_campaign_sending_volume_audit`, `get_mailbox_domain_wise_health_metrics`, `get_mailbox_provider_wise_performance`) — strong signal these are real, documented-adjacent API capabilities even though I did not fetch each help page individually in this pass. **[Documented to exist as API operations, via the tool surface; underlying help-center prose not individually verified this pass]**

---

## Summary Table

| Capability | Native? | API or UI? | Confidence |
|---|---|---|---|
| Auto-Replace | Yes (exists) | Unclear/likely UI, API unconfirmed | Marketing-level only; mechanics largely undocumented |
| Auto-rotate/rotation | Yes | Implicit in campaign settings API | Documented (shallow) |
| Reallocate Mailboxes | Yes | UI-triggered; API unconfirmed | Documented |
| SmartDelivery scheduled tests | Yes | **API confirmed**: POST /spam-test/schedule + report/details/stop endpoints | Documented, solid |
| Warmup + ramp + reputation | Yes | UI + API (read stats) | Documented, solid |
| Bounce auto-pause | Yes | UI-configured threshold + webhook; API config unconfirmed | Documented |
| Blacklist monitoring | Partial | Spamhaus confirmed via copy-link scan; broader claims marketing-only | Mixed |
| ESP matching | Yes | API field `enable_ai_esp_matching` | Documented (field), algorithm undocumented |
| Health scoring / "Smart Agent" | Partial | UI only; "Smart Agent" name not found | Marketing-level |
| OOO auto-restart | Yes | Documented | Documented |

## Key Gaps / Recommended Follow-ups Before Deciding Build-vs-Buy
1. Get a live/paying-account look at the actual Auto-Replace UI and its settings panel — public docs don't show criteria, registrar, naming scheme, or warmup duration.
2. Directly re-fetch `api.smartlead.ai/reference/auto-generate-mailboxes` (my fetch returned mismatched content — possibly a caching artifact) and search api-reference for "reallocate" and "bounce threshold" fields on the campaign settings POST body.
3. Confirm whether campaign-settings API exposes the bounce auto-pause threshold as a writable field (not just triggerable via UI).
4. Verify the "400+ blacklists" and "green/yellow/red 90/70" health-score figures directly against a helpcenter/product page rather than third-party review sites, since those numbers currently rest on marketing/third-party synthesis.
