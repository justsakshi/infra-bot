# Deliverability Workflow 2 — List Hygiene & ICP Filtering

This document details the step-by-step process for sourcing, cleaning, and filtering cold email lists to keep bounce rates below the critical threshold (target < 1%, stop if > 5%).

---

## 📋 What We Need (Requirements)
*   **Verified Emails:** Bounces above 3% put sending domains at immediate risk of ISP suspension. Bounces above 5% require stopping the campaign.
*   **ICP Integrity:** Sourcing companies matching the client's Ideal Customer Profile (avoiding firms that are too small or wrong-country).
*   **Google Postmaster Target:** Spam complaints must remain < 0.1% (permanent workspace block triggers at 0.3%).

---

## 🔎 What We Do (Step-by-Step Lead Verification)

### Step 1: Clean & Score Catch-Alls
Do not just verify syntax. You must process lists through verification tools (like MillionVerifier, Scrubby, or ZeroBounce) to identify:
*   **Valid:** Safe to send.
*   **Invalid:** Hard bounces. Delete immediately.
*   **Catch-all (Unverifiable):** Mail servers that accept all emails initially but might bounce later. Route catch-alls through a secondary verifier (like Scrubby) that tests placement by sending a verification signal, or send to them from a separate "reputation testing" sequence.

### Step 2: ICP Validation & Filtering (July 07 Standup Learnings)
When working with client target profiles (e.g. D2C brands, general contractors):
*   ⚠️ **Unreliable Revenue Data:** Platforms like Apollo or Store Leads often have stale, estimated, or blank revenue numbers. If you apply a hard revenue filter (e.g. "Only show $4M+ revenue"), you will filter out the vast majority of your target companies due to missing data (e.g. Varsha saw a drop from 515 to 245 companies when applying revenue filters).
*   ✅ **Action Rule — Employee Count Proxy:** Filter by **employee count** instead (e.g. 10 to 5,000 employees). If you must use revenue:
    1.  Export the list *without* revenue filters (only filter by employee count/region).
    2.  Filter out companies that have a *known* revenue value that is too low.
    3.  **Keep the blanks** (companies with missing revenue data) and treat them as potential targets to check manually or via traffic proxies.
*   **Traffic Proxies:** For D2C e-commerce, use website traffic (monthly visits) as a proxy for size instead of self-reported revenue numbers.

### Step 3: Run USA Activity Agents
For international lists (e.g. UK e-commerce brands) where you want to target companies active in the US:
*   Use Clay's integration or custom LLM scraper agent to inspect the website footer, terms page, or shipping options.
*   *Verification Heuristic:* Check for mentions of "USD", "USA shipping", or a physical US mailing address. Anjali's test showed that out of 100 UK D2C companies, 60+ were active in the US.

---

## 🛠️ How to Handle High Bounce Rates

If a live campaign's bounce rate rises above 3%:

1.  **PAUSE the Campaign:** Go to Smartlead and pause sending.
2.  **Pull the Lead File:** Export the sent leads and match them against the bounce log.
3.  **Diagnose the Bounce Type:**
    *   **Hard Bounce (Invalid Email):** The email address doesn't exist. This is a list hygiene failure. Re-verify the remainder of the list before resuming.
    *   **Soft Bounce (Block/Spam Filter):** The recipient's server rejected the email due to sender reputation or copy content. Cut campaign volume by 50% for 2 weeks, check DNS settings, and simplify email copy (remove links/images).
4.  **Clean the List:** Remove catch-alls, spam traps, and duplicates.
