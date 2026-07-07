# Deliverability Workflow 4 — API Tools & Cost Scaling

This document outlines the agency's custom technical scripts, API integrations, and current cost controls for tools like LinkedIn Finder and Smartlead.

---

## 📋 What We Need (Requirements)
*   **Cost Viability:** Sourcing tools must scale economically. Scrapers with high initiation costs (regardless of data findings) must be capped or replaced.
*   **Thread Preservation:** Automated inbox swaps must preserve the active thread for follow-up emails so prospects don't get disassociated.
*   **Test Isolation:** Scripts must run local unit tests with zero credit consumption.

---

## 🔎 Technical Workflows & Findings

### 1. The LinkedIn Finder Tool Blocker (July 07 Learnings)
We tested using 85 Compute for exporting company employee names.
*   **The Problem:** The scraper charges an **initiation cost of $0.02 for every company queried**, whether or not they have data on that company.
    *   *Example:* Sourcing leads for a list of 500 companies costs $10.00 in initiation fees alone, even if 0 results are returned. This makes scaling to 1,000+ companies economically unviable.
*   **What to do:**
    1.  Limit the use of the 85 Compute scraper to small lists (<30 companies).
    2.  Ask the agency's WhatsApp group for recommendations on LinkedIn finder/scraping tools with result-only billing.
    3.  Investigate other specific tools (like PhantomBuster, Clay, or Sales Navigator scrapers).

### 2. Smartlead Thread Preservation (API Investigation)
When replacing a P0-broken inbox with a healthy bench inbox using the Smartlead API:
*   **The Concern:** Do follow-up emails continue in the same thread when sent from a different inbox?
*   **The Verification Task:**
    *   Open a support ticket/query with Smartlead: *“When an inbox is replaced inside a campaign via the API, do the remaining leads continue follow-up sequences in the same thread?”*
    *   Verify if there is an API flag or specific campaign setting required to enable cross-mailbox thread matching.

### 3. DNS Checker Script
We added a direct DNS-over-HTTPS (DoH) verification utility in `smartlead_sync/check_dns.py`. It queries Google DNS directly, meaning it has zero dependencies on local terminal tools (`dig`/`nslookup`) and runs on Windows or Linux servers.
*   **To run the CLI checker:**
    ```powershell
    .\.venv\Scripts\python.exe check_dns.py <domain> [selector]
    ```
*   **Integration:** The daily sync gathers all unique domains and runs these queries in parallel, caching the results to avoid Google rate-limits.
