# Deliverability Workflow 3 — Inbox Lifecycle & Rotation

This document details the lifecycle management of sending accounts (inboxes), including provisioning, warmup, volume caps, and automated replacement when deliverability degrades.

---

## 📋 What We Need (Requirements)
*   **Warmed Accounts:** A bench of healthy spare inboxes ready to replace failed campaign senders (minimum 2 weeks warmup, reputation $\ge 90\%$).
*   **Low Daily Limits:** 2026 guidelines require keeping per-inbox sending volume low (conservative target: 30-50/day).
*   **Volume Limits on Outlook:** If inboxes are landing in spam on Outlook/Microsoft recipients, decrease sending volume to **10-15 emails/day** per support guidelines.

---

## 🔄 The Inbox Lifecycle (Step-by-Step)

```text
PROVISIONING (Scaled Mail)
   ├── Setup DNS records (SPF, DKIM, DMARC)
   └── Configure mailboxes on Smartlead (Gmail/Outlook ratio)
         ↓
WARMUP (Min 2-3 weeks)
   ├── Full warmup ON (30/day cap, ramp +5/day)
   └── Maintain warmup reputation >= 90%
         ↓
ACTIVE (Live campaigns)
   ├── Limit sending volume to 30-40 emails/day (Outlook: 10-15/day)
   └── Maintenance warmup trickle ON (5-10/day)
         ↓
ROTATION (When scored as degraded or failing tests)
   ├── Add healthy replacement to campaign (same persona)
   ├── Swap leads / preserve threading
   └── Pause broken inbox, restore Full Warmup to recover
```

---

## 🔎 How We Execute

### 1. Provisioning via Scaled Mail
*   Purchase domains and request mailboxes from Scaled Mail.
*   *Mailbox Ratios:* Ensure we distribute mailboxes safely. For Gmail, limit to **3 mailboxes per domain** (to avoid domain-level filters). For Outlook/other providers, we can configure up to **25 mailboxes per domain**.
*   Request Scaled Mail support to batch-import the new domains and mailboxes into Smartlead (takes ~2 days).

### 2. LinkedIn & Omnichannel Sending (Aaron Expandee Setup)
*   When adding new sender profiles (such as Aaron) to Expandee:
    *   Set up Aaron's profile in Expandee / LinkedIn.
    *   Configure HeyReach to connect both Aaron's profile and existing profiles **to the same campaign** concurrently to distribute connection and message volume safely without triggering LinkedIn blockages.

### 3. Automated Inbox Rotation
If an inbox fails a placement test (P0):
*   `infra-bot`'s rotation scheduler will select a healthy bench inbox from the **same client** with a **matching sender name** (e.g., Sam $\rightarrow$ Sam) to preserve conversation consistency.
*   **Load-Bearing Execution Order:**
    1.  **Add** the replacement inbox to the Smartlead campaign via the API.
    2.  Verify the replacement is attached.
    3.  Handle in-flight leads (reassign conversations to the replacement or pause them).
    4.  **Remove** the broken victim inbox from the campaign (never remove if it's the last inbox in the campaign).
*   The broken inbox is set back to full warmup mode, and the manager is flagged to check copy, list, and DNS records.
