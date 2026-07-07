# Deliverability Workflow 1 — DNS Authentication

This document details the requirements, step-by-step auditing instructions, and remediation steps for domain-level DNS Authentication.

---

## 📋 What We Need (Requirements)

To ensure high inbox placement (and avoid Google/Microsoft spam blocking), every domain we send from must have three core records set up correctly:

| Record | Standard Policy | Unsafe / Negative Policy |
|---|---|---|
| **SPF** | `v=spf1 include:_spf.google.com ~all` (or matching your ESP) | ❌ `+all` (allows anyone to send)<br>❌ Multiple SPF records |
| **DKIM** | **2048-bit** public key published at `default._domainkey.domain.com` | ❌ 1024-bit key (weak and heavily filtered)<br>❌ Missing selector |
| **DMARC** | `v=DMARC1; p=none;` (new domains) $\rightarrow$ `p=quarantine;` (2 weeks) $\rightarrow$ `p=reject;` (30 days clean) | ❌ Missing record<br>❌ Left at `p=none` permanently |

---

## 🔎 What We Do (Step-by-Step Audit)

### Step 1: Run the Automated Audit
You can check a domain's status automatically using the Python DNS checker. Open a terminal in `smartlead_sync/` and run:

```powershell
.\.venv\Scripts\python.exe check_dns.py yourdomain.com [optional_dkim_selector]
```

### Step 2: Query DNS Records manually (Alternative)
If you need to query the raw DNS records, use PowerShell:

```powershell
# Query SPF
Resolve-DnsName -Name yourdomain.com -Type TXT | Where-Object { $_.Strings -match "v=spf1" }

# Query DKIM (replace 'default' with your selector)
Resolve-DnsName -Name default._domainkey.yourdomain.com -Type TXT

# Query DMARC
Resolve-DnsName -Name _dmarc.yourdomain.com -Type TXT
```

---

## 🛠️ How to Fix DNS Record Issues

If the audit flags a gap, you must update the records in your **DNS Hosting Provider** (e.g. GoDaddy, Namecheap, Cloudflare) and/or your **Email Service Provider** (Google Workspace / Microsoft 365).

### 1. Fixing SPF Gaps
*   **Problem:** Multiple SPF records found.
    *   **Fix:** DNS only supports a *single* SPF record per domain. Merge them into one.
    *   *Example:* Merge Google and Sendgrid:
        *   ❌ Record 1: `v=spf1 include:_spf.google.com ~all`
        *   ❌ Record 2: `v=spf1 include:sendgrid.net ~all`
        *   ✅ Merged: `v=spf1 include:_spf.google.com include:sendgrid.net ~all`
*   **Problem:** Record ends with `+all` or does not have `~all`/`-all`.
    *   **Fix:** Change the ending to `~all` (soft fail) or `-all` (hard fail). Never use `+all`.

### 2. Fixing DKIM Gaps
*   **Problem:** DKIM selector is missing or not resolving.
    *   **Fix:** Log in to your Email Provider Admin Panel:
        *   *Google Workspace:* Admin Console $\rightarrow$ Apps $\rightarrow$ Google Workspace $\rightarrow$ Gmail $\rightarrow$ Authenticate Email. Click **Generate New Record** (select **2048** key length), then copy the TXT host name (usually `google._domainkey`) and TXT value.
        *   *Microsoft 365:* Defender Portal $\rightarrow$ Email & Collaboration $\rightarrow$ Policies & Rules $\rightarrow$ Threat Policies $\rightarrow$ Email Authentication Settings $\rightarrow$ DKIM. Enable DKIM and copy keys.
    *   Add this TXT record to your DNS host manager.
*   **Problem:** DKIM key is 1024-bit.
    *   **Fix:** Delete the old 1024-bit record in your DNS manager, generate a new **2048-bit** key inside Google/Microsoft admin consoles, and add it.

### 3. Fixing DMARC Gaps
*   **Problem:** DMARC record is missing.
    *   **Fix:** Add a new TXT record:
        *   **Host/Name:** `_dmarc` (DNS manager will append your domain)
        *   **Value:** `v=DMARC1; p=none; rua=mailto:dmarc-reports@yourdomain.com;`
*   **Problem:** DMARC is stuck at `p=none` for established domains (>30 days old).
    *   **Fix:** Gradually upgrade the policy string:
        1.  `p=none` $\rightarrow$ No enforcement (monitoring phase).
        2.  `p=quarantine` $\rightarrow$ Emails failing SPF/DKIM land in Spam.
        3.  `p=reject` $\rightarrow$ Emails failing SPF/DKIM are deleted/blocked outright (Strongest).
