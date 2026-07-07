import asyncio
import sys
import os
import httpx

# Fix Windows console encoding for unicode emoji output
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

async def resolve_txt(name: str) -> list[str]:
    """Resolve TXT records using Google's DNS-over-HTTPS (DoH) API."""
    url = "https://dns.google/resolve"
    params = {"name": name, "type": "TXT"}
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            
            records = []
            for answer in data.get("Answer", []):
                # Type 16 is TXT
                if answer.get("type") == 16:
                    raw_data = answer.get("data", "")
                    # Clean up quotes and joined text segments (DNS sometimes splits long TXT values)
                    clean_data = raw_data.strip('"').replace('" "', '')
                    records.append(clean_data)
            return records
    except Exception as e:
        print(f"Error querying DNS for {name}: {e}", file=sys.stderr)
        return []

def audit_spf(records: list[str]) -> tuple[bool, str]:
    spf_records = [r for r in records if r.startswith("v=spf1")]
    if not spf_records:
        return False, "❌ Missing SPF record"
    if len(spf_records) > 1:
        return False, f"❌ Multiple SPF records found: {spf_records}"
    
    spf = spf_records[0]
    if "+all" in spf:
        return False, f"❌ SPF has unsafe '+all': {spf}"
    if spf.endswith("~all") or spf.endswith("-all") or "~all " in spf or "-all " in spf:
        return True, f"✅ Valid SPF: {spf}"
    
    return False, f"⚠️ SPF found but policy is not optimal: {spf}"

def audit_dkim(records: list[str]) -> tuple[bool, str]:
    dkim_records = [r for r in records if "v=DKIM1" in r or "k=rsa" in r]
    if not dkim_records:
        return False, "❌ Missing or invalid DKIM record at this selector"
    
    dkim = dkim_records[0]
    # Check key length (simple heuristic based on base64 length)
    # A 2048-bit key is typically ~390+ characters; 1024-bit key is ~220 characters
    p_index = dkim.find("p=")
    if p_index != -1:
        key_part = dkim[p_index+2:].split(";")[0].strip()
        length = len(key_part)
        if length > 300:
            return True, f"✅ Valid DKIM (likely 2048-bit): {dkim[:60]}... (len: {length})"
        else:
            return False, f"⚠️ DKIM found but key might be 1024-bit (len: {length}): {dkim}"
            
    return True, f"✅ Valid DKIM: {dkim[:60]}..."

def audit_dmarc(records: list[str]) -> tuple[bool, str]:
    dmarc_records = [r for r in records if r.startswith("v=DMARC1")]
    if not dmarc_records:
        return False, "❌ Missing DMARC record"
    if len(dmarc_records) > 1:
        return False, f"❌ Multiple DMARC records found: {dmarc_records}"
    
    dmarc = dmarc_records[0]
    if "p=reject" in dmarc:
        return True, f"✅ DMARC at reject policy (Strongest): {dmarc}"
    if "p=quarantine" in dmarc:
        return True, f"✅ DMARC at quarantine policy (Strong): {dmarc}"
    if "p=none" in dmarc:
        return True, f"⚠️ DMARC at none policy (Good for new domains, upgrade to quarantine/reject after 2-4 weeks): {dmarc}"
        
    return False, f"❌ DMARC policy not recognized or invalid: {dmarc}"

async def audit_domain(domain: str, selector: str = "default"):
    print(f"\nAuditing DNS records for domain: {domain} (DKIM selector: '{selector}')")
    print("=" * 70)
    
    # 1. SPF Check
    spf_txts = await resolve_txt(domain)
    spf_ok, spf_msg = audit_spf(spf_txts)
    print(f"SPF Status:   {spf_msg}")
    
    # 2. DKIM Check
    dkim_domain = f"{selector}._domainkey.{domain}"
    dkim_txts = await resolve_txt(dkim_domain)
    dkim_ok, dkim_msg = audit_dkim(dkim_txts)
    print(f"DKIM Status:  {dkim_msg}")
    
    # 3. DMARC Check
    dmarc_domain = f"_dmarc.{domain}"
    dmarc_txts = await resolve_txt(dmarc_domain)
    dmarc_ok, dmarc_msg = audit_dmarc(dmarc_txts)
    print(f"DMARC Status: {dmarc_msg}")
    print("=" * 70)
    
    if spf_ok and dkim_ok and dmarc_ok:
        print("🎉 Domain authentication looks healthy and ready for cold emailing!")
    else:
        print("❌ Action required. Update your DNS settings as indicated above.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python check_dns.py <domain> [dkim_selector]")
        sys.exit(1)
        
    dom = sys.argv[1].strip()
    sel = sys.argv[2].strip() if len(sys.argv) > 2 else "default"
    
    asyncio.run(audit_domain(dom, sel))
