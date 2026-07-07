import asyncio
from smartlead.dns_checker import audit_spf, audit_dkim, audit_dmarc, audit_domain_dns

def ok(c, m):
    print(f"  {'PASS' if c else 'FAIL'}: {m}")
    assert c, m

# 1. Test SPF Auditing
ok_spf, msg_spf = audit_spf(["v=spf1 include:_spf.google.com ~all"])
ok(ok_spf, "valid SPF pass")
ok("~all" in msg_spf, "valid SPF msg contains content")

ok_spf2, msg_spf2 = audit_spf([])
ok(not ok_spf2, "missing SPF fail")
ok("Missing" in msg_spf2, "missing SPF error message")

ok_spf3, msg_spf3 = audit_spf(["v=spf1 include:_spf.google.com +all"])
ok(not ok_spf3, "unsafe SPF fail")
ok("unsafe" in msg_spf3, "unsafe SPF error message")

ok_spf4, msg_spf4 = audit_spf(["v=spf1", "v=spf1 include:x ~all"])
ok(not ok_spf4, "multiple SPF fail")

# 2. Test DKIM Auditing
ok_dkim, msg_dkim = audit_dkim(["v=DKIM1; k=rsa; p=" + ("A" * 350)]) # 2048 bit mock
ok(ok_dkim, "valid DKIM 2048-bit pass")

ok_dkim2, msg_dkim2 = audit_dkim(["v=DKIM1; k=rsa; p=MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDzve3Bs4dTdvQ20PGN"]) # 1024 bit mock
ok(not ok_dkim2, "DKIM 1024-bit warning/fail")
ok("1024-bit" in msg_dkim2, "1024-bit warning message")

ok_dkim3, msg_dkim3 = audit_dkim([])
ok(not ok_dkim3, "missing DKIM fail")

# 3. Test DMARC Auditing
ok_dmarc, msg_dmarc = audit_dmarc(["v=DMARC1; p=quarantine;"])
ok(ok_dmarc, "valid DMARC quarantine pass")

ok_dmarc2, msg_dmarc2 = audit_dmarc(["v=DMARC1; p=none;"])
ok(ok_dmarc2, "valid DMARC none warning/pass")

ok_dmarc3, msg_dmarc3 = audit_dmarc([])
ok(not ok_dmarc3, "missing DMARC fail")

# 4. Test Mock Domain Bypass
async def test_bypass():
    res = await audit_domain_dns("mockdomain.local")
    ok(res["spf_ok"], "mock domain SPF bypass")
    ok(res["dkim_ok"], "mock domain DKIM bypass")
    ok(res["dmarc_ok"], "mock domain DMARC bypass")

asyncio.run(test_bypass())

print("\nALL PASSED (test_dns_checker)")
