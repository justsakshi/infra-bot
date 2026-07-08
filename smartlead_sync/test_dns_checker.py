import asyncio
from smartlead.dns_checker import audit_spf, audit_dkim_multi, audit_dmarc, audit_domain_dns

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

# 2. Test DKIM Auditing (multi-selector: 2026-07-08 fix checks google/
# selector1/selector2/default and passes on any hit — see dns_checker.py)
ok_dkim, msg_dkim = audit_dkim_multi({"default": ["v=DKIM1; k=rsa; p=" + ("A" * 350)]})
ok(ok_dkim, "valid DKIM pass at default selector")

ok_dkim_alt, msg_dkim_alt = audit_dkim_multi({
    "google": [], "default": ["v=DKIM1; k=rsa; p=" + ("A" * 350)],
})
ok(ok_dkim_alt, "valid DKIM pass when only a non-primary selector has the record")

ok_dkim3, msg_dkim3 = audit_dkim_multi({"google": [], "selector1": [], "default": []})
ok(not ok_dkim3, "missing DKIM at every selector -> fail")

ok_dkim4, msg_dkim4 = audit_dkim_multi({"google": None, "default": []})
ok(ok_dkim4, "lookup ERROR at a selector -> status unknown, never a hard fail")
ok("unknown" in msg_dkim4.lower(), "lookup-error message says status unknown")

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
