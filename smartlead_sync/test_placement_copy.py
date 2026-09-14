"""Copy-refresh tests. The failure being guarded against: an email reaching
the seed panel with literal {{merge_field}} braces, or with another client's
copy, and the resulting spam verdict being written against a healthy domain.
"""
from smartlead.placement_copy import (
    resolve_merge_fields, pick_source_campaign, build_variants, copy_hash,
    sample_custom_fields, first_signature,
)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

FIELDS = {"providers_line": "Alesco, ADS and Averick sit behind one login."}
SIG = "<div>Aaron Dix<br>BettrData</div>"

# --- merge fields ---
text, missing = resolve_merge_fields("Hi {{first_name}}, {{providers_line}} Bye.<br>%signature%", FIELDS, SIG)
ok("{{first_name}}" in text, "standard field is left for Smartlead to fill")
ok("Alesco, ADS and Averick" in text, "custom field substituted from the sample lead")
ok("{{providers_line}}" not in text, "no literal braces remain for a resolved field")
ok(text.endswith(SIG), "%signature% replaced with the sender's signature")
ok(missing == [], f"nothing unresolved, got {missing}")

text, missing = resolve_merge_fields("{{providers_line}} and {{promo_code}}", {}, SIG)
ok(missing == ["providers_line", "promo_code"], f"unresolved custom fields are named, got {missing}")
ok("{{promo_code}}" in text, "an unresolved field is left visible, not silently blanked")

text, _ = resolve_merge_fields("Body without signature", FIELDS, "")
ok(text == "Body without signature", "no signature and no placeholder -> text unchanged")

# --- source campaign ---
camps = [
    {"id": 1, "status": "PAUSED", "created_at": "2026-09-01"},
    {"id": 2, "status": "ACTIVE", "created_at": "2026-09-05"},
    {"id": 3, "status": "ACTIVE", "created_at": "2026-09-10"},
    {"id": 4, "status": "DRAFTED", "created_at": "2026-09-12"},
]
ok(pick_source_campaign(camps)["id"] == 3, "newest ACTIVE campaign wins")
ok(pick_source_campaign([camps[0], camps[3]]) is None, "no active campaign -> None, not a guess")

# --- variants ---
STEP = {"sequence_variants": [
    {"variant_label": "A", "subject": "payroll opinions",
     "email_body": "{{providers_line}} Open to fifteen minutes?<br>%signature%"},
    {"variant_label": "B", "subject": "you'd know",
     "email_body": "Because you sign off. {{providers_line}}<br>%signature%"},
]}
variants, problems = build_variants(STEP, FIELDS, SIG)
ok(len(variants) == 2, "both variants kept")
ok(problems == [], f"clean step has no problems, got {problems}")
ok(variants[0]["variant_label"] == "A" and variants[1]["variant_label"] == "B", "labels preserved")
ok(all("{{providers_line}}" not in v["email_body"] for v in variants), "no braces in any body")

# The whole point: an unresolved field must block the write.
_, problems = build_variants(STEP, {}, SIG)
ok(problems and all("providers_line" in p for p in problems),
   f"unresolved field is reported per variant, got {problems}")

_, problems = build_variants({"sequence_variants": [{"variant_label": "A", "subject": "", "email_body": ""}]}, FIELDS, SIG)
ok("no usable variants" in problems, f"empty step is refused, got {problems}")

# --- hash: same copy same hash, any change a different one ---
h1 = copy_hash(variants)
h2 = copy_hash(build_variants(STEP, FIELDS, SIG)[0])
ok(h1 == h2, "identical copy hashes identically")
changed = [dict(v) for v in variants]; changed[0]["subject"] = "different"
ok(copy_hash(changed) != h1, "a subject change changes the hash")

# --- sampling helpers ---
leads = [{"lead": {"custom_fields": {"providers_line": ""}}},
         {"lead": {"custom_fields": {"providers_line": "X sits behind one login."}}}]
ok(sample_custom_fields(leads) == {"providers_line": "X sits behind one login."},
   "first lead with a non-empty field is the sample")
ok(sample_custom_fields([]) == {}, "no leads -> empty sample")
ok(first_signature([{"signature": ""}, {"signature": "  <b>S</b> "}]) == "<b>S</b>",
   "first non-empty signature, trimmed")

print("\nALL PASSED")
