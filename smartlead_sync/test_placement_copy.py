"""Copy-refresh tests. The failure being guarded against: an email reaching
the seed panel with literal {{merge_field}} braces, or with another client's
copy, and the resulting spam verdict being written against a healthy domain.
"""
from smartlead.placement_copy import (
    resolve_merge_fields, pick_source_campaign, build_variants, copy_hash,
    sample_custom_fields, first_signature, step_variants,
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

# --- both variant keys (Smartlead returns one, expects the other on write) ---
ok(step_variants({"sequence_variants": [{"variant_label": "A"}]})[0]["variant_label"] == "A",
   "reads sequence_variants (what GET returns)")
ok(step_variants({"seq_variants": [{"variant_label": "B"}]})[0]["variant_label"] == "B",
   "reads seq_variants (what POST expects) too")
ok(step_variants({}) == [], "a step with neither key yields no variants")

variants, problems = build_variants(
    {"seq_variants": [{"variant_label": "A", "subject": "s",
                       "email_body": "{{providers_line}}"}]}, FIELDS, SIG)
ok(len(variants) == 1 and problems == [],
   f"build_variants works off seq_variants as well, got {variants} {problems}")

# --- source filters (copy sync) ---
mixed = [
    {"id": 10, "name": "Melior - Round Table", "status": "ACTIVE", "created_at": "2026-10-05", "client_id": 12256},
    {"id": 11, "name": "PL own outbound", "status": "ACTIVE", "created_at": "2026-10-01", "client_id": None},
    {"id": 12, "name": "Deliverability Test Campaign", "status": "ACTIVE", "created_at": "2026-10-04", "client_id": None},
    {"id": 13, "name": "PL older outbound", "status": "ACTIVE", "created_at": "2026-09-01", "client_id": None},
]
ok(pick_source_campaign(mixed, client_id=None)["id"] == 11, "own-campaign filter skips Melior and the test campaign")
ok(pick_source_campaign(mixed, client_id=12256)["id"] == 10, "client filter picks that client's newest")
ok(pick_source_campaign(mixed, client_id=None, exclude_ids=(11,))["id"] == 13, "excluded ids are skipped")
ok(pick_source_campaign([mixed[0]], client_id=None) is None, "only Melior active -> None for PL, not Melior's copy")
ok(pick_source_campaign(mixed)["id"] == 10, "no filter keeps the old behaviour")

print("\nALL PASSED")


# --- most active campaign of the week (team rule 2026-10-08) ---
from smartlead.placement_copy import eligible_sources, choose_most_active
camps2 = [
    {"id": 1, "status": "ACTIVE", "name": "Exec search", "created_at": "2026-09-01", "client_id": 12256},
    {"id": 2, "status": "ACTIVE", "name": "Competitor AI", "created_at": "2026-10-05", "client_id": 12256},
    {"id": 3, "status": "PAUSED", "name": "Accounting", "created_at": "2026-08-01", "client_id": 12256},
    {"id": 4, "status": "DRAFTED", "name": "New one", "created_at": "2026-10-07", "client_id": 12256},
    {"id": 5, "status": "ACTIVE", "name": "DT Melior #1", "created_at": "2026-10-06", "client_id": 12256},
    {"id": 6, "status": "ACTIVE", "name": "PL own", "created_at": "2026-10-06", "client_id": None},
]
el = eligible_sources(camps2, client_id=12256)
ok([c["id"] for c in el] == [1, 2, 3], "drafts, tests and other clients are never sources")
best, why = choose_most_active(el, {1: 400, 2: 120})
ok(best["id"] == 1 and "400 emails" in why, "the campaign that SENT most this week wins, not the newest")
best, why = choose_most_active(el, {1: 0, 2: 0})
ok(best["id"] == 2 and "newest active" in why, "nothing sent this week -> newest active")
best, why = choose_most_active([c for c in el if c["status"] != "ACTIVE"], {3: 50})
ok(best["id"] == 3 and "last active" in why, "nothing active -> last active paused campaign")
best, why = choose_most_active([c for c in el if c["status"] != "ACTIVE"], {3: 0})
ok(best is None, "nothing sent at all -> keep the current copy")
print("most-active rule: passed")

pl = [{"id": 7, "status": "COMPLETED", "name": "Data Providers | Bettrdata - Contributor Ingestion", "client_id": None},
      {"id": 8, "status": "PAUSED", "name": "Marketing Agencies | Preciseleads", "client_id": None}]
ok([c["id"] for c in eligible_sources(pl, client_id=None, skip_words=("bettrdata", "melior"))] == [8],
   "PL's own test never copies an old BettrData campaign left on PL's account")
best, why = choose_most_active(pl[1:], {8: 300}, days=30)
ok(best["id"] == 8 and "30 days" in why, "nothing active -> last active in the wider 30-day window")
print("client guards: passed")
