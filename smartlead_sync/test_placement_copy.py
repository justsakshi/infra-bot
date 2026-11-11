"""Copy-refresh tests. The failure being guarded against: an email reaching
the seed panel with literal {{merge_field}} braces, or with another client's
copy, and the resulting spam verdict being written against a healthy domain.
"""
from smartlead.placement_copy import (
    resolve_merge_fields, pick_source_campaign, build_variants, copy_hash,
    sample_lead_fields, merge_names, first_signature, step_variants,
)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

FIELDS = {"first_name": "Dana", "providers_line": "Alesco, ADS and Averick sit behind one login."}
SIG = "<div>Aaron Dix<br>BettrData</div>"

# --- merge fields ---
text, missing = resolve_merge_fields("Hi {{first_name}}, {{providers_line}} Bye.<br>%signature%", FIELDS, SIG)
ok(text.startswith("Hi Dana,"), "standard field filled too: seed inboxes are not leads")
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

# --- sampling: one lead with every field the email uses ---
STEP2 = {"sequence_variants": [{"variant_label": "A", "subject": "{{company_name}} question",
                                "email_body": "Hi {{first_name}}, {{providers_line}}"}]}
ok(merge_names(STEP2) == {"company_name", "first_name", "providers_line"}, "finds fields in subject and body")
leads = [{"lead": {"first_name": "Ann", "company_name": "", "custom_fields": {"providers_line": "X."}}},
         {"lead": {"first_name": "", "company_name": "Acme", "custom_fields": {"providers_line": "Y."}}},
         {"lead": {"first_name": "Raj", "company_name": "Brio", "email": "raj@brio.com",
                   "custom_fields": {"providers_line": "Z."}}}]
sample = sample_lead_fields(leads, merge_names(STEP2))
ok(sample["email"] == "raj@brio.com", "skips leads with any field empty, takes the first complete one")
v, problems = build_variants(STEP2, sample, SIG)
ok(v[0]["subject"] == "Brio question" and v[0]["email_body"] == "Hi Raj, Z." and not problems,
   f"whole email filled from that one lead, got {v}")
ok(sample_lead_fields(leads[:2], merge_names(STEP2)) is None, "no complete lead -> None (copy is kept)")
ok(sample_lead_fields([], set()) is None, "no leads at all -> None")
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
best, why = choose_most_active(el, {1: 400, 2: 120, 3: 900})
ok(best["id"] == 3 and "most sent this week: 900" in why, "most sent this week wins, even if it is paused now")
best, why = choose_most_active(el, {1: 0, 2: 0, 3: 0}, {1: 50, 2: 700})
ok(best["id"] == 2 and "last week: 700" in why, "nothing sent this week -> most sent last week")
best, why = choose_most_active(el, {1: 5}, {2: 700})
ok(best["id"] == 1, "any send this week beats last week")
best, why = choose_most_active(el, {}, {})
ok(best is None, "nothing sent in 14 days -> keep the current copy")
best, why = choose_most_active(el, {1: 400, 2: 120}, {}, {1: 0, 2: 300})
ok(best["id"] == 2 and "300 leads left" in why, "a campaign still sending beats a bigger one that has run out of leads")
best, why = choose_most_active(el, {3: 50}, {2: 900}, {2: 300})
ok(best["id"] == 3, "recency first: anything sent this week beats last week")
best, why = choose_most_active(el, {}, {1: 900, 2: 100}, {1: 0, 2: 40})
ok(best["id"] == 2 and "last week" in why, "last week too: running campaigns first")
print("most-active rule: passed")

pl = [{"id": 7, "status": "COMPLETED", "name": "Data Providers | Bettrdata - Contributor Ingestion", "client_id": None},
      {"id": 8, "status": "PAUSED", "name": "Marketing Agencies | Preciseleads", "client_id": None}]
ok([c["id"] for c in eligible_sources(pl, client_id=None, skip_words=("bettrdata", "melior"))] == [8],
   "PL's own test never copies an old BettrData campaign left on PL's account")
print("client guards: passed")


# --- the neutral note: weekly inbox tests send a fixed plain email, not campaign copy -----------------

from smartlead import placement_copy as pc  # noqa: E402

class _FakeSmartlead:
    """Just the three calls write_neutral_note makes."""
    def __init__(self, steps, track):
        self.steps, self.track, self.writes, self.settings = steps, track, [], []

    async def _get(self, endpoint):
        return self.steps if endpoint.endswith("/sequences") else {"track_settings": self.track}

    async def save_campaign_sequence_variants(self, cid, step_id, variants):
        self.writes.append((cid, step_id, variants))

    async def update_campaign_settings(self, cid, settings):
        self.settings.append((cid, settings))


class _FakeStore:
    def __init__(self):
        self.snaps = []

    def save_copy_snapshot(self, account, cid, current):
        self.snaps.append((account, cid))


def _neutral_step():
    return {"id": 7, "seq_number": 1, "sequence_variants": [
        {"subject": pc.NEUTRAL_SUBJECT, "email_body": pc.NEUTRAL_BODY, "variant_label": "A"}]}


def test_the_neutral_note_has_nothing_a_filter_could_blame_on_the_copy():
    text = pc.NEUTRAL_SUBJECT + " " + pc.NEUTRAL_BODY
    assert "{{" not in text and "%" not in text and "http" not in text.lower()      # no merge field, token or link
    assert not __import__("re").search(r"\{[^{}|]*\|", text)                         # no spintax
    assert pc.is_neutral(_neutral_step(), pc.NEUTRAL_TRACKING)
    assert not pc.is_neutral(_neutral_step(), [])                                    # tracking still on
    two = _neutral_step(); two["sequence_variants"].append({"subject": "x", "email_body": "y"})
    assert not pc.is_neutral(two, pc.NEUTRAL_TRACKING)                               # a second variant is campaign copy
    assert not pc.is_neutral({"id": 7, "sequence_variants": [{"subject": "AI Roundtable", "email_body": "Hi Steve"}]},
                             pc.NEUTRAL_TRACKING)


def test_write_neutral_note_replaces_campaign_copy_and_turns_tracking_off():
    import asyncio
    c = _FakeSmartlead([{"id": 7, "seq_number": 1, "sequence_variants": [
        {"subject": "AI Roundtable", "email_body": "Hi Steve"}, {"subject": "B", "email_body": "b"}]}], [])
    store = _FakeStore()
    dry = asyncio.run(pc.write_neutral_note(c, "ACC", 55, 7, store, dry_run=True))
    assert dry["written"] is False and "would write" in dry["reason"] and not c.writes and not store.snaps
    r = asyncio.run(pc.write_neutral_note(c, "ACC", 55, 7, store))
    assert r["written"] is True and store.snaps == [("ACC", 55)]                    # the old copy is kept
    assert c.writes == [("55", 7, [{"subject": pc.NEUTRAL_SUBJECT, "email_body": pc.NEUTRAL_BODY, "variant_label": "A"}])]
    assert c.settings == [("55", {"track_settings": ["DONT_TRACK_EMAIL_OPEN", "DONT_TRACK_LINK_CLICK"]})]


def test_write_neutral_note_leaves_a_campaign_that_already_has_it():
    import asyncio
    c = _FakeSmartlead([_neutral_step()], ["DONT_EMAIL_OPEN", "DONT_LINK_CLICK"])
    r = asyncio.run(pc.write_neutral_note(c, "ACC", 55, 7, _FakeStore()))
    assert r["written"] is False and "already" in r["reason"] and not c.writes and not c.settings


def test_copy_sync_sends_dt_campaigns_the_neutral_note_and_the_rest_live_copy():
    import copy_sync
    assert copy_sync.is_inbox_test("DT PL #1") and copy_sync.is_inbox_test("DT BettrData #12")
    assert not copy_sync.is_inbox_test("DT Melior copy check")      # a copy check must carry campaign copy
    assert not copy_sync.is_inbox_test("Melior - Deliverability Test")
    assert not copy_sync.is_inbox_test("DT Unknown #1")             # no known client prefix


# --- the copy check: one small campaign per client that carries the real first email ------------------

import asyncio  # noqa: E402
from datetime import date  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import pytest  # noqa: E402

TODAY = date(2026, 11, 11)


def test_a_copy_check_campaign_is_known_by_its_name_and_is_never_an_inbox_test():
    import copy_sync
    for name in ("DT PL copy check", "DT Melior Copy Check", "DT BettrData COPY CHECK"):
        assert copy_sync.is_copy_check(name) and not copy_sync.is_inbox_test(name)
    assert copy_sync.copy_check_prefix("DT Melior copy check") == "DT Melior"
    assert copy_sync.is_inbox_test("DT PL #1") and not copy_sync.is_copy_check("DT PL #1")
    for name in ("DT Unknown copy check", "Melior copy check", "DT PL copy check 2", ""):
        assert not copy_sync.is_copy_check(name) and not copy_sync.is_inbox_test(name)     # never a guessed client


def box(i, email, kind="GMAIL", **kw):
    return {"id": i, "from_email": email, "type": kind, "is_smtp_success": True, "is_imap_success": True,
            "tags": [], "client_id": None, **kw}


def row(email, verdict="PASS", night="2026-11-08"):
    return {"email": email, "night": night, "verdict": verdict}


def test_copy_check_senders_are_capped_at_five_per_type_and_two_per_domain():
    boxes = [box(n, f"a@g{n}.com") for n in range(1, 8)] + [box(20 + n, f"o{n}@out.com", "OUTLOOK") for n in range(4)]
    boxes += [box(30 + n, f"a@o{n}.com", "OUTLOOK") for n in range(1, 6)]
    rows = [row(b["from_email"]) for b in boxes]
    picked = pc.pick_copy_check_senders(boxes, rows, TODAY)
    assert [i for i in picked if i < 20] == [1, 2, 3, 4, 5]                      # 5 of the 7 Google inboxes
    assert [i for i in picked if i >= 20] == [31, 32, 33, 34, 35]               # 5 Outlook: a@o1..o5 sort before out.com
    one_domain = [box(n, f"s{n}@same.com") for n in range(1, 5)]
    assert pc.pick_copy_check_senders(one_domain, [row(b["from_email"]) for b in one_domain], TODAY) == [1, 2]


def test_copy_check_senders_must_be_connected_untagged_and_passing_now():
    boxes = [box(1, "good@a.com"), box(2, "failed@b.com"), box(3, "watch@c.com"), box(4, "untested@d.com"),
             box(5, "stale@e.com"), box(6, "tagged@f.com", tags=[{"tag_name": "Do Not Use - failed v2 (auto)"}]),
             box(7, "nosmtp@g.com", is_smtp_success=False), box(8, "noimap@h.com", is_imap_success=False),
             box(9, "relay@i.com", "SMTP"), box(10, "edge@j.com"), box(11, "named@k.com", tags=[{"name": "DO NOT USE"}]),
             box(12, "kept@l.com", "outlook", tags=[{"tag_name": "warm"}])]
    rows = [row(b["from_email"]) for b in boxes if b["id"] not in (2, 3, 4, 5, 10)]
    rows += [row("failed@b.com", "FAIL"), row("watch@c.com", "WATCH"), row("stale@e.com", night="2026-10-31"),
             row("edge@j.com", night="2026-11-01")]                              # 11 days old, and exactly 10
    assert pc.pick_copy_check_senders(boxes, rows, TODAY) == [1, 10, 12]


def test_the_newest_judged_verdict_decides_and_the_order_never_churns():
    boxes = [box(1, "b@z.com"), box(2, "a@z.com"), box(3, "c@a.com"), box(4, "back@m.com"), box(5, "gone@n.com")]
    rows = [row("b@z.com"), row("a@z.com"), row("c@a.com"),
            row("back@m.com", "FAIL", "2026-11-02"), row("back@m.com", "PASS", "2026-11-08"),
            row("gone@n.com", "PASS", "2026-11-02"), row("gone@n.com", "FAIL", "2026-11-08"),
            row("c@a.com", "INCOMPLETE", "2026-11-10")]                          # not judged: the pass before it stands
    picked = pc.pick_copy_check_senders(boxes, rows, TODAY)
    assert picked == [3, 4, 2, 1]                                                # by domain, then address
    assert pc.pick_copy_check_senders(list(reversed(boxes)), list(reversed(rows)), TODAY) == picked


class _SourceSL:
    """Smartlead as refresh_test_campaign sees it: one drafted Melior campaign, PL's own, a busy Melior one."""
    campaigns = [{"id": 2, "name": "DT Melior copy check", "status": "PAUSED", "client_id": None},
                 {"id": 555, "name": "Melior | New draft", "status": "DRAFTED", "client_id": 12256},
                 {"id": 556, "name": "PL | Own", "status": "ACTIVE", "client_id": None},
                 {"id": 557, "name": "Melior | Busy", "status": "ACTIVE", "client_id": 12256},
                 {"id": 558, "name": "DT Melior #1", "status": "PAUSED", "client_id": 12256}]

    def __init__(self):
        self.writes, self.analytics = [], 0

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def list_campaigns(self):
        return list(self.campaigns)

    async def _get(self, endpoint, params=None):
        cid = int(endpoint.split("/")[2])
        return [{"id": 7, "seq_number": 1, "sequence_variants": [
            {"variant_label": "A", "subject": f"subject of {cid}", "email_body": "Hi {{first_name}}"}]}]

    async def get_campaign_leads(self, cid):
        return [{"lead": {"first_name": "Ann", "email": "ann@lead.com"}}]

    async def get_campaign_email_accounts(self, cid):
        return [{"signature": ""}]

    async def get_analytics_by_date(self, cid, since, until):
        self.analytics += 1
        return {"sent_count": 500 if int(cid) == 557 else 0}

    async def get_campaign_analytics(self, cid):
        return {"campaign_lead_stats": {"notStarted": 10, "inprogress": 0}}

    async def save_campaign_sequence_variants(self, cid, step_id, variants):
        self.writes.append((int(cid), step_id, variants))


@pytest.fixture
def source_sl(monkeypatch):
    import smartlead.api as api
    sl = _SourceSL()
    monkeypatch.setattr(api, "SmartleadClient", lambda key, name: sl)
    acc = SimpleNamespace(name="PRECISE_LEADS", api_key="k")
    sl.refresh = lambda **kw: asyncio.run(pc.refresh_test_campaign(acc, 2, 7, _FakeStore(), source_client_id=12256, **kw))
    return sl


def test_a_named_source_is_used_even_as_a_draft_and_the_most_active_one_is_not_asked(source_sl):
    dry = source_sl.refresh(source_campaign_id=555, dry_run=True)
    assert dry["written"] is False and dry["source"] == 555 and dry["hash"] and not source_sl.writes
    r = source_sl.refresh(source_campaign_id=555)
    assert r["written"] is True and r["source"] == 555 and "--source" in r["reason"]
    assert source_sl.writes == [(2, 7, [{"subject": "subject of 555", "email_body": "Hi Ann", "variant_label": "A"}])]
    assert source_sl.analytics == 0                                # no "most active" search for a named source
    usual = source_sl.refresh()                                    # no source named: the most active one, as before
    assert usual["written"] is True and usual["source"] == 557


def test_a_named_source_of_another_client_or_a_test_campaign_is_refused(source_sl):
    other = source_sl.refresh(source_campaign_id=556)              # PL's own campaign on Melior's copy check
    assert other["written"] is False and other["hash"] == "" and "another client" in other["reason"]
    gone = source_sl.refresh(source_campaign_id=999)
    assert gone["written"] is False and "not in this account" in gone["reason"]
    for cid in (2, 558):                                           # itself, and an inbox-test campaign
        assert "test campaign" in source_sl.refresh(source_campaign_id=cid)["reason"]
    assert source_sl.writes == []
    assert pc.named_source([{"id": 9, "name": "Agencies | Bettrdata old", "client_id": None}], 9, None,
                           skip_words=("bettrdata",))[0] is None   # an old campaign of another client, no client id


class _SyncSL:
    """Smartlead as copy_sync.main sees it: campaigns, inboxes and each campaign's senders. Records every write."""

    def __init__(self, campaigns=(), inboxes=(), attached=None):
        self.campaigns = [dict(c) for c in campaigns]
        self.inboxes = [dict(a) for a in inboxes]
        self.attached = {int(k): set(v) for k, v in (attached or {}).items()}
        self.calls, self.fail_adds = [], False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def list_campaigns(self):
        return [dict(c) for c in self.campaigns]

    async def list_email_accounts(self):
        return list(self.inboxes)

    async def _get(self, endpoint, params=None):
        if endpoint.endswith("/sequences"):
            return [{"id": 7, "seq_number": 1, "sequence_variants": [{"subject": "s", "email_body": "b"}]}]
        return {"track_settings": ["DONT_EMAIL_OPEN"], "scheduler_cron_value": {"tz": "Asia/Kolkata", "days": [1]}}

    async def get_campaign_email_accounts(self, cid):
        return [{"id": i} for i in sorted(self.attached.get(int(cid), set()))]

    async def add_campaign_email_accounts(self, cid, ids):
        self.calls.append(("add", int(cid), list(ids)))
        if not self.fail_adds:
            self.attached.setdefault(int(cid), set()).update(ids)

    async def remove_campaign_email_accounts(self, cid, ids):
        self.calls.append(("remove", int(cid), list(ids)))
        self.attached[int(cid)] -= set(ids)

    # the calls dt_senders._clone makes
    async def create_campaign(self, name):
        self.calls.append(("create", name))
        self.campaigns.append({"id": 900, "name": name, "status": "DRAFTED"})
        return {"id": 900}

    async def save_campaign_sequences(self, cid, subject, body):
        self.calls.append(("step", int(cid), subject))

    async def get_campaign_leads(self, cid):
        return [{"lead": {"email": "placeholder@lead.com"}}]

    async def _request_json(self, method, endpoint, body):
        self.calls.append((method, endpoint))

    async def update_campaign_settings(self, cid, settings):
        self.calls.append(("settings", int(cid), settings))

    async def update_campaign_schedule(self, cid, schedule):
        self.calls.append(("schedule", int(cid)))

    async def set_campaign_status(self, cid, status):
        self.calls.append(("status", int(cid), status))


MELIOR = 12256
PL_CAMPS = [{"id": 1, "name": "DT PL #1", "status": "PAUSED"}, {"id": 3, "name": "DT Melior #2", "status": "PAUSED"},
            {"id": 4, "name": "DT Melior #1", "status": "PAUSED"},
            {"id": 2, "name": "DT Melior copy check", "status": "PAUSED"}]
PL_BOXES = [box(31, "m1@mm.com", client_id=MELIOR), box(32, "m2@mm.com", "OUTLOOK", client_id=MELIOR),
            box(33, "m3@mm.com", client_id=MELIOR), box(34, "m4@other.com", client_id=MELIOR),
            box(41, "own@pp.com"), box(99, "old@gone.com", client_id=MELIOR)]
PL_ROWS = [row("m1@mm.com"), row("m2@mm.com"), row("m3@mm.com"), row("m4@other.com"), row("own@pp.com"),
           row("old@gone.com", "FAIL")]


@pytest.fixture
def sync(monkeypatch):
    import copy_sync
    h = SimpleNamespace(sl={"PRECISE_LEADS": _SyncSL(PL_CAMPS, PL_BOXES, {2: {31, 99}}), "BETTRDATA": _SyncSL()},
                        refreshed=[], neutral=[], verdicts=list(PL_ROWS), verdict_error=None, posts=[])
    monkeypatch.setattr(copy_sync, "discover_accounts", lambda: [SimpleNamespace(name=n, api_key="k") for n in h.sl])
    monkeypatch.setattr(copy_sync, "SmartleadClient", lambda key, name: h.sl[name])
    monkeypatch.setattr(copy_sync, "PlacementStore", lambda: object())

    async def fake_refresh(acc, test_id, step, store, dry_run=False, **kw):
        h.refreshed.append((int(test_id), kw))
        return {"written": not dry_run, "source": kw.get("source_campaign_id") or 700, "hash": "abc", "reason": "ok"}

    async def fake_neutral(c, account, test_id, step, store, dry_run=False):
        h.neutral.append(int(test_id))
        return {"written": not dry_run, "source": None, "hash": "n", "reason": "neutral note"}

    def fake_verdicts(today):
        if h.verdict_error:
            raise h.verdict_error
        return list(h.verdicts)
    monkeypatch.setattr(copy_sync, "refresh_test_campaign", fake_refresh)
    monkeypatch.setattr(copy_sync, "write_neutral_note", fake_neutral)
    monkeypatch.setattr(copy_sync, "read_verdicts", fake_verdicts)
    monkeypatch.setattr(copy_sync, "_today", lambda: TODAY)
    monkeypatch.setattr(copy_sync, "_post_summary", lambda written, problems: h.posts.append((written, list(problems))))
    h.run = lambda *argv: asyncio.run(copy_sync.main(list(argv)))
    h.pl = h.sl["PRECISE_LEADS"]
    return h


def test_a_copy_check_campaign_gets_live_copy_and_no_unknown_prefix_warning(sync, capsys):
    assert sync.run("--only", "PRECISE_LEADS") == 0
    assert sorted(sync.neutral) == [1, 3, 4]                       # the inbox tests: the neutral note
    (kw,) = [kw for cid, kw in sync.refreshed if cid == 2]         # the copy check: live copy, Melior's own
    assert kw["source_client_id"] == MELIOR and not kw.get("source_campaign_id")
    assert "no known client prefix" not in capsys.readouterr().out
    sync.pl.campaigns.append({"id": 5, "name": "DT Nobody copy check", "status": "PAUSED"})
    assert sync.run("--only", "PRECISE_LEADS") == 1               # an unknown client is still never guessed
    assert "'DT Nobody copy check' has no known client prefix" in capsys.readouterr().out


def test_source_loads_one_named_campaign_into_one_copy_check_and_nothing_else(sync, capsys):
    assert sync.run("--apply", "--only-copy-check", "DT Melior", "--source", "555") == 0
    assert sync.refreshed == [(2, {"source_client_id": MELIOR, "skip_words": (), "source_campaign_id": 555})]
    assert sync.neutral == [] and sync.posts == []                 # no other campaign, no channel post
    with pytest.raises(SystemExit):
        sync.run("--apply", "--source", "555")                     # a source needs its copy check named
    assert sync.run("--apply", "--only-copy-check", "DT BettrData") == 1
    assert "no 'DT BettrData copy check' campaign" in capsys.readouterr().out


def test_a_refused_source_fails_the_run(sync, monkeypatch, capsys):
    import copy_sync

    async def refused(acc, test_id, step, store, dry_run=False, **kw):
        return {"written": False, "source": None, "hash": "", "reason": "source campaign 556 belongs to another client"}
    monkeypatch.setattr(copy_sync, "refresh_test_campaign", refused)
    assert sync.run("--apply", "--only-copy-check", "DT Melior", "--source", "556") == 1
    assert "belongs to another client" in capsys.readouterr().out


def test_copy_check_senders_follow_the_inboxes_that_pass(sync, capsys):
    assert sync.run("--apply", "--only", "PRECISE_LEADS") == 0
    # Melior's passing inboxes only (PL's own@pp.com stays out), two per domain; the failed one comes off
    assert [c for c in sync.pl.calls if c[0] in ("add", "remove")] == [("add", 2, [32, 34]), ("remove", 2, [99])]
    assert sync.pl.attached[2] == {31, 32, 34}
    assert sync.posts == [(6, [])]                                 # one line in the channel, no problem
    sync.pl.calls.clear()
    assert sync.run("--apply", "--only", "PRECISE_LEADS") == 0
    assert not [c for c in sync.pl.calls if c[0] in ("add", "remove")]          # already in step: nothing to do


def test_a_dry_run_only_says_what_it_would_attach_and_detach(sync, capsys):
    assert sync.run("--only", "PRECISE_LEADS") == 0
    assert sync.pl.calls == [] and sync.pl.attached[2] == {31, 99}
    out = capsys.readouterr().out
    assert "would attach m2@mm.com, m4@other.com" in out and "would detach old@gone.com" in out


def test_the_campaign_keeps_its_senders_when_nobody_qualifies(sync, capsys):
    sync.verdicts = [row("m1@mm.com", "FAIL"), row("own@pp.com")]
    assert sync.run("--apply", "--only", "PRECISE_LEADS") == 0
    assert sync.pl.calls == [] and sync.pl.attached[2] == {31, 99}
    assert "no inbox qualifies" in capsys.readouterr().out


def test_unreadable_verdicts_change_no_sender_and_the_copy_is_still_refreshed(sync, capsys):
    sync.verdict_error = RuntimeError("no route to mongodb+srv://user:pw@cluster.example.net")
    assert sync.run("--apply", "--only", "PRECISE_LEADS") == 1
    assert sync.pl.calls == [] and sync.pl.attached[2] == {31, 99}
    assert 2 in [cid for cid, _ in sync.refreshed]                 # the copy refresh went on
    (written, problems), = sync.posts
    assert len(problems) == 1 and "v2_verdicts" in problems[0] and "RuntimeError" in problems[0]
    assert "user:pw" not in problems[0] and "user:pw" not in capsys.readouterr().out


def test_a_sender_that_did_not_attach_is_a_problem(sync):
    sync.pl.fail_adds = True
    assert sync.run("--apply", "--only", "PRECISE_LEADS") == 1
    assert "not attached" in sync.posts[0][1][0]


def test_a_copy_check_campaign_that_is_not_paused_keeps_its_senders(sync):
    sync.pl.campaigns[3]["status"] = "ACTIVE"
    assert sync.run("--apply", "--only", "PRECISE_LEADS") == 1
    assert sync.pl.calls == [] and "ACTIVE" in sync.posts[0][1][0]


def test_create_copy_check_clones_the_first_inbox_test_campaign_and_leaves_it_paused(sync, capsys):
    pl = sync.pl
    assert sync.run("--create-copy-check", "DT PL") == 0           # dry run
    assert pl.calls == [] and "would create 'DT PL copy check' by cloning 'DT PL #1'" in capsys.readouterr().out
    pl.inboxes.append(box(42, "two@pp.com", "OUTLOOK"))
    sync.verdicts.append(row("two@pp.com"))
    assert sync.run("--create-copy-check", "DT PL", "--apply") == 0
    assert ("create", "DT PL copy check") in pl.calls and ("status", 900, "PAUSED") in pl.calls
    assert not any(c[0] == "status" and c[2] != "PAUSED" for c in pl.calls)      # never started
    assert ("settings", 900, {"track_settings": ["DONT_EMAIL_OPEN"]}) in pl.calls   # tracking as the clone gave it
    assert sync.refreshed == [(900, {"source_client_id": None, "skip_words": ("bettrdata", "melior"),
                                     "source_campaign_id": None})]
    assert pl.attached[900] == {41, 42} and sync.neutral == []     # PL's own passing inboxes, no other campaign
    n = len(pl.calls)
    assert sync.run("--create-copy-check", "DT PL", "--apply") == 1              # never a second one
    assert "already exists" in capsys.readouterr().out and len(pl.calls) == n


def test_create_copy_check_clones_number_one_and_says_so_when_there_is_nothing_to_clone(sync, capsys):
    sync.pl.campaigns = [c for c in sync.pl.campaigns if c["id"] != 2]
    assert sync.run("--create-copy-check", "DT Melior") == 0
    assert "by cloning 'DT Melior #1' (4)" in capsys.readouterr().out            # #1, not the first one listed
    assert sync.run("--create-copy-check", "DT BettrData", "--apply") == 1
    assert "no 'DT BettrData #n' campaign to clone" in capsys.readouterr().out
