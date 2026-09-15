"""Per-sender placement parsing.

`/spam-test/report/{id}/sender-account-wise` returns one row per seed mailbox
per sender, each carrying the folder the mail landed in plus SPF/DKIM/DMARC.
That is what makes a multi-sender test usable: `/providerwise` blends every
sender together and so forces one test per domain.

What this payload does NOT carry is which provider judged each seed. Verified
on test 532019: all fourteen seeds report a `1e100.net` rdns, yet providerwise
attributes six of them to Office365 — the rdns is Smartlead's own sending
relay, not the receiving MX. Inferring the provider from it would mis-score
every Microsoft seed as Google.

Shapes taken from live responses: 531861 (2 senders, 15 seeds each, all
Inbox) and 532019 (1 sender, 7 Inbox / 7 Spam).
"""
from smartlead.sender_report import summarize_senders, summarize_sender

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m


def seed(folder, spf="PASS", dkim="pass", dmarc="pass"):
    return {"id": 1, "email": "x@blogsmartdelivery.com", "reply": {
        "mail_folder": folder,
        "rdns_result": {"rdns": "sb-in-f27.1e100.net", "status": True},
        "spf_result": {"spf": "x", "status": spf},
        "dkim_result": {"dkim": f"mx.google.com; dkim={dkim}"},
        "dmarc_result": {"dmarc": f"dmarc={dmarc}"},
        "blacklist_status": None,
    }}


# --- the real 532019 shape: half the panel in spam ---
payload = [{"email": "aaron.dix@bettrdatasgroup.com",
            "details": [seed("Inbox") for _ in range(7)] + [seed("Spam") for _ in range(7)]}]
out = summarize_senders(payload)
r = out["aaron.dix@bettrdatasgroup.com"]
ok(r["classified"] == 14, f"14 seeds counted, got {r['classified']}")
ok(r["inbox"] == 7 and r["spam"] == 7, "7 inbox / 7 spam")
ok(r["inbox_pct"] == 50.0, f"50% of the panel delivered, got {r['inbox_pct']}")
ok(r["spam_pct"] == 50.0, "50% spam")
ok(r["has_data"], "a classified panel has data")
ok("by_provider" not in r,
   "no provider split is claimed - this payload cannot support one")

# --- many senders in ONE test, each judged separately. This is the point. ---
payload = [
    {"email": "a@clean.com", "details": [seed("Inbox") for _ in range(15)]},
    {"email": "b@dirty.com", "details": [seed("Spam") for _ in range(15)]},
]
out = summarize_senders(payload)
ok(set(out) == {"a@clean.com", "b@dirty.com"}, "one entry per sender")
ok(out["a@clean.com"]["inbox_pct"] == 100.0, "clean sender scores 100%")
ok(out["b@dirty.com"]["inbox_pct"] == 0.0, "dirty sender scores 0%")
ok(out["a@clean.com"]["classified"] == 15 and out["b@dirty.com"]["classified"] == 15,
   "each sender gets its own full seed panel - panels are not shared between senders")

# --- a sender with no seeds yet must not read as a 0% failure ---
out = summarize_senders([{"email": "c@pending.com", "details": []}])
ok(not out["c@pending.com"]["has_data"], "no seeds -> has_data False, never a failure")
ok(out["c@pending.com"]["classified"] == 0, "zero classified")
ok(out["c@pending.com"]["inbox_pct"] == 0.0,
   "0% is reported but has_data is what callers must gate on")

# --- tab placement is delivery, not spam ---
r = summarize_sender([seed("Promotions"), seed("Inbox")])
ok(r["tab"] == 1, "promotions tab counted separately")
ok(r["spam"] == 0, "a tab landing is not spam")
ok(r["inbox_pct"] == 100.0, "tab counts toward delivered for the verdict")

# --- authentication rolled up from the per-seed results ---
r = summarize_sender([seed("Inbox", spf="PASS", dkim="pass"),
                      seed("Inbox", spf="FAIL", dkim="fail")])
ok(r["spf_fail"] == 1, f"one SPF failure counted, got {r['spf_fail']}")
ok(r["dkim_fail"] == 1, f"one DKIM failure counted, got {r['dkim_fail']}")

# --- malformed rows must not crash a whole batch ---
r = summarize_sender([{"email": "x@y.com"}, {"email": "z@y.com", "reply": {}}])
ok(r["classified"] == 2, "rows with a missing/empty reply still count as classified")
ok(summarize_senders([{"details": [seed("Inbox")]}]) == {},
   "an entry with no sender email is skipped rather than keyed on empty string")
ok(summarize_senders([]) == {}, "empty payload -> empty result")

print("\nALL PASSED")
