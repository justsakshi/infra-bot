from smartlead.placement_report import (build_report, active_campaign, status_for, counts,
                                        format_slack, inbox_rows, domain_verdict, select_tests)

def ok(c, m): print(f"  {'PASS' if c else 'FAIL'}: {m}"); assert c, m

PROV = {"g.com": "Google", "o.com": "Outlook"}
prov = lambda d: PROV.get(d, "other")

ok(status_for(80) == "good" and status_for(79.9) == "watch" and status_for(49.9) == "bad"
   and status_for(None) == "untested", "mailbox: 80 and up inbox, below 80 spam (below 50 is the worst)")

# The team's domain rule (Anjali, 2026-10-06)
ok(domain_verdict(3, 0) == "good", "3 mailboxes, none in spam -> good")
ok(domain_verdict(3, 1) == "avoid_new", "2 inbox + 1 spam -> still inbox, keep out of new campaigns")
ok(domain_verdict(3, 2) == "spam", "1 inbox + 2 spam -> spam")
ok(domain_verdict(2, 1) == "spam", "exactly half in spam -> spam (close to 50% is spam)")
ok(domain_verdict(3, 3) == "spam", "all in spam -> spam")

ok(active_campaign({"status": "ACTIVE", "campaign_lead_stats": {"notStarted": 3, "inprogress": 0}}), "active with leads waiting")
ok(active_campaign({"status": "ACTIVE", "campaign_lead_stats": {"notStarted": 0, "inprogress": 2}}), "active with leads in progress")
ok(not active_campaign({"status": "ACTIVE", "campaign_lead_stats": {"notStarted": 0, "inprogress": 0}}), "running but nothing left to send is NOT active")
ok(not active_campaign({"status": "PAUSED", "campaign_lead_stats": {"notStarted": 9, "inprogress": 9}}), "paused is not active")

senders = {
    "a@gm.com": [{"seed": "x@g.com", "folder": "Inbox"}, {"seed": "y@g.com", "folder": "Spam"},
                 {"seed": "z@o.com", "folder": "Inbox"}, {"seed": "w@o.com", "folder": ""}],
    "b@out.com": [{"seed": "x@g.com", "folder": "Spam"}, {"seed": "z@o.com", "folder": "Inbox"}],
}
types = {"a@gm.com": "GMAIL", "b@out.com": "OUTLOOK"}
rep = build_report(senders, types, prov, {"b@out.com": ["Camp 1"]})
a, b = rep["inboxes"]["a@gm.com"], rep["inboxes"]["b@out.com"]
ok(a["seeds"] == 3 and a["spam"] == 1, "an unclassified seed is not counted as inbox or spam")
ok(round(a["pct"]) == 67 and a["status"] == "watch", "67% inbox = spam side of 80")
ok(b["pct"] == 50.0 and b["campaigns"] == ["Camp 1"], "50% keeps its active campaign list")
ok(rep["pairs"][("Google", "Google")] == {"seeds": 2, "spam": 1, "inboxes_hit": 1}, "Google -> Google counted")
ok(rep["pairs"][("Outlook", "Google")]["spam"] == 1, "Outlook -> Google spam counted")
ok(a["by_receiver"]["Google"] == {"seeds": 2, "spam": 1}, "per-receiver counts kept per inbox")
ok(counts(rep["inboxes"])["watch"] == 2, "counts by status")
txt = format_slack("X", rep, ["t1"], "2026-10-12")
ok(txt.splitlines()[0] == "*:warning: X deliverability (12 Oct): 2 of 2 mailboxes going to spam*",
   f"headline says how many mailboxes are in spam, got {txt.splitlines()[0]}")
ok("*Remove from live campaigns*\n• b@out.com — from Camp 1" in txt,
   "names the mailbox to remove and the campaign to remove it from")
ok("*Going to spam, in no live campaign*\n• a@gm.com" in txt, "names the spam mailbox that sends nowhere")
ok("t1" not in txt and "→" not in txt, "no test names or provider breakdown in Slack")
clean = format_slack("Y", build_report({"c@gm.com": [{"seed": "x@g.com", "folder": "Inbox"}]}, {"c@gm.com": "GMAIL"}, prov), ["t"])
ok(clean == "", "all clear -> no Slack message")

# one domain: 3 mailboxes, one in spam -> the other two stay usable only for existing campaigns
many = {f"m{i}@d.com": [{"seed": "x@g.com", "folder": "Inbox" if i else "Spam"} for _ in range(5)]
        for i in range(3)}
rep2 = build_report(many, {k: "GMAIL" for k in many}, prov)
ok(rep2["domains"]["d.com"]["verdict"] == "avoid_new", "domain with one spam mailbox of three = avoid_new")
ok(rep2["domains"]["d.com"]["cells"]["Google"] == "Inbox", "grid cell: majority fine -> Inbox")
usable = {r[0]: r[9] for r in inbox_rows(rep2)}
ok(usable == {"m0@d.com": "NO", "m1@d.com": "EXISTING", "m2@d.com": "EXISTING"},
   f"Usable column: spam mailbox NO, its healthy siblings EXISTING, got {usable}")
rep3 = build_report({f"m{i}@e.com": [{"seed": "x@g.com", "folder": "Inbox"}] * 5 for i in range(3)},
                    {f"m{i}@e.com": "GMAIL" for i in range(3)}, prov)
ok({r[9] for r in inbox_rows(rep3)} == {"YES"}, "healthy domain -> every mailbox YES")

# Report-list entries as SmartDelivery returned them on 2026-10-12
auto = lambda i, start, run: {"spam_test_id": i, "test_type": "auto", "status": "ACTIVE", "every_days": 7,
                              "created_at": "2026-10-08T13:50:44.154Z", "schedule_start_time": start,
                              "current_test_run_no": run}
listing = [
    {"spam_test_id": 553145, "test_type": "manual", "status": "ACTIVE", "created_at": "2026-10-12T05:03:47.823Z"},
    auto(550497, "2026-10-13T00:30:00.000Z", 0),
    auto(550493, "2026-10-12T00:30:00.000Z", 1),
    {"spam_test_id": 548461, "test_type": "manual", "status": "COMPLETED", "created_at": "2026-10-06T05:50:20.461Z"},
]
picked = {t["spam_test_id"]: t for t in select_tests(listing, "2026-10-12")}
ok(set(picked) == {550493}, f"a scheduled test that ran today is picked though it is ACTIVE and created 8 Oct, got {set(picked)}")
ok(picked[550493]["created_at"] == "2026-10-12" and picked[550493]["run_no"] == 1, "dated by its run, not its creation")
ok(not select_tests([auto(550493, "2026-10-12T00:30:00.000Z", 1)], "2026-10-19"), "last week's run is not re-picked")
ok(select_tests([auto(550493, "2026-10-12T00:30:00.000Z", 2)], "2026-10-19")[0]["created_at"] == "2026-10-19",
   "run 2 is dated one interval after the start")
ok([t["spam_test_id"] for t in select_tests(listing, "2026-10-06")] == [550493, 548461],
   "a COMPLETED manual test still counts; a running manual one does not")
print("\nALL PASSED")
