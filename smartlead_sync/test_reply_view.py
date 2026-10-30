"""The "Show replies" button: only the person's own words, never the whole email."""
from datetime import datetime, timezone

from smartlead.reply_view import (clean_email_reply, clean_text, in_window, render,
                                  short_names)

OUTLOOK = """<html><head><meta charset="utf-8"><style type="text/css" style="display:none">
<!--
p
\t{margin-top:0;
\tmargin-bottom:0}
-->
</style></head><body dir="ltr"><div class="elementToProof">Laurie,</div>
<div class="elementToProof"><br></div><div class="elementToProof">Yes, That would be great.&nbsp; Thank you
for the invite.</div><div id="Signature" class="elementToProof"><p><b>Jane Doe<br></b>
<b>VP of Operations</b></p><p><b>p: 555-123-4567 c: 555-765-4321</b><a href="https://example.com/">example.com</a></p></div>
<div id="appendonsend"></div><hr style="display:inline-block;width:98%"><div id="divRplyFwdMsg">
<b>From:</b> Laurie &lt;laurie@bettrdata.io&gt;<br><b>Sent:</b> Wednesday<br>Hi Jane, we are hosting a roundtable...</div>
</body></html>"""

GMAIL = """<div dir="ltr">Sure. Happy to. Shoot me an invite.<div><br></div><div>Thanks</div>
<div><br clear="all"><div><div dir="ltr" class="gmail_signature">John Roe<br>CEO | Acme<br>+1 408 555 0100</div></div></div></div>
<br><div class="gmail_quote"><div dir="ltr" class="gmail_attr">On Thu, Oct 29, 2026 at 11:40 AM Laurie &lt;l@x.io&gt; wrote:<br></div>
<blockquote class="gmail_quote">Hi John, the roundtable is on...</blockquote></div>"""

PLAIN_SIGNOFF = """Yes please!

Lindsey Kaiser
Chief Product Officer
402-555-4008

> On Oct 29, 2026, at 11:41 AM, Laurie <l@x.io> wrote:
> Hi Lindsey"""


def test_outlook_reply_keeps_only_their_words():
    assert clean_email_reply(OUTLOOK) == "Laurie, Yes, That would be great. Thank you for the invite."


def test_gmail_signature_and_quote_are_cut():
    assert clean_email_reply(GMAIL) == "Sure. Happy to. Shoot me an invite. Thanks"


def test_plain_text_signature_block_and_quote_are_cut():
    assert clean_email_reply(PLAIN_SIGNOFF) == "Yes please!"


def test_on_wrote_line_cuts_the_rest_even_inline():
    t = "Not for us, thanks.\nOn Wed, Oct 28, 2026 at 9:00 AM Aaron <a@x.io> wrote:\nHello"
    assert clean_email_reply(t) == "Not for us, thanks."


def test_outlook_plain_header_block_cuts_the_rest():
    t = ("Please remove me.\n\n-----Original Message-----\nFrom: Aaron\nSent: Tuesday\nHi")
    assert clean_email_reply(t) == "Please remove me."
    t2 = "Please remove me.\n________________________________\nFrom: Aaron <a@x.io>\nSent: Tuesday"
    assert clean_email_reply(t2) == "Please remove me."


def test_sent_from_phone_and_disclaimer_are_cut():
    t = ("Call me next week.\n\nSent from my iPhone\n\nCONFIDENTIALITY NOTICE: This e-mail "
         "and any attachments are confidential.")
    assert clean_email_reply(t) == "Call me next week."


def test_long_reply_is_capped():
    out = clean_email_reply("word " * 200)
    assert len(out) <= 301 and out.endswith("…")


def test_a_reply_that_is_only_a_signature_is_not_emptied():
    # A one-line "Thanks" plus a signature must still show something.
    assert clean_email_reply("Jane Doe\nCEO\n555-123-4567") != ""


def test_clean_text_for_linkedin_trims_and_caps():
    assert clean_text("  Hi Kenny.\n\n Thank you  ") == "Hi Kenny. Thank you"
    assert len(clean_text("x" * 900)) <= 301


def test_window_is_from_inclusive_to_exclusive():
    f = datetime(2026, 10, 29, 3, 30, tzinfo=timezone.utc)
    t = datetime(2026, 10, 30, 3, 30, tzinfo=timezone.utc)
    assert in_window("2026-10-29T03:30:00Z", f, t)
    assert in_window("2026-10-29T15:31:08+00:00", f, t)
    assert not in_window("2026-10-30T03:30:00.000Z", f, t)
    assert not in_window("2026-10-28T17:46:23.000Z", f, t)
    assert in_window("2026-10-28T17:46:23.000Z", None, t)
    assert not in_window("garbage", f, t)


def test_short_names_drop_the_shared_middle():
    names = ["Roundtable | AI in the Data Business | Laurie",
             "Roundtable | AI in the Data Business | Aaron | Open tracking",
             "Marketplace | Hard to Find audiences",
             "PL | Clay in Kochi"]
    s = short_names(names)
    assert s[names[0]] == "Roundtable · Laurie"
    assert s[names[1]] == "Roundtable · Aaron · Open tracking"
    assert s[names[2]] == "Marketplace · Hard to Find audiences"
    assert s[names[3]] == "PL · Clay in Kochi"


def test_a_lone_campaign_keeps_its_full_name():
    s = short_names(["Roundtable | AI in the Data Business | Laurie"])
    assert s["Roundtable | AI in the Data Business | Laurie"] == \
        "Roundtable · AI in the Data Business · Laurie"


def test_render_lists_each_reply_with_who_and_category():
    replies = [{"name": "Jane Doe", "company": "Acme", "category": "Meeting Request",
                "time": "2026-10-29T15:09:33Z", "text": "Yes, that would be great."},
               {"name": "", "company": "", "category": "", "time": "2026-10-29T16:00:00Z",
                "text": ""}]
    out = render("Roundtable · Laurie", "Email", replies, "since Thu 29 Oct, 9:00 IST")
    body = "\n".join(b["text"]["text"] for b in out["blocks"] if b["type"] == "section")
    assert "*Jane Doe* · Acme · Meeting Request · 29 Oct, 20:39 IST" in body
    assert "> Yes, that would be great." in body
    assert "(no text)" in body
    assert out["text"].startswith("2 email replies")


def test_render_with_nothing_found_says_so():
    out = render("PL · Clay in Kochi", "Email", [], "since Thu 29 Oct, 9:00 IST")
    assert "No email replies" in out["blocks"][0]["text"]["text"]


def test_render_caps_the_list_and_says_how_many_more():
    replies = [{"name": f"P{i}", "company": "", "category": "Interested",
                "time": "2026-10-29T15:00:00Z", "text": "ok"} for i in range(30)]
    out = render("X", "Email", replies, "since Thu 29 Oct, 9:00 IST")
    assert len(out["blocks"]) <= 50
    assert "15 more" in out["blocks"][-1]["elements"][0]["text"]


def test_only_the_leads_side_of_the_thread_counts():
    from smartlead.reply_view import from_lead
    assert from_lead("amy@speedeon.com", "amy@speedeon.com")
    assert from_lead("Colleague@Speedeon.com", "amy@speedeon.com")
    assert not from_lead("kenny@bettrdata.io", "amy@speedeon.com")   # the client's team
    assert from_lead("", "amy@speedeon.com")


def _r(name, t):
    return {"name": name, "company": "", "category": "Interested", "time": t, "text": "hi"}


def test_late_categorised_reply_fills_the_gap_and_is_marked():
    from smartlead.reply_view import select_replies
    f = datetime(2026, 10, 29, 3, 30, tzinfo=timezone.utc)
    t = datetime(2026, 10, 30, 3, 30, tzinfo=timezone.utc)
    old = [_r("A", "2026-10-27T17:23:40Z"), _r("B", "2026-10-20T10:00:00Z")]
    got = select_replies(old, f, t, counted=1)
    assert [(r["name"], r.get("earlier")) for r in got] == [("A", True)]
    # Enough people in the window: nothing older is added.
    got = select_replies(old + [_r("C", "2026-10-29T10:00:00Z")], f, t, counted=1)
    assert [r["name"] for r in got] == ["C"]


def test_header_counts_people_when_they_wrote_more_than_once():
    replies = [_r("A", "2026-10-29T10:00:00Z"), _r("A", "2026-10-29T11:00:00Z"),
               _r("B", "2026-10-29T12:00:00Z")]
    out = render("X", "Email", replies, "since Thu 29 Oct, 9:00 IST")
    assert "3 email replies from 2 people since" in out["blocks"][0]["text"]["text"]


def test_a_sign_off_never_takes_the_answer_with_it():
    # Found in review: the loop dropped paragraph after paragraph.
    t = "Hi Manveen,\n\nYes, let's talk next week.\nThursday works.\n\nBest,\nJohn"
    assert clean_email_reply(t) == "Hi Manveen, Yes, let's talk next week. Thursday works."
    h = '<div dir="ltr">Hi,<br><br>Sure, call me on 415-555-1234.<br><br>Thanks,<br>Ann</div>'
    assert clean_email_reply(h) == "Hi, Sure, call me on 415-555-1234."


def test_fill_in_looks_back_two_weeks_only():
    from smartlead.reply_view import select_replies
    f = datetime(2026, 10, 30, 3, 30, tzinfo=timezone.utc)
    t = datetime(2026, 10, 31, 3, 30, tzinfo=timezone.utc)
    assert select_replies([_r("Old", "2026-10-01T10:00:00Z")], f, t, counted=1) == []


def test_two_unnamed_leads_are_two_people():
    a = dict(_r("", "2026-10-29T10:00:00Z"), lead="a@x.com")
    b = dict(_r("", "2026-10-29T11:00:00Z"), lead="b@y.com")
    out = render("X", "Email", [a, b], "since Thu 29 Oct, 9:00 IST")
    assert "2 email replies since" in out["blocks"][0]["text"]["text"]


def test_expandi_log_rows_match_by_campaign_and_day():
    from smartlead.reply_view import expandi_log_replies
    head = ["Full Name", "Job Title", "Compay Name", "Website", "Person LinkedIn", "Location",
            "Lead Category", "Reply date", "Campaign Name", "Linkedin", "Reply from Lead"]
    rows = [head,
            ["Ann", "", "Acme", "", "li/ann", "", "Neutral", "Oct 29, 2026", "Marketplace  - New Agencies", "", "Tell me more"],
            ["Bob", "", "Beta", "", "li/bob", "", "Interested", "Oct 27, 2026", "Marketplace - New Agencies", "", "yes"],
            ["Cy", "", "Co", "", "li/cy", "", "Interested", "Oct 29, 2026", "Other", "", "yes"]]
    f = datetime(2026, 10, 29, 3, 30, tzinfo=timezone.utc)
    t = datetime(2026, 10, 30, 3, 30, tzinfo=timezone.utc)
    got = expandi_log_replies(rows, "marketplace - new agencies", f, t)
    assert [(r["name"], r["time"], r["lead"]) for r in got] == [("Ann", "2026-10-29", "li/ann")]
