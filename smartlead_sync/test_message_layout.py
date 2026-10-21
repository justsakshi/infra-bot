"""Slack layout (Manveen, 21 Oct 2026: "right spacing ... so it looks natural"):
bold title, one blank line between parts, plain dates. Pure, no network."""
from datetime import datetime, timezone

from smartlead import notify


def test_title_is_bold_after_its_emoji():
    assert notify.title("⚠️ Kill warnings, 13 Oct") == "⚠️ *Kill warnings, 13 Oct*"
    assert notify.title(":rotating_light: Key alert") == ":rotating_light: *Key alert*"
    assert notify.title("Volume review, 21 Oct") == "*Volume review, 21 Oct*"
    # already bold, a list line, a quote or a code line: left alone
    for line in ("🩺 *Inbox Health: 2 inboxes need attention*", "• a@x.com", "> quoted", "`code`", ""):
        assert notify.title(line) == line


def test_tidy_puts_one_blank_line_under_the_title_and_never_two():
    assert notify.tidy("⚠️ Title\nfact 1\nfact 2") == "⚠️ *Title*\n\nfact 1\nfact 2"
    assert notify.tidy("✅ Title\n\n\n\npart  \n\n") == "✅ *Title*\n\npart"
    assert notify.tidy("✅ Only a title") == "✅ *Only a title*"


def test_tidy_on_a_thread_reply_only_fixes_spacing():
    assert notify.tidy("*Heading*\nline \n\n\nnext", headline=False) == "*Heading*\nline\n\nnext"


def test_post_sends_the_tidy_text(monkeypatch):
    sent = []

    class R:
        def json(self):
            return {"ok": True, "ts": "1.0"}

    monkeypatch.setattr(notify.requests, "post", lambda url, headers=None, json=None, timeout=None: sent.append(json) or R())
    notify._post("t", "C1", "⚠️ Billing watch\nline")
    notify._post("t", "C1", "reply\n\n\nmore", thread_ts="1.0")
    assert sent[0]["text"] == "⚠️ *Billing watch*\n\nline"
    assert sent[1]["text"] == "reply\n\nmore" and sent[1]["thread_ts"] == "1.0"


def test_plain_dates():
    assert notify.day("2026-11-08") == "8 Nov"
    assert notify.day("2026-11-01T00:00:00Z") == "1 Nov"
    assert notify.day("next week") == "next week"


def test_sync_failure_text_is_plain_and_in_ist():
    t = notify.sync_failure_text([{"tab": "Melior", "error": "APIError: [429] Quota exceeded"}], 9,
                                 datetime(2026, 10, 21, 4, 35, tzinfo=timezone.utc))
    assert t == ("⚠️ *Sheet sync: 1 of 9 tabs not written*\n\n• `Melior`: APIError: [429] Quota exceeded\n\n"
                 "Details on the Last Sync tab · 21 Oct, 10:05 IST")


def test_health_digest_layout():
    rows = [{"priority": "P0", "client": "Bettrdata", "email": "a@x.com", "top_problem": "Failed test",
             "owner": "", "manager": "", "_mgr_slack": "U01"}]
    msg = notify.build_digest(rows, "https://s")
    assert msg.startswith("🩺 *Inbox Health: 1 inbox needs attention*\n\n<https://s|Open the workbook>\n\n")
    assert "*Bettrdata* · <@U01> · 1 item, 1 🔴 P0\n• `P0` a@x.com · Failed test" in msg
    assert "  " not in msg and " — " not in msg


def test_billing_watch_line_has_plain_dates():
    from datetime import date
    from smartlead import billing_watch as bw
    ev = [{"event": "renewed", "provider": "Zapmail", "label": "21 Google inboxes", "clients": ["Melior"],
           "detail": "renewed — next bill 2026-11-08"}]
    head, _ = bw.format_events(ev, date(2026, 10, 21))
    assert head[1] == ":white_check_mark: *Zapmail · 21 Google inboxes* (Melior) · renewed, next bill 8 Nov"


def test_zapmail_billing_line_plain_date_and_no_question_mark():
    from datetime import date
    from zapmail_digest import billing_lines
    lines = billing_lines([{"bills_on": "2026-10-23", "mailboxes": None, "provider": "GOOGLE", "price": 9, "clients": []}],
                          today=date(2026, 10, 21))
    assert lines == ["• 23 Oct: Google inboxes $9.00"]
