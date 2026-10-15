"""The inboxes we use = the domains in the deliverability grid sheet (Manveen 15 Oct). No network."""
from smartlead import tracked


def test_domains_in_a_tab_skip_headers_and_dead_rows():
    rows = [["", "", "", "21 Jul 2025"],
            ["Domains ", "ESP", "", "G suite"],
            ["askmelior.com", "Google", "Scaledmail", ""],
            ["joinmelior.com", "Google", "New", ""],
            ["smartmelior.com", "Google", "Cancelled", "Inbox"],
            ["discovermelior.com", "Outlook", "Schedule for deletion", ""],
            ["progroupe.co", "Google", "14 Aug", "prewarmed (cancel)"],
            ["sendbettrdata.com", "Google", "09 Jun", "replaced"],
            [" HeyBettrdata.com ", "Google", "09 Jun", "old"],
            ["", "", "", ""]]
    assert tracked.parse_tab(rows) == {"askmelior.com", "joinmelior.com", "heybettrdata.com"}


def test_filter_keeps_only_inboxes_on_tracked_domains():
    accts = [{"from_email": "a@askmelior.com"}, {"from_email": "B@Retainer.com"}]
    assert tracked.only_tracked(accts, {"askmelior.com"}) == [{"from_email": "a@askmelior.com"}]
    assert tracked.only_tracked(accts, None) == accts              # sheet unreadable: keep all, never go blind


def test_filter_reads_either_address_key():
    assert tracked.only_tracked([{"email": "x@askmelior.com"}, {"email": "y@other.com"}], {"askmelior.com"}) == [
        {"email": "x@askmelior.com"}]
