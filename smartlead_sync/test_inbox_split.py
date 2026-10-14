"""Per-client split of branded vs pre-warmed inboxes (2026-10-14). No network."""
from smartlead import inbox_split as sp


ZAP = [  # (account, domain row) as Zapmail lists them
    ("PRECISE_LEADS", {"domain": "bettrdatasgroup.com", "isWarmedUp": False, "assignedMailboxesCount": 3}),
    ("PRECISE_LEADS", {"domain": "gohivecloud.co", "isWarmedUp": True, "assignedMailboxesCount": 3}),
    ("PRECISE_LEADS", {"domain": "debutix.co", "isWarmedUp": True, "assignedMailboxesCount": 0}),     # no inboxes
    ("PRECISE_LEADS", {"domain": "mystery.co", "isWarmedUp": True, "assignedMailboxesCount": 2}),
]
SM = [{"domain": "meliorbuild.com", "client": "Melior", "mailboxes": 3},
      {"domain": "warmmeetings.com", "client": "Precise Leads", "mailboxes": 3}]
WHERE = {"gohivecloud.co": "Bettrdata"}          # client from the Smartlead account the inboxes sit in


def test_split_by_client_and_kind():
    infer = lambda d, a: "Bettrdata" if "bettrdata" in d else None
    out = sp.split(ZAP, SM, WHERE, infer)
    assert out == {"Bettrdata": {"branded": 3, "prewarmed": 3}, "Melior": {"branded": 3, "prewarmed": 0},
                   "Precise Leads": {"branded": 3, "prewarmed": 0}, "no client": {"branded": 0, "prewarmed": 2}}


def test_split_line():
    line = sp.split_line({"Bettrdata": {"branded": 45, "prewarmed": 27}, "Melior": {"branded": 155, "prewarmed": 0},
                          "Precise Leads": {"branded": 133, "prewarmed": 0}, "no client": {"branded": 0, "prewarmed": 2}})
    assert line == ("By client: Melior 155 branded · Precise Leads 133 branded · "
                    "Bettrdata 45 branded + 27 pre-warmed · no client 2 pre-warmed")


def test_client_of_smartlead_inbox():
    assert sp.client_of_account("BETTRDATA", None, 12256) == "Bettrdata"
    assert sp.client_of_account("PRECISE_LEADS", 12256, 12256) == "Melior"
    assert sp.client_of_account("PRECISE_LEADS", None, 12256) == "Precise Leads"
