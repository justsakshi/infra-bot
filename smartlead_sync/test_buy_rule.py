"""The buying rule (11 Nov 2026): no batch of a vendor + inbox type until six inboxes of that kind have passed
their placement test twice. PL bought 54 Google inboxes in one go and all of them landed in spam. No network."""
from smartlead import buy_rule as br


def _asset(email, provider="Zapmail", workspace="Google"):
    return {"type": "INBOX", "name": email, "provider": provider, "workspace": workspace}


def _v(email, *nights):
    return [{"email": email, "night": n, "verdict": v} for n, v in nights]


TWICE = (("2026-11-02", "PASS"), ("2026-11-09", "PASS"))


def test_vendor_and_type_are_read_whatever_their_spelling():
    assert br.kind_of(_asset("a@x.com", "Scaled Mail", "google")) == ("scaledmail", "google")
    assert br.kind_of(_asset("a@x.com", "Scaledmail", "Outlook")) == ("scaledmail", "outlook")
    assert br.kind_of(_asset("a@x.com", "ZAPMAIL", "Microsoft")) == ("zapmail", "outlook")
    assert br.kind_of({"type": "DOMAIN", "name": "x.com", "provider": "Zapmail"}) is None       # a domain is not an inbox


def test_six_inboxes_passing_twice_prove_a_kind():
    assets = [_asset(f"i{n}@x.com") for n in range(8)]
    verdicts = [d for n in range(6) for d in _v(f"i{n}@x.com", *TWICE)]
    verdicts += _v("i6@x.com", ("2026-11-09", "PASS"))                                   # once is not twice
    verdicts += _v("i7@x.com", ("2026-11-08", "PASS"), ("2026-11-09", "PASS"))          # one day apart: one test week
    got = br.check("Zapmail", "Google", 30, assets, verdicts)
    assert got["proven"] == 6 and got["ok"] is True and got["limit"] is None
    assert got["why"] == "6 Zapmail Google inboxes have passed their placement test twice"


def test_an_unproven_kind_is_a_trial_of_six():
    assets = [_asset(f"i{n}@x.com") for n in range(8)]
    verdicts = [d for n in range(5) for d in _v(f"i{n}@x.com", *TWICE)]
    verdicts += [d for n in range(5, 8) for d in _v(f"i{n}@x.com", ("2026-11-02", "FAIL"), ("2026-11-09", "FAIL"))]
    big = br.check("zapmail", "google", 30, assets, verdicts)
    assert big["ok"] is False and big["limit"] == 6 and big["proven"] == 5
    assert big["why"] == ("only 5 Zapmail Google inboxes have passed their placement test twice (6 needed): "
                          "buy at most 6 as a trial, test them weekly from the first week, and buy more once they pass twice")
    assert br.check("zapmail", "google", 6, assets, verdicts)["ok"] is True              # a trial-sized order is fine
    assert br.check("zapmail", "outlook", 12, assets, verdicts)["proven"] == 0           # another type: nothing proven


def test_a_pass_followed_by_a_fail_does_not_count():
    assets = [_asset(f"i{n}@x.com") for n in range(6)]
    verdicts = [d for n in range(6) for d in _v(f"i{n}@x.com", *TWICE, ("2026-11-16", "FAIL"))]
    assert br.check("zapmail", "google", 30, assets, verdicts)["proven"] == 0             # the newest result decides
