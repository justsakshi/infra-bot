"""Per-sender placement from `/spam-test/report/{id}/sender-account-wise`.

This endpoint is documented at api.smartlead.ai but we did not find it until
2026-09-15, having concluded from guessed 404s that per-sender reporting did
not exist. It does, and it is strictly better than `/providerwise`:

  providerwise         -> two provider totals for the WHOLE test, so a test
                          carrying several senders blends them together and
                          cannot produce a per-domain verdict.
  sender-account-wise  -> one row per seed mailbox per sender, each carrying
                          the folder it landed in plus SPF/DKIM/DMARC and the
                          receiving MX.

The consequence is that one test can carry many senders and still be scored
per domain — turning a 19-credit weekly sweep into roughly one, and removing
the concurrency problem that stalled the 2026-09-11 batch entirely.

Verified live: test 531861 returns 15 seeds per sender (they are NOT shared
between senders), and 532019 returns 7 Inbox / 7 Spam for a single sender.
"""
from __future__ import annotations

# Anything not in the spam folder reached the person. Gmail's category tabs are
# tracked separately because a Promotions landing is worth knowing about, but
# it is delivery, not a failure.
_SPAM_FOLDERS = {"spam", "junk"}
_TAB_FOLDERS = {"promotions", "social", "updates", "forums", "tab"}

# Which provider judged a seed is NOT derivable from this payload. Both the
# seed address (always *smartdelivery.com) and rdns_result are properties of
# the SENDING path — on test 532019 all fourteen seeds report a 1e100.net
# rdns, including the six that providerwise attributes to Office365. That is
# Smartlead's own Google relay, not the receiving provider.
#
# So: sender-account-wise is authoritative for per-sender and per-folder
# placement; providerwise is authoritative for the provider split. Neither
# alone is enough, and inferring the provider here would silently mis-score
# every Microsoft seed as Google.


def _auth_failed(result: dict | None, key: str) -> bool:
    """SPF reports a `status` field; DKIM and DMARC put the verdict inside a
    free-text blob (`mx.google.com; dkim=pass ...`), so both shapes are read."""
    if not result:
        return False
    status = str(result.get("status", "")).strip().lower()
    if status:
        return status.startswith("fail")
    return f"{key}=fail" in str(result.get(key, "")).lower()


def summarize_sender(details: list[dict]) -> dict:
    """Score one sender's seed panel.

    No provider split here — see the note above. `inbox_pct` is across the
    whole panel, which is the right verdict for a single-sender test (where
    providerwise can supply the split) and the only available one for a
    multi-sender test.
    """
    inbox = spam = tab = 0
    spf_fail = dkim_fail = dmarc_fail = 0

    for row in details or []:
        reply = row.get("reply") or {}
        folder = str(reply.get("mail_folder", "")).strip().lower()

        if folder in _SPAM_FOLDERS:
            spam += 1
        elif folder in _TAB_FOLDERS:
            tab += 1
        else:
            inbox += 1

        spf_fail += _auth_failed(reply.get("spf_result"), "spf")
        dkim_fail += _auth_failed(reply.get("dkim_result"), "dkim")
        dmarc_fail += _auth_failed(reply.get("dmarc_result"), "dmarc")

    classified = inbox + spam + tab
    # A tab landing is delivery, so it counts toward inbox for the verdict.
    delivered = inbox + tab
    return {
        "classified": classified, "inbox": inbox, "spam": spam, "tab": tab,
        "inbox_pct": round(100.0 * delivered / classified, 1) if classified else 0.0,
        "spam_pct": round(100.0 * spam / classified, 1) if classified else 0.0,
        "spf_fail": spf_fail, "dkim_fail": dkim_fail, "dmarc_fail": dmarc_fail,
        # A sender whose seeds have not been classified yet is unmeasured, not
        # failing. Writing 0% for it is how a healthy domain gets retired.
        "has_data": classified > 0,
    }


def summarize_senders(payload: list[dict]) -> dict[str, dict]:
    """{sender email: summary} for a whole test."""
    out: dict[str, dict] = {}
    for entry in payload or []:
        email = str(entry.get("email", "")).strip()
        if email:
            out[email] = summarize_sender(entry.get("details") or [])
    return out
