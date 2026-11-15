"""Smart Delivery v2 results from the API, as the rows of the CSV download. GET only.

v2 has an API after all (Smartlead support, 15 Nov 2026; checked read-only the same day with both keys):
base https://sl-sd-api.smartlead.ai/api/v1/smart-delivery, every call with ?api_key=<the account's normal key>.
  GET /tests                 {items: [{id, name, type: AUTO|MANUAL, status: ACTIVE|STOPPED|COMPLETED}], nextCursor}
  GET /tests/{id}/history    {from, to, items: [{runId, runNo, completedAt, score, band, counts}], nextCursor}
  GET /runs/{runId}/emails   {items: [{provider, senderEmail, placement, sentAt, bounce, ...}], nextCursor}, 50 a page

Each completed run becomes the rows v2_results.parse() makes of that run's CSV download (``rows``), under the
name Smartlead gives the download (``csv_name``), so v2_weekly.py reads a run the same way whichever way it came.
The key travels in the query string: an error raised here never carries the URL or the HTTP library's message,
only what was being read and the HTTP status (``V2ApiError``); callers print ``describe(exc)``.
"""
from __future__ import annotations

import csv
import io
import re
import time
from datetime import date, datetime

from smartlead.v2_results import HEADER, IST

BASE = "https://sl-sd-api.smartlead.ai/api/v1/smart-delivery"
MAX_PAGES = 200             # a listing that keeps handing out cursors stops here (PL: 32 tests on 2 pages)
TIMEOUT = 20.0              # seconds a call may take (15 Nov: all of PL read in a few seconds)
MAX_ERRORS = 3              # an account whose calls keep failing is left for the next hourly run
RATE_WAIT = 15.0             # seconds to wait after HTTP 429, doubled on the next try (the account allows
                            # 200 calls a minute, shared with other tools; 16 Nov 2026 the first live read hit it)
RATE_RETRIES = 2            # tries after a 429 before the call counts as failed

# API placement -> the CSV's Landed value. Checked on the 14 Nov runs against their CSVs: MISSING is the CSV's
# "Never arrived" (pending until 48 h after sending, then final: v2_results), NOT_SENT stays NOT_SENT. TAB is "Inbox (tab)" in
# the CSV (Smartlead support, 15 Nov 2026). A bounce is BOUNCED as in the CSV (no bounced row seen yet).
# Any other placement keeps its own name, which v2_results never counts as final: never judged, never acted on.
LANDED = {"INBOX": "Inbox", "SPAM": "Spam", "TAB": "Inbox (tab)", "MISSING": "Never arrived",
          "NOT_SENT": "NOT_SENT", "BOUNCED": "BOUNCED"}
PLACED = {"INBOX", "SPAM", "TAB"}


class V2ApiError(RuntimeError):
    """A v2 API call that did not answer as expected. Its text names what was read and the HTTP status only."""

    def __init__(self, what: str, status: int | None = None):
        super().__init__(f"{what}{f' (HTTP {status})' if status is not None else ''}")
        self.status = status


def describe(exc: BaseException) -> str:
    """What may be printed about an error: a V2ApiError's own text (built from what was read, the HTTP status and
    an exception type only), else the exception's type. Never another error's text: an HTTP library's holds the
    URL, and so the key."""
    return str(exc) if isinstance(exc, V2ApiError) else type(exc).__name__


class Client:
    """One account's v2 API. ``get``: (path, params without the key) -> (status, json); for tests."""

    def __init__(self, api_key: str, get=None, timeout: float = TIMEOUT, sleep=None):
        self._key = api_key
        self._fake = get
        self._timeout = timeout
        self._sleep = sleep or time.sleep

    def _once(self, path: str, what: str, params: dict) -> tuple[int, dict | None, float | None]:
        """(status, json body or None, seconds the server asked to wait or None)."""
        try:
            if self._fake is not None:
                status, body = self._fake(path, dict(params))
                return status, body, None
            import httpx
            r = httpx.get(BASE + path, params={"api_key": self._key, **params}, timeout=self._timeout)
            try:
                after = float(r.headers.get("retry-after") or "")
            except ValueError:
                after = None
            return r.status_code, (r.json() if r.status_code == 200 else None), after
        except Exception as exc:  # noqa: BLE001 - the library's text holds the URL with the key
            raise V2ApiError(f"{what}: {type(exc).__name__}") from None

    def _get(self, path: str, what: str, **params) -> dict:
        status, body, after = self._once(path, what, params)
        for n in range(RATE_RETRIES):
            if status != 429:
                break
            self._sleep(min(after, 60.0) if after and after > 0 else RATE_WAIT * (n + 1))
            status, body, after = self._once(path, what, params)
        if status != 200:
            raise V2ApiError(what, status)
        if not isinstance(body, dict) or not isinstance(body.get("items"), list):
            raise V2ApiError(f"{what}: unexpected reply")
        return body

    def _all(self, path: str, what: str) -> list[dict]:
        items, cursor, seen = [], None, set()
        for _ in range(MAX_PAGES):
            body = self._get(path, what, **({"cursor": cursor} if cursor else {}))
            items += [x for x in body["items"] if isinstance(x, dict)]
            cursor = body.get("nextCursor")
            if not cursor or cursor in seen:
                return items
            seen.add(cursor)
        raise V2ApiError(f"{what}: more than {MAX_PAGES} pages")

    def tests(self) -> list[dict]:
        return self._all("/tests", "tests")

    def history(self, test_id: str) -> list[dict]:
        return self._all(f"/tests/{test_id}/history", "test history")

    def emails(self, run_id: str) -> list[dict]:
        return self._all(f"/runs/{run_id}/emails", "run emails")


# --- runs as CSV rows ------------------------------------------------------------------

def slug(name: str) -> str:
    """Smartlead's file name for a test: 'pl weekly spam test v2 synced 2' -> 'pl-weekly-spam-test-v2-synced-2'."""
    return re.sub(r"[^a-z0-9]+", "-", str(name or "").lower()).strip("-")


def csv_name(test_name: str, run_no) -> str:
    """The name Smartlead gives a run's CSV download. v2_weekly tells a copy check by its file name, so a run read
    from the API carries this name."""
    return f"{slug(test_name)}-run-{run_no}.csv"


_COPY_SUFFIX = re.compile(r"\s*\(\d+\)$")


def run_key(file_name: str) -> str:
    """One name for a run whichever way it came: 'X-run-1 (2).csv' (a browser's second download) -> 'x-run-1'."""
    stem = str(file_name or "").strip().lower()
    if stem.endswith(".csv"):
        stem = stem[:-4]
    return _COPY_SUFFIX.sub("", stem).strip()


def landed(email: dict) -> str:
    p = str(email.get("placement") or "").strip().upper()
    if p not in PLACED and (p == "BOUNCED" or email.get("bounce")):
        return "BOUNCED"
    return LANDED.get(p, p)


def rows(emails: list[dict]) -> list[dict]:
    """The rows v2_results.parse() makes of the same run's CSV: mailbox, provider, landed, sent_at, send_status."""
    out = []
    for e in emails:
        mb = str(e.get("senderEmail") or "").strip().lower()
        if "@" not in mb:
            continue
        where = landed(e)
        out.append({"mailbox": mb, "provider": str(e.get("provider") or "").strip(), "landed": where,
                    "sent_at": str(e.get("sentAt") or "").strip(),
                    "send_status": "SKIPPED" if where == "NOT_SENT" else "SENT"})
    return out


def to_csv(rows_: list[dict]) -> str:
    """The rows as a v2 CSV (v2_results.HEADER), so a run read from the API is kept and judged again like a
    download. v2_results.parse(to_csv(rows)) == rows."""
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(HEADER)
    for r in rows_:
        w.writerow([r["mailbox"], r["provider"], r["landed"], r["sent_at"], r["send_status"]])
    return buf.getvalue()


def completed_on(run: dict) -> date | None:
    """India date the run completed on, or None while it runs."""
    try:
        return datetime.fromisoformat(str(run.get("completedAt") or "").replace("Z", "+00:00")).astimezone(IST).date()
    except ValueError:
        return None


def completed_runs(client: Client, since: date, skip=None) -> tuple[list[dict], list[str]]:
    """Every run of every test that completed on or after ``since`` (India date), with its rows:
    [{test_id, test, status, run_id, run_no, completed_at, name, rows}], and one line per test or run that could
    not be read (``describe``). ``skip(run)`` -> True: the run is not fetched (already read, or its CSV was).
    A failing test or run is left out and the others are still read; a failing test list raises. After
    MAX_ERRORS failures the account is left for the next run, so a hanging API cannot hold up the hourly job."""
    out, errors = [], []
    for t in client.tests():
        tid, tname = str(t.get("id") or ""), str(t.get("name") or "")
        if len(errors) >= MAX_ERRORS:
            errors.append(f"stopped after {MAX_ERRORS} errors; the rest is read next run")
            break
        if not tid:
            continue
        try:
            history = client.history(tid)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"'{tname}': {describe(exc)}")
            continue
        for h in history:
            done = completed_on(h)
            if not done or done < since or not h.get("runId"):
                continue
            run = {"test_id": tid, "test": tname, "status": str(t.get("status") or ""), "run_id": str(h["runId"]),
                   "run_no": h.get("runNo"), "completed_at": str(h.get("completedAt")),
                   "name": csv_name(tname, h.get("runNo"))}
            if skip and skip(run):
                continue
            try:
                run["rows"] = rows(client.emails(run["run_id"]))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"'{tname}' run {h.get('runNo')}: {describe(exc)}")
                continue
            out.append(run)
    return out, errors
