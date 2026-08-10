# Expandi Shared Campaign Uploaded Total — Design

**Date:** 2026-08-10
**Status:** Approved (pending written spec review)

## Purpose

Make the automated BettrData Outbound Tracker report the current number of
contacts uploaded to an Expandi Shared Campaign. For `Market Place: Agencies`
(shared campaign `63078`), the dashboard must show the live uploaded total
`1,146`, not the `120` contacts currently assigned across its two sending
instances.

The target is the `Dashboard` tab (`gid=939718146`) in the existing BettrData
Outbound Tracker workbook. The change belongs in the existing daily metrics
sync; it does not create a new dashboard or workbook.

## Verified Sources and Semantics

The authenticated Expandi application uses this request for the Shared
Campaign's uploaded contacts:

```http
GET https://api.liaufa.com/api/v1/campaign-contacts/
    ?page=1
    &page_size=1
    &batch__campaign=63078
Authorization: Bearer <short-lived access token>
```

The live response is paginated and its top-level `count` is `1,146`. Only the
count is required; the sync must not page through or store the contact records.

The documented Open API endpoint remains the source for campaign-instance
activity:

```http
GET https://api.liaufa.com/api/v1/open-api/v2/li_accounts/{id}/campaign_instances/
```

Its `stats.people_in_campaign` values are `60 + 60 = 120`. This means
"active batch assigned", not "total leads uploaded". The two values must not
be substituted for one another.

The manually maintained tracker currently shows `1,151` for this row. That is
an older manual value and is five contacts above the current Expandi source of
truth. A successful sync should replace it with `1,146`.

## Scope

In scope:

- Read the top-level `count` for a configured Expandi Shared Campaign.
- Map shared campaign `63078` to the merged `Market Place: Agencies` Expandi
  metrics row.
- Use that count for the existing `total_leads` column.
- Preserve the merged `people_in_campaign` value separately as
  `active_batch_assigned` for internal use and future display.
- Write the resulting row through the existing Campaign Metrics/Google Sheets
  path used by the daily sync.
- Cache the last verified uploaded total with its retrieval timestamp.
- Verify the live value with a read-only smoke test before enabling the sheet
  write.

Out of scope:

- Downloading or storing all Shared Campaign contacts.
- Changing connection, message, response, or campaign-instance calculations.
- Automating Expandi contact uploads.
- Rebuilding the Outbound Tracker workbook.
- Treating a five-minute browser access token as a permanent secret.

## Authentication Gate

The internal endpoint uses a Bearer access token, not the Open API `key` and
`secret` headers. The observed access token has a five-minute lifetime.

Implementation starts with a read-only authentication spike:

1. Observe the Expandi application's supported token renewal request.
2. Determine whether renewal can run unattended using a dedicated server-side
   credential already authorized for this workspace.
3. Confirm the renewed token can call `campaign-contacts` and returns the same
   `count` as the UI.
4. Do not log, commit, print, or store access or refresh tokens in Mongo or
   Google Sheets.

If an unattended renewal flow cannot be established, the private endpoint is
not production-safe. In that case, implementation stops before sheet writes
and reports the blocker. Browser-session scraping is not the production
fallback.

## Architecture

```text
Expandi authenticated session
  -> obtain short-lived access token through verified renewal flow
  -> GET /api/v1/campaign-contacts/?page_size=1&batch__campaign=<id>
  -> uploaded_total = response.count
  -> cache {workspace, shared_campaign_id, count, retrieved_at}

Expandi Open API
  -> fetch campaign instances
  -> merge instances by shared campaign/name
  -> active_batch_assigned = sum(stats.people_in_campaign)

row assembly
  -> total_leads = uploaded_total
  -> all activity metrics remain instance-derived
  -> existing Sheets writer updates the Outbound Tracker Dashboard
```

### Configuration

Shared-campaign mappings must be configuration, not campaign-name guesses:

```text
workspace: BETTRDATA
shared_campaign_id: 63078
dashboard_campaign_name: Market Place: Agencies
```

The implementation may use a JSON environment value or an existing structured
configuration pattern. No token value belongs in this mapping.

### Client Boundary

The internal endpoint client is separate from `ExpandiClient`, which represents
the documented Open API and authenticates with `key` plus `secret`. The new
client has one narrow operation:

```python
get_shared_campaign_uploaded_total(shared_campaign_id: int) -> int
```

It validates that `count` exists, is an integer, and is non-negative. It does
not expose contact records to callers.

## Failure and Freshness Behavior

- On a successful fetch, write the new count and cache it with `retrieved_at`.
- On a transient failure, use the last verified cached count only when it is no
  more than 48 hours old, and emit a warning in the sync log with the cached
  value and retrieval time.
- When no verified count exists, do not relabel `people_in_campaign` as total
  leads. Emit the repository's existing unknown marker (`?`) for the uploaded
  total and leave activity metrics intact.
- A `401` triggers one token renewal and one retry. Repeated authentication
  failure stops the Shared Campaign total fetch for that run.
- `429`, transport errors, and `5xx` responses use the repository's bounded
  retry/backoff pattern.
- The sync must never overwrite `1,146` with `120` merely because the private
  endpoint is unavailable.

## Sheet Mapping

For the Expandi `Market Place: Agencies` row:

| Dashboard field | Source |
|---|---|
| Campaign name | Existing merged campaign identity |
| Platform | `Expandi` |
| Total leads | Internal endpoint `count` (`1,146` at verification time) |
| Active batch assigned | Sum of instance `people_in_campaign` (`120`) |
| Connections sent/accepted | Existing instance statistics |
| Messages and responses | Existing instance/per-lead metrics |

The initial workbook change uses the existing `Total leads` column only.
`active_batch_assigned` is preserved in the row model but does not require a
new visible spreadsheet column in this change.

## Testing and Rollout

1. Add a failing unit test for a `campaign-contacts` response with
   `count=1146`; implement the smallest client needed to pass it.
2. Test invalid, missing, negative, unauthorized, rate-limited, and transient
   responses.
3. Add a row-assembly regression test proving:
   - uploaded total `1,146` becomes `total_leads`;
   - active assigned `120` remains separate;
   - connection and response fields do not change.
4. Test stale-cache and no-cache behavior so `120` can never silently appear
   as the uploaded total.
5. Run the existing Expandi and Campaign Metrics test suites.
6. Run a live read-only smoke test for shared campaign `63078`; require
   `count=1,146` or explicitly report the newer live value if it changed.
7. Run the metrics sync in dry-run mode and inspect the generated
   `Market Place: Agencies` row.
8. Only after those checks, allow the normal daily job to write the existing
   Dashboard tab.

## Success Criteria

- The daily Outbound Tracker row uses Expandi's current Shared Campaign
  uploaded-contact count.
- `Market Place: Agencies` reports `1,146` at the verified snapshot rather than
  `120` or the stale manual `1,151`.
- Activity metrics retain their existing meanings and values.
- Authentication renews unattended without browser scraping or committed
  secrets.
- Endpoint failure cannot cause an incorrect `120` to be shown as total leads.
- Tests and a dry-run demonstrate the mapping before the first sheet write.
