# Expandi API — adding and pausing leads, and the cross-channel pause

**Written 2026-08-06.** Endpoint shapes below come from the live OpenAPI spec at
`https://api.liaufa.com/open-swagger.json`, not from the marketing docs.

**Status: not yet usable.** The `EXPANDEE_API_KEY` / `EXPANDEE_SECRET` pair in
`.env` returns `{"error": "invalid credentials"}`. Both values are well-formed
UUIDs with no whitespace damage, and the endpoint answers with a structured
error rather than an auth wall, so the request shape is right and the
credentials themselves are the problem — most likely not yet activated, or
issued for a different workspace. **Every request example below is unverified
until that is resolved.** See [Before you trust this](#before-you-trust-this).

---

## Why this matters: the double-touch problem

A lead reached on both channels gets messaged twice. Someone who replies on
LinkedIn keeps receiving email sequences; someone who replies to email keeps
getting LinkedIn follow-ups. It reads as careless, and on the email side
continuing to send after a reply is the behaviour that hurts deliverability.

The fix is a pause in the *other* channel the moment a reply lands in one:

```
reply on Smartlead  ──▶  pause that person in Expandi
reply on Expandi    ──▶  pause that person in Smartlead
```

The obstacle is identity: Smartlead knows an **email address**, Expandi knows a
**LinkedIn profile URL**. Neither will accept the other's identifier. See
[The identity problem](#the-identity-problem) — it is the real work here, not
the API calls.

---

## Authentication

Credentials go in **headers**, on every request. There is no login step and no
bearer token.

| Header | Value |
| --- | --- |
| `key` | `EXPANDEE_API_KEY` |
| `secret` | `EXPANDEE_SECRET` |
| `Content-Type` | `application/json` |

Base URL — note the unusual path, which is easy to get wrong:

```
https://api.liaufa.com/api/v1/open-api/v2
```

> Expandi's API runs on the `liaufa.com` domain. That is expected, not a
> misconfiguration.
>
> An earlier probe of this API used `/api/v1` and got 401s everywhere, which
> looked like bad credentials but was the wrong base path. If you see 401 where
> you expect 400, check the path before suspecting the key.

---

## Pausing a lead in Expandi

There is no endpoint named "pause". The mechanism is `update_contact` with
`active: false`, which stops the campaign from progressing that contact while
leaving them in place.

```http
PATCH /li_accounts/campaign_instances/{campaign_instance_id}/update_contact/
```

```json
{
  "profile_link": "https://www.linkedin.com/in/some-person",
  "active": false
}
```

Set `"active": true` to resume. Prefer this over `delete_contact` — deleting
loses the record of them having been in the campaign, and a deleted contact can
be re-added later by a list refresh, restarting the sequence you meant to stop.

### Removing entirely

```http
DELETE /li_accounts/campaign_instances/{campaign_instance_id}/delete_contact/
```

```json
{ "profile_link": "https://www.linkedin.com/in/some-person" }
```

Use for unsubscribes and complaints, where the person should not reappear.

---

## Adding a lead to an Expandi campaign

```http
POST /li_accounts/campaign_instances/{campaign_instance_id}/create_contact/
```

```json
{
  "profile_link": "https://www.linkedin.com/in/some-person",
  "placeholders": { "first_name": "Ada", "company": "Example Ltd" }
}
```

`placeholders` is optional and maps to the campaign's merge fields.

---

## Finding the campaign_instance_id

Every write call needs one. Two GETs get you there:

```http
GET /li_accounts/                              → LinkedIn accounts (id, name)
GET /li_accounts/{id}/campaign_instances/      → campaigns for that account
```

Both are paginated (`count`, `next`, `previous`, `results`) — follow `next`, or
you will silently process only the first page.

---

## Reading replies out of Expandi

For the Expandi → Smartlead direction you need to know who replied. Two routes:

**Webhooks (recommended).** Expandi pushes an event when a lead replies. Push
means you act within seconds; polling means a lead keeps receiving email until
the next poll.

**Messages endpoint (polling fallback).**

```http
GET /li_accounts/messengers/{id}/messages/
```

Lists messages between one lead and one LinkedIn account. Note the shape: it is
per-conversation, so a sweep across a whole campaign costs one call per lead.
That is slow and rate-limit-prone — fine for a handful of active conversations,
not for a nightly full scan.

---

## Pausing on the Smartlead side

Already implemented — `smartlead_sync/smartlead/api.py`:

```python
await client.pause_lead(campaign_id, lead_id)
await client.resume_lead(campaign_id, lead_id)
```

---

## The identity problem

**This is the hard part.** The API calls above are simple; matching a person
across the two systems is not.

Smartlead identifies a lead by **email**. Expandi identifies them by **LinkedIn
profile URL**. To pause across channels you need a mapping between the two, and
neither system will give it to you.

### The policy (decided 2026-08-06)

**Every lead uploaded to Smartlead carries its LinkedIn profile URL as a custom
field. Every lead pushed into Expandi carries its email address.** In each case,
whenever that value is available.

This makes the join exact and permanent, and it costs nothing when both channels
are built from the same source list — the identifier is already in hand at
upload time.

### Two rules that follow from it

**Populate at upload time, not later.** The mapping cannot be reconstructed
after the fact. A lead uploaded without its counterpart identifier stays
unmappable for the life of the campaign.

**Skip loudly when an identifier is missing.** Log and move on; never guess. A
wrong pause silently stops outreach to someone who never replied, and nothing in
either system will show you it happened. A missed pause is the cheaper error.

### Leads already in flight

Anything uploaded before this policy has no mapping. For those, matching on name
+ company is the only fallback — and it is a poor one: names are not unique,
company names are spelled inconsistently, and people change jobs. If you use it,
log every match for human review rather than pausing on it directly.

This backlog shrinks on its own as old campaigns finish. It is not worth a
retrofit unless a specific campaign justifies the manual work.

---

## Sketch: the pause path

Unverified — credentials do not yet authenticate. Written to be read, not
pasted.

```python
import os, json, urllib.request

BASE = "https://api.liaufa.com/api/v1/open-api/v2"
HEADERS = {
    "key": os.environ["EXPANDEE_API_KEY"],
    "secret": os.environ["EXPANDEE_SECRET"],
    "Content-Type": "application/json",
}


def pause_in_expandi(campaign_instance_id: int, profile_link: str) -> dict:
    """Stop an Expandi campaign from progressing one contact.

    `active: false` rather than delete_contact: deleting loses the record of
    them having been in the campaign, and a later list refresh can re-add them,
    restarting the sequence this call exists to stop.
    """
    req = urllib.request.Request(
        f"{BASE}/li_accounts/campaign_instances/{campaign_instance_id}/update_contact/",
        data=json.dumps({"profile_link": profile_link, "active": False}).encode(),
        headers=HEADERS,
        method="PATCH",
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode("utf-8", "replace"))
```

Wiring it to replies, in both directions:

```python
# Smartlead reply -> pause in Expandi
for lead in replied_leads_from_smartlead():
    # Reads the LinkedIn URL custom field set at upload. Empty means the lead
    # predates the identity policy, or had no URL available.
    profile = linkedin_url_for(lead["email"])
    if not profile:
        log.warning("no LinkedIn URL for %s — cannot pause in Expandi", lead["email"])
        continue                                 # skip loudly; never guess
    pause_in_expandi(campaign_instance_id, profile)

# Expandi reply -> pause in Smartlead
for profile in replied_profiles_from_expandi():
    email = email_for(profile)
    if not email:
        log.warning("no email for %s — cannot pause in Smartlead", profile)
        continue
    await client.pause_lead(campaign_id, lead_id_for(email))
```

An unmapped lead should log and skip. A wrong pause is worse than a missed one:
it silently stops outreach to someone who never replied, and nothing in either
system will show you it happened.

---

## What the API cannot do

Reporting still has to come from somewhere else. The spec has **18 endpoints
and two GETs** — one lists campaigns, one lists messages in a conversation.

The campaign object returns `id`, `created`, `updated`, `name`, `li_account`.
**No counts, no connections sent or accepted, no reply totals.** There is no
statistics endpoint at any path.

So this API is for *pushing leads into Expandi and pausing them* — it cannot
populate a metrics dashboard. Those numbers come from webhooks (events only,
from go-live forward) or a manual CSV export.

---

## Full endpoint list

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/li_accounts/` | List LinkedIn accounts |
| GET | `/li_accounts/{id}/campaign_instances/` | List campaigns for an account |
| GET | `/li_accounts/messengers/{id}/messages/` | Messages with one lead |
| POST | `/li_accounts/campaign_instances/{id}/create_contact/` | Add contact to campaign |
| PATCH | `/li_accounts/campaign_instances/{id}/update_contact/` | **Pause / resume** (`active`) |
| DELETE | `/li_accounts/campaign_instances/{id}/delete_contact/` | Remove from campaign |
| POST | `/li_accounts/searches/{id}/create_contact/` | Add contact to a search |
| DELETE | `/li_accounts/searches/{id}/delete_contact/` | Remove from a search |
| POST | `/li_accounts/{id}/actions/connection_request/` | Send connection request |
| POST | `/li_accounts/{id}/actions/message/` | Send LinkedIn message |
| POST | `/li_accounts/{id}/actions/email/` | Send email |
| POST | `/li_accounts/{id}/actions/open_imail/` | Send open InMail |
| POST | `/li_accounts/actions/{id}/check_action_status/` | Check a scheduled action |

The `actions/*` endpoints send directly, outside any campaign. They bypass
campaign pacing and safety limits — LinkedIn rate-limits and restricts accounts
that send too fast, so prefer campaigns for anything at volume.

---

## Before you trust this

1. **Fix the credentials.** Confirm with Expandi that the key/secret are
   activated and belong to the workspace you mean to touch.
2. **Verify with a GET first.** `GET /li_accounts/` is read-only and proves auth
   without changing anything. Do not debug credentials with a PATCH.
3. **Test the pause on one contact you control**, then confirm in the Expandi UI
   that the sequence actually stopped. `200 OK` means the request was accepted,
   not that the campaign halted.
4. **Then wire it to replies** — and only after the identity mapping exists.
