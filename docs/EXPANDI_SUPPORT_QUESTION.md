# Draft question for Expandi support

Copy the message below. Everything in it has been verified against the live API
today (2026-08-07) — the endpoints, the IDs and the counts are real, so support
can reproduce it directly.

---

## The message

**Subject:** Reading Shared Campaign lead totals via the open API

Hi,

We're building an automated reporting dashboard on top of the Expandi open API
(`https://api.liaufa.com/api/v1/open-api/v2`) for workspace **86904**
(BettrData.io), and we've hit one gap we can't solve from the documentation.

**What we can read.** `GET /li_accounts/{id}/campaign_instances/` returns each
campaign instance with its `stats` object. For our Shared Campaign "Market
Place: Agencies" that gives us two instances:

| Instance | LinkedIn account | `people_in_campaign` |
| --- | --- | --- |
| 813733 | Kenny Roush (182607) | 60 |
| 813732 | Aaron Dix (154923) | 60 |

So the API tells us **120** leads.

**What we can see in the UI but not the API.** Opening the Shared Campaign at
`app.expandi.io/workspace/86904/shared-campaigns/63078?tab=tab-placeholders`,
the Placeholders tab reports **1,146 items** — the leads uploaded to the shared
campaign as a whole. The campaign card likewise shows "68 of 120" for
Initiated, so we understand 120 to be the currently-assigned batch rather than
the full uploaded list.

Both figures are meaningful to us, and we would like to report the uploaded
total (1,146) alongside the running total (120).

**Our question:** is the Shared Campaign level exposed through the open API at
all, and if so how do we read its lead/placeholder count?

Specifically:

1. Is there an endpoint for shared campaigns — something equivalent to
   `/shared_campaigns/{id}/` or `/shared_campaigns/{id}/placeholders/`? We
   tried a number of spellings and all returned 404.
2. The `campaign` object nested inside each instance carries the right id
   (`{"id": 63078, "workspace": 86904, "nr_contributors": 2, ...}`), but no
   lead or placeholder count. Is a count available on that object under a
   parameter we've missed?
3. `GET /li_accounts/{id}/messengers/` accepts `campaign_instance_id`. Is there
   an equivalent filter for the shared-campaign id? Passing `?campaign=63078`
   or `?shared_campaign_id=63078` appears to be ignored — the response returns
   the account's full messenger count (1,914) unchanged.

For reference, the spec at `https://api.liaufa.com/open-swagger.json` lists 18
endpoints, none of which mention shared campaigns or placeholders, which is why
we suspect this level simply isn't published yet.

If it isn't currently available, is it on the roadmap — and in the meantime is
there a supported way to obtain the uploaded-lead count per shared campaign?

Thanks very much,

---

## Why we are asking (context for whoever sends this)

The client's manually maintained sheet reports 1,151 leads for this campaign
while our automated dashboard reports 120. Both are correct — they are counting
different things — but until the API exposes the shared-campaign level we
cannot reproduce the manual figure automatically, and the two reports will keep
disagreeing every month.

## What has already been ruled out

Worth knowing so nobody repeats the work, and so the answer can be judged
against it:

- **Not a workspace-scope problem.** `GET /workspaces/` returns exactly one
  workspace (86904, BettrData.io) with two LinkedIn accounts, and our figures
  already aggregate across both.
- **Not a pagination problem.** `GET /li_accounts/{id}/messengers/?campaign_instance_id=813733`
  returns `count=60`, matching that instance's `people_in_campaign` exactly.
  The same holds for the second instance.
- **Not a stale-stats problem.** Three independent sources agree on 120: the
  `stats` counter, the messenger row count, and our own per-lead cache of
  `invited_at` timestamps.
- **Not a searches problem.** `GET /li_accounts/{id}/searches/` lists 10 source
  lists across both accounts, the largest being 598, and none from August.
  Nothing there accounts for 1,146.
