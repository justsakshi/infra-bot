// The 10:00 sheet sync after a restart. Run: node test_sync_catch_up.js   (no network)
// 17 and 18 Nov 2026: Render restarted the bot while the 10:00 sync ran, two days running. The run kept no end,
// nothing ran it again, All Inboxes stayed a day old and the 11:00 digest waited 30 minutes for a run that was gone.
const assert = require('assert');
const { lostRuns, syncCatchUp } = require('./stats_catch_up');

const ist = s => new Date(s + '+05:30');
const sync = (started, more = {}) => ({ job: 'smartlead-sync', slot: '2026-11-18T10:00+05:30', started: ist(started), ...more });
const restart = ist('2026-11-18T10:25:00');

// Runs the old process left open are lost; ended runs, runs of this process and day-old runs are not.
const open = sync('2026-11-18T10:00:04');
const stats = { job: 'campaign-stats-post', slot: '2026-11-18T10:15+05:30', started: ist('2026-11-18T10:15:00') };
const done = { job: 'billing-watch', started: ist('2026-11-18T10:05:00'), ended: ist('2026-11-18T10:05:17') };
const mine = { job: 'fleet-watch', started: ist('2026-11-18T10:30:00') };
const old = { job: 'fleet-watch', started: ist('2026-11-16T10:30:00') };   // over 24 h before the restart
assert.deepStrictEqual(lostRuns([open, stats, done, mine, old], restart), [open, stats]);

// 18 Nov: the sync started 10:00 and the bot restarted at 10:25 -> run it again, once (index.js lock).
assert.deepStrictEqual(syncCatchUp(restart, restart, [open]), { kind: 'lost', slot: '2026-11-18T10:00+05:30' });
// It finished, or this process already started one: nothing to do.
assert.strictEqual(syncCatchUp(restart, restart, [sync('2026-11-18T10:00:04', { ended: ist('2026-11-18T10:36:00') })]), null);
assert.strictEqual(syncCatchUp(ist('2026-11-18T10:40:00'), restart, [open, sync('2026-11-18T10:27:00')]), null);
// The bot was down at 10:00: no record today -> run the missed sync.
assert.deepStrictEqual(syncCatchUp(ist('2026-11-18T10:40:00'), ist('2026-11-18T10:38:00'), []), { kind: 'missed', slot: '2026-11-18T10:00+05:30' });
// Before 10:00 the cron runs it; yesterday's lost run is not today's.
assert.strictEqual(syncCatchUp(ist('2026-11-18T09:59:00'), ist('2026-11-18T09:58:00'), []), null);
assert.deepStrictEqual(syncCatchUp(ist('2026-11-18T10:40:00'), ist('2026-11-18T10:38:00'),
  [{ job: 'smartlead-sync', slot: '2026-11-17T10:00+05:30', started: ist('2026-11-17T10:00:01') }]), { kind: 'missed', slot: '2026-11-18T10:00+05:30' });
// A hand run (PC) is not in cron_runs and doesn't matter; another job's open run doesn't count.
assert.deepStrictEqual(syncCatchUp(restart, restart, [stats]), { kind: 'missed', slot: '2026-11-18T10:00+05:30' });
// UTC server clock: 04:55 UTC is 10:25 IST.
assert.deepStrictEqual(syncCatchUp(new Date('2026-11-18T04:55:00Z'), new Date('2026-11-18T04:55:00Z'), [open]), { kind: 'lost', slot: '2026-11-18T10:00+05:30' });

console.log('sync catch-up: 10 passed');
