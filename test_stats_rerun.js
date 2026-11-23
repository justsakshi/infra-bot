// The 10:15 stats post after a restart. Run: node test_stats_rerun.js   (no network)
// 23 Nov 2026: the bot restarted at 10:22 IST mid-post; the catch-up found the day's lock taken by the dead run, no post.
const assert = require('assert');
const { statsRerun } = require('./stats_catch_up');

const ist = s => new Date(s + '+05:30');
const post = (started, more = {}) => ({ job: 'campaign-stats-post', slot: '2026-11-23T10:15+05:30', started: ist(started), ...more });
const restart = ist('2026-11-23T10:22:30');
const at = ist('2026-11-23T10:24:30');

// 23 Nov: started 10:15, bot restarted 10:22 -> run it again.
assert.strictEqual(statsRerun(at, restart, [post('2026-11-23T10:15:00')]), '2026-11-23T10:15+05:30');
// It finished, or this process started one: nothing to do.
assert.strictEqual(statsRerun(at, restart, [post('2026-11-23T10:15:00', { ended: ist('2026-11-23T10:21:00') })]), null);
assert.strictEqual(statsRerun(at, restart, [post('2026-11-23T10:15:00'), post('2026-11-23T10:23:00')]), null);
// No run today (the plain catch-up covers that), after 12:00, at the weekend, or switched off: nothing.
assert.strictEqual(statsRerun(at, restart, []), null);
assert.strictEqual(statsRerun(ist('2026-11-23T12:01:00'), restart, [post('2026-11-23T10:15:00')]), null);
assert.strictEqual(statsRerun(ist('2026-11-22T10:30:00'), ist('2026-11-22T10:25:00'), [post('2026-11-22T10:15:00')]), null);
assert.strictEqual(statsRerun(at, restart, [post('2026-11-23T10:15:00')], { CAMPAIGN_STATS_POST: 'off' }), null);
// Another job's open run, or yesterday's lost post, does not count.
assert.strictEqual(statsRerun(at, restart, [{ job: 'smartlead-sync', started: ist('2026-11-23T10:00:00') }]), null);
assert.strictEqual(statsRerun(at, restart, [{ job: 'campaign-stats-post', started: ist('2026-11-22T10:15:00') }]), null);
console.log('stats rerun: 9 passed');
