// Missed 10:15 stats post: a bot that starts late on a weekday runs it once.
// Run: node test_stats_catch_up.js   (no network)
const assert = require('assert');
const { missedStatsPost } = require('./stats_catch_up');

const ist = (s) => new Date(s + '+05:30');

// 4 Nov 2026 (Wed): the bot was up at 10:00, the 10:15 run never started.
assert.strictEqual(missedStatsPost(ist('2026-11-04T10:30:00'), {}), true);
assert.strictEqual(missedStatsPost(ist('2026-11-04T10:15:00'), {}), true);   // restart on the minute
assert.strictEqual(missedStatsPost(ist('2026-11-04T10:14:00'), {}), false);  // the cron will run it
// The window ends at 12:00, after the 11:00 standup the post is for.
assert.strictEqual(missedStatsPost(ist('2026-11-04T11:59:00'), {}), true);
assert.strictEqual(missedStatsPost(ist('2026-11-04T12:00:00'), {}), false);
// Weekends have no post; the switch still stops it.
assert.strictEqual(missedStatsPost(ist('2026-11-07T10:30:00'), {}), false);  // Saturday
assert.strictEqual(missedStatsPost(ist('2026-11-08T10:30:00'), {}), false);  // Sunday
assert.strictEqual(missedStatsPost(ist('2026-11-04T10:30:00'), { CAMPAIGN_STATS_POST: 'off' }), false);
// A UTC server: 04:50 UTC is 10:20 IST.
assert.strictEqual(missedStatsPost(new Date('2026-11-04T04:50:00Z'), {}), true);

console.log('stats catch-up: 9 passed');
