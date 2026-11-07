// Late start: the kill report, the placement final run and copy sync are caught up once, inside
// their windows, and recorded against the slot they missed (stats_catch_up.missedRuns).
// Run: node test_job_catch_up.js   (no network)
const assert = require('assert');
const { missedRuns, CATCH_UPS } = require('./stats_catch_up');
const { JOBS } = require('./bot_health');

let n = 0;
const eq = (a, b, msg) => { assert.deepStrictEqual(a, b, msg); n++; };
const ist = s => new Date(s + '+05:30');
const jobs = now => missedRuns(now, {}).map(r => `${r.job} ${r.slot}`);

// Kill warnings: Tue 13:30, caught up until 18:00.
eq(jobs(ist('2026-11-10T13:29:00')), ['placement-report-final 2026-11-10T13:00+05:30'], 'kill report not yet: its own cron runs at 13:30');
eq(jobs(ist('2026-11-10T13:30:00')), ['kill-report 2026-11-10T13:30+05:30', 'placement-report-final 2026-11-10T13:00+05:30']);
eq(jobs(ist('2026-11-10T17:59:00')), ['kill-report 2026-11-10T13:30+05:30', 'placement-report-final 2026-11-10T13:00+05:30']);
eq(jobs(ist('2026-11-10T18:00:00')), [], 'after 18:00: too late');
eq(jobs(ist('2026-11-03T14:00:00')).length, 2, 'any Tuesday');

// Placement final run: Mon and Tue 13:00.
eq(jobs(ist('2026-11-09T13:10:00')), ['placement-report-final 2026-11-09T13:00+05:30'], 'Monday: final run only');
eq(jobs(ist('2026-11-11T13:10:00')), [], 'Wednesday: none');

// Copy sync: Sun 18:30 -> midnight, Mon 05:00-06:00, Tue 05:00-06:30 (before that morning's tests).
eq(jobs(ist('2026-11-08T20:00:00')), ['copy-sync 2026-11-08T18:30+05:30']);
eq(jobs(ist('2026-11-08T23:59:00')), ['copy-sync 2026-11-08T18:30+05:30']);
eq(jobs(ist('2026-11-09T00:30:00')), [], 'Monday night: the 05:00 run is still ahead');
eq(jobs(ist('2026-11-09T05:30:00')), ['copy-sync 2026-11-09T05:00+05:30']);
eq(jobs(ist('2026-11-09T06:00:00')), [], 'Monday tests started at 06:00');
eq(jobs(ist('2026-11-10T06:29:00')), ['copy-sync 2026-11-10T05:00+05:30']);
eq(jobs(ist('2026-11-10T06:30:00')), [], 'Tuesday tests start at 06:30');
// A UTC server: Mon 05:30 IST is 00:00 UTC.
eq(jobs(new Date('2026-11-09T00:00:00Z')), ['copy-sync 2026-11-09T05:00+05:30']);

// The stats post rides along on weekdays 10:15-12:00, and its switch still stops it.
eq(jobs(ist('2026-11-10T10:30:00')), ['campaign-stats-post 2026-11-10T10:15+05:30']);
eq(missedRuns(ist('2026-11-10T10:30:00'), { CAMPAIGN_STATS_POST: 'off' }), []);
eq(jobs(ist('2026-11-07T10:30:00')), [], 'Saturday: nothing');

// Every catch-up slot is a real slot of that job, so the bot-health check counts it as run.
const { parseCron, cronMatches } = require('./bot_health');
for (const c of CATCH_UPS) {
  for (const day of c.days) {
    const date = { 0: '2026-11-08', 1: '2026-11-09', 2: '2026-11-10' }[day];
    const slot = ist(`${date}T${c.from}:00`);
    const crons = JOBS.filter(j => j.job === c.job).map(j => parseCron(j.cron));
    assert.ok(crons.some(cr => cronMatches(cr, slot)), `${c.job} ${date} ${c.from} is a scheduled slot`); n++;
  }
}

console.log(`job catch-up: ${n} passed`);
