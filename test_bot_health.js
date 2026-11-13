// Bot health: the schedule, the missed/failed check, live-write flags, the PC v2 heartbeat and the posts.
// Run: node test_bot_health.js   (no network, no Mongo, no Slack)
const assert = require('assert');
const fs = require('fs');
const path = require('path');
const h = require('./bot_health');

let n = 0;
const ok = (cond, msg) => { assert.ok(cond, msg); n++; };
const eq = (a, b, msg) => { assert.deepStrictEqual(a, b, msg); n++; };
const ist = s => new Date(`${s}+05:30`);

/* ---------- the schedule is unchanged and index.js schedules exactly JOBS ---------- */

// Every cron index.js had before bot health (da2e497), plus the two new posts. A changed cron here is a
// changed schedule, so it must be on purpose.
const BEFORE = {
  'noon-summary': ['0 10 * * 1-5'], 'smartlead-sync': ['0 10 * * *'], retest: ['0 11 * * *'],
  'placement-batch': ['0 10,12,14,16 * * 4'], 'copy-sync': ['30 18 * * 5', '30 18 * * 6', '30 18 * * 0', '0 5 * * 1,2'],   // Fri added 11 Nov 2026: the weekly tests may start on Saturday
  'placement-report': ['0 8-12 * * 1,2'], 'placement-report-final': ['0 13 * * 1,2'],
  'placement-collect': ['30 12,16 * * *'], 'placement-summary': ['50 9 * * *'], 'key-health': ['45 9 * * *'],
  'bounce-protect': ['30 12 * * *'], blacklist: ['0 9 * * 1'], capacity: ['30 9 * * 1'],
  'zapmail-sync': ['30 9 * * *'], 'zapmail-digest': ['40 9 * * *'], 'scaledmail-sync': ['35 9 * * *'],
  'inbox-intake': ['15 */2 * * *'], 'inbox-ready': ['15 */2 * * *'], 'godaddy-sync': ['37 9 * * *'],
  'infra-audit': ['20 10 * * 1-5'], 'billing-watch': ['5 10,12,14,16,18,20 * * *'], 'renewal-review': ['50 9 * * *'],
  'kill-report': ['30 13 * * 2'], 'fleet-watch': ['30 10 * * *'], 'fleet-scorecard': ['50 10 * * 1'],
  'dt-senders': ['0 6 * * *'], 'campaign-metrics': ['0 9 * * *'], 'campaign-stats-post': ['15 10 * * 1-5'],
  'provider-watch': ['40 10 * * 1'], 'volume-review': ['45 13 * * 2'], 'reply-monitor': ['0 13 * * *'],
  reminder: ['0 16 * * 1-5'],
  'bot-health': ['5 10 * * 1-5'], 'v2-watch': ['0 14 * * 2', '0 10 * * 3'],
  'gate-selftest': ['30 11 * * *'],    // 11 Nov 2026: daily, after the sync has written the inbox list
  'real-recipients': ['0 7 * * 0']     // 13 Nov 2026: weekly real-recipient reply check, Sunday morning
};
const jobNames = [...new Set(h.JOBS.map(j => j.job))].sort();
eq(jobNames, Object.keys(BEFORE).sort(), 'JOBS has exactly the scheduled jobs');
for (const [job, crons] of Object.entries(BEFORE)) eq(h.cronsOf(job).sort(), crons.slice().sort(), `${job} cron unchanged`);
for (const j of h.JOBS) ok(j.title && !/[&<>]/.test(j.title), `${j.job} has a plain title`);

const src = fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8');
const atNames = [...src.matchAll(/\bat\('([a-z0-9-]+)'/g)].map(m => m[1]);
eq([...new Set(atNames)].sort(), jobNames, 'index.js schedules every JOBS job with at(), and nothing else');
for (const m of src.matchAll(/runJob\((?:final \? )?'([a-z0-9-]+)'/g)) ok(jobNames.includes(m[1]), `runJob('${m[1]}') is in JOBS`);
eq([...src.matchAll(/cron\.schedule\('([^']+)'/g)].map(m => m[1]), ['*/10 * * * *'], 'only the 10-minute inbox-jobs tick is scheduled outside JOBS');
ok(!/runBilling/.test(src), 'runBilling is gone (runJob records every run)');
for (const f of ['warmup_executor', 'rotation_executor', 'eg_test_executor', 'nc_test_executor']) ok(!src.includes(`${f}.py`), `${f} not in index.js`);
for (const job of ['fleet-watch', 'fleet-scorecard', 'dt-senders', 'kill-report', 'copy-sync', 'placement-report-final', 'bot-health', 'v2-watch']) {
  ok(src.includes(`onceToday('${job}')`), `${job} runs once a day (lock)`);
}
ok(/for \(const account of \['PRECISE_LEADS', 'BETTRDATA'\]\)[\s\S]{0,600}await runJob\(/.test(src), 'placement-report accounts run one after the other');

/* ---------- time ---------- */

eq(h.istDay(new Date('2026-11-08T23:30:00Z')), '2026-11-09', 'Mon 05:00 IST is Sunday in UTC but Monday in IST');
eq(h.slotKey(new Date('2026-11-08T23:30:42Z')), '2026-11-09T05:00+05:30');
eq(+new Date(h.slotKey(ist('2026-11-07T10:05:00'))), +ist('2026-11-07T10:05:00'), 'a slot key parses back to its minute');
eq(h.fmtIst(ist('2026-11-06T09:05:00')), 'Fri 6 Nov 09:05');
eq(h.fmtNight('2026-11-02'), 'Mon 2 Nov');
eq(h.fmtAge(45 * 60000), '45 min');
eq(h.fmtAge(31 * 3600000), '31 h');
eq(h.fmtAge(80 * 3600000), '3 days');
eq(h.lastMonday(ist('2026-11-10T14:00:00')), '2026-11-09', 'Tue -> yesterday');
eq(h.lastMonday(ist('2026-11-11T10:00:00')), '2026-11-09', 'Wed -> two days back');
eq(h.lastMonday(ist('2026-11-09T09:00:00')), '2026-11-09', 'Mon -> today');
eq(h.lastMonday(ist('2026-11-08T23:00:00')), '2026-11-02', 'Sun -> six days back');
eq(h.lastMonday(new Date('2026-11-09T19:00:00Z')), '2026-11-09', 'Tue 00:30 IST (still Monday in UTC) -> Mon 9');

/* ---------- cron ---------- */

const bw = h.parseCron('5 9-21/2 * * *');
ok(h.cronMatches(bw, ist('2026-11-06T09:05:00')) && h.cronMatches(bw, ist('2026-11-06T21:05:00')), '9-21/2 hits 09 and 21');
ok(!h.cronMatches(bw, ist('2026-11-06T10:05:00')), '9-21/2 skips 10');
const pr = h.parseCron('0 8-12 * * 1,2');
ok(h.cronMatches(pr, ist('2026-11-09T08:00:00')) && !h.cronMatches(pr, ist('2026-11-11T08:00:00')), 'Mon yes, Wed no');
ok(h.cronMatches(h.parseCron('30 18 * * 7'), ist('2026-11-08T18:30:00')), 'day 7 is Sunday');
ok(h.cronMatches(h.parseCron('*/10 * * * *'), ist('2026-11-08T18:40:00')), '*/10');
ok(h.cronMatches(h.parseCron('15 */2 * * *'), ist('2026-11-08T00:15:00')) && !h.cronMatches(h.parseCron('15 */2 * * *'), ist('2026-11-08T01:15:00')), '*/2 hours');
assert.throws(() => h.parseCron('0 25 * * *')); n++;
assert.throws(() => h.parseCron('0 10 * *')); n++;

// Fri 10:05 -> Mon 10:02 IST: the weekend.
const wk = h.expectedSlots(ist('2026-11-06T10:05:00'), ist('2026-11-09T10:02:00'), {});
const slotsOf = (list, job) => list.filter(s => s.job === job).map(s => s.slot);
eq(slotsOf(wk, 'copy-sync'), ['2026-11-06T18:30+05:30', '2026-11-07T18:30+05:30', '2026-11-08T18:30+05:30', '2026-11-09T05:00+05:30'], 'Friday, Saturday and Sunday evening, Monday morning');
eq(slotsOf(wk, 'blacklist'), ['2026-11-09T09:00+05:30']);
eq(slotsOf(wk, 'noon-summary'), ['2026-11-09T10:00+05:30'], 'Monday 10:00 only: Friday is before the window, weekends have none');
eq(slotsOf(wk, 'fleet-watch').length, 3, 'Fri, Sat and Sun 10:30');
eq(slotsOf(wk, 'kill-report'), []);
eq(slotsOf(wk, 'inbox-intake'), [], 'switched off: not expected');
eq(slotsOf(wk, 'scaledmail-sync'), [], 'no ScaledMail key: not expected');
eq(slotsOf(wk, 'provider-watch'), [], '10:40 Monday is after the window');
const tue = h.expectedSlots(ist('2026-11-10T12:59:00'), ist('2026-11-10T14:05:00'), { VOLUME_REVIEW_ENABLED: 'true' });
eq(tue.map(s => s.job), ['placement-report-final', 'reply-monitor', 'kill-report', 'volume-review', 'v2-watch', 'billing-watch'], 'Tue afternoon in order');
// The schedule here must mean the same to the scheduler that runs it. node-cron reads a stepped range such as
// 9-21/2 as "the even hours in 9-21"; this file read it as 9, 11, 13 ... and called three real runs a day
// missed (10 Nov 2026). Every cron in JOBS is expanded by both and compared.
const cronLib = (() => { try { return require('node-cron/src/convert-expression'); } catch (e) { return null; } })();
if (cronLib) {
  for (const j of h.JOBS) {
    const lib = cronLib(j.cron).split(' ');                       // sec min hour dom month dow
    const mine = h.parseCron(j.cron);
    const list = set => [...set].sort((a, b) => a - b).join(',');
    eq(list(mine.minute), lib[1], `${j.job} "${j.cron}": minutes`);
    eq(list(mine.hour), lib[2], `${j.job} "${j.cron}": hours as the scheduler runs them`);
    eq(list(new Set([...mine.dow].map(d => d % 7))), lib[5], `${j.job} "${j.cron}": weekdays`);
  }
}
eq(slotsOf(h.expectedSlots(ist('2026-11-10T09:00:00'), ist('2026-11-10T21:30:00'), {}), 'billing-watch').map(s => s.slice(11, 16)),
  ['10:05', '12:05', '14:05', '16:05', '18:05', '20:05'], 'billing watch: the hours it really runs');
eq(slotsOf(h.expectedSlots(ist('2026-11-10T13:00:00'), ist('2026-11-10T14:00:00'), {}), 'volume-review'), [], 'volume review off unless VOLUME_REVIEW_ENABLED=true');
const ready = h.expectedSlots(ist('2026-11-10T00:00:00'), ist('2026-11-10T03:00:00'), { INBOX_READY_ENABLED: 'true', SCALEDMAIL_API_KEY: 'k' });
eq(ready.map(s => s.job), ['inbox-ready', 'inbox-ready'], 'readiness runs when intake is off');
const both = h.expectedSlots(ist('2026-11-10T00:00:00'), ist('2026-11-10T03:00:00'), { INBOX_INTAKE_ENABLED: 'true', INBOX_READY_ENABLED: 'true', SCALEDMAIL_API_KEY: 'k' });
eq(both.map(s => s.job), ['inbox-intake', 'inbox-intake'], 'intake replaces readiness');

/* ---------- exit codes ---------- */

ok(h.exitOk('placement-summary', 2) && h.exitOk('placement-summary', 0), 'below threshold is not a bot failure');
ok(!h.exitOk('placement-summary', 1) && !h.exitOk('placement-summary', null), 'a crash is');
eq(h.exitLine('placement-summary', 0), 'finished with exit code 0 (no mailbox below the placement threshold)');
eq(h.exitLine('placement-summary', 1), 'FAILED with exit code 1 (a Smart Delivery account could not be read, or the job crashed)');
eq(h.exitNote('placement-summary', 1), 'exit code 1 (a Smart Delivery account could not be read, or the job crashed)');
ok(!/above threshold/.test(h.exitLine('placement-summary', 1)) && !/above threshold/.test(h.exitLine('placement-summary', 3)), 'never "all above threshold" on a crash');
eq(h.exitLine('placement-summary', 3), 'FAILED with exit code 3');
eq(h.exitLine('smartlead-sync', 2), 'FAILED with exit code 2 (one or more sheet tabs NOT written, see the Last Sync tab)');
eq(h.exitLine('key-health', 2), 'FAILED with exit code 2 (a Smartlead API key is dead)');
eq(h.exitLine('fleet-watch', null, { signal: 'SIGTERM' }), 'FAILED: stopped by SIGTERM');
eq(h.exitLine('fleet-watch', null, { error: 'spawn python ENOENT' }), 'FAILED: could not start: spawn python ENOENT');
eq(h.exitLine('unknown-job', 0), 'finished with exit code 0');

/* ---------- window ---------- */

const now = ist('2026-11-09T10:05:00');
const ws = h.windowStart({ lastPost: ist('2026-11-06T10:05:00'), firstRecord: ist('2026-10-01T00:00:00'), now });
eq([+ws.since, ws.capped, ws.fromFirst], [+ist('2026-11-06T10:05:00'), false, false]);
const first = h.windowStart({ lastPost: null, firstRecord: ist('2026-11-09T09:58:00'), now });
eq([+first.since, first.fromFirst], [+ist('2026-11-09T09:58:00'), true], 'first post: from the first record');
eq(+h.windowStart({ now }).since, +now, 'nothing recorded yet: an empty window');
const old = h.windowStart({ lastPost: ist('2026-10-20T10:05:00'), now });
eq([+old.since, old.capped], [+now - 4 * 86400000, true], 'capped at 4 days');

/* ---------- summarize ---------- */

const run = (job, slot, extra = {}) => ({ job, slot: `${slot}+05:30`, started: ist(`${slot}:01`), ended: ist(`${slot}:40`), exit_code: 0, ...extra });
const since = ist('2026-11-09T10:05:00');
const at = ist('2026-11-10T10:05:00');
const due = h.expectedSlots(new Date(+since - 3 * 60000), new Date(+at - 3 * 60000), {});
const allRan = due.map(d => run(d.job, d.slot.slice(0, 16)));
let s = h.summarize({ runs: allRan, since, now: at });
eq([s.due, s.missed.length, s.failed.length, s.unfinished.length], [due.length, 0, 0, 0], 'every slot has a run');
ok(due.some(d => d.slot === '2026-11-10T10:00+05:30') && !due.some(d => d.slot === '2026-11-10T10:05+05:30'), 'own slot is not checked yet');
ok(due.some(d => d.job === 'bot-health' && d.slot === '2026-11-09T10:05+05:30'), 'the last post slot is checked once, by the next post');
const less = allRan.filter(r => !(r.job === 'fleet-watch' || (r.job === 'billing-watch' && r.slot.startsWith('2026-11-09T14'))));
s = h.summarize({ runs: less, since, now: at });
eq(s.missed.map(m => `${m.job} ${m.slot}`), ['fleet-watch 2026-11-09T10:30+05:30', 'billing-watch 2026-11-09T14:05+05:30']);
const withFails = [...allRan,
  run('kill-report', '2026-11-09T13:30', { exit_code: 1 }),
  run('placement-summary', '2026-11-10T09:50', { exit_code: 2 }),
  run('noon-summary', '2026-11-10T10:00', { exit_code: 1, skipped: true }),
  run('dt-senders', '2026-11-10T06:00', { exit_code: null, error: 'spawn python ENOENT' }),
  { job: 'retest', slot: '2026-11-09T11:00+05:30', started: ist('2026-11-09T11:00:01') },          // died mid-run
  { job: 'retest', slot: '2026-11-10T09:00+05:30', started: ist('2026-11-10T09:00:01') },          // still running
  run('fleet-watch', '2026-11-08T10:30', { exit_code: 1 })                                         // reported last time
];
s = h.summarize({ runs: withFails, since, now: at });
eq(s.failed.map(r => r.job), ['kill-report', 'dt-senders'], 'not ok codes and start errors; exit 2 of the summary and skipped runs are not failures');
eq(s.unfinished.map(r => r.slot), ['2026-11-09T11:00+05:30'], 'an unfinished run older than 3 hours, once');

/* ---------- live-write flags ---------- */

eq(h.liveWriteFlags({}), ['copy sync (`COPY_SYNC_DISABLED` is not true)', 'provider watch post (`PROVIDER_WATCH_POST` is not off)'], 'defaults');
eq(h.liveWriteFlags({ COPY_SYNC_DISABLED: 'true', PROVIDER_WATCH_POST: 'off' }), []);
eq(h.liveWriteFlags({ COPY_SYNC_DISABLED: 'true', PROVIDER_WATCH_POST: 'off', WARMUP_AUTO_ENABLED: ' True ', VOLUME_REVIEW_ENABLED: 'True',
  PLACEMENT_ENABLED: 'true', ZAPMAIL_ASSET_SYNC_ENABLED: 'true', GODADDY_ASSET_SYNC_ENABLED: 'false', ZAPMAIL_ALLOW_SPEND: 'TRUE',
  RETEST_ENABLED: 'false', SCALEDMAIL_ASSET_SYNC_ENABLED: 'true' }),
['`WARMUP_AUTO_ENABLED`', '`PLACEMENT_ENABLED`', '`ZAPMAIL_ALLOW_SPEND`', '`SCALEDMAIL_ASSET_SYNC_ENABLED`', '`ZAPMAIL_ASSET_SYNC_ENABLED`'],
'Python flags any case; index.js flags only exact "true" (VOLUME_REVIEW_ENABLED=True is off there)');
eq(h.flagsLine({ COPY_SYNC_DISABLED: 'true', PROVIDER_WATCH_POST: 'off' }), 'Live-write flags on: none.');

/* ---------- PC v2 heartbeat ---------- */

const hb = (hoursAgo, extra = {}) => ({ _id: 'pc', last_run: new Date(+at - hoursAgo * 3600000).toISOString(), newest_night: '2026-11-09',
  files_read: 12, mode: 'warn', last_post_ok: true, commit: 'abc1234', behind_main: 0, errors: [], ...extra });
let line = h.heartbeatLine(hb(2), at);
eq([line.red, line.text], [false, 'PC v2 job: last ran 2 h ago, newest v2 night Mon 9 Nov, mode warn.']);
line = h.heartbeatLine(hb(27), at);
ok(line.red && line.text.startsWith('🔴 PC v2 job: last ran 27 h ago (Mon 9 Nov 07:05 IST)') && line.text.includes('"InfraBot v2 weekly"'), 'red after 26 hours');
ok(!h.heartbeatLine(hb(25.9), at).red, '25.9 h is not red yet');
ok(h.heartbeatLine(null, at).red && h.heartbeatLine({ _id: 'pc', last_run: 'nonsense' }, at).red, 'no heartbeat is red');
line = h.heartbeatLine(hb(1, { errors: ['csv <bad> & broken', 'x'], behind_main: 3, last_post_ok: false, newest_night: null }), at);
ok(line.text.includes('no v2 night read yet') && line.text.includes('logged 2 errors: csv &lt;bad&gt; &amp; broken')
  && line.text.includes('Its last Slack post failed.') && line.text.includes('3 commits behind main'), 'extras, escaped for Slack');

const tue14 = ist('2026-11-10T14:00:00');
const hbAt = (t, hoursAgo, night) => ({ _id: 'pc', last_run: new Date(+t - hoursAgo * 3600000).toISOString(), newest_night: night });
eq(h.v2WatchProblems(hbAt(tue14, 0.5, '2026-11-09'), tue14), [], 'Monday is in and the job ran: quiet');
eq(h.v2WatchPost(hbAt(tue14, 0.5, '2026-11-09'), tue14), null);
let probs = h.v2WatchProblems(hbAt(tue14, 0.5, '2026-11-02'), tue14);
eq(probs.length, 1);
ok(probs[0].startsWith('🔴 v2 results for the week of Mon 9 Nov are not in Mongo (the newest night read is Mon 2 Nov)'), probs[0]);
// The weekly tests may start on the Saturday or Sunday before that Monday (10 Nov 2026: a 5-minute gap makes a
// 50-mailbox test take 33 hours, so they start on Sunday to be in on Monday morning).
eq(h.weekStart('2026-11-09'), '2026-11-07');
eq(h.v2WatchProblems(hbAt(tue14, 0.5, '2026-11-08'), tue14), [], 'a Sunday test is this week\'s test');
eq(h.v2WatchProblems(hbAt(tue14, 0.5, '2026-11-07'), tue14), [], 'so is a Saturday one');
eq(h.v2WatchProblems(hbAt(tue14, 0.5, '2026-11-06'), tue14).length, 1, 'a Friday test is last week\'s');
probs = h.v2WatchProblems(hbAt(tue14, 4, '2026-11-09'), tue14);
ok(probs.length === 1 && probs[0].startsWith('🔴 The PC v2 job last ran 4 h ago (Tue 10 Nov 10:00 IST)'), probs[0]);
eq(h.v2WatchProblems(hbAt(tue14, 4, '2026-11-02'), tue14).length, 2, 'both');
eq(h.v2WatchProblems(null, tue14).length, 1, 'no heartbeat: one red line');
ok(h.v2WatchPost(hbAt(tue14, 4, '2026-11-02'), tue14).text.includes('\n\n'), 'blank line between the two lines');
eq(h.v2WatchProblems(hbAt(ist('2026-11-11T10:00:00'), 0.2, '2026-11-09'), ist('2026-11-11T10:00:00')), [], 'Wednesday with Monday in');

/* ---------- the post ---------- */

eq(h.slotList(['2026-11-06T13:05+05:30', '2026-11-06T15:05+05:30', '2026-11-07T09:05+05:30']), 'Fri 6 Nov 13:05, 15:05; Sat 7 Nov 09:05');
eq(h.slotList(['2026-11-06T09:05+05:30', '2026-11-06T11:05+05:30', '2026-11-06T13:05+05:30'], 2), 'Fri 6 Nov 09:05, 11:05 and 1 more');

const quietEnv = { COPY_SYNC_DISABLED: 'true', PROVIDER_WATCH_POST: 'off' };
const dueQuiet = h.expectedSlots(new Date(+since - 3 * 60000), new Date(+at - 3 * 60000), quietEnv).length;
eq(dueQuiet, due.length - 1, 'PROVIDER_WATCH_POST=off: Monday 10:40 is not expected');
let post = h.healthPost({ kind: 'daily', now: at, since, runs: allRan, heartbeat: hb(2), env: { ...quietEnv, RETEST_ENABLED: 'true' } });
eq(post.thread, 'Live-write flags on: `RETEST_ENABLED`.', 'nothing wrong: the thread has only the flags (13 Nov 2026)');
eq(post.text, `Bot health since Mon 9 Nov 10:05 IST: all ${dueQuiet} scheduled runs ran, none failed.\n\n`
  + 'PC v2 job: last ran 2 h ago, newest v2 night Mon 9 Nov, mode warn.');

post = h.healthPost({ kind: 'daily', now: at, since, runs: [...less, ...withFails.slice(allRan.length)], heartbeat: hb(30), env: quietEnv });
ok(post.text.startsWith(`Bot health since Mon 9 Nov 10:05 IST: 2 of ${dueQuiet} scheduled runs did not run, 2 runs failed, 1 run started but never finished. Details in the thread.\n\n🔴 PC v2 job: last ran 30 h ago`), post.text);
ok(post.thread.includes('Did not run (no run recorded for that time):\n• Fleet watch: Mon 9 Nov 10:30\n• Billing watch: Mon 9 Nov 14:05'), post.thread);
ok(post.thread.includes('• Kill warnings, Mon 9 Nov 13:30: exit code 1.'), post.thread);
ok(post.thread.includes('• DT senders, Tue 10 Nov 06:00: could not start: spawn python ENOENT.'), post.thread);
ok(post.thread.includes('• Auto placement test, started Mon 9 Nov 11:00'), post.thread);
ok(post.thread.split('\n\n').length === 4, 'blank lines between the sections, the flags last');
ok(post.thread.endsWith('Live-write flags on: none.') && !post.text.includes('Live-write'), 'flags in the thread only');

post = h.healthPost({ kind: 'restart', now: at, since, runs: allRan, heartbeat: hb(2), env: quietEnv, upSince: ist('2026-11-10T10:02:00') });
eq(post.text, `Bot restarted at 10:02 IST. Bot health since Mon 9 Nov 10:05 IST: all ${dueQuiet} scheduled runs ran, none failed.`,
  'restart: no v2 line when it is fine');
post = h.healthPost({ kind: 'restart', now: at, since, runs: allRan, heartbeat: null, env: quietEnv, upSince: at });
ok(post.text.includes('\n\n🔴 PC v2 job: no heartbeat'), 'restart: the v2 line when it is red');
post = h.healthPost({ kind: 'restart', now: at, since: new Date(+at - 60000), fromFirst: true, runs: [], heartbeat: hb(1), env: quietEnv, upSince: at });
ok(post.text.startsWith('Bot restarted at 10:05 IST. Bot health since Tue 10 Nov 10:04 IST (when run records began): no scheduled runs were due, none failed.'), post.text);
post = h.healthPost({ kind: 'daily', now: at, since: new Date(+at - 4 * 86400000), capped: true, runs: [], heartbeat: hb(1), env: quietEnv });
ok(post.text.includes('(only the last 4 days are checked)'), 'capped window says so');

/* ---------- deployed code: merged is not live until Render deploys it (10 Nov 2026) ---------- */

const RUN = 'b20110715efed8cdc864c7411392b0218cc21c12';
const MAIN = '3c38dbc1e2c1cc71fd0d91e448e65b4e0bfd00a0';
// The API answered 403 from Render on the first deploy, so the public commit feed is the fallback.
const FEED = ['3baff8e4b17bc7f3ba9b91cd31503f54243972ee', '0caab1990789b57daf6d1e07d98056111be51122', '8ac998f2943b281a231d6bc68feefd033c43c1bb']
  .map(sha => `<entry><id>tag:github.com,2008:Grit::Commit/${sha}</id></entry>`).join('');
eq(h.behindFromFeed(FEED, '8ac998f2943b281a231d6bc68feefd033c43c1bb'), { behindBy: 2, mainSha: '3baff8e4b17bc7f3ba9b91cd31503f54243972ee' });
eq(h.behindFromFeed(FEED, '3baff8e4b17bc7f3ba9b91cd31503f54243972ee').behindBy, 0, 'on the newest commit');
eq(h.behindFromFeed(FEED, 'b20110715efed8cdc864c7411392b0218cc21c12').behindBy, 3, 'older than the feed: behind by all of it');
eq(h.behindFromFeed('<feed></feed>', '8ac998f'), null, 'no commits in the feed: no claim');
ok(h.deployLine({ running: '8ac998f2943b', ...h.behindFromFeed(FEED, '8ac998f2943b') }).text
  .startsWith('🔴 Not deployed: the bot runs `8ac998f` and main is 2 commits ahead (`3baff8e`).'), 'the feed feeds the same line');
eq(h.deployLine(undefined), { red: false, text: '' }, 'off Render: no line');
eq(h.deployLine({ running: '' }), { red: false, text: '' });
eq(h.deployLine({ running: RUN, behindBy: 0, mainSha: RUN }), { red: false, text: 'Code running: `b201107`, the latest on main.' });
let dep = h.deployLine({ running: RUN, behindBy: 4, mainSha: MAIN });
ok(dep.red && dep.text.startsWith('🔴 Not deployed: the bot runs `b201107` and main is 4 commits ahead (`3c38dbc`). What was merged since is not live.'), dep.text);
ok(dep.text.includes('Manual Deploy'), 'says what to do');
ok(h.deployLine({ running: RUN, behindBy: 1 }).text.includes('main is 1 commit ahead. '), 'one commit, no sha');
dep = h.deployLine({ running: RUN, error: 'GitHub answered 403 <rate limit>' });
eq(dep, { red: false, text: 'Code running: `b201107`. Could not compare it with main (GitHub answered 403 &lt;rate limit&gt;).' }, 'an unreadable GitHub is not an alarm');
eq(h.deployLine({ running: RUN, behindBy: NaN }).red, false, 'no number: not an alarm');

post = h.healthPost({ kind: 'daily', now: at, since, runs: allRan, heartbeat: hb(2), env: quietEnv, deploy: { running: RUN, behindBy: 4, mainSha: MAIN } });
ok(post.text.startsWith('🔴 Not deployed: the bot runs `b201107`') && post.red, 'undeployed code is the first line and makes the post red');
ok(post.text.includes(`\n\nBot health since Mon 9 Nov 10:05 IST: all ${dueQuiet} scheduled runs ran, none failed.\n\n`), 'the rest of the post is unchanged');
eq(post.thread, 'Live-write flags on: none.', 'only the flags in the thread');
post = h.healthPost({ kind: 'restart', now: at, since, runs: allRan, heartbeat: hb(2), env: quietEnv, upSince: at, deploy: { running: MAIN, behindBy: 0, mainSha: MAIN } });
eq(post.text, `Bot restarted at 10:05 IST. Bot health since Mon 9 Nov 10:05 IST: all ${dueQuiet} scheduled runs ran, none failed.\n\n`
  + 'Code running: `3c38dbc`, the latest on main.', 'after a deploy the restart post confirms the commit');
ok(!post.red, 'up to date is not red');
ok(/async function deployState\(\)[\s\S]{0,900}RENDER_GIT_COMMIT[\s\S]{0,900}compare\/\$\{running\}\.\.\.\$\{branch\}/.test(fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8')),
  'index.js compares the running commit with main');
ok(/commits\/\$\{branch\}\.atom[\s\S]{0,300}health\.behindFromFeed\(data, running\)/.test(fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8')),
  'index.js falls back to the public commit feed');
ok(/const deploy = await deployState\(\);\s+const msg = health\.healthPost\(\{[^}]*deploy \}\)/.test(fs.readFileSync(path.join(__dirname, 'index.js'), 'utf8')),
  'the health post gets the deploy state');

eq(h.dueDailySlot(ist('2026-11-10T11:00:00')), '2026-11-10T10:05+05:30');
eq(h.dueDailySlot(ist('2026-11-10T10:04:00')), null, 'before 10:05 the cron will post');
eq(h.dueDailySlot(ist('2026-11-08T11:00:00')), null, 'Sunday: no daily post');

console.log(`bot health: ${n} passed`);
