// Bot health (mailbox-maintenance audit, 7 Nov 2026, critic fix 6). Every cron runs inside one Node
// process; when Render restarts or redeploys at a job's minute, that run is lost and nobody was told.
//
//   * JOBS below is the schedule. index.js schedules each job from it (at(job, fn)), so the check
//     below and the real schedule cannot drift apart. A new cron = one JOBS line + one at() call.
//   * Each run writes one Mongo cron_runs doc {job, slot, started, ended, exit_code} (index.js runJob/track).
//   * Weekdays 10:05 IST, and once after a restart, the bot posts to the infra channel: scheduled runs
//     with no record, runs that ended with an error, the age of the PC v2 job's heartbeat (Mongo
//     v2_heartbeat, red after 26 hours) and which switches that let the bot write to live tools are on.
//   * Tue 14:00 and Wed 10:00 IST: a red post if Monday's v2 results are not in Mongo or the PC job
//     (hourly) has not run for 3 hours.
//
// Everything in this file is pure (no Mongo, no Slack); index.js does the reads and the posts.

const INFRA_CHANNEL = 'C0AGVSUNEFP';   // #domain-inboxes-expiry-reminder, same as smartlead/config.py
const IST_MS = 330 * 60000;
const MIN = 60000;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
const GRACE_MS = 3 * MIN;              // a run must have started within 3 minutes of its slot to count
const UNFINISHED_MS = 3 * HOUR;        // started, no end recorded after 3 hours: the bot died mid-run
const MAX_WINDOW_MS = 4 * DAY;         // never look back further than this (Fri 10:05 -> Mon 10:05 is 3 days)
const HEARTBEAT_RED_MS = 26 * HOUR;
const V2_STALE_MS = 3 * HOUR;
const PC_TASK = 'InfraBot v2 weekly';  // the Windows scheduled task that runs v2_weekly.py

// title: what the post calls it. when(env): false means the job does not run (switched off), so it is
// not expected. ok: exit codes that are not a failure (default [0]). codes: what an exit code means.
const JOBS = [
  { job: 'dt-senders', cron: '0 6 * * *', title: 'DT senders' },
  { job: 'copy-sync', cron: '30 18 * * 0', title: 'Test-copy sync' },
  { job: 'copy-sync', cron: '0 5 * * 1,2', title: 'Test-copy sync' },
  { job: 'blacklist', cron: '0 9 * * 1', title: 'Blocklist check' },
  { job: 'campaign-metrics', cron: '0 9 * * *', title: 'Campaign metrics',
    codes: { 2: 'some campaign metrics were not written' } },
  { job: 'capacity', cron: '30 9 * * 1', title: 'Capacity planner' },
  { job: 'zapmail-sync', cron: '30 9 * * *', title: 'Zapmail tracker sync' },
  { job: 'scaledmail-sync', cron: '35 9 * * *', title: 'ScaledMail tracker sync', when: env => !!env.SCALEDMAIL_API_KEY },
  { job: 'godaddy-sync', cron: '37 9 * * *', title: 'GoDaddy tracker sync', when: env => !!env.GODADDY_PAT },
  { job: 'zapmail-digest', cron: '40 9 * * *', title: 'Zapmail digest' },
  { job: 'key-health', cron: '45 9 * * *', title: 'API-key check',
    codes: { 0: 'all keys valid', 1: 'Smartlead did not answer for an account', 2: 'a Smartlead API key is dead' } },
  { job: 'placement-summary', cron: '50 9 * * *', title: 'Placement summary', ok: [0, 2],
    codes: { 0: 'no mailbox below the placement threshold', 2: 'a mailbox is below the placement threshold',
      1: 'a Smart Delivery account could not be read, or the job crashed' } },
  { job: 'renewal-review', cron: '50 9 * * *', title: 'Renewal review' },
  { job: 'smartlead-sync', cron: '0 10 * * *', title: 'Smartlead sync',
    codes: { 0: 'all tabs written', 2: 'one or more sheet tabs NOT written, see the Last Sync tab' } },
  { job: 'noon-summary', cron: '0 10 * * 1-5', title: 'Daily Renewal Check' },
  { job: 'bot-health', cron: '5 10 * * 1-5', title: 'Bot health post' },
  { job: 'billing-watch', cron: '5 9-21/2 * * *', title: 'Billing watch' },
  { job: 'campaign-stats-post', cron: '15 10 * * 1-5', title: 'Campaign stats post', when: env => env.CAMPAIGN_STATS_POST !== 'off' },
  { job: 'infra-audit', cron: '20 10 * * 1-5', title: 'Infra audit' },
  { job: 'fleet-watch', cron: '30 10 * * *', title: 'Fleet watch' },
  { job: 'provider-watch', cron: '40 10 * * 1', title: 'Microsoft/Proofpoint watch', when: env => env.PROVIDER_WATCH_POST !== 'off' },
  { job: 'fleet-scorecard', cron: '50 10 * * 1', title: 'Fleet scorecard' },
  { job: 'retest', cron: '0 11 * * *', title: 'Auto placement test' },
  { job: 'placement-batch', cron: '0 10,12,14,16 * * 4', title: 'Weekly placement batch' },
  { job: 'placement-report', cron: '0 8-12 * * 1,2', title: 'Placement report' },
  { job: 'placement-report-final', cron: '0 13 * * 1,2', title: 'Placement report (final run)' },
  { job: 'placement-collect', cron: '30 12,16 * * *', title: 'Placement collector' },
  { job: 'bounce-protect', cron: '30 12 * * *', title: 'Bounce protect' },
  { job: 'reply-monitor', cron: '0 13 * * *', title: 'Reply monitor' },
  { job: 'kill-report', cron: '30 13 * * 2', title: 'Kill warnings' },
  { job: 'volume-review', cron: '45 13 * * 2', title: 'Volume review', when: env => env.VOLUME_REVIEW_ENABLED === 'true' },
  { job: 'v2-watch', cron: '0 14 * * 2', title: 'v2 results check' },
  { job: 'v2-watch', cron: '0 10 * * 3', title: 'v2 results check' },
  { job: 'reminder', cron: '0 16 * * 1-5', title: '4 PM renewal reminder' },
  { job: 'inbox-intake', cron: '15 */2 * * *', title: 'Inbox intake', when: env => env.INBOX_INTAKE_ENABLED === 'true' },
  { job: 'inbox-ready', cron: '15 */2 * * *', title: 'Inbox readiness',
    when: env => env.INBOX_INTAKE_ENABLED !== 'true' && env.INBOX_READY_ENABLED === 'true' && !!env.SCALEDMAIL_API_KEY }
];

const jobInfo = (job, jobs = JOBS) => jobs.find(j => j.job === job) || { job, title: job };
const cronsOf = (job, jobs = JOBS) => jobs.filter(j => j.job === job).map(j => j.cron);

/* ---------- IST time ---------- */

const pad = n => String(n).padStart(2, '0');
const DOW = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
const MON = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

function istParts(date) {
  const d = new Date(+date + IST_MS);            // read as UTC fields
  return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate(),
    hour: d.getUTCHours(), minute: d.getUTCMinutes(), dow: d.getUTCDay() };
}
/** The IST date, YYYY-MM-DD (lock keys are per IST day: 05:00 IST is still the previous day in UTC). */
function istDay(date) {
  const p = istParts(date);
  return `${p.year}-${pad(p.month)}-${pad(p.day)}`;
}
/** The IST minute a run was due, e.g. 2026-11-07T10:05+05:30. cron_runs.slot and the check match on it. */
function slotKey(date) {
  const p = istParts(date);
  return `${istDay(date)}T${pad(p.hour)}:${pad(p.minute)}+05:30`;
}
/** "Fri 7 Nov 10:05" (IST). */
function fmtIst(date) {
  const p = istParts(date);
  return `${DOW[p.dow]} ${p.day} ${MON[p.month - 1]} ${pad(p.hour)}:${pad(p.minute)}`;
}
/** "Mon 2 Nov" for a YYYY-MM-DD date. */
function fmtNight(day) {
  const d = new Date(`${day}T00:00:00Z`);
  if (isNaN(d)) return String(day);
  return `${DOW[d.getUTCDay()]} ${d.getUTCDate()} ${MON[d.getUTCMonth()]}`;
}
/** "45 min" / "5 h" / "3 days". */
function fmtAge(ms) {
  if (ms < HOUR) return `${Math.max(0, Math.round(ms / MIN))} min`;
  if (ms < 72 * HOUR) return `${Math.round(ms / HOUR)} h`;
  return `${Math.round(ms / DAY)} days`;
}
/** The most recent Monday on or before `now` (IST), YYYY-MM-DD. */
function lastMonday(now) {
  const back = (istParts(now).dow + 6) % 7;
  return istDay(new Date(+now - back * DAY));
}

/* ---------- cron ---------- */

function parseField(field, min, max) {
  const out = new Set();
  for (const part of field.split(',')) {
    const [range, stepStr] = part.split('/');
    const step = stepStr === undefined ? 1 : Number(stepStr);
    let lo; let hi;
    if (range === '*') { lo = min; hi = max; } else if (range.includes('-')) { [lo, hi] = range.split('-').map(Number); } else {
      lo = Number(range); hi = stepStr === undefined ? lo : max;
    }
    if (![lo, hi, step].every(Number.isInteger) || step < 1 || lo < min || hi > max || lo > hi) {
      throw new Error(`bad cron field "${field}"`);
    }
    for (let v = lo; v <= hi; v += step) out.add(v);
  }
  return out;
}
/** The five-field cron expressions this bot uses: *, n, a-b, lists and steps. */
function parseCron(expr) {
  const f = String(expr).trim().split(/\s+/);
  if (f.length !== 5) throw new Error(`bad cron "${expr}"`);
  const dow = parseField(f[4], 0, 7);
  if (dow.has(7)) dow.add(0);
  return { minute: parseField(f[0], 0, 59), hour: parseField(f[1], 0, 23), dom: parseField(f[2], 1, 31),
    month: parseField(f[3], 1, 12), dow, domAny: f[2] === '*', dowAny: f[4] === '*' };
}
function cronMatches(c, date) {
  const p = istParts(date);
  if (!c.minute.has(p.minute) || !c.hour.has(p.hour) || !c.month.has(p.month)) return false;
  if (c.domAny || c.dowAny) return c.dom.has(p.day) && c.dow.has(p.dow);
  return c.dom.has(p.day) || c.dow.has(p.dow);
}
/** Every slot a job was due in (from, to]: [{job, slot}], oldest first. Switched-off jobs are left out. */
function expectedSlots(from, to, env = {}, jobs = JOBS) {
  const out = [];
  const seen = new Set();
  const live = jobs.filter(j => !j.when || j.when(env)).map(j => ({ job: j.job, c: parseCron(j.cron) }));
  for (let t = Math.floor(+from / MIN) * MIN + MIN; t <= +to; t += MIN) {
    const at = new Date(t);
    for (const { job, c } of live) {
      if (!cronMatches(c, at)) continue;
      const slot = slotKey(at);
      if (!seen.has(`${job}|${slot}`)) { seen.add(`${job}|${slot}`); out.push({ job, slot }); }
    }
  }
  return out;
}

/* ---------- exit codes ---------- */

const exitOk = (job, code, jobs = JOBS) => (jobInfo(job, jobs).ok || [0]).includes(code);

/** How a run ended, for the post: "exit code 1 (what 1 means for this job)". */
function exitNote(job, code, { error, signal } = {}, jobs = JOBS) {
  if (error) return `could not start: ${error}`;
  if (code === null || code === undefined) return signal ? `stopped by ${signal}` : 'stopped without an exit code';
  const note = (jobInfo(job, jobs).codes || {})[code];
  return `exit code ${code}${note ? ` (${note})` : ''}`;
}
/** The log line: "finished with exit code 0 (...)" or "FAILED with exit code 1 (...)". */
function exitLine(job, code, how = {}, jobs = JOBS) {
  const note = exitNote(job, code, how, jobs);
  if (how.error || code === null || code === undefined) return `FAILED: ${note}`;
  return `${exitOk(job, code, jobs) ? 'finished' : 'FAILED'} with ${note}`;
}

/* ---------- the check ---------- */

/** Where this post's window starts: the last health post, else the first run record, never over 4 days back. */
function windowStart({ lastPost, firstRecord, now }) {
  const floor = +now - MAX_WINDOW_MS;
  const base = lastPost ? +new Date(lastPost) : firstRecord ? +new Date(firstRecord) : +now;
  return { since: new Date(Math.max(base, floor)), capped: base < floor, fromFirst: !lastPost && !!firstRecord };
}

/**
 * runs: cron_runs docs; since = when the last post ran. Each window is shifted so that consecutive posts
 * check every slot and every run exactly once:
 * missed: slots due in (since - 3 min, now - 3 min] with no record at all (a run gets 3 minutes to start).
 * failed: runs that ended in (since, now] with an exit code that is not ok for that job.
 * unfinished: runs started in (since - 3 h, now - 3 h] with no end recorded.
 */
function summarize({ runs = [], since, now, env = {}, jobs = JOBS }) {
  const due = expectedSlots(+since - GRACE_MS, +now - GRACE_MS, env, jobs);
  const have = new Set(runs.map(r => `${r.job}|${r.slot}`));
  const missed = due.filter(d => !have.has(`${d.job}|${d.slot}`));
  const t = d => (d ? +new Date(d) : NaN);
  const failed = runs
    .filter(r => !r.skipped && r.ended && t(r.ended) > +since && t(r.ended) <= +now)
    .filter(r => r.error || !exitOk(r.job, r.exit_code, jobs))
    .sort((a, b) => t(a.started) - t(b.started));
  const unfinished = runs
    .filter(r => !r.ended && t(r.started) > +since - UNFINISHED_MS && t(r.started) <= +now - UNFINISHED_MS)
    .sort((a, b) => t(a.started) - t(b.started));
  return { due: due.length, missed, failed, unfinished };
}

/* ---------- live-write switches ---------- */

const pyOn = v => String(v ?? '').trim().toLowerCase() === 'true';   // how smartlead/config.py reads them
const jsOn = v => v === 'true';                                        // how index.js reads them
const FLAGS = [
  ['WARMUP_AUTO_ENABLED', pyOn], ['ROTATION_ENABLED', pyOn], ['RETEST_ENABLED', pyOn],
  ['RETEST_DISABLE_WARMUP', pyOn], ['PLACEMENT_ENABLED', jsOn], ['NC_TEST_ENABLED', pyOn],
  ['BOUNCE_PROTECT_ENABLED', pyOn], ['INBOX_INTAKE_ENABLED', jsOn], ['INBOX_READY_ENABLED', jsOn],
  ['VOLUME_REVIEW_ENABLED', jsOn], ['ZAPMAIL_ALLOW_SPEND', pyOn], ['SCALEDMAIL_ALLOW_SPEND', pyOn],
  ['SCALEDMAIL_ALLOW_CANCEL', pyOn]
];
/** Switches that let the bot write to a live tool, spend or post, as the code that reads them sees them. */
function liveWriteFlags(env = {}) {
  const on = FLAGS.filter(([k, read]) => read(env[k])).map(([k]) => `\`${k}\``);
  const sync = Object.keys(env).filter(k => /_ASSET_SYNC_ENABLED$/.test(k) && jsOn(env[k])).sort().map(k => `\`${k}\``);
  const inverted = [];
  if (env.COPY_SYNC_DISABLED !== 'true') inverted.push('copy sync (`COPY_SYNC_DISABLED` is not true)');
  if (env.PROVIDER_WATCH_POST !== 'off') inverted.push('provider watch post (`PROVIDER_WATCH_POST` is not off)');
  return [...on, ...sync, ...inverted];
}
const flagsLine = env => {
  const on = liveWriteFlags(env);
  return `Live-write flags on: ${on.length ? on.join(', ') : 'none'}.`;
};

/* ---------- PC v2 heartbeat ---------- */

const esc = s => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
const lastRunOf = hb => (hb && hb.last_run ? +new Date(hb.last_run) : NaN);

/** One line on the PC v2 job; red when it has not run for 26 hours or has never written a heartbeat. */
function heartbeatLine(hb, now) {
  const check = `Check the PC is on and the task "${PC_TASK}" ran.`;
  const last = lastRunOf(hb);
  if (!hb || !(last > 0)) {
    return { red: true, text: `🔴 PC v2 job: no heartbeat in Mongo, so nothing shows it has run. It should run every hour on Manveen's PC. ${check}` };
  }
  const age = +now - last;
  const night = hb.newest_night ? `newest v2 night ${fmtNight(hb.newest_night)}` : 'no v2 night read yet';
  const extra = [];
  if (Array.isArray(hb.errors) && hb.errors.length) {
    const first = String(hb.errors[0]).slice(0, 140);
    extra.push(`Its last run logged ${hb.errors.length} error${hb.errors.length === 1 ? '' : 's'}: ${esc(first)}`);
  }
  if (hb.last_post_ok === false) extra.push('Its last Slack post failed.');
  if (Number(hb.behind_main) > 0) extra.push(`The PC copy is ${hb.behind_main} commit${hb.behind_main === 1 ? '' : 's'} behind main.`);
  const tail = extra.length ? ` ${extra.join(' ')}` : '';
  if (age > HEARTBEAT_RED_MS) {
    return { red: true, text: `🔴 PC v2 job: last ran ${fmtAge(age)} ago (${fmtIst(new Date(last))} IST), ${night}. It should run every hour. ${check}${tail}` };
  }
  const mode = hb.mode ? `, mode ${hb.mode}` : '';
  return { red: false, text: `PC v2 job: last ran ${fmtAge(age)} ago, ${night}${mode}.${tail}` };
}

/** Tue 14:00 / Wed 10:00: red lines if Monday's v2 results are not in or the hourly PC job stopped. [] = fine. */
function v2WatchProblems(hb, now) {
  const check = `Check the PC is on and the task "${PC_TASK}" ran.`;
  const monday = lastMonday(now);
  const last = lastRunOf(hb);
  if (!hb || !(last > 0)) return [`🔴 v2 check: no heartbeat from the PC v2 job in Mongo, so the v2 results for ${fmtNight(monday)} cannot be confirmed. ${check}`];
  const out = [];
  if (!hb.newest_night || hb.newest_night < monday) {
    const had = hb.newest_night ? `the newest night read is ${fmtNight(hb.newest_night)}` : 'no night has been read yet';
    out.push(`🔴 v2 results for ${fmtNight(monday)} are not in Mongo (${had}). Download Monday's v2 CSVs on the PC; the hourly job reads them.`);
  }
  if (+now - last > V2_STALE_MS) {
    out.push(`🔴 The PC v2 job last ran ${fmtAge(+now - last)} ago (${fmtIst(new Date(last))} IST); it should run every hour. ${check}`);
  }
  return out;
}

/* ---------- the post ---------- */

const plural = (n, one, many) => `${n} ${n === 1 ? one : many}`;

/** "Thu 6 Nov 13:05, 15:05; Fri 7 Nov 09:05" (at most `max` times). */
function slotList(slots, max = 6) {
  const shown = slots.slice(0, max);
  const byDay = [];
  for (const s of shown) {
    const d = new Date(s);
    const day = fmtIst(d).slice(0, -6);
    const time = fmtIst(d).slice(-5);
    const last = byDay[byDay.length - 1];
    if (last && last.day === day) last.times.push(time); else byDay.push({ day, times: [time] });
  }
  const text = byDay.map(b => `${b.day} ${b.times.join(', ')}`).join('; ');
  return slots.length > max ? `${text} and ${slots.length - max} more` : text;
}

/**
 * The daily / after-restart post: { text, thread } (thread null when nothing is wrong).
 * kind 'daily' | 'restart'; upSince: when this process started.
 */
function healthPost({ kind = 'daily', now, since, capped = false, fromFirst = false, runs = [], heartbeat, env = {}, upSince, jobs = JOBS }) {
  const s = summarize({ runs, since, now, env, jobs });
  const v2 = heartbeatLine(heartbeat, now);
  const from = `${fmtIst(since)} IST${fromFirst ? ' (when run records began)' : capped ? ' (only the last 4 days are checked)' : ''}`;
  const lead = kind === 'restart' ? `Bot restarted at ${fmtIst(upSince || now).slice(-5)} IST. ` : '';
  const problems = [];
  if (s.missed.length) problems.push(`${s.missed.length} of ${plural(s.due, 'scheduled run', 'scheduled runs')} did not run`);
  if (s.failed.length) problems.push(`${plural(s.failed.length, 'run', 'runs')} failed`);
  if (s.unfinished.length) problems.push(`${plural(s.unfinished.length, 'run', 'runs')} started but never finished`);

  let head;
  if (problems.length) head = `${lead}Bot health since ${from}: ${problems.join(', ')}. Details in the thread.`;
  else if (!s.due) head = `${lead}Bot health since ${from}: no scheduled runs were due, none failed.`;
  else head = `${lead}Bot health since ${from}: all ${plural(s.due, 'scheduled run', 'scheduled runs')} ran, none failed.`;

  const parts = [head];
  if (kind === 'daily' || v2.red) parts.push(v2.text);
  parts.push(flagsLine(env));

  let thread = null;
  if (problems.length) {
    const title = j => jobInfo(j, jobs).title;
    const sections = [];
    if (s.missed.length) {
      const byJob = new Map();
      for (const m of s.missed) byJob.set(m.job, [...(byJob.get(m.job) || []), m.slot]);
      sections.push(['Did not run (no run recorded for that time):',
        ...[...byJob].map(([j, slots]) => `• ${title(j)}: ${slotList(slots)}`)].join('\n'));
    }
    if (s.failed.length) {
      const lines = s.failed.slice(0, 15).map(r => {
        const label = r.label ? ` (${r.label})` : '';
        const when = fmtIst(new Date(r.slot && !isNaN(new Date(r.slot)) ? r.slot : r.started));
        return `• ${title(r.job)}${label}, ${when}: ${exitNote(r.job, r.exit_code, r, jobs)}.`;
      });
      if (s.failed.length > 15) lines.push(`• and ${s.failed.length - 15} more`);
      sections.push(['Failed (the error is in the Render log):', ...lines].join('\n'));
    }
    if (s.unfinished.length) {
      sections.push(['Started but no finish recorded (the bot most likely restarted mid-run):',
        ...s.unfinished.slice(0, 15).map(r => `• ${title(r.job)}${r.label ? ` (${r.label})` : ''}, started ${fmtIst(new Date(r.started))}`)].join('\n'));
    }
    thread = sections.join('\n\n');
  }
  return { text: parts.join('\n\n'), thread, summary: s, red: v2.red };
}

/** The Tue 14:00 / Wed 10:00 v2 post, or null when Monday's results are in and the PC job is running. */
function v2WatchPost(hb, now) {
  const lines = v2WatchProblems(hb, now);
  return lines.length ? { text: lines.join('\n\n'), thread: null } : null;
}

/** Today's 10:05 slot when it is a weekday and 10:05 IST has passed, else null (the restart post covers it). */
function dueDailySlot(now) {
  const p = istParts(now);
  if (p.dow === 0 || p.dow === 6 || p.hour * 60 + p.minute < 10 * 60 + 5) return null;
  return `${istDay(now)}T10:05+05:30`;
}

// cronlocks (index.js takeLock): one doc per run key. Every key carries the IST day (onceToday) or the
// IST hour (the restart post), so a lock never needs to expire, and it must not expire that day: the
// catch-ups run up to 5.5 hours after their slot and the restart post checks today's bot-health lock
// until midnight. The old schema had a 2-hour TTL on createdAt (never built, audit 7 Nov 2026); once
// built, a restart 2+ hours after a slot would re-post Kill warnings, re-run copy sync --apply and the
// final placement report, and make a second daily health post. So the schema has no TTL, and the lock
// time goes in takenAt, which no TTL index covers even if an old createdAt one gets built.
// Never build a cronlocks TTL shorter than 2 days.
const CRON_LOCK_FIELDS = {
  jobName: { type: String, required: true, unique: true },
  takenAt: { type: Date, default: Date.now }
};
/** The cronlocks doc for one run key: _id = key (Mongo keeps _id unique with no extra index), no createdAt. */
const lockDoc = (key, now = new Date()) => ({ _id: key, jobName: key, takenAt: now });

module.exports = {
  CRON_LOCK_FIELDS, lockDoc,
  INFRA_CHANNEL, JOBS, PC_TASK, cronsOf, jobInfo,
  istParts, istDay, slotKey, fmtIst, fmtNight, fmtAge, lastMonday,
  parseCron, cronMatches, expectedSlots, exitOk, exitNote, exitLine,
  windowStart, summarize, liveWriteFlags, flagsLine, heartbeatLine, v2WatchProblems,
  slotList, healthPost, v2WatchPost, dueDailySlot
};
