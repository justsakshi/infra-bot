// The 10:15 IST stats post only fires if the bot is running at 10:15. On
// 4 Nov 2026 it was up at 10:00 but the 10:15 run never started (restart or
// sleep), so no post. A bot that starts between 10:15 and 12:00 IST on a
// weekday runs the missed post; the once-a-day Mongo lock in index.js stops a
// second one. The window ends at 12:00, after the 11:00 standup it is for.

function missedStatsPost(now, env) {
  if (env.CAMPAIGN_STATS_POST === 'off') return false;
  const ist = new Date(now.getTime() + 330 * 60000);          // read as UTC fields
  const day = ist.getUTCDay();
  if (day === 0 || day === 6) return false;
  const mins = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  return mins >= 10 * 60 + 15 && mins < 12 * 60;
}

// Other runs worth doing late (audit 7 Nov 2026: lost runs on restarts). A bot that
// starts inside a window runs that job once, recorded against the slot it missed;
// the job's once-a-day lock skips it if the slot already ran on another instance.
//   kill-report: Tue 13:30 Kill warnings; still useful the same afternoon.
//   placement-report-final: Mon/Tue 13:00 final run; the next day's runs only look at
//     that day's tests, so a missed final run is never reported otherwise.
//   copy-sync: Sun 18:30, Mon 05:00, Tue 05:00; only before that morning's scheduled
//     tests start (Mon 06:00, Tue 06:30), never while they run.
// These windows are safe only because the day's lock lasts all day: cronlocks has no TTL
// (bot_health.CRON_LOCK_FIELDS). Never build a cronlocks TTL shorter than 2 days, or a
// restart inside a window re-runs a job that already ran.
const CATCH_UPS = [
  { job: 'kill-report', days: [2], from: '13:30', until: '18:00' },
  { job: 'placement-report-final', days: [1, 2], from: '13:00', until: '18:00' },
  { job: 'copy-sync', days: [0], from: '18:30', until: '24:00' },
  { job: 'copy-sync', days: [1], from: '05:00', until: '06:00' },
  { job: 'copy-sync', days: [2], from: '05:00', until: '06:30' }
];

const toMins = hhmm => { const [h, m] = hhmm.split(':').map(Number); return h * 60 + m; };

/** The runs a bot starting at `now` missed and should still do: [{job, slot}] (slot = the IST minute it was due). */
function missedRuns(now, env = {}) {
  const ist = new Date(now.getTime() + 330 * 60000);
  const day = ist.getUTCDay();
  const mins = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  const date = ist.toISOString().slice(0, 10);
  const out = [];
  if (missedStatsPost(now, env)) out.push({ job: 'campaign-stats-post', slot: `${date}T10:15+05:30` });
  for (const c of CATCH_UPS) {
    if (c.days.includes(day) && mins >= toMins(c.from) && mins < toMins(c.until)) {
      out.push({ job: c.job, slot: `${date}T${c.from}+05:30` });
    }
  }
  return out;
}

// A restart kills whatever the bot was running and leaves its cron_runs record with no end (17 and 18 Nov 2026: the
// 10:00 sheet sync died mid-run two days running; All Inboxes stayed a day old and the 11:00 digest waited 30 minutes
// for a run that was gone). A new process marks those runs lost and runs the day's sync again, once (index.js).
const DAY_MS = 24 * 60 * 60 * 1000;
const istDate = d => new Date(+d + 330 * 60000).toISOString().slice(0, 10);

/** Runs the previous process left open: started in the 24 h before this process started, no end recorded. */
function lostRuns(runs, processStart) {
  const t = +processStart;
  return runs.filter(r => !r.ended && r.started && +new Date(r.started) < t && +new Date(r.started) > t - DAY_MS);
}

/**
 * What a bot that started at processStart owes today's 10:00 sheet sync, checked at `now`:
 * {kind: 'lost', slot} - it started before this process and never finished (run it again);
 * {kind: 'missed', slot} - no record today (the bot was down at 10:00);
 * null - before 10:00 IST, finished, or this process already started one.
 */
function syncCatchUp(now, processStart, runs) {
  const ist = new Date(+now + 330 * 60000);
  if (ist.getUTCHours() < 10) return null;
  const date = istDate(now);
  const slot = `${date}T10:00+05:30`;
  const today = runs.filter(r => r.job === 'smartlead-sync' && r.started && istDate(new Date(r.started)) === date);
  if (!today.length) return { kind: 'missed', slot };
  if (today.some(r => r.ended || +new Date(r.started) >= +processStart)) return null;
  return { kind: 'lost', slot: today[0].slot || slot };
}

// 23 Nov 2026: the bot restarted at 10:22 IST while the 10:15 stats post ran. The catch-up above found the day's lock
// already taken by the dead run, so #pl_daily got no post. A stats post that a restart cut off is run again, once, while
// it can still land before the standup (missedStatsPost's window).
/** Today's 10:15 slot when its stats post started before this process and never finished, else null. */
function statsRerun(now, processStart, runs, env = {}) {
  if (!missedStatsPost(now, env)) return null;
  const date = istDate(now);
  const today = runs.filter(r => r.job === 'campaign-stats-post' && r.started && istDate(new Date(r.started)) === date);
  if (!today.length || today.some(r => r.ended || +new Date(r.started) >= +processStart)) return null;
  return today[0].slot || `${date}T10:15+05:30`;
}

module.exports = { missedStatsPost, missedRuns, CATCH_UPS, lostRuns, syncCatchUp, statsRerun };
