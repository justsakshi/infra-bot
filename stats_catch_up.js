// The 10:15 IST stats post only fires if the bot is running at 10:15. On
// 4 Nov 2026 it was up at 10:00 but the 10:15 run never started (restart or
// sleep), so no post. A bot that starts between 10:15 and 12:00 IST on a
// weekday runs the missed post; the Mongo lock in index.js stops a second
// one. The lock expires after 2 hours, which is why the window ends at 12:00.

function missedStatsPost(now, env) {
  if (env.CAMPAIGN_STATS_POST === 'off') return false;
  const ist = new Date(now.getTime() + 330 * 60000);          // read as UTC fields
  const day = ist.getUTCDay();
  if (day === 0 || day === 6) return false;
  const mins = ist.getUTCHours() * 60 + ist.getUTCMinutes();
  return mins >= 10 * 60 + 15 && mins < 12 * 60;
}

module.exports = { missedStatsPost };
