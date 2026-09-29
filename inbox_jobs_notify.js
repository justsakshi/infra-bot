/**
 * Moves approved inbox setup jobs forward (every 10 minutes from index.js) and
 * privately tells the person who created a job when it changes: finished,
 * waiting on something, or stopped. Quiet when nothing changed.
 */
const path = require('path');
const { execFile } = require('child_process');

const USER_ID_RE = /^U[A-Z0-9]{6,}$/;
const ICON = { done: ':white_check_mark:', waiting: ':hourglass:', running: ':gear:',
  failed: ':x:', needs_check: ':warning:', cancelled: ':no_entry_sign:' };
const LABEL = { done: 'is done', waiting: 'is waiting', running: 'is running',
  failed: 'stopped', needs_check: 'needs a person to check it', cancelled: 'was cancelled' };

/** One Slack line per changed job. Pure. */
function jobUpdateText(r) {
  const head = (ICON[r.status] || ':information_source:') + ' Inbox setup for *' + r.client + '* ('
    + (r.domains || []).join(', ') + ') ' + (LABEL[r.status] || r.status);
  const lines = [head, '`' + r.job_id + '` · ' + (r.progress || '')];
  if (r.detail) lines.push(r.detail);
  if (r.status === 'needs_check') lines.push('_Nothing will be retried automatically. Check Zapmail billing / the purchase ledger before resuming._');
  return lines.join('\n');
}

function runTick(baseDir) {
  return new Promise(resolve => {
    const python = process.env.INFRABOT_PYTHON || 'python';
    execFile(python, ['inbox_jobs.py', '--tick', '--json'], {
      cwd: path.join(baseDir, 'smartlead_sync'), timeout: 9 * 60 * 1000,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    }, (err, stdout, stderr) => {
      if (stderr) process.stderr.write('[inbox-jobs] ' + stderr);
      const line = String(stdout || '').trim().split('\n').filter(Boolean).pop();
      try { resolve((JSON.parse(line) || {}).ticked || []); } catch (e) {
        if (err) console.warn('[inbox-jobs] tick failed:', err.message);
        resolve([]);
      }
    });
  });
}

async function dm(user, text) {
  const token = process.env.DOMAINS_SLACK_BOT_TOKEN || process.env.SLACK_BOT_TOKEN;
  if (!token || !USER_ID_RE.test(user || '')) { console.log('[inbox-jobs] ' + text.replace(/\n/g, ' | ')); return; }
  try {
    const r = await fetch('https://slack.com/api/chat.postMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=utf-8', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ channel: user, text, unfurl_links: false })
    });
    const j = await r.json();
    if (!j.ok) console.warn('[inbox-jobs] Slack error:', j.error);
  } catch (err) {
    console.warn('[inbox-jobs] Slack post failed:', err.message);
  }
}

/** One tick: advance jobs, DM their creators about the ones that changed. */
async function tickAndNotify(baseDir) {
  const results = await runTick(baseDir);
  for (const r of results.filter(x => x.changed)) await dm(r.created_by, jobUpdateText(r));
  return results;
}

module.exports = { tickAndNotify, jobUpdateText, runTick };
