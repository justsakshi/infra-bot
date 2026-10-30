// "Show replies" button under each campaign in the daily stats post (#pl_daily).
// Manveen, 30 Oct 2026: click a campaign, see its new replies (who, company,
// category, just their words), visible only to whoever clicked, in the thread.
// The work is smartlead_sync/reply_view.py; this file only wires the click.
const { spawn } = require('child_process');
const path = require('path');

// Run reply_view.py with the button's value; resolves to {text, blocks}.
function runReplyView(value, { cwd = path.join(__dirname, 'smartlead_sync'), python = 'python', timeoutMs = 120000 } = {}) {
  return new Promise((resolve) => {
    const proc = spawn(python, ['reply_view.py', value], {
      cwd, env: { ...process.env, PYTHONIOENCODING: 'utf-8' },
    });
    let out = '';
    const timer = setTimeout(() => proc.kill(), timeoutMs);
    proc.stdout.on('data', d => { out += d; });
    proc.stderr.on('data', d => process.stderr.write(`[show-replies] ${d}`));
    proc.on('error', () => resolve(null));
    proc.on('close', () => {
      clearTimeout(timer);
      resolve(parseLastJson(out));
    });
  });
}

// The script's answer is its last JSON line; API retry notes may precede it.
function parseLastJson(out) {
  const lines = String(out || '').trim().split('\n').reverse();
  for (const line of lines) {
    try {
      const v = JSON.parse(line);
      if (v && Array.isArray(v.blocks)) return v;
    } catch (_) { /* not the answer line */ }
  }
  return null;
}

function where(body) {
  const msg = body.message || {};
  return {
    channel: body.channel && body.channel.id,
    user: body.user && body.user.id,
    thread_ts: msg.thread_ts || msg.ts,
  };
}

function registerShowReplies(app, { run = runReplyView } = {}) {
  app.action('show_replies', async ({ ack, body, action, client }) => {
    await ack();
    const { channel, user, thread_ts } = where(body);
    if (!channel || !user) return;
    let title = 'this campaign';
    try { title = JSON.parse(action.value).title || title; } catch (_) { /* keep default */ }
    const say = (m) => client.chat.postEphemeral({ channel, user, thread_ts, ...m })
      .catch(err => console.error('[show-replies] postEphemeral failed:', err.data ? err.data.error : err.message));
    await say({ text: `Fetching the replies for ${title}…` });
    const result = await run(action.value);
    if (!result) {
      await say({ text: `:warning: Couldn't read the replies for ${title}. Try again in a minute.` });
      return;
    }
    await say({ text: result.text || 'Replies', blocks: result.blocks.slice(0, 50) });
  });
}

module.exports = { registerShowReplies, runReplyView, parseLastJson };
