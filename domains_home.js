/**
 * The Domain Suggester app as the team's one front door: DM it (or use its
 * Messages / agent tab) and it answers like `/domains`.
 *
 *   "hi" / "menu"            → the one menu (Zapmail + ScaledMail together, tracker)
 *   "renewals", "status", "domain x.com", "infra expiring 7", "suggest bettrdata", …
 *                             → same as `/domains <text>`
 *   a CSV, or "renew"/"delete" + one name per line
 *                             → the tracker import / renew / delete (as Infra Bot)
 *
 * Slack side (app settings, once): Event Subscriptions → bot events
 * `message.im` (+ `assistant_thread_started` when the agent tab is on);
 * scopes `im:history`, `files:read` (CSV), `assistant:write` (agent tab).
 * Without `message.im` the app never hears DMs — the reason "hi" got no answer.
 */


/** The one menu (domains_unified.js): suggest per client, both providers, tracker. */
function homeBlocks(baseDir) {
  return require('./domains_unified').homeBlocks(baseDir);
}

/** The audit's Slack text, split into sections (each ≤ 2900 chars, ≤ 45 blocks). */
function auditBlocks(text) {
  const parts = String(text || '').split(/\n(?=\*P[0-2] · |_Not checkable)/);
  const blocks = [];
  for (const p of parts) {
    for (let i = 0; i < p.length; i += 2900) {
      blocks.push({ type: 'section', text: { type: 'mrkdwn', text: p.slice(i, i + 2900) } });
    }
  }
  return blocks.slice(0, 45);
}

/** A report (kill_report / infra_audit --json) as a screen: the headline,
 *  then one block per thread section, cut only at line breaks, within `max`
 *  blocks (the rest is in the channel thread). Old text-only results still render. */
function reportBlocks(r, max = 21) {
  const section = t => ({ type: 'section', text: { type: 'mrkdwn', text: t } });
  if (!r || r.error) return [section(':x: ' + ((r && r.error) || 'no result'))];
  if (!Array.isArray(r.headline)) return auditBlocks(r.text || '').slice(0, max);
  // Title line, a blank line, then the facts (same layout as the channel post).
  const [title, ...facts] = r.headline.filter(Boolean);
  const head = [require('./slack_text').boldTitle(title), facts.join('\n')];
  const out = [section(head.filter(Boolean).join('\n\n').slice(0, 2900)), { type: 'divider' }];
  const chunks = [];
  for (const part of r.thread || []) {
    let cur = '';
    for (const line of String(part || '').split('\n')) {
      const piece = line.slice(0, 2900);
      if (cur && cur.length + 1 + piece.length > 2900) { chunks.push(cur); cur = ''; }
      cur = cur ? cur + '\n' + piece : piece;
    }
    if (cur.trim()) chunks.push(cur);
  }
  const room = max - out.length;
  const fits = chunks.length <= room ? chunks : chunks.slice(0, room - 1);
  out.push(...fits.map(section));
  if (fits.length < chunks.length) {
    const n = chunks.length - fits.length;
    out.push({ type: 'context', elements: [{ type: 'mrkdwn',
      text: n + ' more section' + (n === 1 ? '' : 's') + ' not shown here; the full list is in the channel thread.' }] });
  }
  return out;
}

/** A `respond` that posts into the DM (and its thread, in the agent tab). */
function dmResponder(client, channel, threadTs) {
  return (payload) => {
    const p = { ...(typeof payload === 'string' ? { text: payload } : payload) };
    delete p.response_type; delete p.replace_original; delete p.delete_original;
    return client.chat.postMessage({ channel, ...(threadTs ? { thread_ts: threadTs } : {}), ...p });
  };
}

function cleanText(text) {
  return String(text || '')
    .replace(/<@[A-Z0-9]+>/g, ' ')              // mentions
    .replace(/^\s*\/(domains|domain)\b/i, ' ')    // someone typed the slash command as a message
    .replace(/<(https?:\/\/[^|>]+)\|([^>]+)>/g, '$2')   // Slack auto-links: <http://x.com|x.com> → x.com
    .replace(/<(https?:\/\/[^>]+)>/g, '$1')
    .trim();
}

function registerDomainsHome(app, baseDir, { infra, botToken } = {}) {
  const { handleDomainsText } = require('./domains_command');

  app.action('dh_suggest', async ({ ack, respond }) => {
    await ack();
    const flow = require('./domain_suggest_command');
    await require('./slack_reply').replace(respond, { text: 'Suggest domains',
      blocks: flow.clientPickerBlocks(flow.loadProfiles(baseDir)) });
  });
  app.action('dh_zapmail', async ({ ack, respond }) => {
    await ack();
    await require('./slack_reply').replace(respond, { text: 'Zapmail',
      blocks: require('./zapmail_actions').homeBlocks() });
  });

  // Every setup check in one read-only run (~1 min): DNS, name servers,
  // redirects, caps, warmup, signatures, provider mix, IP blacklists.
  app.action('dh_audit', async ({ ack, respond }) => {
    await ack();
    const { replace } = require('./slack_reply');
    await replace(respond, { text: ':mag: Running the infra audit (about a minute)…' });
    try {
      const r = await require('./domain_suggest_command').runPy(['infra_audit.py', '--json'], baseDir, 6 * 60 * 1000);
      if (r.error) throw new Error(r.error);
      await replace(respond, { text: 'Infra audit', blocks: reportBlocks(r, 45) });
    } catch (err) {
      await replace(respond, { text: ':x: Audit failed: ' + require('./slack_text').plainError(err.message) });
    }
  });

  // Agent tab opened: greet with the menu.
  app.event('assistant_thread_started', async ({ event, client }) => {
    const t = event.assistant_thread || {};
    if (!t.channel_id) return;
    await client.chat.postMessage({ channel: t.channel_id, thread_ts: t.thread_ts, text: 'Domains & inboxes', blocks: homeBlocks(baseDir) });
  });

  app.message(async ({ message, client }) => {
    if (!message || message.channel_type !== 'im' || message.bot_id) return;
    if (message.subtype && message.subtype !== 'file_share') return;
    const respond = dmResponder(client, message.channel, message.thread_ts);
    try {
      if (infra && await infra.handleMessage(message, text => respond({ text }), botToken)) return;
      const text = cleanText(message.text);
      if (!text && !(message.files || []).length) return;
      return await handleDomainsText(text || 'menu', {
        user: message.user, userId: message.user, client, trigger_id: null, respond, infra
      }, baseDir);
    } catch (err) {
      console.error('[domains-dm] failed:', err);
      try { await respond({ text: ':x: ' + require('./slack_text').plainError(err.message) }); } catch (_) { /* ignore */ }
    }
  });
}

module.exports = { homeBlocks, registerDomainsHome, dmResponder, cleanText, auditBlocks, reportBlocks };
