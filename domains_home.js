/**
 * The Domain Suggester app as the team's one front door: DM it (or use its
 * Messages / agent tab) and it answers like `/domains`.
 *
 *   "hi" / "menu"            → the home menu (domains, Zapmail, ScaledMail, tracker)
 *   "zapmail renewals", "sm status", "infra expiring 7", "suggest bettrdata", …
 *                             → same as `/domains <text>`
 *   a CSV, or "renew"/"delete" + one name per line
 *                             → the tracker import / renew / delete (as Infra Bot)
 *
 * Slack side (app settings, once): Event Subscriptions → bot events
 * `message.im` (+ `assistant_thread_started` when the agent tab is on);
 * scopes `im:history`, `files:read` (CSV), `assistant:write` (agent tab).
 * Without `message.im` the app never hears DMs — the reason "hi" got no answer.
 */

const NL = String.fromCharCode(10);

function homeBlocks() {
  const btn = (text, action_id, style) => {
    const b = { type: 'button', action_id, text: { type: 'plain_text', text }, value: action_id };
    if (style) b.style = style;
    return b;
  };
  return [
    { type: 'section', text: { type: 'mrkdwn', text: '*Domain Suggester* — domains, mailboxes and the asset tracker in one place. Type here like `/domains …`, or press a button.' } },
    { type: 'actions', block_id: 'dh_1', elements: [
      btn('Suggest domains', 'dh_suggest', 'primary'), btn('Zapmail', 'dh_zapmail'), btn('ScaledMail', 'sm_home'),
      btn('Infra audit', 'dh_audit')] },
    { type: 'section', text: { type: 'mrkdwn', text: '*Tracker* (what Infra Bot `/infra` does)' } },
    { type: 'actions', block_id: 'dh_2', elements: [
      btn('Expiring today', 'infra_expiring_0'), btn('Next 7 days', 'infra_expiring_7'),
      btn('Add asset', 'infra_add_open'), btn('Renew asset', 'infra_renew_open'), btn('List all', 'infra_list')] },
    { type: 'context', elements: [{ type: 'mrkdwn', text: [
      'Try: `zapmail renewals` · `sm status` · `infra expiring 7` · `suggest bettrdata` · `zapmail domain x.com`',
      'Send a CSV (`domains.csv`, `renew_inboxes.csv`, `delete_domains.csv`) or `renew` / `delete` + one name per line.'].join(NL) }] }
  ];
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
    await respond({ response_type: 'ephemeral', replace_original: false, text: 'Suggest domains',
      blocks: flow.clientPickerBlocks(flow.loadProfiles(baseDir)) });
  });
  app.action('dh_zapmail', async ({ ack, respond }) => {
    await ack();
    await respond({ response_type: 'ephemeral', replace_original: false, text: 'Zapmail',
      blocks: require('./zapmail_actions').homeBlocks() });
  });

  // Every setup check in one read-only run (~1 min): DNS, name servers,
  // redirects, caps, warmup, signatures, provider mix, IP blacklists.
  app.action('dh_audit', async ({ ack, respond }) => {
    await ack();
    await respond({ response_type: 'ephemeral', replace_original: false, text: ':mag: Running the infra audit (about a minute)…' });
    try {
      const r = await require('./domain_suggest_command').runPy(['infra_audit.py', '--json'], baseDir, 6 * 60 * 1000);
      if (r.error) throw new Error(r.error);
      await respond({ response_type: 'ephemeral', replace_original: false, text: 'Infra audit', blocks: auditBlocks(r.text) });
    } catch (err) {
      await respond({ response_type: 'ephemeral', replace_original: false, text: ':x: Audit failed: ' + require('./slack_text').plainError(err.message) });
    }
  });

  // Agent tab opened: greet with the menu.
  app.event('assistant_thread_started', async ({ event, client }) => {
    const t = event.assistant_thread || {};
    if (!t.channel_id) return;
    await client.chat.postMessage({ channel: t.channel_id, thread_ts: t.thread_ts, text: 'Domain Suggester', blocks: homeBlocks() });
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

module.exports = { homeBlocks, registerDomainsHome, dmResponder, cleanText, auditBlocks };
