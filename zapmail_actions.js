/**
 * Zapmail actions in Slack: the /zapmail home menu, the domain look-up form,
 * and the buttons that change things on Zapmail.
 *
 * Who can press what:
 *   - Everyone: the home menu, every read-only view, the look-up form.
 *   - Approvers only (Slack user ids in ZAPMAIL_APPROVERS, or the older
 *     ZAPMAIL_BUY_APPROVERS): set up mailboxes, export to Smartlead, auto-renew
 *     on/off, renew, apply the tracker sync. Every one has a confirm dialog or
 *     a form, and the Python side re-checks everything (client routing, export
 *     target, and for money — renew — ZAPMAIL_ALLOW_SPEND=true).
 * Buttons only appear for CURRENT clients' domains; past-client domains are
 * being left to lapse (standup 2026-09-29).
 */

const NL = String.fromCharCode(10);
const DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const NAMES_RE = /^[A-Za-z][A-Za-z .'-]{0,40}(,\s*[A-Za-z][A-Za-z .'-]{0,40}){0,4}$/;
const CURRENT_CLIENTS = ['Bettrdata', 'Belardi Wong', 'Melior', 'Precise Leads'];

function approvers() {
  return (process.env.ZAPMAIL_APPROVERS || process.env.ZAPMAIL_BUY_APPROVERS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
}

function isApprover(userId) {
  return approvers().includes(userId);
}

function packValue(obj) { return JSON.stringify(obj); }
function unpackValue(v) { try { return JSON.parse(v); } catch (e) { return {}; } }

/** Validate a payload that came back from one of our own buttons. */
function checkPayload(p, needs) {
  if (needs.includes('domain') && !DOMAIN_RE.test(String(p.domain || ''))) return 'bad domain';
  if (needs.includes('client') && !CURRENT_CLIENTS.includes(p.client)) return 'not a current client';
  if (needs.includes('domain_id') && !UUID_RE.test(String(p.domain_id || ''))) return 'bad domain id';
  return null;
}

/* ---------------------------------------------------------------- blocks */

function homeBlocks() {
  const btn = (text, action_id, value, style) => {
    const b = { type: 'button', action_id, text: { type: 'plain_text', text }, value: value || 'x' };
    if (style) b.style = style;
    return b;
  };
  return [
    { type: 'section', text: { type: 'mrkdwn', text: '*Zapmail* — domains, mailboxes and renewals for Precise Leads, BettrData, Melior and Belardi Wong. Views are read-only; changes are approver-only and ask first.' } },
    { type: 'actions', block_id: 'zm_home_1', elements: [
      btn('Fleet status', 'zm_nav_status', 'status'),
      btn('Renewals', 'zm_nav_renewals', 'renewals'),
      btn('Purchase plan', 'zm_nav_batches', 'batches'),
      btn("Today's digest", 'zm_nav_digest', 'digest')
    ] },
    { type: 'actions', block_id: 'zm_home_2', elements: [
      btn('Suggest domains', 'zm_home_suggest', 'suggest', 'primary'),
      btn('Look up a domain', 'zm_lookup_open', 'lookup'),
      btn('Pre-warmed mailboxes', 'zm_nav_prewarmed', 'prewarmed'),
      btn('Tracker sync', 'zm_nav_sync', 'sync'),
      btn('Tracker vs Zapmail', 'zm_nav_cross-check', 'cross-check')
    ] },
    { type: 'context', elements: [{ type: 'mrkdwn', text: 'Typed versions: `/domains zapmail status | renewals | batches | digest | domain x.com | prewarmed | sync | cross-check`.' }] }
  ];
}

function lookupModal(channelId) {
  return {
    type: 'modal', callback_id: 'zm_lookup_submit', private_metadata: channelId || '',
    title: { type: 'plain_text', text: 'Look up a domain' },
    submit: { type: 'plain_text', text: 'Look up' },
    blocks: [{
      type: 'input', block_id: 'domain',
      label: { type: 'plain_text', text: 'Domain' },
      element: { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'askbettrdata.com' } }
    }]
  };
}

/** A domain look-up result (zapmail_status.py --domain --json) → blocks with actions. */
function domainBlocks(r) {
  const hits = (r && r.hits) || [];
  if (!hits.length) {
    return [{ type: 'section', text: { type: 'mrkdwn', text: '`' + (r && r.domain) + '` is not in any connected Zapmail account.' } }];
  }
  const blocks = [];
  for (const h of hits) {
    if (h.error) {
      blocks.push({ type: 'section', text: { type: 'mrkdwn', text: ':warning: ' + h.account + ': ' + h.error } });
      continue;
    }
    const boxes = h.mailboxes || [];
    // Zapmail scores a domain with no mailboxes 0 / "critical". That is not a
    // problem with the domain, and a red siren made the team think it was.
    const health = !boxes.length
      ? ' · health n/a (no mailboxes yet)'
      : h.health && h.health.score !== undefined && h.health.score !== null
        ? ' · health ' + h.health.score + (h.health.score <= 30 ? ' :rotating_light:' : '') + (h.health.label ? ' (' + h.health.label + ')' : '')
        : '';
    let text = '*`' + h.domain + '`* — ' + (h.client || '_not a current client_') + ' · ' + h.account
      + ' (' + (h.provider === 'MICROSOFT' ? 'Outlook' : 'Google') + ') · ' + (h.status || '?')
      + ' · expires ' + (h.expire_on || '?') + ' · auto-renew ' + (h.auto_renew ? 'on' : 'off') + health;
    text += NL + (boxes.length
      ? boxes.map(b => '• ' + b.email + ' — ' + (b.status || '?') + (b.warmed_up ? ' · warmed' : '')).join(NL)
      : '_no mailboxes yet_');
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: text.slice(0, 2900) } });
    if (!h.client) {
      blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: 'No actions: not a current client (past-client domains are left to lapse).' }] });
      continue;
    }
    const base = { domain: h.domain, client: h.client, domain_id: h.domain_id };
    const elements = [];
    const active = boxes.filter(b => String(b.status).toUpperCase() === 'ACTIVE').length;
    if (boxes.length < 5) {
      elements.push({ type: 'button', action_id: 'zm_mbx_open', text: { type: 'plain_text', text: 'Set up mailboxes' }, value: packValue({ ...base, existing: boxes.length }) });
    }
    if (active > 0) {
      elements.push({
        type: 'button', action_id: 'zm_export', text: { type: 'plain_text', text: 'Export to Smartlead' }, value: packValue(base),
        confirm: { title: { type: 'plain_text', text: 'Export to Smartlead?' },
          text: { type: 'plain_text', text: 'Export the ' + active + ' active mailbox(es) on ' + h.domain + ' to ' + h.client + '’s Smartlead. Re-exports are capped per mailbox per week.' },
          confirm: { type: 'plain_text', text: 'Export' }, deny: { type: 'plain_text', text: 'Cancel' } }
      });
    }
    elements.push({
      type: 'button', action_id: 'zm_autorenew', text: { type: 'plain_text', text: h.auto_renew ? 'Turn auto-renew off' : 'Turn auto-renew on' },
      value: packValue({ ...base, enable: !h.auto_renew }),
      confirm: { title: { type: 'plain_text', text: 'Change auto-renew?' },
        text: { type: 'plain_text', text: 'Turn auto-renew ' + (h.auto_renew ? 'off' : 'on') + ' for ' + h.domain + '.' },
        confirm: { type: 'plain_text', text: 'Yes' }, deny: { type: 'plain_text', text: 'Cancel' } }
    });
    elements.push({ type: 'button', action_id: 'zm_sync_domain', text: { type: 'plain_text', text: 'Update tracker' }, value: packValue({ domain: h.domain }) });
    blocks.push({ type: 'actions', elements });
  }
  return blocks.slice(0, 48);
}

function mailboxModal(p, channelId) {
  const free = Math.max(1, 5 - (p.existing || 0));
  const options = Array.from({ length: free }, (_, i) => ({ text: { type: 'plain_text', text: String(i + 1) }, value: String(i + 1) }));
  return {
    type: 'modal', callback_id: 'zm_mbx_submit',
    private_metadata: packValue({ domain: p.domain, client: p.client, channel: channelId }),
    title: { type: 'plain_text', text: 'Set up mailboxes' },
    submit: { type: 'plain_text', text: 'Create' },
    blocks: [
      { type: 'section', text: { type: 'mrkdwn', text: '`' + p.domain + '` for *' + p.client + '* — ' + (p.existing || 0) + '/5 mailboxes today.' } },
      { type: 'input', block_id: 'count', label: { type: 'plain_text', text: 'How many to add' },
        element: { type: 'static_select', action_id: 'value', initial_option: options[Math.min(1, options.length - 1)], options } },
      { type: 'input', block_id: 'names', optional: true, label: { type: 'plain_text', text: 'Sender names (optional)' },
        hint: { type: 'plain_text', text: 'Real senders, comma-separated: "Jane Doe, John Roe". Blank = generated names.' },
        element: { type: 'plain_text_input', action_id: 'value' } }
    ]
  };
}

/** Renewals (zapmail_status.py --renewals --json) → current clients with buttons, past clients as a count. */
function renewalBlocks(result) {
  const rows = (result && result.renewals) || [];
  const errors = (result && result.renewal_errors) || [];
  const current = rows.filter(r => r.client).sort((a, b) => (a.expire_on || '').localeCompare(b.expire_on || ''));
  const past = rows.filter(r => !r.client);
  const blocks = [{ type: 'section', text: { type: 'mrkdwn', text: '*Renewals — next 2 months*' + NL
    + current.length + ' current-client domain(s) · ' + past.length + ' past-client domain(s) left to lapse'
    + (errors.length ? NL + errors.map(e => ':warning: could not check ' + e).join(NL) : '') } }];
  for (const r of current.slice(0, 20)) {
    blocks.push({
      type: 'section',
      text: { type: 'mrkdwn', text: '`' + r.domain + '` — ' + r.client + ' · expires *' + r.expire_on + '* · ' + (r.assigned_mailboxes || 0) + ' inboxes' + (r.provider === 'MICROSOFT' ? ' · Outlook' : '') },
      accessory: {
        type: 'overflow', action_id: 'zm_renewal_menu',
        options: [
          // Domain name + account ride along so the reply names the domain and
          // the wallet that actually pays (Melior's domains sit in the Precise
          // Leads account, not a "Melior wallet").
          { text: { type: 'plain_text', text: 'Renew now (~$21)' },
            value: ['renew', r.domain_id, r.client, r.domain, r.account || ''].join('|').slice(0, 150) },
          { text: { type: 'plain_text', text: 'Turn auto-renew on' },
            value: ['autorenew', r.domain_id, r.client, r.domain, r.account || ''].join('|').slice(0, 150) },
          { text: { type: 'plain_text', text: 'Look up' }, value: 'lookup|' + r.domain + '|' + r.client }
        ]
      }
    });
  }
  if (current.length > 20) blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: '…and ' + (current.length - 20) + ' more.' }] });
  if (past.length) {
    blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: 'Past clients (no action): ' + past.slice(0, 30).map(r => r.domain).join(', ') + (past.length > 30 ? ', …' : '') }] });
  }
  return blocks;
}

function prewarmedText(res) {
  const p = (res && res.prewarmed) || {};
  const stock = p.stock || {};
  let t = '*Pre-warmed mailboxes*' + NL + 'Zapmail stock: *' + (stock.google ?? '?') + '* Google · *' + (stock.microsoft ?? '?') + '* Outlook';
  for (const [acc, per] of Object.entries(p.accounts || {})) {
    for (const [prov, s] of Object.entries(per)) {
      if (s.error) { t += NL + '• ' + acc + ' ' + prov + ': :warning: ' + s.error; continue; }
      if (!s.slots) continue;
      const subs = (s.subscriptions || []).map(x => x.plan + ' $' + x.price + '/mo × ' + x.mailboxes + ' (renews ' + x.renews + ')').join('; ');
      t += NL + '• *' + acc + '* ' + (prov === 'MICROSOFT' ? 'Outlook' : 'Google') + ': ' + s.assigned + '/' + s.slots + ' slots used'
        + (s.free ? ' — *' + s.free + ' free*' : '') + ' · ' + subs;
    }
  }
  for (const [prov, list] of Object.entries(p.for_sale || {})) {
    if (!list.length) continue;
    t += NL + NL + '_For sale now (' + (prov === 'MICROSOFT' ? 'Outlook' : 'Google') + ')_: '
      + list.map(d => '`' + d.domain + '`' + (d.mailboxes.length ? ' (' + d.mailboxes.join(', ') + ')' : '')).join(', ');
  }
  return t + NL + '_About $7 per pre-warmed mailbox per month on our current plans. Buying more stays in the Zapmail app._';
}

function syncBlocks(res) {
  const c = (res && res.counts) || {};
  const ops = (res && res.ops) || [];
  let t = '*Tracker sync* — Zapmail → /infra (preview)' + NL
    + 'Domains: ' + (c.domain_insert || 0) + ' to add, ' + (c.domain_update || 0) + ' to update, ' + (c.domain_skipped || 0) + ' skipped (past client / unbranded / lapsed)' + NL
    + 'Inboxes: ' + (c.inbox_insert || 0) + ' to add, ' + (c.inbox_update || 0) + ' to update, ' + (c.inbox_skipped || 0) + ' skipped';
  const adds = ops.filter(o => o.action === 'insert').map(o => '`' + o.name + '`');
  if (adds.length) t += NL + 'New: ' + adds.slice(0, 20).join(', ') + (adds.length > 20 ? ', …' : '');
  const downs = ops.filter(o => o.action === 'update' && /status/.test(o.reason)).map(o => '`' + o.name + '`');
  if (downs.length) t += NL + 'Marked Inactive (lapsed / not active on Zapmail): ' + downs.join(', ');
  (res.errors || []).forEach(e => { t += NL + ':warning: ' + e; });
  const blocks = [{ type: 'section', text: { type: 'mrkdwn', text: t.slice(0, 2900) } }];
  if (ops.length) {
    blocks.push({ type: 'actions', elements: [{
      type: 'button', action_id: 'zm_sync_apply', style: 'primary', text: { type: 'plain_text', text: 'Apply to tracker' }, value: 'apply',
      confirm: { title: { type: 'plain_text', text: 'Update the tracker?' },
        text: { type: 'plain_text', text: 'Write ' + ops.length + ' change(s) to /infra. Expiry dates follow Zapmail; nothing is deleted and nothing is re-activated.' },
        confirm: { type: 'plain_text', text: 'Apply' }, deny: { type: 'plain_text', text: 'Cancel' } }
    }] });
  }
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: 'Runs daily at 9:30 IST (writes only when ZAPMAIL_ASSET_SYNC_ENABLED=true). ScaledMail stays manual.' }] });
  return blocks;
}

/* --------------------------------------------------------------- handlers */

function registerZapmailActions(app, baseDir, { onTrackerChanged } = {}) {
  const { runPy } = require('./domain_suggest_command');

  // Form results: "Only visible to you" in the channel the form came from;
  // a DM (also private) when that is not possible (no channel, bot not in it).
  async function say(client, channel, user, payload) {
    if (channel) {
      try {
        return await client.chat.postEphemeral({ channel, user, ...payload });
      } catch (err) { /* fall through to a DM */ }
    }
    return client.chat.postMessage({ channel: user, ...payload });
  }

  async function guard(body, respond) {
    const user = body.user && body.user.id;
    if (isApprover(user)) return user;
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: ':lock: Only Zapmail approvers can do this (ZAPMAIL_APPROVERS). Ask one, or use the CLI.' });
    return null;
  }

  async function runAndReport(respond, args, label, format, timeoutMs) {
    await respond({ response_type: 'ephemeral', replace_original: false, text: ':hourglass: ' + label + '…' });
    try {
      const r = await runPy(args, baseDir, timeoutMs);
      await respond({ response_type: 'ephemeral', replace_original: false, text: format(r) });
      return r;
    } catch (err) {
      await respond({ response_type: 'ephemeral', replace_original: false,
        text: ':x: ' + label + ' — not done. ' + require('./slack_text').plainError(err.message) });
      return null;
    }
  }

  // Home → suggest: reuse the /domains client picker.
  app.action('zm_home_suggest', async ({ ack, respond }) => {
    await ack();
    const flow = require('./domain_suggest_command');
    await respond({ response_type: 'ephemeral', replace_original: false, text: 'Suggest domains',
      blocks: flow.clientPickerBlocks(flow.loadProfiles(baseDir)) });
  });

  // Look-up form.
  app.action('zm_lookup_open', async ({ ack, body, client }) => {
    await ack();
    await client.views.open({ trigger_id: body.trigger_id, view: lookupModal(body.channel && body.channel.id) });
  });

  app.view('zm_lookup_submit', async ({ ack, body, view, client }) => {
    const domain = String(view.state.values.domain.value.value || '').trim().toLowerCase()
      .replace(/^https?:\/\//, '').replace(/\/.*$/, '');
    if (!DOMAIN_RE.test(domain)) {
      return ack({ response_action: 'errors', errors: { domain: 'Enter a domain like example.com' } });
    }
    await ack();
    const channel = view.private_metadata || null;
    const user = body.user.id;
    try {
      const r = await runPy(['zapmail_status.py', '--domain', domain, '--json'], baseDir);
      await say(client, channel, user, { text: 'Zapmail: ' + domain, blocks: domainBlocks(r) });
    } catch (err) {
      await say(client, channel, user, { text: ':x: Look-up failed: ' + err.message });
    }
  });

  // Mailboxes: form → create (WRITE; approvers).
  app.action('zm_mbx_open', async ({ ack, body, action, client, respond }) => {
    await ack();
    if (!(await guard(body, respond))) return;
    const p = unpackValue(action.value);
    if (checkPayload(p, ['domain', 'client'])) return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: bad request' });
    await client.views.open({ trigger_id: body.trigger_id, view: mailboxModal(p, body.channel && body.channel.id) });
  });

  app.view('zm_mbx_submit', async ({ ack, body, view, client }) => {
    const meta = unpackValue(view.private_metadata);
    const names = String((view.state.values.names.value.value) || '').trim();
    if (names && !NAMES_RE.test(names)) {
      return ack({ response_action: 'errors', errors: { names: 'Letters only, comma-separated, up to 5 names' } });
    }
    await ack();
    const user = body.user.id;
    if (!isApprover(user) || checkPayload(meta, ['domain', 'client'])) return;
    const count = Math.max(1, Math.min(5, parseInt(view.state.values.count.value.selected_option.value, 10) || 1));
    const args = ['zapmail_lifecycle.py', '--mailboxes', meta.domain, '--per-domain', String(count),
      '--client', meta.client, '--approve', '--timeout', '120', '--json'];
    if (names) args.push('--names', names);
    await say(client, meta.channel, user, { text: ':hourglass: <@' + user + '> is creating ' + count + ' mailbox(es) on `' + meta.domain + '`…' });
    try {
      const r = await runPy(args, baseDir, 4 * 60 * 1000);
      const x = Array.isArray(r) ? r[0] || {} : r;
      const text = x.error
        ? ':x: `' + meta.domain + '`: ' + require('./slack_text').plainError(x.error)
        : ':white_check_mark: `' + meta.domain + '`: ' + (x.created || []).join(', ')
          + ((x.pending || []).length ? NL + '_Still being created on Zapmail — Google usually within an hour, Outlook a few hours. You’ll see them in the digest; then use Export to Smartlead._' : '');
      // The new inboxes reach /infra via the webhook or the 9:30 sync once ACTIVE.
      await say(client, meta.channel, user, { text });
    } catch (err) {
      await say(client, meta.channel, user, { text: ':x: Mailbox setup failed: ' + err.message });
    }
  });

  // Export (WRITE; approvers).
  app.action('zm_export', async ({ ack, body, action, respond }) => {
    await ack();
    if (!(await guard(body, respond))) return;
    const p = unpackValue(action.value);
    if (checkPayload(p, ['domain', 'client'])) return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: bad request' });
    await runAndReport(respond, ['zapmail_export.py', '--export', p.domain, '--client', p.client, '--approve', '--json'],
      'Exporting `' + p.domain + '` to ' + p.client + '’s Smartlead',
      r => { const x = Array.isArray(r) ? r[0] || {} : r;
        return x.ok === false ? ':x: Export `' + p.domain + '`: ' + require('./slack_text').plainError(x.error)
          : ':white_check_mark: Export of `' + p.domain + '` started' + (x.export_id ? ' (export ' + x.export_id + ')' : '') + '.'; });
  });

  // Auto-renew (WRITE; approvers).
  async function autoRenew(respond, p, enable) {
    return runAndReport(respond, ['zapmail_maintenance.py', '--auto-renew', p.domain_id,
      enable ? '--enable' : '--disable', '--approve', '--client', p.client, '--json'],
      'Turning auto-renew ' + (enable ? 'on' : 'off') + ' for ' + (p.domain || p.domain_id),
      r => (r && r.error) ? ':x: ' + require('./slack_text').plainError(r.error) : ':white_check_mark: Auto-renew ' + (enable ? 'on' : 'off') + ' for ' + (p.domain || p.domain_id) + '.');
  }

  app.action('zm_autorenew', async ({ ack, body, action, respond }) => {
    await ack();
    if (!(await guard(body, respond))) return;
    const p = unpackValue(action.value);
    if (checkPayload(p, ['client', 'domain_id'])) return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: bad request' });
    await autoRenew(respond, p, Boolean(p.enable));
  });

  // Renewals overflow menu: renew (SPEND) / auto-renew on / look up.
  app.action('zm_renewal_menu', async ({ ack, body, action, respond, client }) => {
    await ack();
    const [verb, id, clientName, domainName, accountName] = String((action.selected_option || {}).value || '').split('|');
    if (verb === 'lookup') {
      if (!DOMAIN_RE.test(id)) return;
      const r = await runPy(['zapmail_status.py', '--domain', id, '--json'], baseDir);
      return respond({ response_type: 'ephemeral', replace_original: false, text: 'Zapmail: ' + id, blocks: domainBlocks(r) });
    }
    if (!(await guard(body, respond))) return;
    // Name the domain in replies, not its Zapmail id.
    const p = { domain_id: id, client: clientName, domain: DOMAIN_RE.test(domainName || '') ? domainName : undefined };
    if (checkPayload(p, ['client', 'domain_id'])) return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: bad request' });
    if (verb === 'autorenew') return autoRenew(respond, p, true);
    if (verb === 'renew') {
      return runAndReport(respond, ['zapmail_maintenance.py', '--renew', id, '--approve', '--client', clientName, '--json'],
        'Renewing ' + (DOMAIN_RE.test(domainName || '') ? '`' + domainName + '`' : id) + ' for ' + clientName
          + (accountName ? ' (paid from the ' + accountName + ' Zapmail wallet)' : ''),
        r => (r && r.error) ? ':x: ' + require('./slack_text').plainError(r.error) : ':white_check_mark: Renewal submitted' + ((r && r.data && r.data.paymentLink) ? ' — <' + r.data.paymentLink + '|payment link>' : '') + '.');
    }
  });

  // Tracker sync: one domain (approvers) / apply all (approvers).
  app.action('zm_sync_domain', async ({ ack, body, action, respond }) => {
    await ack();
    if (!(await guard(body, respond))) return;
    const p = unpackValue(action.value);
    if (checkPayload(p, ['domain'])) return;
    const r = await runAndReport(respond, ['zapmail_asset_sync.py', '--apply', '--domain', p.domain, '--json'],
      'Updating the tracker for `' + p.domain + '`',
      x => ':white_check_mark: Tracker: ' + (x.applied || 0) + ' row(s) written for `' + p.domain + '`.');
    if (r && r.applied && typeof onTrackerChanged === 'function') onTrackerChanged();
  });

  app.action('zm_sync_apply', async ({ ack, body, respond }) => {
    await ack();
    if (!(await guard(body, respond))) return;
    const r = await runAndReport(respond, ['zapmail_asset_sync.py', '--apply', '--json'],
      'Applying Zapmail → tracker sync',
      x => x.note ? ':warning: ' + x.note : ':white_check_mark: Tracker updated: ' + (x.applied || 0) + ' row(s) written.',
      5 * 60 * 1000);
    if (r && r.applied && typeof onTrackerChanged === 'function') onTrackerChanged();
  });
}

module.exports = {
  registerZapmailActions, homeBlocks, domainBlocks, renewalBlocks, prewarmedText,
  syncBlocks, lookupModal, mailboxModal, approvers, isApprover, checkPayload
};
