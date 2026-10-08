/**
 * One-click domain suggestions in Slack: pick a client → tick names → stage → buy.
 *
 *   /domains                  → a button per client ("Suggest for BettrData" …)
 *   click a client            → Zapmail AI Domain Finder + our generator run
 *                               (smartlead_sync/domain_suggest.py, ~1-2 min),
 *                               results come back as tick boxes with prices
 *   "Stage purchase"          → the ticked names become a staggered, ledgered
 *                               batch plan (domain_generator.py --buy, read-only)
 *   "Buy now" (due batches)   → ONLY for Slack users listed in
 *                               ZAPMAIL_BUY_APPROVERS, behind a confirm dialog,
 *                               and still refused by Python unless the server
 *                               has ZAPMAIL_ALLOW_SPEND=true plus every other
 *                               purchase check (ledger, date, live price, account).
 *
 * Client profiles (site + keywords) come from smartlead_sync/domain_clients.json.
 */

const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');

const NL = String.fromCharCode(10);
const SAFE_DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const BATCH_ID_RE = /^[0-9a-f]{12}$/;
const MAX_PER_GROUP = 10;   // Slack: checkboxes element holds at most 10 options

function loadProfiles(baseDir) {
  const file = process.env.DOMAIN_CLIENTS_FILE
    || path.join(baseDir, 'smartlead_sync', 'domain_clients.json');
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8')).clients || {};
  } catch (err) {
    console.error('[domain-suggest] cannot read profiles:', err.message);
    return {};
  }
}

function profileReady(p) {
  return Boolean(p && (p.main_domain || p.brand_stem)) && (p.keywords || []).length >= 3;
}

function approvers() {
  // ZAPMAIL_APPROVERS covers every Zapmail action; ZAPMAIL_BUY_APPROVERS is the older name.
  return (process.env.ZAPMAIL_APPROVERS || process.env.ZAPMAIL_BUY_APPROVERS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
}

/** Run a smartlead_sync script and resolve its last-line JSON (errors too). */
function runPy(args, baseDir, timeoutMs = 4 * 60 * 1000) {
  return new Promise((resolve, reject) => {
    const python = process.env.INFRABOT_PYTHON || 'python';
    const proc = spawn(python, args, {
      cwd: path.join(baseDir, 'smartlead_sync'),
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    let stdout = '';
    let stderr = '';
    proc.stdout.on('data', d => { stdout += d; });
    proc.stderr.on('data', d => { stderr += d; process.stderr.write('[domain-suggest] ' + d); });
    const timer = setTimeout(() => { proc.kill(); reject(new Error('timed out')); }, timeoutMs);
    proc.on('close', code => {
      clearTimeout(timer);
      const line = stdout.trim().split('\n').filter(Boolean).pop();
      let parsed = null;
      try { parsed = line ? JSON.parse(line) : null; } catch (e) { parsed = null; }
      if (parsed && (code === 0 || parsed.error)) return resolve(parsed);
      const tail = stderr.trim().split('\n').slice(-3).join('\n');
      reject(new Error(tail || 'exited with code ' + code));
    });
    proc.on('error', err => { clearTimeout(timer); reject(err); });
  });
}

/** Buttons, one per configured client. */
function clientPickerBlocks(profiles) {
  const ready = Object.entries(profiles).filter(([, p]) => profileReady(p));
  const notReady = Object.entries(profiles).filter(([, p]) => !profileReady(p));
  const blocks = [{
    type: 'section',
    text: { type: 'mrkdwn', text: '*Suggest sending domains* — pick a client. Zapmail’s AI Domain Finder and our generator both run; you get names that are available now, checked against everything we already own and the blacklists. Takes 1-2 minutes.' }
  }];
  if (ready.length) {
    blocks.push({
      type: 'actions',
      block_id: 'domains_clients',
      elements: ready.slice(0, 25).map(([key, p]) => ({
        type: 'button',
        action_id: 'domains_suggest_' + key.replace(/[^a-z0-9]/gi, '_').toLowerCase(),
        text: { type: 'plain_text', text: 'Suggest for ' + (p.label || key) },
        value: key
      }))
    });
  }
  const ctx = ['Profiles: `smartlead_sync/domain_clients.json`. Advanced: `/domains help`.'];
  if (notReady.length) {
    ctx.unshift('Not set up yet: ' + notReady.map(([k, p]) => p.label || k).join(', ')
      + ' (add their website + keywords).');
  }
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: ctx.join(' ') }] });
  return blocks;
}

/** One tick-box line: where it can be bought and for how much. */
function suggestionLine(s) {
  const money = v => '$' + Number(v).toFixed(2);
  const src = { ai: 'Zapmail AI', generator: 'our generator', scaledmail: 'ScaledMail idea' }[s.source] || s.source;
  const zm = s.price !== null && s.price !== undefined
    ? 'Zapmail ' + money(s.price) + (s.renew_price ? ' (renews ' + money(s.renew_price) + ')' : '') : null;
  const sm = s.scaledmail && s.scaledmail.available && s.scaledmail.price !== null && s.scaledmail.price !== undefined
    ? 'ScaledMail ' + money(s.scaledmail.price) + (s.scaledmail.renew_price ? ' (renews ' + money(s.scaledmail.renew_price) + ')' : '') : null;
  return ('`' + s.domain + '` · ' + [zm, sm].filter(Boolean).join(' · ') + ' · ' + src).slice(0, 150);
}

/** Result of domain_suggest.py → tick boxes + stage button. */
function suggestionBlocks(r) {
  if (r.error) return [{ type: 'section', text: { type: 'mrkdwn', text: ':x: ' + r.error } }];
  const rows = r.suggestions || [];
  const head = '*Domain suggestions for ' + r.label + '* (' + r.main_domain + ')'
    + NL + 'Keywords: ' + (r.keywords || []).join(', ')
    + NL + 'Zapmail AI: ' + r.ai_usable + ' usable · our generator: ' + r.generator_usable + ' usable'
    + (r.scaledmail_usable !== undefined ? ' · ScaledMail: ' + r.scaledmail_usable + ' usable' : '')
    + (rows.length ? NL + 'Tick the ones you want, then *Stage purchase*.' : '');
  const blocks = [{ type: 'section', text: { type: 'mrkdwn', text: head } }];
  if (!rows.length) {
    blocks.push({ type: 'section', text: { type: 'mrkdwn', text: 'Nothing available passed our checks this time — try again in 30 minutes (search budget), or widen the keywords.' } });
  }
  for (let i = 0; i < rows.length; i += MAX_PER_GROUP) {
    blocks.push({
      type: 'actions',
      block_id: 'domains_pick_' + (i / MAX_PER_GROUP),
      elements: [{
        type: 'checkboxes',
        action_id: 'domains_pick',
        options: rows.slice(i, i + MAX_PER_GROUP).map(s => ({
          text: {
            type: 'mrkdwn',
            text: suggestionLine(s)
          },
          value: s.domain
        }))
      }]
    });
  }
  const buttons = [];
  if (rows.length) {
    buttons.push({ type: 'button', action_id: 'domains_stage', style: 'primary',
      text: { type: 'plain_text', text: 'Stage purchase (Zapmail)' }, value: r.client });
    if (rows.some(x => x.scaledmail && x.scaledmail.available)) {
      buttons.push({ type: 'button', action_id: 'domains_sm_order',
        text: { type: 'plain_text', text: 'Order on ScaledMail' }, value: r.client });
    }
  }
  buttons.push({ type: 'button', action_id: 'domains_suggest_again',
    text: { type: 'plain_text', text: 'Suggest again' }, value: r.client });
  blocks.push({ type: 'actions', block_id: 'domains_next', elements: buttons });
  const notes = [];
  if ((r.errors || []).length) notes.push(':warning: ' + r.errors.join('; '));
  if (r.estate_ok === false) notes.push(':warning: an owned-domain source failed — double-check before buying');
  notes.push('Prices are first-year; ceiling $' + r.price_ceiling + '. Nothing is bought until a batch is approved.');
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: notes.join(' · ') }] });
  return blocks;
}

/** Ticked domains from a block_actions payload's state. */
function selectedDomains(stateValues) {
  const out = [];
  for (const block of Object.values(stateValues || {})) {
    const el = block && block.domains_pick;
    if (!el || !Array.isArray(el.selected_options)) continue;
    for (const o of el.selected_options) {
      const d = String(o.value || '').toLowerCase();
      if (SAFE_DOMAIN_RE.test(d) && !out.includes(d)) out.push(d);
    }
  }
  return out;
}

/** A "Buy now" button (with a confirm dialog) for each batch due today. */
function buyButtonBlocks(batches, today, accountLabel) {
  return (batches || [])
    .filter(b => ['planned', 'failed'].includes(b.status || 'planned')
      && (b.earliest_date || '') <= today)
    .map(b => ({
      type: 'section',
      text: { type: 'mrkdwn', text: 'Due: `' + b.batch_id + '` ' + (b.client || '') + ' — '
        + (b.domains || []).join(', ') + ' · est. $' + Number(b.estimated_usd || 0).toFixed(2) },
      accessory: {
        type: 'button', action_id: 'domains_buy', style: 'danger',
        text: { type: 'plain_text', text: 'Buy now' }, value: b.batch_id,
        confirm: {
          title: { type: 'plain_text', text: 'Buy these domains?' },
          // plain_text: Slack confirm boxes print *bold* markers literally.
          text: { type: 'plain_text', text: 'Buy ' + (b.domains || []).length + ' domain(s) — '
            + (b.domains || []).join(', ') + ' — for about $' + Number(b.estimated_usd || 0).toFixed(2)
            + ' on ' + (accountLabel ? 'the ' + accountLabel : b.client ? b.client + '’s' : 'the client’s')
            + ' Zapmail account. Availability and price are re-checked live first.' },
          confirm: { type: 'plain_text', text: 'Buy' },
          deny: { type: 'plain_text', text: 'Cancel' }
        }
      }
    }));
}

/** Staged plan → text + a Buy button per due batch (approvers only). */
function stagedBlocks(result, formatBuyResult, today) {
  const blocks = [{ type: 'section', text: { type: 'mrkdwn', text: formatBuyResult(result) } }];
  if (result.error || !(result.batches || []).length) return blocks;
  const canBuy = approvers().length > 0 && result.ledger_ok && result.spend_account;
  if (!canBuy) {
    blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text:
      !result.spend_account
        ? '_This client has no Zapmail account to bill, so buying is refused._'
        : !result.ledger_ok
          ? '_Plan not recorded (ledger unreachable), so it cannot be bought._'
          : '_One-click buying is off (no `ZAPMAIL_BUY_APPROVERS` set) — an operator buys via the CLI._' }] });
    return blocks;
  }
  const buttons = buyButtonBlocks(result.batches, today, result.spend_account);
  blocks.push(...buttons);
  const later = result.batches.length - buttons.length;
  if (later) {
    blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: later
      + ' later batch(es): their Buy button appears in `/domains zapmail batches` on their date.' }] });
  }
  return blocks;
}

/** Find the profile key for free text ("bettrdata", "Belardi Wong"). */
function resolveClientKey(profiles, text) {
  const want = String(text || '').trim().toLowerCase().replace(/_/g, ' ');
  if (!want) return null;
  for (const [key, p] of Object.entries(profiles)) {
    if (key.toLowerCase() === want || String(p.label || '').toLowerCase() === want) return key;
  }
  return null;
}

/** Run suggestions for one client and post them (used by buttons + `/domains suggest X`). */
async function runSuggest(clientKey, respond, baseDir) {
  const p = loadProfiles(baseDir)[clientKey];
  if (!profileReady(p)) {
    return respond({ response_type: 'ephemeral', replace_original: false,
      text: ':x: `' + clientKey + '` has no ready domain profile (website + 3 keywords in domain_clients.json).' });
  }
  await respond({ response_type: 'ephemeral', replace_original: false,
    text: ':mag: Finding domains for *' + (p.label || clientKey) + '* (Zapmail AI + our generator, about 3-4 min — I’ll post here when done)…' });
  try {
    // Measured 2026-10-08: ~3.5 min for Precise Leads (Zapmail's AI finder +
    // live availability checks). The old 4-minute limit cut it off on Render.
    const r = await runPy(['domain_suggest.py', '--client', clientKey, '--count', '12', '--json'], baseDir,
      Number(process.env.DOMAIN_SUGGEST_TIMEOUT_MS) || 10 * 60 * 1000);
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: 'Domain suggestions for ' + (p.label || clientKey), blocks: suggestionBlocks(r) });
  } catch (err) {
    console.error('[domain-suggest] failed:', err);
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: ':x: ' + (err.message === 'timed out'
        ? 'Suggestions for ' + (p.label || clientKey) + ' took over 10 minutes and were stopped (usually Zapmail’s search limit — 100 searches per 30 min). Try again in 30 minutes.'
        : err.message) });
  }
}

function registerDomainSuggestFlow(app, baseDir) {
  const profiles = () => loadProfiles(baseDir);

  // Client buttons (one action_id per client) + "Suggest again".
  app.action(/^domains_suggest(_again|_[a-z0-9_]+)$/, async ({ ack, action, respond }) => {
    await ack();
    await runSuggest(String(action.value || ''), respond, baseDir);
  });

  // Tick boxes: nothing to do until "Stage purchase" reads their state.
  app.action('domains_pick', async ({ ack }) => { await ack(); });

  app.action('domains_stage', async ({ ack, action, body, respond }) => {
    await ack();
    const clientKey = String(action.value || '');
    if (!profiles()[clientKey]) {
      return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: unknown client.' });
    }
    const picked = selectedDomains(body.state && body.state.values);
    if (!picked.length) {
      return respond({ response_type: 'ephemeral', replace_original: false, text: 'Tick at least one domain first.' });
    }
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: 'Staging ' + picked.length + ' domain(s) for *' + clientKey + '* (re-checking availability + price)…' });
    try {
      const { formatBuyResult } = require('./domains_command');
      const r = await runPy(['domain_generator.py', '--buy', picked.join(','), '--client', clientKey, '--json'], baseDir);
      await respond({ response_type: 'ephemeral', replace_original: false,
        text: 'Staged purchase plan for ' + clientKey,
        blocks: stagedBlocks(r, formatBuyResult, new Date().toISOString().slice(0, 10)) });
    } catch (err) {
      console.error('[domain-suggest] stage failed:', err);
      await respond({ response_type: 'ephemeral', replace_original: false, text: ':x: ' + err.message });
    }
  });

  app.action('domains_buy', async ({ ack, action, body, respond }) => {
    await ack();
    const user = body.user && body.user.id;
    const batchId = String(action.value || '');
    if (!approvers().includes(user)) {
      return respond({ response_type: 'ephemeral', replace_original: false,
        text: ':lock: Only approved buyers can buy domains. Ask an approver, or an operator can run `zapmail_buy.py --execute ' + batchId + ' --approve`.' });
    }
    if (!BATCH_ID_RE.test(batchId)) {
      return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: bad batch id.' });
    }
    await respond({ response_type: 'ephemeral', replace_original: false,
      text: ':hourglass: <@' + user + '> approved batch `' + batchId + '` — buying (live price re-check first)…' });
    try {
      const r = await runPy(['zapmail_buy.py', '--execute', batchId, '--approve', '--json'], baseDir, 2 * 60 * 1000);
      const text = r.error
        ? ':no_entry: Not bought. ' + require('./slack_text').plainError(r.error)
        : ':white_check_mark: Bought `' + batchId + '`: ' + (r.domains || []).join(', ')
          + ((r.result && r.result.invoice_link) ? ' (<' + r.result.invoice_link + '|invoice>)' : '')
          + NL + 'Registration now runs on Zapmail (PENDING → ACTIVE; a name the registry rejects is refunded to the wallet). '
          + 'Check with `/domains zapmail domain <name>`, then `zapmail_lifecycle.py --provision ' + (r.domains || []).join(',') + ' --client "<client>"` to create mailboxes.';
      await respond({ response_type: 'ephemeral', replace_original: false, text });
    } catch (err) {
      console.error('[domain-suggest] buy failed:', err);
      await respond({ response_type: 'ephemeral', replace_original: false,
        text: ':warning: Buy of `' + batchId + '` did not finish cleanly: ' + err.message
          + NL + 'Check `/domains zapmail batches` — if it shows *unknown*, run `zapmail_buy.py --reconcile ' + batchId + '` before anything else.' });
    }
  });
}

module.exports = {
  registerDomainSuggestFlow,
  runSuggest,
  resolveClientKey,
  clientPickerBlocks,
  suggestionBlocks,
  suggestionLine,
  selectedDomains,
  stagedBlocks,
  buyButtonBlocks,
  approvers,
  loadProfiles,
  profileReady,
  runPy
};
