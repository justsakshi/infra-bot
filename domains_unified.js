/**
 * One `/domains` for everything: Zapmail and ScaledMail together.
 *
 * The team should not have to know which provider a domain or inbox sits on.
 * Every view here asks BOTH providers at once and shows one answer:
 *
 *   /domains                    → the menu (suggest per client + everything below)
 *   /domains status             → fleet + monthly cost, Zapmail and ScaledMail
 *   /domains renewals           → what bills or renews soon: inbox subscriptions,
 *                                 orders and domain registrations, both providers
 *   /domains domain x.com       → wherever x.com lives, with that provider's buttons
 *   /domains purchases          → staged / placed buys on either provider
 *   /domains prewarmed | sync | digest | audit | quote … | find …
 *
 * DMs to the app accept the same words. The provider-specific menus
 * (`/domains zapmail`, `/domains sm`) still work but are no longer advertised.
 * Buttons on each provider's results are that provider's own handlers
 * (zapmail_actions.js, scaledmail_command.js) — one place, two engines.
 */

const NL = String.fromCharCode(10);
const DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const MAX_BLOCKS = 49;

const SUBS = ['status', 'renewals', 'billing', 'domain', 'lookup', 'purchases', 'prewarmed', 'sync',
  'digest', 'quote', 'find', 'cross-check', 'audit', 'fleet', 'review', 'kill'];

const HELP = [
  '*`/domains`* — one menu for domains, inboxes and renewals on Zapmail *and* ScaledMail.',
  '`/domains status` · `renewals` · `review` · `kill` · `domain x.com` · `purchases` · `prewarmed` · `sync` · `digest` · `audit`',
  '`/domains suggest precise leads` — name ideas with both providers’ prices',
  '`/domains quote 30000 google,outlook 70,30 low` · `/domains find keyword`',
  '`/domains infra add | renew | list | expiring 7` — the asset tracker',
  'Or just DM the Domain Suggester app (“hi”).'
].join(NL);

function section(text) { return { type: 'section', text: { type: 'mrkdwn', text: String(text).slice(0, 2900) } }; }
function textBlocks(text) {
  const out = [];
  const t = String(text || '');
  for (let i = 0; i < t.length && out.length < 6; i += 2900) out.push(section(t.slice(i, i + 2900)));
  return out;
}
function btn(text, action_id, value, style) {
  const b = { type: 'button', action_id, text: { type: 'plain_text', text }, value: value || action_id };
  if (style) b.style = style;
  return b;
}
function money(v) { const n = Number(v); return Number.isFinite(n) ? '$' + (Number.isInteger(n) ? n : n.toFixed(2)) : '$?'; }

/** The one menu. */
function homeBlocks(baseDir) {
  const blocks = [section('*Domains & inboxes* — Zapmail and ScaledMail together. Views are read-only; changes are approver-only and always ask first.')];
  try {
    const flow = require('./domain_suggest_command');
    const picker = flow.clientPickerBlocks(flow.loadProfiles(baseDir || __dirname)).filter(b => b.type === 'actions');
    if (picker.length) {
      blocks.push(section('*Suggest sending domains* (2-4 min, both providers’ prices):'));
      picker.slice(0, 2).forEach(b => blocks.push(b));
    }
  } catch (e) { /* profiles missing: the Suggest button below still opens the picker */ }
  blocks.push({ type: 'actions', block_id: 'u_home_1', elements: [
    btn('Fleet & cost', 'u_nav_status'), btn('Renewals & billing', 'u_nav_renewals', null, 'primary'),
    btn('Look up a domain', 'u_lookup_open'), btn('Purchases', 'u_nav_purchases'), btn("Today's digest", 'u_nav_digest')] });
  blocks.push({ type: 'actions', block_id: 'u_home_2', elements: [
    btn('Order mailboxes', 'sm_order_open', '{}'), btn('Price a volume', 'sm_quote_open'),
    btn('Pre-warmed', 'u_nav_prewarmed'), btn('Tracker sync', 'u_nav_sync'), btn('Infra audit', 'dh_audit'),
    btn('Kill warnings', 'u_nav_kill')] });
  blocks.push(section('*Tracker*'));
  blocks.push({ type: 'actions', block_id: 'u_home_3', elements: [
    btn('Expiring today', 'infra_expiring_0'), btn('Next 7 days', 'infra_expiring_7'),
    btn('Add asset', 'infra_add_open'), btn('Renew asset', 'infra_renew_open'), btn('List all', 'infra_list')] });
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text:
    'Type: `status` · `renewals` · `domain x.com` · `suggest bettrdata` · `infra expiring 7` — after `/domains`, or straight into a DM with this app.' }] });
  return blocks;
}

/** Which provider calls make up a unified view. */
function planFor(sub, args) {
  const zm = require('./zapmail_command');
  const sm = require('./scaledmail_command');
  const a = args || [];
  const z = (zsub, arg) => ({ provider: 'Zapmail', sub: zsub, cli: zm.commandFor(zsub, arg || ''), render: r => zm.render(zsub, r) });
  const s = (ssub, sargs) => ({ provider: 'ScaledMail', sub: ssub, cli: sm.commandFor(ssub, sargs || []), render: r => sm.render(ssub, r) });
  switch (sub) {
    case 'status': case 'fleet': return [z('status'), s('status')];
    case 'renewals': case 'billing':
      return [{ provider: 'Zapmail inbox billing', sub: 'billing', cli: ['zapmail_status.py', '--billing', '--days', '14', '--json'],
        render: r => ({ text: 'Zapmail billing', blocks: billingBlocks(r) }) }, z('renewals'), s('renewals')];
    case 'domain': case 'lookup':
      return DOMAIN_RE.test(a[0] || '') ? [z('domain', a[0]), s('domain', [a[0]])] : null;
    case 'purchases': return [z('batches'), s('plans')];
    case 'prewarmed': return [z('prewarmed'), s('prewarmed')];
    case 'sync': return [z('sync'), s('sync')];
    case 'digest': return [z('digest'), s('digest')];
    case 'cross-check': return [z('cross-check')];
    case 'quote': return [s('quote', a)];
    case 'find': return [s('find', a)];
    case 'kill': return [{ provider: 'Kill warnings', sub: 'kill', cli: ['kill_report.py', '--json'],
      render: r => ({ text: 'Kill warnings', blocks: require('./domains_home').auditBlocks(r.text || (':x: ' + (r.error || 'no result'))) }) }];
    case 'review': return [{ provider: 'Renewal review', sub: 'review', cli: ['renewal_review.py', '--days', String(parseInt(a[0], 10) || 14), '--json'],
      render: r => ({ text: 'Renewal review', blocks: reviewBlocks(r) }) }];
    case 'audit': return [{ provider: 'Infra audit', sub: 'audit', cli: ['infra_audit.py', '--json'],
      render: r => ({ text: 'Infra audit', blocks: require('./domains_home').auditBlocks(r.text) }) }];
    default: return null;
  }
}

/** renewal_review.py → per bill KEEP / RETIRE / CHECK, with one-click retire (Zapmail). */
function reviewBlocks(r) {
  if (!r || r.error) return [section(':x: ' + ((r && r.error) || 'no result'))];
  const blocks = [];
  const reviews = r.reviews || [];
  const save = reviews.reduce((a, x) => a + (x.retire_saves_monthly || 0), 0);
  blocks.push(section('*Before the bill: which inboxes to keep* — next ' + (r.days || 14) + ' days · retiring the flagged ones saves *'
    + money(save) + '/month*' + NL + '_RETIRE = under 80% inbox in its latest test, on a spam domain, or not in Smartlead at all. CHECK = no recent test, broken connection or low warmup reputation._'));
  for (const rv of reviews.slice(0, 12)) {
    const c = rv.counts || {};
    const bad = (rv.rows || []).filter(x => x.verdict === 'RETIRE');
    const chk = (rv.rows || []).filter(x => x.verdict === 'CHECK');
    let t = '*' + rv.bills_on + ' · ' + rv.provider + ' · ' + rv.label + '* — ' + money(rv.price) + ' · ' + (c.KEEP || 0) + ' keep · *'
      + (c.RETIRE || 0) + ' retire* · ' + (c.CHECK || 0) + ' check' + (rv.note ? NL + '_' + rv.note + '_' : '');
    t += bad.slice(0, 12).map(x => NL + ':x: `' + x.email + '` — ' + x.why + ((x.campaigns || []).length ? ' :warning: in a live campaign' : '')).join('');
    t += chk.slice(0, 5).map(x => NL + ':grey_question: `' + x.email + '` — ' + x.why).join('');
    blocks.push(section(t));
    if (rv.support_message) {
      blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: '*Message for ' + rv.provider + ' support* — copy & send:' }] });
      blocks.push(section('```' + rv.support_message.slice(0, 2800) + '```'));
    }
    if (bad.length && rv.provider === 'Zapmail') {
      const byClient = {};
      bad.forEach(x => { if (x.client) (byClient[x.client] = byClient[x.client] || []).push(x.email); });
      const elements = Object.entries(byClient).slice(0, 4).map(([client, emails]) => ({
        type: 'button', action_id: 'u_retire_' + client.toLowerCase().replace(/[^a-z]/g, ''), style: 'danger',
        text: { type: 'plain_text', text: ('Retire ' + emails.length + ' (' + client + ')').slice(0, 75) },
        value: JSON.stringify({ client, emails: emails.slice(0, 40) }).slice(0, 1990),
        confirm: { title: { type: 'plain_text', text: 'Retire at this bill?' },
          text: { type: 'plain_text', text: ('Zapmail removes ' + emails.length + ' inbox(es) at their next bill and stops charging for them. The domains stay; undo is possible before the bill. ' + emails.join(', ')).slice(0, 295) },
          confirm: { type: 'plain_text', text: 'Retire' }, deny: { type: 'plain_text', text: 'Cancel' } } }));
      if (elements.length) blocks.push({ type: 'actions', elements });
    } else if (bad.length && rv.provider === 'ScaledMail') {
      blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: 'ScaledMail cannot drop single inboxes: per domain use *Look up a domain → Replace domain*, or cancel the whole order.' }] });
    }
  }
  (r.errors || []).forEach(e => blocks.push(section(':warning: ' + e)));
  return blocks.slice(0, MAX_BLOCKS);
}

/** Zapmail inbox subscriptions billing soon (zapmail_status.py --billing). */
function billingBlocks(r) {
  const rows = (r && r.billing) || [];
  const horizon = new Date(Date.now() + (r.days || 14) * 864e5).toISOString().slice(0, 10);
  const soon = rows.filter(x => x.bills_on && x.bills_on <= horizon);
  const blocks = [{ type: 'actions', elements: [btn('Review inboxes before their bill', 'u_nav_review', null, 'primary')] },
    section('*Inbox subscriptions billing in the next ' + (r.days || 14) + ' days* — inboxes are seats in these; '
    + 'the card is charged on the date and every inbox in it renews. To stop paying for some inboxes, look up their domain → *Retire inboxes* (removed at the next bill).')];
  (r.billing_errors || []).forEach(e => blocks.push(section(':warning: could not check ' + e)));
  if (!soon.length) blocks.push(section('_nothing billing soon_'));
  for (const x of soon.slice(0, 20)) {
    const s = section('*' + x.bills_on + '* — ' + money(x.price) + ' · ' + (x.mailboxes || '?') + ' ' + (x.provider === 'MICROSOFT' ? 'Outlook' : 'Google')
      + ' ' + x.kind + ' · ' + x.account + (x.clients && x.clients.length ? ' · ' + x.clients.join(', ') : '')
      + (x.payment_failure ? ' · :x: ' + x.payment_failure : '')
      + (x.invoice_url ? ' · <' + x.invoice_url + '|' + (x.payment_failure ? 'Pay / see invoice' : 'Invoice') + '>' : '')
      + (x.domains && x.domains.length ? NL + x.domains.map(d => '`' + d + '`').join(', ') : ''));
    const doms = (x.domains || []).filter(d => DOMAIN_RE.test(d)).slice(0, 10);
    if (doms.length) {
      s.accessory = { type: 'overflow', action_id: 'u_lookup_menu',
        options: doms.map(d => ({ text: { type: 'plain_text', text: ('Look up ' + d).slice(0, 75) }, value: d })) };
    }
    blocks.push(s);
  }
  return blocks;
}

/** Run every provider call of a view in parallel and merge into one message. */
async function runView(sub, args, baseDir) {
  const plan = planFor(sub, args);
  if (!plan) return { text: HELP };
  const { runPy } = require('./domain_suggest_command');
  const results = await Promise.all(plan.map(p => p.cli
    ? runPy(p.cli, baseDir, (sub === 'sync' || sub === 'audit' ? 6 : 3) * 60 * 1000).then(r => ({ p, r }), err => ({ p, err }))
    : Promise.resolve({ p, err: new Error('bad request') })));
  const title = { status: 'Fleet & cost', fleet: 'Fleet & cost', renewals: 'Renewals & billing', billing: 'Renewals & billing',
    domain: 'Domain ' + (args[0] || ''), lookup: 'Domain ' + (args[0] || ''), purchases: 'Purchases', prewarmed: 'Pre-warmed',
    sync: 'Tracker sync', digest: "Today's digest", 'cross-check': 'Tracker vs Zapmail', quote: 'Quote', find: 'Domain ideas',
    audit: 'Infra audit', review: 'Renewal review', kill: 'Kill warnings' }[sub] || sub;
  const blocks = [];
  let shown = 0;
  for (const { p, r, err } of results) {
    if ((sub === 'domain' || sub === 'lookup') && r && !err && notFound(p.provider, r)) continue;
    const part = err ? { text: ':x: ' + require('./slack_text').plainError(err.message) } : p.render(r);
    const partBlocks = (part.blocks && part.blocks.length ? part.blocks : textBlocks(part.text)).slice(0, 22);
    if (results.length > 1) blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: '*' + p.provider + '*' }] });
    blocks.push(...partBlocks, { type: 'divider' });
    shown++;
  }
  if ((sub === 'domain' || sub === 'lookup') && !shown) {
    return { text: '`' + args[0] + '` is not on Zapmail or ScaledMail.', blocks: [section('`' + args[0] + '` is not on Zapmail or ScaledMail (checked both).')] };
  }
  if (blocks.length && blocks[blocks.length - 1].type === 'divider') blocks.pop();
  return { text: title, blocks: blocks.slice(0, MAX_BLOCKS) };
}

function notFound(provider, r) {
  if (provider === 'Zapmail') return !(r.hits || []).some(h => !h.error);
  if (provider === 'ScaledMail') return !r.found && !r.inventory;
  return false;
}

async function runUnified(sub, args, baseDir, respond) {
  if (!planFor(sub, args)) return respond({ response_type: 'ephemeral', replace_original: false, text: HELP });
  await respond({ response_type: 'ephemeral', replace_original: false, text: ':hourglass: Checking Zapmail and ScaledMail…' });
  try {
    const out = await runView(sub, args, baseDir);
    return respond({ response_type: 'ephemeral', replace_original: false, ...out });
  } catch (err) {
    console.error('[domains] unified view failed:', err);
    return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: ' + require('./slack_text').plainError(err.message) });
  }
}

/** `/domains <word> …` → handled here when the word is a unified view. */
function routeText(text) {
  const tokens = String(text || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
  if (!tokens.length) return { home: true };
  const sub = tokens[0] === 'crosscheck' ? 'cross-check' : tokens[0];
  if (SUBS.includes(sub)) return { sub, args: tokens.slice(1) };
  return null;
}

function lookupModal(channel) {
  return { type: 'modal', callback_id: 'u_lookup_submit', private_metadata: channel || '',
    title: { type: 'plain_text', text: 'Look up a domain' }, submit: { type: 'plain_text', text: 'Look up' },
    blocks: [{ type: 'input', block_id: 'domain', label: { type: 'plain_text', text: 'Domain (Zapmail or ScaledMail)' },
      element: { type: 'plain_text_input', action_id: 'value', placeholder: { type: 'plain_text', text: 'askbettrdata.com' } } }] };
}

function registerUnified(app, baseDir) {
  const say = async (client, channel, user, payload) => {
    if (channel) { try { return await client.chat.postEphemeral({ channel, user, ...payload }); } catch (e) { /* DM */ } }
    return client.chat.postMessage({ channel: user, ...payload });
  };
  app.action(/^u_nav_[a-z-]+$/, async ({ ack, action, respond }) => {
    await ack();
    return runUnified(String(action.action_id).replace(/^u_nav_/, ''), [], baseDir, respond);
  });
  // One-click retire of flagged Zapmail inboxes (approver-only; removed at the next bill).
  app.action(/^u_retire_[a-z]+$/, async ({ ack, body, action, respond }) => {
    await ack();
    const { isApprover } = require('./zapmail_actions');
    if (!isApprover(body.user && body.user.id)) {
      return respond({ response_type: 'ephemeral', replace_original: false, text: ':lock: Only Zapmail approvers can retire inboxes (ZAPMAIL_APPROVERS).' });
    }
    let p = {};
    try { p = JSON.parse(action.value); } catch (e) { return; }
    const EMAIL = /^[a-z0-9][a-z0-9._-]{0,63}@[a-z0-9-]+(\.[a-z0-9-]+)+$/;
    const emails = (p.emails || []).filter(e => EMAIL.test(e));
    if (!emails.length || !['Bettrdata', 'Melior', 'Precise Leads'].includes(p.client)) return;
    await respond({ response_type: 'ephemeral', replace_original: false, text: ':hourglass: Retiring ' + emails.length + ' inbox(es) for ' + p.client + '…' });
    const r = await require('./domain_suggest_command').runPy(['zapmail_inboxes.py', '--retire', emails.join(','), '--client', p.client, '--approve', '--json'], baseDir, 3 * 60 * 1000)
      .catch(err => ({ error: err.message }));
    await respond({ response_type: 'ephemeral', replace_original: false, text: r && r.ok
      ? ':white_check_mark: <@' + body.user.id + '> retired ' + (r.inboxes || emails).length + ' inbox(es) for ' + p.client + ' — removed at their next bill. ' + (r.detail || '')
      : ':x: Not retired: ' + require('./slack_text').plainError((r && r.error) || 'no result') });
  });

  app.action('u_lookup_open', async ({ ack, body, client }) => {
    await ack();
    await client.views.open({ trigger_id: body.trigger_id, view: lookupModal(body.channel && body.channel.id) });
  });
  app.action('u_lookup_menu', async ({ ack, action, respond }) => {
    await ack();
    const d = String((action.selected_option || {}).value || '');
    if (DOMAIN_RE.test(d)) return runUnified('domain', [d], baseDir, respond);
  });
  app.view('u_lookup_submit', async ({ ack, body, view, client }) => {
    const domain = String(view.state.values.domain.value.value || '').trim().toLowerCase()
      .replace(/^https?:\/\//, '').replace(/\/.*$/, '');
    if (!DOMAIN_RE.test(domain)) return ack({ response_action: 'errors', errors: { domain: 'Enter a domain like example.com' } });
    await ack();
    const out = await runView('domain', [domain], baseDir).catch(err => ({ text: ':x: ' + err.message }));
    await say(client, view.private_metadata || null, body.user.id, out);
  });
}

module.exports = { homeBlocks, planFor, runView, runUnified, routeText, registerUnified, billingBlocks, reviewBlocks, HELP, SUBS };
