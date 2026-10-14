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

/** The one menu: Buy · Check · Tracker, and one line of help. */
function homeBlocks(baseDir) {
  const blocks = [section('*Domains & inboxes* — Zapmail and ScaledMail together.')];
  blocks.push(section('*Buy* — name ideas for a client (2-4 min, both providers’ prices), or order mailboxes'));
  try {
    const flow = require('./domain_suggest_command');
    const picker = flow.clientPickerBlocks(flow.loadProfiles(baseDir || __dirname)).filter(b => b.type === 'actions');
    picker.slice(0, 2).forEach(b => blocks.push(b));
  } catch (e) { /* profiles missing: Order mailboxes below still works */ }
  blocks.push({ type: 'actions', block_id: 'u_home_buy', elements: [
    btn('Order mailboxes', 'sm_order_open', '{}'), btn('Price a volume', 'sm_quote_open'),
    btn('Pre-warmed', 'u_nav_prewarmed'), btn('Purchases', 'u_nav_purchases')] });
  blocks.push(section('*Check*'));
  blocks.push({ type: 'actions', block_id: 'u_home_check', elements: [
    btn('Renewals & billing', 'u_nav_renewals', null, 'primary'), btn('Before the bill', 'u_nav_review'),
    btn('Fleet & cost', 'u_nav_status'), btn('Look up a domain', 'u_lookup_open')] });
  // Second row: Slack folds a row of more than 5 buttons into "+ N more".
  blocks.push({ type: 'actions', block_id: 'u_home_check_2', elements: [
    btn('Kill warnings', 'u_nav_kill'), btn('Infra audit', 'dh_audit'), btn("Today's digest", 'u_nav_digest')] });
  blocks.push(section('*Tracker*'));
  blocks.push({ type: 'actions', block_id: 'u_home_tracker', elements: [
    btn('Expiring today', 'infra_expiring_0'), btn('Next 7 days', 'infra_expiring_7'),
    btn('Add asset', 'infra_add_open'), btn('Renew asset', 'infra_renew_open'), btn('List all', 'infra_list'),
    btn('Tracker sync', 'u_nav_sync')] });
  blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: 'Type `/domains help` for the typed commands.' }] });
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
      return DOMAIN_RE.test(a[0] || '') ? [z('domain', a[0]), s('domain', [a[0]]), {
        // Renewals / failed payments the billing watch has recorded for this domain.
        provider: 'Bill history', sub: 'history', cli: ['billing_watch.py', '--history', a[0], '--json'],
        render: r => ({ text: 'Bill history', blocks: [section('*Bill history*' + NL + (r.lines || []).join(NL))] }) }] : null;
    case 'purchases': return [z('batches'), s('plans')];
    case 'prewarmed': return [z('prewarmed'), s('prewarmed')];
    case 'sync': return [z('sync'), s('sync')];
    case 'digest': return [z('digest'), s('digest')];
    case 'cross-check': return [z('cross-check')];
    case 'quote': return [{ ...s('quote', a), usage: sm.QUOTE_USAGE }];
    case 'find': return [s('find', a)];
    case 'kill': return [{ provider: 'Kill warnings', sub: 'kill', cli: ['kill_report.py', '--json'],
      render: r => ({ text: 'Kill warnings', blocks: require('./domains_home').reportBlocks(r) }) }];
    case 'review': return [{ provider: 'Renewal review', sub: 'review', cli: ['renewal_review.py', '--days', String(parseInt(a[0], 10) || 14), '--json'],
      render: r => ({ text: 'Renewal review', blocks: reviewBlocks(r) }) }];
    case 'audit': return [{ provider: 'Infra audit', sub: 'audit', cli: ['infra_audit.py', '--json'],
      render: r => ({ text: 'Infra audit', blocks: require('./domains_home').reportBlocks(r) }) }];
    default: return null;
  }
}

const REVIEW_KEY_MAX = 80;
/** A short key that finds the same bill again when *Show inboxes* re-runs the review. */
function billKey(rv) { return String(rv.bills_on || '') + '|' + String(rv.provider || '') + '|' + String(rv.label || '').slice(0, REVIEW_KEY_MAX); }
function plural(n, one, many) { return n + ' ' + (n === 1 ? one : (many || one + 's')); }

/** renewal_review.py → one line per bill (counts + saving) with *Show inboxes*. */
function reviewBlocks(r) {
  if (!r || r.error) return [section(':x: Could not run the review: ' + require('./slack_text').plainError(r && r.error) + ' Try again in a few minutes.')];
  const reviews = r.reviews || [];
  const days = r.days || 14;
  const save = reviews.reduce((a, x) => a + (x.retire_saves_monthly || 0), 0);
  const total = reviews.reduce((a, x) => a + (Number(x.price) || 0), 0);
  const blocks = [section(reviews.length
    ? '*Before the bill — ' + plural(reviews.length, 'bill') + ' in the next ' + days + ' days, ' + money(total)
      + '*' + NL + 'Retiring the flagged inboxes saves *' + money(save) + '/month*.'
    : '*Before the bill* — nothing bills in the next ' + days + ' days.')];
  reviews.slice(0, 20).forEach((rv, i) => {
    const c = rv.counts || {};
    blocks.push({ type: 'section',
      text: { type: 'mrkdwn', text: ('*' + rv.bills_on + ' · ' + rv.provider + ' · ' + rv.label + '* — ' + money(rv.price) + NL
        + (c.KEEP || 0) + ' keep · ' + ((c.RETIRE || 0) ? '*' + c.RETIRE + ' retire*' : '0 retire') + ' · ' + (c.CHECK || 0) + ' check'
        + (rv.retire_saves_monthly ? ' · saves ' + money(rv.retire_saves_monthly) + '/mo' : '')).slice(0, 2900) },
      accessory: btn('Show inboxes', 'u_show_bill', JSON.stringify({ d: days, i, k: billKey(rv) }).slice(0, 1990)) });
  });
  (r.errors || []).forEach(e => blocks.push(section(':warning: ' + e)));
  return blocks.slice(0, MAX_BLOCKS);
}

/** *Show inboxes* for one bill: retire / check lists, the definition (once), support message, retire buttons. */
function billBlocks(rv) {
  const c = rv.counts || {};
  const rows = rv.rows || [];
  const bad = rows.filter(x => x.verdict === 'RETIRE');
  const chk = rows.filter(x => x.verdict === 'CHECK');
  let t = '*' + rv.bills_on + ' · ' + rv.provider + ' · ' + rv.label + '* — ' + money(rv.price) + ' · ' + (c.KEEP || 0) + ' keep · *'
    + (c.RETIRE || 0) + ' retire* · ' + (c.CHECK || 0) + ' check' + (rv.note ? NL + '_' + rv.note + '_' : '');
  t += bad.slice(0, 30).map(x => NL + ':x: `' + x.email + '` — ' + x.why + ((x.campaigns || []).length ? ' :warning: in a live campaign' : '')).join('');
  if (bad.length > 30) t += NL + '…and ' + (bad.length - 30) + ' more to retire';
  t += chk.slice(0, 15).map(x => NL + ':grey_question: `' + x.email + '` — ' + x.why).join('');
  if (chk.length > 15) t += NL + '…and ' + (chk.length - 15) + ' more to check';
  const blocks = [section(t),
    { type: 'context', elements: [{ type: 'mrkdwn', text: 'RETIRE = under 80% inbox in its latest test, on a spam domain, or not in Smartlead at all. CHECK = no recent test, broken connection or low warmup reputation.' }] }];
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
  return blocks;
}

/** Zapmail inbox subscriptions billing soon (zapmail_status.py --billing): headline sum, one line per bill. */
function billingBlocks(r) {
  const rows = (r && r.billing) || [];
  const days = (r && r.days) || 14;
  const horizon = new Date(Date.now() + days * 864e5).toISOString().slice(0, 10);
  const soon = rows.filter(x => x.bills_on && x.bills_on <= horizon);
  const total = soon.reduce((a, x) => a + (Number(x.price) || 0), 0);
  const failed = soon.filter(x => x.payment_failure).length;
  const blocks = [section(soon.length
    ? '*' + money(total) + ' bills in the next ' + days + ' days* (' + plural(soon.length, 'inbox subscription') + ')'
      + (failed ? NL + ':x: ' + plural(failed, 'payment') + ' failed, see below.' : '')
    : '*Nothing bills in the next ' + days + ' days* on Zapmail inbox subscriptions.')];
  const paid = r && r.paid;
  if (paid && paid.plans) {
    // Zapmail has no invoice history: plans whose current period started in the last 30 days were billed then.
    blocks.push(section('Zapmail paid in the last ' + paid.days + ' days: *' + money(paid.paid) + '* (already paid, '
      + plural(paid.plans, 'plan') + ')' + (paid.cancelled_since ? ' · ' + money(paid.cancelled_since) + ' of it on plans cancelled since' : '')
      + (paid.unpaid ? NL + ':x: ' + money(paid.unpaid) + ' billed but not paid (' + plural(paid.unpaid_plans, 'plan') + ')' : '')));
  }
  ((r && r.billing_errors) || []).forEach(e => blocks.push(section(':warning: could not check ' + e)));
  for (const x of soon.slice(0, 20)) {
    const doms = (x.domains || []).filter(d => DOMAIN_RE.test(d));
    const s = section('*' + x.bills_on + '* — ' + money(x.price) + ' · ' + (x.mailboxes || '?') + ' ' + (x.provider === 'MICROSOFT' ? 'Outlook' : 'Google')
      + ' ' + x.kind + ' · ' + x.account + (x.clients && x.clients.length ? ' · ' + x.clients.join(', ') : '')
      + (doms.length ? ' · ' + (doms.length === 1 ? '`' + doms[0] + '`' : doms.length + ' domains') : '')
      + (x.payment_failure ? ' · :x: ' + x.payment_failure : '')
      + (x.invoice_url ? ' · <' + x.invoice_url + '|' + (x.payment_failure ? 'Pay / see invoice' : 'Invoice') + '>' : ''));
    if (doms.length) {
      s.accessory = { type: 'overflow', action_id: 'u_lookup_menu',
        options: doms.slice(0, 10).map(d => ({ text: { type: 'plain_text', text: ('Look up ' + d).slice(0, 75) }, value: d })) };
    }
    blocks.push(s);
  }
  if (soon.length) {
    blocks.push({ type: 'actions', elements: [btn('Review inboxes before their bill', 'u_nav_review', null, 'primary')] });
  }
  return blocks;
}

/** *Show* past-client domains left to lapse on Zapmail (zapmail_status.py --renewals). */
function pastZapmailBlocks(r) {
  if (!r || r.error) return [section(':x: Could not load renewals: ' + require('./slack_text').plainError(r && r.error) + ' Try again in a few minutes.')];
  const past = ((r.renewals) || []).filter(x => !x.client).sort((a, b) => (a.expire_on || '').localeCompare(b.expire_on || ''));
  let t = '*Zapmail past-client domains left to lapse* (' + past.length + ', no action)';
  const lines = past.map(x => '• `' + x.domain + '` — ' + (x.expire_on || '?') + (x.account ? ' · ' + x.account : ''));
  const blocks = [];
  for (let i = 0; i < lines.length; i += 40) {
    blocks.push(section((i ? '' : t + NL) + lines.slice(i, i + 40).join(NL)));
  }
  return blocks.length ? blocks.slice(0, 20) : [section(t + NL + '_none_')];
}

/** Run every provider call of a view in parallel and merge into one message. */
async function runView(sub, args, baseDir) {
  const plan = planFor(sub, args);
  if (!plan) return { text: HELP };
  const { runPy } = require('./domain_suggest_command');
  const results = await Promise.all(plan.map(p => p.cli
    ? runPy(p.cli, baseDir, (sub === 'sync' || sub === 'audit' ? 6 : 3) * 60 * 1000).then(r => ({ p, r }), err => ({ p, err }))
    : Promise.resolve({ p, err: new Error('bad request') })));
  const heading = title(sub, args);
  const blocks = [];
  let shown = 0;
  for (const { p, r, err } of results) {
    if ((sub === 'domain' || sub === 'lookup') && r && !err && notFound(p.provider, r)) continue;
    const part = !p.cli && p.usage ? { text: p.usage } : err ? { text: failText(p.provider, err.message) } : p.render(r);
    const partBlocks = (part.blocks && part.blocks.length ? part.blocks : textBlocks(part.text)).slice(0, 22);
    if (results.length > 1) blocks.push({ type: 'context', elements: [{ type: 'mrkdwn', text: '*' + p.provider + '*' }] });
    blocks.push(...partBlocks, { type: 'divider' });
    shown++;
  }
  if ((sub === 'domain' || sub === 'lookup') && !shown) {
    return { text: '`' + args[0] + '` is not on Zapmail or ScaledMail.', blocks: [section('`' + args[0] + '` is not on Zapmail or ScaledMail (checked both).')] };
  }
  if (blocks.length && blocks[blocks.length - 1].type === 'divider') blocks.pop();
  return { text: heading, blocks: blocks.slice(0, MAX_BLOCKS) };
}

function title(sub, args) {
  const a = args || [];
  return { status: 'Fleet & cost', fleet: 'Fleet & cost', renewals: 'Renewals & billing', billing: 'Renewals & billing',
    domain: 'Domain ' + (a[0] || ''), lookup: 'Domain ' + (a[0] || ''), purchases: 'Purchases', prewarmed: 'Pre-warmed',
    sync: 'Tracker sync', digest: "Today's digest", 'cross-check': 'Tracker vs Zapmail', quote: 'Quote', find: 'Domain ideas',
    audit: 'Infra audit', review: 'Before the bill', kill: 'Kill warnings' }[sub] || sub;
}

/** One sentence: what could not be loaded, why, and what to do. */
function failText(what, raw) {
  const plain = require('./slack_text').plainError(raw).replace(/\.?\s*$/, '.');
  return ':x: Could not load ' + what + '. ' + plain + (/try again|ask an admin/i.test(plain) ? '' : ' Try again in a few minutes.');
}

function notFound(provider, r) {
  if (provider === 'Zapmail') return !(r.hits || []).some(h => !h.error);
  if (provider === 'ScaledMail') return !r.found && !r.inventory;
  if (provider === 'Bill history') return !(r.lines || []).length;
  return false;
}

async function runUnified(sub, args, baseDir, respond, { inPlace = false } = {}) {
  // From a menu button the view replaces the menu (⌂ Home to go back); a typed
  // command posts "Checking…" and the result takes its place.
  const { replace, add, swap } = require('./slack_reply');
  const first = p => (inPlace ? replace(respond, p) : add(respond, p));
  const say = p => (inPlace ? replace(respond, p) : swap(respond, p));
  if (!planFor(sub, args)) return first({ text: HELP });
  await first({ text: ':hourglass: Checking Zapmail and ScaledMail…' });
  try {
    const out = await runView(sub, args, baseDir);
    return say(out);
  } catch (err) {
    console.error('[domains] unified view failed:', err);
    return say({ text: failText(title(sub), err.message) });
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
    const p = require('./slack_reply').withClose(payload);
    if (channel) { try { return await client.chat.postEphemeral({ channel, user, ...p }); } catch (e) { /* DM */ } }
    return client.chat.postMessage({ channel: user, ...p });
  };
  app.action(/^u_nav_[a-z-]+$/, async ({ ack, action, respond }) => {
    await ack();
    return runUnified(String(action.action_id).replace(/^u_nav_/, ''), [], baseDir, respond, { inPlace: true });
  });
  // "Show …" buttons: re-run the CLI the screen used and replace it in place,
  // with ← Back to that screen.
  const show = (actionId, cli, render, back) => app.action(actionId, async ({ ack, action, respond }) => {
    await ack();
    const { replace } = require('./slack_reply');
    const val = String((action && action.value) || '');
    const args = cli(val);
    if (!args) return;
    let out;
    try {
      const r = await require('./domain_suggest_command').runPy(args, baseDir, 3 * 60 * 1000);
      out = render(r, val);
    } catch (err) {
      console.error('[domains] ' + actionId + ' failed:', err);
      out = { text: back.label, blocks: [section(failText(back.label.toLowerCase(), err.message))] };
    }
    return replace(respond, out, back);
  });
  const smCli = sub => require('./scaledmail_command').commandFor(sub, []);
  show('u_show_orders', () => smCli('status'),
    r => ({ text: 'ScaledMail orders', blocks: require('./scaledmail_command').ordersListBlocks(r) }),
    { label: 'Fleet & cost', action_id: 'u_nav_status' });
  show('u_show_past', v => (v === 'sm' ? smCli('renewals') : require('./zapmail_command').commandFor('renewals', '')),
    (r, v) => ({ text: 'Past-client domains', blocks: v === 'sm' ? require('./scaledmail_command').pastRenewalsBlocks(r) : pastZapmailBlocks(r) }),
    { label: 'Renewals', action_id: 'u_nav_renewals' });
  show('u_show_old_plans', () => smCli('plans'),
    r => ({ text: 'Old purchase plans', blocks: require('./scaledmail_command').oldPlansBlocks(r) }),
    { label: 'Purchases', action_id: 'u_nav_purchases' });
  show('u_show_bill', v => {
    let p = {};
    try { p = JSON.parse(v); } catch (e) { return null; }
    return ['renewal_review.py', '--days', String(Math.min(60, Math.max(1, parseInt(p.d, 10) || 14))), '--json'];
  }, (r, v) => {
    const p = JSON.parse(v);
    if (!r || r.error) return { text: 'Before the bill', blocks: reviewBlocks(r) };
    const reviews = r.reviews || [];
    const rv = reviews.find(x => billKey(x) === p.k) || null;
    return rv ? { text: 'Inboxes on this bill', blocks: billBlocks(rv) }
      : { text: 'Before the bill', blocks: [section('That bill is no longer in the next ' + (r.days || p.d) + ' days (it may have been paid). Go back for the current list.')] };
  }, { label: 'Before the bill', action_id: 'u_nav_review' });

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

module.exports = { homeBlocks, planFor, runView, runUnified, routeText, registerUnified, billingBlocks, reviewBlocks, billBlocks,
  pastZapmailBlocks, failText, HELP, SUBS };
