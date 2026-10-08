/**
 * ScaledMail in Slack — the same shape as the Zapmail menu.
 *
 *   /domains scaledmail   (or /domains sm, or /scaledmail where registered)
 *
 * Everyone allowed on the domains app: every view (fleet, renewals, orders,
 * domain look-up, placement reports, pre-warmed, quote, find domains,
 * purchase plans, tracker preview).
 *
 * Approvers only (SCALEDMAIL_APPROVERS, else ZAPMAIL_APPROVERS), each behind
 * a form or a confirm dialog, and re-checked by Python:
 *   - Set client on an order (order tag)                 free
 *   - Rename senders (same addresses, no re-warm)         free, ScaledMail applies it
 *   - Change redirect                                     free, ScaledMail applies it
 *   - Apply the tracker sync                              free
 *   - Stage an order                                      free (prices it, records a plan)
 *   - Place order                                         CHARGES THE CARD — also needs
 *                                                         SCALEDMAIL_ALLOW_SPEND=true
 *   - Cancel order                                        stops every mailbox — also needs
 *                                                         SCALEDMAIL_ALLOW_CANCEL=true
 * Python: smartlead_sync/scaledmail_cli.py. Docs: docs/SCALEDMAIL_INTEGRATION.md.
 */

const NL = String.fromCharCode(10);
const DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;
const ORDER_RE = /^rec[A-Za-z0-9]{8,20}$/;
const PLAN_RE = /^[0-9a-f]{6}$/;
const NAMES_RE = /^[A-Za-z][A-Za-z .'-]{0,40}(,\s*[A-Za-z][A-Za-z .'-]{0,40}){0,24}$/;
const KEYWORD_RE = /^[a-z0-9-]{2,40}$/;
const CLIENTS = ['Bettrdata', 'Melior', 'Precise Leads'];   // Belardi Wong: past client since 2026-10-08
const PROVIDERS = ['google', 'outlook', 'smtp'];
const PNAME = { google: 'Google', outlook: 'Outlook', smtp: 'SMTP' };

const HELP = [
  '*ScaledMail* — `/domains scaledmail` (menu) or `/domains sm <view>`:',
  '`status` · `renewals` · `orders` · `domain x.com` · `reports` · `prewarmed` · `plans` · `sync` · `digest`',
  '`quote 30000 google,outlook 70,30 low` — domains, mailboxes and price for a monthly volume',
  '`find keyword` — available domain names with prices',
  '',
  'Views are open to the domains team. Changes are buttons for approvers (`SCALEDMAIL_APPROVERS`), each asking first.',
  'Placing an order charges the card and also needs `SCALEDMAIL_ALLOW_SPEND=true` on the server; cancelling needs `SCALEDMAIL_ALLOW_CANCEL=true`.'
].join(NL);

function approvers() {
  return (process.env.SCALEDMAIL_APPROVERS || process.env.ZAPMAIL_APPROVERS || process.env.ZAPMAIL_BUY_APPROVERS || '')
    .split(',').map(s => s.trim()).filter(Boolean);
}
function isApprover(u) { return approvers().includes(u); }
function pack(o) { return JSON.stringify(o); }
function unpack(v) { try { return JSON.parse(v); } catch (e) { return {}; } }
function money(v) { const n = Number(v); return Number.isFinite(n) ? '$' + n.toFixed(2) : '$?'; }
function desc(s) { return String(s || '').replace(/\*/g, '×'); }
function plain(e) { return require('./slack_text').plainError(e); }
function section(text) { return { type: 'section', text: { type: 'mrkdwn', text: String(text).slice(0, 2900) } }; }
function context(text) { return { type: 'context', elements: [{ type: 'mrkdwn', text: String(text).slice(0, 2900) }] }; }
function btn(text, action_id, value, style, confirm) {
  const b = { type: 'button', action_id, text: { type: 'plain_text', text }, value: value || 'x' };
  if (style) b.style = style;
  if (confirm) b.confirm = confirm;
  return b;
}
function confirmBox(title, text, yes) {
  return { title: { type: 'plain_text', text: title }, text: { type: 'plain_text', text: text.slice(0, 300) },
    confirm: { type: 'plain_text', text: yes }, deny: { type: 'plain_text', text: 'Cancel' } };
}
const opt = (text, value) => ({ text: { type: 'plain_text', text }, value });

/** Typed sub-command → CLI args (all read-only). null = unknown. */
function commandFor(sub, args) {
  const a = args || [];
  switch (sub) {
    case 'status': case 'renewals': case 'orders': case 'reports': case 'prewarmed':
    case 'plans': case 'sync': case 'digest': case 'packages':
      return ['scaledmail_cli.py', '--json', sub];
    case 'domain':
      return DOMAIN_RE.test(a[0] || '') ? ['scaledmail_cli.py', '--json', 'domain', a[0]] : null;
    case 'find':
      return KEYWORD_RE.test(a[0] || '') ? ['scaledmail_cli.py', '--json', 'suggest', a[0]] : null;
    case 'quote': {
      const vol = parseInt(a[0], 10);
      const provs = String(a[1] || 'google').split(',').filter(p => PROVIDERS.includes(p));
      const split = String(a[2] || '');
      const tier = ['low', 'medium', 'max'].includes(a[3]) ? a[3] : 'low';
      if (!(vol > 0) || !provs.length || (split && !/^\d{1,3}(,\d{1,3}){0,2}$/.test(split))) return null;
      const out = ['scaledmail_cli.py', '--json', 'quote', String(vol), '--providers', provs.join(','), '--tier', tier];
      if (split) out.push('--split', split);
      return out;
    }
    default: return null;
  }
}

/* ------------------------------------------------------------------ views */

function homeBlocks() {
  return [
    section('*ScaledMail* — Google, Outlook and SMTP mailboxes (monthly orders). Views are read-only; changes are approver-only and ask first.'),
    { type: 'actions', block_id: 'sm_home_1', elements: [
      btn('Fleet & cost', 'sm_nav_status', 'status'),
      btn('Renewals & billing', 'sm_nav_renewals', 'renewals'),
      btn('Orders', 'sm_nav_orders', 'orders'),
      btn('Look up a domain', 'sm_lookup_open', 'lookup'),
      btn("Today's digest", 'sm_nav_digest', 'digest')
    ] },
    { type: 'actions', block_id: 'sm_home_2', elements: [
      btn('Price a volume', 'sm_quote_open', 'quote'),
      btn('Find domains', 'sm_find_open', 'find'),
      btn('Order mailboxes', 'sm_order_open', '{}', 'primary'),
      btn('Purchase plans', 'sm_nav_plans', 'plans')
    ] },
    { type: 'actions', block_id: 'sm_home_3', elements: [
      btn('Placement reports', 'sm_nav_reports', 'reports'),
      btn('Pre-warmed', 'sm_nav_prewarmed', 'prewarmed'),
      btn('Tracker sync', 'sm_nav_sync', 'sync')
    ] },
    context('Typed: `/domains sm status | renewals | orders | domain x.com | quote 30000 google,outlook 70,30 low | find keyword | plans | sync`')
  ];
}

function statusBlocks(r) {
  const orders = (r.orders || []).filter(o => o.status === 'Active');
  let t = '*ScaledMail — ' + money(r.monthly_total) + '/month* · ' + (r.active_orders || 0) + ' active order(s) · '
    + (r.domain_count || 0) + ' domains' + NL;
  const bp = r.by_provider || {};
  t += Object.keys(bp).map(p => PNAME[p] + ' ' + bp[p].domains + ' domains / ' + bp[p].mailboxes + ' mailboxes'
    + (bp[p].in_progress ? ' (' + bp[p].in_progress + ' being set up)' : '')).join(' · ') + NL;
  const bc = r.by_client || {};
  t += 'By client: ' + Object.keys(bc).map(c => (c === 'unassigned' ? ':warning: unassigned' : c) + ' ' + bc[c].mailboxes).join(' · ') + NL + NL;
  t += orders.map(o => '• ' + desc(o.description) + ' — ' + money(o.amount) + '/mo · ' + o.mailboxes + ' mailboxes · '
    + (o.clients || []).join(', ') + ' · bills ' + (o.billing_day || '?')).join(NL);
  if ((r.inventory || []).length) {
    t += NL + NL + 'Unused registered domains: ' + r.inventory.map(i => '`' + i.domain + '`').join(', ');
  }
  return [section(t), context('Prices: Google $3.50/mailbox, Outlook $50/domain of 25, SMTP $3.75/domain of 4. Mailboxes bill monthly on the order date.')];
}

function renewalsBlocks(r) {
  const b = (r.billing || []).map(o => '• *' + o.billing_day + '* — ' + desc(o.description) + ' ' + money(o.amount) + ' · ' + (o.clients || []).join(', '));
  const d = (r.domains || []).map(x => '• *' + x.renewal_at + '* — `' + x.domain + '` ' + money(x.price) + ' · ' + (x.client || (x.provider === 'unused' ? 'unused' : 'unassigned')));
  return [section('*Order billing — next ' + (r.billing_days || 14) + ' days*' + NL + (b.join(NL) || '_none_')
    + NL + NL + '*Domain renewals — next ' + (r.domain_days || 60) + ' days*' + NL + (d.join(NL) || '_none_')),
    context('To stop paying for an order, an approver uses *Cancel order* on it (Orders) — every mailbox in that order stops.')];
}

function orderMenu(o) {
  const options = [opt('Set client', 'client|' + o.id)];
  if (o.status === 'Active') options.push(opt('Cancel order…', 'cancel|' + o.id));
  return { type: 'overflow', action_id: 'sm_order_menu', options };
}

function ordersBlocks(r) {
  const blocks = [section('*ScaledMail orders*')];
  for (const o of (r.orders || []).slice(0, 30)) {
    const s = section((o.status === 'Active' ? ':large_green_circle: ' : ':white_circle: ') + '*' + desc(o.description) + '* — '
      + money(o.amount) + '/mo · ' + o.status + ' · ' + o.domains + ' domains / ' + o.mailboxes + ' mailboxes · '
      + ((o.clients || []).join(', ') || '—') + (o.billing_day ? ' · bills ' + o.billing_day : '') + NL + '`' + o.id + '`');
    if (ORDER_RE.test(o.id)) s.accessory = orderMenu(o);
    blocks.push(s);
  }
  return blocks;
}

function domainBlocks(r) {
  if (!r || r.error) return [section(':x: ' + plain(r && r.error))];
  if (!r.found) {
    return [section('`' + r.domain + '` is not on ScaledMail' + (r.inventory ? ' as a live domain — it is an *unused registration* (renews ' + r.inventory.renewal_at + '). It can replace a burnt domain (`/domains sm` → domain → Replace).' : '.'))];
  }
  const d = r.row;
  const boxes = d.mailbox_rows || [];
  let t = '*`' + d.domain + '`* — ' + (d.client || ':warning: no client') + ' · ' + PNAME[d.provider] + ' · ' + d.status
    + ' · ' + d.mailboxes + ' mailboxes' + NL
    + 'Order: ' + desc(d.order || '?') + ' (' + d.order_status + (d.billing_day ? ', bills ' + d.billing_day : '') + ')'
    + ' · domain renews ' + (d.renewal_at || '?') + (d.renewal_price ? ' at ' + money(d.renewal_price) : '')
    + ' · redirect ' + (d.redirect || '_none_') + (d.masking ? ' · masked → ' + (d.masking_target || '?') : '') + NL;
  t += boxes.slice(0, 30).map(b => '• ' + b.email + ' — ' + (b.status || '?') + (b.name ? ' · ' + b.name : '')).join(NL);
  if (boxes.length > 30) t += NL + '…and ' + (boxes.length - 30) + ' more';
  const base = { domain: d.domain, order_id: d.order_id, client: d.client || '', boxes: boxes.length };
  const elements = [];
  if (ORDER_RE.test(d.order_id || '')) elements.push(btn('Set client', 'sm_client_open', pack(base)));
  if (boxes.length) elements.push(btn('Rename senders', 'sm_senders_open', pack(base)));
  elements.push(btn('Change redirect', 'sm_redirect_open', pack({ ...base, current: d.redirect || '' })));
  elements.push(btn('Replace domain', 'sm_swap_open', pack(base)));
  if (ORDER_RE.test(d.order_id || '') && d.order_status === 'Active') {
    elements.push(btn('Cancel its order', 'sm_cancel', pack({ order_id: d.order_id, label: d.order }), 'danger',
      confirmBox('Cancel the whole order?', 'This cancels "' + desc(d.order) + '" — EVERY mailbox in that order stops, not just ' + d.domain + '. ScaledMail cannot cancel one domain.', 'Cancel order')));
  }
  return [section(t), { type: 'actions', elements }];
}

function reportsText(r) {
  if (!r.has_reports) return '*ScaledMail placement reports* — none. Their weekly test is a paid add-on and is not switched on; our own Smartlead tests (Mon/Tue) cover these inboxes.';
  return '*ScaledMail placement — week of ' + r.weekof + '*' + NL + 'Average inbox ' + r.avg_inbox + '% · reputation ' + r.avg_reputation
    + ' · ' + r.domains + ' domains' + NL
    + ((r.flagged || []).map(f => '• `' + f.domain + '` ' + f.score + '%' + (f.blacklisted.length ? ' · listed on ' + f.blacklisted.join(', ') : '')).join(NL) || '_no domain under 80% or blacklisted_');
}

function prewarmedText(r) {
  const g = r.google || []; const o = r.outlook || [];
  if (!g.length && !o.length) return '*ScaledMail pre-warmed* — none in stock right now.';
  const line = x => '• `' + x.domain + '` — ' + x.emailMailboxCount + ' mailboxes, ' + x.warmup_age + ' month(s) warm · '
    + money((x.pricing || {}).oneTimePrice) + ' once + ' + money((x.pricing || {}).monthlyPrice) + '/mo';
  return '*ScaledMail pre-warmed*' + NL + '*Google*' + NL + (g.map(line).join(NL) || '_none_') + NL + '*Outlook*' + NL + (o.map(line).join(NL) || '_none_')
    + NL + '_Buying pre-warmed is CLI-only for now (it charges the card)._';
}

function quoteText(r) {
  if (r.error) return ':x: ' + plain(r.error);
  const q = r.quote || {}; const rq = r.request || {};
  const rows = (q.providerBreakdown || []).map(p => '• ' + PNAME[p.provider] + ' ' + p.percentage + '% — ' + p.requiredDomains + ' domains, '
    + p.requiredMailboxes + ' mailboxes, ' + money(p.totalPrice) + '/mo (≈' + Math.round((q.dailyVolume || 0) * p.percentage / 100 / Math.max(1, p.requiredMailboxes)) + '/day per mailbox)');
  return '*Quote — ' + (q.monthlyVolume || rq.volume) + ' emails/month (' + (q.dailyVolume || '?') + '/day), ' + rq.tier + ' sending*' + NL
    + rows.join(NL) + NL + '*Total ' + money(q.totalPrice) + '/month* · ' + q.totalDomains + ' domains · ' + q.totalMailboxes + ' mailboxes'
    + NL + '_Domain registration (~$15.50 first year, ~$17 renewal per .com) is extra. Nothing was ordered._';
}

function findBlocks(r) {
  if (r.error) return [section(':x: ' + plain(r.error))];
  const av = (r.available || []).slice(0, 25);
  if (!av.length) return [section('No available names for `' + r.keyword + '`.')];
  const t = '*Available for `' + r.keyword + '`* (first year · renewal)' + NL
    + av.map(d => '• `' + d.domain + '` ' + money(d.price) + ' · ' + money(d.renewPrice)).join(NL);
  return [section(t), { type: 'actions', elements: [
    btn('Order mailboxes on some of these', 'sm_order_open', pack({ domains: av.slice(0, 5).map(d => d.domain) }), 'primary')] },
  context('The order form opens with the first 5 filled in — edit the list. Staging only prices it; nothing is bought until an approver presses *Place order*.')];
}

function stagedBlocks(r) {
  if (r.error) return [section(':x: ' + plain(r.error))];
  let t = (r.staged ? ':memo: *Staged* plan `' + r.plan_id + '`' : ':x: *Not staged* — ' + r.why) + NL
    + r.client + ' · ' + PNAME[r.provider] + ' · ' + r.domains.length + ' domains × ' + r.mailboxes_per_domain + ' = ' + r.mailboxes + ' mailboxes'
    + NL + '*' + money(r.monthly_usd) + '/month* + domains ' + money(r.domains_usd) + ' (first year)'
    + NL + 'Senders: ' + (r.senders || []).join(', ') + NL + 'Domains: ' + r.domains.map(d => '`' + d + '`').join(', ');
  ['taken', 'blacklisted', 'over_ceiling'].forEach(k => { if ((r[k] || []).length) t += NL + ':x: ' + k.replace('_', ' ') + ': ' + r[k].join(', '); });
  const blocks = [section(t)];
  if (r.staged) blocks.push({ type: 'actions', elements: [placeButton(r.plan_id, r.monthly_usd, r.domains_usd, r.client)] });
  return blocks;
}

function placeButton(planId, monthly, domains, client) {
  return btn('Place order (charges card)', 'sm_place', planId, 'danger',
    confirmBox('Place this ScaledMail order?', 'Charges the saved card now: ' + money(monthly) + '/month + ' + money(domains)
      + ' for domains, for ' + client + '. Availability is re-checked first. The server must allow spending.', 'Place order'));
}

function plansBlocks(r) {
  if (r.error) return [section(':x: ' + plain(r.error))];
  const plans = (r.plans || []).slice(0, 20);
  if (!plans.length) return [section('*Purchase plans* — none. Use *Order mailboxes* to stage one.')];
  const icon = { planned: ':memo:', placed: ':white_check_mark:', failed: ':x:', unknown: ':warning:', in_progress: ':hourglass:' };
  const blocks = [section('*ScaledMail purchase plans*')];
  for (const p of plans) {
    const s = section((icon[p.status] || '•') + ' `' + p.plan_id + '` ' + p.status + ' · ' + p.client + ' · ' + PNAME[p.provider] + ' · '
      + p.mailboxes + ' mailboxes · ' + money(p.monthly_usd) + '/mo · ' + (p.domains || []).join(', ')
      + (p.error ? NL + '_' + String(p.error).slice(0, 200) + '_' : '') + NL + '_staged ' + String(p.staged_at || '').slice(0, 10) + '_');
    if ((p.status === 'planned' || p.status === 'failed') && PLAN_RE.test(p.plan_id)) {
      s.accessory = placeButton(p.plan_id, p.monthly_usd, p.domains_usd, p.client);
    } else if ((p.status === 'unknown' || p.status === 'in_progress') && PLAN_RE.test(p.plan_id)) {
      s.accessory = btn('Reconcile', 'sm_reconcile', p.plan_id);
    }
    blocks.push(s);
  }
  return blocks;
}

function syncBlocks(r) {
  if (!r.ok) return [section(':x: ' + ((r.errors || []).join('; ') || plain(r.error)))];
  const c = r.counts || {};
  const ops = (r.ops || []).slice(0, 25).map(o => '• ' + o.action + ' ' + o.type.toLowerCase() + ' `' + o.name + '` — ' + o.reason);
  const why = {};
  (r.skipped || []).forEach(s => { why[s.why] = (why[s.why] || 0) + 1; });
  const t = '*ScaledMail → /infra tracker (preview)*' + NL
    + 'domains: ' + (c.domain_insert || 0) + ' new, ' + (c.domain_update || 0) + ' updated · inboxes: ' + (c.inbox_insert || 0) + ' new, ' + (c.inbox_update || 0) + ' updated'
    + NL + (ops.join(NL) || '_nothing to change_') + ((r.ops || []).length > 25 ? NL + '…and ' + (r.ops.length - 25) + ' more' : '')
    + (Object.keys(why).length ? NL + NL + 'Skipped: ' + Object.entries(why).map(([k, v]) => v + ' — ' + k).join('; ') : '');
  const blocks = [section(t)];
  if ((r.ops || []).length) blocks.push({ type: 'actions', elements: [btn('Apply to tracker', 'sm_sync_apply', 'apply', 'primary',
    confirmBox('Write these tracker changes?', 'Expiry dates and missing fields only; team fields are never overwritten and nothing is deleted.', 'Apply'))] });
  blocks.push(context('Runs daily at 9:35 IST (writes only when SCALEDMAIL_ASSET_SYNC_ENABLED=true).'));
  return blocks;
}

function render(sub, r) {
  if (r && r.error && sub !== 'domain') return { text: ':x: ' + plain(r.error) };
  switch (sub) {
    case 'status': return { text: 'ScaledMail fleet', blocks: statusBlocks(r) };
    case 'renewals': return { text: 'ScaledMail renewals', blocks: renewalsBlocks(r) };
    case 'orders': return { text: 'ScaledMail orders', blocks: ordersBlocks(r) };
    case 'domain': return { text: 'ScaledMail: ' + (r.domain || ''), blocks: domainBlocks(r) };
    case 'reports': return { text: reportsText(r) };
    case 'prewarmed': return { text: prewarmedText(r) };
    case 'plans': return { text: 'ScaledMail purchase plans', blocks: plansBlocks(r) };
    case 'sync': return { text: 'ScaledMail tracker sync', blocks: syncBlocks(r) };
    case 'digest': return { text: r.text || '*ScaledMail today* — nothing to act on. (' + money(r.monthly_total) + '/month)' };
    case 'quote': return { text: quoteText(r) };
    case 'find': return { text: 'Domain ideas', blocks: findBlocks(r) };
    case 'packages': return { text: '*ScaledMail packages*' + NL + (r.packages || []).map(p => '• ' + p.tier + ' · ' + p.name + ' — ' + money(p.price) + p.frequency + ' · ' + p.domains + ' domains').join(NL) };
    default: return { text: HELP };
  }
}

/* ------------------------------------------------------------------ forms */

function modal(callback_id, title, submit, meta, blocks) {
  return { type: 'modal', callback_id, private_metadata: pack(meta), title: { type: 'plain_text', text: title },
    submit: { type: 'plain_text', text: submit }, close: { type: 'plain_text', text: 'Cancel' }, blocks };
}
function input(block_id, label, element, optional, hint) {
  const b = { type: 'input', block_id, label: { type: 'plain_text', text: label }, element: { action_id: 'value', ...element } };
  if (optional) b.optional = true;
  if (hint) b.hint = { type: 'plain_text', text: hint };
  return b;
}
const text = (initial, placeholder, multiline) => {
  const e = { type: 'plain_text_input' };
  if (initial) e.initial_value = initial;
  if (placeholder) e.placeholder = { type: 'plain_text', text: placeholder };
  if (multiline) e.multiline = true;
  return e;
};
const select = (options, initial) => {
  const e = { type: 'static_select', options };
  if (initial) e.initial_option = initial;
  return e;
};
const clientOptions = () => CLIENTS.map(c => opt(c, c));

function lookupModal(channel) {
  return modal('sm_lookup_submit', 'Look up a domain', 'Look up', { channel }, [input('domain', 'Domain', text('', 'meliorbuild.com'))]);
}
function quoteModal(channel) {
  return modal('sm_quote_submit', 'Price a volume', 'Price it', { channel }, [
    input('volume', 'Emails per month', text('30000')),
    input('providers', 'Providers', { type: 'checkboxes', options: PROVIDERS.map(p => opt(PNAME[p], p)), initial_options: [opt('Google', 'google')] }),
    input('split', 'Split % (same order as ticked, e.g. 70,30)', text('', '70,30'), true, 'Leave empty for one provider.'),
    input('tier', 'How hard each mailbox sends', select([opt('Low (safest)', 'low'), opt('Medium', 'medium'), opt('Max', 'max')], opt('Low (safest)', 'low')))
  ]);
}
function findModal(channel) {
  return modal('sm_find_submit', 'Find domains', 'Search', { channel }, [input('kw', 'Keyword', text('', 'bettrdata'), false, 'One word: letters, numbers, dashes.')]);
}
function orderModal(channel, p) {
  return modal('sm_order_submit', 'Order mailboxes', 'Stage (no charge)', { channel }, [
    section('Staging prices the order and records a plan. *Nothing is bought* until an approver presses *Place order*.'),
    input('client', 'Client', select(clientOptions(), CLIENTS.includes(p.client) ? opt(p.client, p.client) : undefined)),
    input('provider', 'Mailbox type', select([opt('Google — $3.50/mailbox', 'google'), opt('Outlook — $50/domain of 25', 'outlook'), opt('SMTP — $3.75/domain of 4', 'smtp')], opt('Google — $3.50/mailbox', 'google'))),
    input('per', 'Google mailboxes per domain', select([opt('2', '2'), opt('3', '3'), opt('4', '4')], opt('3', '3')), true, 'Ignored for Outlook (25) and SMTP (4).'),
    input('domains', 'New domains (ScaledMail registers them)', text((p.domains || []).join(', '), 'a.com, b.com', true), false, 'Comma-separated; use Find domains to pick available ones.'),
    input('senders', 'Sender names', text('', 'Jane Doe, John Roe'), false, 'Real senders, comma-separated. Spread across the domains.'),
    input('redirect', 'Redirect to (optional)', text('', 'https://client.com'), true)
  ]);
}
function clientModal(channel, p) {
  return modal('sm_client_submit', 'Set client', 'Save', { channel, order_id: p.order_id, domain: p.domain }, [
    section('Sets the order tag. Every domain in this order then counts as this client (fleet view, tracker sync, digest).'),
    input('client', 'Client', select(clientOptions(), CLIENTS.includes(p.client) ? opt(p.client, p.client) : undefined))]);
}
function sendersModal(channel, p) {
  return modal('sm_senders_submit', 'Rename senders', 'Request', { channel, domain: p.domain }, [
    section('`' + p.domain + '` — ' + p.boxes + ' mailboxes. Only the name people see changes; *addresses stay*, so no re-warm. ScaledMail reviews and applies it (usually same day).'),
    input('names', 'New sender names', text('', 'Jane Doe, John Roe'), false, 'Comma-separated, at most one per mailbox; spread evenly.')]);
}
function redirectModal(channel, p) {
  return modal('sm_redirect_submit', 'Change redirect', 'Request', { channel, domain: p.domain }, [
    section('`' + p.domain + '` now redirects to ' + (p.current || '_nothing_') + '. ScaledMail reviews and applies the change.'),
    input('url', 'New redirect (empty = remove)', text('', 'https://client.com'), true)]);
}
function swapModal(channel, p) {
  return modal('sm_swap_submit', 'Replace domain', 'Request', { channel, domain: p.domain }, [
    section('Replace `' + p.domain + '` (e.g. burnt / in spam) with an *unused domain we already bought on ScaledMail*. Mailboxes move to the new domain and need warming again. ScaledMail reviews and applies it.'),
    input('new', 'Replacement domain', text('', 'preciseleadshq.com'), false, 'Must be in the "Unused registered domains" list (Fleet & cost).')]);
}

/* --------------------------------------------------------------- handlers */

async function say(client, channel, user, payload) {
  if (channel) {
    try { return await client.chat.postEphemeral({ channel, user, ...payload }); } catch (e) { /* DM instead */ }
  }
  return client.chat.postMessage({ channel: user, ...payload });
}

function registerScaledMailActions(app, baseDir, { onTrackerChanged } = {}) {
  const { runPy } = require('./domain_suggest_command');
  const py = (args, ms) => runPy(args, baseDir, ms || 2 * 60 * 1000);
  const DENY = ':lock: Only ScaledMail approvers can do this (SCALEDMAIL_APPROVERS). Ask one, or use `scaledmail_cli.py`.';
  const eph = (respond, payload) => respond({ response_type: 'ephemeral', replace_original: false, ...payload });

  async function guard(body, respond) {
    const u = body.user && body.user.id;
    if (isApprover(u)) return u;
    await eph(respond, { text: DENY });
    return null;
  }
  const chan = body => (body.channel && body.channel.id) || '';

  // From domain suggestions: ticked names → the ScaledMail order form, pre-filled.
  app.action('domains_sm_order', async ({ ack, body, action, client, respond }) => {
    await ack();
    const picked = require('./domain_suggest_command').selectedDomains((body.state || {}).values);
    if (!picked.length) return eph(respond, { text: 'Tick at least one domain first, then press *Order on ScaledMail*.' });
    const clientName = { bettrdata: 'Bettrdata', melior: 'Melior', precise_leads: 'Precise Leads' }[
      String(action.value || '').toLowerCase().replace(/\s+/g, '_')] || String(action.value || '');
    await client.views.open({ trigger_id: body.trigger_id, view: orderModal(chan(body), { domains: picked.slice(0, 30), client: clientName }) });
  });

  app.action('sm_home', async ({ ack, respond }) => {
    await ack();
    return eph(respond, { text: 'ScaledMail', blocks: homeBlocks() });
  });

  app.action(/^sm_nav_[a-z]+$/, async ({ ack, action, respond }) => {
    await ack();
    const sub = String(action.action_id).replace(/^sm_nav_/, '');
    return runSub(sub, [], baseDir, respond);
  });

  const opener = (id, build, needsApprover) => app.action(id, async ({ ack, body, action, client, respond }) => {
    await ack();
    if (needsApprover && !(await guard(body, respond))) return;
    const p = unpack(action.value);
    if (p.domain && !DOMAIN_RE.test(p.domain)) return;
    await client.views.open({ trigger_id: body.trigger_id, view: build(chan(body), p) });
  });
  opener('sm_lookup_open', c => lookupModal(c));
  opener('sm_quote_open', c => quoteModal(c));
  opener('sm_find_open', c => findModal(c));
  opener('sm_order_open', (c, p) => orderModal(c, p));
  opener('sm_client_open', (c, p) => clientModal(c, p), true);
  opener('sm_senders_open', (c, p) => sendersModal(c, p), true);
  opener('sm_redirect_open', (c, p) => redirectModal(c, p), true);
  opener('sm_swap_open', (c, p) => swapModal(c, p), true);

  const val = (v, b) => (v[b] && v[b].value) || {};

  app.view('sm_lookup_submit', async ({ ack, body, view, client }) => {
    const domain = String(val(view.state.values, 'domain').value || '').trim().toLowerCase().replace(/^https?:\/\//, '').replace(/\/.*$/, '');
    if (!DOMAIN_RE.test(domain)) return ack({ response_action: 'errors', errors: { domain: 'Enter a domain like example.com' } });
    await ack();
    const meta = unpack(view.private_metadata);
    const r = await py(['scaledmail_cli.py', '--json', 'domain', domain]).catch(e => ({ error: e.message, domain }));
    await say(client, meta.channel, body.user.id, { text: 'ScaledMail: ' + domain, blocks: domainBlocks(r) });
  });

  app.view('sm_quote_submit', async ({ ack, body, view, client }) => {
    const v = view.state.values;
    const vol = parseInt(String(val(v, 'volume').value || '').replace(/[, ]/g, ''), 10);
    const provs = (val(v, 'providers').selected_options || []).map(o => o.value).filter(p => PROVIDERS.includes(p));
    const split = String(val(v, 'split').value || '').replace(/\s/g, '');
    const tier = ((val(v, 'tier').selected_option) || {}).value || 'low';
    const errors = {};
    if (!(vol >= 500)) errors.volume = 'A number of emails per month, at least 500';
    if (!provs.length) errors.providers = 'Tick at least one';
    if (provs.length > 1) {
      const parts = split.split(',').map(Number);
      if (parts.length !== provs.length || parts.some(n => !Number.isInteger(n)) || parts.reduce((a, b) => a + b, 0) !== 100) {
        errors.split = 'One % per ticked provider, adding up to 100';
      }
    }
    if (Object.keys(errors).length) return ack({ response_action: 'errors', errors });
    await ack();
    const meta = unpack(view.private_metadata);
    const args = commandFor('quote', [String(vol), provs.join(','), provs.length > 1 ? split : '', tier]);
    const r = await py(args).catch(e => ({ error: e.message }));
    await say(client, meta.channel, body.user.id, { text: quoteText(r) });
  });

  app.view('sm_find_submit', async ({ ack, body, view, client }) => {
    const kw = String(val(view.state.values, 'kw').value || '').trim().toLowerCase();
    if (!KEYWORD_RE.test(kw)) return ack({ response_action: 'errors', errors: { kw: 'One word: letters, numbers, dashes' } });
    await ack();
    const meta = unpack(view.private_metadata);
    const r = await py(['scaledmail_cli.py', '--json', 'suggest', kw]).catch(e => ({ error: e.message }));
    await say(client, meta.channel, body.user.id, { text: 'Domain ideas', blocks: findBlocks(r) });
  });

  // Stage: anyone on the team may price an order; only approvers may place it.
  app.view('sm_order_submit', async ({ ack, body, view, client }) => {
    const v = view.state.values;
    const clientName = ((val(v, 'client').selected_option) || {}).value;
    const provider = ((val(v, 'provider').selected_option) || {}).value;
    const per = ((val(v, 'per').selected_option) || {}).value || '3';
    const domains = String(val(v, 'domains').value || '').toLowerCase().split(/[\s,]+/).filter(Boolean);
    const senders = String(val(v, 'senders').value || '').trim();
    const redirect = String(val(v, 'redirect').value || '').trim();
    const errors = {};
    if (!CLIENTS.includes(clientName)) errors.client = 'Pick a client';
    if (!PROVIDERS.includes(provider)) errors.provider = 'Pick a type';
    if (!domains.length || domains.length > 30 || domains.some(d => !DOMAIN_RE.test(d))) errors.domains = '1-30 domains like a.com, b.com';
    if (!NAMES_RE.test(senders) || !senders.split(',').every(n => n.trim().split(/\s+/).length >= 2)) errors.senders = 'First and last name each: "Jane Doe, John Roe"';
    if (redirect && !/^(https?:\/\/)?[a-z0-9.-]+\.[a-z]{2,}(\/[^\s]*)?$/i.test(redirect)) errors.redirect = 'A web address like https://client.com';
    if (Object.keys(errors).length) return ack({ response_action: 'errors', errors });
    await ack();
    const meta = unpack(view.private_metadata);
    const user = body.user.id;
    const args = ['scaledmail_cli.py', '--json', 'stage', '--client', clientName, '--provider', provider,
      '--domains', domains.join(','), '--senders', senders, '--user', user];
    if (provider === 'google') args.push('--per-domain', per);
    if (redirect) args.push('--redirect', redirect);
    await say(client, meta.channel, user, { text: ':hourglass: Checking availability and pricing…' });
    const r = await py(args).catch(e => ({ error: e.message }));
    await say(client, meta.channel, user, { text: 'ScaledMail order plan', blocks: stagedBlocks(r) });
  });

  app.action('sm_place', async ({ ack, body, action, respond }) => {
    await ack();
    const user = await guard(body, respond);
    if (!user || !PLAN_RE.test(String(action.value || ''))) return;
    await eph(respond, { text: ':hourglass: Placing ScaledMail order `' + action.value + '`…' });
    const r = await py(['scaledmail_cli.py', '--json', 'place', action.value, '--approve', '--user', user], 3 * 60 * 1000)
      .catch(e => ({ error: e.message }));
    const t = r.error ? ':x: Not ordered. ' + plain(r.error)
      : r.status === 'placed' ? ':white_check_mark: Ordered (plan `' + r.plan_id + '`, tag `' + r.tag + '`). Domains appear as *In Progress* on ScaledMail, then Active; the 9:35 sync adds them to the tracker.'
        : r.status === 'unknown' ? ':warning: ScaledMail did not answer clearly — it *may* have charged. Press *Reconcile* in Purchase plans before anything else. ' + plain(r.error)
          : ':x: ' + r.status + ': ' + plain(r.error);
    await eph(respond, { text: t });
  });

  app.action('sm_reconcile', async ({ ack, body, action, respond }) => {
    await ack();
    if (!(await guard(body, respond)) || !PLAN_RE.test(String(action.value || ''))) return;
    const r = await py(['scaledmail_cli.py', '--json', 'reconcile', action.value]).catch(e => ({ error: e.message }));
    await eph(respond, { text: r.error ? ':x: ' + plain(r.error) : 'Plan `' + action.value + '`: *' + r.status + '*' + (r.note ? ' — ' + r.note : '') + (r.order_id ? ' (order `' + r.order_id + '`)' : '') });
  });

  app.action('sm_order_menu', async ({ ack, body, action, client, respond }) => {
    await ack();
    const [verb, id] = String((action.selected_option || {}).value || '').split('|');
    if (!ORDER_RE.test(id || '') || !(await guard(body, respond))) return;
    if (verb === 'client') return client.views.open({ trigger_id: body.trigger_id, view: clientModal(chan(body), { order_id: id }) });
    if (verb === 'cancel') {
      return eph(respond, { text: 'Cancel order `' + id + '`?', blocks: [
        section(':warning: Cancelling order `' + id + '` stops *every mailbox in it*. This cannot be undone from here.'),
        { type: 'actions', elements: [btn('Yes, cancel the order', 'sm_cancel', pack({ order_id: id }), 'danger',
          confirmBox('Really cancel?', 'Every mailbox in order ' + id + ' stops.', 'Cancel order'))] }] });
    }
  });

  app.action('sm_cancel', async ({ ack, body, action, respond }) => {
    await ack();
    const user = await guard(body, respond);
    const p = unpack(action.value);
    if (!user || !ORDER_RE.test(p.order_id || '')) return;
    const r = await py(['scaledmail_cli.py', '--json', 'cancel', p.order_id, '--approve']).catch(e => ({ error: e.message }));
    await eph(respond, { text: r.error ? ':x: Not cancelled. ' + plain(r.error) : ':no_entry_sign: Order `' + p.order_id + '` cancelled by <@' + user + '>. Run *Tracker sync* to mark its inboxes inactive.' });
  });

  const write = (id, build) => app.view(id, async ({ ack, body, view, client }) => {
    const meta = unpack(view.private_metadata);
    const res = build(view.state.values, meta);
    if (res.errors) return ack({ response_action: 'errors', errors: res.errors });
    await ack();
    const user = body.user.id;
    if (!isApprover(user)) return say(client, meta.channel, user, { text: DENY });
    const r = await py(res.args).catch(e => ({ error: e.message }));
    await say(client, meta.channel, user, { text: r.error ? ':x: Not done. ' + plain(r.error) : ':white_check_mark: ' + res.done });
  });

  write('sm_client_submit', (v, m) => {
    const c = ((val(v, 'client').selected_option) || {}).value;
    if (!CLIENTS.includes(c) || !ORDER_RE.test(m.order_id || '')) return { errors: { client: 'Pick a client' } };
    return { args: ['scaledmail_cli.py', '--json', 'tag', m.order_id, c, '--approve'],
      done: 'Order `' + m.order_id + '` now belongs to *' + c + '*. The next tracker sync adds its live domains.' };
  });
  write('sm_senders_submit', (v, m) => {
    const names = String(val(v, 'names').value || '').trim();
    if (!NAMES_RE.test(names) || !names.split(',').every(n => n.trim().split(/\s+/).length >= 2)) return { errors: { names: 'First and last name each: "Jane Doe, John Roe"' } };
    if (!DOMAIN_RE.test(m.domain || '')) return { errors: { names: 'bad domain' } };
    return { args: ['scaledmail_cli.py', '--json', 'senders', m.domain, names, '--approve'],
      done: 'Requested new sender names on `' + m.domain + '`: ' + names + '. ScaledMail applies it; update Smartlead names to match afterwards.' };
  });
  write('sm_redirect_submit', (v, m) => {
    const url = String(val(v, 'url').value || '').trim();
    if (url && !/^(https?:\/\/)?[a-z0-9.-]+\.[a-z]{2,}(\/[^\s]*)?$/i.test(url)) return { errors: { url: 'A web address like https://client.com' } };
    if (!DOMAIN_RE.test(m.domain || '')) return { errors: { url: 'bad domain' } };
    return { args: ['scaledmail_cli.py', '--json', 'redirect', m.domain, url, '--approve'],
      done: 'Requested redirect for `' + m.domain + '` → ' + (url || '_none_') + '.' };
  });
  write('sm_swap_submit', (v, m) => {
    const nd = String(val(v, 'new').value || '').trim().toLowerCase();
    if (!DOMAIN_RE.test(nd) || !DOMAIN_RE.test(m.domain || '') || nd === m.domain) return { errors: { new: 'A different domain like newname.com' } };
    return { args: ['scaledmail_cli.py', '--json', 'swap', m.domain, nd, '--approve'],
      done: 'Requested replacing `' + m.domain + '` with `' + nd + '`. Re-add the new inboxes to Smartlead and warm them once ScaledMail finishes.' };
  });

  app.action('sm_sync_apply', async ({ ack, body, respond }) => {
    await ack();
    if (!(await guard(body, respond))) return;
    const r = await py(['scaledmail_cli.py', '--json', 'sync', '--apply'], 5 * 60 * 1000).catch(e => ({ error: e.message }));
    await eph(respond, { text: r.error ? ':x: ' + plain(r.error) : ':white_check_mark: Tracker updated: ' + (r.applied || 0) + ' row(s) written.' });
    if (r && r.applied && typeof onTrackerChanged === 'function') onTrackerChanged();
  });
}

async function runSub(sub, args, baseDir, respond) {
  const cli = commandFor(sub, args);
  if (!cli) return respond({ response_type: 'ephemeral', replace_original: false, text: HELP });
  await respond({ response_type: 'ephemeral', replace_original: false, text: 'Checking ScaledMail (`' + sub + '`)…' });
  try {
    const r = await require('./domain_suggest_command').runPy(cli, baseDir, (sub === 'sync' ? 5 : 2) * 60 * 1000);
    return respond({ response_type: 'ephemeral', replace_original: false, ...render(sub, r) });
  } catch (err) {
    console.error('[scaledmail] failed:', err);
    return respond({ response_type: 'ephemeral', replace_original: false, text: ':x: ' + plain(err.message) });
  }
}

/** Typed text after `/domains scaledmail` or `/scaledmail`. */
function handleScaledMailText(text, baseDir, respond) {
  const tokens = String(text || '').trim().toLowerCase().split(/\s+/).filter(Boolean);
  const sub = tokens[0];
  if (!sub || sub === 'menu' || sub === 'home') {
    return respond({ response_type: 'ephemeral', text: 'ScaledMail', blocks: homeBlocks() });
  }
  if (sub === 'help') return respond({ response_type: 'ephemeral', text: HELP });
  return runSub(sub, tokens.slice(1), baseDir, respond);
}

function registerScaledMailCommand(app, baseDir, opts = {}) {
  const name = process.env.SCALEDMAIL_SLASH_COMMAND;   // only when someone registered it in Slack
  if (name) {
    app.command(name, async ({ ack, command, respond }) => {
      await ack();
      return handleScaledMailText(command.text, baseDir, respond);
    });
  }
  registerScaledMailActions(app, baseDir, opts);
}

module.exports = {
  registerScaledMailCommand, handleScaledMailText, commandFor, render, homeBlocks, statusBlocks,
  renewalsBlocks, ordersBlocks, domainBlocks, quoteText, findBlocks, stagedBlocks, plansBlocks,
  syncBlocks, reportsText, prewarmedText, orderModal, quoteModal, approvers, isApprover, HELP
};
