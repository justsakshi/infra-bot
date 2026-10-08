// ScaledMail in Slack: routing, typed commands, rendering, approver gates.
// Run: node test_scaledmail_command.js   (no Slack, no Python, no network)
const assert = require('assert');

process.env.SCALEDMAIL_APPROVERS = 'UAPPROVER';
const sm = require('./scaledmail_command');
const { registerDomainsCommand } = require('./domains_command');

let n = 0;
const ok = (cond, msg) => { assert.ok(cond, msg); n++; };

// ── /domains sm … routing ────────────────────────────────────────────────
const commands = {};
registerDomainsCommand({ command: (name, fn) => { commands[name] = fn; }, action: () => {}, view: () => {} }, __dirname);
async function call(text) {
  const out = [];
  await commands['/domains']({ ack: async () => {}, command: { text, user_id: 'U1' }, respond: async m => { out.push(m); } });
  return out;
}

// ── typed commands → CLI args ────────────────────────────────────────────
ok(sm.commandFor('status')[2] === 'status', 'status');
ok(sm.commandFor('domain', ['a.com']).join(' ') === 'scaledmail_cli.py --json domain a.com', 'domain');
ok(sm.commandFor('domain', ['--approve']) === null, 'flag cannot pass as a domain');
ok(sm.commandFor('find', ['bettr data']) === null && sm.commandFor('find', ['bettrdata'])[2] === 'suggest', 'find keyword');
ok(sm.commandFor('quote', ['30000', 'google,outlook', '70,30', 'low']).join(' ')
  === 'scaledmail_cli.py --json quote 30000 --providers google,outlook --tier low --split 70,30', 'quote');
ok(sm.commandFor('quote', ['x']) === null, 'bad volume refused');
ok(sm.commandFor('place', ['abc123']) === null && sm.commandFor('cancel', ['recX']) === null,
  'spend / cancel are never typed commands');

// ── rendering with live-shaped results ───────────────────────────────────
const status = { monthly_total: 491.5, active_orders: 4, domain_count: 28,
  by_provider: { google: { domains: 23, mailboxes: 69, live: 23, in_progress: 0 }, outlook: { domains: 5, mailboxes: 125, live: 2, in_progress: 3 } },
  by_client: { Melior: { domains: 12, mailboxes: 80 }, 'Precise Leads': { domains: 13, mailboxes: 39 }, unassigned: { domains: 3, mailboxes: 75 } },
  orders: [{ id: 'recLP7iFrjZ27pq94', description: '3 × ScaledMail - MS', amount: 150, status: 'Active', billing_day: '2026-11-07', domains: 3, mailboxes: 75, clients: ['unassigned'] }],
  inventory: [{ domain: 'preciseleadshq.com', renewal_at: '2027-07-08' }] };
const st = sm.render('status', status).blocks[0].text.text;
ok(/\$491\.50\/month/.test(st) && /unassigned/.test(st) && /preciseleadshq\.com/.test(st), 'status shows cost, unassigned, inventory');

const dom = { domain: 'agencyforumco.com', found: true, order: { id: 'recLP7iFrjZ27pq94' }, row: {
  domain: 'agencyforumco.com', provider: 'outlook', status: 'In Progress', mailboxes: 25, client: null,
  order: '3 × ScaledMail - MS', order_status: 'Active', order_id: 'recLP7iFrjZ27pq94', billing_day: '2026-11-07',
  renewal_at: '2027-10-07', renewal_price: 17, redirect: '', masking: false,
  mailbox_rows: Array.from({ length: 25 }, (_, i) => ({ email: 'r' + i + '@agencyforumco.com', status: 'In Progress', name: 'Ryan Markman' })) } };
const db = sm.domainBlocks(dom);
const ids = db[1].elements.map(e => e.action_id);
ok(/no client/.test(db[0].text.text) && db[0].text.text.length <= 2900, 'domain text, capped');
ok(ids.includes('sm_client_open') && ids.includes('sm_senders_open') && ids.includes('sm_cancel'), 'domain actions');
const cancel = db[1].elements.find(e => e.action_id === 'sm_cancel');
ok(cancel.style === 'danger' && /EVERY mailbox/.test(cancel.confirm.text.text), 'cancel warns it is the whole order');
ok(/unused registration/.test(sm.domainBlocks({ domain: 'x.com', found: false, inventory: { renewal_at: '2027-07-08' } })[0].text.text), 'inventory domain');

const staged = sm.stagedBlocks({ staged: true, plan_id: 'a1b2c3', client: 'Melior', provider: 'google', domains: ['a.com', 'b.com'],
  mailboxes_per_domain: 3, mailboxes: 6, monthly_usd: 21, domains_usd: 31, senders: ['Jane Doe'], taken: [], blacklisted: [], over_ceiling: [] });
const place = staged[1].elements[0];
ok(place.action_id === 'sm_place' && place.value === 'a1b2c3' && /\$21\.00\/month/.test(place.confirm.text.text), 'place button carries the plan and cost');
ok(sm.stagedBlocks({ staged: false, why: 'taken', client: 'Melior', provider: 'google', domains: ['a.com'], mailboxes_per_domain: 3,
  mailboxes: 3, monthly_usd: 10.5, domains_usd: 0, senders: [], taken: ['a.com'] }).length === 1, 'not staged: no button');

const plans = sm.plansBlocks({ plans: [
  { plan_id: 'aaaaaa', status: 'planned', client: 'Melior', provider: 'smtp', mailboxes: 8, monthly_usd: 7.5, domains: ['a.com'] },
  { plan_id: 'bbbbbb', status: 'unknown', client: 'Melior', provider: 'smtp', mailboxes: 8, monthly_usd: 7.5, domains: ['b.com'] },
  { plan_id: 'cccccc', status: 'placed', client: 'Melior', provider: 'smtp', mailboxes: 8, monthly_usd: 7.5, domains: ['c.com'] }] });
ok(plans[1].accessory.action_id === 'sm_place' && plans[2].accessory.action_id === 'sm_reconcile' && !plans[3].accessory,
  'planned → Place, unknown → Reconcile, placed → nothing');

const q = sm.quoteText({ request: { volume: 30000, tier: 'low' }, quote: { monthlyVolume: 30000, dailyVolume: 1000, totalPrice: 318, totalDomains: 27, totalMailboxes: 123,
  providerBreakdown: [{ provider: 'google', percentage: 70, requiredDomains: 24, requiredMailboxes: 48, totalPrice: 168 },
    { provider: 'outlook', percentage: 30, requiredDomains: 3, requiredMailboxes: 75, totalPrice: 150 }] } });
ok(/\$318\.00\/month/.test(q) && /≈15\/day/.test(q) && /≈4\/day/.test(q) && /Nothing was ordered/.test(q), 'quote text with per-mailbox rate');

ok(/switched off/.test(sm.render('status', { error: 'create_custom_order charges the card and SCALEDMAIL_ALLOW_SPEND is not \'true\'.' }).text),
  'errors become plain sentences');

// ── approver gates on the buttons ────────────────────────────────────────
const actions = {}; const views = {};
sm.registerScaledMailCommand({ command: () => {}, action: (id, fn) => { actions[String(id)] = fn; }, view: (id, fn) => { views[id] = fn; } }, __dirname);

(async () => {
  for (const text of ['sm', 'scaledmail', 'SM', 'scaled']) {
    const out = await call(text);
    ok(out[0] && out[0].text === 'ScaledMail' && Array.isArray(out[0].blocks), 'menu for ' + text);
  }
  ok(/ScaledMail/.test((await call('sm help'))[0].text), 'help');
  ok(/Give a domain|Use|scaledmail/i.test((await call('sm domain not_a_domain'))[0].text), 'bad domain → help');
  const zm = await call('zapmail');
  ok(zm[0].text === 'Zapmail', 'zapmail route untouched');

  for (const id of ['sm_place', 'sm_cancel', 'sm_sync_apply', 'sm_reconcile']) {
    const out = [];
    await actions[id]({ ack: async () => {}, body: { user: { id: 'UNOBODY' } }, action: { value: 'a1b2c3' }, respond: async m => { out.push(m); } });
    ok(out.length === 1 && /Only ScaledMail approvers/.test(out[0].text), id + ' refused for a non-approver');
  }
  // Stage form validation (anyone may stage; bad input never reaches Python).
  let acked = null;
  await views.sm_order_submit({ ack: async r => { acked = r; }, body: { user: { id: 'U1' } }, client: {},
    view: { private_metadata: '{}', state: { values: {
      client: { value: { selected_option: { value: 'Darlean' } } }, provider: { value: { selected_option: { value: 'google' } } },
      per: { value: { selected_option: { value: '3' } } }, domains: { value: { value: 'a.com, --approve' } },
      senders: { value: { value: 'Jane' } }, redirect: { value: { value: '' } } } } } });
  ok(acked && acked.response_action === 'errors' && acked.errors.client && acked.errors.domains && acked.errors.senders,
    'order form rejects unknown client, flag-like domain, single-word sender');
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
