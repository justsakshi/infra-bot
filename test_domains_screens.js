// Phase 2 of the Slack redesign: /domains screens show a short headline and
// put long lists behind "Show …" buttons that replace the screen in place.
// Run: node test_domains_screens.js   (Python stubbed; no Slack, no network)
const assert = require('assert');

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.stack || err.message); process.exitCode = 1; }
}
const all = blocks => JSON.stringify(blocks);
const buttons = blocks => blocks.flatMap(b => (b.type === 'actions' ? b.elements : (b.accessory ? [b.accessory] : [])));
const ids = blocks => buttons(blocks).map(e => e.action_id);

// ── live-shaped fixtures ────────────────────────────────────────────────
const SM_STATUS = { monthly_total: 662.75, active_orders: 8, domain_count: 43,
  by_provider: { google: { domains: 30, mailboxes: 90, in_progress: 0 }, outlook: { domains: 5, mailboxes: 125, in_progress: 3 },
    smtp: { domains: 8, mailboxes: 32, in_progress: 0 } },
  by_client: { Melior: { mailboxes: 80 }, 'Precise Leads': { mailboxes: 39 }, unassigned: { mailboxes: 75 } },
  orders: Array.from({ length: 8 }, (_, i) => ({ id: 'recLP7iFrjZ27pq9' + i, description: (i + 3) + ' × ScaledMail - MS', amount: 50 + i,
    status: 'Active', billing_day: '2026-11-0' + (i + 1), domains: 3, mailboxes: 75, clients: ['Melior'] })),
  inventory: [{ domain: 'preciseleadshq.com', renewal_at: '2027-07-08' }] };

const PAST = Array.from({ length: 34 }, (_, i) => ({ domain: 'pastclient' + i + '.com', domain_id: 'p' + i, client: null, expire_on: '2026-11-1' + (i % 9) }));
const ZM_RENEWALS = { renewal_errors: [], renewals: [
  { domain: 'gomelior.com', domain_id: 'd1', client: 'Melior', account: 'PRECISE_LEADS', expire_on: '2026-10-29', assigned_mailboxes: 3 },
  ...PAST] };

const today = new Date().toISOString().slice(0, 10);
const daysAgo = n => new Date(Date.now() - n * 864e5).toISOString();
const PLANS = { plans: [
  { plan_id: 'aaaaaa', status: 'planned', client: 'Melior', provider: 'smtp', mailboxes: 8, monthly_usd: 7.5, domains: ['a.com'], staged_at: daysAgo(1) },
  { plan_id: 'bbbbbb', status: 'unknown', client: 'Melior', provider: 'smtp', mailboxes: 8, monthly_usd: 7.5, domains: ['b.com'], staged_at: daysAgo(20) },
  { plan_id: 'cccccc', status: 'placed', client: 'Melior', provider: 'smtp', mailboxes: 8, monthly_usd: 7.5, domains: ['c.com'], staged_at: daysAgo(30) },
  { plan_id: 'dddddd', status: 'dropped', client: 'Melior', provider: 'google', mailboxes: 3, monthly_usd: 10.5, domains: ['olddropped.com'], staged_at: daysAgo(12) },
  { plan_id: 'eeeeee', status: 'failed', client: 'Melior', provider: 'google', mailboxes: 3, monthly_usd: 10.5, domains: ['oldfailed.com'], staged_at: daysAgo(9) },
  { plan_id: 'ffffff', status: 'failed', client: 'Melior', provider: 'google', mailboxes: 3, monthly_usd: 10.5, domains: ['newfailed.com'], staged_at: daysAgo(2) }] };

const REVIEW = { days: 14, errors: [], reviews: [
  { provider: 'Zapmail', label: '15 Outlook inboxes', bills_on: '2026-10-20', price: 48.75, counts: { KEEP: 1, RETIRE: 2, CHECK: 1 }, retire_saves_monthly: 6.5,
    support_message: 'Hi, please remove these inboxes.',
    rows: [{ email: 'a@x.com', verdict: 'RETIRE', why: '47%', client: 'Bettrdata', campaigns: ['C'] },
      { email: 'b@y.com', verdict: 'RETIRE', why: 'spam domain', client: 'Melior', campaigns: [] },
      { email: 'q@y.com', verdict: 'CHECK', why: 'no recent test', client: 'Melior' },
      { email: 'c@z.com', verdict: 'KEEP', why: '100%', client: 'Bettrdata' }] },
  { provider: 'ScaledMail', label: '39 × Google', bills_on: '2026-10-22', price: 136.5, counts: { KEEP: 0, RETIRE: 1, CHECK: 0 }, retire_saves_monthly: 3.5,
    rows: [{ email: 'd@w.com', verdict: 'RETIRE', why: '60%', client: 'Precise Leads' }] }] };

// Stub the Python runner before anything loads it.
const flow = require('./domain_suggest_command');
const calls = [];
flow.runPy = async (args) => {
  const a = args.join(' ');
  calls.push(a);
  if (a.includes('scaledmail_cli.py --json status')) return SM_STATUS;
  if (a.includes('scaledmail_cli.py --json plans')) return PLANS;
  if (a.includes('scaledmail_cli.py --json renewals')) return { billing: [], domains: [], billing_days: 14, domain_days: 60 };
  if (a.includes('zapmail_status.py --renewals')) return ZM_RENEWALS;
  if (a.includes('renewal_review.py')) return REVIEW;
  return { error: 'unexpected ' + a };
};

const u = require('./domains_unified');
const sm = require('./scaledmail_command');
const za = require('./zapmail_actions');
const zc = require('./zapmail_command');

// Fake Bolt app: collects action handlers by id (string or regex).
const handlers = [];
u.registerUnified({ action: (id, fn) => handlers.push([id, fn]), view: () => {}, command: () => {} }, __dirname);
function handlerFor(actionId) {
  const h = handlers.find(([id]) => (id instanceof RegExp ? id.test(actionId) : id === actionId));
  return h && h[1];
}
async function click(actionId, value) {
  const out = [];
  const fn = handlerFor(actionId);
  assert.ok(fn, 'no handler registered for ' + actionId);
  await fn({ ack: async () => {}, body: { user: { id: 'U1' } }, action: { action_id: actionId, value }, respond: async m => { out.push(m); } });
  return out;
}
function navOf(msg) {
  const nav = msg.blocks[msg.blocks.length - 1];
  assert.strictEqual(nav.block_id, 'nav_row', 'last block is the nav row');
  return nav.elements.map(e => e.action_id);
}

(async () => {
  // ── Home ──
  await test('home: grouped Buy · Check · Tracker, one short help line, no boilerplate', () => {
    const home = u.homeBlocks(__dirname);
    const t = all(home);
    assert.ok(!/read-only/.test(t) && !/approver-only/.test(t), 'no read-only / approver sentence');
    assert.ok(/\*Buy\*/.test(t) && /\*Check\*/.test(t) && /\*Tracker\*/.test(t), 'three groups');
    const ctx = home.filter(b => b.type === 'context');
    assert.strictEqual(ctx.length, 1, 'one help line');
    assert.ok(/\/domains help/.test(all(ctx)) && all(ctx).length < 200, 'short "/domains help" hint');
    const i = ids(home);
    ['u_nav_status', 'u_nav_renewals', 'u_nav_review', 'u_lookup_open', 'u_nav_purchases', 'sm_order_open', 'infra_expiring_7', 'u_nav_kill']
      .forEach(x => assert.ok(i.includes(x), 'home keeps ' + x));
    assert.ok(i.some(x => /^domains_suggest_/.test(x)), 'suggest buttons');
  });

  // ── Fleet status ──
  await test('status: headline + per-provider lines, orders behind Show orders', () => {
    const b = sm.statusBlocks(SM_STATUS);
    const t = all(b);
    assert.ok(/\$662\.75\/mo · 8 orders · 43 domains/.test(t), 'headline');
    assert.ok(/Google 30 domains/.test(t) && /Outlook 5 domains/.test(t) && /SMTP 8 domains/.test(t), 'one line per provider');
    assert.ok(!/ScaledMail - MS/.test(t), 'no order list on the first screen');
    assert.ok(!/Prices:/.test(t), 'no price sentence on the first screen');
    assert.ok(ids(b).includes('u_show_orders'), 'Show orders button');
  });
  await test('Show orders replaces in place with the list, prices once, ← Fleet & cost', async () => {
    const out = await click('u_show_orders', 'x');
    const last = out[out.length - 1];
    assert.ok(out.every(m => m.replace_original === true), 'replaces');
    const t = all(last.blocks);
    assert.strictEqual((t.match(/ScaledMail - MS/g) || []).length, 8, 'every active order listed');
    assert.strictEqual((t.match(/Prices:/g) || []).length, 1, 'prices said once');
    assert.ok(/preciseleadshq\.com/.test(t), 'unused domains in the list');
    assert.deepStrictEqual(navOf(last), ['nav_home', 'u_nav_status', 'nav_close']);
  });
  await test('zapmail status: headline totals then one line per account', () => {
    const t = zc.formatStatus({ PRECISE_LEADS: { ok: true, wallet_balance: 20, auto_recharge: true, domains: { GOOGLE: 10, MICROSOFT: 2 },
      active_mailboxes: { GOOGLE: 30, MICROSOFT: 50 }, placement_credits: 5 }, BETTRDATA: { ok: false, error: 'boom' } });
    const lines = t.split('\n');
    assert.ok(/12 domains · 80 mailboxes/.test(lines[0]), 'headline totals: ' + lines[0]);
    assert.ok(!/read-only/.test(t));
    assert.strictEqual(lines.length, 3, 'headline + one line per account');
  });

  // ── Renewals ──
  await test('zapmail renewals: past clients as one count line + Show', () => {
    const b = za.renewalBlocks(ZM_RENEWALS);
    const t = all(b);
    assert.ok(!/pastclient7\.com/.test(t), 'no past-client names inline');
    assert.ok(/34 past-client domain/.test(t), 'count line');
    const show = buttons(b).find(e => e.action_id === 'u_show_past');
    assert.ok(show && show.value.length <= 2000, 'Show button with a short value');
    assert.ok(/gomelior\.com/.test(t), 'current clients still listed with their menu');
  });
  await test('Show past-client domains replaces in place with all 34, ← Renewals', async () => {
    const out = await click('u_show_past', 'zm');
    const last = out[out.length - 1];
    const t = all(last.blocks);
    assert.ok(/pastclient0\.com/.test(t) && /pastclient33\.com/.test(t), 'full list');
    assert.ok(!/gomelior/.test(t), 'only past clients');
    assert.deepStrictEqual(navOf(last), ['nav_home', 'u_nav_renewals', 'nav_close']);
  });
  await test('billing: headline says what bills in 14 days', () => {
    const b = u.billingBlocks({ days: 14, billing_errors: [], billing: [
      { bills_on: today, price: 48.75, mailboxes: 15, provider: 'MICROSOFT', kind: 'inboxes', account: 'PL', clients: ['Bettrdata'],
        domains: Array.from({ length: 15 }, (_, i) => 'd' + i + '.com') },
      { bills_on: today, price: 21, mailboxes: 6, provider: 'GOOGLE', kind: 'inboxes', account: 'PL', clients: ['Melior'], domains: ['m.com'] }] });
    const t = all(b);
    assert.ok(/\$69\.75 bills in the next 14 days/.test(t), 'headline sum');
    const texts = b.map(x => (x.text ? x.text.text : '')).join('\n');
    assert.ok(!/d14\.com/.test(texts), 'domains live in the Look up menu, not inline');
    assert.ok(!/inboxes are seats/.test(t), 'long explanation gone');
  });

  // ── Before the bill ──
  await test('review: one line per bill with Show inboxes, no emails on first screen', () => {
    const b = u.reviewBlocks(REVIEW);
    const t = all(b);
    assert.ok(/\$10\/month/.test(t), 'total saving in the headline');
    assert.ok(!/@/.test(t.replace(/"value":"[^"]*"/g, '')), 'no inbox address on the first screen');
    assert.ok(!/RETIRE = /.test(t), 'definition not on the first screen');
    const shows = buttons(b).filter(e => e.action_id === 'u_show_bill');
    assert.strictEqual(shows.length, 2, 'one Show inboxes per bill');
    assert.ok(shows.every(s => s.value.length <= 2000));
    assert.ok(/2 retire/.test(t) && /1 check/.test(t) && /saves \$6\.50/.test(t), 'counts + saving per bill');
  });
  await test('Show inboxes replaces in place: definition once, emails, retire buttons, ← Before the bill', async () => {
    const show = buttons(u.reviewBlocks(REVIEW)).find(e => e.action_id === 'u_show_bill');
    const out = await click('u_show_bill', show.value);
    const last = out[out.length - 1];
    const t = all(last.blocks);
    assert.ok(/a@x\.com/.test(t) && /q@y\.com/.test(t) && !/d@w\.com/.test(t), 'only this bill');
    assert.strictEqual((t.match(/RETIRE = /g) || []).length, 1, 'definition once');
    assert.deepStrictEqual(buttons(last.blocks).filter(e => /^u_retire_/.test(e.action_id)).map(e => e.action_id).sort(),
      ['u_retire_bettrdata', 'u_retire_melior']);
    assert.ok(/please remove/.test(t), 'support message');
    assert.deepStrictEqual(navOf(last), ['nav_home', 'u_nav_review', 'nav_close']);
    assert.ok(calls.some(c => /renewal_review\.py --days 14/.test(c)), 're-ran the review');
  });
  await test('ScaledMail bill detail explains per-domain replace', () => {
    const t = all(u.billBlocks(REVIEW.reviews[1]));
    assert.ok(/Replace domain/.test(t) && /d@w\.com/.test(t));
  });

  // ── Purchase plans ──
  await test('plans: active/placed only, old dropped/failed behind Show old plans', () => {
    const b = sm.plansBlocks(PLANS);
    const t = all(b);
    ['aaaaaa', 'bbbbbb', 'cccccc', 'ffffff'].forEach(p => assert.ok(t.includes(p), p + ' shown'));
    assert.ok(!t.includes('dddddd') && !t.includes('eeeeee'), 'old dropped/failed hidden');
    assert.ok(/2 old plan/.test(t), 'count line');
    assert.ok(ids(b).includes('u_show_old_plans'));
    const placed = b.find(x => x.text && /cccccc/.test(x.text.text));
    assert.ok(!placed.accessory, 'placed has no button');
  });
  await test('Show old plans replaces in place, ← Purchases', async () => {
    const out = await click('u_show_old_plans', 'x');
    const last = out[out.length - 1];
    const t = all(last.blocks);
    assert.ok(t.includes('dddddd') && t.includes('eeeeee') && !t.includes('aaaaaa'));
    assert.deepStrictEqual(navOf(last), ['nav_home', 'u_nav_purchases', 'nav_close']);
  });

  // ── Errors ──
  await test('suggest timeout: human sentence with the retry time', () => {
    const now = new Date('2026-10-13T08:30:00Z');   // 14:00 IST
    const t = flow.suggestErrorText('Precise Leads', new Error('timed out'), now);
    assert.ok(/^Suggestions took over 10 minutes and were stopped\. Usually Zapmail’s search limit \(100 searches per 30 min\)\. Try again after 14:30/.test(t), t);
    const g = flow.suggestErrorText('Precise Leads', new Error('Traceback: KeyError foo'), now);
    assert.ok(/Precise Leads/.test(g) && /[Tt]ry again/.test(g), 'generic error says what to do: ' + g);
  });
  await test('Show handler failure says what happened and what to do', async () => {
    const saved = flow.runPy;
    const savedLog = console.error;
    flow.runPy = async () => { throw new Error('timed out'); };
    console.error = () => {};
    const out = await click('u_show_orders', 'x').finally(() => { console.error = savedLog; flow.runPy = saved; });
    const t = all(out[out.length - 1].blocks);
    assert.ok(/[Tt]ry again/.test(t), t);
  });

  console.log(passed + ' passed');
})();
