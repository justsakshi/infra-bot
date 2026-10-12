// Guided buy flow: math, views, handlers. Run: node test_buy_flow.js (no Slack, no Python, no network)
const assert = require('assert');
const bf = require('./buy_flow');

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.message); process.exitCode = 1; }
}
const T = (prov, key) => bf.typesFor(prov).find(t => t.key === key);
const picks = n => Array.from({ length: n }, (_, i) => ({ d: 'name' + i + '.com', zp: 12.99, sp: 15.5 }));

(async () => {
  await test('row math per domain matches the agreed table', () => {
    assert.deepStrictEqual(bf.rowMath(T('scaledmail', 'google'), 7), { domains: 7, inboxes: 21, perDay: 420, perMonth: 9240, monthlyUsd: 73.5 });
    assert.deepStrictEqual(bf.rowMath(T('scaledmail', 'outlook'), 2), { domains: 2, inboxes: 50, perDay: 400, perMonth: 8800, monthlyUsd: 100 });
    assert.deepStrictEqual(bf.rowMath(T('scaledmail', 'smtp'), 5), { domains: 5, inboxes: 20, perDay: 160, perMonth: 3520, monthlyUsd: 18.75 });
    assert.deepStrictEqual(bf.rowMath(T('zapmail', 'GOOGLE'), 1), { domains: 1, inboxes: 3, perDay: 60, perMonth: 1320, monthlyUsd: 9.75 });
    assert.deepStrictEqual(bf.rowMath(T('zapmail', 'MICROSOFT'), 1), { domains: 1, inboxes: 3, perDay: 24, perMonth: 528, monthlyUsd: 9.75 });
  });

  await test('usable keeps only names the provider sells', () => {
    const p = [{ d: 'a.com', zp: 12.99, sp: null }, { d: 'b.com', zp: null, sp: 15.5 }];
    assert.deepStrictEqual(bf.usable(p, 'scaledmail').map(x => x.d), ['b.com']);
    assert.deepStrictEqual(bf.usable(p, 'zapmail').map(x => x.d), ['a.com']);
  });

  await test('rebalance keeps the sum and never goes negative', () => {
    let c = bf.initialCounts('scaledmail', 5);
    assert.deepStrictEqual(c, [5, 0, 0]);
    c = bf.rebalance(c, 1, 1); assert.deepStrictEqual(c, [4, 1, 0]);
    c = bf.rebalance(c, 2, 1); assert.deepStrictEqual(c, [4, 0, 1]);   // takes from the last non-empty other row
    c = bf.rebalance(c, 2, -1); assert.deepStrictEqual(c, [5, 0, 0]);  // gives back to the first row
    c = bf.rebalance(c, 0, -1); assert.deepStrictEqual(c, [4, 1, 0]);  // first row gives to the second
    assert.deepStrictEqual(bf.rebalance([0, 0, 0], 1, 1), [0, 0, 0]);
    assert.deepStrictEqual(bf.rebalance([3, 0, 0], 1, -1), [3, 0, 0]);
  });

  await test('split hands out names in tick order', () => {
    assert.deepStrictEqual(bf.split(['a', 'b', 'c', 'd'], [2, 0, 2]), [['a', 'b'], [], ['c', 'd']]);
  });

  await test('totals add rows and domain registrations', () => {
    const t = bf.totals('scaledmail', picks(4), [3, 1, 0]);
    assert.strictEqual(t.domains, 4);
    assert.strictEqual(t.inboxes, 9 + 25);
    assert.strictEqual(t.perDay, 180 + 200);
    assert.strictEqual(t.perMonth, 380 * 22);
    assert.strictEqual(t.monthlyUsd, 31.5 + 50);
    assert.strictEqual(t.domainsUsd, 62);
    assert.strictEqual(t.firstChargeUsd, 143.5);
  });

  await test('ready dates skip weekends, then 21-30 days of warmup', () => {
    assert.strictEqual(bf.addBusinessDays('2026-10-16', 3), '2026-10-21');          // Fri + 3 = Wed
    assert.deepStrictEqual(bf.readyRange('2026-10-12', 'scaledmail'), ['2026-11-05', '2026-11-14']);
    assert.deepStrictEqual(bf.readyRange('2026-10-12', 'zapmail'), ['2026-11-03', '2026-11-12']);
  });

  const meta = (n, extra) => ({ c: 'Precise Leads', ch: 'C1', n: picks(n), ...extra });
  const txt = v => JSON.stringify(v.blocks);

  await test('provider view: a button per provider that sells something, left-out names listed', () => {
    const n = [{ d: 'a.com', zp: 12.99, sp: null }, { d: 'b.com', zp: 12.99, sp: 15.5 }];
    const v = bf.providerView({ c: 'Melior', ch: 'C1', n });
    const btns = v.blocks.filter(b => b.accessory && b.accessory.action_id === 'buy_provider').map(b => b.accessory.value);
    assert.deepStrictEqual(btns, ['scaledmail', 'zapmail']);
    assert.match(txt(v), /left out: a\.com/);
    const none = bf.providerView({ c: 'Melior', ch: 'C1', n: [{ d: 'a.com', zp: 12.99, sp: null }] });
    assert.deepStrictEqual(none.blocks.filter(b => b.accessory && b.accessory.action_id === 'buy_provider').map(b => b.accessory.value), ['zapmail']);
    assert.ok(!v.submit && v.callback_id === 'buy_flow' && v.title.text.length <= 24);
  });

  await test('mix view: a row per type with − / + and the running total', () => {
    const v = bf.mixView(meta(7, { p: 'scaledmail', k: [7, 0, 0] }));
    const s = txt(v);
    assert.match(s, /Google Workspace/);
    assert.match(s, /3 inboxes\/domain · 60\/day · 1,320\/mo · \$10\.50\/mo per domain/);
    assert.match(s, /21 inboxes · ~420\/day · ~9,240\/mo/);
    assert.match(s, /\$73\.50\/mo/);
    assert.match(s, /ready to send around/i);
    const plus = v.blocks.flatMap(b => b.elements || []).filter(e => e.action_id === 'buy_plus');
    assert.strictEqual(plus.length, 3);
  });

  await test('confirm view: senders prefilled, redirect only for ScaledMail, submit Stage', () => {
    const v = bf.confirmView(meta(2, { p: 'scaledmail', k: [2, 0, 0] }), { senders: 'Avinash Haridas, Aravind Haridas', redirect: 'https://preciseleads.in' });
    assert.strictEqual(v.submit.text, 'Stage (no charge)');
    const ids = v.blocks.filter(b => b.type === 'input').map(b => b.block_id);
    assert.deepStrictEqual(ids, ['senders', 'redirect']);
    assert.strictEqual(v.blocks.find(b => b.block_id === 'senders').element.initial_value, 'Avinash Haridas, Aravind Haridas');
    const z = bf.confirmView(meta(2, { p: 'zapmail', k: [2, 0] }), { senders: '', redirect: '' });
    assert.deepStrictEqual(z.blocks.filter(b => b.type === 'input').map(b => b.block_id), ['senders']);
    assert.ok(!('initial_value' in z.blocks.find(b => b.block_id === 'senders').element));
  });

  await test('20 names fit Slack limits', () => {
    const m = meta(20, { p: 'scaledmail', k: [10, 5, 5] });
    for (const v of [bf.providerView(m), bf.mixView(m), bf.confirmView(m, { senders: 'A B', redirect: '' })]) {
      assert.ok(v.blocks.length <= 100, 'blocks ' + v.blocks.length);
      assert.ok(v.private_metadata.length <= 3000, 'metadata ' + v.private_metadata.length);
    }
  });
  await test('stage args: ScaledMail per row, Zapmail inbox job with ; names', () => {
    const m = meta(3, { p: 'scaledmail', k: [2, 1, 0] });
    assert.deepStrictEqual(bf.stageArgs(m, 0, ['a.com', 'b.com'], 'A B, C D', 'https://x.in', 'U1'),
      ['scaledmail_cli.py', '--json', 'stage', '--client', 'Precise Leads', '--provider', 'google', '--domains', 'a.com,b.com',
        '--senders', 'A B, C D', '--user', 'U1', '--per-domain', '3', '--redirect', 'https://x.in']);
    assert.deepStrictEqual(bf.stageArgs(m, 1, ['c.com'], 'A B', '', 'U1').slice(4, 8), ['Precise Leads', '--provider', 'outlook', '--domains']);
    const z = meta(2, { p: 'zapmail', k: [1, 1] });
    assert.deepStrictEqual(bf.stageArgs(z, 1, ['d.com'], 'A B, C D', '', 'U1'),
      ['inbox_jobs.py', '--create', '--client', 'Precise Leads', '--kind', 'new', '--provider', 'MICROSOFT',
        '--domain', 'd.com', '--per-domain', '3', '--names', 'A B;C D', '--by', 'U1', '--json']);
  });

  await test('defaults: senders from inbox profiles, redirect from profile or main domain', () => {
    const profiles = { 'Precise Leads': { main_domain: 'preciseleads.in', redirect: 'https://preciseleads.in' }, Bettrdata: { main_domain: 'bettrdata.io' } };
    const senders = { 'Precise Leads': ['Aravind Haridas', 'Avinash Haridas'], Bettrdata: [] };
    assert.deepStrictEqual(bf.defaultsFor('Precise Leads', profiles, senders), { senders: 'Aravind Haridas, Avinash Haridas', redirect: 'https://preciseleads.in' });
    assert.deepStrictEqual(bf.defaultsFor('Bettrdata', profiles, senders), { senders: '', redirect: 'https://bettrdata.io' });
  });

  // ── handlers with a fake Slack app ──
  function harness(runPy) {
    const h = { actions: {}, views: {}, opened: [], updated: [], said: [] };
    const app = { action: (id, fn) => { h.actions[id] = fn; }, view: (id, fn) => { h.views[id] = fn; } };
    h.client = { views: { open: async a => { h.opened.push(a.view); }, update: async a => { h.updated.push(a.view); } } };
    bf.registerBuyFlow(app, __dirname, {
      runPy: runPy || (async args => ({ staged: true, plan_id: 'abc123', args })),
      say: async (client, channel, user, payload) => { h.said.push(payload); },
      profiles: () => ({ 'Precise Leads': { main_domain: 'preciseleads.in' }, Bettrdata: { main_domain: 'bettrdata.io' } }),
      senders: () => ({ 'Precise Leads': ['Aravind Haridas', 'Avinash Haridas'], Bettrdata: [] })
    });
    return h;
  }
  const noop = async () => {};
  const tickState = { domains_pick_0: { domains_pick: { selected_options: [{ value: 'a.com|12.99|15.5' }, { value: 'b.com|12.99|15.5' }] } } };

  await test('open → provider → plus → next: views redraw with the right state', async () => {
    const h = harness();
    await h.actions.buy_open({ ack: noop, action: { value: 'Precise Leads' }, body: { state: { values: tickState }, trigger_id: 't', channel: { id: 'C1' } }, client: h.client, respond: noop });
    let m = JSON.parse(h.opened[0].private_metadata);
    assert.deepStrictEqual(m.n.map(p => p.d), ['a.com', 'b.com']);
    const view = v => ({ id: 'V1', hash: 'h', private_metadata: v.private_metadata });
    await h.actions.buy_provider({ ack: noop, action: { value: 'scaledmail' }, body: { view: view(h.opened[0]) }, client: h.client });
    m = JSON.parse(h.updated[0].private_metadata);
    assert.deepStrictEqual(m.k, [2, 0, 0]);
    await h.actions.buy_plus({ ack: noop, action: { value: '1' }, body: { view: view(h.updated[0]) }, client: h.client });
    assert.deepStrictEqual(JSON.parse(h.updated[1].private_metadata).k, [1, 1, 0]);
    await h.actions.buy_next({ ack: noop, action: { value: 'x' }, body: { view: view(h.updated[1]) }, client: h.client });
    assert.strictEqual(h.updated[2].submit.text, 'Stage (no charge)');
    assert.strictEqual(h.updated[2].blocks.find(b => b.block_id === 'senders').element.initial_value, 'Aravind Haridas, Avinash Haridas');
  });

  await test('open with nothing ticked says so and opens nothing', async () => {
    const h = harness();
    const said = [];
    await h.actions.buy_open({ ack: noop, action: { value: 'Precise Leads' }, body: { state: { values: {} }, trigger_id: 't' }, client: h.client, respond: async m => said.push(m) });
    assert.strictEqual(h.opened.length, 0);
    assert.match(said[0].text, /Tick at least one/);
  });

  await test('a stale view (hash_conflict) is swallowed', async () => {
    const h = harness();
    h.client.views.update = async () => { const e = new Error('hash_conflict'); e.data = { error: 'hash_conflict' }; throw e; };
    const v = bf.mixView(meta(2, { p: 'scaledmail', k: [2, 0, 0] }));
    await h.actions.buy_plus({ ack: noop, action: { value: '1' }, body: { view: { id: 'V1', hash: 'h', private_metadata: v.private_metadata } }, client: h.client });
  });

  const submitBody = (m, senders, redirect) => ({ user: { id: 'U1' }, view: { private_metadata: JSON.stringify(m), state: { values: {
    senders: { value: { value: senders } }, ...(redirect !== undefined ? { redirect: { value: { value: redirect } } } : {}) } } } });

  await test('submit refuses senders without a last name', async () => {
    const h = harness();
    let acked;
    await h.views.buy_flow({ ack: async a => { acked = a; }, body: submitBody(meta(1, { p: 'zapmail', k: [1, 0] }), 'Avinash'), view: submitBody(meta(1, { p: 'zapmail', k: [1, 0] }), 'Avinash').view, client: h.client });
    assert.strictEqual(acked.response_action, 'errors');
    assert.ok(acked.errors.senders);
  });

  await test('submit stages each non-empty row; a failed row does not stop the others', async () => {
    const calls = [];
    const h = harness(async args => { calls.push(args); if (args.includes('outlook')) throw new Error('boom'); return { staged: true, plan_id: 'abc123', client: 'Precise Leads', provider: 'google', domains: ['a.com'], mailboxes_per_domain: 3, mailboxes: 3, monthly_usd: 10.5, domains_usd: 15.5, senders: ['A B'] }; });
    const m = meta(3, { p: 'scaledmail', k: [1, 1, 1] });
    const b = submitBody(m, 'Avinash Haridas', 'https://preciseleads.in');
    await h.views.buy_flow({ ack: noop, body: b, view: b.view, client: h.client });
    assert.strictEqual(calls.length, 3);
    assert.deepStrictEqual(calls.map(a => a[a.indexOf('--provider') + 1]), ['google', 'outlook', 'smtp']);
    const all = JSON.stringify(h.said);
    assert.match(all, /Microsoft Outlook.*boom/);
    assert.match(all, /abc123/);
  });
  await test('an answered error still says which row it was', async () => {
    const h = harness(async args => (args.includes('MICROSOFT') ? { error: 'z.com is taken' } : { job_id: 'abcdef0123', status: 'pending_approval', client: 'Precise Leads', kind: 'new', provider: 'GOOGLE', domains: ['a.com'], inboxes_per_domain: 3, cost: null, progress: '' }));
    const m = meta(2, { p: 'zapmail', k: [1, 1] });
    const b = submitBody(m, 'Avinash Haridas');
    await h.views.buy_flow({ ack: noop, body: b, view: b.view, client: h.client });
    const outlook = h.said.find(p => JSON.stringify(p.blocks || []).includes('z.com is taken'));
    assert.ok(outlook, 'error posted');
    assert.match(JSON.stringify(outlook.blocks), /Microsoft Outlook\* · name1\.com/);
  });

  console.log(passed + ' passed');
})();
