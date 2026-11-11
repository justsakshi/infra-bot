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

  await test('mix view: one tidy row per type with a count picker, totals in a grid', () => {
    const v = bf.mixView(meta(7, { p: 'scaledmail', k: [7, 0, 0] }));
    const rows = v.blocks.filter(b => b.accessory && b.accessory.action_id === 'buy_count');
    assert.strictEqual(rows.length, 3);
    assert.match(rows[0].text.text, /\*Google Workspace\*\n3 inboxes\/domain · 60\/day · 1,320\/mo · \$10\.50\/mo per domain/);
    assert.strictEqual(rows[0].accessory.initial_option.value, '0|7');
    assert.strictEqual(rows[0].accessory.options.length, 8);                     // 0..7
    assert.strictEqual(rows[0].accessory.options[1].text.text, '1 domain');
    assert.strictEqual(rows[1].accessory.initial_option.value, '1|0');
    const s = JSON.stringify(v.blocks);
    assert.match(s, /`name0\.com`/);                                            // code, not a link
    assert.ok(v.blocks.filter(b => b.type === 'divider').length >= 3);
    const grid = v.blocks.find(b => b.fields);
    assert.deepStrictEqual(grid.fields.map(f => f.text), ['*Domains*\n7', '*Inboxes*\n21', '*Per day*\n~420', '*Per month*\n~9,240',
      '*First charge*\n$182.00', '*Then*\n$73.50/mo']);
    assert.match(s, /Ready to send around/);
    const nav = v.blocks.find(b => b.block_id === 'nav').elements;
    assert.deepStrictEqual(nav.map(e => [e.action_id, e.style || '']), [['buy_back', ''], ['buy_next', 'primary']]);
  });

  await test('setCount moves domains between rows and keeps the sum', () => {
    assert.deepStrictEqual(bf.setCount([5, 0, 0], 2, 3), [2, 0, 3]);
    assert.deepStrictEqual(bf.setCount([2, 0, 3], 2, 1), [4, 0, 1]);
    assert.deepStrictEqual(bf.setCount([2, 0, 3], 0, 5), [5, 0, 0]);
    assert.deepStrictEqual(bf.setCount([2, 0, 3], 0, 0), [0, 2, 3]);
    assert.deepStrictEqual(bf.setCount([2, 0, 3], 1, 99), [0, 5, 0]);
  });

  await test('confirm view: senders and redirect prefilled for both providers, submit Stage', () => {
    const v = bf.confirmView(meta(2, { p: 'scaledmail', k: [2, 0, 0] }), { senders: 'Avinash Haridas, Aravind Haridas', redirect: 'https://preciseleads.in' });
    assert.strictEqual(v.submit.text, 'Stage (no charge)');
    const ids = v.blocks.filter(b => b.type === 'input').map(b => b.block_id);
    assert.deepStrictEqual(ids, ['senders', 'redirect', 'sig']);
    assert.strictEqual(v.blocks.find(b => b.block_id === 'senders').element.initial_value, 'Avinash Haridas, Aravind Haridas');
    const z = bf.confirmView(meta(2, { p: 'zapmail', k: [2, 0] }), { senders: '', redirect: '' });
    assert.deepStrictEqual(z.blocks.filter(b => b.type === 'input').map(b => b.block_id), ['senders', 'redirect', 'sig']);
    assert.ok(!('initial_value' in z.blocks.find(b => b.block_id === 'senders').element));
  });

  await test('confirm view: signature box, ticked by default, names the client', () => {
    const v = bf.confirmView(meta(1, { p: 'zapmail', k: [1, 0] }), { senders: 'A B', redirect: '' });
    const sig = v.blocks.find(b => b.block_id === 'sig');
    assert.strictEqual(sig.element.type, 'checkboxes');
    assert.match(sig.element.options[0].text.text, /Precise Leads/);
    assert.deepStrictEqual(sig.element.initial_options.map(o => o.value), ['yes']);
    assert.ok(sig.optional);
  });

  await test('stage args: Zapmail job asks for the signature when ticked', () => {
    const z = meta(1, { p: 'zapmail', k: [1, 0] });
    const a = bf.stageArgs(z, 0, ['d.com'], 'A B', '', 'U1', { signature: true });
    assert.deepStrictEqual(a.slice(-4), ['--signature', '--by', 'U1', '--json']);
    assert.ok(!bf.stageArgs(z, 0, ['d.com'], 'A B', '', 'U1').includes('--signature'));
  });

  await test('stage args: Zapmail job carries the redirect', () => {
    const z = meta(1, { p: 'zapmail', k: [1, 0] });
    const a = bf.stageArgs(z, 0, ['d.com'], 'A B', 'https://preciseleads.in', 'U1');
    assert.deepStrictEqual(a.slice(a.indexOf('--redirect'), a.indexOf('--redirect') + 2), ['--redirect', 'https://preciseleads.in']);
    assert.ok(!bf.stageArgs(z, 0, ['d.com'], 'A B', '', 'U1').includes('--redirect'));
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
  function harness(runPy, check) {
    const h = { actions: {}, views: {}, opened: [], updated: [], said: [] };
    const app = { action: (id, fn) => { h.actions[id] = fn; }, view: (id, fn) => { h.views[id] = fn; } };
    h.client = { views: { open: async a => { h.opened.push(a.view); }, update: async a => { h.updated.push(a.view); } } };
    bf.registerBuyFlow(app, __dirname, {
      runPy: runPy || (async args => ({ staged: true, plan_id: 'abc123', args })),
      check: check || (async () => ({ ok: true })),
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

  await test('picking a count redraws with the new split', async () => {
    const h = harness();
    const v = bf.mixView(meta(3, { p: 'scaledmail', k: [3, 0, 0] }));
    await h.actions.buy_count({ ack: noop, action: { selected_option: { value: '2|1' } }, body: { view: { id: 'V1', hash: 'h', private_metadata: v.private_metadata } }, client: h.client });
    assert.deepStrictEqual(JSON.parse(h.updated[0].private_metadata).k, [2, 0, 1]);
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

  await test('submit passes the ticked signature to the Zapmail job', async () => {
    const calls = [];
    const h = harness(async args => { calls.push(args); return { job_id: 'abcdef0123', status: 'awaiting_approval', client: 'Precise Leads', domains: ['a.com'], provider: 'GOOGLE', cost: null }; });
    const m = meta(1, { p: 'zapmail', k: [1, 0] });
    const b = submitBody(m, 'Avinash Haridas');
    b.view.state.values.sig = { value: { selected_options: [{ value: 'yes' }] } };
    await h.views.buy_flow({ ack: noop, body: b, view: b.view, client: h.client });
    assert.ok(calls[0].includes('--signature'));
  });

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
    assert.match(JSON.stringify(outlook.blocks), /Microsoft Outlook\* · `name1\.com`/);
  });

  await test('stage replaces the suggestions message; each card is its own message', async () => {
    const posts = [];
    const h = { actions: {}, views: {}, said: [] };
    const app = { action: (id, fn) => { h.actions[id] = fn; }, view: (id, fn) => { h.views[id] = fn; } };
    bf.registerBuyFlow(app, __dirname, {
      runPy: async () => ({ staged: true, plan_id: 'abc123', client: 'Precise Leads', provider: 'google', domains: ['a.com'], mailboxes_per_domain: 3, mailboxes: 3, monthly_usd: 10.5, domains_usd: 15.5, senders: ['A B'] }),
      say: async (c, ch, u, p) => { h.said.push(p); },
      post: async (url, body) => { posts.push({ url, body }); },
      profiles: () => ({}), senders: () => ({})
    });
    const opened = [];
    const client = { views: { open: async a => opened.push(a.view) } };
    await h.actions.buy_open({ ack: noop, action: { value: 'Precise Leads' }, body: { state: { values: tickState }, trigger_id: 't', channel: { id: 'C1' }, response_url: 'https://hooks.slack.com/x' }, client, respond: noop });
    const m = JSON.parse(opened[0].private_metadata);
    assert.strictEqual(m.ru, 'https://hooks.slack.com/x');
    m.p = 'scaledmail'; m.k = [1, 1, 0];
    const b = submitBody(m, 'Avinash Haridas', '');
    await h.views.buy_flow({ ack: noop, body: b, view: b.view, client });
    assert.strictEqual(h.said.length, 0, 'nothing posted outside the thread of clicks');
    assert.deepStrictEqual(posts.map(x => x.body.replace_original), [true, false, false, true]);
    assert.match(posts[0].body.text, /Staging/);
    assert.match(JSON.stringify(posts[3].body.blocks), /Staged 2 order/);
    assert.strictEqual(posts[3].body.blocks[posts[3].body.blocks.length - 1].block_id, 'nav_row');
    assert.ok(posts.length <= 5, 'Slack allows 5 uses of a response_url');
  });

  await test('an expired response_url falls back to posting the cards', async () => {
    const h = { actions: {}, views: {}, said: [] };
    const app = { action: (id, fn) => { h.actions[id] = fn; }, view: (id, fn) => { h.views[id] = fn; } };
    bf.registerBuyFlow(app, __dirname, {
      runPy: async () => ({ staged: true, plan_id: 'abc123', client: 'Precise Leads', provider: 'google', domains: ['a.com'], mailboxes_per_domain: 3, mailboxes: 3, monthly_usd: 10.5, domains_usd: 15.5, senders: ['A B'] }),
      say: async (c, ch, u, p) => { h.said.push(p); },
      post: async () => { throw new Error('expired_url'); },
      profiles: () => ({}), senders: () => ({})
    });
    const m = meta(1, { p: 'scaledmail', k: [1, 0, 0], ru: 'https://hooks.slack.com/x' });
    const b = submitBody(m, 'Avinash Haridas', '');
    await h.views.buy_flow({ ack: noop, body: b, view: b.view, client: {} });
    assert.ok(JSON.stringify(h.said).includes('abc123'));
  });

  // ── buying rule on the confirm step: a warning, never a block (smartlead_sync/buy_check.py) ──
  const QUIET = '_Could not check the buying rule just now, so no trial warning is shown._';
  const WHY = 'only 0 Zapmail Google inboxes have passed their placement test twice (6 needed): buy at most 6 as a trial, '
    + 'test them weekly from the first week, and buy more once they pass twice';
  const LINE30 = ':warning: Trial first: Only 0 Zapmail Google inboxes have passed their placement test twice (6 needed): '
    + 'buy at most 6 as a trial, test them weekly from the first week, and buy more once they pass twice. This order has 30 of them.';
  const unproven = (type, count, why) => ({ vendor: 'zapmail', type, count, proven: 0, tested: 0, ok: false, limit: 6, why: why || WHY });
  const fine = (type, count, proven) => ({ vendor: 'zapmail', type, count, proven, tested: proven, ok: true,
    limit: proven >= 6 ? null : 6, why: 'not shown' });
  const quietly = async fn => {
    const saved = console.error;
    console.error = () => {};
    try { return await fn(); } finally { console.error = saved; }
  };

  await test('order inboxes: per inbox type, in inboxes, not domains', () => {
    assert.deepStrictEqual(bf.orderInboxes('zapmail', [10, 4]), [{ type: 'google', inboxes: 30 }, { type: 'outlook', inboxes: 12 }]);
    assert.deepStrictEqual(bf.orderInboxes('scaledmail', [1, 1, 1]),
      [{ type: 'google', inboxes: 3 }, { type: 'outlook', inboxes: 25 }, { type: 'smtp', inboxes: 4 }]);
    assert.deepStrictEqual(bf.orderInboxes('scaledmail', [0, 2, 0]), [{ type: 'outlook', inboxes: 50 }]);   // empty rows left out
    assert.deepStrictEqual(bf.orderInboxes('nobody', [3]), []);
    assert.deepStrictEqual(bf.orderInboxes('zapmail', undefined), []);
  });

  await test('buy check args: vendor, type and inbox count for each type in the order', () => {
    assert.deepStrictEqual(bf.buyCheckArgs('zapmail', bf.orderInboxes('zapmail', [10, 4])), [
      ['buy_check.py', '--vendor', 'zapmail', '--type', 'google', '--count', '30', '--json'],
      ['buy_check.py', '--vendor', 'zapmail', '--type', 'outlook', '--count', '12', '--json']]);
    assert.deepStrictEqual(bf.buyCheckArgs('scaledmail', bf.orderInboxes('scaledmail', [2, 1, 3])), [
      ['buy_check.py', '--vendor', 'scaledmail', '--type', 'google', '--count', '6', '--json'],
      ['buy_check.py', '--vendor', 'scaledmail', '--type', 'outlook', '--count', '25', '--json'],
      ['buy_check.py', '--vendor', 'scaledmail', '--type', 'smtp', '--count', '12', '--json']]);
    assert.deepStrictEqual(bf.buyCheckArgs('zapmail', []), []);
  });

  await test('buy warnings: 30 inboxes of an unproven kind give one line with the reason and the count', () => {
    assert.deepStrictEqual(bf.buyWarnings([{ type: 'google', inboxes: 30 }], [unproven('google', 30)]), [LINE30]);
  });

  await test('buy warnings: a trial-sized order or a proven kind gives no line', () => {
    assert.deepStrictEqual(bf.buyWarnings([{ type: 'google', inboxes: 6 }], [fine('google', 6, 0)]), []);
    assert.deepStrictEqual(bf.buyWarnings([{ type: 'google', inboxes: 30 }], [fine('google', 30, 7)]), []);
    assert.deepStrictEqual(bf.buyWarnings([], []), []);
  });

  await test('buy warnings: two unproven types give two lines', () => {
    const lines = bf.buyWarnings([{ type: 'google', inboxes: 30 }, { type: 'outlook', inboxes: 12 }],
      [unproven('google', 30), unproven('outlook', 12, 'only 2 Zapmail Outlook inboxes have passed twice.')]);
    assert.deepStrictEqual(lines, [LINE30,
      ':warning: Trial first: Only 2 Zapmail Outlook inboxes have passed twice. This order has 12 of them.']);
    // one unproven, one proven: only the unproven one
    assert.deepStrictEqual(bf.buyWarnings([{ type: 'google', inboxes: 30 }, { type: 'outlook', inboxes: 12 }],
      [unproven('google', 30), fine('outlook', 12, 9)]), [LINE30]);
  });

  await test('buy warnings: an unreadable check gives the one quiet line, no warning, and nothing throws', () => {
    const bads = [{ error: 'could not read the tracker or the v2 verdicts (SecretError)' }, new Error('SecretError: timed out'),
      null, undefined, 'SecretError not json', 7, [], { staged: true }, { ok: false }, { ok: 'false', why: 'x' }, { ok: false, why: '' }];
    for (const bad of bads) {
      const lines = bf.buyWarnings([{ type: 'google', inboxes: 30 }], [bad]);
      assert.deepStrictEqual(lines, [QUIET], 'for ' + JSON.stringify(bad));
    }
    // two unreadable types: still one line, and the raw error text is never shown
    const two = bf.buyWarnings([{ type: 'google', inboxes: 30 }, { type: 'outlook', inboxes: 12 }], [bads[0], bads[1]]);
    assert.deepStrictEqual(two, [QUIET]);
    assert.ok(!two.join(' ').includes('SecretError'));
    assert.deepStrictEqual(bf.buyWarnings([{ type: 'google', inboxes: 30 }], undefined), [QUIET]);
    assert.deepStrictEqual(bf.buyWarnings([{ type: 'google', inboxes: 30 }], []), [QUIET]);
  });

  await test('buy warnings: one type read, one not: the warning that was read, then a note that some may be missing', () => {
    const lines = bf.buyWarnings([{ type: 'google', inboxes: 30 }, { type: 'outlook', inboxes: 12 }], [unproven('google', 30), { error: 'x' }]);
    assert.deepStrictEqual(lines, [LINE30,
      '_Could not check the buying rule for every inbox type just now, so a trial warning may be missing._']);
  });

  await test('order warnings: one check per type, run in parallel, with inbox counts', async () => {
    const calls = [];
    let running = 0;
    let most = 0;
    const check = async args => {
      calls.push(args);
      running += 1; most = Math.max(most, running);
      await new Promise(r => setTimeout(r, 15));
      running -= 1;
      return unproven(args[4], Number(args[6]));
    };
    const lines = await bf.orderWarnings({ p: 'zapmail', k: [10, 4] }, check);
    assert.deepStrictEqual(calls, [
      ['buy_check.py', '--vendor', 'zapmail', '--type', 'google', '--count', '30', '--json'],
      ['buy_check.py', '--vendor', 'zapmail', '--type', 'outlook', '--count', '12', '--json']]);
    assert.strictEqual(most, 2, 'both checks were running at once');
    assert.strictEqual(lines.length, 2);
    assert.match(lines[1], /This order has 12 of them\.$/);
  });

  await test('order warnings: a check error, a thrown error and malformed output each give the quiet line; nothing throws', async () => {
    const checks = [
      async () => ({ error: 'could not read the tracker or the v2 verdicts (SecretError)' }),
      () => { throw new Error('SecretError thrown'); },
      async () => { throw new Error('SecretError: timed out'); },
      async () => 'SecretError garbage',
      async () => ({ staged: true }),
      async () => null];
    for (const check of checks) {
      const lines = await quietly(() => bf.orderWarnings({ p: 'zapmail', k: [10, 0] }, check));
      assert.deepStrictEqual(lines, [QUIET]);
    }
    assert.deepStrictEqual(await quietly(() => bf.orderWarnings({ p: 'zapmail', k: [10, 0] }, undefined)), [QUIET]);
    assert.deepStrictEqual(await bf.orderWarnings({}, async () => { throw new Error('never asked'); }), []);   // nothing to check
  });

  await test('confirm view: warnings sit on top, above the totals; with none the view is unchanged', () => {
    const m = meta(10, { p: 'zapmail', k: [10, 0] });
    const d = { senders: 'A B', redirect: '' };
    const plain = bf.confirmView(m, d);
    assert.strictEqual(plain.blocks[0].type, 'section');
    assert.ok(plain.blocks[0].fields, 'the totals grid is first when there is no warning');
    assert.deepStrictEqual(bf.confirmView(m, d, []), plain);
    assert.ok(!txt(plain).includes('Trial first') && !txt(plain).includes('buying rule'));

    const v = bf.confirmView(m, d, [LINE30]);
    assert.deepStrictEqual(v.blocks[0], { type: 'section', text: { type: 'mrkdwn', text: LINE30 } });
    assert.deepStrictEqual(v.blocks.slice(1), plain.blocks);
    assert.strictEqual(v.submit.text, 'Stage (no charge)');
    assert.strictEqual(v.private_metadata, plain.private_metadata);

    const two = bf.confirmView(m, d, [LINE30, LINE30.replace('30 of them', '12 of them')]);
    assert.strictEqual(two.blocks[0].text.text.split('\n').length, 2);                 // one line each, one block
    assert.deepStrictEqual(two.blocks.slice(1), plain.blocks);

    const q = bf.confirmView(m, d, [QUIET]);
    assert.deepStrictEqual(q.blocks[0], { type: 'context', elements: [{ type: 'mrkdwn', text: QUIET }] });
    assert.deepStrictEqual(q.blocks.slice(1), plain.blocks);
    assert.strictEqual(q.submit.text, 'Stage (no charge)');
  });

  const nextBody = m => ({ view: { id: 'V1', hash: 'h', private_metadata: bf.mixView(m).private_metadata } });
  const zapJob = { job_id: 'abcdef0123', status: 'awaiting_approval', client: 'Client', domains: ['a.com'], provider: 'GOOGLE', cost: null };

  await test('next: an unproven kind shows the warning, and the order is still staged', async () => {
    const checks = [];
    const stages = [];
    const h = harness(async args => { stages.push(args); return zapJob; }, async args => { checks.push(args); return unproven('google', 30); });
    const m = meta(10, { p: 'zapmail', k: [10, 0] });
    await h.actions.buy_next({ ack: noop, action: { value: 'x' }, body: nextBody(m), client: h.client });
    assert.deepStrictEqual(checks, [['buy_check.py', '--vendor', 'zapmail', '--type', 'google', '--count', '30', '--json']]);
    const v = h.updated[0];
    assert.strictEqual(v.blocks[0].text.text, LINE30);
    assert.strictEqual(v.submit.text, 'Stage (no charge)');
    assert.deepStrictEqual(v.blocks.filter(b => b.type === 'input').map(b => b.block_id), ['senders', 'redirect', 'sig']);
    // Stage: accepted without errors, the order goes to the job, the rule is not asked again.
    const b = submitBody(JSON.parse(v.private_metadata), 'Jane Doe');
    let acked = 'not called';
    await h.views.buy_flow({ ack: async a => { acked = a; }, body: b, view: b.view, client: h.client });
    assert.strictEqual(acked, undefined);
    assert.strictEqual(stages.length, 1);
    assert.strictEqual(stages[0][0], 'inbox_jobs.py');
    assert.strictEqual(checks.length, 1);
  });

  await test('next: a proven kind adds nothing to the confirm step', async () => {
    const h = harness(null, async () => fine('google', 30, 8));
    const m = meta(10, { p: 'zapmail', k: [10, 0] });
    await h.actions.buy_next({ ack: noop, action: { value: 'x' }, body: nextBody(m), client: h.client });
    const v = h.updated[0];
    assert.ok(v.blocks[0].fields, 'the totals grid is still first');
    assert.ok(!txt(v).includes('Trial first') && !txt(v).includes('buying rule'));
    assert.deepStrictEqual(v.blocks.map(b => b.type), bf.confirmView(m, { senders: '', redirect: '' }).blocks.map(b => b.type));
  });

  await test('next: a check that fails still opens the confirm step, with the quiet line', async () => {
    for (const check of [async () => { throw new Error('SecretError: timed out'); }, async () => ({ error: 'SecretError' }), async () => 'x']) {
      const h = harness(null, check);
      const m = meta(10, { p: 'zapmail', k: [10, 0] });
      await quietly(() => h.actions.buy_next({ ack: noop, action: { value: 'x' }, body: nextBody(m), client: h.client }));
      const v = h.updated[0];
      assert.strictEqual(v.blocks[0].elements[0].text, QUIET);
      assert.strictEqual(v.submit.text, 'Stage (no charge)');
      assert.ok(!txt(v).includes('SecretError') && !txt(v).includes('Trial first'));
    }
  });

  // The real runner, with node standing in for Python and a throwaway script (still no Python, no network).
  await test('run check: exit code 1 (trial only) is an answer, not a failure; bad output and a timeout reject', async () => {
    const os = require('os');
    const fs = require('fs');
    const path = require('path');
    const base = fs.mkdtempSync(path.join(os.tmpdir(), 'buy-check-'));
    fs.mkdirSync(path.join(base, 'smartlead_sync'));
    fs.writeFileSync(path.join(base, 'smartlead_sync', 'fake_check.js'), [
      'const mode = process.argv[2];',
      'if (mode === "trial") { console.log("a log line"); console.log(JSON.stringify({ ok: false, limit: 6, why: "w" })); process.exit(1); }',
      'if (mode === "fine") { console.log(JSON.stringify({ ok: true, limit: null, why: "w" })); process.exit(0); }',
      'if (mode === "error") { console.log(JSON.stringify({ error: "e" })); process.exit(2); }',
      'if (mode === "garbage") { console.log("Traceback (most recent call last):"); process.exit(1); }',
      'if (mode === "hang") setTimeout(() => {}, 30000);'].join('\n'));
    const saved = process.env.INFRABOT_PYTHON;
    process.env.INFRABOT_PYTHON = process.execPath;
    try {
      assert.deepStrictEqual(await bf.runCheck(['fake_check.js', 'trial'], base, 20000), { ok: false, limit: 6, why: 'w' });
      assert.deepStrictEqual(await bf.runCheck(['fake_check.js', 'fine'], base, 20000), { ok: true, limit: null, why: 'w' });
      assert.deepStrictEqual(await bf.runCheck(['fake_check.js', 'error'], base, 20000), { error: 'e' });
      await assert.rejects(bf.runCheck(['fake_check.js', 'garbage'], base, 20000));
      await assert.rejects(bf.runCheck(['fake_check.js', 'silent'], base, 20000));
      await assert.rejects(bf.runCheck(['fake_check.js', 'hang'], base, 300), /timed out/);
      // and end to end: the trial-only answer becomes the warning
      const lines = await bf.orderWarnings({ p: 'zapmail', k: [10, 0] }, () => bf.runCheck(['fake_check.js', 'trial'], base, 20000));
      assert.deepStrictEqual(lines, [':warning: Trial first: W. This order has 30 of them.']);
    } finally {
      if (saved === undefined) delete process.env.INFRABOT_PYTHON; else process.env.INFRABOT_PYTHON = saved;
      fs.rmSync(base, { recursive: true, force: true });
    }
  });

  console.log(passed + ' passed');
})();
