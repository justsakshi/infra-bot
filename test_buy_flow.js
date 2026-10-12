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
  console.log(passed + ' passed');
})();
