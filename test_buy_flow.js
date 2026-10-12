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

  console.log(passed + ' passed');
})();
