// One message per conversation: replace + nav row. Run: node test_slack_reply.js
const assert = require('assert');
const r = require('./slack_reply');

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.message); process.exitCode = 1; }
}

(async () => {
  await test('replace swaps the clicked message and adds Home', async () => {
    const out = [];
    await r.replace(async m => out.push(m), { text: 'Orders', blocks: [{ type: 'section', text: { type: 'mrkdwn', text: 'x' } }] });
    assert.strictEqual(out[0].replace_original, true);
    assert.strictEqual(out[0].response_type, 'ephemeral');
    const nav = out[0].blocks[out[0].blocks.length - 1];
    assert.deepStrictEqual(nav.elements.map(e => e.action_id), ['nav_home']);
  });

  await test('text-only screens get a section, and Back when given', async () => {
    const out = [];
    await r.replace(async m => out.push(m), { text: ':x: nope' }, { label: 'ScaledMail', action_id: 'sm_home' });
    assert.match(out[0].blocks[0].text.text, /nope/);
    assert.deepStrictEqual(out[0].blocks[1].elements.map(e => [e.action_id, e.text.text]), [['nav_home', '⌂ Home'], ['sm_home', '← ScaledMail']]);
  });

  await test('a long screen keeps Slack\'s 50-block cap with nav last', async () => {
    const out = [];
    const blocks = Array.from({ length: 60 }, () => ({ type: 'divider' }));
    await r.replace(async m => out.push(m), { text: 't', blocks });
    assert.strictEqual(out[0].blocks.length, 50);
    assert.strictEqual(out[0].blocks[49].block_id, 'nav_row');
  });

  await test('add posts a new message and leaves the clicked one alone', async () => {
    const out = [];
    await r.add(async m => out.push(m), { text: 'Tick at least one domain first.' });
    assert.strictEqual(out[0].replace_original, false);
    assert.ok(!out[0].blocks);
  });

  await test('nav_home redraws the domains menu in place', async () => {
    const h = {};
    r.registerNav({ action: (id, fn) => { h[id] = fn; } }, __dirname);
    const out = [];
    await h.nav_home({ ack: async () => {}, respond: async m => out.push(m) });
    assert.strictEqual(out[0].replace_original, true);
    assert.ok(out[0].blocks.length > 1);
  });

  console.log(passed + ' passed');
})();
