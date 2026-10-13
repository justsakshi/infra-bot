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
    assert.deepStrictEqual(nav.elements.map(e => e.action_id), ['nav_home', 'nav_close']);
  });

  await test('text-only screens get a section, and Back when given', async () => {
    const out = [];
    await r.replace(async m => out.push(m), { text: ':x: nope' }, { label: 'ScaledMail', action_id: 'sm_home' });
    assert.match(out[0].blocks[0].text.text, /nope/);
    assert.deepStrictEqual(out[0].blocks[1].elements.map(e => [e.action_id, e.text.text]),
      [['nav_home', '⌂ Home'], ['sm_home', '← ScaledMail'], ['nav_close', '✕ Close']]);
  });

  await test('a long screen keeps Slack\'s 50-block cap with nav last', async () => {
    const out = [];
    const blocks = Array.from({ length: 60 }, () => ({ type: 'divider' }));
    await r.replace(async m => out.push(m), { text: 't', blocks });
    assert.strictEqual(out[0].blocks.length, 50);
    assert.strictEqual(out[0].blocks[49].block_id, 'nav_row');
  });

  await test('add posts a new message, with a Close button, and leaves the clicked one alone', async () => {
    const out = [];
    await r.add(async m => out.push(m), { text: 'Tick at least one domain first.' });
    assert.strictEqual(out[0].replace_original, false);
    assert.match(out[0].blocks[0].text.text, /Tick at least one/);
    assert.deepStrictEqual(out[0].blocks[1].elements.map(e => e.action_id), ['nav_close']);
  });

  await test('withClose adds one Close button and leaves public or delete-only messages alone', async () => {
    const once = r.withClose(r.withClose({ text: 'hi' }));
    assert.strictEqual(JSON.stringify(once).split('nav_close').length - 1, 1, 'no second Close');
    assert.strictEqual(once.text, 'hi', 'text kept for notifications');
    assert.ok(!r.withClose({ response_type: 'in_channel', text: 'everyone' }).blocks, 'everyone sees it: no Close');
    assert.deepStrictEqual(r.withClose({ delete_original: true }), { delete_original: true });
    assert.strictEqual(r.withClose('plain').blocks[0].text.text, 'plain', 'a string reply works too');
  });

  await test('no reply carries a key, even one that skipped plainError', async () => {
    const p = r.withClose({ text: ':x: ' + "401 for url 'https://server.smartlead.ai/x?api_key=SECRET123'", blocks: [
      { type: 'section', text: { type: 'mrkdwn', text: 'Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abc' } }] });
    assert.ok(!/SECRET123|eyJhbGci/.test(JSON.stringify(p)), JSON.stringify(p));
  });

  await test('a long text reply is split into sections, not cut', async () => {
    const line = 'x'.repeat(100);
    const text = Array.from({ length: 80 }, () => line).join('\n');
    const b = r.withClose({ text }).blocks;
    const sections = b.filter(x => x.type === 'section');
    assert.ok(sections.length >= 3 && sections.every(x => x.text.text.length <= 2900));
    assert.strictEqual(sections.map(x => x.text.text).join('\n'), text);
  });

  await test('nav_close deletes the clicked message', async () => {
    const h = {};
    r.registerNav({ action: (id, fn) => { h[id] = fn; } }, __dirname);
    const out = [];
    await h.nav_close({ ack: async () => {}, respond: async m => out.push(m) });
    assert.deepStrictEqual(out, [{ delete_original: true }]);
  });

  await test('swap turns the typed command\'s "Checking…" message into the result', async () => {
    const out = [];
    await r.swap(async m => out.push(m), { text: 'Fleet' });
    assert.strictEqual(out[0].replace_original, true);
    assert.ok(JSON.stringify(out[0]).includes('nav_close') && !JSON.stringify(out[0]).includes('nav_home'));
    const tries = [];
    await r.swap(async m => { tries.push(m.replace_original); if (m.replace_original) throw new Error('400'); }, { text: 'Fleet' });
    assert.deepStrictEqual(tries, [true, false], 'Slack refused the replace: post it as a new message');
  });

  await test('withCloseButtons: every listener\'s respond adds Close; others pass through', async () => {
    const h = {};
    const fake = { action: (id, fn) => { h[id] = fn; }, message: fn => { h.message = fn; }, client: { x: 1 } };
    const ui = r.withCloseButtons(fake);
    const out = [];
    ui.action('a', async ({ respond }) => respond({ text: 'screen' }));
    await h.a({ respond: async m => out.push(m) });
    assert.ok(JSON.stringify(out[0]).includes('nav_close'));
    let saw;
    ui.message(async args => { saw = args.message; });
    await h.message({ message: 'm' });
    assert.strictEqual(saw, 'm');
    assert.strictEqual(ui.client.x, 1);
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
