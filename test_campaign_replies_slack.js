// "Show replies" click: private, in the thread, never silent. Run: node test_campaign_replies_slack.js
const assert = require('assert');
const { registerShowReplies, parseLastJson } = require('./campaign_replies_slack');

let passed = 0;
async function test(name, fn) {
  try { await fn(); passed += 1; } catch (err) { console.error('FAIL', name, '\n ', err.message); process.exitCode = 1; }
}

function harness(result) {
  const handlers = {};
  const app = { action: (id, fn) => { handlers[id] = fn; } };
  const posted = [];
  const ran = [];
  registerShowReplies(app, { run: async (v) => { ran.push(v); return result; } });
  const client = { chat: { postEphemeral: async (m) => { posted.push(m); return { ok: true }; } } };
  return { handlers, posted, ran, client };
}

const value = JSON.stringify({ title: 'Roundtable · Laurie', platform: 'Smartlead', id: '1' });
const body = { channel: { id: 'C1' }, user: { id: 'U1' }, message: { ts: '200.1', thread_ts: '100.1' } };

(async () => {
  await test('acks, says it is fetching, then shows the replies to the clicker in the thread', async () => {
    const blocks = [{ type: 'section', text: { type: 'mrkdwn', text: 'x' } }];
    const h = harness({ text: '2 email replies', blocks });
    let acked = false;
    await h.handlers.show_replies({ ack: async () => { acked = true; }, body, action: { value }, client: h.client });
    assert.ok(acked);
    assert.deepStrictEqual(h.ran, [value]);
    assert.strictEqual(h.posted.length, 2);
    assert.match(h.posted[0].text, /Fetching the replies for Roundtable · Laurie/);
    for (const m of h.posted) {
      assert.strictEqual(m.channel, 'C1'); assert.strictEqual(m.user, 'U1');
      assert.strictEqual(m.thread_ts, '100.1');            // the thread, not a new top-level post
    }
    assert.deepStrictEqual(h.posted[1].blocks, blocks);
  });

  await test('a failed run says so instead of going quiet', async () => {
    const h = harness(null);
    await h.handlers.show_replies({ ack: async () => {}, body, action: { value }, client: h.client });
    assert.match(h.posted[1].text, /Couldn't read the replies/);
  });

  await test('a click on a message outside a thread uses that message as the thread', async () => {
    const h = harness({ text: 't', blocks: [] });
    const b = { ...body, message: { ts: '300.1' } };
    await h.handlers.show_replies({ ack: async () => {}, body: b, action: { value }, client: h.client });
    assert.strictEqual(h.posted[0].thread_ts, '300.1');
  });

  await test('the answer is the last JSON line, after any retry notes', async () => {
    const out = '  [!] 429 on /campaigns - retrying\n{"text":"t","blocks":[{"type":"divider"}]}\n';
    assert.deepStrictEqual(parseLastJson(out), { text: 't', blocks: [{ type: 'divider' }] });
    assert.strictEqual(parseLastJson('Traceback ...'), null);
  });

  console.log(`${passed} passed`);
})();
