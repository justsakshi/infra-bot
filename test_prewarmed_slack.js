// Pre-warmed purchase from Slack: view buttons, form, job card. Run: node test_prewarmed_slack.js
const assert = require('assert');
const { prewarmedBlocks, prewarmedModal, jobBlocks } = require('./zapmail_actions');

let n = 0;
const res = { prewarmed: { stock: { google: 472, microsoft: 71 }, accounts: {}, for_sale: {
  GOOGLE: [{ domain: 'apexgtmcraft.co', id: '3f9c2a1e-7b4d', price: 39, mailboxes: ['Christy Hughes', 'Blair Gray'] },
    { domain: 'bad domain', id: 'x' }],
  MICROSOFT: [{ domain: 'amberdealgroup.co', id: 'ab12cd34ef', mailboxes: [] }] } } };

const blocks = prewarmedBlocks(res);
const buttons = blocks.filter(b => b.accessory && b.accessory.action_id === 'zm_pw_open');
assert.strictEqual(buttons.length, 2, 'one button per valid for-sale domain'); n++;
const v = JSON.parse(buttons[0].accessory.value);
assert.deepStrictEqual(v, { domain: 'apexgtmcraft.co', id: '3f9c2a1e-7b4d', provider: 'GOOGLE' }); n++;
assert.ok(/Christy Hughes/.test(buttons[0].text.text)); n++;
assert.ok(!/stays in the Zapmail app/.test(JSON.stringify(blocks)), 'old "buy in the app" text gone'); n++;

const m = prewarmedModal(v, 'C1');
assert.strictEqual(m.callback_id, 'zm_pw_submit');
assert.deepStrictEqual(m.blocks[1].element.options.map(o => o.value), ['Bettrdata', 'Precise Leads', 'Melior']); n++;
assert.ok(/nothing is bought until someone approves/.test(JSON.stringify(m))); n++;

const awaiting = jobBlocks({ job_id: 'abcdef0123', client: 'Melior', domains: ['apexgtmcraft.co'], provider: 'GOOGLE',
  status: 'awaiting_approval', progress: 'Pre-warmed slot · Assign', cost: { lines: [
    { what: 'pre-warmed starter plan (3 inboxes)', usd: 39, when: 'first month, then $24/month' }], total_now_usd: 39 } });
const act = awaiting.find(b => b.type === 'actions');
assert.ok(act && act.elements[0].action_id === 'zm_job_approve' && act.elements[0].value === 'abcdef0123'); n++; assert.strictEqual(act.elements[0].style, 'danger', 'Approve & start can spend: red'); n++;
assert.ok(/\$39\.00/.test(act.elements[0].confirm.text.text) && /Total charged when it runs: \$39\.00/.test(awaiting[0].text.text)); n++;

const done = jobBlocks({ job_id: 'abcdef0123', client: 'Melior', domains: ['x.co'], provider: 'GOOGLE', status: 'done', cost: { lines: [], total_now_usd: 0 } });
assert.ok(!done.find(b => b.type === 'actions'), 'no approve button once running/done'); n++;
assert.ok(/:x:/.test(jobBlocks({ error: 'x.co is not confirmed available' })[0].text.text)); n++;
console.log(n + ' passed');

// Slots per provider; "used" only for the whole account (Zapmail counts it that way). Live 2026-10-13 showed "27/15".
{
  const { prewarmedText } = require('./zapmail_actions');
  const sub = (n, p) => ({ plan: 'Growth Addon', price: p, mailboxes: n, renews: '2026-10-18' });
  const t = prewarmedText({ prewarmed: { stock: { google: 460, microsoft: 52 }, accounts: { PRECISE_LEADS: {
    GOOGLE: { slots: 15, assigned: null, assigned_all: 27, slots_all: 27, free_all: 0, subscriptions: [sub(15, 105)] },
    MICROSOFT: { slots: 12, assigned: null, assigned_all: 27, slots_all: 27, free_all: 0, subscriptions: [sub(12, 84)] } } }, for_sale: {} } });
  assert.ok(/\*PRECISE_LEADS\* Google: 15 slots · Growth Addon \$105\/mo × 15/.test(t), t); n++;
  assert.ok(/\*PRECISE_LEADS\* Outlook: 12 slots/.test(t) && !/27\/15/.test(t), t); n++;
  assert.strictEqual((t.match(/27 of 27 pre-warmed slots used/g) || []).length, 1, t); n++;
  console.log('prewarmed slots: 3 passed');
}
