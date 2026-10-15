// Weekly volume review: the Apply button (approvers only) runs the CLI and says what changed.
// Run: node test_volume_slack.js   (Python stubbed; no Slack, no network)
const assert = require('assert');
let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

process.env.ZAPMAIL_APPROVERS = 'UAPP';
const calls = [];
require('./domain_suggest_command').runPy = async (args) => {
  calls.push(args);
  return { client: 'Precise Leads', changed: ['a@pl.com 15 -> 17'], skipped: [], failed: [],
    text: ':white_check_mark: Precise Leads: changed the daily limit on 1 inbox (read back from Smartlead).\n• a@pl.com 15 -> 17' };
};
const h = {};
require('./volume_slack').registerVolumeSlack({ action: (id, fn) => { h[String(id)] = fn; } }, __dirname);
const handler = h[Object.keys(h).find(k => k.includes('volume_apply'))];
const value = JSON.stringify({ r: '2026-11-03', c: 'Precise Leads' });

(async () => {
  const out = [];
  const respond = async m => out.push(m);
  await handler({ ack: async () => {}, action: { action_id: 'volume_apply_0', value }, body: { user: { id: 'UNOPE' } }, respond });
  ok(!calls.length && /Only approvers/.test(out[0].text) && out[0].response_type === 'ephemeral', 'non-approver is refused');

  await handler({ ack: async () => {}, action: { action_id: 'volume_apply_0', value }, body: { user: { id: 'UAPP' } }, respond });
  ok(calls[0].join(' ') === 'volume_review.py --apply-client Precise Leads --review 2026-11-03 --by UAPP --json', calls[0].join(' '));
  const last = out[out.length - 1];
  ok(last.response_type === 'in_channel' && !last.replace_original, 'the result is posted for the team');
  ok(/^<@UAPP> applied: :white_check_mark: Precise Leads: changed the daily limit on 1 inbox/.test(last.text), last.text);

  const before = calls.length;
  for (const v of [JSON.stringify({ r: '2026-11-03; rm', c: 'Precise Leads' }), JSON.stringify({ r: '2026-11-03', c: 'Evil' }), 'not json']) {
    await handler({ ack: async () => {}, action: { action_id: 'volume_apply_0', value: v }, body: { user: { id: 'UAPP' } }, respond });
  }
  ok(calls.length === before, 'a bad review id or client never reaches Python');
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
