// "Whose are these?" buttons: an answer sets the domain up and the DM says what happened.
// Run: node test_intake_slack.js   (Python stubbed; no Slack, no network)
const assert = require('assert');
let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

const calls = [];
const flow = require('./domain_suggest_command');
flow.runPy = async (args) => {
  calls.push(args.join(' '));
  if (args.includes('skip')) return { domain: 'gone.com', client: 'skip', set_up: 0, problems: [] };
  return { domain: 'mystery.com', client: 'Melior', set_up: 3, ready_on: '2026-10-29', problems: [] };
};
const h = {};
require('./intake_slack').registerIntakeSlack({ action: (id, fn) => { h[String(id)] = fn; } }, __dirname);
const handler = h[Object.keys(h).find(k => k.includes('intake_client'))];

(async () => {
  const out = [];
  const respond = async m => out.push(m);
  await handler({ ack: async () => {}, action: { action_id: 'intake_client_melior', value: 'mystery.com' }, body: { user: { id: 'U1' } }, respond });
  ok(calls[0] === 'inbox_intake_cli.py --answer mystery.com melior --by U1', calls[0]);
  ok(out[0].replace_original && /Setting up \*mystery\.com\* for Melior/.test(out[0].text), 'working message first');
  ok(out[1].replace_original && /:white_check_mark: Set up 3 inboxes on \*mystery\.com\* for Melior, ready to use from 29 Oct/.test(out[1].text), out[1].text);

  await handler({ ack: async () => {}, action: { action_id: 'intake_client_skip', value: 'gone.com' }, body: { user: { id: 'U1' } }, respond });
  ok(/\*gone\.com\* is not ours/.test(out[out.length - 1].text), 'skip says it will not ask again');

  const before = calls.length;
  await handler({ ack: async () => {}, action: { action_id: 'intake_client_pl', value: 'bad domain; rm -rf' }, body: { user: { id: 'U1' } }, respond });
  ok(calls.length === before, 'a value that is not a domain never reaches Python');
  console.log(n + ' passed');
})().catch(e => { console.error(e); process.exit(1); });
