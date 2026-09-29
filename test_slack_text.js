// Error texts a teammate can act on; renewal menu names the domain + wallet.
// Run: node test_slack_text.js
const assert = require('assert');
const { plainError } = require('./slack_text');
const { renewalBlocks } = require('./zapmail_actions');

let n = 0;
// The two real refusals from the 29 Sep test run.
assert.ok(/switched off.*nothing was charged/i.test(
  plainError('refusing to spend: execute_one needs approve=True AND ZAPMAIL_ALLOW_SPEND=true.'))); n++;
assert.ok(/switched off/.test(
  plainError("ERROR: refusing to renew: ZAPMAIL_ALLOW_SPEND is not 'true'."))); n++;
assert.ok(/no Smartlead account connected/.test(plainError('FAILED — no export target set for this client'))); n++;
assert.strictEqual(plainError('ERROR: something new'), 'something new'); n++;   // unknown: passed through
assert.ok(plainError('').length > 0); n++;

const blocks = renewalBlocks({ renewals: [{
  domain: 'gomelior.com', domain_id: '050da6e0-82a7-4e9d-834b-7b45d9735666',
  client: 'Melior', account: 'PRECISE_LEADS', expire_on: '2026-10-29' }] });
const renew = blocks[1].accessory.options[0].value.split('|');
assert.deepStrictEqual(renew, ['renew', '050da6e0-82a7-4e9d-834b-7b45d9735666', 'Melior', 'gomelior.com', 'PRECISE_LEADS']); n++;
assert.ok(blocks[1].accessory.options.every(o => o.value.length <= 150)); n++;
const auto = blocks[1].accessory.options[1].value.split('|');
assert.deepStrictEqual(auto.slice(0, 4), ['autorenew', '050da6e0-82a7-4e9d-834b-7b45d9735666', 'Melior', 'gomelior.com']); n++;
const { domainBlocks } = require('./zapmail_actions');
const empty = domainBlocks({ hits: [{ domain: 'gomelior.com', client: 'Melior', account: 'PRECISE_LEADS', status: 'ACTIVE', health: { score: 0, label: 'critical' }, mailboxes: [] }] });
assert.ok(/health n\/a \(no mailboxes yet\)/.test(empty[0].text.text) && !/rotating_light/.test(empty[0].text.text)); n++;
console.log(n + ' passed');

