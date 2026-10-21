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

// Keys never reach Slack, and HTTP errors read as one sentence.
{
  const sl401 = plainError("HTTPStatusError: Client error '401 Unauthorized' for url 'https://server.smartlead.ai/api/v1/email-accounts/?api_key=SECRETKEY123&offset=0&limit=100'");
  assert.ok(!/SECRETKEY123|api_key|https?:/.test(sl401), 'no key or URL: ' + sl401); n++;
  assert.ok(/Smartlead rejected an API key/.test(sl401) && /SMARTLEAD_IGNORE_ACCOUNTS/.test(sl401), sl401); n++;
  const html = plainError('502 on /organizations: <html><head><title>502 Bad Gateway</title></head><body>nginx</body></html>');
  assert.ok(!/<html|nginx/.test(html) && /502/.test(html) && /try again/i.test(html), html); n++;
  const sm = plainError("Server error '503 Service Unavailable' for url 'https://server.scaledmail.com/api/v1/orders'");
  assert.ok(/ScaledMail/.test(sm) && /503/.test(sm) && !/https?:/.test(sm), sm); n++;
  const passthrough = plainError('odd failure with Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxIn0.abcDEF and token=xoxb-123-456-abc');
  assert.ok(!/eyJhbGci|xoxb-123/.test(passthrough) && /odd failure/.test(passthrough), passthrough); n++;
}

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
// Provider dates read as people write them (natural spacing, 21 Oct 2026).
{
  const { day } = require('./slack_text');
  const now = new Date('2026-10-21T10:00:00Z');
  assert.strictEqual(day('2026-11-08', now), '8 Nov'); n++;
  assert.strictEqual(day('2026-11-08T08:26:00.000Z', now), '8 Nov'); n++;
  assert.strictEqual(day('2027-07-06', now), '6 Jul 2027'); n++;
  assert.strictEqual(day('', now), ''); n++;
  assert.strictEqual(day('next week', now), 'next week'); n++;
  const { boldTitle } = require('./slack_text');
  assert.strictEqual(boldTitle('⚠️ Kill warnings, 13 Oct'), '⚠️ *Kill warnings, 13 Oct*'); n++;
  assert.strictEqual(boldTitle(':rotating_light: Key alert'), ':rotating_light: *Key alert*'); n++;
  assert.strictEqual(boldTitle('Volume review'), '*Volume review*'); n++;
  for (const keep of ['🩺 *Already bold*', '• a@x.com', '> quote', '`code`', '<https://x|Link>', '']) {
    assert.strictEqual(boldTitle(keep), keep); n++;
  }
}
console.log(n + ' passed');
