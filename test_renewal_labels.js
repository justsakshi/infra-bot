// Renewal alerts say when a date is a guess. Run: node test_renewal_labels.js
const assert = require('assert');
const { expirySource, assetLine, estimatedNote } = require('./renewal_labels');

let n = 0;
const inboxkit = { type: 'DOMAIN', name: 'prospectcube.com', provider: 'Inboxkit',
  purchaseDate: new Date('2025-09-29'), expiryDate: null };
const zap = { type: 'INBOX', name: 'jennifer@joinbettrdata.com', provider: 'Zapmail',
  purchaseDate: new Date('2026-07-29'), expiryDate: new Date('2026-09-30') };

assert.strictEqual(expirySource(inboxkit), 'estimated'); n++;
assert.strictEqual(expirySource(zap), 'saved'); n++;
assert.strictEqual(expirySource({ type: 'DOMAIN', name: 'x.com' }), null); n++;

const l1 = assetLine(inboxkit);
assert.ok(l1.includes('prospectcube.com · Inboxkit'), l1);
assert.ok(/estimated: no expiry saved, bought 29 Sep 2025 \+ 1 year\. Check Inboxkit/.test(l1), l1); n++;
assert.strictEqual(assetLine(zap), '📧 jennifer@joinbettrdata.com · Zapmail'); n++;

assert.strictEqual(estimatedNote([zap]), ''); n++;
assert.ok(/all estimated/.test(estimatedNote([inboxkit, inboxkit]))); n++;
assert.ok(/1 estimated/.test(estimatedNote([inboxkit, zap]))); n++;
console.log(n + ' passed');

// Renewal review flags show in the Daily Renewal Check.
{
  const { assetLine } = require('./renewal_labels');
  const drop = assetLine({ type: 'INBOX', name: 'a@x.com', provider: 'Scaledmail', expiryDate: new Date(), renewalFlag: 'DROP', renewalFlagReason: '47% inbox' });
  require('assert').ok(/don't renew\*: 47% inbox/.test(drop), 'DROP flag shown');
  const chk = assetLine({ type: 'INBOX', name: 'b@x.com', expiryDate: new Date(), renewalFlag: 'CHECK', renewalFlagReason: 'no test' });
  require('assert').ok(/check before renewing: no test/.test(chk), 'CHECK flag shown');
  require('assert').ok(!/renew/.test(assetLine({ type: 'INBOX', name: 'c@x.com', expiryDate: new Date() })), 'no flag, no note');
  console.log('flags: 3 passed');
}

// Daily renewal check layout (21 Oct 2026: natural spacing). Button ids and
// values must stay exactly as before: *Read more* re-reads the label from the value.
{
  const assert2 = require('assert');
  const dayjs = require('dayjs');
  const { renewalCheckBlocks, expiringDetail, reminderText } = require('./renewal_labels');
  const a = { type: 'DOMAIN', name: 'x.com', provider: 'Zapmail', client: 'Melior', expiryDate: new Date() };
  const groups = [{ assets: [a, { ...a, type: 'INBOX', name: 'r@x.com' }], label: 'Today', icon: '🔴', key: 'noon_0', daysFilter: [0] },
    { assets: [], label: 'Tomorrow (Thu 22 Oct)', icon: '🟠', key: 'noon_1', daysFilter: [1] }];
  const out = renewalCheckBlocks(groups, { channel: 'C1', baseUrl: 'https://h', now: dayjs('2026-10-21') });
  assert2.strictEqual(out.text, 'Daily Renewal Check, 21 Oct');
  assert2.strictEqual(out.blocks[0].text.text, '⏰ Daily Renewal Check, 21 Oct');
  const s = out.blocks[1];
  assert2.strictEqual(s.text.text, '🔴 *1 domain* and *1 inbox* expire *today*');
  assert2.strictEqual(s.accessory.action_id, 'view_details_noon_0');
  assert2.deepStrictEqual(JSON.parse(s.accessory.value), { key: 'noon_0', channel: 'C1', daysFilter: [0], label: 'Today' });
  assert2.ok(!out.blocks.some(b => b.type === 'divider'), 'no divider lines between one-line groups');
  assert2.strictEqual(out.blocks.length, 3, 'header, one group, link');
  assert2.ok(/Nothing expires/.test(JSON.stringify(renewalCheckBlocks([groups[1]], { channel: 'C1', baseUrl: 'https://h' }).blocks)));
  assert2.strictEqual(expiringDetail('Today', groups[0].assets),
    '*Expiring today*\n1 domain · 1 inbox\n\n*Melior*\n🌐 x.com · Zapmail\n📧 r@x.com · Zapmail');
  assert2.ok(/<https:\/\/h\/\|Open dashboard>/.test(reminderText('https://h')));
  console.log('renewal check layout: 8 passed');
}
