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
assert.strictEqual(assetLine(zap), '  📧 jennifer@joinbettrdata.com · Zapmail'); n++;

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
