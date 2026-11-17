// Zapmail webhook summaries. Run: node test_zapmail_webhooks.js
const assert = require('assert');
const { summarizeEvent } = require('./zapmail_webhooks');
let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

const live = summarizeEvent({ type: 'subscription.status_changed', data: {
  subscriptionId: 'sub_1', subscriptionStatus: 'ACTIVE', uniquePlanKey: 'zap_google_growth_monthly',
  totalMailboxQuantity: 21, price: 68.25, periodEnd: '2026-11-08T08:26:00.000Z' } });
ok(/21 mailboxes/.test(live.text) && /ACTIVE/.test(live.text) && /next bill 8 Nov/.test(live.text) && !/2026-11-08/.test(live.text) && !/\?/.test(live.text), 'reads Zapmail field names');
ok(live.alert === false, 'a normal renewal is not an alert');
const failed = summarizeEvent({ type: 'subscription.status_changed', data: { subscription: { id: 's', status: 'past_due', paymentFailureMessage: 'card declined' } } });
ok(failed.alert && /PAST_DUE/.test(failed.text) && /card declined/.test(failed.text), 'failed payment alerts');
const empty = summarizeEvent({ type: 'subscription.status_changed', data: {} });
ok(!/\*\?\*/.test(empty.text) && empty.alert === false, 'unknown shape: no "?" spam, no alert');
// 17 Nov 2026: with the digest on, a subscription failure is billing watch's urgent item (one post per bill), and a
// finished Zapmail placement test asks nothing; an export failure still posts at once and says what to do.
const { postsNow } = require('./zapmail_webhooks');
ok(!postsNow('subscription.status_changed', failed, {}), 'digest on: billing watch posts the failed payment, not the webhook');
ok(postsNow('subscription.status_changed', failed, { DIGEST_MODE: 'off' }), 'digest off: the webhook posts as before');
const done = summarizeEvent({ type: 'placement_test.status_changed', data: { id: 't1', status: 'COMPLETED' } });
ok(done.alert && !postsNow('placement_test.status_changed', done, {}), 'a finished placement test is not posted');
const exp = summarizeEvent({ type: 'export.failed', data: { app_name: 'Smartlead', export_id: 'e1', error: 'auth', mailboxes: [{ email: 'a@x.com' }] } });
ok(postsNow('export.failed', exp, {}) && /a@x\.com/.test(exp.text) && /run it again/.test(exp.text), 'export failure names the inboxes and the step');
console.log(n + ' passed');
