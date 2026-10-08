// Zapmail webhook summaries. Run: node test_zapmail_webhooks.js
const assert = require('assert');
const { summarizeEvent } = require('./zapmail_webhooks');
let n = 0;
const ok = (c, m) => { assert.ok(c, m); n++; };

const live = summarizeEvent({ type: 'subscription.status_changed', data: {
  subscriptionId: 'sub_1', subscriptionStatus: 'ACTIVE', uniquePlanKey: 'zap_google_growth_monthly',
  totalMailboxQuantity: 21, price: 68.25, periodEnd: '2026-11-08T08:26:00.000Z' } });
ok(/21 mailboxes/.test(live.text) && /ACTIVE/.test(live.text) && /2026-11-08/.test(live.text) && !/\?/.test(live.text), 'reads Zapmail field names');
ok(live.alert === false, 'a normal renewal is not an alert');
const failed = summarizeEvent({ type: 'subscription.status_changed', data: { subscription: { id: 's', status: 'past_due', paymentFailureMessage: 'card declined' } } });
ok(failed.alert && /PAST_DUE/.test(failed.text) && /card declined/.test(failed.text), 'failed payment alerts');
const empty = summarizeEvent({ type: 'subscription.status_changed', data: {} });
ok(!/\*\?\*/.test(empty.text) && empty.alert === false, 'unknown shape: no "?" spam, no alert');
console.log(n + ' passed');
