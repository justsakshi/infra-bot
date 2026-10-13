// Zapmail status: domains per provider from each row, one mailbox total (Zapmail
// ignores the provider header on lists, live 2026-10-13). Run: node test_zapmail_status_total.js
const assert = require('assert');
const { formatStatus } = require('./zapmail_command');

const s = { PRECISE_LEADS: { ok: true, wallet_balance: 38.5, auto_recharge: true, active_plan: 'Growth',
  domains: { GOOGLE: 193, MICROSOFT: 20 }, active_mailboxes: {}, active_mailboxes_total: 69, placement_credits: 31 } };
const out = formatStatus(s);
const t = typeof out === 'string' ? out : JSON.stringify(out);
assert.match(t, /213 domains/);
assert.match(t, /69 mailboxes/);
assert.match(t, /Google 193 domains/);
assert.match(t, /Outlook 20 domains/);
assert.ok(!/\? mailboxes/.test(t), 'no "?" counts');
console.log('5 passed');
