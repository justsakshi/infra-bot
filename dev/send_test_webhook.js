/**
 * Send a signed test Zapmail webhook to the local harness (or any URL).
 *
 *   node dev/send_test_webhook.js mailbox-failed
 *   node dev/send_test_webhook.js domain-critical  [domain]
 *   node dev/send_test_webhook.js export-failed
 *   node dev/send_test_webhook.js placement-done
 *   node dev/send_test_webhook.js bad-signature
 *
 * Env: WEBHOOK_URL (default http://localhost:10099/webhooks/zapmail/precise_leads),
 *      WEBHOOK_SECRET (default: the harness's local-test-secret).
 * Expect 200 "ok" (bad-signature → 401) and a line in the harness log; with
 * ZAPMAIL_NOTIFY_CHANNEL set the alert also lands in Slack.
 */
const crypto = require('crypto');

const kind = process.argv[2] || 'mailbox-failed';
const domain = process.argv[3] || 'askbettrdata.com';
const url = process.env.WEBHOOK_URL || 'http://localhost:10099/webhooks/zapmail/precise_leads';
const secret = process.env.WEBHOOK_SECRET || 'local-test-secret';

const events = {
  'mailbox-failed': { type: 'mailbox.updated', data: { mailboxDetails: { username: 'test', domain, status: 'FAILED' }, previousState: { status: 'IN_PROGRESS' } } },
  'domain-critical': { type: 'domain.updated', data: { domainDetails: { domain, status: 'ACTIVE', healthScore: 12, assignedMailboxesCount: 3 }, previousState: { status: 'PENDING' } } },
  'export-failed': { type: 'export.failed', data: { export_id: '12345', app_name: 'SMARTLEAD', export_status: 'FAILED', mailboxes: [], error: 'Mailbox is expired (test)' } },
  'placement-done': { type: 'placement_test.status_changed', data: { id: 'test-order', previous_status: 'SCHEDULED', status: 'COMPLETED' } }
};

(async () => {
  const evt = events[kind === 'bad-signature' ? 'mailbox-failed' : kind];
  if (!evt) { console.error('unknown kind: ' + kind); process.exit(2); }
  const body = JSON.stringify({ id: 'evt_test_' + Date.now(), created: Math.floor(Date.now() / 1000), api_version: '2026-07-01', ...evt });
  const t = Math.floor(Date.now() / 1000);
  const v1 = crypto.createHmac('sha256', kind === 'bad-signature' ? 'wrong-secret' : secret).update(t + '.' + body).digest('hex');
  const res = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Zapmail-Signature': 't=' + t + ',v1=' + v1 }, body });
  console.log(res.status, await res.text());
})();
