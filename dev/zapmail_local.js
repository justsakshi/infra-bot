/**
 * Local test harness for the Zapmail Slack features — ONLY /domains + /zapmail
 * (the separate "domains" Slack app) and the Zapmail webhook route.
 *
 * Unlike `node index.js` it starts no crons, no main Infra Bot app and no
 * startup sheet sync, so testing never duplicates production jobs.
 *
 *   node dev/zapmail_local.js
 *
 * Notes:
 *  - Uses infra-bot/.env. Spawned Python uses smartlead_sync/.venv unless
 *    INFRABOT_PYTHON is set.
 *  - Slack: runs on a SEPARATE dev Slack app (dev/slack_dev_app_manifest.yml)
 *    with commands /domains-dev and /zapmail-dev, from DEV_DOMAINS_SLACK_BOT_TOKEN
 *    and DEV_DOMAINS_SLACK_APP_TOKEN. Render keeps serving /domains and
 *    /zapmail untouched — no need to suspend it.
 *    Sharing Render's DOMAINS_SLACK_* tokens instead makes Slack split events
 *    between the two processes at random; that needs DEV_USE_PROD_SLACK=true
 *    and Render suspended.
 *  - Staged purchase plans go to a TEST ledger collection unless you set
 *    ZAPMAIL_BATCH_COLLECTION yourself.
 *  - Webhooks listen on http://localhost:${DEV_PORT || 10099}/webhooks/zapmail/<account>;
 *    use dev/send_test_webhook.js to send signed test events.
 */
const path = require('path');
const ROOT = path.join(__dirname, '..');
require('dotenv').config({ path: path.join(ROOT, '.env') });

process.env.INFRABOT_PYTHON = process.env.INFRABOT_PYTHON
  || path.join(ROOT, 'smartlead_sync', '.venv', process.platform === 'win32' ? 'Scripts' : 'bin',
    process.platform === 'win32' ? 'python.exe' : 'python');
process.env.ZAPMAIL_BATCH_COLLECTION = process.env.ZAPMAIL_BATCH_COLLECTION || 'zapmail_domain_batches_selftest';
process.env.ZAPMAIL_WEBHOOK_SECRET_PRECISE_LEADS = process.env.ZAPMAIL_WEBHOOK_SECRET_PRECISE_LEADS || 'local-test-secret';

// Slack: the dev app's tokens, never Render's, unless explicitly asked.
let slackMode;
if (process.env.DEV_DOMAINS_SLACK_BOT_TOKEN && process.env.DEV_DOMAINS_SLACK_APP_TOKEN) {
  process.env.DOMAINS_SLACK_BOT_TOKEN = process.env.DEV_DOMAINS_SLACK_BOT_TOKEN;
  process.env.DOMAINS_SLACK_APP_TOKEN = process.env.DEV_DOMAINS_SLACK_APP_TOKEN;
  process.env.DOMAINS_SLASH_COMMAND = process.env.DOMAINS_SLASH_COMMAND || '/domains-dev';
  process.env.ZAPMAIL_SLASH_COMMAND = process.env.ZAPMAIL_SLASH_COMMAND || '/zapmail-dev';
  slackMode = 'dev app — use ' + process.env.DOMAINS_SLASH_COMMAND + ' and ' + process.env.ZAPMAIL_SLASH_COMMAND;
} else if (process.env.DEV_USE_PROD_SLACK === 'true') {
  slackMode = 'PRODUCTION app tokens — Render must be suspended or events split between the two';
} else {
  delete process.env.DOMAINS_SLACK_BOT_TOKEN;
  delete process.env.DOMAINS_SLACK_APP_TOKEN;
  slackMode = 'off — set DEV_DOMAINS_SLACK_BOT_TOKEN + DEV_DOMAINS_SLACK_APP_TOKEN (see dev/slack_dev_app_manifest.yml)';
}

const express = require('express');
const { startDomainsApp } = require('../domains_command');
const { registerZapmailWebhook } = require('../zapmail_webhooks');

const onTrackerChanged = async () => console.log('[dev] tracker changed — the sheet re-sync runs in index.js on Render; skipped locally');

(async () => {
  console.log('[dev] slack:', slackMode);
  console.log('[dev] python:', process.env.INFRABOT_PYTHON);
  console.log('[dev] ledger collection:', process.env.ZAPMAIL_BATCH_COLLECTION);
  console.log('[dev] approvers:', process.env.ZAPMAIL_APPROVERS || process.env.ZAPMAIL_BUY_APPROVERS || '(none — action buttons will refuse)');
  console.log('[dev] spend switch:', process.env.ZAPMAIL_ALLOW_SPEND === 'true' ? 'ON (real money!)' : 'off');
  console.log('[dev] tracker writes:', process.env.ZAPMAIL_ASSET_SYNC_ENABLED === 'true' ? 'ON' : 'off (webhooks won’t write /infra)');

  const web = express();
  registerZapmailWebhook(web, express, { baseDir: ROOT, onTrackerChanged });
  const port = Number(process.env.DEV_PORT || 10099);
  web.listen(port, () => console.log('[dev] webhooks on http://localhost:' + port + '/webhooks/zapmail/<account>'));

  const app = await startDomainsApp(ROOT, { onTrackerChanged });
  if (!app) console.log('[dev] Slack not started — webhooks only');
})();
