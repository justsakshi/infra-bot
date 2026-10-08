/**
 * Zapmail → Infrabot webhooks: Zapmail pushes changes, Infrabot reacts.
 *
 *   POST /webhooks/zapmail/:account   (account = "precise_leads", "primary", …)
 *
 * Every delivery is verified before anything is trusted: header
 * `X-Zapmail-Signature: t=<unix>,v1=<hex>`, v1 = HMAC-SHA256(secret,
 * `${t}.${rawBody}`), timestamp within 5 minutes, constant-time compare.
 * Secrets: ZAPMAIL_WEBHOOK_SECRET_<ACCOUNT> (ZAPMAIL_WEBHOOK_SECRET for
 * "primary") — printed once by `zapmail_webhooks.py --register`. No secret
 * configured → the route answers 503 and does nothing.
 *
 * Zapmail wants a fast 2xx (10s timeout, retries 1m…24h, 20 failures disable
 * the endpoint), so we answer first and work after:
 *   - failures / status changes / finished placement tests / billing changes
 *     → a Slack alert (only if ZAPMAIL_NOTIFY_CHANNEL is set; else logged)
 *   - domain / mailbox changes → refresh that domain in the /infra tracker
 *     (only if ZAPMAIL_ASSET_SYNC_ENABLED=true), then the tracker sheet.
 */

const crypto = require('crypto');
const path = require('path');
const { spawn } = require('child_process');

const TOLERANCE_S = 5 * 60;
const SEEN_MAX = 1000;
const ACCOUNT_RE = /^[a-z0-9_]{1,40}$/;
const DOMAIN_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$/;

function secretFor(account) {
  if (!ACCOUNT_RE.test(account || '')) return null;
  if (account === 'primary') return process.env.ZAPMAIL_WEBHOOK_SECRET || null;
  return process.env['ZAPMAIL_WEBHOOK_SECRET_' + account.toUpperCase()] || null;
}

/** True only for a correctly signed, fresh delivery. */
function verifySignature(rawBody, header, secret, nowS = Math.floor(Date.now() / 1000)) {
  if (!header || !secret || !Buffer.isBuffer(rawBody)) return false;
  const parts = {};
  for (const kv of String(header).split(',')) {
    const i = kv.indexOf('=');
    if (i > 0) parts[kv.slice(0, i).trim()] = kv.slice(i + 1).trim();
  }
  const t = Number(parts.t);
  if (!Number.isFinite(t) || Math.abs(nowS - t) > TOLERANCE_S || !parts.v1) return false;
  const expected = crypto.createHmac('sha256', secret)
    .update(String(parts.t) + '.').update(rawBody).digest('hex');
  const a = Buffer.from(expected, 'hex');
  const b = Buffer.from(String(parts.v1), 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/** What an event means for us: {text, domain, alert}. Pure. */
function summarizeEvent(evt) {
  const type = String((evt && evt.type) || '');
  const d = (evt && evt.data) || {};
  // Zapmail's event names are domain.updated / domain.connection_status_changed
  // / mailbox.updated (a first guess of *.status_changed was rejected at
  // registration). The payload shape is read defensively: details object if
  // present, else top-level fields.
  const prevOf = () => (d.previousState || {}).status || d.previous_status || d.previousStatus;
  if (type.startsWith('domain.')) {
    const x = d.domainDetails || d.domain || d;
    const name = x.domain || x.domainName || d.domainName;
    const status = x.status || x.connectionStatus || d.status || d.connection_status;
    const prev = prevOf();
    const score = x.healthScore;
    const critical = typeof score === 'number' && score <= 30 && (x.assignedMailboxesCount || 0) > 0;
    return {
      domain: name, alert: Boolean(prev && status && prev !== status) || critical,
      text: (critical ? ':rotating_light: ' : ':globe_with_meridians: ') + '`' + (name || '?') + '`'
        + (status ? ' is now *' + status + '*' : ' was updated') + (prev ? ' (was ' + prev + ')' : '')
        + (typeof score === 'number' ? ' · health ' + score + (critical ? ' — CRITICAL' : '') : '')
    };
  }
  if (type.startsWith('mailbox.')) {
    const x = d.mailboxDetails || d.mailbox || d;
    const email = x.email || (x.username && x.domain ? x.username + '@' + x.domain : '?');
    const status = x.status || d.status;
    const failed = String(status || '').toUpperCase() === 'FAILED';
    const prev = prevOf();
    return {
      domain: x.domain || String(email).split('@')[1], alert: failed,
      text: (failed ? ':x: ' : ':envelope: ') + 'Mailbox `' + email + '`'
        + (status ? ' is *' + status + '*' : ' was updated') + (prev ? ' (was ' + prev + ')' : '')
    };
  }
  if (type === 'export.completed') {
    return {
      domain: null, alert: false,
      text: ':white_check_mark: Zapmail export to *' + (d.app_name || '?') + '* completed (export ' + d.export_id + ')'
        + ((d.mailboxes || []).length ? ' · ' + d.mailboxes.length + ' mailbox(es)' : '')
    };
  }
  if (type === 'subscription.status_changed') {
    // 2026-10-08 live: our first reading found none of the fields and posted
    // "subscription ? is now *?*". Read every spelling Zapmail uses elsewhere
    // (GET /v2/subscriptions: subscriptionId, subscriptionStatus, plan,
    // totalMailboxQuantity, price, periodEnd) and log the keys when still unknown.
    const x = d.subscriptionDetails || d.subscription || d;
    const id = x.subscriptionId || x.subscription_id || x.id || d.subscription_id;
    const status = String(x.subscriptionStatus || x.status || d.status || '').toUpperCase();
    const boxes = x.totalMailboxQuantity || x.mailboxQuantity;
    const what = [x.uniquePlanKey || x.plan, boxes ? boxes + ' mailboxes' : '', x.price ? '$' + x.price + '/mo' : '']
      .filter(Boolean).join(', ');
    const renews = String(x.periodEnd || x.period_end || '').slice(0, 10);
    const failure = x.paymentFailureMessage || d.paymentFailureMessage;
    if (!status) console.log('[zapmail-webhook] subscription event without a status; keys: ' + Object.keys(x).join(','));
    return {
      // Renewing normally (ACTIVE) is not news; a failed payment or a stop is.
      domain: null, alert: Boolean(failure) || Boolean(status && status !== 'ACTIVE'),
      text: ':credit_card: Zapmail subscription ' + (what || id || '(no details)')
        + (status ? ' is now *' + status + '*' : ' changed') + (prevOf() ? ' (was ' + prevOf() + ')' : '')
        + (renews ? ' · next bill ' + renews : '') + (failure ? ' · :x: payment failed: ' + failure : '')
    };
  }
  if (type === 'export.failed') {
    return {
      domain: null, alert: true,
      text: ':x: Zapmail export to *' + (d.app_name || '?') + '* failed (export ' + d.export_id + '): '
        + (d.error || 'no reason given') + ((d.mailboxes || []).length ? ' · ' + d.mailboxes.length + ' mailbox(es)' : '')
    };
  }
  if (type === 'placement_test.status_changed') {
    const done = ['COMPLETED', 'FAILED'].includes(String(d.status || '').toUpperCase());
    return {
      domain: null, alert: done,
      text: ':test_tube: Placement test ' + d.id + ' is *' + d.status + '*'
        + (done ? ' — results in the Zapmail app, or `zapmail_placement.py --report <order id>`' : '')
    };
  }
  if (type === 'subscription.billing_changed') {
    const p = (d.changes || {}).price || {};
    return {
      domain: null, alert: true,
      text: ':credit_card: Zapmail subscription ' + d.subscription_id + ' billing changed'
        + (p.previous !== undefined ? ': ' + p.previous + ' → ' + p.current : '')
    };
  }
  return { domain: null, alert: false, text: 'Zapmail event ' + type };
}

async function postSlack(text) {
  const channel = process.env.ZAPMAIL_NOTIFY_CHANNEL;
  const token = process.env.DOMAINS_SLACK_BOT_TOKEN || process.env.SLACK_BOT_TOKEN;
  if (!channel || !token || typeof fetch !== 'function') {
    console.log('[zapmail-webhook] (not posted) ' + text);
    return;
  }
  try {
    const r = await fetch('https://slack.com/api/chat.postMessage', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json; charset=utf-8', Authorization: 'Bearer ' + token },
      body: JSON.stringify({ channel, text, unfurl_links: false })
    });
    const j = await r.json();
    if (!j.ok) console.warn('[zapmail-webhook] Slack error:', j.error);
  } catch (err) {
    console.warn('[zapmail-webhook] Slack post failed:', err.message);
  }
}

function refreshTracker(domain, baseDir, onTrackerChanged) {
  if (process.env.ZAPMAIL_ASSET_SYNC_ENABLED !== 'true' || !DOMAIN_RE.test(domain || '')) return;
  const python = process.env.INFRABOT_PYTHON || 'python';
  const proc = spawn(python, ['zapmail_asset_sync.py', '--apply', '--domain', domain], {
    cwd: path.join(baseDir, 'smartlead_sync'),
    env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
  });
  proc.stdout.on('data', d => process.stdout.write('[zapmail-sync] ' + d));
  proc.stderr.on('data', d => process.stderr.write('[zapmail-sync] ' + d));
  proc.on('close', code => {
    if (code === 0 && typeof onTrackerChanged === 'function') {
      Promise.resolve(onTrackerChanged()).catch(err =>
        console.warn('[zapmail-sync] sheet refresh failed:', err.message));
    }
  });
}

/**
 * Mount the route. Call BEFORE `expressApp.use(express.json())` — signature
 * checks need the raw bytes.
 */
function registerZapmailWebhook(expressApp, express, { baseDir, onTrackerChanged } = {}) {
  const seen = new Set();
  expressApp.post('/webhooks/zapmail/:account', express.raw({ type: '*/*', limit: '1mb' }), (req, res) => {
    const account = String(req.params.account || '').toLowerCase();
    const secret = secretFor(account);
    if (!secret) return res.status(503).send('webhook not configured');
    if (!verifySignature(req.body, req.get('X-Zapmail-Signature'), secret)) {
      console.warn('[zapmail-webhook] rejected unsigned/invalid delivery for ' + account);
      return res.status(401).send('bad signature');
    }
    let evt;
    try { evt = JSON.parse(req.body.toString('utf8')); } catch (e) { return res.status(400).send('bad json'); }
    res.status(200).send('ok');
    if (evt.id && seen.has(evt.id)) return;          // Zapmail retries: handle once
    if (evt.id) {
      seen.add(evt.id);
      if (seen.size > SEEN_MAX) seen.delete(seen.values().next().value);
    }
    setImmediate(async () => {
      const s = summarizeEvent(evt);
      console.log('[zapmail-webhook] ' + account + ' ' + evt.type + ': ' + s.text);
      if (s.alert) await postSlack(s.text);
      if (s.domain) refreshTracker(String(s.domain).toLowerCase(), baseDir, onTrackerChanged);
      // A change Zapmail reports can unblock an inbox setup job: move jobs now
      // rather than at the next 10-minute tick (grouped, one run at a time).
      const jobs = require('./inbox_jobs_notify');
      if (jobs.eventMovesJobs(evt.type)) jobs.nudge(baseDir);
    });
  });
}

module.exports = { registerZapmailWebhook, verifySignature, summarizeEvent, secretFor };
