require('dotenv').config();

process.on('unhandledRejection', (reason) => {
  console.warn('[WARN] Unhandled rejection (process kept alive):', reason?.message || reason);
});
const mongoose = require('mongoose');
const { App } = require('@slack/bolt');
const cron = require('node-cron');
const dayjs = require('dayjs');
const renewalLabels = require('./renewal_labels');
const { spawn } = require('child_process');
const customParseFormat = require('dayjs/plugin/customParseFormat');
const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const { Readable } = require('stream');
const axios = require('axios');
const { syncAllAssetsToSheet } = require('./sheets');
const { startDomainsApp } = require('./domains_command');
const { registerZapmailWebhook } = require('./zapmail_webhooks');

dayjs.extend(customParseFormat);

/* -------------------- Config -------------------- */
const REMINDER_DAYS = [3, 1];
const ESCALATION_THRESHOLD = 3;

/* -------------------- Owner Name → Slack ID Map -------------------- */
const OWNER_MAP = {
  'varsha':      'U0767GZUM8S',
  'anjali':      'U045NBCSA3F',
  'balasankar':  'U091D7REGGN',
  'aravind':     'U03AF9U985V',
  'avinash':     'U026H4M2X09',
  'manveen':     'U09TQ9D7YNM',
  'keerthika':   'U0ACYQDBLJ0',
  'sakshi':      'U09T9FAPJP8'
};

/**
 * Resolve a primaryOwner value to a Slack ID.
 * Accepts a Slack ID directly (starts with U) or a name from OWNER_MAP.
 */
function resolveOwner(value) {
  if (!value) return null;
  const trimmed = value.trim();
  if (trimmed.startsWith('U') && trimmed.length > 5) return trimmed;
  return OWNER_MAP[trimmed.toLowerCase()] || null;
}

const PORT = process.env.PORT || 10000;

/* -------------------- MongoDB -------------------- */
mongoose.set('bufferCommands', false);

/* -------------------- Slack App -------------------- */
// Started in start(); held at module scope so shutdown() can close its socket.
let domainsApp = null;

const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true
});

// Socket-mode's finity state machine throws "Unhandled event 'server explicit
// disconnect' in state 'connecting'" when Slack forces a disconnect (e.g. during
// a deploy overlap where two containers briefly share the app token) while the
// client is mid-reconnect. Attach an error listener so that race is logged, not
// thrown as an unhandled rejection that spams the crash handler.
try {
  const smClient = app.receiver && app.receiver.client;
  if (smClient && typeof smClient.on === 'function') {
    smClient.on('error', (err) => {
      console.warn('⚠️  Slack socket-mode error (auto-reconnecting):', err?.message || err);
    });
    smClient.on('disconnect', () => {
      console.log('ℹ️  Slack socket-mode disconnected — will reconnect');
    });
  }
} catch (err) {
  console.warn('⚠️  Could not attach socket-mode error handler:', err?.message || err);
}

/* -------------------- Asset Schema -------------------- */
const AssetSchema = new mongoose.Schema({
  type: { type: String, enum: ['DOMAIN', 'INBOX'], required: true },
  name: { type: String, required: true, unique: true },

  client: String,
  provider: String,
  workspace: String,
  campaign: String,
  purchaseDate: Date,
  expiryDate: Date,
  status: String,
  brandedPrewarmed: String,
  primaryOwner: String,
  visibilityChannel: String,
  yearlyCost: Number,
  monthlyCost: Number,
  currency: String,
  notes: String,

  domain: String,
  // Set by the renewal review before each bill: DROP (don't pay for it again)
  // or CHECK, with the reason; cleared when the inbox is fine.
  renewalFlag: String,
  renewalFlagReason: String,
  renewalFlagAt: Date,

  remindersSent: [{ daysBefore: Number, sentAt: Date }],
  createdBy: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

AssetSchema.pre('save', function (next) {
  this.updatedAt = new Date();
  next();
});

const Asset = mongoose.model('Asset', AssetSchema);

/* -------------------- CronLock Schema -------------------- */
const CronLockSchema = new mongoose.Schema({
  jobName: { type: String, required: true, unique: true },
  createdAt: { type: Date, expires: '2h', default: Date.now }
});
const CronLock = mongoose.model('CronLock', CronLockSchema);

/* -------------------- Helper Functions -------------------- */

function parseDate(dateString) {
  if (!dateString || dateString.trim() === '') return null;
  const p1 = dayjs(dateString, 'DD/MM/YYYY', true);
  if (p1.isValid()) return p1.toDate();
  const p2 = dayjs(dateString, 'YYYY-MM-DD', true);
  if (p2.isValid()) return p2.toDate();
  const p3 = dayjs(dateString, 'DD MMM YYYY', true);
  if (p3.isValid()) return p3.toDate();
  const p4 = dayjs(dateString, 'D MMM YYYY', true);
  if (p4.isValid()) return p4.toDate();
  return null;
}

function computeDaysLeft(asset) {
  const today = dayjs().startOf('day');
  let expiryDate = asset.expiryDate;

  if (!expiryDate && asset.purchaseDate) {
    if (asset.type === 'DOMAIN') {
      expiryDate = dayjs(asset.purchaseDate).add(365, 'day').toDate();
    } else if (asset.type === 'INBOX') {
      const purchaseDay = dayjs(asset.purchaseDate);
      let nextExpiry = dayjs()
        .year(dayjs().year())
        .month(dayjs().month())
        .date(purchaseDay.date());
      if (!nextExpiry.isAfter(today)) nextExpiry = nextExpiry.add(1, 'month');
      expiryDate = nextExpiry.toDate();
    }
  }

  if (!expiryDate) return null;
  return dayjs(expiryDate).startOf('day').diff(today, 'day');
}

function parseCost(value) {
  if (!value || value === '') return null;
  const match = String(value).match(/[\d,.]+/);
  if (!match) return null;
  const num = Number(match[0].replace(/,/g, ''));
  return isNaN(num) ? null : num;
}

function parseCurrency(value) {
  if (!value || value === '') return 'USD';
  const v = value.trim();
  const symbolMap = { '$': 'USD', '₹': 'INR', '€': 'EUR', '£': 'GBP' };
  if (symbolMap[v]) return symbolMap[v];
  return v.toUpperCase();
}

function escapeRegex(value) {
  return String(value).replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function buildNameQuery(name) {
  return { name: { $regex: new RegExp(`^${escapeRegex(name.trim())}$`, 'i') } };
}

function getWorkingDayTargets() {
  const today = dayjs().startOf('day');
  const dayOfWeek = today.day();

  const targets = new Set();

  const addWorkingDays = (from, n) => {
    let date = from;
    let added = 0;
    while (added < n) {
      date = date.add(1, 'day');
      if (date.day() !== 0 && date.day() !== 6) added++;
    }
    return date;
  };

  const oneWD = addWorkingDays(today, 1);
  const threeWD = addWorkingDays(today, 3);

  targets.add(oneWD.format('YYYY-MM-DD'));
  targets.add(threeWD.format('YYYY-MM-DD'));

  if (dayOfWeek === 4) {
    targets.add(today.add(2, 'day').format('YYYY-MM-DD'));
    targets.add(today.add(3, 'day').format('YYYY-MM-DD'));
  }

  return targets;
}

function prepareAssetsForSheet(assets) {
  return assets.map(a => {
    const obj = a.toObject ? a.toObject() : a;
    obj.daysLeft = computeDaysLeft(obj);
    obj.purchaseDateFormatted = obj.purchaseDate ? dayjs(obj.purchaseDate).format('DD/MM/YYYY') : '';
    obj.expiryDateFormatted = obj.expiryDate ? dayjs(obj.expiryDate).format('DD/MM/YYYY') : '';
    obj.yearlyCostFormatted = obj.yearlyCost !== null && obj.yearlyCost !== undefined ? `${obj.yearlyCost}` : '';
    obj.monthlyCostFormatted = obj.monthlyCost !== null && obj.monthlyCost !== undefined ? `${obj.monthlyCost}` : '';
    return obj;
  });
}

/** Re-read every asset and rewrite the tracker sheet (after a Zapmail sync). */
async function resyncAssetSheet() {
  const allAssets = await Asset.find();
  await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
  console.log('[zapmail-sync] asset sheet re-synced');
}

/* -------------------- CSV Parsing -------------------- */

function parseCSVBuffer(buffer, type) {
  return new Promise((resolve, reject) => {
    const assets = [];
    const stream = Readable.from(buffer.toString());

    stream
      .pipe(csv())
      .on('data', (row) => {
        if (type === 'DOMAIN') {
          const name = row['Domain'] ? row['Domain'].trim() : '';
          if (!name || name.startsWith('(')) return;

          assets.push({
            type: 'DOMAIN',
            name: name.toLowerCase(),
            client: row['Client'] || null,
            provider: row['Provider'] || null,
            workspace: row['Workspace'] || null,
            campaign: row['Campaign'] || null,
            purchaseDate: parseDate(row['Purchase Date (format to DATE)'] || row['Purchase Date']),
            expiryDate: parseDate(row['Expiry Date (format to DATE)'] || row['Expiry Date']),
            status: row['Status'] || 'Active',
            brandedPrewarmed: row['Branded/Pre-warmed'] || null,
            primaryOwner: resolveOwner(row['Primary Owner']),
            visibilityChannel: row['Visibility Channel'] || null,
            yearlyCost: parseCost(row['Yearly Cost']),
            currency: parseCurrency(row['Currency']),
            notes: row['Notes'] || null,
            createdBy: 'CSV_UPLOAD',
            updatedAt: new Date()
          });

        } else if (type === 'INBOX') {
          const name = row['Inbox'] ? row['Inbox'].trim() : '';
          if (!name || name.startsWith('(')) return;

          const inboxAsset = {
            type: 'INBOX',
            name: name.toLowerCase(),
            client: row['Client'] || null,
            provider: row['Provider'] || null,
            workspace: row['Workspace'] || null,
            domain: row['Domain'] || null,
            campaign: row['Campaign'] || null,
            purchaseDate: parseDate(row['Purchase Date (format to DATE)'] || row['Purchase Date']),
            expiryDate: parseDate(row['Expiry Date (format to DATE)'] || row['Expiry Date']),
            status: row['Status'] || 'Active',
            brandedPrewarmed: row['Branded/Pre-warmed'] || null,
            primaryOwner: resolveOwner(row['Primary Owner']),
            visibilityChannel: row['Visibility Channel'] || null,
            monthlyCost: parseCost(row['Monthly Cost']),
            currency: parseCurrency(row['Currency']),
            notes: row['Notes'] || null,
            createdBy: 'CSV_UPLOAD',
            updatedAt: new Date()
          };

          if (!inboxAsset.domain && inboxAsset.name.includes('@')) {
            inboxAsset.domain = inboxAsset.name.split('@')[1];
          }

          assets.push(inboxAsset);
        }
      })
      .on('end', () => resolve(assets))
      .on('error', reject);
  });
}

/* -------------------- Express HTTP Server -------------------- */

const expressApp = express();
const upload = multer({ storage: multer.memoryStorage() });
const path = require('path');

// Zapmail webhooks need the raw body for signature checks, so they mount
// BEFORE the JSON parser. Tracker changes re-sync the asset sheet.
registerZapmailWebhook(expressApp, express, {
  baseDir: __dirname,
  onTrackerChanged: resyncAssetSheet
});

expressApp.use(express.json());
expressApp.use(express.static(path.join(__dirname, 'public')));

/* -------------------- REST API -------------------- */

expressApp.get('/api/assets', async (req, res) => {
  try {
    const assets = await Asset.find().sort({ type: 1, name: 1 });
    const prepared = prepareAssetsForSheet(assets);
    res.json({ assets: prepared });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

expressApp.post('/api/assets', async (req, res) => {
  try {
    const body = req.body;
    const type = body.type;
    const asset = {
      type,
      name: body.name.trim().toLowerCase(),
      client: body.client || null,
      provider: body.provider || null,
      workspace: body.workspace || null,
      campaign: body.campaign || null,
      purchaseDate: parseDate(body.purchaseDate),
      expiryDate: parseDate(body.expiryDate),
      status: body.status || 'Active',
      brandedPrewarmed: body.brandedPrewarmed || null,
      primaryOwner: resolveOwner(body.primaryOwner),
      yearlyCost: type === 'DOMAIN' ? parseCost(body.cost) : null,
      monthlyCost: type === 'INBOX' ? parseCost(body.cost) : null,
      currency: body.currency || 'USD',
      notes: body.notes || null,
      createdBy: 'WEB_UI',
      updatedAt: new Date()
    };
    if (type === 'INBOX' && asset.name.includes('@')) {
      asset.domain = asset.name.split('@')[1];
    }
    await Asset.findOneAndUpdate({ name: asset.name }, asset, { upsert: true, new: true });
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

expressApp.put('/api/assets/:name', async (req, res) => {
  try {
    const body = req.body;
    const type = body.type;
    const asset = {
      type,
      name: body.name.trim().toLowerCase(),
      client: body.client || null,
      provider: body.provider || null,
      workspace: body.workspace || null,
      campaign: body.campaign || null,
      purchaseDate: parseDate(body.purchaseDate),
      expiryDate: parseDate(body.expiryDate),
      status: body.status || 'Active',
      brandedPrewarmed: body.brandedPrewarmed || null,
      primaryOwner: resolveOwner(body.primaryOwner),
      yearlyCost: type === 'DOMAIN' ? parseCost(body.cost) : null,
      monthlyCost: type === 'INBOX' ? parseCost(body.cost) : null,
      currency: body.currency || 'USD',
      notes: body.notes || null,
      updatedAt: new Date()
    };
    if (type === 'INBOX' && asset.name.includes('@')) {
      asset.domain = asset.name.split('@')[1];
    }
    const oldName = decodeURIComponent(req.params.name);
    if (oldName !== asset.name) {
      await Asset.deleteOne({ name: oldName });
    }
    await Asset.findOneAndUpdate({ name: asset.name }, asset, { upsert: true, new: true });
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

expressApp.delete('/api/assets/:name', async (req, res) => {
  try {
    const name = decodeURIComponent(req.params.name);
    const result = await Asset.deleteOne(buildNameQuery(name));
    if (result.deletedCount === 0) return res.status(404).json({ ok: false, error: 'Not found' });
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

expressApp.post('/api/assets/:name/renew', async (req, res) => {
  try {
    const name = decodeURIComponent(req.params.name);
    const existing = await Asset.findOne(buildNameQuery(name));
    if (!existing) return res.status(404).json({ ok: false, error: 'Not found' });

    const today = dayjs().startOf('day');
    let newExpiryDate = null;

    if (existing.type === 'INBOX') {
      newExpiryDate = today.add(1, 'month').toDate();
    }

    await Asset.findOneAndUpdate({ name: existing.name }, {
      purchaseDate: today.toDate(),
      expiryDate: newExpiryDate,
      remindersSent: [],
      updatedAt: new Date()
    });
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

expressApp.get('/upload', (req, res) => {
  res.redirect('/');
});

expressApp.post('/upload-csv', upload.single('csv'), async (req, res) => {
  try {
    const type = req.body.type;
    if (!type || !['domain', 'inbox'].includes(type)) {
      return res.status(400).json({ error: 'Invalid type. Must be "domain" or "inbox".' });
    }
    if (!req.file) {
      return res.status(400).json({ error: 'No CSV file uploaded.' });
    }

    const assetType = type === 'domain' ? 'DOMAIN' : 'INBOX';
    const assets = await parseCSVBuffer(req.file.buffer, assetType);

    if (assets.length === 0) {
      return res.status(400).json({ error: 'No valid rows found in CSV. Check the file format and column headers.' });
    }

    let inserted = 0;
    let updated = 0;

    for (const asset of assets) {
      const existing = await Asset.findOne({ name: asset.name });
      if (existing) {
        await Asset.findOneAndUpdate({ name: asset.name }, asset, { new: true });
        updated++;
      } else {
        await Asset.create(asset);
        inserted++;
      }
    }

    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));

    return res.json({ inserted, updated, total: assets.length });
  } catch (err) {
    console.error('CSV upload error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// Manual trigger for noon summary (for testing or missed cron)
expressApp.get('/trigger-summary', async (req, res) => {
  try {
    console.log('Manual trigger: runNoonSummary');
    await runNoonSummary();
    res.json({ ok: true, message: 'Noon summary triggered successfully' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Manual trigger for Smartlead sync
expressApp.get('/trigger-smartlead', (req, res) => {
  try {
    console.log('Manual trigger: Smartlead sync');
    const syncDir = path.join(__dirname, 'smartlead_sync');
    const proc = spawn('python', ['run.py'], {
      cwd: syncDir,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    proc.stdout.on('data', d => process.stdout.write(`[smartlead] ${d}`));
    proc.stderr.on('data', d => process.stderr.write(`[smartlead] ${d}`));
    proc.on('close', code => console.log(`[smartlead] manual sync finished with code ${code}`));
    res.json({ ok: true, message: 'Smartlead sync started (check server logs for progress)' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Manual trigger for the simple 5:15 PM reminder (for testing)
expressApp.get('/trigger-reminder', async (req, res) => {
  try {
    console.log('Manual trigger: runSimpleReminder');
    await runSimpleReminder();
    res.json({ ok: true, message: 'Simple reminder triggered successfully' });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Diagnostic endpoint — hit /debug-summary to see exactly what the bot sees
expressApp.get('/debug-summary', async (req, res) => {
  try {
    const CHANNEL = 'C0AGVSUNEFP';
    const assets = await Asset.find();
    const prepared = prepareAssetsForSheet(assets);
    const dayOfWeek = dayjs().day();

    const addWorkingDays = (n) => {
      let d = dayjs().startOf('day');
      let added = 0;
      while (added < n) {
        d = d.add(1, 'day');
        if (d.day() !== 0 && d.day() !== 6) added++;
      }
      return d.diff(dayjs().startOf('day'), 'day');
    };

    const wd1CalDays = addWorkingDays(1);
    const wd3CalDays = addWorkingDays(3);
    const isThursday = dayOfWeek === 4;

    const active = prepared.filter(a => a.status === 'Active' && a.daysLeft !== null);
    const inactive = prepared.filter(a => a.status !== 'Active');
    const noExpiry = prepared.filter(a => a.daysLeft === null);

    const group0 = active.filter(a => a.daysLeft === 0);
    const group1 = active.filter(a => a.daysLeft === wd1CalDays);
    const group3 = active.filter(a => isThursday ? (a.daysLeft === 2 || a.daysLeft === 3) : a.daysLeft === wd3CalDays);

    // Try sending a test ping to Slack right now
    let slackTest = null;
    try {
      const result = await app.client.chat.postMessage({
        token: process.env.SLACK_BOT_TOKEN,
        channel: CHANNEL,
        text: `🔧 Debug ping from /debug-summary — bot can reach this channel. Total assets: ${assets.length}, Active with expiry: ${active.length}, Expiring soon: ${group0.length + group1.length + group3.length}`
      });
      slackTest = { ok: true, ts: result.ts };
    } catch (e) {
      slackTest = { ok: false, error: e.message, data: e.data };
    }

    res.json({
      serverTimeUTC: new Date().toISOString(),
      serverTimeIST: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
      dayOfWeek,
      isThursday,
      wd1CalDays,
      wd3CalDays,
      totalAssets: assets.length,
      activeWithExpiry: active.length,
      inactiveCount: inactive.length,
      noExpiryCount: noExpiry.length,
      expiringToday: group0.map(a => ({ name: a.name, daysLeft: a.daysLeft, status: a.status, expiryDate: a.expiryDateFormatted })),
      expiringTomorrow: group1.map(a => ({ name: a.name, daysLeft: a.daysLeft, status: a.status, expiryDate: a.expiryDateFormatted })),
      expiringIn3WD: group3.map(a => ({ name: a.name, daysLeft: a.daysLeft, status: a.status, expiryDate: a.expiryDateFormatted })),
      sampleAllActive: active.slice(0, 20).map(a => ({ name: a.name, daysLeft: a.daysLeft, status: a.status, expiryDate: a.expiryDateFormatted })),
      slackPingResult: slackTest,
      channelId: CHANNEL
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

expressApp.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

/* -------------------- /infra (shared with the Domain Suggester app) -------------------- */
// Add / renew / list, CSV import-renew-delete and renew/delete name lists live in
// infra_slack.js so the Domain Suggester app can offer the same features.
const INFRA_DEPS = {
  Asset, dayjs, axios, syncAllAssetsToSheet, prepareAssetsForSheet, parseDate, parseCost,
  resolveOwner, computeDaysLeft, parseCSVBuffer, buildNameQuery
};
require('./infra_slack').registerInfraHandlers(app, INFRA_DEPS, {
  command: '/infra', messages: true, botToken: process.env.SLACK_BOT_TOKEN
});

/* -------------------- View Details Button Handlers -------------------- */
app.action(/^view_details_/, async ({ ack, body, client }) => {
  await ack();
  try {
    const payload = JSON.parse(body.actions[0].value);

    let detailText;

    if (payload.daysFilter) {
      // Noon summary format: re-compute full list from DB on click (no size limit)
      const assets = await Asset.find();
      const prepared = prepareAssetsForSheet(assets);
      const active = prepared.filter(a => a.status === 'Active' && a.daysLeft !== null);
      const gAssets = active.filter(a => payload.daysFilter.includes(a.daysLeft));

      const domains = gAssets.filter(a => a.type === 'DOMAIN');
      const inboxes = gAssets.filter(a => a.type === 'INBOX');

      const byClient = {};
      for (const a of gAssets) {
        const c = a.client || 'No Client';
        if (!byClient[c]) byClient[c] = [];
        byClient[c].push(a);
      }

      let detail = `*Expiring ${payload.label}*\n`;
      detail += `Domains: ${domains.length} | Inboxes: ${inboxes.length}\n\n`;
      for (const [c, items] of Object.entries(byClient)) {
        detail += `*${c}*\n`;
        for (const a of items) detail += renewalLabels.assetLine(a) + '\n';
      }
      detailText = detail.trim();
    } else {
      // Legacy / per-owner alerts: detail was pre-serialized in the button value
      detailText = payload.detail;
    }

    await client.chat.postMessage({
      channel: payload.channel,
      thread_ts: body.message.ts,
      text: detailText,
      mrkdwn: true
    });
  } catch (e) {
    console.error('view_details action error:', e.message);
  }
});

app.action('view_expired_details', async ({ ack, body, client }) => {
  await ack();
  try {
    const payload = JSON.parse(body.actions[0].value);
    await client.chat.postMessage({
      channel: payload.channel,
      thread_ts: body.message.ts,
      text: payload.detail,
      mrkdwn: true
    });
  } catch (e) {
    console.error('view_expired_details action error:', e.message);
  }
});

/* -------------------- Daily Summary Reminders -------------------- */
async function runDailySummary() {
  console.log('Running daily summary...');
  const assets = await Asset.find();

  const buckets = {};

  const addToBucket = (dest, asset, daysLeft, isExpired) => {
    if (!dest) return;
    if (!buckets[dest]) buckets[dest] = { expiring: [], expired: [] };
    if (isExpired) buckets[dest].expired.push({ asset, daysLeft });
    else buckets[dest].expiring.push({ asset, daysLeft });
  };

  for (const asset of assets) {
    const daysLeft = computeDaysLeft(asset);
    if (daysLeft === null) continue;

    const isExpired = daysLeft < 0 && asset.status === 'Active';

    const expiryDateStr = (() => {
      let expiry = asset.expiryDate;
      if (!expiry && asset.purchaseDate) {
        if (asset.type === 'DOMAIN') {
          expiry = dayjs(asset.purchaseDate).add(365, 'day').toDate();
        } else {
          const p = dayjs(asset.purchaseDate);
          let next = dayjs().year(dayjs().year()).month(dayjs().month()).date(p.date());
          if (!next.isAfter(dayjs().startOf('day'))) next = next.add(1, 'month');
          expiry = next.toDate();
        }
      }
      return expiry ? dayjs(expiry).format('YYYY-MM-DD') : null;
    })();

    const workingTargets = getWorkingDayTargets();
    const isReminderDay = expiryDateStr && workingTargets.has(expiryDateStr);

    if (!isExpired && !isReminderDay) continue;

    addToBucket(asset.primaryOwner, asset, daysLeft, isExpired);

    if (asset.visibilityChannel) {
      addToBucket(asset.visibilityChannel, asset, daysLeft, isExpired);
    }
  }

  for (const [dest, { expiring, expired }] of Object.entries(buckets)) {

    if (expiring.length > 0) {
      const byDay = {};
      for (const item of expiring) {
        const key = item.daysLeft;
        if (!byDay[key]) byDay[key] = [];
        byDay[key].push(item);
      }

      const blocks = [
        {
          type: 'header',
          text: { type: 'plain_text', text: `Renewal Alerts — ${dayjs().format('DD MMM YYYY')}` }
        }
      ];

      for (const daysLeft of Object.keys(byDay).sort((a, b) => Number(a) - Number(b))) {
        const items = byDay[daysLeft];
        const domains = items.filter(i => i.asset.type === 'DOMAIN');
        const inboxes = items.filter(i => i.asset.type === 'INBOX');
        const label = daysLeft === '0' ? 'today' : daysLeft === '1' ? 'tomorrow' : `in ${daysLeft} days`;

        const parts = [];
        if (domains.length) parts.push(`*${domains.length} domain${domains.length !== 1 ? 's' : ''}*`);
        if (inboxes.length) parts.push(`*${inboxes.length} inbox${inboxes.length !== 1 ? 'es' : ''}*`);
        const summaryText = `${parts.join(' and ')} expire${items.length === 1 ? 's' : ''} ${label}`;

        const byClient = {};
        for (const { asset } of items) {
          const c = asset.client || 'No Client';
          if (!byClient[c]) byClient[c] = [];
          byClient[c].push(asset);
        }

        let detail = `*Expiring ${label.charAt(0).toUpperCase() + label.slice(1)}*\n`;
        detail += `Domains: ${domains.length} | Inboxes: ${inboxes.length}\n\n`;

        for (const [client, clientAssets] of Object.entries(byClient)) {
          detail += `*${client}*\n`;
          for (const asset of clientAssets) {
            const provider = asset.provider ? ` · ${asset.provider}` : '';
            const typeLabel = asset.type === 'DOMAIN' ? '🌐' : '📧';
            detail += `  ${typeLabel} ${asset.name}${provider}\n`;
          }
        }

        blocks.push({
          type: 'section',
          text: { type: 'mrkdwn', text: summaryText },
          accessory: {
            type: 'button',
            text: { type: 'plain_text', text: 'Read more' },
            action_id: `view_details_${daysLeft}`,
            value: JSON.stringify({ detail: detail.trim(), channel: dest })
          }
        });
        blocks.push({ type: 'divider' });
      }

      blocks.push({
        type: 'context',
        elements: [{
          type: 'mrkdwn',
          text: `🔗 <https://infra-bot-1.onrender.com/|View full dashboard> to renew or manage assets`
        }]
      });

      try {
        await app.client.chat.postMessage({
          channel: dest,
          text: `Renewal Alerts — ${dayjs().format('DD MMM YYYY')}`,
          blocks
        });
        console.log(`Sent expiry reminder to ${dest}`);
      } catch (e) {
        console.error(`Failed to send reminder to ${dest}:`, e.message);
      }
    }

    if (expired.length > 0) {
      const domains = expired.filter(i => i.asset.type === 'DOMAIN');
      const inboxes = expired.filter(i => i.asset.type === 'INBOX');

      const parts = [];
      if (domains.length) parts.push(`*${domains.length} domain${domains.length !== 1 ? 's' : ''}*`);
      if (inboxes.length) parts.push(`*${inboxes.length} inbox${inboxes.length !== 1 ? 'es' : ''}*`);
      const summary = `${parts.join(' and ')} expired but still marked Active — please review`;

      const byClient = {};
      for (const { asset, daysLeft } of expired) {
        const c = asset.client || 'No Client';
        if (!byClient[c]) byClient[c] = [];
        byClient[c].push({ asset, daysLeft });
      }
      let detail = `*Expired & Still Active*\n`;
      detail += `Domains: ${domains.length} | Inboxes: ${inboxes.length}\n\n`;
      for (const [client, items] of Object.entries(byClient)) {
        detail += `*${client}*\n`;
        for (const { asset, daysLeft } of items) {
          const provider = asset.provider ? ` · ${asset.provider}` : '';
          const typeLabel = asset.type === 'DOMAIN' ? '🌐' : '📧';
          detail += `  ${typeLabel} ${asset.name}${provider} — ${Math.abs(daysLeft)}d overdue\n`;
        }
      }

      const blocks = [
        {
          type: 'header',
          text: { type: 'plain_text', text: `Expired & Still Active — ${dayjs().format('DD MMM YYYY')}` }
        },
        {
          type: 'section',
          text: { type: 'mrkdwn', text: summary },
          accessory: {
            type: 'button',
            text: { type: 'plain_text', text: 'Read more' },
            action_id: 'view_expired_details',
            value: JSON.stringify({ detail: detail.trim(), channel: dest })
          }
        },
        {
          type: 'context',
          elements: [{
            type: 'mrkdwn',
            text: `🔗 <https://infra-bot-1.onrender.com/|View full dashboard> to manage these assets`
          }]
        }
      ];

      try {
        await app.client.chat.postMessage({
          channel: dest,
          text: `Expired & Still Active — ${dayjs().format('DD MMM YYYY')}`,
          blocks
        });
        console.log(`Sent overdue alert to ${dest}`);
      } catch (e) {
        console.error(`Failed to send overdue alert to ${dest}:`, e.message);
      }
    }
  }
}

/* -------------------- 4:00 PM IST Summary (Structured) -------------------- */
async function runNoonSummary() {
  try {
    const todayStr = dayjs().format('YYYY-MM-DD');
    try {
      await CronLock.create({ jobName: `noonSummary-${todayStr}` });
    } catch (e) {
      if (e.code === 11000) {
        console.log('[runNoonSummary] Lock already acquired by another instance. Skipping.');
        return;
      }
      throw e;
    }

    const CHANNEL = 'C0AGVSUNEFP';
    const assets = await Asset.find();
    const prepared = prepareAssetsForSheet(assets);
    const dayOfWeek = dayjs().day(); // 0=Sun, 1=Mon, ..., 4=Thu, 5=Fri, 6=Sat

    console.log(`[runNoonSummary] ${assets.length} total assets, dayOfWeek=${dayOfWeek}`);

    const isThursday = dayOfWeek === 4;

    const addWorkingDays = (n) => {
      let d = dayjs().startOf('day');
      let added = 0;
      while (added < n) {
        d = d.add(1, 'day');
        if (d.day() !== 0 && d.day() !== 6) added++;
      }
      return d.diff(dayjs().startOf('day'), 'day');
    };

    const wd1CalDays = addWorkingDays(1);
    const wd3CalDays = addWorkingDays(3);

    console.log(`[runNoonSummary] wd1=${wd1CalDays} cal days, wd3=${wd3CalDays} cal days, isThursday=${isThursday}`);

    const active = prepared.filter(a => a.status === 'Active' && a.daysLeft !== null);
    console.log(`[runNoonSummary] active assets with daysLeft: ${active.length}`);

    const allDaysLeft = active.map(a => `${a.name}=${a.daysLeft}`).join(', ');
    console.log(`[runNoonSummary] all active daysLeft: ${allDaysLeft}`);

    const groups = [
      {
        assets: active.filter(a => a.daysLeft === 0),
        label: 'Today',
        icon: '🔴',
        key: 'noon_0',
        daysFilter: [0]
      },
      {
        assets: active.filter(a => a.daysLeft === wd1CalDays),
        label: `Tomorrow (${dayjs().add(wd1CalDays, 'day').format('ddd DD MMM')})`,
        icon: '🟠',
        key: 'noon_1',
        daysFilter: [wd1CalDays]
      },
      {
        assets: active.filter(a => {
          if (isThursday) return a.daysLeft === 2 || a.daysLeft === 3;
          return a.daysLeft === wd3CalDays;
        }),
        label: isThursday
          ? `This weekend & Monday (${dayjs().add(3, 'day').format('ddd DD MMM')})`
          : `In ${wd3CalDays} days (${dayjs().add(wd3CalDays, 'day').format('ddd DD MMM')})`,
        icon: '🟡',
        key: 'noon_3',
        daysFilter: isThursday ? [2, 3] : [wd3CalDays]
      }
    ];

    groups.forEach(g => {
      console.log(`[runNoonSummary] group "${g.label}": ${g.assets.length} assets`);
    });

    const totalExpiring = groups.reduce((sum, g) => sum + g.assets.length, 0);
    console.log(`[runNoonSummary] totalExpiring: ${totalExpiring}`);

    const blocks = [
      {
        type: 'header',
        text: { type: 'plain_text', text: `⏰ Daily Renewal Check — ${dayjs().format('DD MMM YYYY')}` }
      }
    ];

    if (totalExpiring === 0) {
      blocks.push({
        type: 'section',
        text: {
          type: 'mrkdwn',
          text: `✅ No assets expiring today, tomorrow, or in the next 3 working days. All clear!`
        }
      });
    } else {
      for (const g of groups) {
        const { label, icon, key, daysFilter, assets: gAssets } = g;
        if (gAssets.length === 0) continue;

        const domains = gAssets.filter(a => a.type === 'DOMAIN');
        const inboxes = gAssets.filter(a => a.type === 'INBOX');

        const parts = [];
        if (domains.length) parts.push(`*${domains.length} domain${domains.length !== 1 ? 's' : ''}*`);
        if (inboxes.length) parts.push(`*${inboxes.length} inbox${inboxes.length !== 1 ? 'es' : ''}*`);
        const summaryText = `${icon}  ${parts.join(' and ')} expire${gAssets.length === 1 ? 's' : ''} *${label}*`
          + renewalLabels.estimatedNote(gAssets);

        const byClient = {};
        for (const a of gAssets) {
          const c = a.client || 'No Client';
          if (!byClient[c]) byClient[c] = [];
          byClient[c].push(a);
        }
        let detail = `*Expiring ${label}*\n`;
        detail += `Domains: ${domains.length} | Inboxes: ${inboxes.length}\n\n`;
        for (const [client, items] of Object.entries(byClient)) {
          detail += `*${client}*\n`;
          for (const a of items) detail += renewalLabels.assetLine(a) + '\n';
        }

        blocks.push({
          type: 'section',
          text: { type: 'mrkdwn', text: summaryText },
          accessory: {
            type: 'button',
            text: { type: 'plain_text', text: 'Read more' },
            action_id: `view_details_${key}`,
            // Store only a compact lookup key — detail is re-fetched from DB on click
            // to avoid Slack's 2001-char button value limit
            value: JSON.stringify({ key, channel: CHANNEL, daysFilter, label })
          }
        });
        blocks.push({ type: 'divider' });
      }
    }

    const baseUrl = process.env.RENDER_EXTERNAL_HOSTNAME ? `https://${process.env.RENDER_EXTERNAL_HOSTNAME}` : 'https://infra-bot-1.onrender.com';

    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: `🔗 <${baseUrl}/|Open dashboard> to renew or manage assets`
      }]
    });

    console.log(`[runNoonSummary] sending message with ${blocks.length} blocks to ${CHANNEL}`);
    const result = await app.client.chat.postMessage({
      token: process.env.SLACK_BOT_TOKEN,
      channel: CHANNEL,
      text: `Daily Renewal Check — ${dayjs().format('DD MMM YYYY')}`,
      blocks
    });
    console.log(`✅ Noon summary sent, ts: ${result.ts}`);

  } catch (error) {
    console.error('❌ Error sending noon summary:', error);
    if (error.data) console.error('Slack error data:', JSON.stringify(error.data));
  }
}

/* -------------------- 5:15 PM IST Simple Reminder -------------------- */
async function runSimpleReminder() {
  try {
    const todayStr = dayjs().format('YYYY-MM-DD');
    try {
      await CronLock.create({ jobName: `simpleReminder-${todayStr}` });
    } catch (e) {
      if (e.code === 11000) {
        console.log('[runSimpleReminder] Lock already acquired by another instance. Skipping.');
        return;
      }
      throw e;
    }

    const CHANNEL = 'C0AGVSUNEFP';
    const baseUrl = process.env.RENDER_EXTERNAL_HOSTNAME ? `https://${process.env.RENDER_EXTERNAL_HOSTNAME}` : 'https://infra-bot-1.onrender.com';
    console.log('[runSimpleReminder] Sending 5:15 PM reminder...');
    await app.client.chat.postMessage({
      token: process.env.SLACK_BOT_TOKEN,
      channel: CHANNEL,
      text: `🔔 Reminder: Please review and renew any expiring domains or inboxes → ${baseUrl}/`
    });
    console.log('✅ Simple reminder sent');
  } catch (error) {
    console.error('❌ Error sending simple reminder:', error);
    if (error.data) console.error('Slack error data:', JSON.stringify(error.data));
  }
}

/* -------------------- Startup -------------------- */
async function start() {
  try {
    await mongoose.connect(process.env.MONGO_URI);
    console.log('✅ MongoDB connected');

    try {
      const allAssets = await Asset.find();
      await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
      console.log('✅ Google Sheets synced on startup');
    } catch (sheetsErr) {
      console.warn('⚠️  Google Sheets sync skipped on startup:', sheetsErr.message);
    }

    await app.start();
    console.log('✅ Slack bot running in socket mode');

    // /domains lives on its own Slack app (separate tokens) because the Infra
    // Bot app is owned by a deactivated user and its commands cannot be
    // edited. Failure here must not stop the main bot from serving.
    try {
      domainsApp = await startDomainsApp(__dirname, { onTrackerChanged: resyncAssetSheet, infraDeps: INFRA_DEPS });
    } catch (domErr) {
      console.warn('⚠️  /domains app failed to start:', domErr?.message || domErr);
    }

    expressApp.listen(PORT, '0.0.0.0', () => {
      console.log(`✅ Upload UI running at http://0.0.0.0:${PORT}`);
    });

    // Schedule daily structured summary at 10:00 AM IST, Mon–Fri only
    cron.schedule('0 10 * * 1-5', async () => {
      const now = new Date().toISOString();
      console.log(`[CRON] Daily summary firing at ${now}`);
      await runNoonSummary();
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Smartlead dashboard sync at 10:00 AM IST, every day
    cron.schedule('0 10 * * *', async () => {
      const now = new Date().toISOString();
      console.log(`[CRON] Smartlead sync firing at ${now}`);
      const todayStr = dayjs().format('YYYY-MM-DD');
      try {
        await CronLock.create({ jobName: `smartleadSync-${todayStr}` });
      } catch (e) {
        if (e.code === 11000) {
          console.log('[CRON] Smartlead sync lock already acquired. Skipping.');
          return;
        }
      }
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['run.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[smartlead] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[smartlead] ${d}`));
      proc.on('close', code => {
        // run.py exits 2 when any sheet tab was not written; the Last Sync tab
        // lists which and why. Anything else non-zero is a crash before the
        // verdict, so the ledger may be incomplete - treat as failure too.
        if (code === 0) console.log('[smartlead] ✅ sync finished: all tabs written');
        else if (code === 2) console.error('[smartlead] ⚠ SYNC INCOMPLETE: one or more tabs NOT written - see the Last Sync tab');
        else console.error(`[smartlead] ❌ sync crashed with code ${code} - tabs may be stale`);
      });
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Auto placement-test executor at 11:00 AM IST daily (1h after sync so
    // health data is fresh). Ships DRY-RUN (RETEST_ENABLED=false) — spends
    // nothing until explicitly enabled via the RETEST_ENABLED env var.
    cron.schedule('0 11 * * *', () => {
      console.log(`[CRON] Auto placement-test firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['retest_executor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[retest] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[retest] ${d}`));
      proc.on('close', code => console.log(`[retest] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Weekly placement batch, Thursdays, every two hours from 10:00 IST. Each
    // run fires at most one wave (PLACEMENT_WAVE_SIZE) and only once the prior
    // wave has cleared, so ~19 domains cover in four waves without a
    // long-running process. Firing all at once starved the seed queue.
    cron.schedule('0 10,12,14,16 * * 4', () => {
      console.log(`[CRON] Weekly placement batch firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const args = ['placement_executor.py'];
      if (process.env.PLACEMENT_ENABLED !== 'true') args.push('--dry-run');
      const proc = spawn('python', args, {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[placement] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[placement] ${d}`));
      proc.on('close', code => console.log(`[placement] batch finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Deliverability-test copy sync: Sunday 18:30 and Monday/Tuesday 05:00 IST, i.e.
    // before the 06:00 scheduled tests. Copies the newest ACTIVE campaign's first step
    // into each test campaign (Precise Leads, Melior, BettrData) so the tests send
    // what is really running. Writes by default; COPY_SYNC_DISABLED=true makes it
    // a dry run. Never writes to a live campaign.
    cron.schedule('30 18 * * 0', () => runCopySync(), { timezone: 'Asia/Kolkata' });
    cron.schedule('0 5 * * 1,2', () => runCopySync(), { timezone: 'Asia/Kolkata' });
    function runCopySync() {
      console.log(`[CRON] Test-copy sync firing at ${new Date().toISOString()}`);
      const args = ['copy_sync.py'];
      if (process.env.COPY_SYNC_DISABLED !== 'true') args.push('--apply');
      const proc = spawn('python', args, {
        cwd: path.join(__dirname, 'smartlead_sync'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[copy-sync] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[copy-sync] ${d}`));
      proc.on('close', code => console.log(`[copy-sync] finished with code ${code}`));
    }

    // Deliverability report, Mondays and Tuesdays, hourly 08:00-12:00 IST plus a final
    // run at 13:00 (tests start 06:00). Each run reports only tests not reported before, and only once
    // 90% of a test's seeds are classified (SmartDelivery marks a test COMPLETED
    // before that); the 13:00 run reports whatever is in. Posts to Slack (channel
    // C0AGVSUNEFP), keeps per-inbox status in Mongo and writes the Inbox Status
    // tab Campaign Desk reads. Grid cells only with PLACEMENT_GRID_ENABLED=true.
    // PLACEMENT_REPORT_DISABLED=true makes it print-only.
    cron.schedule('0 8-12 * * 1,2', () => runPlacementReport(false), { timezone: 'Asia/Kolkata' });
    cron.schedule('0 13 * * 1,2', () => runPlacementReport(true), { timezone: 'Asia/Kolkata' });
    function runPlacementReport(final) {
      const today = new Date().toISOString().slice(0, 10);
      ['PRECISE_LEADS', 'BETTRDATA'].forEach(account => {
        const args = ['placement_report.py', '--account', account, '--since', today, '--only-new'];
        if (final) args.push('--final');
        if (process.env.PLACEMENT_REPORT_DISABLED !== 'true') args.push('--post', '--save', '--status-tab');
        if (process.env.PLACEMENT_GRID_ENABLED === 'true') args.push('--grid');
        const proc = spawn('python', args, {
          cwd: path.join(__dirname, 'smartlead_sync'),
          env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
        });
        proc.stdout.on('data', d => process.stdout.write(`[placement-report ${account}] ${d}`));
        proc.stderr.on('data', d => process.stderr.write(`[placement-report ${account}] ${d}`));
        proc.on('close', code => console.log(`[placement-report ${account}] finished with code ${code}`));
      });
    }

    // Placement result collector, 12:30 and 16:30 IST daily. Thursday's batch
    // lands in the first window; the later run catches stragglers, and the
    // other days pick up any on-demand test. Collect-only: never creates.
    cron.schedule('30 12,16 * * *', () => {
      console.log(`[CRON] Placement collector firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['placement_executor.py', '--collect-only'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[placement] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[placement] ${d}`));
      proc.on('close', code => console.log(`[placement] collect finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Inbox setup jobs: every 10 minutes move approved jobs forward and DM each
    // job's creator when it changes. Only approved jobs run; paid steps also
    // need ZAPMAIL_ALLOW_SPEND=true. Quiet when there is nothing to do.
    cron.schedule('*/10 * * * *', () => {
      require('./inbox_jobs_notify').tickAndNotify(__dirname)
        .catch(err => console.warn('[inbox-jobs] tick error:', err.message));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Fleet placement summary at 09:50 IST daily. Read-only and credit-free —
    // it reads placement already rolled up across past tests, so drift shows
    // up the next morning instead of waiting for the weekly test. Exit 2 means
    // at least one mailbox is below threshold.
    cron.schedule('50 9 * * *', () => {
      console.log(`[CRON] Placement summary firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['placement_summary.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[placement-summary] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[placement-summary] ${d}`));
      proc.on('close', code => {
        if (code === 2) console.warn('[placement-summary] ⚠ mailbox(es) below placement threshold');
        else console.log('[placement-summary] all mailboxes above threshold');
      });
    }, {
      timezone: 'Asia/Kolkata'
    });

    // API-key health watchdog at 09:45 IST daily — 15 minutes before the sync,
    // so a rotated/revoked key is reported BEFORE the day's jobs run blind on
    // it. Read-only, no enable flag. Exit code 2 = dead key, 1 = unreachable.
    cron.schedule('45 9 * * *', () => {
      console.log(`[CRON] API-key health check firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['key_health_monitor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[key-health] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[key-health] ${d}`));
      proc.on('close', code => {
        if (code === 2) console.error('[key-health] 🚨 DEAD API KEY(S) — clients are blind until fixed');
        else if (code === 1) console.warn('[key-health] ⚠ Smartlead unreachable for one or more accounts');
        else console.log('[key-health] all keys valid');
      });
    }, {
      timezone: 'Asia/Kolkata'
    });

    // EmailGuard placement tests at 11:20 IST daily (after the Smartlead
    // retest job). Credit-free testing that works for every client from one
    // workspace pool — Pass A scores finished tests, Pass B creates new ones
    // worst-first. Ships DRY-RUN (EG_TEST_ENABLED=false).
    cron.schedule('20 11 * * *', () => {
      console.log(`[CRON] EmailGuard placement test firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['eg_test_executor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[eg-test] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[eg-test] ${d}`));
      proc.on('close', code => console.log(`[eg-test] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Non-connected (Anjali-style) placement tests: hourly 9:00-21:00 IST at
    // :15. Watches the "NC Tests" sheet tab — a human creates the non-connected
    // test in the PL UI and pastes seed list + Track-ID; this executor sends
    // from the inbox via its own account's campaign, polls the PL test, and
    // writes results. Ships DRY-RUN (NC_TEST_ENABLED=false) — logs the plan,
    // mutates nothing until explicitly enabled via the NC_TEST_ENABLED env var.
    cron.schedule('15 9-21 * * *', () => {
      console.log(`[CRON] NC placement-test firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['nc_test_executor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[nc-test] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[nc-test] ${d}`));
      proc.on('close', code => console.log(`[nc-test] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Auto-warmup executor at 11:30 AM IST daily. Ships DRY-RUN
    // (WARMUP_AUTO_ENABLED=false) — logs would-change, applies nothing until
    // explicitly enabled via the WARMUP_AUTO_ENABLED env var.
    cron.schedule('30 11 * * *', () => {
      console.log(`[CRON] Auto-warmup firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['warmup_executor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[warmup] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[warmup] ${d}`));
      proc.on('close', code => console.log(`[warmup] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Auto inbox-rotation at 12:00 PM IST daily. Ships DRY-RUN
    // (ROTATION_ENABLED=false) — logs planned swaps, applies nothing until
    // validated on a dummy campaign and enabled via the ROTATION_ENABLED env var.
    cron.schedule('0 12 * * *', () => {
      console.log(`[CRON] Inbox rotation firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['rotation_executor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[rotation] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[rotation] ${d}`));
      proc.on('close', code => console.log(`[rotation] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // NOTE: the warmup-headroom job that used to run at 12:15 IST was REMOVED
    // on 2026-07-10. It raised max_email_per_day to make room for warmup inside
    // what we believed was one shared daily bucket. That premise was wrong:
    // warmup has its own separate allowance (warmup_details.max_email_per_day)
    // — proven live, inboxes capped at 10 campaign emails/day were sending
    // 41-43 warmup emails/day. The job only ever raised the COLD ceiling for no
    // benefit, and its 15 live changes were rolled back from the snapshot.

    // Bounce auto-protection sweep at 12:30 PM IST daily. Ships DRY-RUN
    // (BOUNCE_PROTECT_ENABLED=false) — logs ACTIVE campaigns missing the
    // Smartlead bounce auto-pause threshold, applies nothing until enabled.
    cron.schedule('30 12 * * *', () => {
      console.log(`[CRON] Bounce-protect sweep firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['bounce_protect_executor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[bounce-protect] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[bounce-protect] ${d}`));
      proc.on('close', code => console.log(`[bounce-protect] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Blacklist monitor at 9:00 AM IST every Monday. Read-only (DNSBL lookups
    // via Google DoH against Spamhaus DBL / SURBL / URIBL) — no enable flag needed.
    cron.schedule('0 9 * * 1', () => {
      console.log(`[CRON] Blacklist monitor firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['blacklist_monitor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[blacklist] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[blacklist] ${d}`));
      proc.on('close', code => console.log(`[blacklist] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Capacity planner at 9:30 AM IST every Monday (after the blacklist run).
    // Read-only: writes the Capacity advisory tab + domain registry only.
    cron.schedule('30 9 * * 1', () => {
      console.log(`[CRON] Capacity planner firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['capacity_planner.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[capacity] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[capacity] ${d}`));
      proc.on('close', code => console.log(`[capacity] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Zapmail → /infra tracker sync at 9:30 AM IST: expiry dates, lapsed
    // domains, new current-client domains/inboxes. Writes only when
    // ZAPMAIL_ASSET_SYNC_ENABLED=true (otherwise logs a preview), then
    // re-syncs the tracker sheet. ScaledMail has its own sync at 9:35.
    cron.schedule('30 9 * * *', () => {
      const apply = process.env.ZAPMAIL_ASSET_SYNC_ENABLED === 'true';
      console.log(`[CRON] Zapmail tracker sync (${apply ? 'apply' : 'preview'}) firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['zapmail_asset_sync.py', ...(apply ? ['--apply'] : [])], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[zapmail-sync] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[zapmail-sync] ${d}`));
      proc.on('close', code => {
        console.log(`[zapmail-sync] finished with code ${code}`);
        if (apply && code === 0) {
          resyncAssetSheet().catch(err => console.warn('[zapmail-sync] sheet refresh failed:', err.message));
        }
      });
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Zapmail daily digest at 9:40 AM IST (read-only): wallets, domains
    // expiring soon, purchase batches due or needing reconcile. Posts only when
    // ZAPMAIL_NOTIFY_CHANNEL is set; otherwise it just logs.
    cron.schedule('40 9 * * *', () => {
      console.log(`[CRON] Zapmail digest firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['zapmail_digest.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[zapmail-digest] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[zapmail-digest] ${d}`));
      proc.on('close', code => console.log(`[zapmail-digest] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // ScaledMail tracker sync at 9:35 IST: writes only when
    // SCALEDMAIL_ASSET_SYNC_ENABLED=true; skips without a key.
    const runScaledMail = (tag, args, after) => {
      if (!process.env.SCALEDMAIL_API_KEY) return;
      const proc = spawn('python', ['scaledmail_cli.py', ...args], {
        cwd: path.join(__dirname, 'smartlead_sync'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[${tag}] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[${tag}] ${d}`));
      proc.on('close', code => { console.log(`[${tag}] finished with code ${code}`); if (after) after(code); });
    };
    cron.schedule('35 9 * * *', () => {
      const apply = process.env.SCALEDMAIL_ASSET_SYNC_ENABLED === 'true';
      console.log(`[CRON] ScaledMail tracker sync (${apply ? 'apply' : 'preview'}) firing at ${new Date().toISOString()}`);
      runScaledMail('scaledmail-sync', ['sync', ...(apply ? ['--apply'] : [])], code => {
        if (apply && code === 0) {
          resyncAssetSheet().catch(err => console.warn('[scaledmail-sync] sheet refresh failed:', err.message));
        }
      });
    }, {
      timezone: 'Asia/Kolkata'
    });
    // (The ScaledMail digest is part of the 9:40 "Domains & inboxes daily".)

    // Infra audit at 10:20 AM IST Mon-Fri (read-only): MX / DMARC policy,
    // name-server footprint, redirects, main-domain use, mailbox and domain
    // caps, young inboxes in campaigns, warmup, signatures, provider mix, ESP
    // matching, SMTP IP blacklists. Posts only when INFRA_AUDIT_CHANNEL is set.
    cron.schedule('20 10 * * 1-5', () => {
      console.log(`[CRON] Infra audit firing at ${new Date().toISOString()}`);
      const args = ['infra_audit.py', ...(process.env.INFRA_AUDIT_CHANNEL ? ['--post'] : [])];
      const proc = spawn('python', args, {
        cwd: path.join(__dirname, 'smartlead_sync'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[infra-audit] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[infra-audit] ${d}`));
      proc.on('close', code => console.log(`[infra-audit] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Inbox billing: did every Zapmail subscription / ScaledMail order renew?
    // Every 2 hours 9-21 IST; posts only changes (not renewed, payment failed,
    // cancelled, bought outside the bot) to RENEWAL_REVIEW_CHANNEL / ZAPMAIL_NOTIFY_CHANNEL.
    // Renewal review at 9:50 IST: inboxes on bills due in 3 days → keep / retire / check.
    const runBilling = (tag, args) => {
      const proc = spawn('python', args, {
        cwd: path.join(__dirname, 'smartlead_sync'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[${tag}] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[${tag}] ${d}`));
      proc.on('close', code => console.log(`[${tag}] finished with code ${code}`));
    };
    cron.schedule('5 9-21/2 * * *', () => runBilling('billing-watch', ['billing_watch.py']), { timezone: 'Asia/Kolkata' });
    // --flag-tracker marks DROP / CHECK on the tracker rows, so the 10:00 Daily
    // Renewal Check shows "don't renew" next to them (7 days ahead).
    cron.schedule('50 9 * * *', () => runBilling('renewal-review', ['renewal_review.py', '--days', '7', '--post', '--flag-tracker']), { timezone: 'Asia/Kolkata' });

    // Per-domain reply-rate early warning at 1:00 PM IST daily (read-only).
    cron.schedule('0 13 * * *', () => {
      console.log(`[CRON] Reply monitor firing at ${new Date().toISOString()}`);
      const syncDir = path.join(__dirname, 'smartlead_sync');
      const proc = spawn('python', ['reply_monitor.py'], {
        cwd: syncDir,
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[reply-mon] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[reply-mon] ${d}`));
      proc.on('close', code => console.log(`[reply-mon] finished with code ${code}`));
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Simple one-line reminder at 4:00 PM IST, Mon–Fri only
    cron.schedule('0 16 * * 1-5', async () => {
      const now = new Date().toISOString();
      console.log(`[CRON] Simple reminder firing at ${now}`);
      await runSimpleReminder();
    }, {
      timezone: 'Asia/Kolkata'
    });

    const nowUtc = new Date().toISOString();
    const nowIst = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    console.log(`✅ Crons scheduled — 10:00 AM IST Smartlead sync + 10:00 AM IST summary (Mon–Fri) + 4:00 PM IST reminder (Mon–Fri) (server UTC: ${nowUtc} | IST: ${nowIst})`);

  } catch (error) {
    console.error('❌ Error during startup:', error);
    process.exit(1);
  }
}

process.on('unhandledRejection', (reason, promise) => {
  console.error('❌ [process] Unhandled Promise Rejection:', reason);
  // Optional: keep the process alive. If you prefer to restart on errors, uncomment:
  // process.exit(1);
});

process.on('uncaughtException', (err) => {
  console.error('❌ [process] Uncaught Exception:', err);
  // Optional: exit so the platform (Render) restarts cleanly:
  // process.exit(1);
});
start();

async function shutdown(signal) {
  console.log(`Received ${signal} — shutting down gracefully...`);
  try {
    // Disconnect the Slack socket first so the new deploy's container can
    // claim the single allowed socket-mode connection without Slack forcing
    // a "server explicit disconnect" on the overlapping old container.
    await app.stop();
    console.log('✅ Slack socket disconnected');
    if (domainsApp) {
      await domainsApp.stop();
      console.log('✅ /domains socket disconnected');
    }
  } catch (err) {
    console.warn('⚠️  Error stopping Slack app:', err?.message || err);
  }
  try {
    await mongoose.connection.close();
  } catch (err) {
    console.warn('⚠️  Error closing MongoDB:', err?.message || err);
  }
  process.exit(0);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
