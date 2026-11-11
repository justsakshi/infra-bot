require('dotenv').config();

process.on('unhandledRejection', (reason) => {
  console.warn('[WARN] Unhandled rejection (process kept alive):', reason?.message || reason);
});
const mongoose = require('mongoose');
const { App } = require('@slack/bolt');
const cron = require('node-cron');
const dayjs = require('dayjs');
const renewalLabels = require('./renewal_labels');
const health = require('./bot_health');
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
// No TTL: a lock must last its whole IST day (bot_health.CRON_LOCK_FIELDS says why). Never build a
// cronlocks TTL shorter than 2 days.
const CronLockSchema = new mongoose.Schema(health.CRON_LOCK_FIELDS);
const CronLock = mongoose.model('CronLock', CronLockSchema);

/* -------------------- Locks and run records (bot health) -------------------- */
const PROCESS_START = new Date();

// One run per key across instances (two overlap during a deploy). The key is the cronlocks _id, which
// Mongo keeps unique with no extra index: the unique jobName index the schema declares was never built
// (audit 7 Nov 2026), so CronLock.create never refused a second run. A lock the code before this change
// took (jobName only) still counts. If Mongo fails, the job runs anyway, as before.
async function takeLock(key) {
  try {
    if (await CronLock.collection.findOne({ jobName: key }, { projection: { _id: 1 } })) {
      console.log(`[lock] ${key} already taken. Skipping.`);
      return false;
    }
    await CronLock.collection.insertOne(health.lockDoc(key));
    return true;
  } catch (e) {
    if (e.code === 11000) {
      console.log(`[lock] ${key} already taken on another instance. Skipping.`);
      return false;
    }
    console.warn(`[lock] ${key}: could not take the lock (${e.message}); running anyway`);
    return true;
  }
}
/** Once per IST day per job (05:00 IST is still the previous day in UTC). */
const onceToday = (job, now = new Date()) => takeLock(`${job}-${health.istDay(now)}`);

// Every scheduled run writes one Mongo cron_runs doc {job, slot, started, ended, exit_code} in the
// HEALTH_HISTORY_DB the Python jobs use; the bot-health post reads them. Recording never stops or
// fails a job: every write is caught and only logged.
const HEALTH_DB = process.env.HEALTH_HISTORY_DB || 'infrabot';
const healthCol = name => mongoose.connection.getClient().db(HEALTH_DB).collection(name);

function recordStart(job, slot, extra = {}) {
  const doc = { job, slot, started: new Date(), ...extra };
  const id = Promise.resolve()
    .then(() => healthCol('cron_runs').insertOne({ ...doc }))
    .then(r => r.insertedId)
    .catch(err => { console.warn(`[cron_runs] ${job}: start not recorded (${err.message})`); return null; });
  return { doc, id };
}
function recordEnd(rec, fields) {
  return rec.id
    .then(id => (id
      ? healthCol('cron_runs').updateOne({ _id: id }, { $set: fields })
      : healthCol('cron_runs').insertOne({ ...rec.doc, ...fields })))
    .catch(err => console.warn(`[cron_runs] ${rec.doc.job}: end not recorded (${err.message})`));
}

/**
 * Run one smartlead_sync Python job: output prefixed [name label], one cron_runs record, and the exit
 * code explained from bot_health.JOBS (a code a job does not expect is logged as FAILED).
 * opts: slot (the IST minute it was due; default now), label (e.g. the account).
 * Resolves with the exit code, or null if Python could not start or was killed.
 */
function runJob(name, args, opts = {}) {
  const tag = opts.label ? `${name} ${opts.label}` : name;
  const rec = recordStart(name, opts.slot || health.slotKey(new Date()), opts.label ? { label: opts.label } : {});
  return new Promise(resolve => {
    let done = false;
    const finish = (code, how = {}) => {
      if (done) return;
      done = true;
      const ok = !how.error && health.exitOk(name, code);
      (ok ? console.log : console.error)(`[${tag}] ${health.exitLine(name, code, how)}`);
      const fields = { ended: new Date(), exit_code: code };
      if (how.error) fields.error = String(how.error).slice(0, 300);
      if (how.signal) fields.signal = how.signal;
      recordEnd(rec, fields);
      resolve(code);
    };
    try {
      const proc = spawn('python', args, {
        cwd: path.join(__dirname, 'smartlead_sync'),
        env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
      });
      proc.stdout.on('data', d => process.stdout.write(`[${tag}] ${d}`));
      proc.stderr.on('data', d => process.stderr.write(`[${tag}] ${d}`));
      proc.on('error', err => finish(null, { error: err.message }));
      proc.on('close', (code, signal) => finish(code, { signal }));
    } catch (err) {
      finish(null, { error: err.message });
    }
  });
}

/** A scheduled job written in JS (renewal posts, bot health): same record; fn returning false = failed. */
async function track(name, fn, opts = {}) {
  const rec = recordStart(name, opts.slot === undefined ? health.slotKey(new Date()) : opts.slot, opts.extra || {});
  let result;
  try {
    result = await fn();
  } catch (err) {
    console.error(`[${name}] ${err.message}`);
    result = false;
  }
  await recordEnd(rec, { ended: new Date(), exit_code: result === false ? 1 : 0, posted: result === true,
    ...(result === 'skipped' ? { skipped: true } : {}) });
  return result;
}

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

// Everything below needs the dashboard login (web_auth.js). The Zapmail
// webhook above keeps its own signature check; /healthz stays open.
const webAuth = require('./web_auth');
const triggers = webAuth.singleFlight();
expressApp.get('/healthz', (req, res) => res.json({ ok: true }));
expressApp.use(webAuth.requireLogin());
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
  // One sync at a time: repeated calls used to start parallel full syncs and
  // burn the Smartlead rate limit.
  if (!triggers.tryStart('smartlead')) {
    return res.status(409).json({ ok: false, error: 'a Smartlead sync started from here is still running' });
  }
  try {
    console.log('Manual trigger: Smartlead sync');
    const syncDir = path.join(__dirname, 'smartlead_sync');
    const proc = spawn('python', ['run.py'], {
      cwd: syncDir,
      env: { ...process.env, PYTHONIOENCODING: 'utf-8' }
    });
    proc.stdout.on('data', d => process.stdout.write(`[smartlead] ${d}`));
    proc.stderr.on('data', d => process.stderr.write(`[smartlead] ${d}`));
    proc.on('close', code => { triggers.done('smartlead'); console.log(`[smartlead] manual sync finished with code ${code}`); });
    proc.on('error', () => triggers.done('smartlead'));
    res.json({ ok: true, message: 'Smartlead sync started (check server logs for progress)' });
  } catch (e) {
    triggers.done('smartlead');
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
// "Show replies" buttons in the 10:30 campaign stats post.
require('./campaign_replies_slack').registerShowReplies(app);
require('./campaign_replies_slack').registerCampaignSummary(app);

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
      detailText = renewalLabels.expiringDetail(payload.label, gAssets);
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
// Returns true when posted, 'skipped' when another instance already posted today, false on an error.
async function runNoonSummary() {
  try {
    if (!(await onceToday('noonSummary'))) {
      console.log('[runNoonSummary] Lock already acquired by another instance. Skipping.');
      return 'skipped';
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

    const baseUrl = process.env.RENDER_EXTERNAL_HOSTNAME ? `https://${process.env.RENDER_EXTERNAL_HOSTNAME}` : 'https://infra-bot-1.onrender.com';
    const { text: summaryTitle, blocks } = renewalLabels.renewalCheckBlocks(groups, { channel: CHANNEL, baseUrl, now: dayjs() });

    console.log(`[runNoonSummary] sending message with ${blocks.length} blocks to ${CHANNEL}`);
    const result = await app.client.chat.postMessage({
      token: process.env.SLACK_BOT_TOKEN,
      channel: CHANNEL,
      text: summaryTitle,
      blocks
    });
    console.log(`✅ Noon summary sent, ts: ${result.ts}`);
    return true;
  } catch (error) {
    console.error('❌ Error sending noon summary:', error);
    if (error.data) console.error('Slack error data:', JSON.stringify(error.data));
    return false;
  }
}

/* -------------------- 5:15 PM IST Simple Reminder -------------------- */
async function runSimpleReminder() {
  try {
    if (!(await onceToday('simpleReminder'))) {
      console.log('[runSimpleReminder] Lock already acquired by another instance. Skipping.');
      return 'skipped';
    }

    const CHANNEL = 'C0AGVSUNEFP';
    const baseUrl = process.env.RENDER_EXTERNAL_HOSTNAME ? `https://${process.env.RENDER_EXTERNAL_HOSTNAME}` : 'https://infra-bot-1.onrender.com';
    console.log('[runSimpleReminder] Sending 5:15 PM reminder...');
    await app.client.chat.postMessage({
      token: process.env.SLACK_BOT_TOKEN,
      channel: CHANNEL,
      text: renewalLabels.reminderText(baseUrl)
    });
    console.log('✅ Simple reminder sent');
    return true;
  } catch (error) {
    console.error('❌ Error sending simple reminder:', error);
    if (error.data) console.error('Slack error data:', JSON.stringify(error.data));
    return false;
  }
}

/* -------------------- Bot health posts (bot_health.js) -------------------- */
// Weekdays 10:05 IST and once after a restart, to the infra channel (BOT_HEALTH_CHANNEL overrides;
// BOT_HEALTH_POST=off records runs but posts nothing). Tue 14:00 / Wed 10:00: the v2 check.
const healthChannel = () => process.env.BOT_HEALTH_CHANNEL || health.INFRA_CHANNEL;

/** Headline, then the detail in its thread. True when the headline posted; 'off' when switched off. */
async function postHealthMessage(tag, msg) {
  if (process.env.BOT_HEALTH_POST === 'off') {
    console.log(`[${tag}] BOT_HEALTH_POST=off, not posted:\n${msg.text}${msg.thread ? `\n--- thread ---\n${msg.thread}` : ''}`);
    return 'off';
  }
  const channel = healthChannel();
  const res = await app.client.chat.postMessage({ channel, text: msg.text, unfurl_links: false });
  if (msg.thread) {
    try {
      await app.client.chat.postMessage({ channel, thread_ts: res.ts, text: msg.thread, unfurl_links: false });
    } catch (err) {
      console.error(`[${tag}] thread not posted: ${err.message}`);
    }
  }
  console.log(`[${tag}] posted to ${channel}`);
  return true;
}

/**
 * The commit the bot runs against main on GitHub (public repo, no token): { running, behindBy, mainSha }
 * or { running, error }. null off Render (no RENDER_GIT_COMMIT). Never throws: the health post must go out.
 */
async function deployState() {
  const running = process.env.RENDER_GIT_COMMIT;
  if (!running) return null;
  const repo = process.env.RENDER_GIT_REPO_SLUG || health.REPO;
  const branch = process.env.RENDER_GIT_BRANCH || 'main';
  try {
    const { data } = await axios.get(`https://api.github.com/repos/${repo}/compare/${running}...${branch}`, {
      timeout: 15000, headers: { 'User-Agent': 'infra-bot', Accept: 'application/vnd.github+json' }
    });
    const commits = Array.isArray(data.commits) ? data.commits : [];
    return { running, behindBy: Number(data.ahead_by), mainSha: commits.length ? commits[commits.length - 1].sha : running };
  } catch (err) {
    const why = err.response ? `GitHub answered ${err.response.status}` : err.message;
    // The API refuses anonymous calls from a busy shared address. The public commit feed does not.
    try {
      const { data } = await axios.get(`https://github.com/${repo}/commits/${branch}.atom`, {
        timeout: 15000, headers: { 'User-Agent': 'infra-bot' }, responseType: 'text'
      });
      const got = health.behindFromFeed(data, running);
      if (got) return { running, ...got };
    } catch (err2) {
      console.warn(`[bot-health] commit feed not read: ${err2.message}`);
    }
    return { running, error: why };
  }
}

/** kind 'daily' | 'restart'. Window: since the last health post that reached Slack. */
async function postBotHealth(kind) {
  const now = new Date();
  const runs = healthCol('cron_runs');
  const [last] = await runs.find({ job: 'bot-health', posted: true }).sort({ started: -1 }).limit(1).toArray();
  const [first] = last ? [] : await runs.find({}).sort({ started: 1 }).limit(1).toArray();
  const { since, capped, fromFirst } = health.windowStart({ lastPost: last && last.started, firstRecord: first && first.started, now });
  const recent = await runs.find({ $or: [
    { started: { $gt: new Date(+since - 3 * 3600 * 1000 - 5 * 60 * 1000) } },
    { ended: { $gt: since } }
  ] }).toArray();
  let heartbeat = null;
  try {
    heartbeat = await healthCol('v2_heartbeat').findOne({ _id: 'pc' });
  } catch (err) {
    console.warn(`[bot-health] v2 heartbeat not read: ${err.message}`);
  }
  const deploy = await deployState();
  const msg = health.healthPost({ kind, now, since, capped, fromFirst, runs: recent, heartbeat, env: process.env, upSince: PROCESS_START, deploy });
  return postHealthMessage('bot-health', msg);
}

/** Tue 14:00 / Wed 10:00: a red post only if Monday's v2 results are missing or the PC job stopped. */
async function runV2Watch() {
  const heartbeat = await healthCol('v2_heartbeat').findOne({ _id: 'pc' });
  const msg = health.v2WatchPost(heartbeat, new Date());
  if (!msg) {
    console.log('[v2-watch] Monday v2 results are in and the PC job ran in the last 3 hours');
    return 'quiet';
  }
  return postHealthMessage('v2-watch', msg);
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

    // Every scheduled job is in bot_health.JOBS with its cron (IST); at() schedules it from there, so the
    // bot-health check and the real schedule are one list. fn gets { slot }: the IST minute it was due.
    // A new cron = one JOBS line + one at() call (test_bot_health.js checks they match).
    const at = (job, fn) => {
      const crons = health.cronsOf(job);
      if (!crons.length) console.error(`[cron] ${job} is not in bot_health.JOBS, so it is NOT scheduled`);
      for (const expr of crons) {
        cron.schedule(expr, () => {
          const slot = health.slotKey(new Date());
          Promise.resolve().then(() => fn({ slot })).catch(err => console.error(`[${job}] ${err.message}`));
        }, { timezone: 'Asia/Kolkata' });
      }
    };

    // Daily Renewal Check at 10:00 AM IST, Mon–Fri only
    at('noon-summary', ({ slot }) => {
      console.log(`[CRON] Daily summary firing at ${new Date().toISOString()}`);
      return track('noon-summary', runNoonSummary, { slot });
    });

    // Smartlead dashboard sync at 10:00 AM IST, every day. run.py exits 2 when any sheet tab was not
    // written (the Last Sync tab lists which and why); anything else non-zero is a crash before the
    // verdict, so the ledger may be incomplete. Both are logged as FAILED (bot_health.JOBS codes).
    at('smartlead-sync', async ({ slot }) => {
      console.log(`[CRON] Smartlead sync firing at ${new Date().toISOString()}`);
      if (await onceToday('smartleadSync')) return runJob('smartlead-sync', ['run.py'], { slot });
    });

    // Auto placement-test executor at 11:00 AM IST daily (1h after sync so
    // health data is fresh). Ships DRY-RUN (RETEST_ENABLED=false) — spends
    // nothing until explicitly enabled via the RETEST_ENABLED env var.
    at('retest', ({ slot }) => runJob('retest', ['retest_executor.py'], { slot }));

    // Weekly placement batch, Thursdays, every two hours from 10:00 IST. Each
    // run fires at most one wave (PLACEMENT_WAVE_SIZE) and only once the prior
    // wave has cleared, so ~19 domains cover in four waves without a
    // long-running process. Firing all at once starved the seed queue.
    at('placement-batch', ({ slot }) => {
      const args = ['placement_executor.py'];
      if (process.env.PLACEMENT_ENABLED !== 'true') args.push('--dry-run');
      return runJob('placement-batch', args, { slot });
    });

    // Deliverability-test copy sync: Sunday 18:30 and Monday/Tuesday 05:00 IST, i.e.
    // before the 06:00 scheduled tests. Copies the first step of each client's
    // campaign that sent the most emails this week (still-running campaigns with
    // leads left first; nothing this week -> last week) into every test campaign,
    // the five "DT ..." ones included, so the tests send what is really running. Posts a summary to Slack.
    // Writes by default; COPY_SYNC_DISABLED=true makes it a dry run. Never writes to
    // a live campaign. Once a day (lock); a bot that starts late catches it up before the tests.
    const runCopySync = async ({ slot }) => {
      if (!(await onceToday('copy-sync'))) return;
      console.log(`[CRON] Test-copy sync firing at ${new Date().toISOString()}`);
      const args = ['copy_sync.py'];
      if (process.env.COPY_SYNC_DISABLED !== 'true') args.push('--apply');
      return runJob('copy-sync', args, { slot });
    };
    at('copy-sync', runCopySync);

    // Deliverability report, Mondays and Tuesdays, hourly 08:00-12:00 IST plus a final
    // run at 13:00 (tests start 06:00). Each run reports only tests not reported before, and only once
    // 90% of a test's seeds are classified (SmartDelivery marks a test COMPLETED
    // before that); the 13:00 run reports whatever is in. Posts to Slack (channel
    // C0AGVSUNEFP), keeps per-inbox status in Mongo and writes the Inbox Status
    // tab Campaign Desk reads. Grid cells only with PLACEMENT_GRID_ENABLED=true.
    // PLACEMENT_REPORT_DISABLED=true makes it print-only. The two accounts run one after the
    // other: each reads, clears and rewrites the same Inbox Status tab (audit PLACEMENT-6).
    const runPlacementReport = async (final, { slot }) => {
      if (final && !(await onceToday('placement-report-final'))) return;
      const today = new Date().toISOString().slice(0, 10);
      for (const account of ['PRECISE_LEADS', 'BETTRDATA']) {
        const args = ['placement_report.py', '--account', account, '--since', today, '--only-new'];
        if (final) args.push('--final');
        if (process.env.PLACEMENT_REPORT_DISABLED !== 'true') args.push('--post', '--save', '--status-tab');
        if (process.env.PLACEMENT_GRID_ENABLED === 'true') args.push('--grid');
        await runJob(final ? 'placement-report-final' : 'placement-report', args, { slot, label: account });
      }
    };
    at('placement-report', s => runPlacementReport(false, s));
    at('placement-report-final', s => runPlacementReport(true, s));

    // Placement result collector, 12:30 and 16:30 IST daily. Thursday's batch
    // lands in the first window; the later run catches stragglers, and the
    // other days pick up any on-demand test. Collect-only: never creates.
    at('placement-collect', ({ slot }) => runJob('placement-collect', ['placement_executor.py', '--collect-only'], { slot }));

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
    // at least one mailbox is below threshold; 1 means an account could not be
    // read or the job crashed, which is logged as FAILED, never as all clear.
    at('placement-summary', ({ slot }) => runJob('placement-summary', ['placement_summary.py'], { slot }));

    // API-key health watchdog at 09:45 IST daily — 15 minutes before the sync,
    // so a rotated/revoked key is reported BEFORE the day's jobs run blind on
    // it. Read-only, no enable flag. Exit code 2 = dead key, 1 = unreachable.
    at('key-health', ({ slot }) => runJob('key-health', ['key_health_monitor.py'], { slot }));

    // Off the schedule since 7 Nov 2026 (mailbox-maintenance audit): EmailGuard tests (EmailGuard is not used),
    // NC placement tests (they raise an inbox's daily limit, against the never-raise rule), the warm-up executor
    // and inbox rotation (both judged unsafe in docs/AUDIT_2026-10-08.md). Each was one env flag away from
    // writing to live Smartlead. The scripts stay in smartlead_sync/ and can be run by hand.

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
    at('bounce-protect', ({ slot }) => runJob('bounce-protect', ['bounce_protect_executor.py'], { slot }));

    // Blacklist monitor at 9:00 AM IST every Monday. Read-only (DNSBL lookups
    // via Google DoH against Spamhaus DBL / SURBL / URIBL) — no enable flag needed.
    at('blacklist', ({ slot }) => runJob('blacklist', ['blacklist_monitor.py'], { slot }));

    // Gate self-test, daily 11:30 IST, after the 10:00 sync has written the inbox list. Read-only for
    // Smartlead. It reads the tags, the v2 verdicts, the published list and the live campaigns again and
    // checks that they agree; it posts only when one does not, and every Monday. GATE_SELFTEST=off stops it.
    at('gate-selftest', ({ slot }) => {
      if (process.env.GATE_SELFTEST === 'off') return;
      return runJob('gate-selftest', ['gate_selftest.py', '--save', '--post'], { slot });
    });

    // Capacity planner at 9:30 AM IST every Monday (after the blacklist run).
    // Read-only: writes the Capacity advisory tab + domain registry only.
    at('capacity', ({ slot }) => runJob('capacity', ['capacity_planner.py'], { slot }));

    // Zapmail → /infra tracker sync at 9:30 AM IST: expiry dates, lapsed
    // domains, new current-client domains/inboxes. Writes only when
    // ZAPMAIL_ASSET_SYNC_ENABLED=true (otherwise logs a preview), then
    // re-syncs the tracker sheet. ScaledMail has its own sync at 9:35.
    at('zapmail-sync', async ({ slot }) => {
      const apply = process.env.ZAPMAIL_ASSET_SYNC_ENABLED === 'true';
      console.log(`[CRON] Zapmail tracker sync (${apply ? 'apply' : 'preview'}) firing at ${new Date().toISOString()}`);
      const code = await runJob('zapmail-sync', ['zapmail_asset_sync.py', ...(apply ? ['--apply'] : [])], { slot });
      if (apply && code === 0) {
        resyncAssetSheet().catch(err => console.warn('[zapmail-sync] sheet refresh failed:', err.message));
      }
    });

    // Zapmail daily digest at 9:40 AM IST (read-only): wallets, domains
    // expiring soon, purchase batches due or needing reconcile. Posts only when
    // ZAPMAIL_NOTIFY_CHANNEL is set; otherwise it just logs.
    at('zapmail-digest', ({ slot }) => runJob('zapmail-digest', ['zapmail_digest.py'], { slot }));

    // ScaledMail tracker sync at 9:35 IST: writes only when
    // SCALEDMAIL_ASSET_SYNC_ENABLED=true; skips without a key.
    at('scaledmail-sync', async ({ slot }) => {
      if (!process.env.SCALEDMAIL_API_KEY) return;
      const apply = process.env.SCALEDMAIL_ASSET_SYNC_ENABLED === 'true';
      console.log(`[CRON] ScaledMail tracker sync (${apply ? 'apply' : 'preview'}) firing at ${new Date().toISOString()}`);
      const code = await runJob('scaledmail-sync', ['scaledmail_cli.py', 'sync', ...(apply ? ['--apply'] : [])], { slot });
      if (apply && code === 0) {
        resyncAssetSheet().catch(err => console.warn('[scaledmail-sync] sheet refresh failed:', err.message));
      }
    });
    // (The ScaledMail digest is part of the 9:40 "Domains & inboxes daily".)

    // Every 2 hours at :15. Inbox intake (spec 2026-10-13): every new inbox -> the team's
    // standard, once, proven by read-back, when INBOX_INTAKE_ENABLED=true. Until then the
    // old ScaledMail readiness job (smartlead/inbox_ready.py: signature, client, tags) runs
    // when INBOX_READY_ENABLED=true and a ScaledMail key is set; it posts only when
    // something became ready.
    at('inbox-intake', ({ slot }) => {
      if (process.env.INBOX_INTAKE_ENABLED !== 'true') return;
      console.log(`[CRON] Inbox intake firing at ${new Date().toISOString()}`);
      return runJob('inbox-intake', ['inbox_intake_cli.py', '--apply', '--post', '--json'], { slot });
    });
    at('inbox-ready', ({ slot }) => {
      if (process.env.INBOX_INTAKE_ENABLED === 'true' || process.env.INBOX_READY_ENABLED !== 'true') return;
      if (!process.env.SCALEDMAIL_API_KEY) return;
      console.log(`[CRON] Inbox readiness firing at ${new Date().toISOString()}`);
      return runJob('inbox-ready', ['scaledmail_cli.py', '--json', 'ready', '--apply', '--post'], { slot });
    });

    // GoDaddy -> tracker at 9:37 AM IST: domains the bot bought on GoDaddy get a
    // row (client from the purchase ledger), and every GoDaddy domain's expiry
    // follows GoDaddy. Writes only with GODADDY_ASSET_SYNC_ENABLED=true; skips
    // without GODADDY_PAT.
    at('godaddy-sync', async ({ slot }) => {
      if (!process.env.GODADDY_PAT) return;
      const apply = process.env.GODADDY_ASSET_SYNC_ENABLED === 'true';
      console.log(`[CRON] GoDaddy tracker sync (${apply ? 'apply' : 'preview'}) firing at ${new Date().toISOString()}`);
      const code = await runJob('godaddy-sync', ['godaddy_cli.py', 'sync', ...(apply ? ['--apply'] : [])], { slot });
      if (apply && code === 0) {
        resyncAssetSheet().catch(err => console.warn('[godaddy-sync] sheet refresh failed:', err.message));
      }
    });

    // Infra audit at 10:20 AM IST Mon-Fri (read-only): MX / DMARC policy,
    // name-server footprint, redirects, main-domain use, mailbox and domain
    // caps, young inboxes in campaigns, warmup, signatures, provider mix, ESP
    // matching, SMTP IP blacklists. Posts only when INFRA_AUDIT_CHANNEL is set.
    at('infra-audit', ({ slot }) => {
      const args = ['infra_audit.py', ...(process.env.INFRA_AUDIT_CHANNEL ? ['--post'] : [])];
      return runJob('infra-audit', args, { slot });
    });

    // Inbox billing: did every Zapmail subscription / ScaledMail order renew?
    // Every 2 hours 9-21 IST; posts only changes (not renewed, payment failed,
    // cancelled, bought outside the bot) to RENEWAL_REVIEW_CHANNEL / ZAPMAIL_NOTIFY_CHANNEL.
    // Renewal review at 9:50 IST: inboxes on bills due in 3 days → keep / retire / check.
    at('billing-watch', ({ slot }) => runJob('billing-watch', ['billing_watch.py'], { slot }));
    // --flag-tracker marks DROP / CHECK on the tracker rows, so the 10:00 Daily
    // Renewal Check shows "don't renew" next to them (7 days ahead).
    at('renewal-review', ({ slot }) => runJob('renewal-review', ['renewal_review.py', '--days', '7', '--post', '--flag-tracker'], { slot }));

    // Kill warnings, Tuesday 13:30 IST, after the Mon/Tue placement reports:
    // which inboxes SHOULD be killed by the team's rule (two strikes, <50% on
    // 20+ seeds, or a dead domain) and which to watch. Warning only. Once a day
    // (lock); a bot that starts late catches it up the same afternoon.
    const runKillReport = async ({ slot }) => {
      if (await onceToday('kill-report')) return runJob('kill-report', ['kill_report.py', '--post'], { slot });
    };
    at('kill-report', runKillReport);

    // Fleet watch (spec 2026-10-15): daily record of every inbox, new problems
    // with what to do (10:30 IST), scorecard Mondays 10:50. Never writes to Smartlead.
    // Once a day each, like the other daily jobs: two instances overlap during a deploy.
    at('fleet-watch', async ({ slot }) => {
      if (await onceToday('fleet-watch')) return runJob('fleet-watch', ['fleet_watch.py', '--post'], { slot });
    });
    at('fleet-scorecard', async ({ slot }) => {
      if (await onceToday('fleet-scorecard')) return runJob('fleet-scorecard', ['fleet_watch.py', '--scorecard', '--post'], { slot });
    });

    // Every current-client inbox on its client's deliverability test campaigns
    // (paused only), daily 06:00 IST, so any inbox can go into the Monday test.
    at('dt-senders', async ({ slot }) => {
      if (await onceToday('dt-senders')) return runJob('dt-senders', ['dt_senders.py', '--apply'], { slot });
    });

    // Campaign stats (Manveen, 29 Oct 2026). 09:00 IST every day: Campaign
    // Metrics tabs + today's Campaign History rows (one row per campaign per
    // day, so progress is visible). 10:15 IST Mon-Fri: the last working day's
    // email + LinkedIn numbers (and week / month), in #pl_daily before the 11:00 standup.
    // The Mongo lock keeps a second instance (during a deploy) from posting twice.
    // CAMPAIGN_STATS_POST=off stops the post; the 09:00 history keeps running.
    at('campaign-metrics', async ({ slot }) => {
      if (await onceToday('campaign-metrics')) return runJob('campaign-metrics', ['metrics_only.py'], { slot });
    });
    // 10:15 since 2 Nov: the post now counts by date from the platforms
    // (several minutes), so it still lands before the 11:00 standup.
    const runStatsPost = async ({ slot }) => {
      if (process.env.CAMPAIGN_STATS_POST === 'off') return;
      if (await onceToday('campaign-stats-post')) return runJob('campaign-stats-post', ['campaign_stats_post.py', '--post'], { slot });
    };
    at('campaign-stats-post', runStatsPost);

    // Microsoft / Proofpoint reach (Manveen, 1 Nov 2026): Mondays 10:40 IST in
    // #pl_daily, after the stats post. Real sends, each campaign against its own
    // Google recipients (seed tests said 7/7 inbox while BettrData's Microsoft
    // recipients answered at 0.11x). Report only. PROVIDER_WATCH_POST=off stops it.
    at('provider-watch', async ({ slot }) => {
      if (process.env.PROVIDER_WATCH_POST === 'off') return;
      if (await onceToday('provider-watch')) return runJob('provider-watch', ['provider_reply_watch.py', '--post'], { slot });
    });

    // Weekly volume review, Tuesday 13:45 IST, after Kill warnings: hold or step back
    // daily limits per domain; it never raises one (smartlead/volume_rule.py). Changes only the pilot
    // domains (volume_review.py --pilot-add); every other domain is reported.
    at('volume-review', ({ slot }) => {
      if (process.env.VOLUME_REVIEW_ENABLED !== 'true') return;
      return runJob('volume-review', ['volume_review.py', '--post', '--json'], { slot });
    });

    // Per-domain reply-rate early warning at 1:00 PM IST daily (read-only).
    at('reply-monitor', ({ slot }) => runJob('reply-monitor', ['reply_monitor.py'], { slot }));

    // Simple one-line reminder at 4:00 PM IST, Mon–Fri only
    at('reminder', ({ slot }) => {
      console.log(`[CRON] Simple reminder firing at ${new Date().toISOString()}`);
      return track('reminder', runSimpleReminder, { slot });
    });

    // Bot health, weekdays 10:05 IST (bot_health.js): runs that did not happen or failed since the
    // last health post, the PC v2 job's heartbeat, and the live-write flags that are on.
    at('bot-health', async ({ slot }) => {
      if (await onceToday('bot-health')) {
        return track('bot-health', () => postBotHealth('daily'), { slot, extra: { kind: 'daily' } });
      }
    });
    // Tue 14:00 and Wed 10:00 IST: a red post if Monday's v2 results are not in Mongo or the
    // hourly PC job has not run for 3 hours. Quiet otherwise.
    at('v2-watch', async ({ slot }) => {
      if (await onceToday('v2-watch')) return track('v2-watch', runV2Watch, { slot });
    });

    // Started late (restart, deploy, Render waking up)? Catch up the runs still worth doing
    // (stats_catch_up.js); each runner's once-a-day lock skips a slot that already ran.
    const CATCH_UP = {
      'campaign-stats-post': runStatsPost,
      'kill-report': runKillReport,
      'placement-report-final': s => runPlacementReport(true, s),
      'copy-sync': runCopySync
    };
    const missedAtStart = require('./stats_catch_up').missedRuns(new Date(), process.env);
    if (missedAtStart.length) {
      setTimeout(() => {
        for (const { job, slot } of missedAtStart) {
          console.log(`[${job}] the bot started after its ${slot} run: catching it up unless it already ran`);
          Promise.resolve().then(() => CATCH_UP[job]({ slot }))
            .catch(err => console.error(`[${job}] catch-up failed: ${err.message}`));
        }
      }, 60 * 1000);
    }

    // Once after a restart, 3 minutes in (after the catch-ups started): the bot-health post for
    // the time since the last one. On a weekday after 10:05 with no 10:05 post yet, it is that
    // day's post. At most one restart post an hour, so a crash loop cannot flood the channel.
    setTimeout(async () => {
      try {
        const now = new Date();
        const daily = health.dueDailySlot(now);
        if (daily && (await onceToday('bot-health'))) {
          await track('bot-health', () => postBotHealth('restart'), { slot: daily, extra: { kind: 'restart' } });
        } else if (await takeLock(`bot-health-restart-${health.slotKey(now).slice(0, 13)}`)) {
          await track('bot-health', () => postBotHealth('restart'), { slot: null, extra: { kind: 'restart' } });
        }
      } catch (err) {
        console.error(`[bot-health] restart post failed: ${err.message}`);
      }
    }, 3 * 60 * 1000);

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
