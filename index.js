require('dotenv').config();
const mongoose = require('mongoose');
const { App } = require('@slack/bolt');
const cron = require('node-cron');
const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');
const express = require('express');
const multer = require('multer');
const csv = require('csv-parser');
const { Readable } = require('stream');
const axios = require('axios');
const { syncAllAssetsToSheet } = require('./sheets');

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

const PORT = process.env.PORT || 3000;

/* -------------------- MongoDB -------------------- */
mongoose.set('bufferCommands', false);

/* -------------------- Slack App -------------------- */
const app = new App({
  token: process.env.SLACK_BOT_TOKEN,
  appToken: process.env.SLACK_APP_TOKEN,
  socketMode: true
});

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
    const result = await Asset.deleteOne({ name: { $regex: new RegExp(`^${name}$`, 'i') } });
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
    const existing = await Asset.findOne({ name: { $regex: new RegExp(`^${name}$`, 'i') } });
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

/* -------------------- Slash Command -------------------- */
app.command('/infra', async ({ ack, body, client, command }) => {
  await ack();
  const text = command.text.trim();

  if (text === 'add') {
    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: 'modal',
        callback_id: 'ADD_ASSET_MODAL',
        title: { type: 'plain_text', text: 'Add Asset' },
        submit: { type: 'plain_text', text: 'Save' },
        close: { type: 'plain_text', text: 'Cancel' },
        blocks: [
          {
            type: 'input',
            block_id: 'type',
            label: { type: 'plain_text', text: 'Asset Type' },
            element: {
              type: 'static_select',
              action_id: 'value',
              options: [
                { text: { type: 'plain_text', text: 'Domain' }, value: 'DOMAIN' },
                { text: { type: 'plain_text', text: 'Inbox' }, value: 'INBOX' }
              ]
            }
          },
          {
            type: 'input',
            block_id: 'name',
            label: { type: 'plain_text', text: 'Name (Domain / Email)' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., example.com or user@example.com' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'client',
            label: { type: 'plain_text', text: 'Client' },
            element: { type: 'plain_text_input', action_id: 'value' }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'provider',
            label: { type: 'plain_text', text: 'Provider' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., Zapmail, GoDaddy' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'workspace',
            label: { type: 'plain_text', text: 'Workspace' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., Google, Outlook' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'campaign',
            label: { type: 'plain_text', text: 'Campaign' },
            element: { type: 'plain_text_input', action_id: 'value' }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'purchaseDate',
            label: { type: 'plain_text', text: 'Purchase Date (DD/MM/YYYY)' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., 04/06/2025' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'expiryDate',
            label: { type: 'plain_text', text: 'Expiry Date (DD/MM/YYYY)' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'Leave blank for auto-calculation' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'status',
            label: { type: 'plain_text', text: 'Status' },
            element: {
              type: 'static_select',
              action_id: 'value',
              options: [
                { text: { type: 'plain_text', text: 'Active' }, value: 'Active' },
                { text: { type: 'plain_text', text: 'Inactive' }, value: 'Inactive' },
                { text: { type: 'plain_text', text: 'Not In Use' }, value: 'Not In Use' }
              ]
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'brandedPrewarmed',
            label: { type: 'plain_text', text: 'Branded / Pre-warmed?' },
            element: {
              type: 'static_select',
              action_id: 'value',
              options: [
                { text: { type: 'plain_text', text: 'Yes — Branded' }, value: 'Branded' },
                { text: { type: 'plain_text', text: 'Yes — Pre-warmed' }, value: 'Pre-warmed' },
                { text: { type: 'plain_text', text: 'No' }, value: 'No' }
              ]
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'primaryOwner',
            label: { type: 'plain_text', text: 'Primary Owner' },
            element: { type: 'users_select', action_id: 'value' }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'visibilityChannel',
            label: { type: 'plain_text', text: 'Visibility Channel' },
            element: { type: 'channels_select', action_id: 'value' }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'cost',
            label: { type: 'plain_text', text: 'Cost (Yearly for Domains, Monthly for Inboxes)' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., 3.25' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'currency',
            label: { type: 'plain_text', text: 'Currency' },
            element: {
              type: 'static_select',
              action_id: 'value',
              initial_option: { text: { type: 'plain_text', text: 'USD' }, value: 'USD' },
              options: [
                { text: { type: 'plain_text', text: 'USD' }, value: 'USD' },
                { text: { type: 'plain_text', text: 'INR' }, value: 'INR' },
                { text: { type: 'plain_text', text: 'EUR' }, value: 'EUR' },
                { text: { type: 'plain_text', text: 'GBP' }, value: 'GBP' }
              ]
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'notes',
            label: { type: 'plain_text', text: 'Notes' },
            element: { type: 'plain_text_input', action_id: 'value', multiline: true }
          }
        ]
      }
    });

  } else if (text === 'renew') {
    await client.views.open({
      trigger_id: body.trigger_id,
      view: {
        type: 'modal',
        callback_id: 'RENEW_ASSET_MODAL',
        title: { type: 'plain_text', text: 'Renew Asset' },
        submit: { type: 'plain_text', text: 'Renew' },
        close: { type: 'plain_text', text: 'Cancel' },
        blocks: [
          {
            type: 'input',
            block_id: 'name',
            label: { type: 'plain_text', text: 'Domain or Inbox name' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., example.com or user@example.com' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'purchaseDate',
            label: { type: 'plain_text', text: 'New Purchase Date (DD/MM/YYYY)' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'e.g., 24/02/2026' }
            }
          },
          {
            type: 'input',
            optional: true,
            block_id: 'expiryDate',
            label: { type: 'plain_text', text: 'New Expiry Date (DD/MM/YYYY)' },
            element: {
              type: 'plain_text_input',
              action_id: 'value',
              placeholder: { type: 'plain_text', text: 'Leave blank to auto-calculate from purchase date' }
            }
          }
        ]
      }
    });

  } else if (text === 'list') {
    const assets = await Asset.find().sort({ type: 1, name: 1 });
    const prepared = prepareAssetsForSheet(assets);

    let message = '📋 *Infrastructure Assets*\n\n';
    const domains = prepared.filter(a => a.type === 'DOMAIN');
    const inboxes = prepared.filter(a => a.type === 'INBOX');

    if (domains.length > 0) {
      message += `*DOMAINS (${domains.length})*\n`;
      domains.forEach(d => {
        message += `• ${d.name} — ${d.status || 'N/A'} — ${d.daysLeft !== null ? `${d.daysLeft} days left` : 'No expiry'}\n`;
      });
      message += '\n';
    }

    if (inboxes.length > 0) {
      message += `*INBOXES (${inboxes.length})*\n`;
      inboxes.forEach(i => {
        message += `• ${i.name} — ${i.status || 'N/A'} — ${i.daysLeft !== null ? `${i.daysLeft} days left` : 'No expiry'}\n`;
      });
    }

    await client.chat.postMessage({ channel: body.user_id, text: message });
  }
});

/* -------------------- Unified Message Handler -------------------- */
app.message(async ({ message, client }) => {
  try {
    if (message.subtype) return;

    if (message.files && message.files.length > 0) {
      for (const file of message.files) {
        if (!file.mimetype?.includes('csv') && !file.name?.endsWith('.csv')) continue;

        const fileName = file.name.toLowerCase();
        const isDelete = fileName.startsWith('delete_');
        const isRenew = fileName.startsWith('renew_');

        let assetType = null;
        if (fileName.includes('domain')) assetType = 'DOMAIN';
        else if (fileName.includes('inbox')) assetType = 'INBOX';

        if (!assetType) {
          await client.chat.postMessage({
            channel: message.channel,
            text: `Couldn't detect asset type from filename *${file.name}*. Please name your file with "domain" or "inbox" in it.`
          });
          continue;
        }

        await client.chat.postMessage({
          channel: message.channel,
          text: isDelete
            ? `Detected *${assetType}* delete CSV — removing listed entries from *${file.name}*...`
            : isRenew
            ? `Detected *${assetType}* renewal CSV — updating dates from *${file.name}*...`
            : `Detected *${assetType}* CSV — importing *${file.name}*...`
        });

        const downloadUrl = file.url_private_download || file.url_private;
        const response = await axios.get(downloadUrl, {
          headers: { Authorization: `Bearer ${process.env.SLACK_BOT_TOKEN}` },
          responseType: 'arraybuffer'
        });

        const buffer = Buffer.from(response.data);
        const assets = await parseCSVBuffer(buffer, assetType);

        if (assets.length === 0) {
          await client.chat.postMessage({
            channel: message.channel,
            text: `No valid rows found in *${file.name}*. Check that your column headers match the expected format.`
          });
          continue;
        }

        if (isRenew) {
          let renewed = 0, notFound = 0;
          for (const asset of assets) {
            const existing = await Asset.findOne({ name: asset.name });
            if (!existing) { notFound++; continue; }
            const updates = { updatedAt: new Date(), remindersSent: [] };
            if (asset.purchaseDate) updates.purchaseDate = asset.purchaseDate;
            if (asset.expiryDate) updates.expiryDate = asset.expiryDate;
            if (asset.purchaseDate && !asset.expiryDate) updates.expiryDate = null;
            await Asset.findOneAndUpdate({ name: asset.name }, updates);
            renewed++;
          }
          const allAssets = await Asset.find();
          await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
          await client.chat.postMessage({
            channel: message.channel,
            text: `Renewed ${renewed} asset${renewed !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Reminder history cleared. Google Sheets synced ✓`
          });

        } else if (isDelete) {
          let deleted = 0, notFound = 0;
          for (const asset of assets) {
            const result = await Asset.deleteOne({ name: asset.name });
            if (result.deletedCount > 0) deleted++;
            else notFound++;
          }
          const allAssets = await Asset.find();
          await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
          await client.chat.postMessage({
            channel: message.channel,
            text: `Deleted ${deleted} asset${deleted !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Google Sheets synced ✓`
          });

        } else {
          let inserted = 0, updated = 0;
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
          await client.chat.postMessage({
            channel: message.channel,
            text: `*${file.name}* imported successfully!\n• Inserted: ${inserted}\n• Updated: ${updated}\n• Total: ${assets.length}\n• Google Sheets synced ✓`
          });
        }
      }
      return;
    }

    if (!message.text) return;

    const lines = message.text.trim().split(/\r?\n/).map(l => l.trim()).filter(Boolean);

    if (lines.length < 2) return;

    const command = lines[0].toLowerCase();
    if (command !== 'delete' && command !== 'renew') return;

    const names = lines.slice(1).map(l => {
      const mailtoMatch = l.match(/<mailto:[^|]+\|([^>]+)>/);
      if (mailtoMatch) return mailtoMatch[1].trim().toLowerCase();
      return l.trim().toLowerCase();
    }).filter(Boolean);

    if (command === 'delete') {
      let deleted = 0, notFound = 0;
      for (const name of names) {
        let result = await Asset.deleteOne({ name });
        if (result.deletedCount === 0) {
          result = await Asset.deleteOne({ name: { $regex: new RegExp(`^${name}$`, 'i') } });
        }
        if (result.deletedCount > 0) deleted++;
        else notFound++;
      }
      const allAssets = await Asset.find();
      await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
      await client.chat.postMessage({
        channel: message.channel,
        text: `Deleted ${deleted} asset${deleted !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Google Sheets synced ✓`
      });

    } else if (command === 'renew') {
      let renewed = 0, notFound = 0;
      const today = dayjs().startOf('day');
      for (const name of names) {
        const existing = await Asset.findOne({ name: { $regex: new RegExp(`^${name}$`, 'i') } });
        if (!existing) { notFound++; continue; }

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
        renewed++;
      }
      const allAssets = await Asset.find();
      await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
      await client.chat.postMessage({
        channel: message.channel,
        text: `Renewed ${renewed} asset${renewed !== 1 ? 's' : ''}${notFound > 0 ? `, ${notFound} not found` : ''}. Purchase date set to today, expiry auto-calculated. Google Sheets synced ✓`
      });
    }

  } catch (err) {
    console.error('Message handler error:', err);
    try {
      await client.chat.postMessage({ channel: message.channel, text: `Error: ${err.message}` });
    } catch (_) {}
  }
});

/* -------------------- View Details Button Handlers -------------------- */
app.action(/^view_details_/, async ({ ack, body, client }) => {
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

/* -------------------- Modal Submit Handler -------------------- */
app.view('ADD_ASSET_MODAL', async ({ ack, body, view, client }) => {
  await ack();

  const v = view.state.values;
  const type = v.type.value.selected_option.value;

  const asset = {
    type,
    name: v.name.value.value.trim().toLowerCase(),
    client: v.client?.value?.value?.trim() || null,
    provider: v.provider?.value?.value?.trim() || null,
    workspace: v.workspace?.value?.value?.trim() || null,
    campaign: v.campaign?.value?.value?.trim() || null,
    purchaseDate: parseDate(v.purchaseDate?.value?.value),
    expiryDate: parseDate(v.expiryDate?.value?.value),
    status: v.status?.value?.selected_option?.value || 'Active',
    brandedPrewarmed: v.brandedPrewarmed?.value?.selected_option?.value || null,
    primaryOwner: resolveOwner(v.primaryOwner?.value?.selected_user) || null,
    visibilityChannel: v.visibilityChannel?.value?.selected_channel || null,
    yearlyCost: type === 'DOMAIN' ? parseCost(v.cost?.value?.value) : null,
    monthlyCost: type === 'INBOX' ? parseCost(v.cost?.value?.value) : null,
    currency: v.currency?.value?.selected_option?.value || 'USD',
    notes: v.notes?.value?.value?.trim() || null,
    createdBy: body.user.id,
    updatedAt: new Date()
  };

  if (type === 'INBOX' && asset.name.includes('@')) {
    asset.domain = asset.name.split('@')[1];
  }

  try {
    await Asset.findOneAndUpdate({ name: asset.name }, asset, { upsert: true, new: true });

    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));

    await client.chat.postMessage({
      channel: body.user.id,
      text: `✅ *${asset.name}* (${type}) saved successfully!\n` +
            `Status: ${asset.status}\n` +
            `Expiry: ${asset.expiryDate ? dayjs(asset.expiryDate).format('DD/MM/YYYY') : 'Auto-calculated'}`
    });
  } catch (error) {
    console.error('Error saving asset:', error);
    await client.chat.postMessage({
      channel: body.user.id,
      text: `❌ Error saving asset: ${error.message}`
    });
  }
});

/* -------------------- Renew Asset Modal Handler -------------------- */
app.view('RENEW_ASSET_MODAL', async ({ ack, body, view, client }) => {
  await ack();

  const v = view.state.values;
  const name = v.name.value.value.trim().toLowerCase();
  const newPurchaseDate = parseDate(v.purchaseDate?.value?.value);
  const newExpiryDate = parseDate(v.expiryDate?.value?.value);

  try {
    const asset = await Asset.findOne({ name });

    if (!asset) {
      await client.chat.postMessage({
        channel: body.user.id,
        text: `❌ Asset *${name}* not found in the database. Check the name and try again.`
      });
      return;
    }

    const updates = { updatedAt: new Date(), remindersSent: [] };
    if (newPurchaseDate) updates.purchaseDate = newPurchaseDate;
    if (newExpiryDate) updates.expiryDate = newExpiryDate;
    if (newPurchaseDate && !newExpiryDate) updates.expiryDate = null;

    await Asset.findOneAndUpdate({ name }, updates);

    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));

    const updated = await Asset.findOne({ name });
    const daysLeft = computeDaysLeft(updated);

    await client.chat.postMessage({
      channel: body.user.id,
      text: `✅ *${name}* renewed successfully!\n` +
            `• New Purchase Date: ${newPurchaseDate ? dayjs(newPurchaseDate).format('DD/MM/YYYY') : 'Unchanged'}\n` +
            `• New Expiry Date: ${updates.expiryDate ? dayjs(updates.expiryDate).format('DD/MM/YYYY') : 'Auto-calculated'}\n` +
            `• Days until expiry: ${daysLeft !== null ? daysLeft : 'N/A'}\n` +
            `• Reminder history cleared ✓\n` +
            `• Google Sheets synced ✓`
    });

  } catch (error) {
    console.error('Error renewing asset:', error);
    await client.chat.postMessage({
      channel: body.user.id,
      text: `❌ Error renewing asset: ${error.message}`
    });
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
          text: `🔗 <https://infra-bot.onrender.com/|View full dashboard> to renew or manage assets`
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
            text: `🔗 <https://infra-bot.onrender.com/|View full dashboard> to manage these assets`
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
        key: 'noon_0'
      },
      {
        assets: active.filter(a => a.daysLeft === wd1CalDays),
        label: `Tomorrow (${dayjs().add(wd1CalDays, 'day').format('ddd DD MMM')})`,
        icon: '🟠',
        key: 'noon_1'
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
        key: 'noon_3'
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
        const { label, icon, key, assets: gAssets } = g;
        if (gAssets.length === 0) continue;

        const domains = gAssets.filter(a => a.type === 'DOMAIN');
        const inboxes = gAssets.filter(a => a.type === 'INBOX');

        const parts = [];
        if (domains.length) parts.push(`*${domains.length} domain${domains.length !== 1 ? 's' : ''}*`);
        if (inboxes.length) parts.push(`*${inboxes.length} inbox${inboxes.length !== 1 ? 'es' : ''}*`);
        const summaryText = `${icon}  ${parts.join(' and ')} expire${gAssets.length === 1 ? 's' : ''} *${label}*`;

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
          for (const a of items) {
            const providerPart = a.provider ? ` · ${a.provider}` : '';
            const typeIcon = a.type === 'DOMAIN' ? '🌐' : '📧';
            detail += `  ${typeIcon} ${a.name}${providerPart}\n`;
          }
        }

        blocks.push({
          type: 'section',
          text: { type: 'mrkdwn', text: summaryText },
          accessory: {
            type: 'button',
            text: { type: 'plain_text', text: 'Read more' },
            action_id: `view_details_${key}`,
            value: JSON.stringify({ detail: detail.trim(), channel: CHANNEL })
          }
        });
        blocks.push({ type: 'divider' });
      }
    }

    blocks.push({
      type: 'context',
      elements: [{
        type: 'mrkdwn',
        text: `🔗 <https://infra-bot.onrender.com/|Open dashboard> to renew or manage assets`
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
    const CHANNEL = 'C0AGVSUNEFP';
    console.log('[runSimpleReminder] Sending 5:15 PM reminder...');
    await app.client.chat.postMessage({
      token: process.env.SLACK_BOT_TOKEN,
      channel: CHANNEL,
      text: `🔔 Reminder: Please review and renew any expiring domains or inboxes → https://infra-bot.onrender.com/`
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

    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
    console.log('✅ Google Sheets synced on startup');

    await app.start();
    console.log('✅ Slack bot running in socket mode');

    expressApp.listen(PORT, () => {
      console.log(`✅ Upload UI running at http://localhost:${PORT}`);
    });

    // Schedule daily structured summary at 4:00 PM IST, Mon–Fri only
    cron.schedule('0 16 * * 1-5', async () => {
      const now = new Date().toISOString();
      console.log(`[CRON] Daily summary firing at ${now}`);
      await runNoonSummary();
    }, {
      timezone: 'Asia/Kolkata'
    });

    // Simple one-line reminder at 5:15 PM IST, Mon–Fri only
    cron.schedule('15 17 * * 1-5', async () => {
      const now = new Date().toISOString();
      console.log(`[CRON] Simple reminder firing at ${now}`);
      await runSimpleReminder();
    }, {
      timezone: 'Asia/Kolkata'
    });

    const nowUtc = new Date().toISOString();
    const nowIst = new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' });
    console.log(`✅ Crons scheduled — 4:00 PM IST summary + 5:15 PM IST reminder (server UTC: ${nowUtc} | IST: ${nowIst})`);

  } catch (error) {
    console.error('❌ Error during startup:', error);
    process.exit(1);
  }
}
start();

process.on('SIGINT', async () => {
  console.log('Shutting down gracefully...');
  await mongoose.connection.close();
  process.exit(0);
});