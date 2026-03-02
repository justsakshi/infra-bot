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
  // Already a Slack ID
  if (trimmed.startsWith('U') && trimmed.length > 5) return trimmed;
  // Look up by name (case-insensitive)
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
  name: { type: String, required: true, unique: true }, // Domain or Inbox email

  // Common fields
  client: String,
  provider: String,         // Provider (e.g. Zapmail, Google) — replaces registrar/inboxProvider
  workspace: String,        // NEW: Workspace (e.g. Google, Outlook)
  campaign: String,         // replaces purposeCampaign
  purchaseDate: Date,
  expiryDate: Date,
  status: String,           // Active, Inactive, Not In Use
  brandedPrewarmed: String, // NEW: Branded/Pre-warmed
  primaryOwner: String,     // Slack User ID
  visibilityChannel: String,// Slack Channel ID
  yearlyCost: Number,       // For DOMAIN only
  monthlyCost: Number,      // For INBOX only
  currency: String,
  notes: String,

  // Inbox-specific
  domain: String,           // For INBOX only (the domain part of the email)

  // Tracking
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
  // DD/MM/YYYY
  const p1 = dayjs(dateString, 'DD/MM/YYYY', true);
  if (p1.isValid()) return p1.toDate();
  // YYYY-MM-DD
  const p2 = dayjs(dateString, 'YYYY-MM-DD', true);
  if (p2.isValid()) return p2.toDate();
  // DD MMM YYYY (e.g. 27 Feb 2026)
  const p3 = dayjs(dateString, 'DD MMM YYYY', true);
  if (p3.isValid()) return p3.toDate();
  // D MMM YYYY (e.g. 7 Feb 2026)
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
      // Use isSameOrBefore so that if purchase date == today, expiry moves to next month
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
  return v.toUpperCase(); // already USD, INR, etc.
}

/**
 * Given today, return the next N working days (Mon-Fri) ahead.
 * e.g. getWorkingDayTargets() returns the dates that are 1 and 3 working days from today.
 * On Thursday: 1 working day = Friday, 3 working days = Tuesday next week.
 * BUT: anything expiring Sat/Sun also gets caught on Thursday (as 1 working day ahead).
 */
function getWorkingDayTargets() {
  const today = dayjs().startOf('day');
  const dayOfWeek = today.day(); // 0=Sun,1=Mon,...,5=Fri,6=Sat

  const targets = new Set();

  // Calculate what date is N working days from today
  const addWorkingDays = (from, n) => {
    let date = from;
    let added = 0;
    while (added < n) {
      date = date.add(1, 'day');
      if (date.day() !== 0 && date.day() !== 6) added++; // skip weekends
    }
    return date;
  };

  const oneWD = addWorkingDays(today, 1);
  const threeWD = addWorkingDays(today, 3);

  targets.add(oneWD.format('YYYY-MM-DD'));
  targets.add(threeWD.format('YYYY-MM-DD'));

  // On Thursday: also include Saturday and Sunday so nothing is missed over the weekend
  if (dayOfWeek === 4) { // Thursday
    targets.add(today.add(2, 'day').format('YYYY-MM-DD')); // Saturday
    targets.add(today.add(3, 'day').format('YYYY-MM-DD')); // Sunday
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

/**
 * Parse a CSV buffer (domain or inbox) and return asset objects.
 * Skips instruction/example rows (first data row where Domain/Inbox cell is empty).
 */
function parseCSVBuffer(buffer, type) {
  return new Promise((resolve, reject) => {
    const assets = [];
    const stream = Readable.from(buffer.toString());

    stream
      .pipe(csv())
      .on('data', (row) => {
        if (type === 'DOMAIN') {
          // Skip empty/example rows
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

          // Auto-extract domain from inbox email if not set
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

/* -------------------- Express HTTP Server (CSV Upload) -------------------- */

const expressApp = express();
const upload = multer({ storage: multer.memoryStorage() });
const path = require('path');

// Serve frontend
expressApp.use(express.json());
expressApp.use(express.static(path.join(__dirname, 'public')));

/* -------------------- REST API -------------------- */

// GET all assets
expressApp.get('/api/assets', async (req, res) => {
  try {
    const assets = await Asset.find().sort({ type: 1, name: 1 });
    const prepared = prepareAssetsForSheet(assets);
    res.json({ assets: prepared });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST create asset
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

// PUT update asset
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

// DELETE asset
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

// POST renew asset
expressApp.post('/api/assets/:name/renew', async (req, res) => {
  try {
    const name = decodeURIComponent(req.params.name);
    const existing = await Asset.findOne({ name: { $regex: new RegExp(`^${name}$`, 'i') } });
    if (!existing) return res.status(404).json({ ok: false, error: 'Not found' });
    await Asset.findOneAndUpdate({ name: existing.name }, {
      purchaseDate: dayjs().startOf('day').toDate(),
      expiryDate: null,
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

// Serve the upload UI (legacy redirect to frontend)
expressApp.get('/upload', (req, res) => {
  res.redirect('/');
});

// Old upload UI replaced — keep the route but point to new frontend
expressApp.get('/old-upload', (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Infra Bot — CSV Upload</title>
  <style>
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      background: #0f0f0f;
      color: #e0e0e0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }

    .card {
      background: #1a1a1a;
      border: 1px solid #2a2a2a;
      border-radius: 16px;
      padding: 40px;
      width: 100%;
      max-width: 520px;
      box-shadow: 0 24px 48px rgba(0,0,0,0.4);
    }

    .logo {
      display: flex;
      align-items: center;
      gap: 10px;
      margin-bottom: 28px;
    }

    .logo-icon {
      width: 36px;
      height: 36px;
      background: linear-gradient(135deg, #6366f1, #8b5cf6);
      border-radius: 8px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 18px;
    }

    h1 {
      font-size: 20px;
      font-weight: 600;
      color: #fff;
    }

    .subtitle {
      font-size: 13px;
      color: #666;
      margin-top: 2px;
    }

    .divider {
      height: 1px;
      background: #2a2a2a;
      margin: 28px 0;
    }

    label.field-label {
      display: block;
      font-size: 12px;
      font-weight: 500;
      color: #888;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 8px;
    }

    select {
      width: 100%;
      background: #111;
      border: 1px solid #2e2e2e;
      border-radius: 8px;
      color: #e0e0e0;
      padding: 10px 14px;
      font-size: 14px;
      margin-bottom: 20px;
      appearance: none;
      background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%23666' stroke-width='1.5' fill='none' stroke-linecap='round'/%3E%3C/svg%3E");
      background-repeat: no-repeat;
      background-position: right 14px center;
      cursor: pointer;
    }

    select:focus { outline: none; border-color: #6366f1; }

    .drop-zone {
      border: 2px dashed #2e2e2e;
      border-radius: 12px;
      padding: 36px 24px;
      text-align: center;
      cursor: pointer;
      transition: border-color 0.2s, background 0.2s;
      position: relative;
      margin-bottom: 20px;
    }

    .drop-zone:hover, .drop-zone.dragover {
      border-color: #6366f1;
      background: rgba(99,102,241,0.05);
    }

    .drop-zone input[type="file"] {
      position: absolute;
      inset: 0;
      opacity: 0;
      cursor: pointer;
      width: 100%;
      height: 100%;
    }

    .drop-icon { font-size: 32px; margin-bottom: 10px; }

    .drop-text {
      font-size: 14px;
      color: #888;
    }

    .drop-text strong { color: #aaa; }

    .file-selected {
      margin-top: 8px;
      font-size: 13px;
      color: #6366f1;
      font-weight: 500;
    }

    .schema-hint {
      background: #111;
      border: 1px solid #2a2a2a;
      border-radius: 8px;
      padding: 12px 14px;
      font-size: 12px;
      color: #666;
      margin-bottom: 20px;
      line-height: 1.6;
    }

    .schema-hint strong { color: #888; }

    button[type="submit"] {
      width: 100%;
      padding: 12px;
      background: linear-gradient(135deg, #6366f1, #8b5cf6);
      color: #fff;
      border: none;
      border-radius: 8px;
      font-size: 15px;
      font-weight: 600;
      cursor: pointer;
      transition: opacity 0.2s;
    }

    button[type="submit"]:hover { opacity: 0.9; }
    button[type="submit"]:disabled { opacity: 0.4; cursor: not-allowed; }

    #result {
      margin-top: 20px;
      padding: 14px;
      border-radius: 8px;
      font-size: 13px;
      display: none;
    }

    #result.success {
      background: rgba(34,197,94,0.1);
      border: 1px solid rgba(34,197,94,0.3);
      color: #4ade80;
    }

    #result.error {
      background: rgba(239,68,68,0.1);
      border: 1px solid rgba(239,68,68,0.3);
      color: #f87171;
    }

    .spinner {
      display: inline-block;
      width: 14px;
      height: 14px;
      border: 2px solid rgba(255,255,255,0.3);
      border-top-color: #fff;
      border-radius: 50%;
      animation: spin 0.7s linear infinite;
      vertical-align: middle;
      margin-right: 8px;
    }

    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">
      <div class="logo-icon">🤖</div>
      <div>
        <h1>Infra Bot</h1>
        <div class="subtitle">CSV Asset Upload</div>
      </div>
    </div>

    <form id="uploadForm">
      <label class="field-label">Asset Type</label>
      <select id="assetType" name="type" required>
        <option value="">— Select type —</option>
        <option value="domain">Domains</option>
        <option value="inbox">Inboxes</option>
      </select>

      <label class="field-label">CSV File</label>
      <div class="drop-zone" id="dropZone">
        <input type="file" id="csvFile" name="csv" accept=".csv" required />
        <div class="drop-icon">📂</div>
        <div class="drop-text"><strong>Click to browse</strong> or drag & drop your CSV</div>
        <div class="file-selected" id="fileName"></div>
      </div>

      <div class="schema-hint" id="schemaHint">
        Select an asset type above to see the expected column order.
      </div>

      <button type="submit" id="submitBtn">Upload & Import</button>
    </form>

    <div id="result"></div>
  </div>

  <script>
    const domainSchema = 'Domain, Client, Provider, Workspace, Campaign, Purchase Date, Expiry Date, Status, Branded/Pre-warmed, Primary Owner, Visibility Channel, Yearly Cost, Currency, Notes';
    const inboxSchema  = 'Inbox, Client, Provider, Workspace, Domain, Campaign, Purchase Date, Expiry Date, Status, Branded/Pre-warmed, Primary Owner, Visibility Channel, Monthly Cost, Currency, Notes';

    document.getElementById('assetType').addEventListener('change', function () {
      const hint = document.getElementById('schemaHint');
      if (this.value === 'domain') {
        hint.innerHTML = '<strong>Expected columns:</strong><br>' + domainSchema;
      } else if (this.value === 'inbox') {
        hint.innerHTML = '<strong>Expected columns:</strong><br>' + inboxSchema;
      } else {
        hint.innerHTML = 'Select an asset type above to see the expected column order.';
      }
    });

    document.getElementById('csvFile').addEventListener('change', function () {
      document.getElementById('fileName').textContent = this.files[0] ? '✓ ' + this.files[0].name : '';
    });

    const dropZone = document.getElementById('dropZone');
    dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('dragover'); });
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));
    dropZone.addEventListener('drop', e => {
      e.preventDefault();
      dropZone.classList.remove('dragover');
      const file = e.dataTransfer.files[0];
      if (file) {
        const input = document.getElementById('csvFile');
        const dt = new DataTransfer();
        dt.items.add(file);
        input.files = dt.files;
        document.getElementById('fileName').textContent = '✓ ' + file.name;
      }
    });

    document.getElementById('uploadForm').addEventListener('submit', async function (e) {
      e.preventDefault();
      const btn = document.getElementById('submitBtn');
      const result = document.getElementById('result');
      const type = document.getElementById('assetType').value;
      const file = document.getElementById('csvFile').files[0];

      if (!type || !file) return;

      btn.disabled = true;
      btn.innerHTML = '<span class="spinner"></span>Importing...';
      result.style.display = 'none';

      const formData = new FormData();
      formData.append('type', type);
      formData.append('csv', file);

      try {
        const res = await fetch('/upload-csv', { method: 'POST', body: formData });
        const data = await res.json();

        result.style.display = 'block';
        if (res.ok) {
          result.className = 'success';
          result.innerHTML = '✅ <strong>Import successful!</strong><br>' +
            'Inserted: ' + data.inserted + ' &nbsp;|&nbsp; Updated: ' + data.updated + ' &nbsp;|&nbsp; Total: ' + data.total;
        } else {
          result.className = 'error';
          result.innerHTML = '❌ <strong>Error:</strong> ' + (data.error || 'Unknown error');
        }
      } catch (err) {
        result.style.display = 'block';
        result.className = 'error';
        result.innerHTML = '❌ <strong>Network error:</strong> ' + err.message;
      } finally {
        btn.disabled = false;
        btn.innerHTML = 'Upload & Import';
      }
    });
  </script>
</body>
</html>`);
});

// CSV upload endpoint
expressApp.post('/upload-csv', upload.single('csv'), async (req, res) => {
  try {
    const type = req.body.type; // 'domain' or 'inbox'
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

    // Sync to Google Sheets
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));

    return res.json({ inserted, updated, total: assets.length });
  } catch (err) {
    console.error('CSV upload error:', err);
    return res.status(500).json({ error: err.message });
  }
});

// Catch-all — serve frontend for any unmatched GET route
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

    // ---- File upload handler ----
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

    // ---- Text command handler (delete / renew) ----
    if (!message.text) return;

    const lines = message.text.trim().split(/\r?\n/).map(l => l.trim()).filter(Boolean);

    if (lines.length < 2) return;

    const command = lines[0].toLowerCase();
    if (command !== 'delete' && command !== 'renew') return;



    // Slack auto-formats emails as <mailto:email|email> — strip that
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
        await Asset.findOneAndUpdate({ name: existing.name }, {
          purchaseDate: today.toDate(),
          expiryDate: null,
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

  // Auto-extract domain from inbox email
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

    // Only update date fields — leave everything else untouched
    const updates = { updatedAt: new Date(), remindersSent: [] };
    if (newPurchaseDate) updates.purchaseDate = newPurchaseDate;
    if (newExpiryDate) updates.expiryDate = newExpiryDate;
    // If only purchase date given, clear old expiry so it auto-calculates
    if (newPurchaseDate && !newExpiryDate) updates.expiryDate = null;

    await Asset.findOneAndUpdate({ name }, updates);

    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));

    const updated = await Asset.findOne({ name });
    const daysLeft = computeDaysLeft(updated);

    await client.chat.postMessage({
      channel: body.user.id,
      text: `✅ *${name}* renewed successfully!
` +
            `• New Purchase Date: ${newPurchaseDate ? dayjs(newPurchaseDate).format('DD/MM/YYYY') : 'Unchanged'}
` +
            `• New Expiry Date: ${updates.expiryDate ? dayjs(updates.expiryDate).format('DD/MM/YYYY') : 'Auto-calculated'}
` +
            `• Days until expiry: ${daysLeft !== null ? daysLeft : 'N/A'}
` +
            `• Reminder history cleared ✓
` +
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

  // Buckets: keyed by destination (owner ID or channel ID)
  // Each entry: { expiring: [], expired: [] }
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

    // Use working day targets instead of fixed day counts
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

    // Send to primary owner DM
    addToBucket(asset.primaryOwner, asset, daysLeft, isExpired);

    // Also post to visibility channel if set
    if (asset.visibilityChannel) {
      addToBucket(asset.visibilityChannel, asset, daysLeft, isExpired);
    }
  }

  for (const [dest, { expiring, expired }] of Object.entries(buckets)) {

    // ---- Expiring: one summary line per daysLeft group + "View Details" button ----
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

      // Store detail text per daysLeft key so button handler can retrieve it
      const detailMap = {};

      for (const daysLeft of Object.keys(byDay).sort((a, b) => Number(a) - Number(b))) {
        const items = byDay[daysLeft];
        const domains = items.filter(i => i.asset.type === 'DOMAIN');
        const inboxes = items.filter(i => i.asset.type === 'INBOX');
        const label = daysLeft === '1' ? 'tomorrow' : `in ${daysLeft} days`;

        const parts = [];
        if (domains.length) parts.push(`${domains.length} domain${domains.length !== 1 ? 's' : ''}`);
        if (inboxes.length) parts.push(`${inboxes.length} inbox${inboxes.length !== 1 ? 'es' : ''}`);
        const summary = `${parts.join(' and ')} expire${items.length === 1 ? 's' : ''} ${label}`;

        // Build detail grouped by client
        const byClient = {};
        for (const { asset } of items) {
          const c = asset.client || 'No Client';
          if (!byClient[c]) byClient[c] = [];
          byClient[c].push(asset);
        }
        let detail = `*Expiring ${label.charAt(0).toUpperCase() + label.slice(1)}*\n\n`;
        for (const [client, assets] of Object.entries(byClient)) {
          detail += `*${client}*\n`;
          for (const asset of assets) {
            const provider = asset.provider ? ` — ${asset.provider}` : '';
            detail += `  • ${asset.name} (${asset.type === 'DOMAIN' ? 'Domain' : 'Inbox'})${provider}\n`;
          }
        }
        detailMap[daysLeft] = detail.trim();

        blocks.push({
          type: 'section',
          text: { type: 'mrkdwn', text: `*${summary}*` },
          accessory: {
            type: 'button',
            text: { type: 'plain_text', text: 'View Details' },
            action_id: `view_details_${daysLeft}`,
            value: JSON.stringify({ detail: detailMap[daysLeft], channel: dest })
          }
        });
        blocks.push({ type: 'divider' });
      }

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

    // ---- Overdue: summary + "View Details" button ----
    if (expired.length > 0) {
      const domains = expired.filter(i => i.asset.type === 'DOMAIN');
      const inboxes = expired.filter(i => i.asset.type === 'INBOX');

      const parts = [];
      if (domains.length) parts.push(`${domains.length} domain${domains.length !== 1 ? 's' : ''}`);
      if (inboxes.length) parts.push(`${inboxes.length} inbox${inboxes.length !== 1 ? 'es' : ''}`);
      const summary = `${parts.join(' and ')} expired but still marked Active — please review`;

      const byClient = {};
      for (const { asset, daysLeft } of expired) {
        const c = asset.client || 'No Client';
        if (!byClient[c]) byClient[c] = [];
        byClient[c].push({ asset, daysLeft });
      }
      let detail = `*Expired & Still Active*\n\n`;
      for (const [client, items] of Object.entries(byClient)) {
        detail += `*${client}*\n`;
        for (const { asset, daysLeft } of items) {
          const provider = asset.provider ? ` — ${asset.provider}` : '';
          detail += `  • ${asset.name} (${asset.type === 'DOMAIN' ? 'Domain' : 'Inbox'})${provider} — ${Math.abs(daysLeft)}d overdue\n`;
        }
      }

      const blocks = [
        {
          type: 'header',
          text: { type: 'plain_text', text: `Expired & Still Active — ${dayjs().format('DD MMM YYYY')}` }
        },
        {
          type: 'section',
          text: { type: 'mrkdwn', text: `*${summary}*` },
          accessory: {
            type: 'button',
            text: { type: 'plain_text', text: 'View Details' },
            action_id: 'view_expired_details',
            value: JSON.stringify({ detail: detail.trim(), channel: dest })
          }
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

    // Start HTTP server for CSV upload UI
    expressApp.listen(PORT, () => {
      console.log(`✅ Upload UI running at http://localhost:${PORT}`);
    });

    // Schedule daily reminder at 9 AM
    cron.schedule('0 9 * * *', () => {
      console.log('Executing scheduled daily summary...');
      runDailySummary();
    });
    console.log('✅ Daily reminder scheduled for 9:00 AM');

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