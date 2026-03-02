const { google } = require('googleapis');
const dayjs = require('dayjs');

/* -------------------- Auth -------------------- */
async function getSheetsClient() {
  // Try service account first (for local development)
  if (process.env.GOOGLE_SERVICE_ACCOUNT_JSON) {
    try {
      const serviceAccountJson = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON);
      const auth = new google.auth.GoogleAuth({
        credentials: serviceAccountJson,
        scopes: ['https://www.googleapis.com/auth/spreadsheets']
      });
      const client = await auth.getClient();
      return google.sheets({ version: 'v4', auth: client });
    } catch (e) {
      console.error('Failed to authenticate with service account JSON:', e.message);
      throw new Error('Invalid GOOGLE_SERVICE_ACCOUNT_JSON environment variable');
    }
  }

  // Fallback to API key (read-only, for development)
  if (process.env.GOOGLE_API_KEY) {
    return google.sheets({
      version: 'v4',
      auth: process.env.GOOGLE_API_KEY
    });
  }

  throw new Error('No Google authentication method configured. Set either GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_API_KEY.');
}

/* -------------------- Helpers -------------------- */
function formatDate(d) {
  if (!d) return '';
  return dayjs(d).format('DD/MM/YYYY');
}

function calculateDaysLeft(expiryDate) {
  if (!expiryDate) return '';
  const today = dayjs().startOf('day');
  const expiry = dayjs(expiryDate).startOf('day');
  return expiry.diff(today, 'day');
}

/* -------------------- Expiry Logic -------------------- */
function getExpiryDate(asset) {
  if (asset.expiryDate) return new Date(asset.expiryDate);

  if (!asset.purchaseDate) return null;

  const purchase = dayjs(asset.purchaseDate).startOf('day');

  if (asset.type === 'DOMAIN') {
    return purchase.add(365, 'day').toDate();
  }

  if (asset.type === 'INBOX') {
    const today = dayjs().startOf('day');
    let nextExpiry = dayjs()
      .year(dayjs().year())
      .month(dayjs().month())
      .date(purchase.date());

    if (nextExpiry.isBefore(today)) nextExpiry = nextExpiry.add(1, 'month');
    return nextExpiry.toDate();
  }

  return null;
}

/* -------------------- Domains Sheet Sync -------------------- */
async function syncDomainsToSheet(domains) {
  const sheets = await getSheetsClient();

  // Columns match new CSV schema exactly
  const headers = [
    'Domain',
    'Client',
    'Provider',
    'Workspace',
    'Campaign',
    'Purchase Date',
    'Expiry Date',
    'Days Left',
    'Status',
    'Branded/Pre-warmed',
    'Primary Owner',
    'Visibility Channel',
    'Yearly Cost',
    'Currency',
    'Notes'
  ];

  const values = domains.map(d => {
    const expiry = getExpiryDate(d);
    const daysLeft = d.daysLeft !== undefined && d.daysLeft !== null
      ? d.daysLeft
      : calculateDaysLeft(expiry);

    return [
      d.name || '',
      d.client || '',
      d.provider || '',
      d.workspace || '',
      d.campaign || '',
      formatDate(d.purchaseDate),
      formatDate(expiry),
      daysLeft !== '' ? daysLeft : '',
      d.status || '',
      d.brandedPrewarmed || '',
      d.primaryOwner || '',
      d.visibilityChannel || '',
      d.yearlyCost !== null && d.yearlyCost !== undefined ? d.yearlyCost : '',
      d.currency || '',
      d.notes || ''
    ];
  });

  try {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: 'Domains!A:Z'
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: 'Domains!A1',
      valueInputOption: 'RAW',
      requestBody: { values: [headers, ...values] }
    });

    console.log(`📄 Domains sheet synced: ${domains.length} rows`);
  } catch (error) {
    console.error('Error syncing Domains sheet:', error.message);
    throw error;
  }
}

/* -------------------- Inboxes Sheet Sync -------------------- */
async function syncInboxesToSheet(inboxes) {
  const sheets = await getSheetsClient();

  // Columns match new CSV schema exactly
  const headers = [
    'Inbox',
    'Client',
    'Provider',
    'Workspace',
    'Domain',
    'Campaign',
    'Purchase Date',
    'Expiry Date',
    'Days Left',
    'Status',
    'Branded/Pre-warmed',
    'Primary Owner',
    'Visibility Channel',
    'Monthly Cost',
    'Currency',
    'Notes'
  ];

  const values = inboxes.map(i => {
    const expiry = getExpiryDate(i);
    const daysLeft = i.daysLeft !== undefined && i.daysLeft !== null
      ? i.daysLeft
      : calculateDaysLeft(expiry);

    return [
      i.name || '',
      i.client || '',
      i.provider || '',
      i.workspace || '',
      i.domain || '',
      i.campaign || '',
      formatDate(i.purchaseDate),
      formatDate(expiry),
      daysLeft !== '' ? daysLeft : '',
      i.status || '',
      i.brandedPrewarmed || '',
      i.primaryOwner || '',
      i.visibilityChannel || '',
      i.monthlyCost !== null && i.monthlyCost !== undefined ? i.monthlyCost : '',
      i.currency || '',
      i.notes || ''
    ];
  });

  try {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: 'Inboxes!A:Z'
    });

    await sheets.spreadsheets.values.update({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: 'Inboxes!A1',
      valueInputOption: 'RAW',
      requestBody: { values: [headers, ...values] }
    });

    console.log(`📧 Inboxes sheet synced: ${inboxes.length} rows`);
  } catch (error) {
    console.error('Error syncing Inboxes sheet:', error.message);
    throw error;
  }
}

/* -------------------- Read from Sheets → Database -------------------- */

async function readDomainsFromSheet() {
  const sheets = await getSheetsClient();

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: 'Domains!A:O'
    });

    const rows = response.data.values;
    if (!rows || rows.length <= 1) {
      console.log('No data found in Domains sheet');
      return [];
    }

    const parseDateDDMMYYYY = (dateStr) => {
      if (!dateStr || dateStr.trim() === '') return null;
      const parsed = dayjs(dateStr, 'DD/MM/YYYY', true);
      return parsed.isValid() ? parsed.toDate() : null;
    };

    // Skip header row. Column order:
    // 0:Domain 1:Client 2:Provider 3:Workspace 4:Campaign
    // 5:Purchase Date 6:Expiry Date 7:Days Left(skip) 8:Status
    // 9:Branded/Pre-warmed 10:Primary Owner 11:Visibility Channel
    // 12:Yearly Cost 13:Currency 14:Notes
    return rows.slice(1).map(row => ({
      type: 'DOMAIN',
      name: row[0] ? row[0].trim().toLowerCase() : '',
      client: row[1] || null,
      provider: row[2] || null,
      workspace: row[3] || null,
      campaign: row[4] || null,
      purchaseDate: parseDateDDMMYYYY(row[5]),
      expiryDate: parseDateDDMMYYYY(row[6]),
      // row[7] = Days Left — calculated, skip
      status: row[8] || 'Active',
      brandedPrewarmed: row[9] || null,
      primaryOwner: row[10] || null,
      visibilityChannel: row[11] || null,
      yearlyCost: row[12] && row[12] !== '' ? parseFloat(row[12]) : null,
      currency: row[13] || 'USD',
      notes: row[14] || null
    })).filter(d => d.name);
  } catch (error) {
    console.error('Error reading Domains from sheet:', error.message);
    return [];
  }
}

async function readInboxesFromSheet() {
  const sheets = await getSheetsClient();

  try {
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: 'Inboxes!A:P'
    });

    const rows = response.data.values;
    if (!rows || rows.length <= 1) {
      console.log('No data found in Inboxes sheet');
      return [];
    }

    const parseDateDDMMYYYY = (dateStr) => {
      if (!dateStr || dateStr.trim() === '') return null;
      const parsed = dayjs(dateStr, 'DD/MM/YYYY', true);
      return parsed.isValid() ? parsed.toDate() : null;
    };

    // Column order:
    // 0:Inbox 1:Client 2:Provider 3:Workspace 4:Domain 5:Campaign
    // 6:Purchase Date 7:Expiry Date 8:Days Left(skip) 9:Status
    // 10:Branded/Pre-warmed 11:Primary Owner 12:Visibility Channel
    // 13:Monthly Cost 14:Currency 15:Notes
    return rows.slice(1).map(row => ({
      type: 'INBOX',
      name: row[0] ? row[0].trim().toLowerCase() : '',
      client: row[1] || null,
      provider: row[2] || null,
      workspace: row[3] || null,
      domain: row[4] || null,
      campaign: row[5] || null,
      purchaseDate: parseDateDDMMYYYY(row[6]),
      expiryDate: parseDateDDMMYYYY(row[7]),
      // row[8] = Days Left — calculated, skip
      status: row[9] || 'Active',
      brandedPrewarmed: row[10] || null,
      primaryOwner: row[11] || null,
      visibilityChannel: row[12] || null,
      monthlyCost: row[13] && row[13] !== '' ? parseFloat(row[13]) : null,
      currency: row[14] || 'USD',
      notes: row[15] || null
    })).filter(i => i.name);
  } catch (error) {
    console.error('Error reading Inboxes from sheet:', error.message);
    return [];
  }
}

async function readAllAssetsFromSheet() {
  console.log('📥 Reading assets from Google Sheets...');
  const [domains, inboxes] = await Promise.all([
    readDomainsFromSheet(),
    readInboxesFromSheet()
  ]);
  console.log(`📥 Read ${domains.length} domains and ${inboxes.length} inboxes from sheets`);
  return [...domains, ...inboxes];
}

/* -------------------- Unified Sync (Database → Sheets) -------------------- */
async function syncAllAssetsToSheet(assets) {
  console.log(`🔄 Syncing ${assets.length} total assets to Google Sheets...`);

  const domains = assets.filter(a => a.type === 'DOMAIN');
  const inboxes = assets.filter(a => a.type === 'INBOX');

  console.log(`   - ${domains.length} domains`);
  console.log(`   - ${inboxes.length} inboxes`);

  try {
    await Promise.all([
      syncDomainsToSheet(domains),
      syncInboxesToSheet(inboxes)
    ]);
    console.log('✅ All assets synced to Google Sheets successfully');
  } catch (error) {
    console.error('❌ Error syncing to Google Sheets:', error.message);
    throw error;
  }
}

module.exports = {
  syncAllAssetsToSheet,
  readAllAssetsFromSheet,
  readDomainsFromSheet,
  readInboxesFromSheet
};