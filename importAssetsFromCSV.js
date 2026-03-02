require('dotenv').config();
const mongoose = require('mongoose');
const fs = require('fs');
const csv = require('csv-parser');
const dayjs = require('dayjs');
const customParseFormat = require('dayjs/plugin/customParseFormat');
const { syncAllAssetsToSheet } = require('./sheets');

dayjs.extend(customParseFormat);

/* -------------------- Asset Schema (same as index.js) -------------------- */
const AssetSchema = new mongoose.Schema({
  type: { type: String, enum: ['DOMAIN', 'INBOX'], required: true },
  name: { type: String, required: true, unique: true },

  // Common fields
  client: String,
  purposeCampaign: String,
  purchaseDate: Date,
  expiryDate: Date,
  status: String,
  primaryOwner: String,
  visibilityChannel: String,
  currency: String,
  notes: String,

  // Domain-specific fields
  registrar: String,
  renewalCost: Number,

  // Inbox-specific fields
  inboxProvider: String,
  domain: String,
  monthlyCost: Number,

  // Tracking
  remindersSent: [{ daysBefore: Number, sentAt: Date }],
  createdBy: String,
  createdAt: { type: Date, default: Date.now },
  updatedAt: { type: Date, default: Date.now }
});

const Asset = mongoose.model('Asset', AssetSchema);

/* -------------------- Helper Functions -------------------- */
function parseDate(dateString) {
  if (!dateString || dateString.trim() === '') return null;
  
  // Try DD/MM/YYYY format
  const parsed = dayjs(dateString, 'DD/MM/YYYY', true);
  if (parsed.isValid()) return parsed.toDate();
  
  // Try MM/DD/YYYY format
  const parsed2 = dayjs(dateString, 'MM/DD/YYYY', true);
  if (parsed2.isValid()) return parsed2.toDate();
  
  return null;
}

function parseCost(value) {
  if (!value || value === '') return null;
  const match = String(value).match(/[\d,.]+/);
  if (!match) return null;
  const num = Number(match[0].replace(/,/g, ''));
  return isNaN(num) ? null : num;
}

/* -------------------- Auto-detect file type -------------------- */
function detectFileType(filePath) {
  const lowerPath = filePath.toLowerCase();
  
  if (lowerPath.includes('inbox')) {
    return 'inboxes';
  } else if (lowerPath.includes('domain')) {
    return 'domains';
  }
  
  // If can't detect from filename, try to detect from CSV headers
  return null;
}

/* -------------------- Import Domains CSV -------------------- */
async function importDomains(filePath) {
  return new Promise((resolve, reject) => {
    const domains = [];
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        // Skip empty rows
        if (!row.Domain || row.Domain.trim() === '') return;
        
        const domain = {
          type: 'DOMAIN',
          name: row.Domain.trim().toLowerCase(),
          client: row.Client || null,
          purposeCampaign: row['Purpose / Campaign'] || null,
          registrar: row.Registrar || null,
          purchaseDate: parseDate(row['Purchase Date']),
          expiryDate: parseDate(row['Expiry Date']),
          status: row.Status || 'Active',
          primaryOwner: row['Primary Owner'] || null,
          visibilityChannel: row['Visibility Channel'] || null,
          renewalCost: parseCost(row['Renewal Cost']),
          currency: row.Currency || 'USD',
          notes: row.Notes || null,
          createdBy: 'CSV_IMPORT',
          updatedAt: new Date()
        };
        
        domains.push(domain);
      })
      .on('end', () => {
        resolve(domains);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

/* -------------------- Import Inboxes CSV -------------------- */
async function importInboxes(filePath) {
  return new Promise((resolve, reject) => {
    const inboxes = [];
    
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => {
        // Skip empty rows
        if (!row.Inbox || row.Inbox.trim() === '') return;
        
        const inbox = {
          type: 'INBOX',
          name: row.Inbox.trim().toLowerCase(),
          client: row.Client || null,
          inboxProvider: row['Inbox Provider'] || null,
          domain: row.Domain || null,
          purposeCampaign: row['Purpose / Campaign'] || null,
          purchaseDate: parseDate(row['Purchase Date']),
          expiryDate: parseDate(row['Expiry Date']),
          status: row.Status || 'Active',
          primaryOwner: row['Primary Owner'] || null,
          visibilityChannel: row['Visibility Channel'] || null,
          monthlyCost: parseCost(row['Monthly Cost']),
          currency: row.Currency || 'USD',
          notes: row.Notes || null,
          createdBy: 'CSV_IMPORT',
          updatedAt: new Date()
        };
        
        inboxes.push(inbox);
      })
      .on('end', () => {
        resolve(inboxes);
      })
      .on('error', (error) => {
        reject(error);
      });
  });
}

/* -------------------- Prepare for Sheets -------------------- */
function computeDaysLeft(asset) {
  const today = dayjs().startOf('day');
  let expiryDate = asset.expiryDate;

  if (!expiryDate && asset.purchaseDate) {
    if (asset.type === 'DOMAIN') {
      expiryDate = dayjs(asset.purchaseDate).add(365, 'day').toDate();
    } else if (asset.type === 'INBOX') {
      const purchaseDay = dayjs(asset.purchaseDate);
      const currentMonth = dayjs().month();
      const currentYear = dayjs().year();
      
      let nextExpiry = dayjs().year(currentYear).month(currentMonth).date(purchaseDay.date());
      
      if (nextExpiry.isBefore(today)) {
        nextExpiry = nextExpiry.add(1, 'month');
      }
      
      expiryDate = nextExpiry.toDate();
    }
  }

  if (!expiryDate) return null;
  
  return dayjs(expiryDate).startOf('day').diff(today, 'day');
}

function prepareAssetsForSheet(assets) {
  return assets.map(a => {
    const obj = a.toObject ? a.toObject() : a;
    obj.daysLeft = computeDaysLeft(obj);
    return obj;
  });
}

/* -------------------- Main Import Function -------------------- */
async function importCSV(filePath, typeOverride = null) {
  try {
    // Auto-detect type from filename if not provided
    const type = typeOverride || detectFileType(filePath);
    
    if (!type) {
      throw new Error('Could not detect file type. Filename should contain "domain" or "inbox"');
    }

    console.log(`\n🔄 Detected file type: ${type.toUpperCase()}`);
    console.log(`📂 Importing from: ${filePath}`);
    
    // Connect to MongoDB
    await mongoose.connect(process.env.MONGO_URI);
    console.log('✅ MongoDB connected');

    let assets = [];
    
    if (type === 'domains') {
      assets = await importDomains(filePath);
      console.log(`📄 Parsed ${assets.length} domains from CSV`);
    } else if (type === 'inboxes') {
      assets = await importInboxes(filePath);
      console.log(`📧 Parsed ${assets.length} inboxes from CSV`);
    } else {
      throw new Error('Invalid type. Use "domains" or "inboxes"');
    }

    // Save to database using upsert (update if exists, insert if new)
    let inserted = 0;
    let updated = 0;

    for (const asset of assets) {
      const existing = await Asset.findOne({ name: asset.name });
      
      if (existing) {
        await Asset.findOneAndUpdate({ name: asset.name }, asset);
        updated++;
      } else {
        await Asset.create(asset);
        inserted++;
      }
    }

    console.log(`\n📊 Import Summary:`);
    console.log(`   ✅ Inserted: ${inserted}`);
    console.log(`   🔄 Updated: ${updated}`);
    console.log(`   📝 Total processed: ${assets.length}`);

    // Sync all assets to Google Sheets
    console.log('\n🔄 Syncing to Google Sheets...');
    const allAssets = await Asset.find();
    await syncAllAssetsToSheet(prepareAssetsForSheet(allAssets));
    
    console.log('\n✅ Import completed successfully!');
    
    // Close connection
    await mongoose.connection.close();
    process.exit(0);

  } catch (error) {
    console.error('\n❌ Error during import:', error.message);
    console.error(error);
    if (mongoose.connection.readyState === 1) {
      await mongoose.connection.close();
    }
    process.exit(1);
  }
}

/* -------------------- CLI -------------------- */
const args = process.argv.slice(2);

if (args.length < 1) {
  console.log('\n📥 CSV Import Tool\n');
  console.log('Usage:');
  console.log('  node importAssetsFromCSV.js <file-path> [type]\n');
  console.log('Examples:');
  console.log('  node importAssetsFromCSV.js "Domains & Inboxes - Master Sheet - inbox.csv"');
  console.log('  node importAssetsFromCSV.js "Domains & Inboxes - Master Sheet - domain.csv"');
  console.log('  node importAssetsFromCSV.js "./my-domains.csv" domains');
  console.log('  node importAssetsFromCSV.js "./my-inboxes.csv" inboxes\n');
  console.log('Note: File type is auto-detected from filename (looks for "domain" or "inbox")');
  console.log('      You can optionally specify type as second argument.\n');
  process.exit(1);
}

const filePath = args[0];
const typeOverride = args[1]; // Optional

// Check if file exists
if (!fs.existsSync(filePath)) {
  console.error(`\n❌ Error: File not found: ${filePath}\n`);
  process.exit(1);
}

// Run import
importCSV(filePath, typeOverride);