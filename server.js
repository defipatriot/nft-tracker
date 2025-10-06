const express = require('express');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;
const CRON_SECRET = process.env.CRON_SECRET; // TWEAK 1: Read the secret from environment

// Enable CORS for all routes
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization'); // Allow Authorization header
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// TWEAK 2: Create authorization middleware to protect cron endpoints
const authorizeCron = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    if (!CRON_SECRET || authHeader !== `Bearer ${CRON_SECRET}`) {
        console.warn(`[${getUTCTimestamp()}] Unauthorized attempt to trigger a job.`);
        return res.status(401).send('Unauthorized');
    }
    next();
};

const DATA_DIR = process.env.DATA_DIR || './data';
const SOURCE_URL = 'https://deving.zone/en/nfts/alliance_daos.json';

async function ensureDirectories() {
  const dirs = [
    path.join(DATA_DIR, 'snapshots'),
    path.join(DATA_DIR, 'daily'),
    path.join(DATA_DIR, 'weekly'),
    path.join(DATA_DIR, 'monthly'),
    path.join(DATA_DIR, 'yearly')
  ];
  for (const dir of dirs) {
    await fs.mkdir(dir, { recursive: true });
  }
}

function getUTCTimestamp() {
  return new Date().toISOString();
}

function formatDate(date, format = 'full') {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  const hour = String(date.getUTCHours()).padStart(2, '0');
  switch(format) {
    case 'full': return `${year}-${month}-${day}-${hour}00`;
    case 'date': return `${year}-${month}-${day}`;
    case 'week': return `${year}-W${getWeekNumber(date)}`;
    case 'month': return `${year}-${month}`;
    case 'year': return `${year}`;
    default: return `${year}-${month}-${day}`;
  }
}

function getWeekNumber(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return String(weekNo).padStart(2, '0');
}

// Pass the authorizeCron middleware to all POST endpoints
app.post('/capture-snapshot', authorizeCron, async (req, res) => {
  try {
    console.log(`[${getUTCTimestamp()}] Starting hourly snapshot capture...`);
    const response = await axios.get(SOURCE_URL, { timeout: 30000 });
    const data = response.data;
    const timestamp = formatDate(new Date(), 'full');
    const filename = `${timestamp}.json`;
    const filepath = path.join(DATA_DIR, 'snapshots', filename);
    await fs.writeFile(filepath, JSON.stringify(data, null, 2));
    console.log(`[${getUTCTimestamp()}] Snapshot saved: ${filename}`);
    const nftCount = data.nfts ? data.nfts.length : (Array.isArray(data) ? data.length : 0);
    res.json({ success: true, timestamp, filename, nft_count: nftCount });
  } catch (error) {
    console.error(`[${getUTCTimestamp()}] Snapshot capture failed:`, error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

app.post('/process-daily-events', authorizeCron, async (req, res) => {
  try {
    console.log(`[${getUTCTimestamp()}] Starting daily event processing...`);
    const yesterday = new Date();
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);
    const targetDate = formatDate(yesterday, 'date');
    const snapshots = [];
    for (let hour = 0; hour < 24; hour++) {
      const timestamp = `${targetDate}-${String(hour).padStart(2, '0')}00`;
      const filepath = path.join(DATA_DIR, 'snapshots', `${timestamp}.json`);
      try {
        const content = await fs.readFile(filepath, 'utf8');
        snapshots.push(JSON.parse(content));
      } catch (err) {
        console.warn(`Missing snapshot for ${timestamp}`);
      }
    }
    if (snapshots.length < 2) { // Need at least two snapshots to compare
      console.log(`Not enough snapshots for ${targetDate} to generate a daily summary.`);
      return res.json({ success: true, message: `Not enough snapshots for ${targetDate}` });
    }
    console.log(`Found ${snapshots.length} snapshots for ${targetDate}`);
    const events = detectEvents(snapshots);
    const dailyFile = path.join(DATA_DIR, 'daily', `${targetDate}.json`);
    await fs.writeFile(dailyFile, JSON.stringify(events, null, 2));
    console.log(`[${getUTCTimestamp()}] Daily summary saved: ${targetDate}.json`);
    res.json({ success: true, date: targetDate, snapshots_processed: snapshots.length, total_events: events.summary.total_events });
  } catch (error) {
    console.error(`[${getUTCTimestamp()}] Daily processing failed:`, error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

function detectEvents(snapshots) {
  const activityLog = {};
  const summary = { bbl_sales: 0, boost_sales: 0, transfers: 0, bbl_listings: 0, bbl_delistings: 0, boost_listings: 0, boost_delistings: 0, daodao_stakes: 0, daodao_unstakes: 0, enterprise_stakes: 0, enterprise_unstakes: 0, breaks: 0, total_events: 0 };
  const normalizedSnapshots = snapshots.map(s => s.nfts && Array.isArray(s.nfts) ? s.nfts : (Array.isArray(s) ? s : []));
  for (let nftId = 1; nftId <= 10000; nftId++) {
    const events = [];
    for (let i = 1; i < normalizedSnapshots.length; i++) {
      const prev = normalizedSnapshots[i - 1].find(n => n.id === nftId);
      const curr = normalizedSnapshots[i].find(n => n.id === nftId);
      if (!prev || !curr) continue;
      // ... (rest of your event detection logic is fine)
      if (prev.owner !== curr.owner) {
        const wasListedBbl = prev.bbl === true;
        const wasListedBoost = prev.boost === true;
        if (wasListedBbl) {
          events.push({ type: 'sale', marketplace: 'bbl', from: prev.owner, to: curr.owner, hour: i });
          summary.bbl_sales++;
        } else if (wasListedBoost) {
          events.push({ type: 'sale', marketplace: 'boost', from: prev.owner, to: curr.owner, hour: i });
          summary.boost_sales++;
        } else {
          events.push({ type: 'transfer', from: prev.owner, to: curr.owner, hour: i });
          summary.transfers++;
        }
        summary.total_events++;
      }
      // (This is just a snippet, your full logic goes here)
    }
    if (events.length > 0) activityLog[nftId] = events;
  }
  return { summary, activity_log: activityLog };
}

app.post('/aggregate-weekly', authorizeCron, async (req, res) => { /* ... your logic ... */ });
app.post('/aggregate-monthly', authorizeCron, async (req, res) => { /* ... your logic ... */ });
app.post('/aggregate-yearly', authorizeCron, async (req, res) => { /* ... your logic ... */ });

async function aggregatePeriod(sourceType, targetType, periodId, maxFiles) { /* ... your logic ... */ }

app.use('/data', express.static(DATA_DIR));
app.get('/health', (req, res) => res.json({ status: 'ok', timestamp: getUTCTimestamp() }));

// TWEAK 3: Rewrite the /status endpoint to be non-crashing and more efficient
app.get('/status', async (req, res) => {
  try {
    const stats = { snapshots: 0, daily: 0, weekly: 0, monthly: 0, yearly: 0 };
    const types = Object.keys(stats);

    // Use Promise.allSettled to read all directories in parallel without crashing
    const results = await Promise.allSettled(
      types.map(type => {
        const dir = path.join(DATA_DIR, type);
        return fs.readdir(dir);
      })
    );

    // Process the results safely
    results.forEach((result, index) => {
      const type = types[index];
      if (result.status === 'fulfilled') {
        stats[type] = result.value.length; // result.value is the array of files
      } else {
        // This means the directory likely doesn't exist, which is fine.
        stats[type] = 0;
      }
    });
    
    res.json(stats);
  } catch (error) {
    console.error(`[${getUTCTimestamp()}] Error in /status endpoint:`, error.message);
    res.status(500).json({ error: "Failed to retrieve status.", details: error.message });
  }
});

// Your other endpoints like /test-detection and /list-snapshots are fine
app.get('/list-snapshots', async (req, res) => { /* ... your logic ... */ });
app.get('/test-detection/:file1/:file2', async (req, res) => { /* ... your logic ... */ });

async function start() {
  await ensureDirectories();
  app.listen(PORT, () => {
    console.log(`NFT Activity Tracker running on port ${PORT}`);
    console.log(`Data directory: ${DATA_DIR}`);
  });
}

start();
