const express = require('express');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// API Key for authentication
const API_KEY = process.env.API_KEY || 'CHANGE_THIS_SECRET_KEY';

// Rate limiting setup
const requestCounts = new Map();

function rateLimit(maxRequests, windowMs) {
  return (req, res, next) => {
    const ip = req.headers['x-forwarded-for'] || req.connection.remoteAddress;
    const now = Date.now();
    const key = `${ip}-${req.path}`;
    
    if (!requestCounts.has(key)) {
      requestCounts.set(key, []);
    }
    
    const timestamps = requestCounts.get(key).filter(time => now - time < windowMs);
    
    if (timestamps.length >= maxRequests) {
      return res.status(429).json({ 
        error: 'Rate limit exceeded. Please try again later.',
        retry_after: Math.ceil(windowMs / 1000)
      });
    }
    
    timestamps.push(now);
    requestCounts.set(key, timestamps);
    next();
  };
}

// Clean up old rate limit entries every 10 minutes
setInterval(() => {
  const now = Date.now();
  for (const [key, timestamps] of requestCounts.entries()) {
    const recent = timestamps.filter(time => now - time < 3600000);
    if (recent.length === 0) {
      requestCounts.delete(key);
    } else {
      requestCounts.set(key, recent);
    }
  }
}, 600000);

// Authentication middleware
function requireAuth(req, res, next) {
  const provided = req.headers['x-api-key'];
  if (API_KEY === 'CHANGE_THIS_SECRET_KEY') {
    console.warn('WARNING: Using default API key. Set API_KEY environment variable!');
  }
  if (!provided || provided !== API_KEY) {
    return res.status(401).json({ error: 'Unauthorized - Invalid or missing API key' });
  }
  next();
}

// Enable CORS for local HTML file access
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type, x-api-key');
  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// Data directory - will be on persistent disk when deployed
const DATA_DIR = process.env.DATA_DIR || './data';
const SOURCE_URL = 'https://deving.zone/en/nfts/alliance_daos.json';

// Ensure data directory structure exists
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

// Utility: Get current UTC timestamp
function getUTCTimestamp() {
  return new Date().toISOString();
}

// Utility: Format date for filenames
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

// Get ISO week number
function getWeekNumber(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return String(weekNo).padStart(2, '0');
}

// Endpoint 1: Hourly snapshot capture (PROTECTED)
app.post('/capture-snapshot', requireAuth, rateLimit(10, 3600000), async (req, res) => {
  try {
    console.log(`[${getUTCTimestamp()}] Starting hourly snapshot capture...`);
    
    // Fetch the master JSON
    const response = await axios.get(SOURCE_URL, { timeout: 30000 });
    const data = response.data;
    
    // Create timestamp for filename
    const timestamp = formatDate(new Date(), 'full');
    const filename = `${timestamp}.json`;
    const filepath = path.join(DATA_DIR, 'snapshots', filename);
    
    // Save raw snapshot
    await fs.writeFile(filepath, JSON.stringify(data, null, 2));
    
    console.log(`[${getUTCTimestamp()}] Snapshot saved: ${filename}`);
    
    // Extract NFT count - data structure has collection_stats and nfts array
    const nftCount = data.nfts ? data.nfts.length : (Array.isArray(data) ? data.length : Object.keys(data).length);
    
    res.json({
      success: true,
      timestamp,
      filename,
      nft_count: nftCount
    });
  } catch (error) {
    console.error(`[${getUTCTimestamp()}] Snapshot capture failed:`, error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Endpoint 2: Daily event summarization (PROTECTED)
app.post('/process-daily-events', requireAuth, rateLimit(5, 3600000), async (req, res) => {
  try {
    console.log(`[${getUTCTimestamp()}] Starting daily event processing...`);
    
    // Process previous day's data
    const yesterday = new Date();
    yesterday.setUTCDate(yesterday.getUTCDate() - 1);
    const targetDate = formatDate(yesterday, 'date');
    
    // Load all 24 hourly snapshots from yesterday
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
    
    if (snapshots.length === 0) {
      throw new Error(`No snapshots found for ${targetDate}`);
    }
    
    console.log(`Found ${snapshots.length} snapshots for ${targetDate}`);
    
    // Detect events
    const events = detectEvents(snapshots);
    
    // Save daily summary
    const dailyFile = path.join(DATA_DIR, 'daily', `${targetDate}.json`);
    await fs.writeFile(dailyFile, JSON.stringify(events, null, 2));
    
    console.log(`[${getUTCTimestamp()}] Daily summary saved: ${targetDate}.json`);
    
    res.json({
      success: true,
      date: targetDate,
      snapshots_processed: snapshots.length,
      total_events: events.summary.total_events
    });
  } catch (error) {
    console.error(`[${getUTCTimestamp()}] Daily processing failed:`, error.message);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Event detection logic
function detectEvents(snapshots) {
  const activityLog = {};
  const summary = {
    bbl_sales: 0,
    boost_sales: 0,
    transfers: 0,
    bbl_listings: 0,
    bbl_delistings: 0,
    boost_listings: 0,
    boost_delistings: 0,
    daodao_stakes: 0,
    daodao_unstakes: 0,
    enterprise_stakes: 0,
    enterprise_unstakes: 0,
    breaks: 0,
    total_events: 0
  };
  
  // Normalize snapshots to array format - handle both array and {nfts: []} format
  const normalizedSnapshots = snapshots.map(s => {
    if (s.nfts && Array.isArray(s.nfts)) {
      return s.nfts; // New format with collection_stats wrapper
    }
    return Array.isArray(s) ? s : Object.values(s);
  });
  
  // Process each NFT (1-10000)
  for (let nftId = 1; nftId <= 10000; nftId++) {
    const events = [];
    
    for (let i = 1; i < normalizedSnapshots.length; i++) {
      const prev = normalizedSnapshots[i - 1].find(n => n.id === nftId);
      const curr = normalizedSnapshots[i].find(n => n.id === nftId);
      
      if (!prev || !curr) continue;
      
      // Owner change detection
      if (prev.owner !== curr.owner) {
        const wasListedBbl = prev.bbl === true;
        const wasListedBoost = prev.boost === true;
        
        if (wasListedBbl) {
          events.push({
            type: 'sale',
            marketplace: 'bbl',
            from: prev.owner,
            to: curr.owner,
            hour: i
          });
          summary.bbl_sales++;
        } else if (wasListedBoost) {
          events.push({
            type: 'sale',
            marketplace: 'boost',
            from: prev.owner,
            to: curr.owner,
            hour: i
          });
          summary.boost_sales++;
        } else {
          events.push({
            type: 'transfer',
            from: prev.owner,
            to: curr.owner,
            hour: i
          });
          summary.transfers++;
        }
        summary.total_events++;
      }
      
      // Marketplace listing changes
      if (prev.bbl !== curr.bbl) {
        if (curr.bbl === true) {
          events.push({ type: 'listing', marketplace: 'bbl', hour: i });
          summary.bbl_listings++;
        } else {
          events.push({ type: 'delisting', marketplace: 'bbl', hour: i });
          summary.bbl_delistings++;
        }
        summary.total_events++;
      }
      
      if (prev.boost !== curr.boost) {
        if (curr.boost === true) {
          events.push({ type: 'listing', marketplace: 'boost', hour: i });
          summary.boost_listings++;
        } else {
          events.push({ type: 'delisting', marketplace: 'boost', hour: i });
          summary.boost_delistings++;
        }
        summary.total_events++;
      }
      
      // Staking changes
      if (prev.daodao !== curr.daodao) {
        if (curr.daodao === true) {
          events.push({ type: 'stake', protocol: 'daodao', hour: i });
          summary.daodao_stakes++;
        } else {
          events.push({ type: 'unstake', protocol: 'daodao', hour: i });
          summary.daodao_unstakes++;
        }
        summary.total_events++;
      }
      
      if (prev.enterprise !== curr.enterprise) {
        if (curr.enterprise === true) {
          events.push({ type: 'stake', protocol: 'enterprise', hour: i });
          summary.enterprise_stakes++;
        } else {
          events.push({ type: 'unstake', protocol: 'enterprise', hour: i });
          summary.enterprise_unstakes++;
        }
        summary.total_events++;
      }
      
      // Break detection
      if (prev.broken !== curr.broken) {
        events.push({
          type: 'break_change',
          from: prev.broken,
          to: curr.broken,
          hour: i
        });
        summary.breaks++;
        summary.total_events++;
      }
    }
    
    // Only add to log if there were events
    if (events.length > 0) {
      activityLog[nftId] = events;
    }
  }
  
  return {
    summary,
    activity_log: activityLog
  };
}

// Endpoint 3: Weekly aggregation (PROTECTED)
app.post('/aggregate-weekly', requireAuth, rateLimit(5, 3600000), async (req, res) => {
  try {
    const lastWeek = new Date();
    lastWeek.setUTCDate(lastWeek.getUTCDate() - 7);
    const weekId = formatDate(lastWeek, 'week');
    
    const aggregated = await aggregatePeriod('daily', 'weekly', weekId, 7);
    
    res.json({
      success: true,
      week: weekId,
      total_events: aggregated.summary.total_events
    });
  } catch (error) {
    console.error('Weekly aggregation failed:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Endpoint 4: Monthly aggregation (PROTECTED)
app.post('/aggregate-monthly', requireAuth, rateLimit(5, 3600000), async (req, res) => {
  try {
    const lastMonth = new Date();
    lastMonth.setUTCMonth(lastMonth.getUTCMonth() - 1);
    const monthId = formatDate(lastMonth, 'month');
    
    const aggregated = await aggregatePeriod('daily', 'monthly', monthId, 31);
    
    res.json({
      success: true,
      month: monthId,
      total_events: aggregated.summary.total_events
    });
  } catch (error) {
    console.error('Monthly aggregation failed:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Endpoint 5: Yearly aggregation (PROTECTED)
app.post('/aggregate-yearly', requireAuth, rateLimit(5, 3600000), async (req, res) => {
  try {
    const lastYear = new Date();
    lastYear.setUTCFullYear(lastYear.getUTCFullYear() - 1);
    const yearId = formatDate(lastYear, 'year');
    
    const aggregated = await aggregatePeriod('monthly', 'yearly', yearId, 12);
    
    res.json({
      success: true,
      year: yearId,
      total_events: aggregated.summary.total_events
    });
  } catch (error) {
    console.error('Yearly aggregation failed:', error.message);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Generic aggregation function
async function aggregatePeriod(sourceType, targetType, periodId, maxFiles) {
  const sourceDir = path.join(DATA_DIR, sourceType);
  const files = await fs.readdir(sourceDir);
  
  // Filter files for the period
  const relevantFiles = files.filter(f => f.startsWith(periodId.split('-')[0]));
  
  const combined = {
    summary: {
      bbl_sales: 0,
      boost_sales: 0,
      transfers: 0,
      bbl_listings: 0,
      bbl_delistings: 0,
      boost_listings: 0,
      boost_delistings: 0,
      daodao_stakes: 0,
      daodao_unstakes: 0,
      enterprise_stakes: 0,
      enterprise_unstakes: 0,
      breaks: 0,
      total_events: 0
    },
    activity_log: {}
  };
  
  for (const file of relevantFiles.slice(0, maxFiles)) {
    const content = await fs.readFile(path.join(sourceDir, file), 'utf8');
    const data = JSON.parse(content);
    
    // Merge summaries
    for (const key in data.summary) {
      combined.summary[key] += data.summary[key];
    }
    
    // Merge activity logs
    for (const nftId in data.activity_log) {
      if (!combined.activity_log[nftId]) {
        combined.activity_log[nftId] = [];
      }
      combined.activity_log[nftId].push(...data.activity_log[nftId]);
    }
  }
  
  const outputFile = path.join(DATA_DIR, targetType, `${periodId}.json`);
  await fs.writeFile(outputFile, JSON.stringify(combined, null, 2));
  
  return combined;
}

// Public data serving - Rate limited to prevent bandwidth abuse
app.use('/data', rateLimit(50, 900000), express.static(DATA_DIR)); // 50 requests per 15 min

// Health check - Minimal rate limit
app.get('/health', rateLimit(100, 60000), (req, res) => {
  res.json({ status: 'ok', timestamp: getUTCTimestamp() });
});

// Status endpoint - Rate limited
app.get('/status', rateLimit(30, 60000), async (req, res) => {
  try {
    const stats = {
      snapshots: 0,
      daily: 0,
      weekly: 0,
      monthly: 0,
      yearly: 0
    };
    
    for (const type of Object.keys(stats)) {
      const dir = path.join(DATA_DIR, type === 'snapshots' ? 'snapshots' : type);
      try {
        const files = await fs.readdir(dir);
        stats[type] = files.length;
      } catch (err) {
        // Directory doesn't exist yet
      }
    }
    
    res.json(stats);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Test endpoint - compare two specific snapshots (PROTECTED - prevents abuse)
app.get('/test-detection/:file1/:file2', requireAuth, rateLimit(20, 3600000), async (req, res) => {
  try {
    const { file1, file2 } = req.params;
    
    // Load the two snapshots
    const snapshot1Path = path.join(DATA_DIR, 'snapshots', file1);
    const snapshot2Path = path.join(DATA_DIR, 'snapshots', file2);
    
    const data1 = JSON.parse(await fs.readFile(snapshot1Path, 'utf8'));
    const data2 = JSON.parse(await fs.readFile(snapshot2Path, 'utf8'));
    
    // Normalize to arrays
    const nfts1 = data1.nfts || (Array.isArray(data1) ? data1 : Object.values(data1));
    const nfts2 = data2.nfts || (Array.isArray(data2) ? data2 : Object.values(data2));
    
    // Detect events between these two snapshots
    const events = detectEventsBetweenTwo(nfts1, nfts2);
    
    res.json({
      file1,
      file2,
      nfts_compared: nfts1.length,
      ...events
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// List all snapshot files (Rate limited)
app.get('/list-snapshots', rateLimit(20, 60000), async (req, res) => {
  try {
    const snapshotDir = path.join(DATA_DIR, 'snapshots');
    const files = await fs.readdir(snapshotDir);
    const sortedFiles = files.sort();
    
    res.json({
      count: files.length,
      files: sortedFiles
    });
  } catch (error) {
    res.status(500).json({ error: error.message, files: [] });
  }
});

// Helper function to detect events between two snapshots
function detectEventsBetweenTwo(prev, curr) {
  const activityLog = {};
  const summary = {
    bbl_sales: 0,
    boost_sales: 0,
    transfers: 0,
    bbl_listings: 0,
    bbl_delistings: 0,
    boost_listings: 0,
    boost_delistings: 0,
    daodao_stakes: 0,
    daodao_unstakes: 0,
    enterprise_stakes: 0,
    enterprise_unstakes: 0,
    breaks: 0,
    total_events: 0
  };
  
  // Process each NFT
  for (let nftId = 1; nftId <= 10000; nftId++) {
    const prevNFT = prev.find(n => n.id === nftId);
    const currNFT = curr.find(n => n.id === nftId);
    
    if (!prevNFT || !currNFT) continue;
    
    const events = [];
    
    // Owner change detection
    if (prevNFT.owner !== currNFT.owner) {
      const wasListedBbl = prevNFT.bbl === true;
      const wasListedBoost = prevNFT.boost === true;
      
      if (wasListedBbl) {
        events.push({
          type: 'sale',
          marketplace: 'bbl',
          from: prevNFT.owner,
          to: currNFT.owner
        });
        summary.bbl_sales++;
      } else if (wasListedBoost) {
        events.push({
          type: 'sale',
          marketplace: 'boost',
          from: prevNFT.owner,
          to: currNFT.owner
        });
        summary.boost_sales++;
      } else {
        events.push({
          type: 'transfer',
          from: prevNFT.owner,
          to: currNFT.owner
        });
        summary.transfers++;
      }
      summary.total_events++;
    }
    
    // Marketplace listing changes
    if (prevNFT.bbl !== currNFT.bbl) {
      if (currNFT.bbl === true) {
        events.push({ type: 'listing', marketplace: 'bbl' });
        summary.bbl_listings++;
      } else {
        events.push({ type: 'delisting', marketplace: 'bbl' });
        summary.bbl_delistings++;
      }
      summary.total_events++;
    }
    
    if (prevNFT.boost !== currNFT.boost) {
      if (currNFT.boost === true) {
        events.push({ type: 'listing', marketplace: 'boost' });
        summary.boost_listings++;
      } else {
        events.push({ type: 'delisting', marketplace: 'boost' });
        summary.boost_delistings++;
      }
      summary.total_events++;
    }
    
    // Staking changes
    if (prevNFT.daodao !== currNFT.daodao) {
      if (currNFT.daodao === true) {
        events.push({ type: 'stake', protocol: 'daodao' });
        summary.daodao_stakes++;
      } else {
        events.push({ type: 'unstake', protocol: 'daodao' });
        summary.daodao_unstakes++;
      }
      summary.total_events++;
    }
    
    if (prevNFT.enterprise !== currNFT.enterprise) {
      if (currNFT.enterprise === true) {
        events.push({ type: 'stake', protocol: 'enterprise' });
        summary.enterprise_stakes++;
      } else {
        events.push({ type: 'unstake', protocol: 'enterprise' });
        summary.enterprise_unstakes++;
      }
      summary.total_events++;
    }
    
    // Break detection
    if (prevNFT.broken !== currNFT.broken) {
      events.push({
        type: 'break_change',
        from: prevNFT.broken,
        to: currNFT.broken
      });
      summary.breaks++;
      summary.total_events++;
    }
    
    // Only add to log if there were events
    if (events.length > 0) {
      activityLog[nftId] = events;
    }
  }
  
  return {
    summary,
    activity_log: activityLog
  };
}

// Initialize and start server
async function start() {
  await ensureDirectories();
  app.listen(PORT, () => {
    console.log(`NFT Activity Tracker running on port ${PORT}`);
    console.log(`Data directory: ${DATA_DIR}`);
  });
}

start();
