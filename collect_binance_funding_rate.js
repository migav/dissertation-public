import BinanceSpot from './connector/spotExchanges/BinanceSpot.js';
import BinancePerp from './connector/futuresExchanges/BinancePerp.js';
import TradingPair from './connector/TradingPair.js';
import FundingRateHistory from './modules/FundingRateHistory.js';
import DB from './modules/DB.js';
import fs from 'fs';

// Configuration
const BTC_PAIR = new TradingPair('BTC', 'USDT');
const DEFAULT_YEARS_BACK = 2;
const DEFAULT_OUTPUT_FILE = './data/binance_btc_funding_rate_history.csv';
const EXCHANGE = 'binance';

// Interval configuration - aligned with funding periods
// Binance funding period = 8 hours
// Using 1 week intervals = 21 funding periods (keeps data aligned)
const FUNDING_PERIOD_MS = 8 * 60 * 60 * 1000; // 8 hours in ms
const INTERVAL_FUNDING_PERIODS = 21; // 21 funding periods = 1 week
const INTERVAL_MS = FUNDING_PERIOD_MS * INTERVAL_FUNDING_PERIODS; // ~1 week

/**
 * Align timestamp to nearest funding period boundary (00:00, 08:00, 16:00 UTC)
 * Always rounds DOWN to ensure we only include completed funding periods
 */
function alignToFundingPeriod(timestamp) {
    const date = new Date(timestamp);
    const hours = date.getUTCHours();
    const fundingHour = Math.floor(hours / 8) * 8;
    date.setUTCHours(fundingHour, 0, 0, 0);
    return date.getTime();
}

/**
 * Format duration in human-readable format
 */
function formatDuration(ms) {
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    
    if (hours > 0) {
        return `${hours}h ${minutes % 60}m ${seconds % 60}s`;
    } else if (minutes > 0) {
        return `${minutes}m ${seconds % 60}s`;
    }
    return `${seconds}s`;
}

function parseArgs() {
    const args = process.argv.slice(2);
    let yearsBack = DEFAULT_YEARS_BACK;
    let outputFile = DEFAULT_OUTPUT_FILE;

    for (let i = 0; i < args.length; i++) {
        if (args[i] === '--years' || args[i] === '-y') {
            const value = Number(args[i + 1]);
            if (!Number.isNaN(value) && value > 0) {
                yearsBack = value;
                i += 1;
            }
        } else if (args[i] === '--output' || args[i] === '-o') {
            if (args[i + 1]) {
                outputFile = args[i + 1];
                i += 1;
            }
        }
    }

    return { yearsBack, outputFile };
}

async function collectFundingRateHistory() {
    const { yearsBack, outputFile } = parseArgs();
    console.log('═══════════════════════════════════════════════════════════════');
    console.log('  Binance BTC Funding Rate History Collection');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`Collecting data for last ${yearsBack} years`);
    console.log(`Interval size: ${INTERVAL_FUNDING_PERIODS} funding periods (~${Math.round(INTERVAL_MS / (24 * 60 * 60 * 1000))} days)`);
    console.log(`Output file: ${outputFile}`);
    console.log(`Exchange: ${EXCHANGE}`);
    
    // Initialize database
    const db = new DB();
    await db.setupTable();
    await db.migrateAddExchangeColumn();
    console.log('✅ Database initialized');

    // Initialize connectors (API keys not needed for public data)
    const spotClient = new BinanceSpot('', '');
    const futuresClient = new BinancePerp('', '');

    // Initialize FundingRateHistory with exchange parameter
    const frh = new FundingRateHistory(spotClient, futuresClient, db, EXCHANGE);

    // Calculate date range (last N years)
    const rawEndDate = Date.now();
    const rawStartDate = rawEndDate - (yearsBack * 365.25 * 24 * 60 * 60 * 1000);
    
    // Align to funding period boundaries
    // End date = last COMPLETED funding period (round down to exclude current incomplete period)
    // Start date = aligned to funding period boundary
    const startDate = alignToFundingPeriod(rawStartDate);
    const endDate = alignToFundingPeriod(rawEndDate);  // Round DOWN to last completed period
    
    console.log(`\nDate range: ${new Date(startDate).toISOString()} to ${new Date(endDate).toISOString()}`);
    console.log(`(Only complete funding periods - current incomplete period excluded)`);
    
    // Calculate number of intervals
    const totalDuration = endDate - startDate;
    const numIntervals = Math.ceil(totalDuration / INTERVAL_MS);
    
    console.log(`Total intervals to process: ${numIntervals}`);
    console.log('═══════════════════════════════════════════════════════════════\n');

    const collectionStartTime = Date.now();
    let allData = [];
    let successfulIntervals = 0;
    let failedIntervals = 0;

    // Process each interval
    for (let i = 0; i < numIntervals; i++) {
        const intervalStart = startDate + (i * INTERVAL_MS);
        const intervalEnd = Math.min(intervalStart + INTERVAL_MS, endDate);
        
        const progress = ((i + 1) / numIntervals * 100).toFixed(1);
        const elapsed = Date.now() - collectionStartTime;
        const estimatedTotal = (elapsed / (i + 1)) * numIntervals;
        const remaining = estimatedTotal - elapsed;
        
        console.log(`\n┌─────────────────────────────────────────────────────────────┐`);
        console.log(`│ Interval ${i + 1}/${numIntervals} (${progress}%)                                    │`);
        console.log(`│ ${new Date(intervalStart).toISOString()} → ${new Date(intervalEnd).toISOString()} │`);
        console.log(`│ Elapsed: ${formatDuration(elapsed)} | ETA: ${formatDuration(remaining)}                  │`);
        console.log(`└─────────────────────────────────────────────────────────────┘`);

        try {
            const intervalStartTime = Date.now();
            
            // Fetch data for this interval (compress=false for raw data)
            const jsonData = await frh.getHistoricalData(BTC_PAIR, intervalStart, intervalEnd, false);
            const intervalData = JSON.parse(jsonData);
            
            const intervalDuration = Date.now() - intervalStartTime;
            
            if (intervalData.length > 0) {
                allData = allData.concat(intervalData);
                successfulIntervals++;
                console.log(`✅ Collected ${intervalData.length} data points in ${formatDuration(intervalDuration)}`);
                console.log(`   Total so far: ${allData.length} data points`);
            } else {
                failedIntervals++;
                console.log(`⚠️  No data for this interval (took ${formatDuration(intervalDuration)})`);
            }
            
        } catch (error) {
            failedIntervals++;
            console.error(`❌ Error in interval ${i + 1}: ${error.message}`);
        }
    }

    const totalDurationMs = Date.now() - collectionStartTime;
    
    console.log('\n═══════════════════════════════════════════════════════════════');
    console.log('  Collection Complete');
    console.log('═══════════════════════════════════════════════════════════════');
    console.log(`Total time: ${formatDuration(totalDurationMs)}`);
    console.log(`Successful intervals: ${successfulIntervals}/${numIntervals}`);
    console.log(`Failed intervals: ${failedIntervals}`);
    console.log(`Total data points: ${allData.length}`);

    if (allData.length === 0) {
        console.error('\n❌ No data collected!');
        process.exit(1);
    }

    // Sort data by timestamp (in case intervals returned overlapping data)
    allData.sort((a, b) => a.timestamp - b.timestamp);
    
    // Remove duplicates based on timestamp
    const uniqueData = allData.filter((item, index, self) =>
        index === self.findIndex(t => t.timestamp === item.timestamp)
    );
    
    console.log(`Unique data points after deduplication: ${uniqueData.length}`);

    // Convert to CSV
    const csvHeader = 'timestamp,datetime,spotPrice,futuresPrice,indexPrice,premiumIndex,estimatedFundingRate,realisedFundingRate,fundingPeriodStart,fundingPeriodEnd\n';
    const csvRows = uniqueData.map(row => {
        return [
            row.timestamp,
            new Date(row.timestamp).toISOString(),
            row.spotPrice,
            row.futuresPrice,
            row.indexPrice,
            row.premiumIndex,
            row.estimatedFundingRate,
            row.realisedFundingRate,
            row.fundingPeriodStart,
            row.fundingPeriodEnd
        ].join(',');
    }).join('\n');

    const csvContent = csvHeader + csvRows;

    // Write to file
    fs.writeFileSync(outputFile, csvContent, 'utf8');
    console.log(`\n✅ Data saved to ${outputFile}`);
    console.log(`Total records: ${uniqueData.length}`);

    // Print sample data
    console.log('\nSample data (first 3 rows):');
    console.log(csvHeader.trim());
    csvRows.split('\n').slice(0, 3).forEach(row => console.log(row));
    
    console.log('\nSample data (last 3 rows):');
    csvRows.split('\n').slice(-3).forEach(row => console.log(row));
}

// Run the collection
collectFundingRateHistory().catch(console.error);
