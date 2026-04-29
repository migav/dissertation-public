import BaseSpotExchange from '../BaseSpotExchange.js';
import axios from 'axios';

const BINANCE_SPOT_BASE_URL = 'https://api.binance.com';

/**
 * Simple Binance spot client for data collection (no API keys required for public data)
 */
export default class BinanceSpot extends BaseSpotExchange {
    constructor(apiKey, apiSecret, onOpen) {
        super('Binance');
        
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;
    }

    getSymbol(tradingPair) {
        return tradingPair.getCustomFormat('');
    }

    async fetchWithRetry(url, retries = 3) {
        let lastError = null;
        for (let attempt = 1; attempt <= retries; attempt++) {
            try {
                const response = await axios.get(url);
                return response.data;
            } catch (error) {
                lastError = error;
                console.log(`Binance API request failed (attempt ${attempt}/${retries}): ${error.message}`);
                await new Promise(resolve => setTimeout(resolve, 300 * attempt));
            }
        }
        throw lastError;
    }

    async getPriceHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        const interval = '1m';
        let final = [];
        let currentStart = start;

        while (currentStart < end) {
            const url = `${BINANCE_SPOT_BASE_URL}/api/v3/klines?symbol=${symbol}&interval=${interval}&startTime=${currentStart}&limit=1000`;
            const data = await this.fetchWithRetry(url);

            if (data.length === 0) break;

            for (let i = 0; i < data.length; i++) {
                const timestamp = parseInt(data[i][0]);
                if (timestamp > end) break;
                final.push([timestamp, parseFloat(data[i][4])]); // close price
            }

            if (final.length > 0) {
                currentStart = final[final.length - 1][0] + 60000;
            } else {
                break;
            }
        }

        return final;
    }
}
