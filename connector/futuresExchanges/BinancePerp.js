import BaseSpotExchange from '../BaseSpotExchange.js';
import TradingPair from '../TradingPair.js';
import axios from 'axios';

const BINANCE_FUTURES_BASE_URL = 'https://fapi.binance.com';

export default class BinancePerp extends BaseSpotExchange {
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
            const url = `${BINANCE_FUTURES_BASE_URL}/fapi/v1/klines?symbol=${symbol}&interval=${interval}&startTime=${currentStart}&limit=1500`;
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

    async getIndexPriceHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        const interval = '1m';
        let final = [];
        let currentStart = start;
        let count = 1;

        while (currentStart + 60 * 1000 < end) {
            const url = `${BINANCE_FUTURES_BASE_URL}/fapi/v1/indexPriceKlines?pair=${symbol}&interval=${interval}&startTime=${currentStart}&limit=1500`;
            const data = await this.fetchWithRetry(url);

            console.log('Query #', count, data.length);
            count += 1;

            if (data.length === 0) break;

            for (let i = 0; i < data.length; i++) {
                const timestamp = parseInt(data[i][0]);
                if (timestamp > end) return final;
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

    async getPremiumIndexHistory(tradingPair, start, end) {
        // Binance: Premium Index = (Mark Price - Index Price) / Index Price
        // We need to fetch both mark price and index price klines, then compute the ratio
        const symbol = this.getSymbol(tradingPair);
        const interval = '1m';
        
        // Fetch mark price klines
        let markPrices = [];
        let currentStart = start;
        let count = 1;

        console.log('Fetching mark prices for premium index calculation...');
        while (currentStart + 60 * 1000 < end) {
            const url = `${BINANCE_FUTURES_BASE_URL}/fapi/v1/markPriceKlines?symbol=${symbol}&interval=${interval}&startTime=${currentStart}&limit=1500`;
            const data = await this.fetchWithRetry(url);

            console.log('Query #', count, data.length);
            count += 1;

            if (data.length === 0) break;

            for (let i = 0; i < data.length; i++) {
                const timestamp = parseInt(data[i][0]);
                if (timestamp > end) break;
                markPrices.push([timestamp, parseFloat(data[i][4])]); // close mark price
            }

            if (markPrices.length > 0) {
                currentStart = markPrices[markPrices.length - 1][0] + 60000;
            } else {
                break;
            }
        }

        // Fetch index price klines for the same period
        let indexPrices = [];
        currentStart = start;
        count = 1;

        console.log('Fetching index prices for premium index calculation...');
        while (currentStart + 60 * 1000 < end) {
            const url = `${BINANCE_FUTURES_BASE_URL}/fapi/v1/indexPriceKlines?pair=${symbol}&interval=${interval}&startTime=${currentStart}&limit=1500`;
            const data = await this.fetchWithRetry(url);

            console.log('Query #', count, data.length);
            count += 1;

            if (data.length === 0) break;

            for (let i = 0; i < data.length; i++) {
                const timestamp = parseInt(data[i][0]);
                if (timestamp > end) break;
                indexPrices.push([timestamp, parseFloat(data[i][4])]); // close index price
            }

            if (indexPrices.length > 0) {
                currentStart = indexPrices[indexPrices.length - 1][0] + 60000;
            } else {
                break;
            }
        }

        // Create a map for index prices
        const indexPriceMap = {};
        for (const [timestamp, price] of indexPrices) {
            indexPriceMap[timestamp] = price;
        }

        // Compute premium index: (Mark Price - Index Price) / Index Price
        const final = [];
        for (const [timestamp, markPrice] of markPrices) {
            const indexPrice = indexPriceMap[timestamp];
            if (indexPrice && indexPrice !== 0) {
                const premiumIndex = (markPrice - indexPrice) / indexPrice;
                final.push([timestamp, premiumIndex]);
            }
        }

        return final;
    }

    async getRealisedFundingRateHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        let final = [];
        let currentStart = start;

        while (currentStart < end) {
            const url = `${BINANCE_FUTURES_BASE_URL}/fapi/v1/fundingRate?symbol=${symbol}&startTime=${currentStart}&endTime=${end}&limit=1000`;
            const data = await this.fetchWithRetry(url);

            if (data.length === 0) break;

            for (let i = 0; i < data.length; i++) {
                // Normalize timestamp to minute boundary (floor to nearest minute)
                // Binance sometimes returns timestamps with milliseconds like 1769040000010
                const rawTimestamp = parseInt(data[i].fundingTime);
                const normalizedTimestamp = Math.floor(rawTimestamp / 60000) * 60000;
                
                final.push([
                    normalizedTimestamp,
                    parseFloat(data[i].fundingRate)
                ]);
            }

            if (data.length > 0) {
                currentStart = parseInt(data[data.length - 1].fundingTime) + 1;
            } else {
                break;
            }

            await new Promise(resolve => setTimeout(resolve, 100));
        }

        // Remove duplicates
        final = final.filter((v, i, a) => a.findIndex(t => t[0] === v[0]) === i);

        // Sort by timestamp
        final.sort((a, b) => a[0] - b[0]);

        return final;
    }

    async getFuturesPair(tradingPair) {
        // Binance futures pairs are typically just the same symbol
        // BTCUSDT in spot = BTCUSDT in futures
        return new TradingPair(tradingPair.base, tradingPair.quote);
    }

    generatePotentialSymbols(symbol) {
        return [symbol, '1000' + symbol];
    }
}
