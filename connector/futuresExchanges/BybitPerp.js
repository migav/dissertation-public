import BaseSpotExchange from '../BaseSpotExchange.js';
import { RestClientV5 } from 'bybit-api';
import TradingPair from '../TradingPair.js';
import axios from 'axios';

export default class BybitPerp extends BaseSpotExchange {
    constructor(apiKey, apiSecret, onOpen) {
        super('Bybit');
        
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;

        this.restClient = new RestClientV5({key: apiKey, secret: apiSecret});
    }

    async fetchKlineList(requestFn, params, retries = 3) {
        let lastError = null;
        for (let attempt = 1; attempt <= retries; attempt++) {
            const res = await this.manageResponse(requestFn, params);
            const list = res?.result?.list;
            if (Array.isArray(list)) {
                return list;
            }
            lastError = new Error(`Bybit API returned no list (attempt ${attempt}/${retries})`);
            await new Promise(resolve => setTimeout(resolve, 300 * attempt));
        }
        throw lastError;
    }

    async getPriceHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        const interval = '1'; // 30 minutes
        let final = [];
        let currentStart = start;

        while (currentStart < end) {
            const res = await this.manageResponse(this.restClient.getKline.bind(this.restClient), {
                category: 'linear',
                symbol: symbol,
                interval: interval,
                start: currentStart,
                // end: end,
                limit: 1000
            });

            res.result.list.reverse();

            if (res.result.list.length === 0) break;

            if (final.length > 0) {
                // remove last element of final array -- to avoid duplicates
                final.pop();
            }

            for (let i = 0; i < res.result.list.length; i++) {
                if (res.result.list[i][0] > end) return final;
                final.push([parseInt(res.result.list[i][0]), parseFloat(res.result.list[i][4])]);
            }

            currentStart = parseInt(res.result.list[res.result.list.length - 1][0]) + 1;
        }

        return final;
    }


    async getIndexPriceHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        const interval = '1'; // 1 minute
        let final = [];
        let currentStart = start;

        let count = 1;

        while (currentStart + 60 * 1000 < end) {
            const list = await this.fetchKlineList(this.restClient.getIndexPriceKline.bind(this.restClient), {
                category: 'linear',
                symbol: symbol,
                interval: interval,
                start: currentStart,
                // end: end,
                limit: 1000
            });
            list.reverse();
            if (list.length === 0) break;

            console.log('Query #', count, list.length);
            count += 1;

            if (final.length > 0) {
                // remove last element of final array -- to avoid duplicates
                final.pop();
            }

            for (let i = 0; i < list.length; i++) {
                if (list[i][0] > end) return final;
                final.push([parseInt(list[i][0]), parseFloat(list[i][4])]);
            }

            currentStart = parseInt(list[list.length - 1][0]) + 1;
        }

        return final;
    }

    async getPremiumIndexHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        const interval = '1';
        let final = [];
        let currentStart = start;

        let count = 1;

        while (currentStart + 60 * 1000 < end) {
            const list = await this.fetchKlineList(this.restClient.getPremiumIndexPriceKline.bind(this.restClient), {
                category: 'linear',
                symbol: symbol,
                interval: interval,
                start: currentStart,
                // end: end,
                limit: 1000
            });
            list.reverse();
            if (list.length === 0) break;

            console.log('Query #', count, list.length);
            count += 1;

            if (final.length > 0) {
                // remove last element of final array -- to avoid duplicates
                final.pop();
            }

            for (let i = 0; i < list.length; i++) {
                if (list[i][0] > end) return final;
                final.push([parseInt(list[i][0]), parseFloat(list[i][4])]);
            }

            currentStart = parseInt(list[list.length - 1][0]) + 1;
        }

        return final;
    }

    async getRealisedFundingRateHistoryLowLevel(category, symbol, start, end, limit) {
        let config = {
            method: 'get',
            url: `https://api.bybit.com/v5/market/funding/history?category=${category}&symbol=${symbol}&startTime=${start}&endTime=${end}&limit=${limit}`,
            headers: { }
        };

        // console.log(`https://api.bybit.com/v5/market/funding/history?category=${category}&symbol=${symbol}&startTime=${start}&endTime=${end}&limit=${limit}`);
          
        const response = await axios(config);

        return response.data;

    }

    async getRealisedFundingRateHistory(tradingPair, start, end) {
        const symbol = this.getSymbol(tradingPair);
        let final = [];
        let currentEnd = end;

        let i = 0;

        while (currentEnd > start) {
            const res = await this.getRealisedFundingRateHistoryLowLevel('linear', symbol, start, currentEnd, 200);

            i++;
            // console.log(i, ': ', res.result.list[0], res.result.list[res.result.list.length - 1]);

            res.result.list.reverse();

            if (res.result.list.length === 0) break;

            for (let i = 0; i < res.result.list.length; i++) {
                final.push([parseInt(res.result.list[i].fundingRateTimestamp), parseFloat(res.result.list[i].fundingRate)]);
            }

            currentEnd = parseInt(res.result.list[0].fundingRateTimestamp) - 1;

            await new Promise(resolve => setTimeout(resolve, 100));
        }

        // remove duplicates from final
        final = final.filter((v, i, a) => a.findIndex(t => (t[0] === v[0])) === i);

        return final;
    }

    async getFuturesPair(tradingPair) {
        const symbol = this.getSymbol(tradingPair);
        const res = await this.manageResponse(this.restClient.getInstrumentsInfo.bind(this.restClient), {category: 'linear'});

        const details = res.result.list;
        const potentialSymbols = this.generatePotentialSymbols(symbol);

        for (let i = 0; i < details.length; i++) {
            const id = potentialSymbols.findIndex( s => s === details[i].symbol);
            if(id !== -1) return new TradingPair(details[i].baseCoin, details[i].quoteCoin);

        }

    }

    generatePotentialSymbols(symbol) {
        return [symbol, '1000' + symbol, '1000000' + symbol];
    }
}