import fs from 'node:fs';

export default class FundingRateHistory {
    constructor(spotClient, futuresClient, db, exchange = 'bybit') {
        this.spotClient = spotClient;
        this.futuresClient = futuresClient;
        this.db = db;
        this.exchange = exchange;
    }

    async getRecentData(spotPair, compress = true, comressTime = 15) {
        const futuresPair = await this.futuresClient.getFuturesPair(spotPair);
        const multiplier = futuresPair.getMultiplier();

        console.log('GETTING RECENT DATA');

        const fundingPeriodInHours = await this.getFundingPeriodInHours(futuresPair);

        const endDate = Date.now();
        let startDate = this.processTimePeriod(Date.now(), Date.now(), fundingPeriodInHours)[0];

        startDate += 60000;

        const data = await this.getHistoricalDataFromExchange(spotPair, futuresPair, multiplier, fundingPeriodInHours, startDate, endDate);

        let res = [];

        if (compress) {
            res = this.compressFinalFundingAnalyticsData(data, comressTime);
        } else {
            res = data;
        }


        return this.transformIntoJSON(res);
    }

    async getHistoricalDataFromDB(spotPair, startDate, endDate, compress = true, comressTime = 15) {

        const rawdbData = await this.db.getSnapshots(spotPair.getStandardFormat(), startDate, endDate, this.exchange);
        let dbData = rawdbData.map(({ id, exchange, pair, ...rest }) => Object.values(rest));

        if (compress) {
            dbData = this.compressFinalFundingAnalyticsData(dbData, comressTime); 
        }

        return this.transformIntoJSON(dbData);
    }  

    async getHistoricalData(spotPair, startDate, endDate, compress = true, comressTime = 15) {

        const futuresPair = await this.futuresClient.getFuturesPair(spotPair);
        const multiplier = futuresPair.getMultiplier();

        const fundingPeriodInHours = await this.getFundingPeriodInHours(futuresPair);

        startDate += 60000;

        // await this.db.setupTable();
        
        // console.log('until', endDate);
        const rawdbData = await this.db.getSnapshots(spotPair.getStandardFormat(), startDate, endDate, this.exchange);
        console.log(rawdbData[0]);
        const dbData = rawdbData.map(({ id, exchange, pair, ...rest }) => Object.values(rest));
        // console.log(dbData);

        console.log('dbData.length = ', dbData.length);

        let res7 = [];
        
        if (dbData.length > 0) {
            console.log('dbData.length > 0');
            console.log(startDate, dbData[0][0]);
            console.log(endDate, dbData[dbData.length - 1][0]);
        }
        
        if (dbData.length > 0 && dbData[0][0] == startDate && dbData[dbData.length - 1][0] == endDate) {
            if (compress) {
                res7 = this.compressFinalFundingAnalyticsData(dbData, comressTime);
            } else {
                res7 = dbData;
            }

            console.log(res7.length);
            console.log('here');
            // this.transformIntoCSV(res7);
            return this.transformIntoJSON(res7);
        } else if (dbData.length > 0) {
            const part1 = await this.getHistoricalDataFromExchange(spotPair, futuresPair, multiplier, fundingPeriodInHours, startDate, dbData[0][0]);
            // const part1 = [];
            const part2 = dbData;
            const part3 = await this.getHistoricalDataFromExchange(spotPair, futuresPair, multiplier, fundingPeriodInHours, dbData[dbData.length - 1][0], endDate);

            if (part1.length > 0 && part1[part1.length - 1][0] == dbData[0][0]) {
                part1.pop();
            }

            console.log(startDate, dbData[0][0], dbData[dbData.length - 1][0], endDate);
            if (part1.length > 0) console.log(part1[part1.length - 1][0]);

            console.log('inserting part1');
            // console.log(part1);
            await this.db.insertSnapshots(spotPair.getStandardFormat(), part1, this.exchange);
            console.log('inserted part1');

            if (part3.length > 0 && part3[0][0] == dbData[dbData.length - 1][0]) {
                part3.shift();
            }

            // console.log(part3);

            await this.db.insertSnapshots(spotPair.getStandardFormat(), part3, this.exchange);
            console.log('inserted part3');
            

            if (compress) {
                res7 = this.compressFinalFundingAnalyticsData(part1.concat(part2).concat(part3), comressTime);
            } else {
                res7 = part1.concat(part2).concat(part3);
            }


            console.log('res7.length', res7.length);
            // this.transformIntoCSV(res7);
            return this.transformIntoJSON(res7);

        } else {
            const part = await this.getHistoricalDataFromExchange(spotPair, futuresPair, multiplier, fundingPeriodInHours, startDate, endDate);
            console.log('part.length ', part.length);

            await this.db.insertSnapshots(spotPair.getStandardFormat(), part, this.exchange);

            if (compress) {
                res7 = this.compressFinalFundingAnalyticsData(part, comressTime);
            } else {
                res7 = part;
            }

            console.log('res7.length ', res7.length);
            // this.transformIntoCSV(res7);
            return this.transformIntoJSON(res7);

        }

        

    }


    async getHistoricalDataFromExchange(spotPair, futuresPair, multiplier, fundingPeriodInHours, startDate, endDate) {

        if (startDate >= endDate) return [];

        // let [startDateRounded, endDateRounded] = this.processTimePeriod(startDate, endDate, fundingPeriodInHours);
        let startDateRounded = startDate;
        let endDateRounded = endDate;

        console.log('getHistoricalDataFromExchange: ' + spotPair.getStandardFormat() + ' ' + futuresPair.getStandardFormat());
        console.log('📊 Fetching spot price history...');
        const res = await this.spotClient.getPriceHistory(spotPair, startDateRounded, endDateRounded);
        console.log(`✅ Spot prices: ${res.length} data points (${res[0] ? new Date(res[0][0]).toISOString() : 'N/A'} to ${res[res.length - 1] ? new Date(res[res.length - 1][0]).toISOString() : 'N/A'})`);

        if (this.dataGaps(res)) {
            return [];
        }

        console.log('📊 Fetching futures price history...');
        const res2 = await this.futuresClient.getPriceHistory(futuresPair, startDateRounded, endDateRounded);
        console.log(`✅ Futures prices: ${res2.length} data points (${res2[0] ? new Date(res2[0][0]).toISOString() : 'N/A'} to ${res2[res2.length - 1] ? new Date(res2[res2.length - 1][0]).toISOString() : 'N/A'})`);

        if (this.dataGaps(res2)) {
            return [];
        }

        console.log('📊 Fetching index price history...');
        const res3 = await this.futuresClient.getIndexPriceHistory(futuresPair, startDateRounded, endDateRounded);
        console.log(`✅ Index prices: ${res3.length} data points (${res3[0] ? new Date(res3[0][0]).toISOString() : 'N/A'} to ${res3[res3.length - 1] ? new Date(res3[res3.length - 1][0]).toISOString() : 'N/A'})`);

        if (this.dataGaps(res3)) {
            return [];
        }

        console.log('📊 Fetching premium index history...');
        const res4 = await this.futuresClient.getPremiumIndexHistory(futuresPair, startDateRounded, endDateRounded);
        console.log(`✅ Premium index: ${res4.length} data points (${res4[0] ? new Date(res4[0][0]).toISOString() : 'N/A'} to ${res4[res4.length - 1] ? new Date(res4[res4.length - 1][0]).toISOString() : 'N/A'})`);

        if (this.dataGaps(res4)) {
            return [];
        }

        console.log('📊 Fetching realised funding rate history...');
        // Fetch funding rates aligned to period boundaries so we can compute the first/last full period
        const fundingPeriodMs = fundingPeriodInHours * 60 * 60 * 1000;
        const [fundingRateFetchStart] = this.processTimePeriod(startDateRounded, startDateRounded, fundingPeriodInHours);
        const fundingRateFetchEnd = endDateRounded + fundingPeriodMs;
        const res5 = await this.futuresClient.getRealisedFundingRateHistory(
            futuresPair,
            fundingRateFetchStart,
            fundingRateFetchEnd
        );
        console.log(`✅ Funding rates: ${res5.length} data points (${res5[0] ? new Date(res5[0][0]).toISOString() : 'N/A'} to ${res5[res5.length - 1] ? new Date(res5[res5.length - 1][0]).toISOString() : 'N/A'})`);

        // if (this.dataGaps(res5, 8 * 60 * 60 * 1000)) { // 8 hours of funding
        //     return [];
        // }
        
        let res6 = [];
        
        if (res5.length == 0) {
            res6 = this.processFundingAnalyticsDataRecent(res, res2, res3, res4, multiplier, fundingPeriodInHours);
        } else {
            res6 = this.processFundingAnalyticsData(res, res2, res3, res4, res5, multiplier);
        }

        // console.log(res6);
        console.log('res6.length = ', res6.length);

        res6 = res6.filter(dataPoint => dataPoint[0] >= startDate && dataPoint[0] <= endDate);

        if (this.dataGaps(res6)) {
            console.log('DATA GAPS IN FINAL');
            return [];
        }


        // this.db.insertSnapshots(spotPair.getStandardFormat(), res6);

        // const res7 = this.compressFinalFundingAnalyticsData(res6, 15);
        // console.log(res7.length);

        // this.transformIntoJSON(res7);
        // this.transformIntoCSV(res7);

        if (res6.length > 0) {
            // remove all data points with timestamp out of [startDate, dbdata[0][0] ] range
            while (res6[0][0] < startDate) {
                res6.shift();
            }

            while (res6[res6.length - 1][0] > endDate) {
                res6.pop();
            }
        }

        return res6;
    }


    async getSigma(spotPair, updateWithRecentData = false) {

        let fundingRateData = [];

        const startDate = Date.now() - 1000 * 60 * 60 * 24 * 30 * 6;
        const endDate = Date.now();

        const [startDate2, endDate2] = this.processTimePeriod(startDate, endDate);

        if (updateWithRecentData) {
            fundingRateData = JSON.parse(await this.getHistoricalData(spotPair, startDate2, endDate2, true, 5));
        } else {
            fundingRateData = JSON.parse(await this.getHistoricalDataFromDB(spotPair, startDate2, endDate2, true, 5));
        }

        const basisValues = fundingRateData.map(({
            spotPrice,
            futuresPrice
        }) => (futuresPrice - spotPrice) / spotPrice);

        const N = basisValues.length;
        const mean = basisValues.reduce((a, b) => a + b, 0) / N;
        const variance = basisValues.reduce((a, b) => a + (b - mean) ** 2, 0) / N;

        return Math.sqrt(variance);

    }

    dataGaps(res, interval = 60000, maxTolerableGapMinutes = 5) {
        const maxTolerableGapMs = maxTolerableGapMinutes * 60 * 1000;
        for (let i = 1; i < res.length; i++) {
            const timeDiff = res[i][0] - res[i - 1][0];
            if (timeDiff === 0) {
                console.log('Duplicate timestamp detected, removing: ', res[i][0]);
                res.splice(i, 1);
                i--; // Adjust index after removal
            } else if (timeDiff != interval) {
                if (timeDiff <= maxTolerableGapMs) {
                    // Small gap - tolerate it and log a warning
                    console.log(`Small data gap (${timeDiff / 60000} min) tolerated: ${new Date(res[i-1][0]).toISOString()} - ${new Date(res[i][0]).toISOString()}`);
                } else {
                    console.log('Data gap detected: ', res[i][0], ' - ', res[i - 1][0]);
                    return true;
                }
            }
        }
        return false;
    }

    transformIntoCSV(res) {
        let text = '';
    
        for (let i = 0; i < res.length; i++) {
            for (let j = 0; j < res[i].length - 1; j++) {
                text += res[i][j] + '\t';
            }
            text += res[i][res[i].length - 1] + '\n';
        }
    
        fs.writeFile('data.txt', text, () => {});
    }

    transformIntoJSON(res) {
        // console.log(res);
        
        const mappedRes = res.map((x) => ({
            timestamp: x[0],
            spotPrice: x[1],
            futuresPrice: x[2],
            indexPrice: x[3],
            premiumIndex: x[4],
            estimatedFundingRate: x[5],
            realisedFundingRate: x[6],
            fundingPeriodStart: x[7],
            fundingPeriodEnd: x[8]
        }));

        const json = JSON.stringify(mappedRes);
    
        // fs.writeFile('data.json', json, () => {});
        return json;
    }

    async getFundingPeriodInHours(spotPair) {
        const futuresPair = await this.futuresClient.getFuturesPair(spotPair);
        const data = await this.futuresClient.getRealisedFundingRateHistory(futuresPair, Date.now() - 1000 * 60 * 60 * 24, Date.now());
        return (data[1][0] - data[0][0]) / 1000 / 60 / 60;
    }

    processTimePeriod(startDate, endDate, fundingPeriodInHours = 8) {
        const adjustToUTC = (timestamp) => {
            const date = new Date(timestamp);
            const hours = date.getUTCHours();
            const offset = hours % fundingPeriodInHours;
            if (offset !== 0) {
                date.setUTCHours(hours - offset, 0, 0, 0);
            } else {
                date.setUTCHours(hours, 0, 0, 0);
            }
            return date.getTime();
        };

        startDate = adjustToUTC(startDate);
        endDate = adjustToUTC(endDate);

        return [startDate, endDate];
    }

    processFundingAnalyticsDataRecent(spotPrices, futuresPrices, indexPrices, premiumIndexes, multiplier, fundingPeriodInHours) {
        let [startDate, endDate] = this.processTimePeriod(Date.now(), Date.now(), fundingPeriodInHours);

        startDate += fundingPeriodInHours * 60 * 60 * 1000;
        endDate += fundingPeriodInHours * 60 * 60 * 1000;

        let res = [];

        const futuresPricesMap = this.arrayToMap(futuresPrices);
        const indexPricesMap = this.arrayToMap(indexPrices);
        const premiumIndexesMap = this.arrayToMap(premiumIndexes);

        for (let i = 0; i < spotPrices.length; i++) {

            const timestamp = spotPrices[i][0];

            const averagePremiumIndex = this.calculateAveragePremiumIndex(premiumIndexesMap, timestamp, startDate + 60000);
            const estimatedfundingRate = averagePremiumIndex + this.clamp(0.0001 - averagePremiumIndex, -0.0005, 0.0005);

            const spotPrice = spotPrices[i][1];
            const futuresPrice = futuresPricesMap[timestamp] / multiplier;
            const indexPrice = indexPricesMap[timestamp] / multiplier;
            const premiumIndex = premiumIndexesMap[timestamp];
            const realisedFundingRate = estimatedfundingRate;


            if (isNaN(estimatedfundingRate)) {
                console.log(i, ' PremiumIndex = ', averagePremiumIndex);
                continue;
            }

            res.push([timestamp, spotPrice, futuresPrice, indexPrice, premiumIndex, estimatedfundingRate, realisedFundingRate, startDate, endDate]);
        }

        return res;

    }

    processFundingAnalyticsData(spotPrices, futuresPrices, indexPrices, premiumIndexes, realisedFundingRates, multiplier) {
        let res = [];
        
        if (realisedFundingRates.length == 1) return res;

        let currentRealisedFundingRateIndex = 0;
        // const FUNDING_PERIOD = 8 * 60 * 60 * 1000; // 8 hours of funding
        const futuresPricesMap = this.arrayToMap(futuresPrices);
        const indexPricesMap = this.arrayToMap(indexPrices);
        const premiumIndexesMap = this.arrayToMap(premiumIndexes);

        let firstSpotIndex = 0;

        // Unify starting point

        while (spotPrices[firstSpotIndex][0] < realisedFundingRates[0][0]) {
            firstSpotIndex += 1;
        }

        while (spotPrices[firstSpotIndex][0] >= realisedFundingRates[currentRealisedFundingRateIndex][0]) {
            currentRealisedFundingRateIndex += 1;
            if(!realisedFundingRates[currentRealisedFundingRateIndex]) {
                console.log(realisedFundingRates);
                console.log('[processFundingAnalyticsData] error will be here: ', currentRealisedFundingRateIndex);
                //break;
            }
        }


        currentRealisedFundingRateIndex -= 1;
        console.log('starting index', currentRealisedFundingRateIndex);
        console.log(spotPrices[firstSpotIndex][0]);
        console.log(realisedFundingRates[currentRealisedFundingRateIndex][0]);
        
        
        // Now spotPrice timestamp = realisedFundingRate timestamp + 1min

        // Now we actually calculate stuff
        for (let i = firstSpotIndex; i < spotPrices.length; i++) {
            if (currentRealisedFundingRateIndex == realisedFundingRates.length - 1) return res;
            //if (spotPrices[i][0] > realisedFundingRates[currentRealisedFundingRateIndex][0] + FUNDING_PERIOD) {
            if (spotPrices[i][0] > realisedFundingRates[currentRealisedFundingRateIndex + 1][0]) {
                currentRealisedFundingRateIndex += 1;
                if (currentRealisedFundingRateIndex == realisedFundingRates.length - 1) return res;
            }

            const timestamp = spotPrices[i][0];
            // console.log(currentRealisedFundingRateIndex, realisedFundingRates.length);
            const averagePremiumIndex = this.calculateAveragePremiumIndex(premiumIndexesMap, timestamp, realisedFundingRates[currentRealisedFundingRateIndex][0] + 60000);
            const estimatedfundingRate = averagePremiumIndex + this.clamp(0.0001 - averagePremiumIndex, -0.0005, 0.0005);

            const spotPrice = spotPrices[i][1];
            const futuresPrice = futuresPricesMap[timestamp] / multiplier;
            const indexPrice = indexPricesMap[timestamp] / multiplier;
            const premiumIndex = premiumIndexesMap[timestamp];
            const realisedFundingRate = realisedFundingRates[currentRealisedFundingRateIndex + 1][1];


            if (isNaN(estimatedfundingRate)) {
                console.log(i, ' PremiumIndex = ', averagePremiumIndex);
                continue;
            }

            const fundingPeriodStart = realisedFundingRates[currentRealisedFundingRateIndex][0];
            const fundingPeriodEnd = realisedFundingRates[currentRealisedFundingRateIndex + 1][0];

            res.push([timestamp, spotPrice, futuresPrice, indexPrice, premiumIndex, estimatedfundingRate, realisedFundingRate, fundingPeriodStart, fundingPeriodEnd]);
        }

        return res;
    }


    compressFinalFundingAnalyticsData(res, interval) { // interval is a number of minutes
        let finalRes = [];
        let currentRes = [];

        for (let i = 0; i < res.length; i++) {
            if (currentRes.length == 0) {
                currentRes.push(res[i]);
            } else if (res[i][0] - currentRes[0][0] <= (interval - 1) * 60000) {
                currentRes.push(res[i]);
            } else {
                finalRes.push(this.calculateAverage(currentRes));
                i -= 1;
                currentRes = [];
            }
        }

        if (currentRes.length > 0) {
            finalRes.push(this.calculateAverage(currentRes));
        }
        return finalRes;
    }

    calculateAverage(res) {
        let answer = [];
        for (let i = 0; i < res[0].length; i++) {
            if (i == 0) { answer.push(res[res.length - 1][0]); continue; }
            if (i == 7) { answer.push(res[res.length - 1][7]); continue; }
            if (i == 8) { answer.push(res[res.length - 1][8]); continue; }

            let sum = 0;
            for (let j = 0; j < res.length; j++) {
                sum += res[j][i];
            }
            answer.push(sum / res.length);
            // if (i != 0) { answer.push(res[res.length - 1][i]); continue; }
            
        }

        return answer;

    }

    calculateAveragePremiumIndex(premiumIndexesMap, timestamp, fundingIntervalBeforeTimestamp) {
        let res = 0;
        let sum = 0;

        let i = fundingIntervalBeforeTimestamp;
        let count = 1;

        // console.log(premiumIndexesMap[i], i);

        while (i < timestamp) {
            const premiumIndex = premiumIndexesMap[i];
            // Skip missing timestamps (small gaps in data)
            if (premiumIndex !== undefined && !isNaN(premiumIndex)) {
                res += count * premiumIndex;
                sum += count;
            }
            count += 1;
            i += 60000;
        }
        
        if (sum == 0) return 0;
        return res / sum;
    }

    arrayToMap(arr) {
        const res = {};
        arr.forEach(element => {
            res[element[0]] = element[1];
        });
        return res;
    };
    
    clamp(x, startRange, endRange) {
        return Math.max(Math.min(x, endRange), startRange);
    }

}