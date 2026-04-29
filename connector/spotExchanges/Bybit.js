import BaseSpotExchange from '../BaseSpotExchange.js';
import { WebsocketClient, RestClientV5, WS_KEY_MAP } from 'bybit-api';
import {
  OrderBookLevel,
  OrderBooksStore,
} from 'orderbooks';

import {join} from '../utils.js';

export default class Bybit extends BaseSpotExchange {
  constructor(apiKey, apiSecret, onOpen) {
    super('Bybit');

    this.promises = {};
    this.userCallbacks = {};
    this.orderBooks = {};

    this.apiKey = apiKey;
    this.apiSecret = apiSecret;

    this.restClient = new RestClientV5({key: apiKey, secret: apiSecret});

    this.client = new WebsocketClient({key: apiKey, secret: apiSecret, market: 'v5'});

    this.client.on('update', (data) => {
      if (data.topic && data.topic.includes('orderbook')) {
        this.handleOrderbookUpdate(data);
      } else {
        if(this.userCallbacks[data.topic]) this.userCallbacks[data.topic](data);
      }
    });

    this.client.on('open', (data) => {
      if(onOpen) onOpen(super.getExchangeName);
    });

    this.client.on('response', (data) => {
      // handle responses if needed
    });

    this.client.on('reconnect', ({ wsKey }) => {
      console.log('ws automatically reconnecting.... ', wsKey);
    });

    this.client.on('reconnected', (data) => {
      console.log('ws has reconnected ', data?.wsKey);
    });

    this.client.on('error', (data) => {
      console.log('ws exception: ', data);
    });
  }

  subscribeToOrderBook(tradingPair, depth, resCallback, self) {
    const symbol = this.getSymbol(tradingPair);
    depth = depth || 50;

    const topic = 'orderbook.' + depth + '.' + symbol;
    this.client.subscribeV5(topic, 'spot');

    this.orderBooks[topic] = new OrderBooksStore({
      traceLog: false,
      checkTimestamps: false,
      maxDepth: 100
    });

    this.userCallbacks[topic] = (data) => {
      data.tradingPair = tradingPair;
      data.type = 'spot';
      data.self = self;
      resCallback(data);
    };
  }

  unsubscribeFromTopic(topic) {
    if (this.userCallbacks[topic]) {
      this.client.unsubscribeV5(topic);
      delete this.userCallbacks[topic];
      delete this.callbacksData[topic];
      delete this.orderBooks[topic];
    }
  }

  subscribeToOrdersChanges(resCallback, self) { // присылает только измененный ордер
    const topic = 'order';
    this.client.subscribeV5(topic, 'spot');
    //this.userCallbacks[topic] = resCallback;
    this.userCallbacks[topic] = (data) => {
      data.self = self;
      resCallback(data);
    };
  }

  subscribeToFastExecutionChanges(resCallback, self) { //
    const topic = 'execution.fast';
    this.client.subscribeV5(topic, 'spot'); //category = The value is only important when connecting to public topics and will be ignored for private topics.
    //this.userCallbacks[topic] = resCallback;
    this.userCallbacks[topic] = (data) => {
      data.self = self;
      resCallback(data);
    };
  }

  subscribeToPublicTrade(tradingPair, resCallback, self) {
    const symbol = this.getSymbol(tradingPair);
    const topic = 'publicTrade.' + symbol;
    this.client.subscribeV5(topic, 'spot');
    this.userCallbacks[topic] = (data) => {
      data.tradingPair = tradingPair;
      data.type = 'spot.publicTrade';
      data.self = self;
      resCallback(data);
    };
  }

  processOrderBook(orderBook) {
    const processedOrderBook = {
      lastUpdateId: orderBook.lastUpdateTimestamp,
      bids: orderBook.book
        .filter(([_, __, type]) => type === 'Buy')
        .map(([_, price, __, quantity]) => [parseFloat(price), parseFloat(quantity)]),
      asks: orderBook.book
        .filter(([_, __, type]) => type === 'Sell')
        .map(([_, price, __, quantity]) => [parseFloat(price), parseFloat(quantity)])
        .reverse()
    };


    return processedOrderBook;
  }

  handleOrderbookUpdate(message) {
    if (this.orderBooks[message.topic] === undefined) return;

    const { topic, type, data, cts } = message;
    const [topicKey, symbol] = topic.split('.');

    const bidsArray = data.b.map(([price, amount]) => {
      return OrderBookLevel(symbol, +price, 'Buy', +amount);
    });

    const asksArray = data.a.map(([price, amount]) => {
      return OrderBookLevel(symbol, +price, 'Sell', +amount);
    });

    const allBidsAndAsks = [...bidsArray, ...asksArray];

    if (type === 'snapshot') {
      // store inititial snapshot
      const storedOrderbook = this.orderBooks[topic].handleSnapshot(
        symbol,
        allBidsAndAsks,
        cts,
      );

      this.userCallbacks[topic](this.processOrderBook(storedOrderbook));
      return;
    }

    if (type === 'delta') {
      const upsertLevels = [];
      const deleteLevels = [];

      // Seperate "deletes" from "updates/inserts"
      allBidsAndAsks.forEach((level) => {
        const [_symbol, _price, _side, qty] = level;

        if (qty === 0) {
          deleteLevels.push(level);
        } else {
          upsertLevels.push(level);
        }
      });

      // Feed delta into orderbook store
      const storedOrderbook = this.orderBooks[topic].handleDelta(
        symbol,
        deleteLevels,
        upsertLevels,
        [],
        cts,
      );

      this.userCallbacks[topic](this.processOrderBook(storedOrderbook));
      return;
    }

    console.error('unhandled orderbook update type: ', type);

  }

  async getOrderBook(tradingPair) {
    const symbol = this.getSymbol(tradingPair);

    const res = await this.restClient.getOrderbook({category: 'spot', symbol: symbol, limit: 50});
    return this.processRestOrderBook(res);
  }

  processRestOrderBook(orderBook) {
    const processedOrderBook = {
      lastUpdateId: orderBook.time,
      bids: orderBook.result.b.map(([price, quantity]) => [parseFloat(price), parseFloat(quantity)]),
      asks: orderBook.result.a.map(([price, quantity]) => [parseFloat(price), parseFloat(quantity)])
    };

    return processedOrderBook;
  }

  async createNewOrder(order) {
    order = this.getOrder(order);

    return this.manageResponse(this.restClient.submitOrder.bind(this.restClient), order);

  }

  async createNewOrderWS(order) {
    order = this.getOrder(order);
    order.category = "spot";

    return this.manageResponse(this.client.sendWSAPIRequest.bind(this.client), WS_KEY_MAP.v5PrivateTrade, 'order.create', order);

  }

  async updateOrderWS(updateOrder) {
    updateOrder = this.getUpdateOrder(updateOrder);

    return this.manageResponse(this.client.sendWSAPIRequest.bind(this.client), WS_KEY_MAP.v5PrivateTrade, 'order.amend', updateOrder);
  }

  async cancelOrderWS(options) {
    return this.manageResponse(this.client.sendWSAPIRequest.bind(this.client), WS_KEY_MAP.v5PrivateTrade, 'order.cancel', options);
  }

  async createNewOrders(orders) { // exchanges limit max number of orders in a batch, so in case of larger batches, the code should be improved
    orders.forEach( (order, index) => {
      orders[index] = this.getOrder(order);
    });

    return this.manageResponse(this.restClient.batchSubmitOrders.bind(this.restClient), 'spot', orders);
  }

  async updateOrders(updateOrders) {
    updateOrders.forEach( (updateOrder, index) => {
      updateOrders[index] = this.getUpdateOrder(updateOrder);
    });

    return this.manageResponse(this.restClient.batchAmendOrders.bind(this.restClient), 'spot', updateOrders);
  }

  async getActiveOrders(tradingPair) {
    const symbol = this.getSymbol(tradingPair);
    const options = {
      category: 'spot',
      symbol: symbol,
      openOnly: 0,
      limit: 10,
    };

    return this.manageResponse(this.restClient.getActiveOrders.bind(this.restClient), options);
  }

  async cancelAllOrders(options) {
    /*options = {
        category: 'spot',
        settleCoin: 'USDC',
    }*/

    return this.manageResponse(this.restClient.cancelAllOrders.bind(this.restClient), options);

  }

  async cancelOrder(options) {
    return this.manageResponse(this.restClient.cancelOrder.bind(this.restClient), options);
  }


  async getPublicTradeHistory(tradingPair, _from, _to, isByApi) {
    const symbol = tradingPair.getCustomFormat('');
    const trades = [];

    // Convert _from and _to (assumed in ms) to Date objects.
    const startDate = new Date(Number(_from));
    const endDate = new Date(Number(_to));
    if(isByApi) {
      //https://github.com/tiagosiebler/bybit-api/blob/master/src/rest-client-v5.ts#L394
      const options = {
        category: 'spot',
        symbol: symbol
      };
      return this.manageResponse(this.restClient.getPublicTradingHistory.bind(this.restClient), options);

    } else {
      // Build the Bybit public file list URL
      let url = 'https://api2.bybit.com/quote/public/support/download/list-files?bizType=spot&productId=trade&symbols=';
      url += symbol + '&interval=daily&startDay=';
      url += this.getCustomDateString(startDate) + '&endDay=' + this.getCustomDateString(endDate);

      const response = await fetch(url);
      const data = await response.json();

      if (!data.result || !Array.isArray(data.result.list)) return [];

      // Sort files by date ascending
      const fileList = data.result.list.sort((a, b) => a.date.localeCompare(b.date));

      for (const file of fileList) {
        const fileUrl = file.url;
        try {
          console.log(`[Bybit] Downloading and unzipping trade file for date: ${file.date}`);
          const resp = await fetch(fileUrl);
          if (!resp.ok) {
            console.error(`Failed to download archive for ${file.date}`);
            continue;
          }
          const arrayBuffer = await resp.arrayBuffer();
          const buffer = Buffer.from(arrayBuffer);

          // Dynamically import 'node:zlib' and 'node:stream' for gunzip
          const { createGunzip } = await import('node:zlib');
          const { Readable } = await import('node:stream');
          // Dynamically import 'node:buffer' for TextDecoder if needed
          const { TextDecoder } = await import('node:util');

          // Unzip .gz file
          const gunzip = createGunzip();
          const chunks = [];
          await new Promise((resolve, reject) => {
            const readable = Readable.from(buffer);
            readable.pipe(gunzip)
              .on('data', chunk => chunks.push(chunk))
              .on('end', resolve)
              .on('error', reject);
          });
          const csvData = Buffer.concat(chunks).toString('utf8');

          // Parse CSV: first line as header, subsequent lines as records.
          const lines = csvData.split('\n').filter(line => line.trim().length > 0);
          if (lines.length < 2) {
            console.log(`[Bybit] No trades found in file for date: ${file.date}`);
            continue;
          }
          const headers = lines[0].split(',').map(h => h.trim());
          let tradesInFile = 0;
          for (let i = 1; i < lines.length; i++) {
            const fields = lines[i].split(',');
            const trade = {};
            headers.forEach((header, index) => {
              trade[header] = fields[index] ? fields[index].trim() : '';
            });
            // Filter by timestamp
            if (trade.timestamp && Number(trade.timestamp) >= _from && Number(trade.timestamp) <= _to) {
              trades.push(trade);
              tradesInFile++;
            }
          }
          console.log(`[Bybit] Unzipped and parsed ${tradesInFile} trades for date: ${file.date}`);
        } catch (err) {
          console.error(`Error processing trade history for ${file.date}:`, err);
        }
      }

      // Map and transform the CSV trades to the desired structure and reverse their order.
      const mappedTrades = trades.map(trade => ({
        id: trade.id,
        createTime: trade.timestamp ? Math.floor(Number(trade.timestamp) / 1000).toString() : '',
        createTimeMs: trade.timestamp || '',
        currencyPair: tradingPair.getCustomFormat('_'),
        side: trade.side ? trade.side.toLowerCase() : '',
        amount: trade.volume ? parseFloat(trade.volume) : '',
        price: trade.price ? parseFloat(trade.price) : ''
      }));

      console.log(`[Bybit] Total trades collected: ${mappedTrades.length}`);

      return mappedTrades;
    }

  }

  getCustomDateString(date) {
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const dateStr = `${year}-${month}-${day}`;
    return dateStr;
  }

  async getWalletBalance(accountType) {
    accountType = accountType || 'UNIFIED';
    const res = await this.manageResponse(this.restClient.getWalletBalance.bind(this.restClient), {accountType: accountType});
    return res;
  }


  async getBalances(coins, accountType, isRequestByCoins) {
    ///v5/asset/transfer/query-account-coins-balance
    //https://bybit-exchange.github.io/docs/v5/asset/balance/all-balance
    accountType = accountType || 'UNIFIED';
    if(!coins) isRequestByCoins = false;
    let res = [];
    //const str = coins.length>1 ? coins.join(',') : coins[0];
    /*for (let i = 0; i < coins.length; i++) {
        const x = await this.manageResponse(this.restClient.getAllCoinsBalance.bind(this.restClient), {accountType: 'UNIFIED', coin: coins[i]});
        res.push(x.result.balance);
    }*/
    //res = await this.manageResponse(this.restClient.getAllCoinsBalance.bind(this.restClient), {accountType: accountType, coin: str});
    if(isRequestByCoins) {
      for (let i = 0; i < coins.length; i++) {
        const x = await this.manageResponse(this.restClient.getAllCoinsBalance.bind(this.restClient), {accountType: 'UNIFIED', coin: coins[i]});
        res.push(x.result.balance);
      }
    } else {
      let str;
      if(coins) str = coins.length>1 ? coins.join(',') : coins[0];
      res = await this.manageResponse(this.restClient.getAllCoinsBalance.bind(this.restClient), {accountType: accountType, coin: str});
    }
    return res;
  }


  async getTradeHistory(tradingPair, _from, _to) {
    const symbol = this.getSymbol(tradingPair);
    let allTrades = [];
    const MAX_WINDOW = 7 * 24 * 60 * 60 * 1000; // 7 days in ms
    let start = _from;
    let end = Math.min(_to, start + MAX_WINDOW - 1);

    while (start < _to) {
      const options = {
        category: 'spot',
        symbol: symbol,
        startTime: start,
        endTime: end,
        limit: 100,
      };
      console.log(options);

      let nextPageCursor = undefined;
      let done = false;

      while (!done) {
        if (nextPageCursor) options.cursor = nextPageCursor;
        const res = await this.manageResponse(this.restClient.getExecutionList.bind(this.restClient), options);

        if (!res.result || !Array.isArray(res.result.list) || res.result.list.length === 0) break;

        // Filter trades within the time range
        const filtered = res.result.list.filter(trade => {
          const execTime = Number(trade.execTime);
          return execTime >= start && execTime <= end && execTime <= _to;
        });

        // Map to unified format
        const mapped = filtered.map(trade => ({
          id: trade.execId || '',
          createTime: trade.execTime ? String(Math.floor(Number(trade.execTime) / 1000)) : '',
          createTimeMs: trade.execTime ? String(trade.execTime) : '',
          currencyPair: trade.symbol || '',
          side: trade.side ? trade.side.toLowerCase() : '',
          role: trade.isMaker === undefined ? '' : (trade.isMaker ? 'maker' : 'taker'),
          amount: trade.execValue || '',
          price: trade.execPrice || '',
          orderId: trade.orderId || '',
          fee: trade.execFee || '',
          feeCurrency: trade.feeCurrency || '',
          pointFee: '',
          gtFee: '',
          amendText: '',
          sequenceId: trade.seq ? String(trade.seq) : '',
          text: trade.orderLinkId || ''
        }));

        allTrades.push(...mapped);

        nextPageCursor = res.result.nextPageCursor;
        if (!nextPageCursor) break;

        // If the last trade's execTime >= end, stop paginating this window
        const lastTrade = res.result.list[res.result.list.length - 1];
        if (Number(lastTrade.execTime) >= end) break;
      }

      start = end + 1;
      end = Math.min(_to, start + MAX_WINDOW - 1);
    }

    return allTrades;
  }

  async getKline({
                   tradingPair,
                   interval = 5,
                   start = null,
                   end   = null,
                   limit = 1000,
                   window_days = null
                 }) {
    const nowMs = Date.now();
    const symbol = this.getSymbol(tradingPair);
    window_days = window_days || 7;

    // compute UNIX timestamps in **milliseconds** (Bybit V5 uses ms)
    const endMs   = end   ? new Date(end).getTime()   : nowMs;
    let startMs = start ? new Date(start).getTime() : nowMs - 24 * 3600 * 1000 * window_days;


    let allKlines = [];
    let currentEndTime = endMs;
    while (true) {
      let data = await this.restClient.getKline({
        symbol,
        category: 'spot',
        interval,
        start: startMs,
        end:   null,
        limit
      });
      //console.log(data);
      //console.log(data?.result?.list);
      //if (!data.length) break;
      if(!data?.result?.list.length) break;
      data = data?.result?.list;
      allKlines = [...data, ...allKlines];

      const endTime = parseInt(data[0][0], 10);
      console.log('0', new Date(parseInt(data[0][0], 10)));
      console.log('endTime', new Date(endTime));
      console.log('startMs', new Date(startMs));
      if (endTime >= endMs || data.length < limit) break;

      startMs = endTime + 1;
    }
    return allKlines;

    /*const resp = await this.restClient.getKline({
      symbol,
      category: 'spot',
      interval,
      startTime: startMs,
      endTime:   endMs,
      limit
    });

    // result.spot[symbol] is an array of [ open, high, low, close, volume, timestamp ]
    return (resp) || [];*/

  }

  async getStakedPosition(coins) {
    const category = "FlexibleSaving";

    const options = {category: category};
    if(coins) {
      options.coin= coins.length > 1 ? coins.join(',') : coins[0];
    }
    return this.manageResponse(this.restClient.getEarnPosition.bind(this.restClient), options);
  }

  async createStakeRedeemOrder(orderType /*Stake, Redeem*/, options) {
    if(!hasAllFields(options, ["accountType", "amount", "coin", "productId", "orderLinkId"])) return 'createStakeRedeemOrder error';
    options.category = "FlexibleSaving";
    options.orderType = orderType;
    return this.manageResponse(this.restClient.submitStakeRedeem.bind(this.restClient), options);
  }


  async getPriceHistory(tradingPair, start, end) {
    const symbol = this.getSymbol(tradingPair);
    const interval = '1';
    let final = [];
    let currentStart = start;

    while (currentStart < end) {
      const res = await this.manageResponse(this.restClient.getKline.bind(this.restClient), {
        category: 'spot',
        symbol: symbol,
        interval: interval,
        start: currentStart,
        // end: end,
        limit: 1000
      });

      if (res.result.list.length === 0) break;

      res.result.list.reverse();
      // console.log(res.result.list[0]);
      // console.log(res.result.list[res.result.list.length - 1]);

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


}

function hasAllFields(objectToCheck, requiredFields) {
  return requiredFields.every(field => objectToCheck.hasOwnProperty(field));
}