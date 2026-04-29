import BaseSpotExchange from '../BaseSpotExchange.js';
import { Spot, WebsocketAPI, WebsocketStream } from '@binance/connector';
import { randomString } from '@binance/connector/src/helpers/utils.js';

// Behind Bybit connector in terms of features support, current priority -- Bybit
export default class Binance extends BaseSpotExchange {
    constructor(apiKey, apiSecret, onOpen) {
        super();
        
        this.promises = {};
        this.apiKey = apiKey;
        this.apiSecret = apiSecret;
    
        const callbacks = {
            open: () => {
                console.log('Connected with websocket server');
                onOpen();
            },
            close: () => console.log('Disconnected with Websocket server'),
            message: (data) => {
                data = JSON.parse(data);
                if (!this.promises[data.id]) return;
                const { resolve } = this.promises[data.id];
                resolve(this.processOrderBook(data.result));
                delete this.promises[data.id];
            }
        }
        
        this.client = new Spot(apiKey, apiSecret);

        this.websocketAPIClient = new WebsocketAPI(apiKey, apiSecret, {callbacks});

        // this.websocketStreamClient = new WebsocketStream(apiKey, apiSecret, {callbacks});
        
    }

    getOrderBook(tradingPair) {

        return new Promise((resolve, reject) => {
            if (!this.websocketAPIClient) {
                return reject(new Error('WebSocket client is not connected'));
            }
            
            const id = randomString();
            this.promises[id] = { resolve, reject };
            console.log(this.promises);

            this.websocketAPIClient.orderbook(tradingPair, {id});

        });
    }

    processOrderBook(orderBook) {
        const processedOrderBook = {
            lastUpdateId: orderBook.lastUpdateId,
            bids: orderBook.bids.map(([price, quantity]) => [parseFloat(price), parseFloat(quantity)]),
            asks: orderBook.asks.map(([price, quantity]) => [parseFloat(price), parseFloat(quantity)])
        };
    
        return processedOrderBook;
    }

    subscribeToOrderBook(tradingPair, resCallback) {
        const stream = new WebsocketStream({
            apiKey: this.apiKey, 
            apiSecret: this.apiSecret, 
            callbacks: {
                message: data => resCallback(this.processOrderBook(JSON.parse(data)))
            }
        });
        stream.partialBookDepth(tradingPair, '20', '100ms');
        return stream;
    }

    // to be tested
    createNewOrder(order) {
        return new Promise((resolve, reject) => {
            if (!this.websocketAPIClient) {
                return reject(new Error('WebSocket client is not connected'));
            }
            
            const id = randomString();
            this.promises[id] = { resolve, reject };
            console.log(this.promises);

            this.websocketAPIClient.newOrder(order.tradingPair, order.side, order.type, {
                timeInForce: '',
                price: order.price,
                quantity: order.amount,
                newClientOrderId: order.client_order_id,
                newOrderRespType: 'FULL',
                id: id
            });

        });

    
    }

    // to be tested
    cancelOrder(tradingPair, client_order_id) {
        return new Promise((resolve, reject) => {
            if (!this.websocketAPIClient) {
                return reject(new Error('WebSocket client is not connected'));
            }
            
            const id = randomString();
            this.promises[id] = { resolve, reject };
            console.log(this.promises);

            this.websocketAPIClient.cancelOrder(tradingPair, {newClientOrderId: client_order_id, id: id});

        });
        
    }

    stakeUSDT(amount) {
        this.client.subscribeFlexibleProduct('USDT001', amount);
    }

    stakeUSDC(amount) {
        this.client.subscribeFlexibleProduct('USDC001', amount);
    }

    redeemUSDT(amount) {
        this.client.redeemFlexibleProduct('USDT001', {amount})
        .then(response => console.log(response.data))
        .catch(error => console.error(error));
    }

    redeemUSDC(amount) {
        this.client.redeemFlexibleProduct('USDC001', {amount})
        .then(response => console.log(response.data))
        .catch(error => console.error(error));
    }
}