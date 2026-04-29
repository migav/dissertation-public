export default class BaseSpotExchange {

    constructor(exchangeName) {
        this.exchangeName = exchangeName;
    }

    get getExchangeName() {return this.exchangeName}

    getOrderBook(tradingPair) {}
    subscribeToOrderBook(tradingPair, callbackFunction) {}

    createNewOrder(order) {}
    cancelOrder(tradingPair, client_order_id) {}

    stakeUSDT(amount) {}
    stakeUSDC(amount) {}

    redeemUSDT(amount) {}
    redeemUSDC(amount) {}

    getSymbol(tradingPair) {
        if (this.exchangeName == 'Bybit') return tradingPair.getCustomFormat('');
        else if (this.exchangeName == 'Binance') return tradingPair.getCustomFormat('');
    }

    getOrder(order) {
        if (this.exchangeName == 'Bybit') {
            order.symbol = this.getSymbol(order.tradingPair);
            order.category = 'spot';
            order.qty = String(order.qty);
            order.price = String(order.price);
            order.orderLinkId = order.customOrderID;

            delete order.tradingPair;
            delete order.customOrderID;

            return order;
        } else if (this.exchangeName == 'Binance') {
            return;
        }
    }

    getUpdateOrder(updateOrder) {
        if (this.exchangeName == 'Bybit') {
            updateOrder.symbol = this.getSymbol(updateOrder.tradingPair);
            updateOrder.category = 'spot';
            updateOrder.qty = String(updateOrder.qty);
            updateOrder.price = String(updateOrder.price);
            updateOrder.orderLinkId = updateOrder.customOrderID;

            delete updateOrder.tradingPair;
            delete updateOrder.customOrderID;

            if (updateOrder.qty == '') delete updateOrder.qty;
            if (updateOrder.price == '') delete updateOrder.price;
            return updateOrder;
        } else if (this.exchangeName == 'Binance') {
            return;
        }
    }

    async getKline({
                       tradingPair,
                       interval,
                       start = null,
                       end   = null,
                   }) {

    }

    async manageResponse(func, ...args) {
        return new Promise((resolve, reject) => {
            func(...args)
                .then((response) => {
                    if(response?.retMsg === "OK" || response?.retMsg === 'success' || response?.retCode === 0) {
                        resolve(response);
                    } else {
                        console.log('[ERR] bad response ', response?.retMsg);
                        resolve(response);
                        //reject(response);
                    }
                })
                .catch((error) => {
                    //console.log('\x1b[41m%s\x1b[0m','!!! manageResponse error:');
                    console.log('\x1b[41m%s\x1b[0m','!!! manageResponse error - ' +  error.retMsg);
                    /*
!!! manageResponse error: {
  wsKey: 'v5PrivateTrade',
  reqId: '2',
  retCode: 170213,
  retMsg: 'Order does not exist.',
  op: 'order.amend',
  data: {},
  retExtInfo: {},
  header: {
    'X-Bapi-Limit': '10',
    'X-Bapi-Limit-Status': '9',
    'X-Bapi-Limit-Reset-Timestamp': '1742989374153',
    Traceid: '45cfef24e39df8d02de9de2f286b4ef8',
    Timenow: '1742989374154'
  },
  connId: 'cuulu06p49kmdhdni600-4v9px'
}
                    * */
                    console.error(args);
                    //reject(error)
                });
        });
    };

}
