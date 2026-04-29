import Binance from './spotExchanges/Binance.js';
import Bybit from './spotExchanges/Bybit.js';
import BybitPerp from './futuresExchanges/BybitPerp.js';

export default class Connector {
    constructor(exchangeName, isSpot, apiKey, apiSecret, onOpen, passphrase = null) {
        let type = 'futures';
        if (isSpot) type = 'spot';

        this.exchangeName = exchangeName;

        if (exchangeName == 'Binance') this.client = new Binance(apiKey, apiSecret, onOpen);
        else if (exchangeName == 'Bybit' && isSpot) this.client = new Bybit(apiKey, apiSecret, onOpen);
        else if (exchangeName == 'Bybit' && !isSpot) this.client = new BybitPerp(apiKey, apiSecret, onOpen);
        else {
            console.log('Exchange not supported');
            return;
        }
    }
}


