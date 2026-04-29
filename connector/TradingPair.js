export default class TradingPair {
  constructor(base, quote, decimals) {
    this.base = base;
    this.quote = quote;
    this.decimals = decimals || 4;
    /*this.market = {
      spot: {best: {bid: [0,0], ask: [0,0], timestamp: null}, last: [0,0], timestamp: null},
      perp: {best: {bid: [0,0], ask: [0,0], timestamp: null}, last: [0,0], timestamp: null},
    }*/
    this.subscriptions = {
      spot: [],
      perp: []
    }
  }

  /*getReferencePrice(category, side) {
    let price;
    if(side === 'bid') {
      price = this.market[category].best.bid[0];
      if(this.market[category].timestamp > this.market[category].best.timestamp) price = Math.min(price, this.market[category].last[0]);
    } else {
      price = this.market[category].best.ask[0];
      if(this.market[category].timestamp > this.market[category].best.timestamp) price = Math.max(price, this.market[category].last[0]);
    }
    return price;
  }*/

  getStandardFormat() {
    return this.base + '-' + this.quote;
  }

  getCustomFormat(delimeter) {
    return this.base + delimeter + this.quote;
  }

  getCoinsArray() {return [this.base, this.quote]};

  getMultiplier() {
    const cases = this.base.match(/\d+/g);

    if (cases) {
      return cases[0];
    } else {
      return 1;
    }
  }

  Round(data, decimals) {
    decimals = decimals || this.decimals;
    return (Math.round(data * 10**decimals)/ 10**decimals);
  }
  get getOnePips() {
    return 1/(10**this.decimals);
  }
}