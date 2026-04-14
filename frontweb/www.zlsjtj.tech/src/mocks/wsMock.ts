export type MockWsOptions = {
  onMessage: (data: string) => void;
  symbols?: string[];
};

function isoNow() {
  return new Date().toISOString();
}

function round2(n: number) {
  return Math.round(n * 100) / 100;
}

function randBetween(min: number, max: number) {
  return min + Math.random() * (max - min);
}

type OrderState = {
  id: string;
  symbol: string;
  side: 'buy' | 'sell';
  orderType: 'market' | 'limit';
  qty: number;
  price?: number;
  filledQty: number;
  status: 'new' | 'partially_filled' | 'filled' | 'canceled' | 'rejected';
};

let orderSeq = 0;

function nextOrderId() {
  orderSeq += 1;
  return `mock_ord_${orderSeq}_${Date.now().toString(16)}`;
}

// Mock WS generator: emits messages that follow the contract and can drive the UI demo.
export function startMockWs(options: MockWsOptions) {
  const symbols = options.symbols ?? ['BTCUSDT', 'ETHUSDT', 'AAPL'];
  const prices: Record<string, number> = {
    BTCUSDT: 65_000,
    ETHUSDT: 3_200,
    AAPL: 190,
  };
  for (const s of symbols) {
    prices[s] ??= randBetween(50, 500);
  }

  let equity = 100_000;
  let peak = equity;

  const openOrders: OrderState[] = [];

  const tickTimer = window.setInterval(() => {
    for (const symbol of symbols) {
      const last = prices[symbol]!;
      const step = (Math.random() - 0.5) * (symbol.includes('BTC') ? 120 : symbol.includes('ETH') ? 10 : 0.8);
      const price = Math.max(0.01, last + step);
      prices[symbol] = price;
      const msg = JSON.stringify({
        type: 'tick',
        symbol,
        ts: isoNow(),
        price: round2(price),
        bid: round2(price - randBetween(0.01, 0.5)),
        ask: round2(price + randBetween(0.01, 0.5)),
        volume: round2(randBetween(0.01, 3.5)),
      });
      options.onMessage(msg);
    }
  }, 1000);

  const equityTimer = window.setInterval(() => {
    const drift = (Math.random() - 0.45) * 120;
    equity = Math.max(1, equity + drift);
    peak = Math.max(peak, equity);
    const dd = peak <= 0 ? 0 : (peak - equity) / peak;
    const msg = JSON.stringify({
      type: 'equity',
      ts: isoNow(),
      equity: round2(equity),
      pnl: round2(equity - 100_000),
      dd: round2(dd),
    });
    options.onMessage(msg);
  }, 1200);

  const positionTimer = window.setInterval(() => {
    const pairs = [
      { symbol: 'BTCUSDT', qty: 0.08, avgPrice: 62_000 },
      { symbol: 'ETHUSDT', qty: 1.1, avgPrice: 3_000 },
    ];
    for (const p of pairs) {
      if (!symbols.includes(p.symbol)) continue;
      const last = prices[p.symbol] ?? p.avgPrice;
      const msg = JSON.stringify({
        type: 'position',
        ts: isoNow(),
        symbol: p.symbol,
        qty: p.qty,
        avgPrice: p.avgPrice,
        unrealizedPnl: round2((last - p.avgPrice) * p.qty),
      });
      options.onMessage(msg);
    }
  }, 3000);

  const orderTimer = window.setInterval(() => {
    // Create a new order sometimes
    if (openOrders.length < 3 && Math.random() < 0.6) {
      const symbol = symbols[Math.floor(Math.random() * symbols.length)]!;
      const side = Math.random() > 0.5 ? 'buy' : 'sell';
      const orderType = Math.random() > 0.3 ? 'limit' : 'market';
      const qty = round2(randBetween(0.01, symbol.includes('USDT') ? 0.08 : 10));
      const price = orderType === 'limit' ? round2(prices[symbol]! * randBetween(0.995, 1.005)) : undefined;
      openOrders.push({
        id: nextOrderId(),
        symbol,
        side,
        orderType,
        qty,
        price,
        filledQty: 0,
        status: 'new',
      });
    }

    // Progress existing orders
    for (const o of openOrders) {
      if (o.status === 'filled' || o.status === 'canceled' || o.status === 'rejected') continue;
      const r = Math.random();
      if (r < 0.1) {
        o.status = 'canceled';
      } else if (r < 0.85) {
        o.filledQty = Math.min(o.qty, o.filledQty + o.qty * randBetween(0.2, 0.6));
        o.status = o.filledQty >= o.qty ? 'filled' : 'partially_filled';
      }

      const msg = JSON.stringify({
        type: 'order',
        id: o.id,
        ts: isoNow(),
        symbol: o.symbol,
        side: o.side,
        orderType: o.orderType,
        qty: o.qty,
        price: o.price,
        filledQty: round2(o.filledQty),
        status: o.status,
      });
      options.onMessage(msg);
    }

    // Remove finished orders
    for (let i = openOrders.length - 1; i >= 0; i -= 1) {
      const o = openOrders[i]!;
      if (o.status === 'filled' || o.status === 'canceled' || o.status === 'rejected') openOrders.splice(i, 1);
    }
  }, 2500);

  const logTimer = window.setInterval(() => {
    const levels = ['info', 'warn', 'error'] as const;
    const lvl = levels[Math.floor(Math.random() * levels.length)]!;
    const msg = JSON.stringify({
      type: 'log',
      level: lvl,
      source: Math.random() > 0.5 ? 'system' : 'strategy',
      ts: isoNow(),
      message: lvl === 'error' ? 'Mock alert: simulated error' : 'Mock log message',
    });
    options.onMessage(msg);
  }, 5000);

  return () => {
    window.clearInterval(tickTimer);
    window.clearInterval(equityTimer);
    window.clearInterval(positionTimer);
    window.clearInterval(orderTimer);
    window.clearInterval(logTimer);
  };
}
