import { create } from 'zustand';

import type { Fill, LogEntry, Order, Position } from '@/types/api';
import type { EquityMessage, LogMessage, OrderMessage, PositionMessage, TickMessage } from '@/types/ws';
import { newId } from '@/utils/id';

export type Candle = {
  time: number; // epoch seconds (lightweight-charts UTCTimestamp)
  open: number;
  high: number;
  low: number;
  close: number;
};

function isoNow() {
  return new Date().toISOString();
}

function toEpochSec(iso: string) {
  const ms = Date.parse(iso);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : Math.floor(Date.now() / 1000);
}

function clampLen<T>(arr: T[], maxLen: number) {
  if (arr.length <= maxLen) return arr;
  return arr.slice(arr.length - maxLen);
}

// Aggregate ticks into 15-minute K-line buckets, keep close price changing on each tick.
const candleBucketSec = 15 * 60;
const candleWindowSec = 24 * 60 * 60;
const maxCandlesInWindow = candleWindowSec / candleBucketSec; // 96 bars (1 day, 15m each)

function trimCandlesToDayWindow(candles: Candle[], latestBucket: number) {
  const minBucket = latestBucket - candleWindowSec + candleBucketSec;
  return candles.filter((c) => c.time >= minBucket).slice(-maxCandlesInWindow);
}

type LiveState = {
  ticksBySymbol: Record<string, TickMessage>;
  candlesBySymbol: Record<string, Candle[]>;
  equitySeries: EquityMessage[];
  positionsBySymbol: Record<string, Position>;
  ordersById: Record<string, Order>;
  fills: Fill[];
  wsLogs: LogEntry[];

  setSnapshot: (snapshot: { positions: Position[]; orders: Order[]; fills: Fill[] }) => void;
  setCandles: (symbol: string, candles: Candle[]) => void;
  pushTick: (tick: TickMessage) => void;
  pushEquity: (eq: EquityMessage) => void;
  upsertPosition: (msg: PositionMessage) => void;
  upsertOrder: (msg: OrderMessage) => void;
  pushLog: (msg: LogMessage) => void;
  clear: () => void;
};

export const useLiveStore = create<LiveState>()((set, get) => ({
  ticksBySymbol: {},
  candlesBySymbol: {},
  equitySeries: [],
  positionsBySymbol: {},
  ordersById: {},
  fills: [],
  wsLogs: [],

  setSnapshot: ({ positions, orders, fills }) => {
    const positionsBySymbol = Object.fromEntries(positions.map((p) => [p.symbol, p]));
    const ordersById = Object.fromEntries(orders.map((o) => [o.id, o]));
    set({ positionsBySymbol, ordersById, fills: fills.slice(-500) });
  },

  setCandles: (symbol, candles) => {
    const ordered = [...candles].sort((a, b) => a.time - b.time);
    const latestBucket = ordered.length > 0 ? ordered[ordered.length - 1]!.time : Math.floor(Date.now() / 1000 / candleBucketSec) * candleBucketSec;
    const clipped = trimCandlesToDayWindow(ordered, latestBucket);
    set({ candlesBySymbol: { ...get().candlesBySymbol, [symbol]: clipped } });
  },

  pushTick: (tick) => {
    const state = get();

    const symbol = tick.symbol;
    const nextTicks = { ...state.ticksBySymbol, [symbol]: tick };

    // Aggregate ticks into 15-minute bars.
    const t = toEpochSec(tick.ts);
    const bucket = Math.floor(t / candleBucketSec) * candleBucketSec;
    const candles = state.candlesBySymbol[symbol] ? [...state.candlesBySymbol[symbol]!] : [];
    const candleIndex = candles.findIndex((c) => c.time === bucket);
    if (candleIndex === -1) {
      candles.push({ time: bucket, open: tick.price, high: tick.price, low: tick.price, close: tick.price });
      candles.sort((a, b) => a.time - b.time);
    } else {
      const existing = candles[candleIndex]!;
      candles[candleIndex] = {
        ...existing,
        high: Math.max(existing.high, tick.price),
        low: Math.min(existing.low, tick.price),
        close: tick.price,
      };
    }

    const nextCandles = { ...state.candlesBySymbol, [symbol]: trimCandlesToDayWindow(candles, bucket) };

    // Update position mark-to-market if we have it.
    const pos = state.positionsBySymbol[symbol];
    let nextPositions = state.positionsBySymbol;
    if (pos) {
      const nextPos: Position = {
        ...pos,
        ts: tick.ts,
        lastPrice: tick.price,
        unrealizedPnl: Number(((tick.price - pos.avgPrice) * pos.qty).toFixed(2)),
      };
      nextPositions = { ...state.positionsBySymbol, [symbol]: nextPos };
    }

    set({ ticksBySymbol: nextTicks, candlesBySymbol: nextCandles, positionsBySymbol: nextPositions });
  },

  pushEquity: (eq) => {
    const next = clampLen([...get().equitySeries, eq], 800);
    set({ equitySeries: next });
  },

  upsertPosition: (msg) => {
    const tick = get().ticksBySymbol[msg.symbol];
    const lastPrice = tick?.price ?? msg.avgPrice;
    const pos: Position = {
      ts: msg.ts,
      symbol: msg.symbol,
      qty: msg.qty,
      avgPrice: msg.avgPrice,
      lastPrice,
      unrealizedPnl: msg.unrealizedPnl,
    };
    set({ positionsBySymbol: { ...get().positionsBySymbol, [msg.symbol]: pos } });
  },

  upsertOrder: (msg) => {
    const order: Order = {
      id: msg.id,
      ts: msg.ts,
      symbol: msg.symbol,
      side: msg.side,
      type: msg.orderType,
      qty: msg.qty,
      price: msg.price,
      filledQty: msg.filledQty,
      status: msg.status,
    };
    set({ ordersById: { ...get().ordersById, [order.id]: order } });

    // Emit fills in mock-ish way when the order becomes filled (if we don't have explicit fill stream).
    if (order.status === 'filled' && order.filledQty > 0) {
      const fill: Fill = {
        id: newId('fill'),
        ts: order.ts,
        symbol: order.symbol,
        side: order.side,
        qty: order.filledQty,
        price: order.price ?? get().ticksBySymbol[order.symbol]?.price ?? 0,
        fee: 0,
        orderId: order.id,
      };
      set({ fills: clampLen([...get().fills, fill], 800) });
    }
  },

  pushLog: (msg) => {
    const entry: LogEntry = {
      id: newId('wslog'),
      ts: msg.ts,
      level: msg.level,
      source: (msg.source === 'system' || msg.source === 'strategy' || msg.source === 'backtest'
        ? msg.source
        : 'ws') as LogEntry['source'],
      message: msg.message,
    };
    set({ wsLogs: clampLen([...get().wsLogs, entry], 400) });
  },

  clear: () => {
    set({
      ticksBySymbol: {},
      candlesBySymbol: {},
      equitySeries: [],
      positionsBySymbol: {},
      ordersById: {},
      fills: [],
      wsLogs: [
        {
          id: newId('log'),
          ts: isoNow(),
          level: 'info',
          source: 'mock',
          message: 'Live store cleared.',
        },
      ],
    });
  },
}));

