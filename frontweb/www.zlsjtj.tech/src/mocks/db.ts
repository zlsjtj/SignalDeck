import type {
  Backtest,
  BacktestDetail,
  Fill,
  LogEntry,
  Order,
  Portfolio,
  Position,
  RiskParams,
  Strategy,
} from '@/types/api';
import { newId } from '@/utils/id';

function isoNow() {
  return new Date().toISOString();
}

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function round2(n: number) {
  return Math.round(n * 100) / 100;
}

function sample<T>(arr: T[]) {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

export type MockDb = {
  strategies: Strategy[];
  backtests: Backtest[];
  backtestDetails: Record<string, BacktestDetail>;
  backtestLogs: Record<string, LogEntry[]>;
  portfolio: Portfolio;
  positions: Position[];
  orders: Order[];
  fills: Fill[];
  risk: RiskParams;
  logs: LogEntry[];
};

export const mockDb: MockDb = {
  strategies: [],
  backtests: [],
  backtestDetails: {},
  backtestLogs: {},
  portfolio: {
    ts: isoNow(),
    equity: 100_000,
    cash: 100_000,
    pnlToday: 0,
    pnlWeek: 0,
    maxDrawdown: 0.02,
    winRate: 0.54,
    tradesToday: 12,
    tradesWeek: 88,
    equityCurve: [],
  },
  positions: [],
  orders: [],
  fills: [],
  risk: {
    enabled: true,
    maxDrawdownPct: 0.2,
    maxPositionPct: 0.3,
    maxRiskPerTradePct: 0.01,
    maxLeverage: 2,
    dailyLossLimitPct: 0.03,
    updatedAt: isoNow(),
    triggered: [],
  },
  logs: [],
};

export function seedMockDb() {
  if (mockDb.strategies.length > 0) return;

  const baseTs = isoNow();
  const s1: Strategy = {
    id: newId('stg'),
    name: 'BTC Mean Reversion',
    type: 'mean_reversion',
    status: 'running',
    config: { symbols: ['BTCUSDT'], timeframe: '1m', params: { lookback: 20, z: 1.4 } },
    createdAt: baseTs,
    updatedAt: baseTs,
  };
  const s2: Strategy = {
    id: newId('stg'),
    name: 'ETH Trend Following',
    type: 'trend_following',
    status: 'stopped',
    config: { symbols: ['ETHUSDT'], timeframe: '5m', params: { fast: 10, slow: 30 } },
    createdAt: baseTs,
    updatedAt: baseTs,
  };
  const s3: Strategy = {
    id: newId('stg'),
    name: 'AAPL Demo Strategy',
    type: 'custom',
    status: 'stopped',
    config: { symbols: ['AAPL'], timeframe: '15m', params: { note: 'demo' } },
    createdAt: baseTs,
    updatedAt: baseTs,
  };
  mockDb.strategies.push(s1, s2, s3);

  mockDb.positions = [
    {
      ts: baseTs,
      symbol: 'BTCUSDT',
      qty: 0.08,
      avgPrice: 62_000,
      lastPrice: 65_000,
      unrealizedPnl: round2((65_000 - 62_000) * 0.08),
    },
    {
      ts: baseTs,
      symbol: 'ETHUSDT',
      qty: 1.1,
      avgPrice: 3_000,
      lastPrice: 3_200,
      unrealizedPnl: round2((3_200 - 3_000) * 1.1),
    },
  ];

  mockDb.portfolio.equityCurve = Array.from({ length: 60 }).map((_, i) => {
    const ts = new Date(Date.now() - (59 - i) * 60_000).toISOString();
    const drift = (i - 30) * 6;
    const noise = (Math.random() - 0.5) * 120;
    const equity = 100_000 + drift + noise;
    return { ts, equity: round2(equity) };
  });
  mockDb.portfolio.ts = isoNow();
  mockDb.portfolio.equity = mockDb.portfolio.equityCurve.at(-1)?.equity ?? 100_000;
  mockDb.portfolio.cash = 80_000;
  mockDb.portfolio.pnlToday = round2((Math.random() - 0.3) * 900);
  mockDb.portfolio.pnlWeek = round2((Math.random() - 0.2) * 3500);

  const levels: LogEntry['level'][] = ['info', 'info', 'warn', 'error'];
  const sources: LogEntry['source'][] = ['system', 'strategy', 'backtest', 'ws'];
  mockDb.logs = Array.from({ length: 30 }).map((_, i) => ({
    id: newId('log'),
    ts: new Date(Date.now() - (29 - i) * 30_000).toISOString(),
    level: sample(levels),
    source: sample(sources),
    message: `Mock log #${i + 1}`,
  }));

  // Add a realistic "down" style log occasionally
  if (Math.random() < 0.25) {
    mockDb.logs.push({
      id: newId('log'),
      ts: isoNow(),
      level: 'error',
      source: 'system',
      message: 'Risk engine reported a transient failure (mock).',
    });
  }

  // Keep drawdown within [0, 0.5] range in mock
  mockDb.portfolio.maxDrawdown = clamp(mockDb.portfolio.maxDrawdown, 0, 0.5);
}

seedMockDb();

