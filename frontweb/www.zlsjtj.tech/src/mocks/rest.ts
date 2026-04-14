import type {
  AuditLogEntry,
  AuditLogsQueryParams,
  Backtest,
  BacktestDetail,
  CreateBacktestRequest,
  CreateStrategyRequest,
  HealthResponse,
  LogEntry,
  LogsQueryParams,
  Portfolio,
  RiskParams,
  UpdateRiskRequest,
  Strategy,
  UpdateStrategyRequest,
} from '@/types/api';
import { sleep } from '@/utils/sleep';
import { newId } from '@/utils/id';
import { mockDb, seedMockDb } from '@/mocks/db';

function isoNow() {
  return new Date().toISOString();
}

function round2(n: number) {
  return Math.round(n * 100) / 100;
}

function calcMaxDd(equity: number[], peak?: number) {
  let p = peak ?? equity[0] ?? 1;
  let maxDd = 0;
  for (const x of equity) {
    if (x > p) p = x;
    const dd = p <= 0 ? 0 : (p - x) / p;
    if (dd > maxDd) maxDd = dd;
  }
  return maxDd;
}

function assertFound<T>(v: T | undefined, msg: string): T {
  if (!v) throw new Error(msg);
  return v;
}

type BacktestJob = {
  timer: number;
};

const backtestJobs = new Map<string, BacktestJob>();

export const mockApi = {
  async health(): Promise<HealthResponse> {
    seedMockDb();
    await sleep(120);
    return { ok: true, ts: new Date().toISOString(), version: 'mock-0.1.0' };
  },

  async listStrategies(): Promise<Strategy[]> {
    seedMockDb();
    await sleep(180);
    return [...mockDb.strategies].sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
  },

  async createStrategy(req: CreateStrategyRequest): Promise<Strategy> {
    seedMockDb();
    await sleep(220);
    const now = isoNow();
    const stg: Strategy = {
      id: newId('stg'),
      name: req.name,
      type: req.type,
      status: 'stopped',
      config: req.config,
      createdAt: now,
      updatedAt: now,
    };
    mockDb.strategies.unshift(stg);
    mockDb.logs.push({
      id: newId('log'),
      ts: now,
      level: 'info',
      source: 'strategy',
      strategyId: stg.id,
      message: `Strategy created: ${stg.name}`,
    });
    return stg;
  },

  async getStrategy(id: string): Promise<Strategy> {
    seedMockDb();
    await sleep(140);
    const stg = mockDb.strategies.find((s) => s.id === id);
    return assertFound(stg, `Strategy not found: ${id}`);
  },

  async updateStrategy(id: string, req: UpdateStrategyRequest): Promise<Strategy> {
    seedMockDb();
    await sleep(240);
    const stg = assertFound(
      mockDb.strategies.find((s) => s.id === id),
      `Strategy not found: ${id}`,
    );
    stg.name = req.name;
    stg.type = req.type;
    stg.config = req.config;
    stg.updatedAt = isoNow();
    mockDb.logs.push({
      id: newId('log'),
      ts: stg.updatedAt,
      level: 'info',
      source: 'strategy',
      strategyId: stg.id,
      message: `Strategy updated: ${stg.name}`,
    });
    return stg;
  },

  async startStrategy(id: string): Promise<{ ok: boolean }> {
    seedMockDb();
    await sleep(260);
    const stg = assertFound(
      mockDb.strategies.find((s) => s.id === id),
      `Strategy not found: ${id}`,
    );
    stg.status = 'running';
    stg.updatedAt = isoNow();
    mockDb.logs.push({
      id: newId('log'),
      ts: stg.updatedAt,
      level: 'info',
      source: 'strategy',
      strategyId: stg.id,
      message: `Strategy started: ${stg.name}`,
    });
    return { ok: true };
  },

  async stopStrategy(id: string): Promise<{ ok: boolean }> {
    seedMockDb();
    await sleep(260);
    const stg = assertFound(
      mockDb.strategies.find((s) => s.id === id),
      `Strategy not found: ${id}`,
    );
    stg.status = 'stopped';
    stg.updatedAt = isoNow();
    mockDb.logs.push({
      id: newId('log'),
      ts: stg.updatedAt,
      level: 'warn',
      source: 'strategy',
      strategyId: stg.id,
      message: `Strategy stopped: ${stg.name}`,
    });
    return { ok: true };
  },

  async createBacktest(req: CreateBacktestRequest): Promise<Backtest> {
    seedMockDb();
    await sleep(260);
    const now = isoNow();
    const stg = assertFound(
      mockDb.strategies.find((s) => s.id === req.strategyId),
      `Strategy not found: ${req.strategyId}`,
    );

    const bt: Backtest = {
      id: newId('bt'),
      strategyId: stg.id,
      strategyName: stg.name,
      symbol: req.symbol,
      startAt: req.startAt,
      endAt: req.endAt,
      initialCapital: req.initialCapital,
      feeRate: req.feeRate,
      slippage: req.slippage,
      status: 'running',
      progress: 0,
      createdAt: now,
      updatedAt: now,
    };
    mockDb.backtests.unshift(bt);

    const detail: BacktestDetail = {
      ...bt,
      metrics: {
        cagr: 0.12,
        sharpe: 1.3,
        maxDrawdown: 0.08,
        calmar: 1.5,
        winRate: 0.54,
        trades: 0,
        pnlTotal: 0,
      },
      equityCurve: [],
      drawdownCurve: [],
      trades: [],
    };
    mockDb.backtestDetails[bt.id] = detail;
    mockDb.backtestLogs[bt.id] = [
      {
        id: newId('log'),
        ts: now,
        level: 'info',
        source: 'backtest',
        backtestId: bt.id,
        message: `Backtest created (mock): strategy=${stg.name}, symbol=${req.symbol}`,
      },
    ];

    // Simulate progress in background
    const jobTimer = window.setInterval(() => {
      const cur = mockDb.backtests.find((x) => x.id === bt.id);
      const det = mockDb.backtestDetails[bt.id];
      const logs = mockDb.backtestLogs[bt.id];
      if (!cur || !det || !logs) return;
      if (cur.status !== 'running') return;

      const inc = 5 + Math.floor(Math.random() * 12);
      cur.progress = Math.min(100, (cur.progress ?? 0) + inc);
      cur.updatedAt = isoNow();
      det.progress = cur.progress;
      det.updatedAt = cur.updatedAt;

      const lastEquity = det.equityCurve.at(-1)?.equity ?? cur.initialCapital;
      const noise = (Math.random() - 0.48) * (cur.initialCapital * 0.002);
      const equity = Math.max(1, lastEquity + noise);
      const pnl = equity - cur.initialCapital;
      const equityArr = det.equityCurve.map((p) => p.equity).concat([equity]);
      const dd = calcMaxDd(equityArr);
      const ts = isoNow();

      det.equityCurve.push({ ts, equity: round2(equity), pnl: round2(pnl), dd: round2(dd) });
      det.drawdownCurve.push({ ts, dd: round2(dd) });

      if (Math.random() < 0.2) {
        det.trades.push({
          id: newId('trd'),
          ts,
          symbol: cur.symbol,
          side: Math.random() > 0.5 ? 'buy' : 'sell',
          qty: round2(0.01 + Math.random() * 0.05),
          price: round2(100 + Math.random() * 1000),
          fee: round2(0.2 + Math.random() * 1.2),
          pnl: round2((Math.random() - 0.5) * 20),
          orderId: newId('ord'),
        });
      }

      logs.push({
        id: newId('log'),
        ts,
        level: 'info',
        source: 'backtest',
        backtestId: cur.id,
        message: `Backtest running... ${cur.progress}%`,
      });

      // Finish
      if ((cur.progress ?? 0) >= 100) {
        cur.status = 'success';
        cur.updatedAt = isoNow();
        det.status = 'success';
        det.updatedAt = cur.updatedAt;

        const pnlTotal = det.equityCurve.at(-1)?.pnl ?? 0;
        det.metrics = {
          cagr: 0.18,
          sharpe: 1.6,
          maxDrawdown: calcMaxDd(det.equityCurve.map((p) => p.equity)),
          calmar: 1.9,
          winRate: 0.56,
          trades: det.trades.length,
          pnlTotal: round2(pnlTotal),
        };

        logs.push({
          id: newId('log'),
          ts: isoNow(),
          level: 'info',
          source: 'backtest',
          backtestId: cur.id,
          message: 'Backtest finished: success',
        });

        const job = backtestJobs.get(cur.id);
        if (job) window.clearInterval(job.timer);
        backtestJobs.delete(cur.id);
      }
    }, 650);

    backtestJobs.set(bt.id, { timer: jobTimer });

    return bt;
  },

  async listBacktests(): Promise<Backtest[]> {
    seedMockDb();
    await sleep(180);
    return [...mockDb.backtests].sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
  },

  async getBacktest(id: string): Promise<BacktestDetail> {
    seedMockDb();
    await sleep(180);
    const det = mockDb.backtestDetails[id];
    return assertFound(det, `Backtest not found: ${id}`);
  },

  async getBacktestLogs(id: string): Promise<LogEntry[]> {
    seedMockDb();
    await sleep(160);
    const logs = mockDb.backtestLogs[id];
    return assertFound(logs, `Backtest logs not found: ${id}`);
  },

  async getPortfolio(): Promise<Portfolio> {
    seedMockDb();
    await sleep(160);
    // Drift equity a little to keep dashboard alive.
    const last = mockDb.portfolio.equity;
    const next = Math.max(1, last + (Math.random() - 0.45) * 120);
    const ts = isoNow();
    mockDb.portfolio.ts = ts;
    mockDb.portfolio.equity = round2(next);
    mockDb.portfolio.equityCurve = [...mockDb.portfolio.equityCurve, { ts, equity: round2(next) }].slice(
      -120,
    );
    return { ...mockDb.portfolio };
  },

  async getPositions() {
    seedMockDb();
    await sleep(180);
    return [...mockDb.positions];
  },

  async getOrders() {
    seedMockDb();
    await sleep(180);
    return [...mockDb.orders];
  },

  async getFills() {
    seedMockDb();
    await sleep(180);
    return [...mockDb.fills];
  },

  async getRisk(): Promise<RiskParams> {
    seedMockDb();
    await sleep(160);
    return { ...mockDb.risk };
  },

  async updateRisk(req: UpdateRiskRequest): Promise<RiskParams> {
    seedMockDb();
    await sleep(220);
    mockDb.risk = { ...mockDb.risk, ...req, updatedAt: isoNow() };
    mockDb.logs.push({
      id: newId('log'),
      ts: mockDb.risk.updatedAt,
      level: 'warn',
      source: 'system',
      message: 'Risk params updated (mock).',
    });
    return { ...mockDb.risk };
  },

  async getLogs(params: LogsQueryParams): Promise<LogEntry[]> {
    seedMockDb();
    await sleep(140);
    const { type, level, q, limit = 50 } = params;
    const filtered = mockDb.logs
      .filter((l) => {
        if (type === 'system' && l.source !== 'system') return false;
        if (type === 'strategy' && l.source !== 'strategy') return false;
        if (level && l.level !== level) return false;
        if (q && !l.message.toLowerCase().includes(q.toLowerCase())) return false;
        return true;
      })
      .sort((a, b) => (a.ts < b.ts ? 1 : -1))
      .slice(0, limit);
    return filtered;
  },

  async getAuditLogs(_params: AuditLogsQueryParams): Promise<AuditLogEntry[]> {
    await sleep(120);
    return [];
  },
};
