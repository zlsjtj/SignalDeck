import { env } from '@/utils/env';
import { http } from '@/api/http';
import { endpoints } from '@/api/endpoints';
import { byLang } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import type {
  AuditLogEntry,
  AuditLogsQueryParams,
  Backtest,
  BacktestDetail,
  CreateBacktestRequest,
  CreateStrategyRequest,
  Fill,
  HealthResponse,
  LogEntry,
  LogsQueryParams,
  MarketIntelSummary,
  Order,
  Portfolio,
  Position,
  RiskParams,
  RiskEventEntry,
  RiskEventsQueryParams,
  StrategyDiagnostics,
  UpdateRiskRequest,
  Strategy,
  UpdateStrategyRequest,
} from '@/types/api';
import { mockApi } from '@/mocks/rest';
import { quantApi } from '@/api/quantApi';
import type { TickMessage } from '@/types/ws';
import type { Candle } from '@/store/liveStore';

const useQuantApi = !env.useMock && env.apiProfile === 'quant-api-server';

const nowIso = () => new Date().toISOString();

const isGuestMode = () => {
  const state = useAppStore.getState();
  return Boolean(state.isGuest) && !Boolean(state.isAuthenticated);
};

const assertWritableForGuest = () => {
  if (isGuestMode()) {
    throw new Error(byLang('游客模式为只读，当前操作不可用', 'Guest mode is read-only, this action is unavailable'));
  }
};

const guestPortfolio = (): Portfolio => {
  const ts = nowIso();
  return {
    ts,
    equity: 0,
    cash: 0,
    pnlToday: 0,
    pnlWeek: 0,
    maxDrawdown: 0,
    winRate: 0,
    tradesToday: 0,
    tradesWeek: 0,
    running: false,
    stale: true,
    equityCurve: [{ ts, equity: 0 }],
  };
};

const guestRisk = (): RiskParams => ({
  enabled: false,
  maxDrawdownPct: 0,
  maxPositionPct: 0,
  maxRiskPerTradePct: 0,
  maxLeverage: 0,
  dailyLossLimitPct: 0,
  updatedAt: nowIso(),
  triggered: [],
});

const guestDiagnostics = (): StrategyDiagnostics => ({
  strategy_id: '',
  path: '',
  size_bytes: 0,
  updated_at: nowIso(),
  snapshot: {
    generated_at: nowIso(),
    strategy_state: {
      state: 'PAUSED',
      last_switch_reason: byLang('游客模式', 'Guest mode'),
      last_switch_time: nowIso(),
    },
    exceptions: {
      window_days: 10,
      total_count: 0,
      counts_by_day: {},
      last_20: [],
    },
  },
});

export const api = {
  async health(): Promise<HealthResponse> {
    if (env.useMock) return mockApi.health();
    if (useQuantApi) return quantApi.health();
    const { data } = await http.get<{
      ok?: boolean;
      status?: string;
      ts?: string;
      ts_utc?: string;
      version?: string;
      message?: string;
      db?: string;
      db_error?: string;
      db_runtime_failures?: number;
      db_runtime_failure_detail?: Record<string, unknown>;
    }>(
      endpoints.health,
    );
    return {
      ok: data.ok ?? data.status === 'ok',
      status: data.status,
      ts: data.ts ?? data.ts_utc,
      ts_utc: data.ts_utc,
      version: data.version,
      message: data.message,
      db: data.db,
      db_error: data.db_error,
      db_runtime_failures: data.db_runtime_failures,
      db_runtime_failure_detail: data.db_runtime_failure_detail,
    };
  },

  async listStrategies(): Promise<Strategy[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.listStrategies();
    if (useQuantApi) return quantApi.listStrategies();
    const { data } = await http.get<Strategy[]>(endpoints.strategies);
    return data;
  },

  async createStrategy(req: CreateStrategyRequest): Promise<Strategy> {
    assertWritableForGuest();
    if (env.useMock) return mockApi.createStrategy(req);
    if (useQuantApi) return quantApi.createStrategy(req);
    const { data } = await http.post<Strategy>(endpoints.strategies, req);
    return data;
  },

  async getStrategy(id: string): Promise<Strategy> {
    if (isGuestMode()) {
      const ts = nowIso();
      return {
        id,
        name: byLang('游客模式策略', 'Guest Strategy'),
        type: 'custom',
        status: 'stopped',
        config: { symbols: [], timeframe: '1h', params: {} },
        createdAt: ts,
        updatedAt: ts,
      };
    }
    if (env.useMock) return mockApi.getStrategy(id);
    if (useQuantApi) return quantApi.getStrategy(id);
    const { data } = await http.get<Strategy>(endpoints.strategy(id));
    return data;
  },

  async updateStrategy(id: string, req: UpdateStrategyRequest): Promise<Strategy> {
    assertWritableForGuest();
    if (env.useMock) return mockApi.updateStrategy(id, req);
    if (useQuantApi) return quantApi.updateStrategy(id, req);
    const { data } = await http.put<Strategy>(endpoints.strategy(id), req);
    return data;
  },

  async startStrategy(id: string): Promise<{ ok: boolean }> {
    assertWritableForGuest();
    if (env.useMock) return mockApi.startStrategy(id);
    if (useQuantApi) return quantApi.startStrategy(id);
    const { data } = await http.post<{ ok: boolean }>(endpoints.strategyStart(id));
    return data;
  },

  async stopStrategy(id: string): Promise<{ ok: boolean }> {
    assertWritableForGuest();
    if (env.useMock) return mockApi.stopStrategy(id);
    if (useQuantApi) return quantApi.stopStrategy(id);
    const { data } = await http.post<{ ok: boolean }>(endpoints.strategyStop(id));
    return data;
  },

  async createBacktest(req: CreateBacktestRequest): Promise<Backtest> {
    assertWritableForGuest();
    if (env.useMock) return mockApi.createBacktest(req);
    if (useQuantApi) return quantApi.createBacktest(req);
    const { data } = await http.post<Backtest>(endpoints.backtests, req);
    return data;
  },

  async listBacktests(): Promise<Backtest[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.listBacktests();
    if (useQuantApi) return quantApi.listBacktests();
    const { data } = await http.get<Backtest[]>(endpoints.backtests);
    return data;
  },

  async getBacktest(id: string): Promise<BacktestDetail> {
    if (isGuestMode()) {
      const ts = nowIso();
      return {
        id,
        strategyId: '',
        strategyName: byLang('游客模式', 'Guest Mode'),
        symbol: '',
        startAt: ts,
        endAt: ts,
        initialCapital: 0,
        feeRate: 0,
        slippage: 0,
        status: 'success',
        progress: 0,
        createdAt: ts,
        updatedAt: ts,
        metrics: {
          cagr: 0,
          sharpe: 0,
          maxDrawdown: 0,
          calmar: 0,
          winRate: 0,
          trades: 0,
          pnlTotal: 0,
        },
        equityCurve: [],
        drawdownCurve: [],
        trades: [],
      };
    }
    if (env.useMock) return mockApi.getBacktest(id);
    if (useQuantApi) return quantApi.getBacktest(id);
    const { data } = await http.get<BacktestDetail>(endpoints.backtest(id));
    return data;
  },

  async getBacktestLogs(id: string): Promise<LogEntry[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.getBacktestLogs(id);
    if (useQuantApi) return quantApi.getBacktestLogs(id);
    const { data } = await http.get<LogEntry[]>(endpoints.backtestLogs(id));
    return data;
  },

  async getPortfolio(strategyId?: string): Promise<Portfolio> {
    if (isGuestMode()) return guestPortfolio();
    if (env.useMock) return mockApi.getPortfolio();
    if (useQuantApi) return quantApi.getPortfolio(strategyId);
    const { data } = await http.get<Portfolio>(endpoints.portfolio, { params: strategyId ? { strategy_id: strategyId } : undefined });
    return data;
  },

  async getPositions(strategyId?: string): Promise<Position[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.getPositions();
    if (useQuantApi) return quantApi.getPositions(strategyId);
    const { data } = await http.get<Position[]>(endpoints.positions, { params: strategyId ? { strategy_id: strategyId } : undefined });
    return data;
  },

  async getOrders(strategyId?: string): Promise<Order[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.getOrders();
    if (useQuantApi) return quantApi.getOrders(strategyId);
    const { data } = await http.get<Order[]>(endpoints.orders, { params: strategyId ? { strategy_id: strategyId } : undefined });
    return data;
  },

  async getFills(strategyId?: string): Promise<Fill[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.getFills();
    if (useQuantApi) return quantApi.getFills(strategyId);
    const { data } = await http.get<Fill[]>(endpoints.fills, { params: strategyId ? { strategy_id: strategyId } : undefined });
    return data;
  },

  async getStrategyDiagnostics(strategyId?: string): Promise<StrategyDiagnostics> {
    if (isGuestMode()) return guestDiagnostics();
    if (env.useMock) return guestDiagnostics();
    if (useQuantApi) return quantApi.getStrategyDiagnostics(strategyId);
    const { data } = await http.get<StrategyDiagnostics>('/strategy/diagnostics', {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return data;
  },

  async getRisk(strategyId?: string): Promise<RiskParams> {
    if (isGuestMode()) return guestRisk();
    if (env.useMock) return mockApi.getRisk();
    if (useQuantApi) return quantApi.getRisk(strategyId);
    const { data } = await http.get<RiskParams>(endpoints.risk, { params: strategyId ? { strategy_id: strategyId } : undefined });
    return data;
  },

  async updateRisk(req: UpdateRiskRequest, strategyId?: string): Promise<RiskParams> {
    assertWritableForGuest();
    if (env.useMock) return mockApi.updateRisk(req);
    if (useQuantApi) return quantApi.updateRisk(req, strategyId);
    const { data } = await http.put<RiskParams>(endpoints.risk, req, { params: strategyId ? { strategy_id: strategyId } : undefined });
    return data;
  },

  async getLogs(params: LogsQueryParams): Promise<LogEntry[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.getLogs(params);
    if (useQuantApi) return quantApi.getLogs(params);
    const { data } = await http.get<LogEntry[]>(endpoints.logs, { params });
    return data;
  },

  async getAuditLogs(params: AuditLogsQueryParams): Promise<AuditLogEntry[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return mockApi.getAuditLogs(params);
    if (useQuantApi) return quantApi.getAuditLogs(params);
    const { data } = await http.get<AuditLogEntry[]>(endpoints.auditLogs, { params });
    return data;
  },

  async getRiskEvents(params: RiskEventsQueryParams): Promise<RiskEventEntry[]> {
    if (isGuestMode()) return [];
    if (env.useMock) return [];
    if (useQuantApi) return quantApi.getRiskEvents(params);
    const { data } = await http.get<RiskEventEntry[]>(endpoints.riskEvents, {
      params: {
        limit: params.limit ?? 200,
        strategy_id: params.strategyId,
        event_type: params.eventType,
        owner: params.owner,
        start: params.start,
        end: params.end,
        cursor: params.cursor,
      },
    });
    return data;
  },

  async getMarketTicks(): Promise<TickMessage[]> {
    if (env.useMock) return [];
    if (useQuantApi) return quantApi.getMarketTicks();
    const toNumber = (value: unknown): number | undefined => {
      if (typeof value === 'number' && Number.isFinite(value)) return value;
      if (typeof value === 'string') {
        const n = Number(value);
        return Number.isFinite(n) ? n : undefined;
      }
      return undefined;
    };
    const toIso = (value?: string) => {
      if (!value) return nowIso();
      const d = new Date(value);
      return Number.isNaN(d.getTime()) ? nowIso() : d.toISOString();
    };

    const { data } = await http.get<{
      ticks?: Array<{
        symbol?: string;
        ts?: string;
        ts_utc?: string;
        price?: number | string;
        bid?: number | string;
        ask?: number | string;
        volume?: number | string;
      }>;
    }>('/market/ticks', {
      params: {
        config_path: env.marketConfigPath,
        refresh_ms: env.marketPollMs,
      },
    });

    return (data.ticks ?? [])
      .map((row) => {
        const symbol = row.symbol?.trim();
        const price = toNumber(row.price);
        if (!symbol || price === undefined || price <= 0) return null;
        const bid = toNumber(row.bid) ?? price;
        const ask = toNumber(row.ask) ?? price;
        const volume = toNumber(row.volume) ?? 0;
        return {
          type: 'tick',
          symbol,
          ts: toIso(row.ts ?? row.ts_utc),
          price,
          bid,
          ask,
          volume,
        } as TickMessage;
      })
      .filter((v): v is TickMessage => Boolean(v));
  },

  async getMarketKlines(symbol: string): Promise<Candle[]> {
    if (env.useMock) return [];
    if (useQuantApi) return quantApi.getMarketKlines(symbol);
    if (!symbol) return [];
    const { data } = await http.get<{
      rows?: Array<{
        time?: number;
        ts_utc?: string;
        open?: number;
        high?: number;
        low?: number;
        close?: number;
      }>;
    }>('/market/klines', {
      params: {
        symbol,
        config_path: env.marketConfigPath,
        timeframe: '15m',
        lookback_hours: 24,
      },
    });
    return (data.rows ?? [])
      .map((r) => {
        const time =
          typeof r.time === 'number' && Number.isFinite(r.time)
            ? Math.floor(r.time)
            : Math.floor(Date.parse(String(r.ts_utc ?? '')) / 1000);
        if (!Number.isFinite(time)) return null;
        const open = typeof r.open === 'number' && Number.isFinite(r.open) ? r.open : undefined;
        const high = typeof r.high === 'number' && Number.isFinite(r.high) ? r.high : undefined;
        const low = typeof r.low === 'number' && Number.isFinite(r.low) ? r.low : undefined;
        const close = typeof r.close === 'number' && Number.isFinite(r.close) ? r.close : undefined;
        if (open === undefined || high === undefined || low === undefined || close === undefined) return null;
        return { time, open, high, low, close } as Candle;
      })
      .filter((v): v is Candle => Boolean(v))
      .sort((a, b) => a.time - b.time)
      .slice(-96);
  },

  async getMarketIntelSummary(symbol?: string, streamWindowSeconds = 300, lookbackBars = 96): Promise<MarketIntelSummary> {
    if (useQuantApi) return quantApi.getMarketIntelSummary(symbol, streamWindowSeconds, lookbackBars);
    const { data } = await http.get<MarketIntelSummary>('/market/intel/summary', {
      params: {
        symbol,
        config_path: env.marketConfigPath,
        interval: '15m',
        lookback_bars: lookbackBars,
        depth_limit: 20,
        stream_window_seconds: streamWindowSeconds,
      },
    });
    return data;
  },
};
