import { http } from '@/api/http';
import { env } from '@/utils/env';
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
  RiskEventEntry,
  RiskEventsQueryParams,
  RiskParams,
  StrategyDiagnostics,
  StrategyDiagnosticsSnapshot,
  Strategy,
  UpdateRiskRequest,
  UpdateStrategyRequest,
} from '@/types/api';
import type { TickMessage } from '@/types/ws';
import type { Candle } from '@/store/liveStore';

const MARKET_CONFIG_CACHE_TTL_MS = 60_000;

let marketConfigPathCache: { path: string; tsMs: number } = {
  path: env.marketConfigPath || 'config_market.yaml',
  tsMs: 0,
};

type QuantProcessLog = {
  ts_utc?: string;
  source?: string;
  message?: string;
  strategy_id?: string;
  strategyId?: string;
  backtest_id?: string;
  backtestId?: string;
};

type QuantProcessStatus = {
  running?: boolean;
  pid?: number | null;
  return_code?: number | null;
  started_at?: string | null;
  ended_at?: string | null;
  logs?: QuantProcessLog[];
  metadata?: Record<string, unknown>;
  run_id?: string;
};

type QuantHealthResponse = {
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
};

type QuantAuditLogResponse = {
  id?: number;
  ts?: string;
  owner?: string;
  action?: string;
  entity?: string;
  entityId?: string;
  detail?: Record<string, unknown>;
};

type QuantRiskEventResponse = {
  id?: number;
  ts?: string;
  owner?: string;
  strategyKey?: string;
  eventType?: string;
  rule?: string;
  message?: string;
  detail?: Record<string, unknown>;
};

type QuantPaperEquityResponse = {
  rows?: Array<{ ts_utc?: string; equity?: number; cash?: number }>;
};

type QuantPortfolioResponse = {
  ts?: string;
  equity?: number;
  cash?: number;
  pnlToday?: number;
  pnlWeek?: number;
  maxDrawdown?: number;
  winRate?: number;
  tradesToday?: number;
  tradesWeek?: number;
  running?: boolean;
  stale?: boolean;
  equityCurve?: Array<{ ts?: string; equity?: number }>;
};

type QuantMarketTicksResponse = {
  ticks?: Array<{
    symbol?: string;
    ts?: string;
    ts_utc?: string;
    price?: number;
    bid?: number;
    ask?: number;
    volume?: number;
  }>;
};

type QuantMarketKlinesResponse = {
  rows?: Array<{
    time?: number;
    ts_utc?: string;
    open?: number;
    high?: number;
    low?: number;
    close?: number;
  }>;
};

type QuantStrategyDiagnosticsResponse = {
  strategy_id?: string;
  path?: string;
  size_bytes?: number;
  updated_at?: string;
  snapshot?: StrategyDiagnosticsSnapshot;
};

function nowIso() {
  return new Date().toISOString();
}

function toIso(value?: string | null) {
  if (!value) return nowIso();
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? nowIso() : d.toISOString();
}

function toNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
  }
  return undefined;
}

function computeMaxDrawdown(equity: number[]) {
  let peak = Number.NEGATIVE_INFINITY;
  let maxDd = 0;
  equity.forEach((x) => {
    if (x > peak) peak = x;
    if (peak <= 0) return;
    const dd = (peak - x) / peak;
    if (dd > maxDd) maxDd = dd;
  });
  return Number.isFinite(maxDd) ? maxDd : 0;
}

async function getStrategyStatus(logLimit = 120) {
  const { data } = await http.get<QuantProcessStatus>('/strategy/status', { params: { log_limit: logLimit } });
  return data;
}

async function resolveMarketConfigPath(forceRefresh = false): Promise<string> {
  const configuredPath = (env.marketConfigPath || '').trim();
  if (configuredPath) {
    marketConfigPathCache = { path: configuredPath, tsMs: Date.now() };
    return configuredPath;
  }

  const now = Date.now();
  if (!forceRefresh && now - marketConfigPathCache.tsMs < MARKET_CONFIG_CACHE_TTL_MS) {
    return marketConfigPathCache.path;
  }

  try {
    const status = await getStrategyStatus(1);
    const configPath =
      typeof status.metadata?.config_path === 'string' ? status.metadata.config_path : marketConfigPathCache.path;
    marketConfigPathCache = { path: configPath, tsMs: now };
    return configPath;
  } catch {
    if (marketConfigPathCache.tsMs === 0) {
      marketConfigPathCache.tsMs = now;
    }
    return marketConfigPathCache.path;
  }
}

function defaultPortfolio(): Portfolio {
  return {
    ts: nowIso(),
    equity: 0,
    cash: 0,
    pnlToday: 0,
    pnlWeek: 0,
    maxDrawdown: 0,
    winRate: 0,
    tradesToday: 0,
    tradesWeek: 0,
    equityCurve: [],
  };
}

export const quantApi = {
  async health(): Promise<HealthResponse> {
    const { data } = await http.get<QuantHealthResponse>('/health');
    return {
      ok: data.ok ?? data.status === 'ok',
      ts: toIso(data.ts ?? data.ts_utc),
      version: data.version,
      message: data.message,
      db: data.db,
      db_error: data.db_error,
      db_runtime_failures: toNumber(data.db_runtime_failures) ?? undefined,
      db_runtime_failure_detail: data.db_runtime_failure_detail,
    };
  },

  async listStrategies(): Promise<Strategy[]> {
    const { data } = await http.get<Strategy[]>('/strategies');
    return data;
  },

  async createStrategy(_req: CreateStrategyRequest): Promise<Strategy> {
    const { data } = await http.post<Strategy>('/strategies', _req);
    return data;
  },

  async getStrategy(id: string): Promise<Strategy> {
    const { data } = await http.get<Strategy>(`/strategies/${id}`);
    return data;
  },

  async updateStrategy(_id: string, _req: UpdateStrategyRequest): Promise<Strategy> {
    const { data } = await http.put<Strategy>(`/strategies/${_id}`, _req);
    return data;
  },

  async startStrategy(id: string): Promise<{ ok: boolean }> {
    await http.post(`/strategies/${id}/start`);
    return { ok: true };
  },

  async stopStrategy(id: string): Promise<{ ok: boolean }> {
    await http.post(`/strategies/${id}/stop`);
    return { ok: true };
  },

  async createBacktest(req: CreateBacktestRequest): Promise<Backtest> {
    const { data } = await http.post<Backtest>('/backtests', req);
    return data;
  },

  async listBacktests(): Promise<Backtest[]> {
    const { data } = await http.get<Backtest[]>('/backtests');
    return data;
  },

  async getBacktest(id: string): Promise<BacktestDetail> {
    const { data } = await http.get<BacktestDetail>(`/backtests/${id}`);
    return data;
  },

  async getBacktestLogs(id: string): Promise<LogEntry[]> {
    const { data } = await http.get<LogEntry[]>(`/backtests/${id}/logs`);
    return data;
  },

  async getPortfolio(strategyId?: string): Promise<Portfolio> {
    const params: Record<string, unknown> = {};
    if (strategyId) params.strategy_id = strategyId;

    try {
      const { data } = await http.get<QuantPortfolioResponse>('/portfolio', {
        params: Object.keys(params).length > 0 ? params : undefined,
      });
      const curve = (data.equityCurve ?? [])
        .map((r) => {
          const equity = toNumber(r.equity);
          if (equity === undefined) return null;
          return { ts: toIso(r.ts), equity };
        })
        .filter((v): v is NonNullable<typeof v> => Boolean(v))
        .sort((a, b) => (a.ts > b.ts ? 1 : -1));

      const ts = toIso(data.ts);
      const equity = toNumber(data.equity) ?? curve.at(-1)?.equity ?? 0;
      const normalizedCurve = curve.length > 0 ? curve : [{ ts, equity }];
      return {
        ts,
        equity,
        cash: toNumber(data.cash) ?? 0,
        pnlToday: toNumber(data.pnlToday) ?? 0,
        pnlWeek: toNumber(data.pnlWeek) ?? 0,
        maxDrawdown: toNumber(data.maxDrawdown) ?? computeMaxDrawdown(normalizedCurve.map((p) => p.equity)),
        winRate: toNumber(data.winRate) ?? 0,
        tradesToday: Math.max(0, Math.round(toNumber(data.tradesToday) ?? 0)),
        tradesWeek: Math.max(0, Math.round(toNumber(data.tradesWeek) ?? 0)),
        running: Boolean(data.running),
        stale: Boolean(data.stale),
        equityCurve: normalizedCurve,
      };
    } catch {
      // Backward compatibility for older servers that only expose /paper/equity.
    }

    const paperParams: Record<string, unknown> = { limit: 600 };
    if (strategyId) paperParams.strategy_id = strategyId;
    const { data } = await http.get<QuantPaperEquityResponse>('/paper/equity', { params: paperParams });
    const rows = data.rows ?? [];
    if (rows.length === 0) return defaultPortfolio();

    const curve = rows
      .map((r) => {
        const equity = toNumber(r.equity);
        if (equity === undefined) return null;
        return { ts: toIso(r.ts_utc), equity };
      })
      .filter((v): v is NonNullable<typeof v> => Boolean(v))
      .sort((a, b) => (a.ts > b.ts ? 1 : -1));

    if (curve.length === 0) return defaultPortfolio();

    const latest = curve.at(-1)!;
    const todayKey = latest.ts.slice(0, 10);
    const todayRows = curve.filter((r) => r.ts.slice(0, 10) === todayKey);
    const pnlToday = latest.equity - (todayRows[0]?.equity ?? latest.equity);

    const weekAgo = new Date(Date.parse(latest.ts) - 7 * 24 * 3600 * 1000).toISOString();
    const weekBase = curve.find((r) => r.ts >= weekAgo)?.equity ?? curve[0]!.equity;
    const pnlWeek = latest.equity - weekBase;

    return {
      ts: latest.ts,
      equity: latest.equity,
      cash: toNumber(rows.at(-1)?.cash) ?? 0,
      pnlToday,
      pnlWeek,
      maxDrawdown: computeMaxDrawdown(curve.map((p) => p.equity)),
      winRate: 0,
      tradesToday: 0,
      tradesWeek: 0,
      running: true,
      stale: false,
      equityCurve: curve,
    };
  },

  async getPositions(strategyId?: string): Promise<Position[]> {
    const { data } = await http.get<Position[]>('/positions', {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return data;
  },

  async getOrders(strategyId?: string): Promise<Order[]> {
    const { data } = await http.get<Order[]>('/orders', {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return data;
  },

  async getFills(strategyId?: string): Promise<Fill[]> {
    const { data } = await http.get<Fill[]>('/fills', {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return data;
  },

  async getStrategyDiagnostics(strategyId?: string): Promise<StrategyDiagnostics> {
    const { data } = await http.get<QuantStrategyDiagnosticsResponse>('/strategy/diagnostics', {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return {
      strategy_id: data.strategy_id ?? strategyId,
      path: data.path ?? '',
      size_bytes: toNumber(data.size_bytes) ?? 0,
      updated_at: data.updated_at ? toIso(data.updated_at) : nowIso(),
      snapshot:
        data.snapshot && typeof data.snapshot === 'object'
          ? data.snapshot
          : ({} as StrategyDiagnosticsSnapshot),
    };
  },

  async getMarketTicks(): Promise<TickMessage[]> {
    const fetchTicks = async (configPath: string) => {
      const { data } = await http.get<QuantMarketTicksResponse>('/market/ticks', {
        params: {
          config_path: configPath,
          refresh_ms: env.marketPollMs,
        },
      });
      return data.ticks ?? [];
    };

    // Keep per-second polling lightweight by caching config_path for a short TTL.
    let configPath = await resolveMarketConfigPath(false);
    let rows: NonNullable<QuantMarketTicksResponse['ticks']>;

    try {
      rows = await fetchTicks(configPath);
    } catch {
      // Retry once with fresh config_path in case backend switched strategy config.
      configPath = await resolveMarketConfigPath(true);
      rows = await fetchTicks(configPath);
    }

    return rows
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
    if (!symbol) return [];
    let configPath = await resolveMarketConfigPath(false);

    const fetchKlines = async (path: string) => {
      const { data } = await http.get<QuantMarketKlinesResponse>('/market/klines', {
        params: {
          symbol,
          config_path: path,
          timeframe: '15m',
          lookback_hours: 24,
        },
      });
      return data.rows ?? [];
    };

    let rows: NonNullable<QuantMarketKlinesResponse['rows']>;
    try {
      rows = await fetchKlines(configPath);
    } catch {
      configPath = await resolveMarketConfigPath(true);
      rows = await fetchKlines(configPath);
    }

    return rows
      .map((r) => {
        const time = typeof r.time === 'number' && Number.isFinite(r.time) ? Math.floor(r.time) : Math.floor(Date.parse(String(r.ts_utc ?? '')) / 1000);
        const open = toNumber(r.open);
        const high = toNumber(r.high);
        const low = toNumber(r.low);
        const close = toNumber(r.close);
        if (!Number.isFinite(time) || open === undefined || high === undefined || low === undefined || close === undefined) return null;
        return { time, open, high, low, close } as Candle;
      })
      .filter((v): v is Candle => Boolean(v))
      .sort((a, b) => a.time - b.time)
      .slice(-96);
  },

  async getMarketIntelSummary(symbol?: string, streamWindowSeconds = 300, lookbackBars = 96): Promise<MarketIntelSummary> {
    const configPath = await resolveMarketConfigPath(false);
    const { data } = await http.get<MarketIntelSummary>('/market/intel/summary', {
      params: {
        symbol,
        config_path: configPath,
        interval: '15m',
        lookback_bars: lookbackBars,
        depth_limit: 20,
        stream_window_seconds: streamWindowSeconds,
      },
    });
    return data;
  },

  async getRisk(strategyId?: string): Promise<RiskParams> {
    const { data } = await http.get<RiskParams>('/risk', {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return data;
  },

  async updateRisk(req: UpdateRiskRequest, strategyId?: string): Promise<RiskParams> {
    const { data } = await http.put<RiskParams>('/risk', req, {
      params: strategyId ? { strategy_id: strategyId } : undefined,
    });
    return data;
  },

  async getLogs(params: LogsQueryParams): Promise<LogEntry[]> {
    const { data } = await http.get<LogEntry[]>('/logs', {
      params: {
        type: params.type,
        level: params.level,
        q: params.q,
        strategy_id: params.strategyId,
        limit: params.limit ?? 200,
      },
    });
    return data;
  },

  async getAuditLogs(params: AuditLogsQueryParams): Promise<AuditLogEntry[]> {
    const { data } = await http.get<QuantAuditLogResponse[]>('/audit/logs', {
      params: {
        limit: params.limit ?? 200,
        action: params.action,
        entity: params.entity,
        owner: params.owner,
        start: params.start,
        end: params.end,
        cursor: params.cursor,
      },
    });
    return (data ?? []).map((row) => ({
      id: Number(row.id ?? 0),
      ts: toIso(row.ts),
      owner: row.owner?.trim() ?? '',
      action: row.action?.trim() ?? '',
      entity: row.entity?.trim() ?? '',
      entityId: row.entityId?.trim() ?? '',
      detail: row.detail && typeof row.detail === 'object' ? row.detail : {},
    }));
  },

  async getRiskEvents(params: RiskEventsQueryParams): Promise<RiskEventEntry[]> {
    const { data } = await http.get<QuantRiskEventResponse[]>('/risk/events', {
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
    return (data ?? []).map((row) => ({
      id: Number(row.id ?? 0),
      ts: toIso(row.ts),
      owner: row.owner?.trim() ?? '',
      strategyKey: row.strategyKey?.trim() ?? '',
      eventType: row.eventType?.trim() ?? '',
      rule: row.rule?.trim() ?? '',
      message: row.message?.trim() ?? '',
      detail: row.detail && typeof row.detail === 'object' ? row.detail : {},
    }));
  },
};
