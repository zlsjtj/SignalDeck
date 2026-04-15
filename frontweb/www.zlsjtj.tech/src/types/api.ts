export type HealthResponse = {
  ok?: boolean;
  status?: string;
  ts?: string; // ISO
  ts_utc?: string; // ISO
  version?: string;
  message?: string;
  db?: string;
  db_error?: string;
  db_runtime_failures?: number;
  db_runtime_failure_detail?: Record<string, unknown>;
};

export type ISODateString = string;

export type StrategyStatus = 'running' | 'stopped';
export type StrategyType = 'mean_reversion' | 'trend_following' | 'market_making' | 'custom';

export type StrategyConfig = {
  symbols: string[];
  timeframe: '1m' | '5m' | '15m' | '1h' | '1d';
  params: Record<string, number | string | boolean>;
};

export type Strategy = {
  id: string;
  name: string;
  type: StrategyType;
  status: StrategyStatus;
  config: StrategyConfig;
  createdAt: ISODateString;
  updatedAt: ISODateString;
};

export type CreateStrategyRequest = {
  name: string;
  type: StrategyType;
  config: StrategyConfig;
};

export type UpdateStrategyRequest = CreateStrategyRequest;

export type BacktestStatus = 'running' | 'success' | 'failed';

export type CreateBacktestRequest = {
  strategyId: string;
  symbol: string;
  startAt: ISODateString;
  endAt: ISODateString;
  initialCapital: number;
  feeRate: number;
  slippage: number;
};

export type Backtest = {
  id: string;
  strategyId: string;
  strategyName: string;
  symbol: string;
  startAt: ISODateString;
  endAt: ISODateString;
  initialCapital: number;
  feeRate: number;
  slippage: number;
  status: BacktestStatus;
  progress?: number; // 0-100 (mock may provide)
  createdAt: ISODateString;
  updatedAt: ISODateString;
};

export type EquityPoint = {
  ts: ISODateString;
  equity: number;
  pnl: number;
  dd: number;
};

export type BacktestMetrics = {
  cagr: number;
  sharpe: number;
  maxDrawdown: number;
  calmar: number;
  winRate: number;
  trades: number;
  pnlTotal: number;
};

export type BacktestTrade = {
  id: string;
  ts: ISODateString;
  symbol: string;
  side: 'buy' | 'sell';
  qty: number;
  price: number;
  fee: number;
  pnl: number;
  orderId?: string;
};

export type BacktestDetail = Backtest & {
  metrics: BacktestMetrics;
  equityCurve: EquityPoint[];
  drawdownCurve: Array<{ ts: ISODateString; dd: number }>;
  trades: BacktestTrade[];
};

export type Portfolio = {
  ts: ISODateString;
  equity: number;
  cash: number;
  pnlToday: number;
  pnlWeek: number;
  maxDrawdown: number;
  winRate: number;
  tradesToday: number;
  tradesWeek: number;
  equityCurve: Array<{ ts: ISODateString; equity: number }>;
  running?: boolean;
  stale?: boolean;
};

export type Position = {
  ts: ISODateString;
  symbol: string;
  qty: number;
  avgPrice: number;
  lastPrice: number;
  unrealizedPnl: number;
};

export type OrderSide = 'buy' | 'sell';
export type OrderType = 'market' | 'limit';
export type OrderStatus = 'new' | 'partially_filled' | 'filled' | 'canceled' | 'rejected';

export type Order = {
  id: string;
  ts: ISODateString;
  symbol: string;
  side: OrderSide;
  type: OrderType;
  qty: number;
  price?: number;
  filledQty: number;
  status: OrderStatus;
};

export type Fill = {
  id: string;
  ts: ISODateString;
  symbol: string;
  side: OrderSide;
  qty: number;
  price: number;
  fee: number;
  orderId: string;
};

export type RiskTrigger = {
  rule: string;
  ts: ISODateString;
  message: string;
};

export type RiskParams = {
  enabled: boolean;
  maxDrawdownPct: number;
  maxPositionPct: number;
  maxRiskPerTradePct: number;
  maxLeverage: number;
  dailyLossLimitPct: number;
  updatedAt: ISODateString;
  triggered: RiskTrigger[];
};

export type UpdateRiskRequest = Pick<
  RiskParams,
  'enabled' | 'maxDrawdownPct' | 'maxPositionPct' | 'maxRiskPerTradePct' | 'maxLeverage' | 'dailyLossLimitPct'
>;

export type LogLevel = 'info' | 'warn' | 'error';
export type LogType = 'system' | 'strategy';
export type LogSource = 'system' | 'strategy' | 'backtest' | 'ws' | 'mock';

export type LogEntry = {
  id: string;
  ts: ISODateString;
  level: LogLevel;
  source: LogSource;
  message: string;
  strategyId?: string;
  backtestId?: string;
};

export type LogsQueryParams = {
  type: LogType;
  level?: LogLevel;
  q?: string;
  strategyId?: string;
  limit?: number;
};

export type AuditLogEntry = {
  id: number;
  ts: ISODateString;
  owner: string;
  action: string;
  entity: string;
  entityId: string;
  detail: Record<string, unknown>;
};

export type AuditLogsQueryParams = {
  limit?: number;
  action?: string;
  entity?: string;
  owner?: string;
  start?: string;
  end?: string;
  cursor?: number;
};

export type RiskEventEntry = {
  id: number;
  ts: ISODateString;
  owner: string;
  strategyKey: string;
  eventType: string;
  rule: string;
  message: string;
  detail: Record<string, unknown>;
};

export type RiskEventsQueryParams = {
  limit?: number;
  strategyId?: string;
  eventType?: string;
  owner?: string;
  start?: string;
  end?: string;
  cursor?: number;
};

export type MarketIntelVenue = 'spot' | 'futures';

export type MarketIntelLevel = {
  price: number;
  qty: number;
  notional: number;
};

export type MarketIntelOrderbook = {
  bids: MarketIntelLevel[];
  asks: MarketIntelLevel[];
  bestBid: number;
  bestAsk: number;
  mid: number;
  spread: number;
  spreadPct: number;
  bidNotional: number;
  askNotional: number;
  imbalance: number;
  lastUpdateId?: number | string;
};

export type MarketIntelFlow = {
  source: string;
  buyQty: number;
  sellQty: number;
  buyNotional: number;
  sellNotional: number;
  takerBuyRatio: number;
  takerBuyNotionalRatio: number;
  tradeImbalance: number;
  tradeCount: number;
  latestTs: string;
};

export type MarketIntelDerivatives = {
  fundingRate: number | null;
  fundingTime: string;
  openInterest: number | null;
  openInterestChangePct: number | null;
  periodTakerBuyRatio: number | null;
  errors?: string[];
};

export type MarketIntelOfiSummary = {
  samples: number;
  ofi: number;
  ofiNorm: number;
  latestTs: string;
  availableSeconds?: number;
  series?: Array<{
    ts: string;
    ofi: number;
    ofiNorm: number;
    samples: number;
  }>;
};

export type MarketIntelStreamFlow = {
  samples: number;
  buyNotional: number;
  sellNotional: number;
  takerBuyRatio: number;
  imbalance: number;
  latestTs: string;
  availableSeconds?: number;
  series?: Array<{
    ts: string;
    buyNotional: number;
    sellNotional: number;
    takerBuyRatio: number;
    imbalance: number;
    samples: number;
  }>;
};

export type MarketIntelVenueStream = {
  orderbook?: MarketIntelOrderbook;
  ofi?: MarketIntelOfiSummary;
  flow?: MarketIntelStreamFlow;
};

export type MarketIntelSessionEffect = {
  hourUtc: number;
  count: number;
  avgReturnPct: number;
  avgVolume: number;
};

export type MarketIntelSessionHeatmapCell = {
  weekdayUtc: number;
  hourUtc: number;
  count: number;
  avgReturnPct: number;
  avgVolume: number;
};

export type MarketIntelVenueSnapshot = {
  venue: MarketIntelVenue;
  symbol: string;
  binanceSymbol: string;
  ok: boolean;
  error: string;
  orderbook: MarketIntelOrderbook | null;
  flow: MarketIntelFlow | null;
  volumeRatio: number;
  sessionEffect: MarketIntelSessionEffect[];
  sessionHeatmap?: MarketIntelSessionHeatmapCell[];
  derivatives: MarketIntelDerivatives | null;
  stream?: MarketIntelVenueStream;
};

export type MarketIntelRollingCorrelationPoint = {
  ts: string;
  correlation: number;
  samples: number;
  window: number;
};

export type MarketIntelRollingCorrelation = {
  pair: string;
  left: string;
  right: string;
  points: MarketIntelRollingCorrelationPoint[];
  window: number;
};

export type MarketIntelCorrelationBreak = {
  pair: string;
  left: string;
  right: string;
  current: number;
  recentMean: number;
  priorHigh: number;
  severity: string;
  reason: string;
  message: string;
};

export type MarketIntelCorrelation = {
  venue: MarketIntelVenue;
  symbols: string[];
  matrix: Array<{ symbol: string; values: Record<string, number | null> }>;
  rolling?: MarketIntelRollingCorrelation[];
  breaks?: MarketIntelCorrelationBreak[];
};

export type MarketIntelFeedStatus = {
  status: string;
  message: string;
  rows: unknown[];
  aggregate?: MarketIntelLiquidationAggregate;
};

export type MarketIntelLiquidation = {
  ts: string;
  symbol: string;
  side: string;
  orderType: string;
  status: string;
  qty: number;
  price: number;
  notional: number;
};

export type MarketIntelLiquidationDirection = {
  count: number;
  notional: number;
};

export type MarketIntelLiquidationAggregate = {
  byDirection: Record<'long' | 'short' | 'unknown', MarketIntelLiquidationDirection>;
  maxEvent: MarketIntelLiquidation | null;
  last5m: {
    byDirection: Record<'long' | 'short' | 'unknown', MarketIntelLiquidationDirection>;
    longNotionalRatio: number | null;
    shortNotionalRatio: number | null;
    totalNotional: number;
    count: number;
  };
};

export type MarketIntelStreamStatus = {
  status: string;
  startedAt: string;
  updatedAt: string;
  connections: Record<string, { status: string; streams: number; updatedAt: string; error?: string }>;
  venues: Record<string, Record<string, MarketIntelVenueStream>>;
  liquidations: MarketIntelLiquidation[];
  errors: Array<{ ts: string; venue: string; message: string }>;
  windowSeconds: number;
};

export type MarketIntelSummary = {
  ts: string;
  source: string;
  symbols: string[];
  selectedSymbol: string;
  selectedBinanceSymbol: string;
  interval: string;
  lookbackBars: number;
  venues: Record<MarketIntelVenue, MarketIntelVenueSnapshot>;
  correlation: MarketIntelCorrelation;
  stream?: MarketIntelStreamStatus;
  liquidations: MarketIntelFeedStatus;
  news: MarketIntelFeedStatus;
  cache?: {
    hit: boolean;
    ttlSeconds: number;
  };
};

export type StrategyDiagnosticsProbe = {
  ok?: boolean;
  ts?: ISODateString;
  detail?: string;
};

export type StrategyDiagnosticsCondition = {
  name?: string;
  current?: number | string | boolean | null;
  threshold?: number | string | boolean | null;
  pass?: boolean;
};

export type StrategyDiagnosticsException = {
  ts?: ISODateString;
  where?: string;
  type?: string;
  message?: string;
  stack?: string;
};

export type StrategyDiagnosticsSnapshot = {
  generated_at?: ISODateString;
  schema_version?: number;
  process?: {
    pid?: number;
    started_at?: ISODateString;
    uptime_seconds?: number;
    version?: string;
    commit_id?: string;
    config_path?: string;
  };
  config?: {
    path?: string;
    summary?: Record<string, unknown>;
  };
  market_data?: {
    last_tick_time?: ISODateString;
    last_bar_time?: ISODateString;
    data_lag_seconds?: number;
    data_source_status?: string;
    data_source_detail?: string;
  };
  exchange_connection?: {
    fetch_balance?: StrategyDiagnosticsProbe;
    fetch_positions?: StrategyDiagnosticsProbe;
    fetch_open_orders?: StrategyDiagnosticsProbe;
    last_api_error?: {
      api?: string;
      message?: string;
      ts?: ISODateString;
    };
  };
  strategy_state?: {
    state?: 'RUNNING' | 'PAUSED' | 'SAFE_MODE' | 'ERROR' | string;
    last_switch_reason?: string;
    last_switch_time?: ISODateString;
  };
  positions_and_orders?: {
    positions?: Array<Record<string, unknown>>;
    open_orders?: Array<Record<string, unknown>>;
  };
  signal_evaluation?: {
    at?: ISODateString;
    conditions?: StrategyDiagnosticsCondition[];
    entry_signal?: boolean;
    filter_reasons?: string[];
    details?: Record<string, unknown>;
  };
  stop_take_trailing?: {
    sl?: number | string | null;
    tp?: number | string | null;
    ts?: number | string | null;
    price_source?: string;
    last_updated?: ISODateString;
    note?: string;
  };
  last_order_attempt?: Record<string, unknown>;
  logging?: {
    targets?: string[];
    level?: string;
    recent_write_time?: ISODateString;
    disk_free_bytes?: number;
    log_dir?: string;
    writable?: boolean;
  };
  exceptions?: {
    window_days?: number;
    total_count?: number;
    counts_by_day?: Record<string, number>;
    last_20?: StrategyDiagnosticsException[];
  };
  recent_order_attempts?: Array<Record<string, unknown>>;
};

export type StrategyDiagnostics = {
  strategy_id?: string;
  path?: string;
  size_bytes?: number;
  updated_at?: ISODateString;
  snapshot?: StrategyDiagnosticsSnapshot;
};
