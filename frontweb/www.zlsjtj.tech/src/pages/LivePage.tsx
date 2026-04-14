import { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  Descriptions,
  Empty,
  Grid,
  Popconfirm,
  Row,
  Select,
  Space,
  Tag,
  Table,
  Tabs,
  Tooltip,
  Typography,
  message,
} from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { DeleteOutlined, StopOutlined } from '@ant-design/icons';

import { RealtimeEquityChart } from '@/components/charts/RealtimeEquityChart';
import { JsonBlock } from '@/components/common/JsonBlock';
import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useFillsQuery, useOrdersQuery, usePositionsQuery, useStrategyDiagnosticsQuery } from '@/hooks/queries/live';
import { useMarketTicksQuery } from '@/hooks/queries/market';
import { usePortfolioQuery } from '@/hooks/queries/portfolio';
import { useStrategiesQuery } from '@/hooks/queries/strategies';
import { byLang, useI18n } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import { useLiveStore } from '@/store/liveStore';
import type { Fill, Order, Position, StrategyDiagnosticsCondition } from '@/types/api';
import type { TickMessage } from '@/types/ws';
import { formatNumber, formatPriceBySymbol, formatTs } from '@/utils/format';
import { copyText } from '@/utils/copy';

function renderDiagValue(
  value: StrategyDiagnosticsCondition['current'],
  boolTrue: string,
  boolFalse: string,
) {
  if (value === null || value === undefined) return '-';
  if (typeof value === 'number') return formatNumber(value, 6);
  if (typeof value === 'boolean') return value ? boolTrue : boolFalse;
  return String(value);
}

const LIVE_DIAG_REFRESH_OPTIONS = [15_000, 60_000, 120_000, 300_000] as const;

function normalizeDiagRefreshMs(value: unknown): number {
  if (typeof value !== 'number' || !Number.isFinite(value)) return 60_000;
  const normalized = Math.round(value);
  return LIVE_DIAG_REFRESH_OPTIONS.includes(normalized as (typeof LIVE_DIAG_REFRESH_OPTIONS)[number])
    ? normalized
    : 60_000;
}

function parseIsoMs(value: unknown): number | null {
  if (typeof value !== 'string' || !value.trim()) return null;
  const ts = Date.parse(value);
  return Number.isNaN(ts) ? null : ts;
}

function toFiniteNumber(value: unknown): number | undefined {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const n = Number(value);
    return Number.isFinite(n) ? n : undefined;
  }
  return undefined;
}

type DiagPositionRow = {
  key: string;
  symbol: string;
  side: string;
  qty: number;
  avgPrice: number | undefined;
  markPrice: number | undefined;
  notional: number | undefined;
  unrealizedPnl: number | undefined;
};

type DiagOpenOrderRow = {
  key: string;
  ts?: string;
  symbol: string;
  side: string;
  type: string;
  qty?: number;
  price?: number;
  status: string;
};

type DiagOrderAttemptRow = {
  key: string;
  ts?: string;
  symbol: string;
  side: string;
  status: string;
  qty?: number;
  price?: number;
  reason: string;
};

type DiagnosticsTabKey = 'quick' | 'signal' | 'pos-orders' | 'exceptions' | 'raw';

const DIAG_KEY_ZH_MAP: Record<string, string> = {
  generated_at: '生成时间',
  schema_version: '结构版本',
  process: '进程',
  pid: '进程号',
  started_at: '启动时间',
  uptime_seconds: '运行时长秒',
  version: '版本',
  commit_id: '提交号',
  config_path: '配置路径',
  config: '配置',
  path: '路径',
  summary: '摘要',
  exchange: '交易所',
  paper: '模拟盘',
  symbol_count: '标的数量',
  symbols: '标的列表',
  timeframe: '周期',
  lookback_hours: '回看小时',
  rebalance_every_minutes: '再平衡间隔分钟',
  strategy: '策略',
  long_quantile: '做多分位',
  short_quantile: '做空分位',
  score_threshold: '评分阈值',
  weight_mode: '权重模式',
  portfolio: '组合',
  gross_leverage: '总杠杆',
  max_weight_per_symbol: '单标的最大权重',
  min_order_usdt: '最小下单额USDT',
  drift_threshold: '偏离阈值',
  risk: '风险',
  max_daily_loss: '日亏损上限',
  max_strategy_dd: '策略回撤上限',
  stop_out_dd: '止损回撤阈值',
  cool_off_hours: '冷静期小时',
  execution: '执行',
  order_type: '订单类型',
  limit_price_offset_bps: '限价偏移基点',
  diagnostics: '诊断',
  heartbeat_minutes: '心跳间隔分钟',
  snapshot_path: '快照路径',
  exceptions_path: '异常文件路径',
  market_data: '行情数据',
  last_tick_time: '最近逐笔时间',
  last_bar_time: '最近K线时间',
  data_lag_seconds: '数据延迟秒',
  data_source_status: '数据源状态',
  data_source_detail: '数据源详情',
  exchange_connection: '交易所连接',
  fetch_balance: '拉取余额',
  fetch_positions: '拉取持仓',
  fetch_open_orders: '拉取挂单',
  ok: '成功',
  detail: '详情',
  last_api_error: '最近接口错误',
  api: '接口',
  message: '消息',
  strategy_state: '策略状态',
  state: '状态',
  last_switch_reason: '最近切换原因',
  last_switch_time: '最近切换时间',
  positions_and_orders: '持仓与挂单',
  positions: '持仓',
  open_orders: '挂单',
  signal_evaluation: '信号评估',
  at: '评估时间',
  conditions: '条件',
  name: '名称',
  current: '当前值',
  threshold: '阈值',
  pass: '通过',
  entry_signal: '开仓信号',
  filter_reasons: '过滤原因',
  details: '详情',
  stop_take_trailing: '止盈止损与追踪',
  sl: '止损',
  tp: '止盈',
  price_source: '价格来源',
  last_updated: '最近更新时间',
  note: '备注',
  last_order_attempt: '最近下单尝试',
  status: '状态',
  symbol: '标的',
  side: '方向',
  qty: '数量',
  price: '价格',
  failure_reason: '失败原因',
  error: '错误',
  exchange_response: '交易所返回',
  params: '参数',
  logging: '日志',
  targets: '输出目标',
  level: '级别',
  recent_write_time: '最近写入时间',
  disk_free_bytes: '磁盘剩余字节',
  log_dir: '日志目录',
  writable: '可写',
  exceptions: '异常',
  window_days: '窗口天数',
  total_count: '总数',
  counts_by_day: '按日统计',
  last_20: '最近20条',
  where: '位置',
  type: '类型',
  stack: '堆栈',
  recent_order_attempts: '最近下单记录',
  delta_w: '权重变化',
  notional: '名义金额',
  reduce_only: '只减仓',
  position_side: '持仓方向',
  amount: '下单量',
  mode: '模式',
  mark_price: '标记价格',
  unrealized_pnl: '未实现盈亏',
  avg_price: '均价',
  force_rebalance: '强制再平衡',
  top_scores: '高分标的',
  score: '评分',
  target_weight: '目标权重',
  current_weight: '当前权重',
  equity: '权益',
  drawdown: '回撤',
  daily_loss: '日亏损',
  risk_off_active: '风控停机激活',
};

const DIAG_VALUE_ZH_MAP: Record<string, string> = {
  ok: '正常',
  connected: '已连接',
  disconnected: '已断开',
  running: '运行中',
  paused: '已暂停',
  safe_mode: '保护模式',
  error: '异常',
  limit: '限价',
  market: '市价',
  buy: '买入',
  sell: '卖出',
  long: '多头',
  short: '空头',
  paper: '模拟盘',
  filled_paper: '模拟成交',
  fetch_failed: '拉取失败',
  fetch_universe_succeeded: '拉取标的成功',
  normal_loop: '正常循环',
  mark_price: '标记价格',
  unknown: '未知',
  true: '是',
  false: '否',
  none: '无',
  null: '无',
  fetch_balance: '拉取余额',
  fetch_positions: '拉取持仓',
  fetch_open_orders: '拉取挂单',
};

const DIAG_CONDITION_ZH_MAP: Record<string, string> = {
  'daily_loss<=max_daily_loss': '日亏损 <= 日亏损上限',
  'max_score>=score_threshold': '最高评分 >= 评分阈值',
  'risk_off==False': '风控停机 == 否',
  'target_non_zero>0': '非零目标仓位数 > 0',
  'intents_count>0': '下单意图数 > 0',
};

function localizeDiagApiName(api: string | undefined, language: 'zh' | 'en') {
  if (!api) return '-';
  if (language !== 'zh') return api;
  return DIAG_VALUE_ZH_MAP[api] ?? api;
}

function localizeDiagString(value: string, language: 'zh' | 'en') {
  if (language !== 'zh') return value;
  const raw = String(value ?? '').trim();
  if (!raw) return raw;
  const condition = DIAG_CONDITION_ZH_MAP[raw];
  if (condition) return condition;
  const direct = DIAG_VALUE_ZH_MAP[raw];
  if (direct) return direct;
  const lower = raw.toLowerCase();
  const lowerMapped = DIAG_VALUE_ZH_MAP[lower];
  if (lowerMapped) return lowerMapped;
  const normalized = lower.replace(/\s+/g, '_');
  const normalizedMapped = DIAG_VALUE_ZH_MAP[normalized];
  if (normalizedMapped) return normalizedMapped;
  if (raw.startsWith('abs_mom_filtered_symbols=')) {
    const symbols = raw.split('=', 2)[1] ?? '';
    return symbols ? `这些标的被动量过滤：${symbols}` : '部分标的被动量过滤';
  }
  if (raw.includes('no_order_intents_after_filters')) {
    return '过滤后没有可执行下单信号';
  }
  if (raw.includes('risk_off')) {
    return '风控保护触发，当前不允许开仓';
  }
  if (raw.includes('requires "apiKey" credential')) return '缺少接口密钥凭证';
  if (raw.includes('fetchOpenOrders() WARNING')) return '未指定标的拉取挂单，触发交易所频率限制警告';
  if (raw === 'probe skipped') return '已跳过探测';
  if (raw.includes('paper mode without apiKey+secret')) {
    return '已跳过探测：当前为模拟盘且未配置完整实盘密钥';
  }
  if (raw.includes('drawdown/daily-loss controls are active')) {
    return '已启用回撤/日亏损保护；固定止盈止损是否生效取决于策略配置';
  }
  return raw;
}

function localizeDiagKey(key: string, path: string[], language: 'zh' | 'en') {
  if (language !== 'zh') return key;
  if (key === 'ts') {
    return path[path.length - 1] === 'stop_take_trailing' ? '追踪止损' : '时间';
  }
  return DIAG_KEY_ZH_MAP[key] ?? key;
}

function localizeDiagJson(value: unknown, language: 'zh' | 'en', path: string[] = []): unknown {
  if (language !== 'zh') return value;
  if (value === null || value === undefined) return value;
  if (typeof value === 'boolean') return value ? '是' : '否';
  if (typeof value === 'string') return localizeDiagString(value, language);
  if (Array.isArray(value)) {
    return value.map((item) => localizeDiagJson(item, language, path));
  }
  if (typeof value === 'object') {
    const obj = value as Record<string, unknown>;
    return Object.fromEntries(
      Object.entries(obj).map(([key, val]) => [
        localizeDiagKey(key, path, language),
        localizeDiagJson(val, language, [...path, key]),
      ]),
    );
  }
  return value;
}

export function LivePage() {
  const { language, t } = useI18n();
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const [diagTabKey, setDiagTabKey] = useState<DiagnosticsTabKey>('quick');
  const { data: strategies } = useStrategiesQuery();
  const selectedStrategyId = useAppStore((s) => s.selectedLiveStrategyId);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const liveFilters = useAppStore((s) => s.liveFilters);
  const setLiveFilters = useAppStore((s) => s.setLiveFilters);
  const savedLiveFilters =
    liveFilters && typeof liveFilters === 'object' && !Array.isArray(liveFilters)
      ? (liveFilters as Record<string, unknown>)
      : {};
  const diagRefreshMs = normalizeDiagRefreshMs(savedLiveFilters.diagRefreshMs);
  const defaultStrategyId = useMemo(() => {
    const list = strategies ?? [];
    const running = list.find((s) => !!s.id && s.status === 'running')?.id;
    if (running) return running;
    return list.find((s) => !!s.id)?.id;
  }, [strategies]);

  useEffect(() => {
    if (!defaultStrategyId) return;
    const selectedExists = !!selectedStrategyId && (strategies ?? []).some((s) => s.id === selectedStrategyId);
    if (selectedExists) return;
    setSelectedLiveStrategyId(defaultStrategyId);
  }, [defaultStrategyId, selectedStrategyId, setSelectedLiveStrategyId, strategies]);

  const setSnapshot = useLiveStore((s) => s.setSnapshot);
  const clearLive = useLiveStore((s) => s.clear);
  const positionsBySymbol = useLiveStore((s) => s.positionsBySymbol);
  const ordersById = useLiveStore((s) => s.ordersById);
  const fills = useLiveStore((s) => s.fills);
  const ticksBySymbol = useLiveStore((s) => s.ticksBySymbol);
  const pushTick = useLiveStore((s) => s.pushTick);

  const portfolioQuery = usePortfolioQuery(selectedStrategyId);
  const positionsQuery = usePositionsQuery(true, selectedStrategyId);
  const ordersQuery = useOrdersQuery(true, selectedStrategyId);
  const fillsQuery = useFillsQuery(true, selectedStrategyId);
  const marketTicksQuery = useMarketTicksQuery(true);
  const diagnosticsQuery = useStrategyDiagnosticsQuery(true, selectedStrategyId, diagRefreshMs);
  const applyDiagRefreshMs = (nextValue: number) => {
    const normalized = normalizeDiagRefreshMs(nextValue);
    setLiveFilters({ ...savedLiveFilters, diagRefreshMs: normalized });
  };
  const selectedStrategy = useMemo(
    () => (strategies ?? []).find((s) => s.id === selectedStrategyId),
    [selectedStrategyId, strategies],
  );
  const showStrategyStoppedWarning = Boolean(selectedStrategy && selectedStrategy.status !== 'running');

  useEffect(() => {
    clearLive();
  }, [clearLive, selectedStrategyId]);

  useEffect(() => {
    if (!positionsQuery.data || !ordersQuery.data || !fillsQuery.data) return;
    setSnapshot({
      positions: positionsQuery.data,
      orders: ordersQuery.data,
      fills: fillsQuery.data,
    });
  }, [fillsQuery.data, ordersQuery.data, positionsQuery.data, setSnapshot]);

  useEffect(() => {
    const rows = marketTicksQuery.data ?? [];
    if (rows.length === 0) return;
    rows.forEach((t) => pushTick(t));
  }, [marketTicksQuery.data, pushTick]);

  const positions = useMemo(() => Object.values(positionsBySymbol), [positionsBySymbol]);
  const orders = useMemo(
    () => Object.values(ordersById).sort((a, b) => (a.ts < b.ts ? 1 : -1)),
    [ordersById],
  );
  const ticks = useMemo(
    () => Object.values(ticksBySymbol).sort((a, b) => (a.symbol < b.symbol ? -1 : 1)),
    [ticksBySymbol],
  );
  const diagnostics = diagnosticsQuery.data;
  const diagnosticsSnapshot = diagnostics?.snapshot;
  const diagnosticsState = diagnosticsSnapshot?.strategy_state?.state ?? 'UNKNOWN';
  const diagnosticsStateColor =
    diagnosticsState === 'RUNNING'
      ? 'green'
      : diagnosticsState === 'SAFE_MODE'
        ? 'orange'
        : diagnosticsState === 'ERROR'
          ? 'red'
          : 'default';
  const signalConditions = useMemo(
    () => diagnosticsSnapshot?.signal_evaluation?.conditions ?? [],
    [diagnosticsSnapshot?.signal_evaluation?.conditions],
  );
  const exceptionRows = diagnosticsSnapshot?.exceptions?.last_20 ?? [];
  const exceptionCount = diagnosticsSnapshot?.exceptions?.total_count ?? 0;
  const exchange = diagnosticsSnapshot?.exchange_connection;
  const process = diagnosticsSnapshot?.process;
  const marketData = diagnosticsSnapshot?.market_data;
  const localizedSignalConditions = useMemo<StrategyDiagnosticsCondition[]>(
    () =>
      signalConditions.map((row) => ({
        ...row,
        name: row.name ? localizeDiagString(String(row.name), language) : row.name,
      })),
    [language, signalConditions],
  );
  const localizedFilterReasons = useMemo(
    () =>
      (diagnosticsSnapshot?.signal_evaluation?.filter_reasons ?? []).map((reason) =>
        localizeDiagString(reason, language),
      ),
    [diagnosticsSnapshot?.signal_evaluation?.filter_reasons, language],
  );
  const hasNoOrderIntentReason = useMemo(
    () =>
      (diagnosticsSnapshot?.signal_evaluation?.filter_reasons ?? []).some((reason) =>
        reason.includes('no_order_intents_after_filters'),
      ),
    [diagnosticsSnapshot?.signal_evaluation?.filter_reasons],
  );
  const showNoOrderIntentWarning = Boolean(
    selectedStrategy &&
      selectedStrategy.status === 'running' &&
      positions.length === 0 &&
      orders.length === 0 &&
      fills.length === 0 &&
      hasNoOrderIntentReason,
  );
  const localizedDataSource = useMemo(() => {
    const status = localizeDiagString(marketData?.data_source_status ?? '-', language);
    const detail = localizeDiagString(marketData?.data_source_detail ?? '', language);
    if (!detail) return status;
    return language === 'zh' ? `${status}，${detail}` : `${status} ${detail}`.trim();
  }, [language, marketData?.data_source_detail, marketData?.data_source_status]);
  const localizedLastApiError = useMemo(() => {
    const api = exchange?.last_api_error?.api;
    if (!api) return '-';
    const apiName = localizeDiagApiName(api, language);
    const msg = exchange?.last_api_error?.message;
    if (!msg) return apiName;
    return `${apiName}: ${localizeDiagString(msg, language)}`;
  }, [exchange?.last_api_error?.api, exchange?.last_api_error?.message, language]);
  const diagnosticsStateText =
    diagnosticsState === 'RUNNING'
      ? t('diagStateRunning')
      : diagnosticsState === 'PAUSED'
        ? t('diagStatePaused')
        : diagnosticsState === 'SAFE_MODE'
          ? t('diagStateSafeMode')
          : diagnosticsState === 'ERROR'
            ? t('diagStateError')
            : t('diagStateUnknown');
  const snapshotUpdatedAt = diagnostics?.updated_at ?? diagnosticsSnapshot?.generated_at ?? '';
  const snapshotAgeSec = useMemo(() => {
    const ms = parseIsoMs(snapshotUpdatedAt);
    if (ms === null) return null;
    return Math.max(0, Math.floor((Date.now() - ms) / 1000));
  }, [snapshotUpdatedAt]);
  const snapshotFreshness = useMemo<'fresh' | 'delayed' | 'stale' | 'unknown'>(() => {
    if (snapshotAgeSec === null) return 'unknown';
    const delayThreshold = Math.max(30, Math.floor(diagRefreshMs / 1000) * 2);
    if (snapshotAgeSec <= delayThreshold) return 'fresh';
    if (snapshotAgeSec <= 300) return 'delayed';
    return 'stale';
  }, [diagRefreshMs, snapshotAgeSec]);
  const snapshotFreshnessLabel =
    snapshotFreshness === 'fresh'
      ? t('diagQuickFresh')
      : snapshotFreshness === 'delayed'
        ? t('diagQuickDelayed')
        : snapshotFreshness === 'stale'
          ? t('diagQuickStale')
          : t('diagQuickUnknown');
  const snapshotFreshnessColor =
    snapshotFreshness === 'fresh' ? 'green' : snapshotFreshness === 'delayed' ? 'gold' : snapshotFreshness === 'stale' ? 'red' : 'default';
  const snapshotAgeText = useMemo(() => {
    if (snapshotAgeSec === null) return '-';
    if (snapshotAgeSec < 120) return `${snapshotAgeSec}${t('diagQuickSecUnit')}`;
    const mins = Math.round((snapshotAgeSec / 60) * 10) / 10;
    return `${mins}${t('diagQuickMinUnit')}`;
  }, [snapshotAgeSec, t]);
  const exchangeProbeValues = [
    exchange?.fetch_balance?.ok,
    exchange?.fetch_positions?.ok,
    exchange?.fetch_open_orders?.ok,
  ];
  const exchangeProbeTotal = exchangeProbeValues.filter((v) => typeof v === 'boolean').length;
  const exchangeProbeFailCount = exchangeProbeValues.filter((v) => v === false).length;
  const exchangeProbeOkCount = exchangeProbeValues.filter((v) => v === true).length;
  const noLiveActivity = positions.length === 0 && orders.length === 0 && fills.length === 0;
  const latestFillTs = useMemo(() => {
    const filledRows = fills
      .map((fill) => String(fill.ts ?? '').trim())
      .filter((ts) => ts.length > 0)
      .sort((a, b) => (a < b ? 1 : -1));
    return filledRows[0] ?? null;
  }, [fills]);
  const latestFillText = latestFillTs ? formatTs(latestFillTs) : t('diagQuickNoFillYet');
  const quickRead = useMemo(() => {
    const issues: string[] = [];
    const actions = new Set<string>();
    const focusTabs = new Set<DiagnosticsTabKey>();
    let level: 'success' | 'warning' | 'error' = 'success';

    const raiseLevel = (next: 'warning' | 'error') => {
      if (next === 'error') {
        level = 'error';
        return;
      }
      if (level === 'success') level = 'warning';
    };
    const addIssue = (issue: string) => {
      if (!issues.includes(issue)) issues.push(issue);
    };
    const addAction = (action: string, focusTab?: DiagnosticsTabKey) => {
      actions.add(action);
      if (focusTab) focusTabs.add(focusTab);
    };

    if (diagnosticsState === 'ERROR') {
      raiseLevel('error');
      addIssue(t('diagQuickIssueStateError'));
      addAction(t('diagQuickActionCheckProcess'));
      addAction(t('diagQuickActionViewExceptions'), 'exceptions');
    } else if (diagnosticsState === 'SAFE_MODE') {
      raiseLevel('warning');
      addIssue(t('diagQuickIssueStateSafeMode'));
      addAction(t('diagQuickActionViewReasons'), 'signal');
    } else if (diagnosticsState === 'PAUSED') {
      raiseLevel('warning');
      addIssue(t('diagQuickIssueStatePaused'));
      addAction(t('diagQuickActionCheckProcess'));
    }

    if (snapshotFreshness === 'stale') {
      raiseLevel('error');
      addIssue(t('diagQuickIssueSnapshotStale'));
      addAction(t('diagQuickActionCheckProcess'));
    } else if (snapshotFreshness === 'delayed') {
      raiseLevel('warning');
      addIssue(t('diagQuickIssueSnapshotDelayed'));
    }

    if (exchangeProbeFailCount > 0) {
      raiseLevel(exchangeProbeFailCount >= 2 ? 'error' : 'warning');
      addIssue(t('diagQuickIssueExchangeFail'));
      addAction(t('diagQuickActionCheckExchange'));
    }

    if (exceptionCount > 0) {
      raiseLevel('warning');
      addAction(t('diagQuickActionViewExceptions'), 'exceptions');
    }

    if (hasNoOrderIntentReason) {
      raiseLevel('warning');
      addIssue(t('diagQuickIssueNoOrderSignal'));
      addAction(t('diagQuickActionTuneFilter'));
      addAction(t('diagQuickActionViewReasons'), 'signal');
    }

    if (diagnosticsState === 'RUNNING' && noLiveActivity && !hasNoOrderIntentReason) {
      raiseLevel('warning');
      addIssue(t('diagQuickIssueNoActivity'));
      addAction(t('diagQuickActionViewPosOrders'), 'pos-orders');
    }

    if (issues.length === 0) addIssue(t('diagQuickNoIssue'));
    if (actions.size === 0) addAction(t('diagQuickActionNone'));

    const summary = {
      success: t('diagQuickOverallGood'),
      warning: t('diagQuickOverallWarn'),
      error: t('diagQuickOverallError'),
    }[level];

    return {
      level,
      summary,
      issues,
      actions: Array.from(actions),
      focusTabs: Array.from(focusTabs),
    };
  }, [diagnosticsState, exceptionCount, exchangeProbeFailCount, hasNoOrderIntentReason, noLiveActivity, snapshotFreshness, t]);
  const quickSummaryText = useMemo(
    () =>
      [
        `${t('strategy')}: ${selectedStrategy?.name ?? selectedStrategyId ?? '-'}`,
        `${t('diagQuickStateTitle')}: ${diagnosticsStateText}`,
        `${t('diagQuickFreshness')}: ${snapshotFreshnessLabel}`,
        `${t('diagQuickUpdatedAgo')}: ${snapshotAgeText}`,
        `${t('diagQuickConnHealth')}: ${exchangeProbeTotal > 0 ? `${exchangeProbeOkCount}/${exchangeProbeTotal}` : '-'}`,
        `${t('diagQuickActivity')}: ${positions.length}/${orders.length}/${fills.length}`,
        `${t('diagQuickLastFill')}: ${latestFillText}`,
        `${t('diagQuickIssueTitle')}:`,
        ...quickRead.issues.map((item) => `- ${item}`),
        `${t('diagQuickActionTitle')}:`,
        ...quickRead.actions.map((item) => `- ${item}`),
      ].join('\n'),
    [
      fills.length,
      diagnosticsStateText,
      exchangeProbeOkCount,
      exchangeProbeTotal,
      latestFillText,
      orders.length,
      positions.length,
      quickRead.actions,
      quickRead.issues,
      selectedStrategy?.name,
      selectedStrategyId,
      snapshotAgeText,
      snapshotFreshnessLabel,
      t,
    ],
  );
  const troubleshootingChecklist = useMemo(() => {
    const items: string[] = [];
    items.push(`${t('strategy')}: ${selectedStrategy?.name ?? selectedStrategyId ?? '-'}`);
    items.push(`${t('diagQuickStateTitle')}: ${diagnosticsStateText}`);
    items.push(`${t('diagQuickFreshness')}: ${snapshotFreshnessLabel} (${snapshotAgeText})`);
    items.push(`${t('diagQuickActivity')}: ${positions.length}/${orders.length}/${fills.length}`);
    items.push(`${t('diagQuickLastFill')}: ${latestFillText}`);
    if (quickRead.level !== 'success') {
      quickRead.issues.forEach((issue) => items.push(`[Issue] ${issue}`));
    }
    quickRead.actions.forEach((action) => items.push(`[Action] ${action}`));
    return items;
  }, [
    fills.length,
    diagnosticsStateText,
    latestFillText,
    orders.length,
    positions.length,
    quickRead.actions,
    quickRead.issues,
    quickRead.level,
    selectedStrategy?.name,
    selectedStrategyId,
    snapshotAgeText,
    snapshotFreshnessLabel,
    t,
  ]);

  const signalConditionColumns = useMemo<ColumnsType<StrategyDiagnosticsCondition>>(
    () => [
      { title: t('diagCondName'), dataIndex: 'name', width: 240 },
      {
        title: t('diagCondCurrent'),
        dataIndex: 'current',
        width: 180,
        render: (v: StrategyDiagnosticsCondition['current']) => renderDiagValue(v, t('diagBoolTrue'), t('diagBoolFalse')),
      },
      {
        title: t('diagCondThreshold'),
        dataIndex: 'threshold',
        width: 180,
        render: (v: StrategyDiagnosticsCondition['threshold']) => renderDiagValue(v, t('diagBoolTrue'), t('diagBoolFalse')),
      },
      {
        title: t('diagCondResult'),
        dataIndex: 'pass',
        width: 100,
        render: (v?: boolean) => <Tag color={v ? 'green' : 'red'}>{v ? t('diagCondPass') : t('diagCondBlock')}</Tag>,
      },
    ],
    [t],
  );

  const positionColumns = useMemo<ColumnsType<Position>>(
    () => [
      { title: t('symbol'), dataIndex: 'symbol', width: 120 },
      { title: t('qty'), dataIndex: 'qty', width: 100, render: (v: number) => formatNumber(v, 4) },
      {
        title: t('avg'),
        dataIndex: 'avgPrice',
        width: 120,
        render: (v: number) => formatNumber(v, 2),
      },
      {
        title: t('last'),
        dataIndex: 'lastPrice',
        width: 120,
        render: (v: number) => formatNumber(v, 2),
      },
      {
        title: t('upnl'),
        dataIndex: 'unrealizedPnl',
        width: 120,
        sorter: (a, b) => a.unrealizedPnl - b.unrealizedPnl,
        render: (v: number) => (
          <Typography.Text style={{ color: v >= 0 ? '#00b96b' : '#ff4d4f' }}>
            {formatNumber(v, 2)}
          </Typography.Text>
        ),
      },
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
    ],
    [t],
  );

  const orderColumns = useMemo<ColumnsType<Order>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('symbol'), dataIndex: 'symbol', width: 120 },
      { title: t('side'), dataIndex: 'side', width: 90 },
      { title: t('orderType'), dataIndex: 'type', width: 90 },
      { title: t('qty'), dataIndex: 'qty', width: 100, render: (v: number) => formatNumber(v, 4) },
      {
        title: t('filled'),
        dataIndex: 'filledQty',
        width: 100,
        render: (v: number) => formatNumber(v, 4),
      },
      {
        title: t('priceLabel'),
        dataIndex: 'price',
        width: 120,
        render: (v?: number) => formatNumber(v, 2),
      },
      { title: t('status'), dataIndex: 'status', width: 140 },
    ],
    [t],
  );

  const fillColumns = useMemo<ColumnsType<Fill>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('symbol'), dataIndex: 'symbol', width: 120 },
      { title: t('side'), dataIndex: 'side', width: 90 },
      { title: t('qty'), dataIndex: 'qty', width: 100, render: (v: number) => formatNumber(v, 4) },
      { title: t('priceLabel'), dataIndex: 'price', width: 120, render: (v: number) => formatNumber(v, 2) },
      { title: t('fee'), dataIndex: 'fee', width: 100, render: (v: number) => formatNumber(v, 2) },
      { title: t('orderId'), dataIndex: 'orderId', width: 220, ellipsis: true },
    ],
    [t],
  );

  const tickColumns = useMemo<ColumnsType<TickMessage>>(
    () => [
      { title: t('symbol'), dataIndex: 'symbol', width: 120 },
      { title: t('priceLabel'), dataIndex: 'price', width: 120, render: (v: number, r) => formatPriceBySymbol(r.symbol, v) },
      { title: t('bid'), dataIndex: 'bid', width: 120, render: (v: number, r) => formatPriceBySymbol(r.symbol, v) },
      { title: t('ask'), dataIndex: 'ask', width: 120, render: (v: number, r) => formatPriceBySymbol(r.symbol, v) },
      { title: t('vol'), dataIndex: 'volume', width: 100, render: (v: number) => formatNumber(v, 4) },
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
    ],
    [t],
  );
  const diagPositionRows = useMemo<DiagPositionRow[]>(() => {
    const rows = diagnosticsSnapshot?.positions_and_orders?.positions;
    if (!Array.isArray(rows)) return [];
    const mapped: DiagPositionRow[] = [];
    rows.forEach((row, idx) => {
      const item = row && typeof row === 'object' ? (row as Record<string, unknown>) : {};
      const symbol = String(item.symbol ?? '').trim();
      if (!symbol) return;
      mapped.push({
        key: `${symbol}_${idx}`,
        symbol,
        side: String(item.side ?? ''),
        qty: toFiniteNumber(item.qty) ?? 0,
        avgPrice: toFiniteNumber(item.avg_price),
        markPrice: toFiniteNumber(item.mark_price),
        notional: toFiniteNumber(item.notional),
        unrealizedPnl: toFiniteNumber(item.unrealized_pnl),
      });
    });
    return mapped;
  }, [diagnosticsSnapshot?.positions_and_orders?.positions]);
  const diagOpenOrderRows = useMemo<DiagOpenOrderRow[]>(() => {
    const rows = diagnosticsSnapshot?.positions_and_orders?.open_orders;
    if (!Array.isArray(rows)) return [];
    return rows
      .map((row, idx) => {
        const item = row && typeof row === 'object' ? (row as Record<string, unknown>) : {};
        const symbol = String(item.symbol ?? '').trim();
        return {
          key: String(item.id ?? `${symbol || 'order'}_${idx}`),
          ts: String(item.ts ?? item.timestamp ?? item.created_at ?? ''),
          symbol: symbol || '-',
          side: String(item.side ?? ''),
          type: String(item.type ?? item.order_type ?? ''),
          qty: toFiniteNumber(item.qty ?? item.amount ?? item.size),
          price: toFiniteNumber(item.price ?? item.avg_price),
          status: String(item.status ?? ''),
        };
      })
      .filter((row) => row.symbol && row.symbol !== '-');
  }, [diagnosticsSnapshot?.positions_and_orders?.open_orders]);
  const diagOrderAttemptRows = useMemo<DiagOrderAttemptRow[]>(() => {
    const rows = diagnosticsSnapshot?.recent_order_attempts;
    if (!Array.isArray(rows)) return [];
    return rows
      .map((row, idx) => {
        const item = row && typeof row === 'object' ? (row as Record<string, unknown>) : {};
        const symbol = String(item.symbol ?? '').trim();
        const failureReason = String(item.failure_reason ?? item.error ?? '').trim();
        return {
          key: String(item.id ?? `${symbol || 'attempt'}_${idx}`),
          ts: String(item.ts ?? ''),
          symbol: symbol || '-',
          side: String(item.side ?? ''),
          status: String(item.status ?? ''),
          qty: toFiniteNumber(item.qty ?? item.amount),
          price: toFiniteNumber(item.price),
          reason: failureReason || '-',
        };
      })
      .filter((row) => row.symbol && row.symbol !== '-');
  }, [diagnosticsSnapshot?.recent_order_attempts]);
  const diagPositionColumns = useMemo<ColumnsType<DiagPositionRow>>(
    () => [
      { title: t('symbol'), dataIndex: 'symbol', width: 140 },
      { title: t('side'), dataIndex: 'side', width: 100, render: (v: string) => localizeDiagString(v, language) },
      { title: t('qty'), dataIndex: 'qty', width: 120, render: (v: number) => formatNumber(v, 4) },
      { title: t('avg'), dataIndex: 'avgPrice', width: 120, render: (v?: number) => formatNumber(v, 2) },
      { title: t('last'), dataIndex: 'markPrice', width: 120, render: (v?: number) => formatNumber(v, 2) },
      { title: t('notional'), dataIndex: 'notional', width: 120, render: (v?: number) => formatNumber(v, 2) },
      {
        title: t('upnl'),
        dataIndex: 'unrealizedPnl',
        width: 130,
        render: (v?: number) =>
          v === undefined ? (
            '-'
          ) : (
            <Typography.Text style={{ color: v >= 0 ? '#00b96b' : '#ff4d4f' }}>{formatNumber(v, 2)}</Typography.Text>
          ),
      },
    ],
    [language, t],
  );
  const diagOpenOrderColumns = useMemo<ColumnsType<DiagOpenOrderRow>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v?: string) => formatTs(v) },
      { title: t('symbol'), dataIndex: 'symbol', width: 140 },
      { title: t('side'), dataIndex: 'side', width: 100, render: (v: string) => localizeDiagString(v, language) },
      { title: t('orderType'), dataIndex: 'type', width: 100, render: (v: string) => localizeDiagString(v, language) || '-' },
      { title: t('qty'), dataIndex: 'qty', width: 110, render: (v?: number) => formatNumber(v, 4) },
      { title: t('priceLabel'), dataIndex: 'price', width: 120, render: (v?: number) => formatNumber(v, 2) },
      { title: t('status'), dataIndex: 'status', width: 120, render: (v: string) => localizeDiagString(v, language) || '-' },
    ],
    [language, t],
  );
  const diagAttemptColumns = useMemo<ColumnsType<DiagOrderAttemptRow>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v?: string) => formatTs(v) },
      { title: t('symbol'), dataIndex: 'symbol', width: 140 },
      { title: t('side'), dataIndex: 'side', width: 90, render: (v: string) => localizeDiagString(v, language) },
      { title: t('status'), dataIndex: 'status', width: 120, render: (v: string) => localizeDiagString(v, language) },
      { title: t('qty'), dataIndex: 'qty', width: 100, render: (v?: number) => formatNumber(v, 4) },
      { title: t('priceLabel'), dataIndex: 'price', width: 120, render: (v?: number) => formatNumber(v, 2) },
      {
        title: t('failureReason'),
        dataIndex: 'reason',
        width: 260,
        render: (v: string) => localizeDiagString(v, language),
      },
    ],
    [language, t],
  );
  const lastOrderAttempt = (diagnosticsSnapshot?.last_order_attempt ?? {}) as Record<string, unknown>;
  const stopTakeTrailing = (diagnosticsSnapshot?.stop_take_trailing ?? {}) as Record<string, unknown>;
  const posOrdersEmptyAlert = useMemo(() => {
    if (diagPositionRows.length > 0 || diagOpenOrderRows.length > 0 || diagOrderAttemptRows.length > 0) return null;
    if (diagnosticsState === 'PAUSED') {
      return {
        type: 'info' as const,
        message: t('diagPosOrdersEmptyPausedTitle'),
        description: t('diagPosOrdersEmptyPausedDesc'),
      };
    }
    if (diagnosticsState === 'SAFE_MODE') {
      return {
        type: 'warning' as const,
        message: t('diagPosOrdersEmptySafeModeTitle'),
        description: t('diagPosOrdersEmptySafeModeDesc'),
      };
    }
    if (hasNoOrderIntentReason) {
      return {
        type: 'info' as const,
        message: t('diagPosOrdersEmptyNoSignalTitle'),
        description: t('diagPosOrdersEmptyNoSignalDesc'),
      };
    }
    if (snapshotFreshness === 'stale' || snapshotFreshness === 'delayed') {
      return {
        type: 'warning' as const,
        message: t('diagPosOrdersEmptyStaleTitle'),
        description: t('diagPosOrdersEmptyStaleDesc'),
      };
    }
    return {
      type: 'info' as const,
      message: t('diagPosOrdersEmptyNeutralTitle'),
      description: t('diagPosOrdersEmptyNeutralDesc'),
    };
  }, [
    diagOpenOrderRows.length,
    diagOrderAttemptRows.length,
    diagPositionRows.length,
    diagnosticsState,
    hasNoOrderIntentReason,
    snapshotFreshness,
    t,
  ]);

  return (
    <div className="page-shell">
      <Typography.Title level={3} style={{ margin: 0 }}>
        {t('liveMonitor')}
      </Typography.Title>
      <NonTechGuideCard
        title={byLang('实盘监控建议', 'Live monitoring guidance')}
        summary={byLang(
          '这里主要用于观察运行状态，不建议在未确认风控前频繁调整配置。',
          'This page is mainly for monitoring. Avoid frequent config changes before risk is confirmed.',
        )}
        steps={[
          byLang('先确认策略状态是“运行中”', 'Verify strategy is running'),
          byLang('重点看权益曲线与风控提醒', 'Focus on equity curve and risk warnings'),
          byLang('若无持仓/订单，先看诊断里的过滤原因', 'If no positions/orders, check diagnostics filter reasons'),
        ]}
        tip={byLang(
          '空数据不一定是错误，也可能是策略当前无交易信号。',
          'Empty data is not always an error; it may simply mean no active signal.',
        )}
      />
      <Card size="small">
        <Space wrap>
          <Typography.Text>{t('strategy')}</Typography.Text>
          <Select
            className="strategy-select"
            popupClassName="strategy-select-dropdown"
            value={selectedStrategyId}
            onChange={(v: string) => setSelectedLiveStrategyId(v)}
            options={(strategies ?? []).map((s) => ({
              value: s.id,
              label: (
                <span className="strategy-option-label" title={`${s.name} (${s.id})`}>
                  {s.name}
                </span>
              ),
            }))}
            placeholder={t('strategy')}
          />
          <Typography.Text type="secondary">
            {t('equity')}={formatNumber(portfolioQuery.data?.equity ?? 0, 2)} {t('cash')}={formatNumber(portfolioQuery.data?.cash ?? 0, 2)}
          </Typography.Text>
        </Space>
      </Card>
      {showStrategyStoppedWarning ? (
        <Alert
          type="warning"
          showIcon
          message={t('liveStrategyNotRunningTitle')}
          description={t('liveStrategyNotRunningDesc')}
        />
      ) : null}
      {showNoOrderIntentWarning ? (
        <Alert
          type="info"
          showIcon
          message={t('liveNoOrderIntentTitle')}
          description={t('liveNoOrderIntentDesc')}
        />
      ) : null}

      <Row gutter={[16, 16]} align="stretch">
        <Col xs={24} lg={16}>
          <Card
            style={{ height: '100%' }}
            title={
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: 12,
                }}
              >
                <Typography.Text strong>{t('liveEquity')}</Typography.Text>
                {isMobile ? (
                  <div style={{ display: 'flex', gap: 8, width: '100%' }}>
                    <Popconfirm
                      title={byLang('确认清空当前实时数据？', 'Clear current realtime data?')}
                      description={byLang(
                        '仅清空页面缓存，不会停止策略或删除后端数据。',
                        'This only clears page cache and will not stop strategy or delete backend data.',
                      )}
                      okText={byLang('确认清空', 'Clear')}
                      cancelText={byLang('取消', 'Cancel')}
                      onConfirm={() => {
                        clearLive();
                        message.success(t('paperErrorClear'));
                      }}
                    >
                      <Tooltip title={t('clearTip')}>
                        <Button icon={<DeleteOutlined />} style={{ flex: 1 }}>
                          {t('clear')}
                        </Button>
                      </Tooltip>
                    </Popconfirm>
                    <Tooltip title={t('closeAllTip')}>
                      <Button icon={<StopOutlined />} disabled style={{ flex: 1 }}>
                        {t('closeAll')}
                      </Button>
                    </Tooltip>
                  </div>
                ) : (
                  <Space size={8}>
                    <Popconfirm
                      title={byLang('确认清空当前实时数据？', 'Clear current realtime data?')}
                      description={byLang(
                        '仅清空页面缓存，不会停止策略或删除后端数据。',
                        'This only clears page cache and will not stop strategy or delete backend data.',
                      )}
                      okText={byLang('确认清空', 'Clear')}
                      cancelText={byLang('取消', 'Cancel')}
                      onConfirm={() => {
                        clearLive();
                        message.success(t('paperErrorClear'));
                      }}
                    >
                      <Tooltip title={t('clearTip')}>
                        <Button icon={<DeleteOutlined />}>{t('clear')}</Button>
                      </Tooltip>
                    </Popconfirm>
                    <Tooltip title={t('closeAllTip')}>
                      <Button icon={<StopOutlined />} disabled>
                        {t('closeAll')}
                      </Button>
                    </Tooltip>
                  </Space>
                )}
              </div>
            }
          >
            <RealtimeEquityChart height={320} />
          </Card>
        </Col>
        <Col xs={24} lg={8}>
          <Card title={t('realtimeMarket')} style={{ height: '100%' }}>
            {ticks.length === 0 ? (
              <Empty description={t('waitingWs')} />
            ) : (
              <Table
                rowKey="symbol"
                size="small"
                columns={tickColumns}
                dataSource={ticks}
                pagination={{ pageSize: 6, hideOnSinglePage: true }}
                scroll={{ x: 740, y: 290 }}
              />
            )}
          </Card>
        </Col>
      </Row>

      <Card title={t('assetsOrders')}>
        <Tabs
          items={[
            {
              key: 'positions',
              label: `${t('positions')} (${positions.length})`,
              children:
                positions.length === 0 ? (
                  <Empty description={t('noPositions')} />
                ) : (
                  <Table
                    rowKey="symbol"
                    size="small"
                    columns={positionColumns}
                    dataSource={positions}
                    pagination={{ pageSize: 8 }}
                    scroll={{ x: 900 }}
                  />
                ),
            },
            {
              key: 'orders',
              label: `${t('orders')} (${orders.length})`,
              children:
                orders.length === 0 ? (
                  <Empty description={t('noOrders')} />
                ) : (
                  <Table
                    rowKey="id"
                    size="small"
                    columns={orderColumns}
                    dataSource={orders}
                    pagination={{ pageSize: 8 }}
                    scroll={{ x: 980 }}
                  />
                ),
            },
            {
              key: 'fills',
              label: `${t('fills')} (${fills.length})`,
              children:
                fills.length === 0 ? (
                  <Empty description={t('noFills')} />
                ) : (
                  <Table
                    rowKey="id"
                    size="small"
                    columns={fillColumns}
                    dataSource={fills}
                    pagination={{ pageSize: 8 }}
                    scroll={{ x: 960 }}
                  />
                ),
            },
          ]}
        />
      </Card>

      <Card
        title={t('diagPanelTitle')}
        extra={
          <Space wrap>
            <Typography.Text type="secondary">{t('diagRefreshLabel')}</Typography.Text>
            <Select
              value={diagRefreshMs}
              style={{ minWidth: 120 }}
              onChange={(v: number) => applyDiagRefreshMs(v)}
              options={[
                { value: 15_000, label: t('diagRefresh15s') },
                { value: 60_000, label: t('diagRefresh1m') },
                { value: 120_000, label: t('diagRefresh2m') },
                { value: 300_000, label: t('diagRefresh5m') },
              ]}
            />
            <Button onClick={() => void diagnosticsQuery.refetch()}>{t('refresh')}</Button>
            <Button
              disabled={!diagnosticsSnapshot}
              onClick={() => void copyText(quickSummaryText)}
            >
              {t('diagCopySummary')}
            </Button>
            <Button
              disabled={!diagnosticsSnapshot}
              onClick={() =>
                void copyText(JSON.stringify(localizeDiagJson(diagnosticsSnapshot ?? {}, language), null, 2))
              }
            >
              {t('diagCopyJson')}
            </Button>
          </Space>
        }
        loading={diagnosticsQuery.isPending}
      >
        {diagnosticsQuery.isError ? (
          <ActionableErrorAlert
            title={t('diagFetchFailed')}
            steps={[
              byLang('点击“刷新”重试诊断拉取。', 'Click Refresh to retry diagnostics fetch.'),
              byLang('确认策略已启动并有最近数据更新。', 'Confirm strategy is running and data is updating.'),
              byLang('仍失败时到日志中心查看系统报错。', 'If it still fails, inspect errors in Logs Center.'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void diagnosticsQuery.refetch()}
          />
        ) : !diagnosticsSnapshot ? (
          <Empty description={t('diagNoSnapshot')} />
        ) : (
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Descriptions size="small" bordered column={isMobile ? 1 : 2}>
              <Descriptions.Item label={t('diagState')}>
                <Space wrap>
                  <Tag color={diagnosticsStateColor}>{diagnosticsStateText}</Tag>
                  <Typography.Text type="secondary">
                    {formatTs(diagnosticsSnapshot.strategy_state?.last_switch_time)}
                  </Typography.Text>
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label={t('diagSnapshotUpdated')}>
                {formatTs(diagnostics?.updated_at)}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagProcessStarted')}>
                {formatTs(process?.started_at)}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagVersionCommit')}>
                {`${process?.version ?? '-'} / ${process?.commit_id ?? '-'}`}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagConfigPath')} span={isMobile ? 1 : 2}>
                <Typography.Text copyable>{process?.config_path ?? diagnosticsSnapshot.config?.path ?? '-'}</Typography.Text>
              </Descriptions.Item>
              <Descriptions.Item label={t('diagLastTick')}>
                {formatTs(marketData?.last_tick_time)}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagLastBar')}>
                {formatTs(marketData?.last_bar_time)}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagDataLagSeconds')}>
                {formatNumber(marketData?.data_lag_seconds, 2)}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagDataSource')}>
                {localizedDataSource}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagExchangeConnectivity')} span={isMobile ? 1 : 2}>
                <Space wrap>
                  <Tag color={exchange?.fetch_balance?.ok ? 'green' : 'red'}>
                    {`${t('diagConnBalance')}:${exchange?.fetch_balance?.ok ? t('diagConnOk') : t('diagConnErr')}`}
                  </Tag>
                  <Tag color={exchange?.fetch_positions?.ok ? 'green' : 'red'}>
                    {`${t('diagConnPositions')}:${exchange?.fetch_positions?.ok ? t('diagConnOk') : t('diagConnErr')}`}
                  </Tag>
                  <Tag color={exchange?.fetch_open_orders?.ok ? 'green' : 'red'}>
                    {`${t('diagConnOrders')}:${exchange?.fetch_open_orders?.ok ? t('diagConnOk') : t('diagConnErr')}`}
                  </Tag>
                </Space>
              </Descriptions.Item>
              <Descriptions.Item label={t('diagExceptionCount10d')}>
                {exceptionCount}
              </Descriptions.Item>
              <Descriptions.Item label={t('diagLastApiError')}>
                {localizedLastApiError}
              </Descriptions.Item>
            </Descriptions>

            <Tabs
              activeKey={diagTabKey}
              onChange={(key) => setDiagTabKey(key as DiagnosticsTabKey)}
              items={[
                {
                  key: 'quick',
                  label: t('diagTabQuick'),
                  children: (
                    <Space direction="vertical" size={12} style={{ width: '100%' }}>
                      <Alert
                        type={quickRead.level}
                        showIcon
                        message={quickRead.summary}
                        description={
                          <Space direction="vertical" size={6}>
                            <Typography.Text>
                              {t('diagQuickStateTitle')}: {diagnosticsStateText}
                            </Typography.Text>
                            <Space wrap>
                              <Typography.Text>{t('diagQuickFreshness')}:</Typography.Text>
                              <Tag color={snapshotFreshnessColor}>{snapshotFreshnessLabel}</Tag>
                              <Typography.Text type="secondary">
                                {t('diagQuickUpdatedAgo')}: {snapshotAgeText}
                              </Typography.Text>
                            </Space>
                            <Typography.Text>
                              {t('diagQuickConnHealth')}:{' '}
                              {exchangeProbeTotal > 0 ? `${exchangeProbeOkCount}/${exchangeProbeTotal}` : '-'}
                            </Typography.Text>
                            <Typography.Text>
                              {t('diagQuickActivity')}: {positions.length}/{orders.length}/{fills.length}
                            </Typography.Text>
                            <Typography.Text>
                              {t('diagQuickLastFill')}: {latestFillText}
                            </Typography.Text>
                          </Space>
                        }
                      />
                      <Typography.Text strong>{t('diagQuickIssueTitle')}</Typography.Text>
                      <Space direction="vertical" size={4} style={{ width: '100%' }}>
                        {quickRead.issues.map((item, idx) => (
                          <Typography.Text key={`diag_issue_${idx}`}>{`${idx + 1}. ${item}`}</Typography.Text>
                        ))}
                      </Space>
                      <Typography.Text strong>{t('diagQuickActionTitle')}</Typography.Text>
                      <Space direction="vertical" size={4} style={{ width: '100%' }}>
                        {quickRead.actions.map((item, idx) => (
                          <Typography.Text key={`diag_action_${idx}`}>{`${idx + 1}. ${item}`}</Typography.Text>
                        ))}
                      </Space>
                      {quickRead.focusTabs.length > 0 ? (
                        <Card size="small" title={t('diagQuickJumpTitle')}>
                          <Space wrap>
                            {quickRead.focusTabs.includes('signal') ? (
                              <Button size="small" onClick={() => setDiagTabKey('signal')}>
                                {t('diagQuickGoSignal')}
                              </Button>
                            ) : null}
                            {quickRead.focusTabs.includes('pos-orders') ? (
                              <Button size="small" onClick={() => setDiagTabKey('pos-orders')}>
                                {t('diagQuickGoPosOrders')}
                              </Button>
                            ) : null}
                            {quickRead.focusTabs.includes('exceptions') ? (
                              <Button size="small" onClick={() => setDiagTabKey('exceptions')}>
                                {t('diagQuickGoExceptions')}
                              </Button>
                            ) : null}
                          </Space>
                        </Card>
                      ) : null}
                      <Card
                        size="small"
                        title={t('diagChecklistTitle')}
                        extra={
                          <Button size="small" onClick={() => void copyText(troubleshootingChecklist.map((item, idx) => `${idx + 1}. ${item}`).join('\n'))}>
                            {t('diagCopyChecklist')}
                          </Button>
                        }
                      >
                        <Space direction="vertical" size={4} style={{ width: '100%' }}>
                          {troubleshootingChecklist.map((item, idx) => (
                            <Typography.Text key={`diag_checklist_${idx}`}>{`${idx + 1}. ${item}`}</Typography.Text>
                          ))}
                        </Space>
                      </Card>
                    </Space>
                  ),
                },
                {
                  key: 'signal',
                  label: t('diagTabSignal'),
                  children: (
                    <Space direction="vertical" size={12} style={{ width: '100%' }}>
                      <Space wrap>
                        <Typography.Text strong>{t('diagEntrySignal')}</Typography.Text>
                        <Tag color={diagnosticsSnapshot.signal_evaluation?.entry_signal ? 'green' : 'default'}>
                          {diagnosticsSnapshot.signal_evaluation?.entry_signal ? t('diagBoolTrue') : t('diagBoolFalse')}
                        </Tag>
                      </Space>
                      <Typography.Text>
                        {t('diagFilterReasons')}:{' '}
                        {localizedFilterReasons.join('；') || '-'}
                      </Typography.Text>
                      <Table
                        size="small"
                        rowKey={(r, idx) => `${r.name ?? 'cond'}_${idx}`}
                        columns={signalConditionColumns}
                        dataSource={localizedSignalConditions}
                        pagination={{ pageSize: 8, hideOnSinglePage: true }}
                        scroll={{ x: 760 }}
                      />
                      <JsonBlock
                        title={t('diagSignalDetails')}
                        value={localizeDiagJson(diagnosticsSnapshot.signal_evaluation?.details ?? {}, language)}
                      />
                    </Space>
                  ),
                },
                {
                  key: 'pos-orders',
                  label: t('diagTabPosOrders'),
                  children: (
                    <Space direction="vertical" size={12} style={{ width: '100%' }}>
                      {posOrdersEmptyAlert ? (
                        <Alert
                          showIcon
                          type={posOrdersEmptyAlert.type}
                          message={posOrdersEmptyAlert.message}
                          description={posOrdersEmptyAlert.description}
                        />
                      ) : null}
                      <Typography.Text strong>{t('diagPosTableTitle')}</Typography.Text>
                      {diagPositionRows.length === 0 ? (
                        <Empty description={t('noPositions')} />
                      ) : (
                        <Table
                          rowKey="key"
                          size="small"
                          columns={diagPositionColumns}
                          dataSource={diagPositionRows}
                          pagination={{ pageSize: 8, hideOnSinglePage: true }}
                          scroll={{ x: 980 }}
                        />
                      )}

                      <Typography.Text strong>{t('diagOpenOrderTableTitle')}</Typography.Text>
                      {diagOpenOrderRows.length === 0 ? (
                        <Empty description={t('diagNoOpenOrders')} />
                      ) : (
                        <Table
                          rowKey="key"
                          size="small"
                          columns={diagOpenOrderColumns}
                          dataSource={diagOpenOrderRows}
                          pagination={{ pageSize: 8, hideOnSinglePage: true }}
                          scroll={{ x: 980 }}
                        />
                      )}

                      <Typography.Text strong>{t('diagAttemptTableTitle')}</Typography.Text>
                      {diagOrderAttemptRows.length === 0 ? (
                        <Empty description={t('diagNoRecentAttempts')} />
                      ) : (
                        <Table
                          rowKey="key"
                          size="small"
                          columns={diagAttemptColumns}
                          dataSource={diagOrderAttemptRows}
                          pagination={{ pageSize: 8, hideOnSinglePage: true }}
                          scroll={{ x: 1060 }}
                        />
                      )}

                      <Typography.Text strong>{t('diagLastOrderAttempt')}</Typography.Text>
                      <Descriptions size="small" bordered column={isMobile ? 1 : 2}>
                        <Descriptions.Item label={t('time')}>{formatTs(String(lastOrderAttempt.ts ?? ''))}</Descriptions.Item>
                        <Descriptions.Item label={t('status')}>
                          {localizeDiagString(String(lastOrderAttempt.status ?? '-'), language)}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('symbol')}>{String(lastOrderAttempt.symbol ?? '-')}</Descriptions.Item>
                        <Descriptions.Item label={t('side')}>
                          {localizeDiagString(String(lastOrderAttempt.side ?? '-'), language)}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('qty')}>
                          {formatNumber(toFiniteNumber(lastOrderAttempt.qty), 4)}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('priceLabel')}>
                          {formatNumber(toFiniteNumber(lastOrderAttempt.price), 2)}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('failureReason')} span={isMobile ? 1 : 2}>
                          {localizeDiagString(
                            String(lastOrderAttempt.failure_reason ?? lastOrderAttempt.error ?? '-') || '-',
                            language,
                          )}
                        </Descriptions.Item>
                      </Descriptions>

                      <Typography.Text strong>{t('diagSlTpTs')}</Typography.Text>
                      <Descriptions size="small" bordered column={isMobile ? 1 : 2}>
                        <Descriptions.Item label="SL">{String(stopTakeTrailing.sl ?? '-')}</Descriptions.Item>
                        <Descriptions.Item label="TP">{String(stopTakeTrailing.tp ?? '-')}</Descriptions.Item>
                        <Descriptions.Item label="TS">{String(stopTakeTrailing.ts ?? '-')}</Descriptions.Item>
                        <Descriptions.Item label={t('priceLabel')}>
                          {localizeDiagString(String(stopTakeTrailing.price_source ?? '-'), language)}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('updatedAt')}>
                          {formatTs(String(stopTakeTrailing.last_updated ?? ''))}
                        </Descriptions.Item>
                        <Descriptions.Item label={t('details')}>
                          {localizeDiagString(String(stopTakeTrailing.note ?? '-'), language)}
                        </Descriptions.Item>
                      </Descriptions>
                    </Space>
                  ),
                },
                {
                  key: 'exceptions',
                  label: t('diagTabExceptions'),
                  children: (
                    <Space direction="vertical" size={12} style={{ width: '100%' }}>
                      <JsonBlock
                        title={t('diagExceptionDist10d')}
                        value={localizeDiagJson(diagnosticsSnapshot.exceptions?.counts_by_day ?? {}, language)}
                      />
                      <JsonBlock
                        title={t('diagLast20ExceptionStacks')}
                        value={localizeDiagJson(exceptionRows, language)}
                      />
                    </Space>
                  ),
                },
                {
                  key: 'raw',
                  label: t('diagTabRawJson'),
                  children: <JsonBlock value={localizeDiagJson(diagnosticsSnapshot, language)} />,
                },
              ]}
            />
          </Space>
        )}
      </Card>
    </div>
  );
}
