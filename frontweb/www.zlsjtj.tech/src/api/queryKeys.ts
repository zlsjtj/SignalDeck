import type { AuditLogsQueryParams, LogsQueryParams, RiskEventsQueryParams } from '@/types/api';

export const queryKeys = {
  health: ['health'] as const,

  strategies: ['strategies'] as const,
  strategy: (id: string) => ['strategies', id] as const,

  backtests: ['backtests'] as const,
  backtest: (id: string) => ['backtests', id] as const,
  backtestLogs: (id: string) => ['backtests', id, 'logs'] as const,

  portfolio: (strategyId?: string) => ['portfolio', strategyId ?? 'all'] as const,
  positions: (strategyId?: string) => ['positions', strategyId ?? 'all'] as const,
  orders: (strategyId?: string) => ['orders', strategyId ?? 'all'] as const,
  fills: (strategyId?: string) => ['fills', strategyId ?? 'all'] as const,
  strategyDiagnostics: (strategyId?: string) => ['strategy', 'diagnostics', strategyId ?? 'all'] as const,

  risk: (strategyId?: string) => ['risk', strategyId ?? 'all'] as const,
  riskEvents: (params: RiskEventsQueryParams) => ['riskEvents', params] as const,
  logs: (params: LogsQueryParams) => ['logs', params] as const,
  auditLogs: (params: AuditLogsQueryParams) => ['auditLogs', params] as const,
  marketTicks: ['market', 'ticks'] as const,
  marketKlines: (symbol: string) => ['market', 'klines', symbol] as const,
} as const;
