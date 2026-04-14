export const endpoints = {
  health: '/health',

  strategies: '/strategies',
  strategy: (id: string) => `/strategies/${id}`,
  strategyStart: (id: string) => `/strategies/${id}/start`,
  strategyStop: (id: string) => `/strategies/${id}/stop`,

  backtests: '/backtests',
  backtest: (id: string) => `/backtests/${id}`,
  backtestLogs: (id: string) => `/backtests/${id}/logs`,

  portfolio: '/portfolio',
  positions: '/positions',
  orders: '/orders',
  fills: '/fills',

  risk: '/risk',
  riskEvents: '/risk/events',
  logs: '/logs',
  auditLogs: '/audit/logs',
} as const;
