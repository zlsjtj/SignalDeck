export type ApiProfile = 'standard' | 'quant-api-server';

const rawApiProfile = (import.meta.env.VITE_API_PROFILE as string | undefined) ?? 'standard';
const apiProfile: ApiProfile = rawApiProfile === 'quant-api-server' ? 'quant-api-server' : 'standard';
const useMock = import.meta.env.VITE_USE_MOCK === 'true';
const rawMarketPollMs = Number(import.meta.env.VITE_MARKET_POLL_MS ?? 1000);
const marketPollMs = Number.isFinite(rawMarketPollMs)
  ? Math.min(10_000, Math.max(200, Math.round(rawMarketPollMs)))
  : 1000;

const wsProtocol = location.protocol === 'https:' ? 'wss' : 'ws';

export const env = {
  apiBaseUrl: (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? '/api',
  wsUrl: (import.meta.env.VITE_WS_URL as string | undefined) ?? `${wsProtocol}://${location.host}/ws`,
  marketConfigPath: (import.meta.env.VITE_MARKET_CONFIG_PATH as string | undefined) ?? 'config_market.yaml',
  apiToken: (import.meta.env.VITE_API_TOKEN as string | undefined) ?? '',
  useMock,
  apiProfile,
  marketPollMs,
  // quant-api-server profile now supports WS via /ws.
  wsEnabled: useMock || apiProfile === 'standard' || apiProfile === 'quant-api-server',
};
