import type { StrategyType } from '@/types/api';

export type StrategyRiskPreset = 'conservative' | 'balanced' | 'aggressive';

export const COMMON_SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'BCHUSDT', 'TRXUSDT'];

export function prettyJson(v: unknown) {
  try {
    return JSON.stringify(v ?? {}, null, 2);
  } catch {
    return '{}';
  }
}

export function buildAutoParams(type: StrategyType, riskPreset: StrategyRiskPreset): Record<string, number | string | boolean> {
  const risk =
    riskPreset === 'conservative'
      ? { maxPositionPct: 0.08, stopLossPct: 0.015, takeProfitPct: 0.03 }
      : riskPreset === 'aggressive'
        ? { maxPositionPct: 0.2, stopLossPct: 0.03, takeProfitPct: 0.06 }
        : { maxPositionPct: 0.12, stopLossPct: 0.02, takeProfitPct: 0.04 };

  if (type === 'mean_reversion') {
    return {
      lookback: 24,
      entryZ: riskPreset === 'conservative' ? 2.2 : riskPreset === 'aggressive' ? 1.4 : 1.8,
      exitZ: 0.4,
      rebalanceMinutes: 30,
      ...risk,
    };
  }
  if (type === 'trend_following') {
    return {
      lookback: 55,
      breakoutPct: riskPreset === 'conservative' ? 0.015 : riskPreset === 'aggressive' ? 0.007 : 0.01,
      trailingStopPct: riskPreset === 'conservative' ? 0.02 : riskPreset === 'aggressive' ? 0.035 : 0.028,
      rebalanceMinutes: 15,
      ...risk,
    };
  }
  if (type === 'market_making') {
    return {
      spreadBps: riskPreset === 'conservative' ? 22 : riskPreset === 'aggressive' ? 8 : 14,
      inventoryTarget: 0,
      maxInventoryPct: risk.maxPositionPct,
      quoteRefreshSeconds: 8,
      ...risk,
    };
  }
  return {
    lookback: 20,
    rebalanceMinutes: 30,
    useStopLoss: true,
    ...risk,
  };
}

export function normalizeSymbols(symbols: string[]) {
  return Array.from(
    new Set(
      symbols
        .map((item) => String(item ?? '').trim().toUpperCase())
        .filter((item) => item.length > 0),
    ),
  );
}
