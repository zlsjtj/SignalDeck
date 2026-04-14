export type RiskRehearsalInput = {
  equity: number;
  pnlToday: number;
  maxDrawdown: number;
  maxDrawdownLimit: number;
  dailyLossLimit: number;
  maxLeverage: number;
  maxPositionPct: number;
  shockPct: number;
  grossExposurePct: number;
  largestPositionPct: number;
};

export type RiskRehearsalResult = {
  projectedLoss: number;
  projectedLossPct: number;
  projectedEquity: number;
  currentDailyLossPct: number;
  dailyLossAfterShock: number;
  drawdownAfterShock: number;
  leverageUsed: number;
  moveRoomBeforeStop: number | null;
  utilization: {
    drawdown: number;
    dailyLoss: number;
    leverage: number;
    concentration: number;
  };
  triggeredRules: Array<'drawdown' | 'dailyLoss' | 'leverage' | 'concentration'>;
  status: 'ok' | 'watch' | 'breach';
};

function clamp(n: number, min: number, max: number) {
  if (!Number.isFinite(n)) return min;
  return Math.min(max, Math.max(min, n));
}

function safePositive(n: number) {
  return Number.isFinite(n) ? Math.max(0, n) : 0;
}

function utilization(value: number, limit: number) {
  if (!Number.isFinite(limit) || limit <= 0) return value > 0 ? 1 : 0;
  return safePositive(value) / limit;
}

export function calculateRiskRehearsal(input: RiskRehearsalInput): RiskRehearsalResult {
  const equity = safePositive(input.equity);
  const shockPct = safePositive(input.shockPct);
  const grossExposurePct = safePositive(input.grossExposurePct);
  const largestPositionPct = safePositive(input.largestPositionPct);
  const maxDrawdown = clamp(safePositive(input.maxDrawdown), 0, 1);
  const maxDrawdownLimit = clamp(safePositive(input.maxDrawdownLimit), 0, 1);
  const dailyLossLimit = clamp(safePositive(input.dailyLossLimit), 0, 1);
  const maxLeverage = safePositive(input.maxLeverage);
  const maxPositionPct = clamp(safePositive(input.maxPositionPct), 0, 1);

  const currentDailyLossPct = equity > 0 ? clamp(Math.max(0, -input.pnlToday / equity), 0, 1) : 0;
  const leverageUsed = grossExposurePct;
  const projectedLossPct = clamp(shockPct * grossExposurePct, 0, 1);
  const projectedLoss = equity * projectedLossPct;
  const projectedEquity = Math.max(0, equity - projectedLoss);
  const dailyLossAfterShock = clamp(currentDailyLossPct + projectedLossPct, 0, 1);
  const drawdownAfterShock = clamp(maxDrawdown + projectedLossPct, 0, 1);

  const drawdownRoom = Math.max(0, maxDrawdownLimit - maxDrawdown);
  const dailyLossRoom = Math.max(0, dailyLossLimit - currentDailyLossPct);
  const moveRoomBeforeStop = grossExposurePct > 0 ? Math.min(drawdownRoom, dailyLossRoom) / grossExposurePct : null;

  const resultUtilization = {
    drawdown: utilization(drawdownAfterShock, maxDrawdownLimit),
    dailyLoss: utilization(dailyLossAfterShock, dailyLossLimit),
    leverage: utilization(leverageUsed, maxLeverage),
    concentration: utilization(largestPositionPct, maxPositionPct),
  };

  const triggeredRules: RiskRehearsalResult['triggeredRules'] = [];
  if (maxDrawdownLimit > 0 && drawdownAfterShock >= maxDrawdownLimit) triggeredRules.push('drawdown');
  if (dailyLossLimit > 0 && dailyLossAfterShock >= dailyLossLimit) triggeredRules.push('dailyLoss');
  if (maxLeverage > 0 && leverageUsed > maxLeverage) triggeredRules.push('leverage');
  if (maxPositionPct > 0 && largestPositionPct > maxPositionPct) triggeredRules.push('concentration');

  const maxUtilization = Math.max(...Object.values(resultUtilization));
  const status = triggeredRules.length > 0 ? 'breach' : maxUtilization >= 0.8 ? 'watch' : 'ok';

  return {
    projectedLoss,
    projectedLossPct,
    projectedEquity,
    currentDailyLossPct,
    dailyLossAfterShock,
    drawdownAfterShock,
    leverageUsed,
    moveRoomBeforeStop,
    utilization: resultUtilization,
    triggeredRules,
    status,
  };
}
