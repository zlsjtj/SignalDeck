import { describe, expect, it } from 'vitest';

import { calculateRiskRehearsal } from './riskRehearsal';

const baseInput = {
  equity: 10_000,
  pnlToday: 100,
  maxDrawdown: 0.04,
  maxDrawdownLimit: 0.2,
  dailyLossLimit: 0.08,
  maxLeverage: 2,
  maxPositionPct: 0.4,
  shockPct: 0.03,
  grossExposurePct: 1.2,
  largestPositionPct: 0.25,
};

describe('calculateRiskRehearsal', () => {
  it('projects adverse move loss from equity and gross exposure', () => {
    const result = calculateRiskRehearsal(baseInput);

    expect(result.projectedLoss).toBeCloseTo(360);
    expect(result.projectedLossPct).toBeCloseTo(0.036);
    expect(result.projectedEquity).toBeCloseTo(9640);
    expect(result.status).toBe('ok');
    expect(result.triggeredRules).toEqual([]);
  });

  it('flags drawdown and daily loss breaches', () => {
    const result = calculateRiskRehearsal({
      ...baseInput,
      maxDrawdown: 0.16,
      shockPct: 0.06,
      grossExposurePct: 1,
    });

    expect(result.drawdownAfterShock).toBeCloseTo(0.22);
    expect(result.dailyLossAfterShock).toBeCloseTo(0.06);
    expect(result.triggeredRules).toContain('drawdown');
    expect(result.status).toBe('breach');
  });

  it('includes existing daily loss before testing the shock', () => {
    const result = calculateRiskRehearsal({
      ...baseInput,
      pnlToday: -500,
      shockPct: 0.04,
      grossExposurePct: 1,
    });

    expect(result.currentDailyLossPct).toBeCloseTo(0.05);
    expect(result.dailyLossAfterShock).toBeCloseTo(0.09);
    expect(result.triggeredRules).toContain('dailyLoss');
  });

  it('flags leverage and concentration independently', () => {
    const result = calculateRiskRehearsal({
      ...baseInput,
      grossExposurePct: 2.4,
      largestPositionPct: 0.55,
    });

    expect(result.triggeredRules).toContain('leverage');
    expect(result.triggeredRules).toContain('concentration');
  });
});
