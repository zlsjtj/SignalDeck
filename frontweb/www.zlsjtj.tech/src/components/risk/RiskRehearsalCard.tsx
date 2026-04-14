import { useMemo, useState } from 'react';
import { Card, Col, Empty, InputNumber, Progress, Row, Slider, Space, Statistic, Tag, Typography } from 'antd';

import { byLang } from '@/i18n';
import type { Portfolio, UpdateRiskRequest } from '@/types/api';
import { formatNumber, formatPercent } from '@/utils/format';
import { calculateRiskRehearsal } from '@/utils/riskRehearsal';

type RiskRehearsalCardProps = {
  portfolio?: Portfolio;
  riskParams?: UpdateRiskRequest;
  loading?: boolean;
};

const ruleLabel: Record<string, { zh: string; en: string }> = {
  drawdown: { zh: '回撤', en: 'Drawdown' },
  dailyLoss: { zh: '单日亏损', en: 'Daily loss' },
  leverage: { zh: '杠杆', en: 'Leverage' },
  concentration: { zh: '集中度', en: 'Concentration' },
};

function percentControl(value: number, onChange: (next: number) => void, max: number) {
  return (
    <Space.Compact style={{ width: '100%' }}>
      <Slider
        min={0}
        max={max}
        step={1}
        value={value}
        onChange={onChange}
        style={{ flex: 1, marginInlineEnd: 12 }}
        tooltip={{ formatter: (v) => `${v ?? 0}%` }}
      />
      <InputNumber
        min={0}
        max={max}
        step={1}
        value={value}
        onChange={(next) => onChange(Number(next ?? 0))}
        addonAfter="%"
        style={{ width: 120 }}
      />
    </Space.Compact>
  );
}

function statusTag(status: 'ok' | 'watch' | 'breach') {
  if (status === 'breach') return <Tag color="red">{byLang('触线', 'Breach')}</Tag>;
  if (status === 'watch') return <Tag color="gold">{byLang('接近阈值', 'Watch')}</Tag>;
  return <Tag color="green">{byLang('未触线', 'Clear')}</Tag>;
}

export function RiskRehearsalCard({ portfolio, riskParams, loading }: RiskRehearsalCardProps) {
  const [shockPct, setShockPct] = useState(8);
  const [grossExposurePct, setGrossExposurePct] = useState(120);
  const [largestPositionPct, setLargestPositionPct] = useState(35);

  const result = useMemo(() => {
    if (!portfolio || !riskParams || portfolio.equity <= 0) return undefined;
    return calculateRiskRehearsal({
      equity: portfolio.equity,
      pnlToday: portfolio.pnlToday,
      maxDrawdown: portfolio.maxDrawdown,
      maxDrawdownLimit: riskParams.maxDrawdownPct,
      dailyLossLimit: riskParams.dailyLossLimitPct,
      maxLeverage: riskParams.maxLeverage,
      maxPositionPct: riskParams.maxPositionPct,
      shockPct: shockPct / 100,
      grossExposurePct: grossExposurePct / 100,
      largestPositionPct: largestPositionPct / 100,
    });
  }, [grossExposurePct, largestPositionPct, portfolio, riskParams, shockPct]);

  return (
    <Card
      loading={loading}
      title={byLang('风险预演', 'Risk rehearsal')}
      extra={result ? statusTag(result.status) : null}
    >
      {!result ? (
        <Empty description={byLang('暂无组合数据，无法预演。', 'No portfolio data to rehearse.')} />
      ) : (
        <Space direction="vertical" size={16} style={{ width: '100%' }}>
          <Typography.Text type="secondary">
            {byLang(
              '调一个反向行情，先看会不会碰到回撤、单日亏损、杠杆或集中度阈值。这里不保存，也不会下单。',
              'Move the scenario knobs before changing live limits. Nothing is saved or sent to execution.',
            )}
          </Typography.Text>

          <Row gutter={[16, 16]}>
            <Col xs={24} lg={10}>
              <Space direction="vertical" size={14} style={{ width: '100%' }}>
                <div>
                  <Typography.Text strong>{byLang('反向波动', 'Adverse move')}</Typography.Text>
                  {percentControl(shockPct, setShockPct, 40)}
                </div>
                <div>
                  <Typography.Text strong>{byLang('总敞口 / 权益', 'Gross exposure / equity')}</Typography.Text>
                  {percentControl(grossExposurePct, setGrossExposurePct, 400)}
                </div>
                <div>
                  <Typography.Text strong>{byLang('最大单仓 / 权益', 'Largest position / equity')}</Typography.Text>
                  {percentControl(largestPositionPct, setLargestPositionPct, 100)}
                </div>
              </Space>
            </Col>

            <Col xs={24} lg={14}>
              <Row gutter={[12, 12]}>
                <Col xs={24} sm={12}>
                  <Statistic title={byLang('预估亏损', 'Projected loss')} value={result.projectedLoss} precision={2} prefix="-" />
                </Col>
                <Col xs={24} sm={12}>
                  <Statistic title={byLang('冲击后权益', 'Equity after shock')} value={result.projectedEquity} precision={2} />
                </Col>
                <Col xs={24} sm={12}>
                  <Statistic title={byLang('止损空间', 'Move room')} value={result.moveRoomBeforeStop === null ? '-' : formatPercent(result.moveRoomBeforeStop)} />
                </Col>
                <Col xs={24} sm={12}>
                  <Statistic title={byLang('预估亏损率', 'Loss / equity')} value={formatPercent(result.projectedLossPct)} />
                </Col>
              </Row>

              <div style={{ marginTop: 16 }}>
                {[
                  [byLang('回撤', 'Drawdown'), result.utilization.drawdown],
                  [byLang('单日亏损', 'Daily loss'), result.utilization.dailyLoss],
                  [byLang('杠杆', 'Leverage'), result.utilization.leverage],
                  [byLang('集中度', 'Concentration'), result.utilization.concentration],
                ].map(([label, value]) => {
                  const percent = Math.round(Math.min(1.5, Number(value)) * 100);
                  const color = percent >= 100 ? '#ff4d4f' : percent >= 80 ? '#faad14' : '#52c41a';
                  return (
                    <div key={label} style={{ marginBottom: 8 }}>
                      <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                        <Typography.Text>{label}</Typography.Text>
                        <Typography.Text type="secondary">{formatNumber(Number(value) * 100, 0)}%</Typography.Text>
                      </Space>
                      <Progress percent={percent} showInfo={false} strokeColor={color} />
                    </div>
                  );
                })}
              </div>
            </Col>
          </Row>

          <Typography.Text type={result.triggeredRules.length > 0 ? 'danger' : 'secondary'}>
            {result.triggeredRules.length > 0
              ? byLang(
                  `会触发: ${result.triggeredRules.map((rule) => ruleLabel[rule].zh).join('、')}`,
                  `Would trip: ${result.triggeredRules.map((rule) => ruleLabel[rule].en).join(', ')}`,
                )
              : byLang('该情景未触发当前阈值。', 'This scenario stays inside the current limits.')}
          </Typography.Text>
        </Space>
      )}
    </Card>
  );
}
