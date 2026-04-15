import { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Empty, Grid, Progress, Row, Segmented, Select, Space, Statistic, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import ReactECharts from 'echarts-for-react';

import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useMarketIntelSummaryQuery } from '@/hooks/queries/market';
import { byLang } from '@/i18n';
import type {
  MarketIntelLevel,
  MarketIntelLiquidation,
  MarketIntelLiquidationAggregate,
  MarketIntelRollingCorrelation,
  MarketIntelSessionHeatmapCell,
  MarketIntelStreamStatus,
  MarketIntelVenue,
  MarketIntelVenueSnapshot,
} from '@/types/api';
import { formatNumber, formatPercent, formatTs } from '@/utils/format';

type StreamWindowSeconds = 300 | 900 | 3600;

type MiniChartPoint = {
  ts: string;
  value: number;
  samples: number;
};

type PressureMetric = 'book' | 'flow' | 'ofi';
type SessionTimezone = 'utc' | 'asia-shanghai';

const PRESSURE_THRESHOLD = 0.15;
const WEEKDAY_LABELS_ZH = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'];
const WEEKDAY_LABELS_EN = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];

function compactSymbol(symbol: string) {
  return symbol.replace('/USDT:USDT', '').replace('/USDT', '').replace('USDT', '');
}

function compactPair(left: string, right: string) {
  return `${compactSymbol(left)}-${compactSymbol(right)}`;
}

function signedPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${formatPercent(value, digits)}`;
}

function barColor(value: number) {
  if (value >= PRESSURE_THRESHOLD) return '#ff4d4f';
  if (value <= -PRESSURE_THRESHOLD) return '#1677ff';
  return '#52c41a';
}

function pressureText(value?: number | null) {
  const v = value ?? 0;
  if (v >= PRESSURE_THRESHOLD) return byLang('买方偏强', 'Buy pressure');
  if (v <= -PRESSURE_THRESHOLD) return byLang('卖方偏强', 'Sell pressure');
  return byLang('相对均衡', 'Balanced');
}

function pressureTagColor(value?: number | null) {
  const v = value ?? 0;
  if (v >= PRESSURE_THRESHOLD) return 'red';
  if (v <= -PRESSURE_THRESHOLD) return 'blue';
  return 'green';
}

function coverageText(seconds?: number, targetSeconds?: number) {
  if (!seconds || seconds <= 0) return byLang('等待实时样本', 'Waiting for live samples');
  const text = seconds < 60
    ? byLang(`约 ${seconds} 秒样本`, `About ${seconds}s covered`)
    : byLang(`约 ${Math.round(seconds / 60)} 分钟样本`, `About ${Math.round(seconds / 60)}m covered`);
  if (targetSeconds && seconds < Math.min(targetSeconds, 120)) {
    return byLang(`${text}，样本仍在积累`, `${text}; samples are still accumulating`);
  }
  return text;
}

function metricExplanation(metric: PressureMetric) {
  if (metric === 'book') {
    return byLang(
      'Book skew 比较当前 Level 2 买卖盘名义额，正值表示买盘更厚，负值表示卖盘更厚。',
      'Book skew compares Level 2 bid and ask notional; positive means thicker bids, negative means thicker asks.',
    );
  }
  if (metric === 'flow') {
    return byLang(
      'Flow skew 比较近期主动买入和主动卖出名义额，正值表示主动买入占优。',
      'Flow skew compares recent taker-buy and taker-sell notional; positive means taker buying dominates.',
    );
  }
  return byLang(
    'OFI 衡量订单薄更新带来的短窗订单流压力，正值偏买方，负值偏卖方。',
    'OFI measures short-window order-flow pressure from book updates; positive leans buy-side and negative leans sell-side.',
  );
}

function metricLabel(metric: PressureMetric) {
  if (metric === 'book') return byLang('订单薄', 'Book');
  if (metric === 'flow') return byLang('成交流', 'Flow');
  return byLang('实时 OFI', 'Live OFI');
}

function displayHour(hourUtc: number, timezone: SessionTimezone) {
  return timezone === 'asia-shanghai' ? (hourUtc + 8) % 24 : hourUtc;
}

function displayWeekday(weekdayUtc: number, hourUtc: number, timezone: SessionTimezone) {
  if (timezone === 'utc') return weekdayUtc;
  return (weekdayUtc + Math.floor((hourUtc + 8) / 24)) % 7;
}

function timezoneLabel(timezone: SessionTimezone) {
  return timezone === 'asia-shanghai' ? 'Asia/Shanghai' : 'UTC';
}

function MiniStreamChart({
  title,
  data,
  color,
  baseline,
  emptyText,
  min,
  max,
}: {
  title: string;
  data: MiniChartPoint[];
  color: string;
  baseline: number;
  emptyText: string;
  min?: number;
  max?: number;
}) {
  const option = useMemo(() => {
    const xs = data.map((point) => point.ts);
    const ys = data.map((point) => point.value);
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const point = Array.isArray(params) ? params[0] : params;
          const value = typeof point?.data === 'number' ? formatPercent(point.data) : '-';
          const source = data.find((item) => item.ts === point?.axisValue);
          const samples = source?.samples ?? 0;
          return `${formatTs(String(point?.axisValue ?? ''))}<br/>${title}: ${value}<br/>${byLang('样本', 'Samples')}: ${samples}`;
        },
      },
      grid: { left: 42, right: 12, top: 10, bottom: 28 },
      xAxis: {
        type: 'category',
        data: xs,
        axisLabel: {
          color: 'rgba(215,226,240,0.72)',
          formatter: (v: string) => formatTs(v, 'HH:mm'),
        },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        min,
        max,
        axisLabel: {
          color: 'rgba(215,226,240,0.72)',
          formatter: (v: number) => `${(v * 100).toFixed(0)}%`,
        },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      series: [
        {
          type: 'line',
          data: ys,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2, color },
          areaStyle: { opacity: 0.12, color },
          markLine: {
            symbol: 'none',
            label: { show: false },
            lineStyle: { color: 'rgba(215,226,240,0.34)', type: 'dashed' },
            data: [{ yAxis: baseline }],
          },
        },
      ],
    };
  }, [baseline, color, data, max, min, title]);

  return (
    <div style={{ minHeight: 172 }}>
      <Typography.Text strong>{title}</Typography.Text>
      {data.length === 0 ? (
        <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description={emptyText} />
      ) : (
        <ReactECharts option={option} style={{ height: 146 }} notMerge lazyUpdate />
      )}
    </div>
  );
}

function PressureSummary({
  values,
  streamWindowSeconds,
}: {
  values: Record<PressureMetric, number | undefined | null>;
  streamWindowSeconds: StreamWindowSeconds;
}) {
  return (
    <Space direction="vertical" size={6} style={{ width: '100%' }}>
      <Typography.Text type="secondary">
        {byLang(
          '颜色阈值：高于 +15% 标记为买方压力偏强，低于 -15% 标记为卖方压力偏强；中间区间仅表示短窗内相对均衡。',
          'Color thresholds: above +15% marks stronger buy-side pressure, below -15% marks stronger sell-side pressure; the middle band only means the short window is relatively balanced.',
        )}
      </Typography.Text>
      <Space wrap>
        {(['book', 'flow', 'ofi'] as PressureMetric[]).map((metric) => (
          <Tag key={metric} color={pressureTagColor(values[metric])}>
            {metricLabel(metric)}: {pressureText(values[metric])}
          </Tag>
        ))}
        <Typography.Text type="secondary">
          {byLang('窗口', 'Window')}: {streamWindowSeconds / 60}m
        </Typography.Text>
      </Space>
      <Space direction="vertical" size={2}>
        {(['book', 'flow', 'ofi'] as PressureMetric[]).map((metric) => (
          <Typography.Text key={metric} type="secondary">
            {metricLabel(metric)}: {metricExplanation(metric)}
          </Typography.Text>
        ))}
      </Space>
    </Space>
  );
}

function VenueCard({
  venue,
  isPrimary,
  streamWindowSeconds,
}: {
  venue?: MarketIntelVenueSnapshot;
  isPrimary: boolean;
  streamWindowSeconds: StreamWindowSeconds;
}) {
  if (!venue) return null;
  const ob = venue.orderbook;
  const flow = venue.flow;
  const deriv = venue.derivatives;
  const stream = venue.stream;
  const ofiSeries = (stream?.ofi?.series ?? []).map((point) => ({
    ts: point.ts,
    value: point.ofiNorm,
    samples: point.samples,
  }));
  const takerFlowSeries = (stream?.flow?.series ?? []).map((point) => ({
    ts: point.ts,
    value: point.takerBuyRatio,
    samples: point.samples,
  }));
  const coverageSeconds = Math.max(stream?.ofi?.availableSeconds ?? 0, stream?.flow?.availableSeconds ?? 0);
  return (
    <Card
      style={isPrimary ? { borderColor: '#ff4d4f' } : undefined}
      title={
        <Space wrap>
          <Typography.Text strong>{venue.venue === 'spot' ? byLang('现货', 'Spot') : byLang('合约', 'Futures')}</Typography.Text>
          <Tag>{compactSymbol(venue.symbol)}</Tag>
          {isPrimary ? <Tag color="red">{byLang('主视角', 'Primary view')}</Tag> : <Tag>{byLang('辅助视角', 'Secondary view')}</Tag>}
        </Space>
      }
      extra={<Typography.Text type="secondary">{coverageText(coverageSeconds, streamWindowSeconds)}</Typography.Text>}
    >
      {!venue.ok ? (
        <Alert type="warning" showIcon message={venue.error || byLang('数据源暂不可用', 'Source unavailable')} />
      ) : null}
      <Row gutter={[12, 12]}>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('中间价', 'Mid')} value={ob?.mid ?? 0} precision={2} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('价差', 'Spread')} value={formatPercent(ob?.spreadPct ?? 0, 3)} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('订单薄偏斜', 'Book skew')} value={signedPercent(ob?.imbalance ?? 0)} valueStyle={{ color: barColor(ob?.imbalance ?? 0) }} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('主动买入', 'Taker buy')} value={formatPercent(flow?.takerBuyRatio ?? 0)} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('成交流偏斜', 'Flow skew')} value={signedPercent(flow?.tradeImbalance ?? 0)} valueStyle={{ color: barColor(flow?.tradeImbalance ?? 0) }} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('量比', 'Volume ratio')} value={`${formatNumber(venue.volumeRatio, 2)}x`} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('实时 OFI', 'Live OFI')} value={signedPercent(stream?.ofi?.ofiNorm ?? 0)} valueStyle={{ color: barColor(stream?.ofi?.ofiNorm ?? 0) }} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('实时主动买入', 'Live taker buy')} value={formatPercent(stream?.flow?.takerBuyRatio ?? 0)} />
        </Col>
        <Col xs={12} lg={8}>
          <Statistic title={byLang('实时样本', 'Live samples')} value={(stream?.ofi?.samples ?? 0) + (stream?.flow?.samples ?? 0)} />
        </Col>
        {venue.venue === 'futures' ? (
          <>
            <Col xs={12} lg={8}>
              <Statistic title={byLang('资金费率', 'Funding')} value={deriv?.fundingRate == null ? '-' : formatPercent(deriv.fundingRate, 4)} />
            </Col>
            <Col xs={12} lg={8}>
              <Statistic title={byLang('持仓量变化', 'OI change')} value={deriv?.openInterestChangePct == null ? '-' : signedPercent(deriv.openInterestChangePct)} />
            </Col>
            <Col xs={12} lg={8}>
              <Statistic title={byLang('周期主动买入', 'Period taker buy')} value={deriv?.periodTakerBuyRatio == null ? '-' : formatPercent(deriv.periodTakerBuyRatio)} />
            </Col>
          </>
        ) : null}
      </Row>
      <div style={{ marginTop: 12 }}>
        <Progress
          percent={Math.round(((ob?.imbalance ?? 0) + 1) * 50)}
          showInfo={false}
          strokeColor={barColor(ob?.imbalance ?? 0)}
        />
        <PressureSummary
          values={{
            book: ob?.imbalance,
            flow: flow?.tradeImbalance,
            ofi: stream?.ofi?.ofiNorm,
          }}
          streamWindowSeconds={streamWindowSeconds}
        />
      </div>
      <Row gutter={[12, 12]} style={{ marginTop: 12 }}>
        <Col xs={24} md={12}>
          <MiniStreamChart
            title={byLang('实时 OFI 序列', 'Live OFI series')}
            data={ofiSeries}
            color={barColor(stream?.ofi?.ofiNorm ?? 0)}
            baseline={0}
            emptyText={byLang(
              '等待订单薄实时更新样本；刚启动或断线重连后短时间为空是正常状态。',
              'Waiting for live book-update samples; an empty chart is normal right after startup or reconnect.',
            )}
          />
        </Col>
        <Col xs={24} md={12}>
          <MiniStreamChart
            title={byLang('实时主动流序列', 'Live taker flow series')}
            data={takerFlowSeries}
            color={(stream?.flow?.takerBuyRatio ?? 0.5) >= 0.5 ? '#ff4d4f' : '#1677ff'}
            baseline={0.5}
            min={0}
            max={1}
            emptyText={byLang(
              '等待实时成交样本；无样本表示当前窗口还没有可聚合的主动成交流。',
              'Waiting for live trade samples; no samples means the current window has no taker-flow aggregation yet.',
            )}
          />
        </Col>
      </Row>
      <div style={{ marginTop: 6 }}>
        <Typography.Text type="secondary">
          {byLang(
            '这些指标用于监测短窗供需压力，不能单独作为交易建议。',
            'These indicators monitor short-window pressure and are not standalone trading advice.',
          )}
        </Typography.Text>
      </div>
    </Card>
  );
}

function OrderbookTable({ venue }: { venue?: MarketIntelVenueSnapshot }) {
  const ob = venue?.orderbook;
  const rows = useMemo(() => {
    const bids = ob?.bids ?? [];
    const asks = ob?.asks ?? [];
    const max = Math.max(bids.length, asks.length);
    return Array.from({ length: Math.min(max, 12) }, (_, idx) => ({
      key: idx,
      bid: bids[idx],
      ask: asks[idx],
    }));
  }, [ob]);

  const columns: ColumnsType<{ key: number; bid?: MarketIntelLevel; ask?: MarketIntelLevel }> = [
    {
      title: byLang('买盘', 'Bid'),
      dataIndex: 'bid',
      render: (level?: MarketIntelLevel) => level ? `${formatNumber(level.price, 2)} / ${formatNumber(level.qty, 4)}` : '-',
    },
    {
      title: byLang('买盘名义额', 'Bid notional'),
      dataIndex: 'bid',
      responsive: ['md'],
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.notional, 0) : '-',
    },
    {
      title: byLang('卖盘', 'Ask'),
      dataIndex: 'ask',
      render: (level?: MarketIntelLevel) => level ? `${formatNumber(level.price, 2)} / ${formatNumber(level.qty, 4)}` : '-',
    },
    {
      title: byLang('卖盘名义额', 'Ask notional'),
      dataIndex: 'ask',
      responsive: ['md'],
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.notional, 0) : '-',
    },
  ];

  return (
    <Card
      title={
        <Space wrap>
          <Typography.Text>{byLang('主视角 Level 2 订单薄', 'Primary Level 2 order book')}</Typography.Text>
          {venue ? <Tag>{venue.venue === 'spot' ? 'Spot' : 'Futures'}</Tag> : null}
        </Space>
      }
    >
      {rows.length === 0 ? (
        <Empty description={byLang('主视角暂无订单薄快照', 'No primary-view book snapshot')} />
      ) : (
        <Table size="small" pagination={false} columns={columns} dataSource={rows} />
      )}
    </Card>
  );
}

function CorrelationHeatmap({ data }: { data?: Array<{ symbol: string; values: Record<string, number | null> }> }) {
  const rows = data ?? [];
  const symbols = rows.map((row) => row.symbol);
  return (
    <Card title={byLang('跨资产相关', 'Cross-asset correlation')}>
      {rows.length === 0 ? (
        <Empty description={byLang('暂无相关性数据', 'No correlation data')} />
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', minWidth: 560 }}>
            <thead>
              <tr>
                <th style={{ textAlign: 'left', padding: 6 }}>{byLang('标的', 'Symbol')}</th>
                {symbols.map((symbol) => (
                  <th key={symbol} style={{ textAlign: 'center', padding: 6 }}>{compactSymbol(symbol)}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.symbol}>
                  <td style={{ padding: 6, fontWeight: 600 }}>{compactSymbol(row.symbol)}</td>
                  {symbols.map((symbol) => {
                    const value = row.values[symbol];
                    const alpha = value == null ? 0 : Math.min(0.86, Math.abs(value) * 0.72 + 0.08);
                    const bg = value == null ? 'transparent' : value >= 0 ? `rgba(255, 77, 79, ${alpha})` : `rgba(22, 119, 255, ${alpha})`;
                    return (
                      <td key={symbol} style={{ padding: 6, textAlign: 'center', background: bg }}>
                        {value == null ? '-' : value.toFixed(2)}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}

function RollingCorrelationPanel({
  rows,
  breaks,
}: {
  rows?: MarketIntelRollingCorrelation[];
  breaks?: Array<{
    pair: string;
    left: string;
    right: string;
    current: number;
    recentMean: number;
    priorHigh: number;
    severity: string;
    reason: string;
    message: string;
  }>;
}) {
  const seriesRows = useMemo(() => rows ?? [], [rows]);
  const option = useMemo(() => {
    const timestamps = Array.from(new Set(seriesRows.flatMap((row) => row.points.map((point) => point.ts)))).sort();
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const items = Array.isArray(params) ? params : [params];
          const lines = items.map((item) => `${item.seriesName}: ${typeof item.data === 'number' ? item.data.toFixed(2) : '-'}`);
          return `${formatTs(String(items[0]?.axisValue ?? ''))}<br/>${lines.join('<br/>')}`;
        },
      },
      legend: { top: 0, textStyle: { color: 'rgba(215,226,240,0.72)' } },
      grid: { left: 42, right: 16, top: 36, bottom: 28 },
      xAxis: {
        type: 'category',
        data: timestamps,
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: string) => formatTs(v, 'MM-DD HH:mm') },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        min: -1,
        max: 1,
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: number) => v.toFixed(1) },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      series: seriesRows.map((row) => {
        const values = new Map(row.points.map((point) => [point.ts, point.correlation]));
        return {
          name: compactPair(row.left, row.right),
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: timestamps.map((ts) => values.get(ts) ?? null),
        };
      }),
    };
  }, [seriesRows]);

  return (
    <Card title={byLang('滚动相关结构', 'Rolling correlation structure')}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Typography.Text type="secondary">
          {byLang(
            '滚动相关用于监测跨资产联动结构变化，不构成交易建议。',
            'Rolling correlation monitors cross-asset structure changes and is not trading advice.',
          )}
        </Typography.Text>
        {(breaks ?? []).length > 0 ? (
          <Space direction="vertical" size={8} style={{ width: '100%' }}>
            {(breaks ?? []).map((item) => (
              <Alert
                key={`${item.pair}-${item.reason}`}
                type={item.severity === 'warning' ? 'warning' : 'info'}
                showIcon
                message={`${compactPair(item.left, item.right)} ${byLang('相关性变化提示', 'correlation change monitor')}`}
                description={byLang(
                  `当前 ${item.current.toFixed(2)}，近期均值 ${item.recentMean.toFixed(2)}，前期高点 ${item.priorHigh.toFixed(2)}。这只是结构变化监测，不是交易信号。`,
                  `Current ${item.current.toFixed(2)}, recent mean ${item.recentMean.toFixed(2)}, prior high ${item.priorHigh.toFixed(2)}. This is a structure-change monitor, not a trading signal.`,
                )}
              />
            ))}
          </Space>
        ) : null}
        {seriesRows.length === 0 ? (
          <Empty description={byLang('暂无足够 K 线生成滚动相关', 'Not enough kline data for rolling correlation')} />
        ) : (
          <ReactECharts option={option} style={{ height: 260 }} notMerge lazyUpdate />
        )}
      </Space>
    </Card>
  );
}

function SessionHeatmap({
  rows,
  timezone,
}: {
  rows?: MarketIntelSessionHeatmapCell[];
  timezone: SessionTimezone;
}) {
  const cells = useMemo(() => rows ?? [], [rows]);
  const weekdayLabels = WEEKDAY_LABELS_ZH.map((label, idx) => byLang(label, WEEKDAY_LABELS_EN[idx]));
  const option = useMemo(() => {
    const keyed = cells.map((cell) => ({
      ...cell,
      hour: displayHour(cell.hourUtc, timezone),
      weekday: displayWeekday(cell.weekdayUtc, cell.hourUtc, timezone),
    }));
    const maxAbs = Math.max(0.001, ...keyed.map((cell) => Math.abs(cell.avgReturnPct)));
    return {
      backgroundColor: 'transparent',
      tooltip: {
        formatter: (params: any) => {
          const cell = keyed[params.dataIndex];
          if (!cell) return '';
          return [
            `${weekdayLabels[cell.weekday]} ${String(cell.hour).padStart(2, '0')}:00 ${timezoneLabel(timezone)}`,
            `${byLang('均值收益', 'Avg return')}: ${signedPercent(cell.avgReturnPct, 3)}`,
            `${byLang('均量', 'Avg volume')}: ${formatNumber(cell.avgVolume, 2)}`,
            `${byLang('样本', 'Bars')}: ${cell.count}`,
          ].join('<br/>');
        },
      },
      grid: { left: 46, right: 16, top: 12, bottom: 38 },
      xAxis: {
        type: 'category',
        data: Array.from({ length: 24 }, (_, hour) => `${String(hour).padStart(2, '0')}:00`),
        axisLabel: { color: 'rgba(215,226,240,0.72)' },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'category',
        data: weekdayLabels,
        axisLabel: { color: 'rgba(215,226,240,0.72)' },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
        axisTick: { show: false },
      },
      visualMap: {
        min: -maxAbs,
        max: maxAbs,
        show: false,
        inRange: { color: ['#1677ff', '#2f3b45', '#ff4d4f'] },
      },
      series: [
        {
          type: 'heatmap',
          data: keyed.map((cell) => [cell.hour, cell.weekday, cell.avgReturnPct]),
          label: { show: false },
          emphasis: { itemStyle: { borderColor: '#fff', borderWidth: 1 } },
        },
      ],
    };
  }, [cells, timezone, weekdayLabels]);

  return (
    <Card title={byLang('weekday + hour 热力图', 'Weekday + hour heatmap')}>
      {cells.length < 12 ? (
        <Empty description={byLang('样本不足，暂不展示时间段热力图', 'Not enough samples for the session heatmap yet')} />
      ) : (
        <ReactECharts option={option} style={{ height: 260 }} notMerge lazyUpdate />
      )}
    </Card>
  );
}

function StreamObservability({ stream }: { stream?: MarketIntelStreamStatus }) {
  const connectionRows = Object.entries(stream?.connections ?? {}).map(([venue, conn]) => ({
    key: venue,
    venue,
    ...conn,
  }));
  const errorRows = (stream?.errors ?? []).slice(0, 6).map((row, idx) => ({ ...row, key: `${row.ts}-${idx}` }));

  return (
    <Card title={byLang('采集器状态', 'Collector status')}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Row gutter={[12, 12]}>
          <Col xs={12} md={6}>
            <Statistic title={byLang('实时流', 'Stream')} value={stream?.status ?? 'stopped'} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('订阅流数量', 'Subscribed streams')} value={connectionRows.reduce((sum, row) => sum + (row.streams ?? 0), 0)} />
          </Col>
          <Col xs={24} md={6}>
            <Statistic title={byLang('启动时间', 'Started')} value={stream?.startedAt ? formatTs(stream.startedAt) : '-'} />
          </Col>
          <Col xs={24} md={6}>
            <Statistic title={byLang('最近更新', 'Updated')} value={stream?.updatedAt ? formatTs(stream.updatedAt) : '-'} />
          </Col>
        </Row>
        <Table
          size="small"
          pagination={false}
          dataSource={connectionRows}
          columns={[
            { title: byLang('市场', 'Venue'), dataIndex: 'venue', render: (v: string) => v === 'spot' ? 'Spot' : 'Futures' },
            {
              title: byLang('连接状态', 'Connection'),
              dataIndex: 'status',
              render: (v: string) => <Tag color={v === 'open' ? 'green' : v === 'error' ? 'red' : 'default'}>{v}</Tag>,
            },
            { title: byLang('流数量', 'Streams'), dataIndex: 'streams' },
            { title: byLang('更新时间', 'Updated'), dataIndex: 'updatedAt', render: (v: string) => v ? formatTs(v) : '-' },
            { title: byLang('错误', 'Error'), dataIndex: 'error', render: (v: string) => v || '-' },
          ]}
        />
        {errorRows.length > 0 ? (
          <Table
            size="small"
            pagination={false}
            dataSource={errorRows}
            columns={[
              { title: byLang('时间', 'Time'), dataIndex: 'ts', render: (v: string) => formatTs(v) },
              { title: byLang('市场', 'Venue'), dataIndex: 'venue', render: (v: string) => v || '-' },
              { title: byLang('最近错误', 'Recent error'), dataIndex: 'message' },
            ]}
          />
        ) : (
          <Typography.Text type="secondary">
            {byLang('暂无采集器错误。', 'No collector errors.')}
          </Typography.Text>
        )}
      </Space>
    </Card>
  );
}

function SessionEffectTable({ venue, timezone }: { venue?: MarketIntelVenueSnapshot; timezone: SessionTimezone }) {
  const rows = venue?.sessionEffect;
  const topRows = [...(rows ?? [])]
    .sort((a, b) => Math.abs(b.avgReturnPct) - Math.abs(a.avgReturnPct))
    .slice(0, 8)
    .sort((a, b) => a.hourUtc - b.hourUtc);
  return (
    <Card
      title={
        <Space wrap>
          <Typography.Text>{byLang('主视角时间段效应', 'Primary session effect')}</Typography.Text>
          {venue ? <Tag>{venue.venue === 'spot' ? 'Spot' : 'Futures'}</Tag> : null}
          <Tag>{timezoneLabel(timezone)}</Tag>
        </Space>
      }
    >
      {topRows.length === 0 ? (
        <Empty description={byLang('主视角暂无分时统计', 'No primary-view session stats')} />
      ) : (
        <Table
          size="small"
          pagination={false}
          dataSource={topRows.map((row) => ({ ...row, key: row.hourUtc }))}
          columns={[
            { title: byLang('小时', 'Hour'), dataIndex: 'hourUtc', render: (v: number) => `${String(displayHour(v, timezone)).padStart(2, '0')}:00` },
            { title: byLang('均值收益', 'Avg return'), dataIndex: 'avgReturnPct', render: (v: number) => signedPercent(v, 3) },
            { title: byLang('均量', 'Avg volume'), dataIndex: 'avgVolume', render: (v: number) => formatNumber(v, 2) },
            { title: byLang('样本', 'Bars'), dataIndex: 'count' },
          ]}
        />
      )}
    </Card>
  );
}

function liquidationSideText(side?: string) {
  const normalized = String(side ?? '').toUpperCase();
  if (normalized === 'SELL') return byLang('多头爆仓', 'Long liquidation');
  if (normalized === 'BUY') return byLang('空头爆仓', 'Short liquidation');
  return byLang('未知方向', 'Unknown side');
}

function LiquidationPanel({
  rows,
  status,
  apiAggregate,
}: {
  rows: MarketIntelLiquidation[];
  status?: string;
  apiAggregate?: MarketIntelLiquidationAggregate;
}) {
  const fallbackAggregate = useMemo(() => {
    const byDirection = {
      long: { count: 0, notional: 0 },
      short: { count: 0, notional: 0 },
      unknown: { count: 0, notional: 0 },
    };
    let maxEvent: MarketIntelLiquidation | null = null;
    const now = Date.now();
    const last5m = {
      byDirection: {
        long: { count: 0, notional: 0 },
        short: { count: 0, notional: 0 },
        unknown: { count: 0, notional: 0 },
      },
      totalNotional: 0,
      count: 0,
    };
    for (const row of rows) {
      const side = String(row.side ?? '').toUpperCase();
      const direction = side === 'SELL' ? 'long' : side === 'BUY' ? 'short' : 'unknown';
      byDirection[direction].count += 1;
      byDirection[direction].notional += row.notional;
      if (!maxEvent || row.notional > maxEvent.notional) maxEvent = row;
      const ts = Date.parse(row.ts);
      if (Number.isFinite(ts) && now - ts <= 5 * 60 * 1000) {
        last5m.byDirection[direction].count += 1;
        last5m.byDirection[direction].notional += row.notional;
        last5m.totalNotional += row.notional;
        last5m.count += 1;
      }
    }
    return {
      byDirection,
      maxEvent,
      last5m: {
        ...last5m,
        longNotionalRatio: last5m.totalNotional > 0 ? last5m.byDirection.long.notional / last5m.totalNotional : null,
        shortNotionalRatio: last5m.totalNotional > 0 ? last5m.byDirection.short.notional / last5m.totalNotional : null,
      },
    };
  }, [rows]);
  const aggregate = apiAggregate ?? fallbackAggregate;

  return (
    <Card title={byLang('爆仓流', 'Liquidations')}>
      {rows.length === 0 ? (
        <Typography.Text type="secondary">
          {status === 'running'
            ? byLang(
              '实时流已连接；只有发生强平时这里才会出现记录。没有爆仓不是错误。',
              'Stream is connected; rows appear only when a liquidation occurs. No liquidation is not an error.',
            )
            : byLang('爆仓实时流未运行。', 'Liquidation stream is not running.')}
        </Typography.Text>
      ) : (
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Row gutter={[12, 12]}>
            <Col xs={12} md={6}>
              <Statistic title={byLang('多头爆仓名义额', 'Long liq notional')} value={aggregate.byDirection.long.notional} precision={0} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('空头爆仓名义额', 'Short liq notional')} value={aggregate.byDirection.short.notional} precision={0} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('最大单笔', 'Largest order')} value={aggregate.maxEvent?.notional ?? 0} precision={0} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('最近 5 分钟笔数', 'Last 5m orders')} value={aggregate.last5m.count} />
            </Col>
          </Row>
          <div>
            <Space wrap style={{ marginBottom: 6 }}>
              <Typography.Text type="secondary">{byLang('最近 5 分钟多空名义额比例', 'Last 5m long/short notional ratio')}</Typography.Text>
              <Tag color="red">{byLang('多头爆仓', 'Long liq')}: {aggregate.last5m.longNotionalRatio == null ? '-' : formatPercent(aggregate.last5m.longNotionalRatio)}</Tag>
              <Tag color="blue">{byLang('空头爆仓', 'Short liq')}: {aggregate.last5m.shortNotionalRatio == null ? '-' : formatPercent(aggregate.last5m.shortNotionalRatio)}</Tag>
            </Space>
            <Progress
              percent={Math.round((aggregate.last5m.longNotionalRatio ?? 0) * 100)}
              success={{ percent: Math.round((aggregate.last5m.shortNotionalRatio ?? 0) * 100), strokeColor: '#1677ff' }}
              strokeColor="#ff4d4f"
              showInfo={false}
            />
          </div>
          <Table
            size="small"
            pagination={false}
            dataSource={rows.slice(0, 8).map((row, idx) => ({ ...row, key: `${row.ts}-${idx}` }))}
            columns={[
              { title: byLang('时间', 'Time'), dataIndex: 'ts', render: (v: string) => formatTs(v) },
              { title: byLang('标的', 'Symbol'), dataIndex: 'symbol' },
              { title: byLang('方向', 'Side'), dataIndex: 'side', render: (v: string) => liquidationSideText(v) },
              { title: byLang('名义额', 'Notional'), dataIndex: 'notional', render: (v: number) => formatNumber(v, 0) },
            ]}
          />
        </Space>
      )}
    </Card>
  );
}

export function MarketStructurePage() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const [symbol, setSymbol] = useState<string | undefined>(undefined);
  const [primaryVenue, setPrimaryVenue] = useState<MarketIntelVenue>('futures');
  const [streamWindowSeconds, setStreamWindowSeconds] = useState<StreamWindowSeconds>(300);
  const [sessionTimezone, setSessionTimezone] = useState<SessionTimezone>('utc');
  const query = useMarketIntelSummaryQuery(symbol, streamWindowSeconds);
  const data = query.data;
  const selectedVenue = data?.venues[primaryVenue];
  const secondaryVenueKey: MarketIntelVenue = primaryVenue === 'futures' ? 'spot' : 'futures';
  const secondaryVenue = data?.venues[secondaryVenueKey];
  const liquidationRows = (data?.liquidations.rows ?? []) as MarketIntelLiquidation[];

  useEffect(() => {
    if (!symbol && data?.selectedSymbol) setSymbol(data.selectedSymbol);
  }, [data?.selectedSymbol, symbol]);

  const options = (data?.symbols ?? ['BTC/USDT:USDT', 'ETH/USDT:USDT']).map((item) => ({
    value: item,
    label: item,
  }));

  const venueOptions = [
    { value: 'futures', label: byLang('合约主视角', 'Futures view') },
    { value: 'spot', label: byLang('现货主视角', 'Spot view') },
  ];

  const streamWindowOptions = [
    { value: 300, label: byLang('最近 5 分钟', 'Last 5m') },
    { value: 900, label: byLang('最近 15 分钟', 'Last 15m') },
    { value: 3600, label: byLang('最近 1 小时', 'Last 1h') },
  ];

  return (
    <div className="page-shell">
      <div className="page-header">
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            {byLang('市场结构', 'Market Structure')}
          </Typography.Title>
          <Typography.Text type="secondary">
            {byLang(
              '现货和合约分开看；这些数据用于监测市场结构和辅助判断，不构成交易建议。',
              'Spot and futures are separated; these data monitor market structure and support judgment, not trading advice.',
            )}
          </Typography.Text>
        </div>
        <Space wrap style={{ width: isMobile ? '100%' : undefined, justifyContent: isMobile ? 'stretch' : 'flex-end' }}>
          <Select
            style={{ width: isMobile ? '100%' : 240 }}
            value={symbol ?? data?.selectedSymbol}
            options={options}
            onChange={(value) => setSymbol(value)}
          />
          <Select
            style={{ width: isMobile ? '100%' : 160 }}
            value={primaryVenue}
            options={venueOptions}
            onChange={(value) => setPrimaryVenue(value)}
          />
          <Segmented
            value={streamWindowSeconds}
            options={streamWindowOptions}
            onChange={(value) => setStreamWindowSeconds(Number(value) as StreamWindowSeconds)}
          />
        </Space>
      </div>

      {query.isError ? (
        <ActionableErrorAlert
          title={byLang('市场结构数据加载失败', 'Market structure data failed')}
          steps={[
            byLang('确认服务器能访问 Binance public API', 'Confirm the server can reach Binance public APIs'),
            byLang('切换标的后重试', 'Switch symbol and retry'),
          ]}
          retryText={byLang('重试', 'Retry')}
          onRetry={() => void query.refetch()}
        />
      ) : null}

      <Card size="small" loading={query.isPending}>
        <Space wrap>
          <Tag>{data?.source ?? 'binance-public'}</Tag>
          <Tag color="red">{byLang('主视角', 'Primary view')}: {primaryVenue === 'spot' ? 'Spot' : 'Futures'}</Tag>
          <Typography.Text type="secondary">{byLang('更新时间', 'Updated')}: {formatTs(data?.ts)}</Typography.Text>
          <Typography.Text type="secondary">{byLang('周期', 'Interval')}: {data?.interval ?? '15m'}</Typography.Text>
          <Typography.Text type="secondary">{byLang('实时窗口', 'Live window')}: {streamWindowOptions.find((item) => item.value === (data?.stream?.windowSeconds ?? streamWindowSeconds))?.label}</Typography.Text>
          <Typography.Text type="secondary">{byLang('缓存', 'Cache')}: {data?.cache?.hit ? byLang('命中', 'hit') : byLang('刷新', 'miss')}</Typography.Text>
          <Typography.Text type="secondary">{byLang('实时流', 'Stream')}: {data?.stream?.status ?? 'stopped'}</Typography.Text>
          {Object.entries(data?.stream?.connections ?? {}).map(([venue, conn]) => (
            <Tag key={venue} color={conn.status === 'open' ? 'green' : conn.status === 'error' ? 'red' : 'default'}>
              {venue === 'spot' ? byLang('现货', 'spot') : byLang('合约', 'futures')}: {conn.status}
            </Tag>
          ))}
        </Space>
      </Card>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={14}>
          <VenueCard venue={selectedVenue} isPrimary streamWindowSeconds={streamWindowSeconds} />
        </Col>
        <Col xs={24} xl={10}>
          <VenueCard venue={secondaryVenue} isPrimary={false} streamWindowSeconds={streamWindowSeconds} />
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={12}>
          <OrderbookTable venue={selectedVenue} />
        </Col>
        <Col xs={24} xl={12}>
          <Space direction="vertical" size={16} style={{ width: '100%' }}>
            <Segmented
              value={sessionTimezone}
              options={[
                { value: 'utc', label: 'UTC' },
                { value: 'asia-shanghai', label: 'Asia-Shanghai' },
              ]}
              onChange={(value) => setSessionTimezone(value as SessionTimezone)}
            />
            <SessionEffectTable venue={selectedVenue} timezone={sessionTimezone} />
          </Space>
        </Col>
      </Row>

      <SessionHeatmap rows={selectedVenue?.sessionHeatmap} timezone={sessionTimezone} />

      <RollingCorrelationPanel rows={data?.correlation.rolling} breaks={data?.correlation.breaks} />

      <CorrelationHeatmap data={data?.correlation.matrix} />

      <StreamObservability stream={data?.stream} />

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={12}>
          <LiquidationPanel rows={liquidationRows} status={data?.liquidations.status} apiAggregate={data?.liquidations.aggregate} />
        </Col>
        <Col xs={24} lg={12}>
          <Card title={byLang('新闻 NLP / 情绪', 'News NLP / sentiment')}>
            <Typography.Text type="secondary">
              {byLang('新闻情绪需要先配置新闻源或本地 NLP feed；当前未接入。', 'News sentiment needs a news source or local NLP feed; none is configured yet.')}
            </Typography.Text>
          </Card>
        </Col>
      </Row>
    </div>
  );
}
