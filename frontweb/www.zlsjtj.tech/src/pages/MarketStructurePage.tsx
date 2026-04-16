import { useEffect, useMemo, useState } from 'react';
import type { ReactNode } from 'react';
import { Alert, Button, Card, Col, Empty, Grid, Progress, Row, Segmented, Select, Space, Statistic, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import ReactECharts from 'echarts-for-react';

import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useMarketIntelSummaryQuery } from '@/hooks/queries/market';
import { byLang } from '@/i18n';
import type {
  MarketIntelBasis,
  MarketIntelLevel,
  MarketIntelLiquidation,
  MarketIntelLiquidationAggregate,
  MarketIntelRollingCorrelation,
  MarketIntelSessionHeatmapCell,
  MarketIntelStreamStatus,
  MarketIntelSummary,
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
type PressureDirection = 'buy' | 'sell' | 'balanced' | 'unknown';
type SessionTimezone = 'utc' | 'asia-shanghai';
type OiPeriod = '15m' | '30m' | '1h' | '4h' | '1d';
type MarketLookbackBars = 96 | 288 | 672 | 1000;

const PRESSURE_THRESHOLD = 0.15;
const OI_PERIODS: OiPeriod[] = ['15m', '30m', '1h', '4h', '1d'];
const MARKET_LOOKBACK_OPTIONS: MarketLookbackBars[] = [96, 288, 672, 1000];
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

function pressureDirection(value?: number | null): PressureDirection {
  if (value === null || value === undefined || Number.isNaN(value)) return 'unknown';
  if (value >= PRESSURE_THRESHOLD) return 'buy';
  if (value <= -PRESSURE_THRESHOLD) return 'sell';
  return 'balanced';
}

function directionTag(direction: PressureDirection) {
  if (direction === 'buy') return <Tag color="red">{byLang('买方压力', 'Buy pressure')}</Tag>;
  if (direction === 'sell') return <Tag color="blue">{byLang('卖方压力', 'Sell pressure')}</Tag>;
  if (direction === 'balanced') return <Tag color="green">{byLang('相对均衡', 'Balanced')}</Tag>;
  return <Tag>{byLang('等待数据', 'Waiting')}</Tag>;
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

function lookbackLabel(bars: number) {
  const hours = Math.round((bars * 15) / 60);
  if (hours < 24) return byLang(`${hours} 小时`, `${hours}h`);
  const days = Math.round(hours / 24);
  return byLang(`约 ${days} 天`, `About ${days}d`);
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

function MarketSection({
  id,
  title,
  description,
  children,
}: {
  id: string;
  title: string;
  description: string;
  children: ReactNode;
}) {
  return (
    <section id={id} style={{ scrollMarginTop: 84 }}>
      <Space direction="vertical" size={10} style={{ width: '100%' }}>
        <div>
          <Typography.Title level={4} style={{ margin: 0 }}>
            {title}
          </Typography.Title>
          <Typography.Text type="secondary">{description}</Typography.Text>
        </div>
        {children}
      </Space>
    </section>
  );
}

function MetricDirectory({
  selectedVenue,
  liquidationCount,
  rollingCount,
  basisOk,
}: {
  selectedVenue?: MarketIntelVenueSnapshot;
  liquidationCount: number;
  rollingCount: number;
  basisOk?: boolean;
}) {
  const deriv = selectedVenue?.derivatives;
  const scrollToSection = (id: string) => {
    document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
  };
  const items = [
    {
      key: 'pressure',
      target: 'market-live-flow',
      title: byLang('盘口 / OFI / Taker', 'Book / OFI / Taker'),
      value: signedPercent(selectedVenue?.stream?.ofi?.ofiNorm ?? selectedVenue?.flow?.tradeImbalance),
      detail: byLang('实时压力区', 'Live pressure'),
    },
    {
      key: 'volume',
      target: 'market-live-flow',
      title: byLang('成交量比率', 'Volume ratio'),
      value: `${formatNumber(selectedVenue?.volumeRatio ?? 0, 2)}x`,
      detail: byLang('主视角卡片', 'Primary card'),
    },
    {
      key: 'oi',
      target: 'market-futures-positioning',
      title: byLang('持仓量变化', 'OI change'),
      value: deriv?.openInterestChangePct == null ? '-' : signedPercent(deriv.openInterestChangePct),
      detail: byLang('合约持仓区', 'Futures OI'),
    },
    {
      key: 'funding',
      target: 'market-futures-positioning',
      title: byLang('资金费率', 'Funding'),
      value: deriv?.fundingRate == null ? '-' : formatPercent(deriv.fundingRate, 4),
      detail: byLang('合约持仓区', 'Futures OI'),
    },
    {
      key: 'basis',
      target: 'market-futures-positioning',
      title: byLang('期现价差', 'Basis'),
      value: basisOk ? byLang('已计算', 'ready') : byLang('等待数据', 'waiting'),
      detail: byLang('期现结构', 'Basis'),
    },
    {
      key: 'level2',
      target: 'market-level2',
      title: byLang('Level 2 订单薄', 'Level 2 book'),
      value: selectedVenue?.orderbook?.bids?.length ?? 0,
      detail: byLang('订单薄区', 'Book section'),
    },
    {
      key: 'liq',
      target: 'market-liquidations',
      title: byLang('爆仓数据', 'Liquidations'),
      value: liquidationCount,
      detail: byLang('爆仓流区', 'Liquidation flow'),
    },
    {
      key: 'corr',
      target: 'market-cross-asset',
      title: byLang('跨资产相关', 'Correlation'),
      value: rollingCount,
      detail: byLang('跨资产区', 'Cross-asset'),
    },
    {
      key: 'news',
      target: 'market-runtime',
      title: byLang('新闻情绪', 'News sentiment'),
      value: byLang('未配置', 'not configured'),
      detail: byLang('未接入源', 'No source'),
    },
  ];

  return (
    <Card size="small" title={byLang('数据索引', 'Data index')}>
      <Row gutter={[8, 8]}>
        {items.map((item) => (
          <Col key={item.key} xs={12} md={6} xl={3}>
            <button
              type="button"
              onClick={() => scrollToSection(item.target)}
              style={{
                width: '100%',
                minHeight: 82,
                padding: 10,
                border: '1px solid rgba(127,127,127,0.18)',
                borderRadius: 8,
                background: 'transparent',
                color: 'inherit',
                textAlign: 'left',
                cursor: 'pointer',
              }}
            >
              <Typography.Text type="secondary" style={{ display: 'block' }}>{item.title}</Typography.Text>
              <Typography.Text strong style={{ display: 'block', marginTop: 4 }}>{item.value}</Typography.Text>
              <Typography.Text type="secondary" style={{ display: 'block', marginTop: 4, fontSize: 12 }}>{item.detail}</Typography.Text>
            </button>
          </Col>
        ))}
      </Row>
    </Card>
  );
}

function SignalOverviewPanel({
  selectedVenue,
  basis,
  liquidationCount,
  rollingCount,
}: {
  selectedVenue?: MarketIntelVenueSnapshot;
  basis?: MarketIntelBasis;
  liquidationCount: number;
  rollingCount: number;
}) {
  const deriv = selectedVenue?.derivatives;
  const orderbookLevels = (selectedVenue?.orderbook?.bids?.length ?? 0) + (selectedVenue?.orderbook?.asks?.length ?? 0);
  const sessionRows = (selectedVenue?.sessionEffect?.length ?? 0) + (selectedVenue?.sessionHeatmap?.length ?? 0);
  const rows = [
    {
      key: 'volume',
      group: byLang('成交结构', 'Trade structure'),
      metric: byLang('成交量比率', 'Volume ratio'),
      value: selectedVenue ? `${formatNumber(selectedVenue.volumeRatio, 2)}x` : '-',
      status: selectedVenue?.ok ? 'ok' : 'waiting',
      target: 'market-live-flow',
      source: byLang('主视角 K 线聚合', 'Primary-view kline aggregation'),
    },
    {
      key: 'taker',
      group: byLang('成交结构', 'Trade structure'),
      metric: 'Taker buy',
      value: selectedVenue?.flow ? formatPercent(selectedVenue.flow.takerBuyRatio) : '-',
      status: selectedVenue?.flow ? 'ok' : 'waiting',
      target: 'market-live-flow',
      source: byLang('Binance public trades', 'Binance public trades'),
    },
    {
      key: 'ofi',
      group: byLang('盘口压力', 'Book pressure'),
      metric: 'OFI',
      value: selectedVenue?.stream?.ofi ? signedPercent(selectedVenue.stream.ofi.ofiNorm) : '-',
      status: selectedVenue?.stream?.ofi ? 'ok' : 'waiting',
      target: 'market-live-flow',
      source: byLang('实时订单薄滚动窗口', 'Live book rolling window'),
    },
    {
      key: 'level2',
      group: byLang('盘口压力', 'Book pressure'),
      metric: 'Level 2',
      value: orderbookLevels > 0 ? byLang(`${orderbookLevels} 档`, `${orderbookLevels} levels`) : '-',
      status: orderbookLevels > 0 ? 'ok' : 'waiting',
      target: 'market-level2',
      source: byLang('Binance public depth', 'Binance public depth'),
    },
    {
      key: 'funding',
      group: byLang('合约结构', 'Futures structure'),
      metric: 'Funding',
      value: deriv?.fundingRate == null ? '-' : formatPercent(deriv.fundingRate, 4),
      status: deriv?.fundingRate == null ? 'waiting' : 'ok',
      target: 'market-futures-positioning',
      source: byLang('Binance USD-M Futures public', 'Binance USD-M Futures public'),
    },
    {
      key: 'oi',
      group: byLang('合约结构', 'Futures structure'),
      metric: byLang('持仓量变化', 'OI change'),
      value: deriv?.openInterestChangePct == null ? '-' : signedPercent(deriv.openInterestChangePct, 3),
      status: deriv?.openInterestChangePct == null ? 'waiting' : 'ok',
      target: 'market-futures-positioning',
      source: byLang('15m / 30m / 1h / 4h / 1d', '15m / 30m / 1h / 4h / 1d'),
    },
    {
      key: 'basis',
      group: byLang('合约结构', 'Futures structure'),
      metric: byLang('期现价差', 'Basis'),
      value: basis?.basisPct == null ? '-' : signedPercent(basis.basisPct, 3),
      status: basis?.ok ? 'ok' : 'waiting',
      target: 'market-futures-positioning',
      source: byLang('Spot / Futures 中间价', 'Spot / Futures mid prices'),
    },
    {
      key: 'liq',
      group: byLang('风险事件', 'Risk events'),
      metric: byLang('爆仓数据', 'Liquidations'),
      value: liquidationCount,
      status: liquidationCount > 0 ? 'ok' : 'empty',
      target: 'market-liquidations',
      source: byLang('Futures forceOrder stream', 'Futures forceOrder stream'),
    },
    {
      key: 'session',
      group: byLang('时间结构', 'Time structure'),
      metric: byLang('时间段效应', 'Session effect'),
      value: sessionRows > 0 ? sessionRows : '-',
      status: sessionRows > 0 ? 'ok' : 'waiting',
      target: 'market-time-structure',
      source: byLang('历史 K 线统计', 'Historical kline stats'),
    },
    {
      key: 'corr',
      group: byLang('跨资产', 'Cross-asset'),
      metric: byLang('实时相关', 'Correlation'),
      value: rollingCount > 0 ? rollingCount : '-',
      status: rollingCount > 0 ? 'ok' : 'waiting',
      target: 'market-cross-asset',
      source: byLang('核心交易对滚动收益', 'Core-pair rolling returns'),
    },
    {
      key: 'onchain',
      group: byLang('外部数据', 'External data'),
      metric: byLang('链上数据', 'On-chain data'),
      value: byLang('未配置', 'not configured'),
      status: 'not_configured',
      target: 'market-runtime',
      source: byLang('等待配置数据源', 'Waiting for a configured source'),
    },
    {
      key: 'news',
      group: byLang('外部数据', 'External data'),
      metric: byLang('新闻 NLP / 情绪', 'News NLP / sentiment'),
      value: byLang('未配置', 'not configured'),
      status: 'not_configured',
      target: 'market-runtime',
      source: byLang('等待 RSS 或本地 feed', 'Waiting for RSS or local feed'),
    },
  ];

  const statusTag = (status: string) => {
    if (status === 'ok') return <Tag color="green">{byLang('有数据', 'ready')}</Tag>;
    if (status === 'empty') return <Tag>{byLang('正常空状态', 'normal empty')}</Tag>;
    if (status === 'not_configured') return <Tag color="default">{byLang('未配置', 'not configured')}</Tag>;
    return <Tag color="gold">{byLang('等待数据', 'waiting')}</Tag>;
  };

  return (
    <Card
      size="small"
      title={byLang('信号总览', 'Signal overview')}
      extra={<Typography.Text type="secondary">{byLang('点击位置跳到对应板块', 'Open the related section from Location')}</Typography.Text>}
    >
      <Table
        size="small"
        pagination={false}
        dataSource={rows}
        columns={[
          { title: byLang('分组', 'Group'), dataIndex: 'group', responsive: ['md'] },
          { title: byLang('指标', 'Metric'), dataIndex: 'metric' },
          { title: byLang('当前值', 'Current'), dataIndex: 'value', render: (value: string | number) => String(value) },
          { title: byLang('状态', 'Status'), dataIndex: 'status', render: (status: string) => statusTag(status) },
          { title: byLang('来源 / 窗口', 'Source / window'), dataIndex: 'source', responsive: ['lg'] },
          {
            title: byLang('位置', 'Location'),
            dataIndex: 'target',
            render: (target: string) => <a href={`#${target}`}>{byLang('查看', 'Open')}</a>,
          },
        ]}
      />
      <Typography.Text type="secondary">
        {byLang(
          '总览只汇总监测状态和当前值；未配置的数据源不会展示模拟数据。',
          'The overview only summarizes monitoring status and current values; unconfigured sources do not show simulated data.',
        )}
      </Typography.Text>
    </Card>
  );
}

function MarketContextPanel({
  data,
  primaryVenue,
  streamWindowLabel,
  loading,
  refreshing,
  onRefresh,
}: {
  data?: MarketIntelSummary;
  primaryVenue: MarketIntelVenue;
  streamWindowLabel?: string;
  loading: boolean;
  refreshing: boolean;
  onRefresh: () => void;
}) {
  const connections = Object.entries(data?.stream?.connections ?? {});
  const source = data?.source ?? 'binance-public';
  const cacheText = data?.cache?.hit ? byLang('命中', 'hit') : byLang('刷新', 'miss');

  return (
    <Card
      size="small"
      loading={loading}
      title={byLang('当前上下文', 'Current context')}
      extra={
        <Button size="small" loading={refreshing && !loading} onClick={onRefresh}>
          {byLang('刷新', 'Refresh')}
        </Button>
      }
    >
      <Row gutter={[12, 12]}>
        <Col xs={12} md={6} xl={4}>
          <Typography.Text type="secondary" style={{ display: 'block' }}>{byLang('数据源', 'Source')}</Typography.Text>
          <Tag style={{ marginTop: 4 }}>{source}</Tag>
        </Col>
        <Col xs={12} md={6} xl={4}>
          <Typography.Text type="secondary" style={{ display: 'block' }}>{byLang('主视角', 'Primary view')}</Typography.Text>
          <Tag color="red" style={{ marginTop: 4 }}>{primaryVenue === 'spot' ? 'Spot' : 'Futures'}</Tag>
        </Col>
        <Col xs={12} md={6} xl={4}>
          <Typography.Text type="secondary" style={{ display: 'block' }}>{byLang('更新时间', 'Updated')}</Typography.Text>
          <Typography.Text strong>{formatTs(data?.ts)}</Typography.Text>
        </Col>
        <Col xs={12} md={6} xl={4}>
          <Typography.Text type="secondary" style={{ display: 'block' }}>{byLang('周期', 'Interval')}</Typography.Text>
          <Typography.Text strong>{data?.interval ?? '15m'}</Typography.Text>
        </Col>
        <Col xs={12} md={6} xl={4}>
          <Typography.Text type="secondary" style={{ display: 'block' }}>{byLang('实时窗口', 'Live window')}</Typography.Text>
          <Typography.Text strong>{streamWindowLabel ?? '-'}</Typography.Text>
        </Col>
        <Col xs={12} md={6} xl={4}>
          <Typography.Text type="secondary" style={{ display: 'block' }}>{byLang('缓存', 'Cache')}</Typography.Text>
          <Typography.Text strong>{cacheText}</Typography.Text>
        </Col>
      </Row>
      <Space wrap style={{ marginTop: 12 }}>
        <Typography.Text type="secondary">{byLang('实时流', 'Stream')}: {data?.stream?.status ?? 'stopped'}</Typography.Text>
        {connections.length === 0 ? (
          <Tag>{byLang('等待连接状态', 'waiting for connections')}</Tag>
        ) : (
          connections.map(([venue, conn]) => (
            <Tag key={venue} color={conn.status === 'open' ? 'green' : conn.status === 'error' ? 'red' : 'default'}>
              {venue === 'spot' ? 'Spot' : 'Futures'}: {conn.status}
            </Tag>
          ))
        )}
      </Space>
    </Card>
  );
}

function StructureInsightPanel({
  selectedVenue,
  secondaryVenue,
  basis,
  stream,
  liquidationRows,
  correlationBreaks,
  streamWindowSeconds,
}: {
  selectedVenue?: MarketIntelVenueSnapshot;
  secondaryVenue?: MarketIntelVenueSnapshot;
  basis?: MarketIntelBasis;
  stream?: MarketIntelStreamStatus;
  liquidationRows: MarketIntelLiquidation[];
  correlationBreaks?: MarketIntelSummary['correlation']['breaks'];
  streamWindowSeconds: StreamWindowSeconds;
}) {
  const bookDirection = pressureDirection(selectedVenue?.orderbook?.imbalance);
  const flowDirection = pressureDirection(selectedVenue?.flow?.tradeImbalance);
  const ofiDirection = pressureDirection(selectedVenue?.stream?.ofi?.ofiNorm);
  const directions = [bookDirection, flowDirection, ofiDirection].filter((item) => item !== 'unknown');
  const buyCount = directions.filter((item) => item === 'buy').length;
  const sellCount = directions.filter((item) => item === 'sell').length;
  const liveSamples = (selectedVenue?.stream?.ofi?.samples ?? 0) + (selectedVenue?.stream?.flow?.samples ?? 0);
  const secondaryFlow = secondaryVenue?.flow?.tradeImbalance;
  const oiChange = selectedVenue?.derivatives?.openInterestChangePct;
  const fundingRate = selectedVenue?.derivatives?.fundingRate;
  const basisPct = basis?.basisPct;
  const hasDivergence = buyCount > 0 && sellCount > 0;
  const alignedDirection: PressureDirection = buyCount >= 2 ? 'buy' : sellCount >= 2 ? 'sell' : directions.length > 0 ? 'balanced' : 'unknown';
  const headline = hasDivergence
    ? byLang('短窗指标出现背离', 'Short-window indicators are diverging')
    : alignedDirection === 'buy'
      ? byLang('短窗买方压力较集中', 'Short-window buy pressure is concentrated')
      : alignedDirection === 'sell'
        ? byLang('短窗卖方压力较集中', 'Short-window sell pressure is concentrated')
        : alignedDirection === 'balanced'
          ? byLang('短窗结构相对均衡', 'Short-window structure is relatively balanced')
          : byLang('等待实时结构样本', 'Waiting for live structure samples');
  const streamUpdatedMs = stream?.updatedAt ? Date.parse(stream.updatedAt) : Number.NaN;
  const streamAgeSeconds = Number.isFinite(streamUpdatedMs) ? Math.max(0, Math.round((Date.now() - streamUpdatedMs) / 1000)) : null;
  const streamState = stream?.status === 'running'
    ? byLang('实时流运行中', 'Live stream running')
    : byLang('实时流未运行', 'Live stream not running');

  return (
    <Card
      size="small"
      title={byLang('结构摘要', 'Structure brief')}
      extra={<Tag color={alignedDirection === 'buy' ? 'red' : alignedDirection === 'sell' ? 'blue' : 'green'}>{headline}</Tag>}
    >
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Row gutter={[12, 12]}>
          <Col xs={12} md={6}>
            <Statistic title={byLang('Book / Flow / OFI 一致性', 'Book / Flow / OFI alignment')} value={`${Math.max(buyCount, sellCount)}/3`} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('实时样本', 'Live samples')} value={liveSamples} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('最近爆仓记录', 'Recent liquidations')} value={liquidationRows.length} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('相关性提示', 'Correlation monitors')} value={correlationBreaks?.length ?? 0} />
          </Col>
        </Row>
        <Space wrap>
          <Typography.Text type="secondary">{byLang('主视角', 'Primary')}:</Typography.Text>
          {directionTag(alignedDirection)}
          <Tag>{byLang('窗口', 'Window')}: {streamWindowSeconds / 60}m</Tag>
          <Tag color={stream?.status === 'running' ? 'green' : 'gold'}>{streamState}</Tag>
          <Tag>{byLang('更新延迟', 'Update lag')}: {streamAgeSeconds == null ? '-' : `${streamAgeSeconds}s`}</Tag>
          <Tag>{byLang('辅助视角成交流', 'Secondary flow')}: {signedPercent(secondaryFlow)}</Tag>
          <Tag>{byLang('OI', 'OI')}: {signedPercent(oiChange, 3)}</Tag>
          <Tag>{byLang('Funding', 'Funding')}: {fundingRate == null ? '-' : formatPercent(fundingRate, 4)}</Tag>
          <Tag>{byLang('Basis', 'Basis')}: {signedPercent(basisPct, 3)}</Tag>
        </Space>
        <Typography.Text type="secondary">
          {byLang(
            '结构摘要把盘口、主动成交、OFI、合约持仓、期现价差和事件流放在同一处对齐；它用于监测一致性和背离，不构成交易建议。',
            'The brief aligns book, taker flow, OFI, positioning, basis and event flow in one place; it monitors agreement and divergence and is not trading advice.',
          )}
        </Typography.Text>
      </Space>
    </Card>
  );
}

function StructureConsistencyPanel({
  selectedVenue,
  secondaryVenue,
  basis,
}: {
  selectedVenue?: MarketIntelVenueSnapshot;
  secondaryVenue?: MarketIntelVenueSnapshot;
  basis?: MarketIntelBasis;
}) {
  const rows = [
    {
      key: 'book',
      metric: byLang('订单薄偏斜', 'Book skew'),
      value: signedPercent(selectedVenue?.orderbook?.imbalance),
      direction: pressureDirection(selectedVenue?.orderbook?.imbalance),
      role: byLang('挂单厚度压力', 'Displayed-depth pressure'),
    },
    {
      key: 'flow',
      metric: byLang('主视角成交流', 'Primary taker flow'),
      value: signedPercent(selectedVenue?.flow?.tradeImbalance),
      direction: pressureDirection(selectedVenue?.flow?.tradeImbalance),
      role: byLang('主动成交压力', 'Aggressive-trade pressure'),
    },
    {
      key: 'ofi',
      metric: 'OFI',
      value: signedPercent(selectedVenue?.stream?.ofi?.ofiNorm),
      direction: pressureDirection(selectedVenue?.stream?.ofi?.ofiNorm),
      role: byLang('订单薄更新压力', 'Book-update pressure'),
    },
    {
      key: 'secondary-flow',
      metric: byLang('辅助视角成交流', 'Secondary taker flow'),
      value: signedPercent(secondaryVenue?.flow?.tradeImbalance),
      direction: pressureDirection(secondaryVenue?.flow?.tradeImbalance),
      role: byLang('Spot/Futures 交叉检查', 'Spot/Futures cross-check'),
    },
    {
      key: 'oi',
      metric: byLang('持仓量变化', 'OI change'),
      value: signedPercent(selectedVenue?.derivatives?.openInterestChangePct, 3),
      direction: pressureDirection(selectedVenue?.derivatives?.openInterestChangePct),
      role: byLang('合约仓位规模变化', 'Positioning size change'),
    },
    {
      key: 'basis',
      metric: byLang('期现价差', 'Basis'),
      value: signedPercent(basis?.basisPct, 3),
      direction: pressureDirection(basis?.basisPct),
      role: byLang('期现结构变化', 'Spot-futures structure change'),
    },
  ];
  const pressureRows = rows.filter((row) => row.direction === 'buy' || row.direction === 'sell');
  const buyCount = pressureRows.filter((row) => row.direction === 'buy').length;
  const sellCount = pressureRows.filter((row) => row.direction === 'sell').length;
  const hasDivergence = buyCount > 0 && sellCount > 0;

  return (
    <Card title={byLang('一致性与背离监控', 'Alignment and divergence monitor')}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Alert
          type={hasDivergence ? 'warning' : 'info'}
          showIcon
          message={hasDivergence ? byLang('部分结构指标方向不一致', 'Some structure indicators disagree') : byLang('未检测到明显方向背离', 'No clear directional divergence detected')}
          description={byLang(
            '方向只基于当前阈值和最新窗口，用于发现需要进一步查看的结构变化。',
            'Direction only uses current thresholds and the latest window to surface structure changes worth reviewing.',
          )}
        />
        <Table
          size="small"
          pagination={false}
          dataSource={rows}
          columns={[
            { title: byLang('指标', 'Metric'), dataIndex: 'metric' },
            { title: byLang('当前值', 'Current'), dataIndex: 'value' },
            {
              title: byLang('方向', 'Direction'),
              dataIndex: 'direction',
              render: (direction: PressureDirection) => directionTag(direction),
            },
            { title: byLang('作用', 'Role'), dataIndex: 'role', responsive: ['md'] },
          ]}
        />
        <Typography.Text type="secondary">
          {byLang(
            'OI 和 Basis 的方向不能直接等同多空，只作为合约结构和期现结构的背景项。',
            'OI and Basis direction is not a direct long/short call; it is used as positioning and basis context.',
          )}
        </Typography.Text>
      </Space>
    </Card>
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
      <Typography.Text type="secondary">
        {byLang(
          'Book 看挂单厚度，Flow 看主动成交，OFI 看订单薄更新压力；颜色只标记短窗压力方向。',
          'Book tracks displayed depth, Flow tracks taker trades, and OFI tracks book-update pressure; colors only mark the short-window pressure direction.',
        )}
      </Typography.Text>
    </Space>
  );
}

function MetricTile({
  title,
  value,
  color,
  detail,
}: {
  title: string;
  value: ReactNode;
  color?: string;
  detail?: string;
}) {
  return (
    <div
      style={{
        minHeight: 74,
        padding: 10,
        border: '1px solid rgba(127,127,127,0.16)',
        borderRadius: 8,
      }}
    >
      <Typography.Text type="secondary" style={{ display: 'block', fontSize: 12 }}>
        {title}
      </Typography.Text>
      <Typography.Text strong style={{ display: 'block', marginTop: 4, color, fontSize: 18 }}>
        {value}
      </Typography.Text>
      {detail ? (
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 2, fontSize: 12 }}>
          {detail}
        </Typography.Text>
      ) : null}
    </div>
  );
}

function MetricGroup({ title, children }: { title: string; children: ReactNode }) {
  return (
    <Space direction="vertical" size={8} style={{ width: '100%' }}>
      <Typography.Text strong>{title}</Typography.Text>
      <Row gutter={[8, 8]}>{children}</Row>
    </Space>
  );
}

function MicrostructureFocusPanel({
  venue,
  secondaryVenue,
  streamWindowSeconds,
}: {
  venue?: MarketIntelVenueSnapshot;
  secondaryVenue?: MarketIntelVenueSnapshot;
  streamWindowSeconds: StreamWindowSeconds;
}) {
  const flow = venue?.flow;
  const streamFlow = venue?.stream?.flow;
  const ofi = venue?.stream?.ofi;
  const book = venue?.orderbook;
  const liveFlowSeries = useMemo(() => streamFlow?.series ?? [], [streamFlow?.series]);
  const ofiSeries = useMemo(() => ofi?.series ?? [], [ofi?.series]);
  const timestamps = useMemo(() => Array.from(new Set([...liveFlowSeries.map((point) => point.ts), ...ofiSeries.map((point) => point.ts)])).sort(), [liveFlowSeries, ofiSeries]);
  const primaryFlowDirection = pressureDirection(flow?.tradeImbalance);
  const liveFlowDirection = pressureDirection(streamFlow?.imbalance);
  const ofiDirection = pressureDirection(ofi?.ofiNorm);
  const bookDirection = pressureDirection(book?.imbalance);
  const divergence = [primaryFlowDirection, liveFlowDirection, ofiDirection, bookDirection].filter((item) => item === 'buy' || item === 'sell');
  const hasDivergence = divergence.includes('buy') && divergence.includes('sell');
  const option = useMemo(() => {
    const takerValues = new Map(liveFlowSeries.map((point) => [point.ts, point.takerBuyRatio]));
    const flowImbalanceValues = new Map(liveFlowSeries.map((point) => [point.ts, point.imbalance]));
    const ofiValues = new Map(ofiSeries.map((point) => [point.ts, point.ofiNorm]));
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const items = Array.isArray(params) ? params : [params];
          const lines = items.map((item) => `${item.seriesName}: ${typeof item.data === 'number' ? signedPercent(item.data) : '-'}`);
          return `${formatTs(String(items[0]?.axisValue ?? ''))}<br/>${lines.join('<br/>')}`;
        },
      },
      legend: { top: 0, textStyle: { color: 'rgba(215,226,240,0.72)' } },
      grid: { left: 46, right: 16, top: 38, bottom: 30 },
      xAxis: {
        type: 'category',
        data: timestamps,
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: string) => formatTs(v, 'HH:mm') },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        min: -1,
        max: 1,
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: number) => `${(v * 100).toFixed(0)}%` },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      series: [
        {
          name: 'Taker buy - 50%',
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: timestamps.map((ts) => (takerValues.has(ts) ? (takerValues.get(ts) ?? 0.5) - 0.5 : null)),
          lineStyle: { width: 2, color: '#faad14' },
        },
        {
          name: 'Flow skew',
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: timestamps.map((ts) => flowImbalanceValues.get(ts) ?? null),
          lineStyle: { width: 2, color: '#ff4d4f' },
        },
        {
          name: 'OFI',
          type: 'line',
          smooth: true,
          showSymbol: false,
          data: timestamps.map((ts) => ofiValues.get(ts) ?? null),
          lineStyle: { width: 2, color: '#1677ff' },
        },
      ],
    };
  }, [liveFlowSeries, ofiSeries, timestamps]);

  return (
    <Card title={byLang('Taker ratio / OFI 核心监控', 'Taker ratio / OFI core monitor')}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Alert
          type={hasDivergence ? 'warning' : 'info'}
          showIcon
          message={hasDivergence ? byLang('成交与订单薄压力出现背离', 'Trade flow and book pressure are diverging') : byLang('成交与订单薄压力未出现明显背离', 'No clear divergence between trade flow and book pressure')}
          description={byLang(
            'Taker ratio 看主动成交方向，OFI 看订单薄更新压力，Level 2 偏斜看静态深度；三者一起用于观察短窗微观结构。',
            'Taker ratio tracks aggressive trade direction, OFI tracks book-update pressure, and Level 2 skew tracks displayed depth; together they monitor short-window microstructure.',
          )}
        />
        <Row gutter={[12, 12]}>
          <Col xs={12} md={6}>
            <Statistic title="Taker buy" value={flow?.takerBuyRatio == null ? '-' : formatPercent(flow.takerBuyRatio)} valueStyle={{ color: barColor((flow?.takerBuyRatio ?? 0.5) - 0.5) }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('实时 Taker buy', 'Live taker buy')} value={streamFlow?.takerBuyRatio == null ? '-' : formatPercent(streamFlow.takerBuyRatio)} valueStyle={{ color: barColor((streamFlow?.takerBuyRatio ?? 0.5) - 0.5) }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title="OFI" value={signedPercent(ofi?.ofiNorm)} valueStyle={{ color: barColor(ofi?.ofiNorm ?? 0) }} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('Level 2 偏斜', 'Level 2 skew')} value={signedPercent(book?.imbalance)} valueStyle={{ color: barColor(book?.imbalance ?? 0) }} />
          </Col>
        </Row>
        <Space wrap>
          <Typography.Text type="secondary">{byLang('方向检查', 'Direction check')}:</Typography.Text>
          <Space size={4}>REST Flow {directionTag(primaryFlowDirection)}</Space>
          <Space size={4}>Live Flow {directionTag(liveFlowDirection)}</Space>
          <Space size={4}>OFI {directionTag(ofiDirection)}</Space>
          <Space size={4}>Book {directionTag(bookDirection)}</Space>
          <Tag>{byLang('窗口', 'Window')}: {streamWindowSeconds / 60}m</Tag>
          <Tag>{byLang('辅助视角', 'Secondary')}: {signedPercent(secondaryVenue?.flow?.tradeImbalance)}</Tag>
        </Space>
        {timestamps.length === 0 ? (
          <Empty description={byLang('等待实时 Taker / OFI 序列样本', 'Waiting for live Taker / OFI series samples')} />
        ) : (
          <ReactECharts option={option} style={{ height: 286 }} notMerge lazyUpdate />
        )}
        <Typography.Text type="secondary">
          {byLang(
            'Taker buy 在图中减去 50% 后展示，便于和 Flow skew、OFI 在同一坐标比较；这些是监测指标，不是交易建议。',
            'Taker buy is shown minus 50% so it can be compared with Flow skew and OFI on the same axis; these are monitoring metrics, not trading advice.',
          )}
        </Typography.Text>
      </Space>
    </Card>
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
  const sourceErrors = [
    ...(venue.sourceErrors ?? []),
    ...(deriv?.errors ?? []).map((item) => `derivatives: ${item}`),
  ];
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
      {venue.ok && sourceErrors.length > 0 ? (
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 12 }}
          message={byLang('部分市场结构数据暂不可用', 'Some market-structure data is temporarily unavailable')}
          description={
            <Space direction="vertical" size={2}>
              <Typography.Text type="secondary">
                {byLang(
                  '已保留可用的盘口、成交、K 线或合约数据；下列来源会在下一次刷新时重试。',
                  'Available book, trade, kline or futures data is kept; the sources below will retry on the next refresh.',
                )}
              </Typography.Text>
              {sourceErrors.slice(0, 4).map((item, idx) => (
                <Typography.Text key={`${item}-${idx}`} type="secondary">
                  {item}
                </Typography.Text>
              ))}
            </Space>
          }
        />
      ) : null}
      <Space direction="vertical" size={14} style={{ width: '100%' }}>
        <MetricGroup title={byLang('价格与盘口', 'Price and book')}>
          <Col xs={12} lg={8}>
            <MetricTile title={byLang('中间价', 'Mid')} value={formatNumber(ob?.mid ?? 0, 2)} />
          </Col>
          <Col xs={12} lg={8}>
            <MetricTile title={byLang('价差', 'Spread')} value={formatPercent(ob?.spreadPct ?? 0, 3)} />
          </Col>
          <Col xs={12} lg={8}>
            <MetricTile
              title={byLang('订单薄偏斜', 'Book skew')}
              value={signedPercent(ob?.imbalance ?? 0)}
              color={barColor(ob?.imbalance ?? 0)}
              detail={pressureText(ob?.imbalance)}
            />
          </Col>
        </MetricGroup>

        <MetricGroup title={byLang('主动成交', 'Taker flow')}>
          <Col xs={12} lg={8}>
            <MetricTile title={byLang('主动买入', 'Taker buy')} value={formatPercent(flow?.takerBuyRatio ?? 0)} />
          </Col>
          <Col xs={12} lg={8}>
            <MetricTile
              title={byLang('成交流偏斜', 'Flow skew')}
              value={signedPercent(flow?.tradeImbalance ?? 0)}
              color={barColor(flow?.tradeImbalance ?? 0)}
              detail={pressureText(flow?.tradeImbalance)}
            />
          </Col>
          <Col xs={12} lg={8}>
            <MetricTile title={byLang('量比', 'Volume ratio')} value={`${formatNumber(venue.volumeRatio, 2)}x`} />
          </Col>
        </MetricGroup>

        <MetricGroup title={byLang('实时窗口', 'Live window')}>
          <Col xs={12} lg={8}>
            <MetricTile
              title={byLang('实时 OFI', 'Live OFI')}
              value={signedPercent(stream?.ofi?.ofiNorm ?? 0)}
              color={barColor(stream?.ofi?.ofiNorm ?? 0)}
              detail={pressureText(stream?.ofi?.ofiNorm)}
            />
          </Col>
          <Col xs={12} lg={8}>
            <MetricTile title={byLang('实时主动买入', 'Live taker buy')} value={formatPercent(stream?.flow?.takerBuyRatio ?? 0)} />
          </Col>
          <Col xs={12} lg={8}>
            <MetricTile title={byLang('实时样本', 'Live samples')} value={(stream?.ofi?.samples ?? 0) + (stream?.flow?.samples ?? 0)} />
          </Col>
        </MetricGroup>

        {venue.venue === 'futures' ? (
          <MetricGroup title={byLang('合约结构', 'Futures structure')}>
            <Col xs={12} lg={8}>
              <MetricTile title={byLang('资金费率', 'Funding')} value={deriv?.fundingRate == null ? '-' : formatPercent(deriv.fundingRate, 4)} />
            </Col>
            <Col xs={12} lg={8}>
              <MetricTile
                title={byLang('持仓量变化', 'OI change')}
                value={deriv?.openInterestChangePct == null ? '-' : signedPercent(deriv.openInterestChangePct)}
                color={deriv?.openInterestChangePct == null ? undefined : barColor(deriv.openInterestChangePct)}
              />
            </Col>
            <Col xs={12} lg={8}>
              <MetricTile title={byLang('周期主动买入', 'Period taker buy')} value={deriv?.periodTakerBuyRatio == null ? '-' : formatPercent(deriv.periodTakerBuyRatio)} />
            </Col>
          </MetricGroup>
        ) : null}
      </Space>
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
    let cumulativeBid = 0;
    let cumulativeAsk = 0;
    return Array.from({ length: Math.min(max, 12) }, (_, idx) => ({
      key: idx,
      bid: bids[idx],
      ask: asks[idx],
      bidCumulative: bids[idx] ? (cumulativeBid += bids[idx].notional) : cumulativeBid,
      askCumulative: asks[idx] ? (cumulativeAsk += asks[idx].notional) : cumulativeAsk,
    }));
  }, [ob]);
  const totalBid = ob?.bidNotional ?? 0;
  const totalAsk = ob?.askNotional ?? 0;
  const totalDepth = totalBid + totalAsk;
  const topBidShare = totalBid > 0 ? (ob?.bids?.[0]?.notional ?? 0) / totalBid : 0;
  const topAskShare = totalAsk > 0 ? (ob?.asks?.[0]?.notional ?? 0) / totalAsk : 0;
  const depthImbalance = ob?.imbalance ?? 0;

  const columns: ColumnsType<{ key: number; bid?: MarketIntelLevel; ask?: MarketIntelLevel; bidCumulative: number; askCumulative: number }> = [
    {
      title: byLang('买价', 'Bid price'),
      dataIndex: 'bid',
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.price, 2) : '-',
    },
    {
      title: byLang('买量', 'Bid qty'),
      dataIndex: 'bid',
      responsive: ['md'],
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.qty, 4) : '-',
    },
    {
      title: byLang('买盘名义额', 'Bid notional'),
      dataIndex: 'bid',
      responsive: ['lg'],
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.notional, 0) : '-',
    },
    {
      title: byLang('买盘累计', 'Bid cumulative'),
      dataIndex: 'bidCumulative',
      responsive: ['xl'],
      render: (v: number) => formatNumber(v, 0),
    },
    {
      title: byLang('卖价', 'Ask price'),
      dataIndex: 'ask',
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.price, 2) : '-',
    },
    {
      title: byLang('卖量', 'Ask qty'),
      dataIndex: 'ask',
      responsive: ['md'],
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.qty, 4) : '-',
    },
    {
      title: byLang('卖盘名义额', 'Ask notional'),
      dataIndex: 'ask',
      responsive: ['lg'],
      render: (level?: MarketIntelLevel) => level ? formatNumber(level.notional, 0) : '-',
    },
    {
      title: byLang('卖盘累计', 'Ask cumulative'),
      dataIndex: 'askCumulative',
      responsive: ['xl'],
      render: (v: number) => formatNumber(v, 0),
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
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Row gutter={[12, 12]}>
            <Col xs={12} md={6}>
              <Statistic title={byLang('买盘深度', 'Bid depth')} value={formatNumber(totalBid, 0)} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('卖盘深度', 'Ask depth')} value={formatNumber(totalAsk, 0)} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('深度偏斜', 'Depth skew')} value={signedPercent(depthImbalance)} valueStyle={{ color: barColor(depthImbalance) }} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('价差', 'Spread')} value={formatPercent(ob?.spreadPct ?? 0, 3)} />
            </Col>
          </Row>
          <div>
            <Space wrap style={{ marginBottom: 6 }}>
              <Typography.Text type="secondary">{byLang('买卖盘深度占比', 'Bid/ask depth share')}</Typography.Text>
              <Tag color="red">{byLang('买盘', 'Bid')}: {totalDepth > 0 ? formatPercent(totalBid / totalDepth) : '-'}</Tag>
              <Tag color="blue">{byLang('卖盘', 'Ask')}: {totalDepth > 0 ? formatPercent(totalAsk / totalDepth) : '-'}</Tag>
              <Tag>{byLang('买一集中度', 'Top bid concentration')}: {formatPercent(topBidShare)}</Tag>
              <Tag>{byLang('卖一集中度', 'Top ask concentration')}: {formatPercent(topAskShare)}</Tag>
            </Space>
            <Progress
              percent={Math.round((totalDepth > 0 ? totalBid / totalDepth : 0) * 100)}
              success={{ percent: Math.round((totalDepth > 0 ? totalAsk / totalDepth : 0) * 100), strokeColor: '#1677ff' }}
              strokeColor="#ff4d4f"
              showInfo={false}
            />
          </div>
          <Table size="small" pagination={false} columns={columns} dataSource={rows} />
          <Typography.Text type="secondary">
            {byLang(
              '累计深度用于观察挂单墙和深度集中度；订单薄是快照，可能快速变化。',
              'Cumulative depth helps inspect displayed walls and concentration; the order book is a snapshot and can change quickly.',
            )}
          </Typography.Text>
        </Space>
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

function BasisPanel({ basis }: { basis?: MarketIntelBasis }) {
  const basisPct = basis?.basisPct;
  const color = basisPct == null ? undefined : basisPct > 0 ? '#ff4d4f' : basisPct < 0 ? '#1677ff' : '#52c41a';

  return (
    <Card title={byLang('期现价差', 'Spot-futures basis')}>
      {!basis?.ok ? (
        <Alert
          type="info"
          showIcon
          message={byLang('期现价差暂不可用', 'Spot-futures basis unavailable')}
          description={byLang(
            '需要同时取得 Spot 和 Futures 中间价后才能计算。',
            'Both Spot and Futures mid prices are required before basis can be calculated.',
          )}
        />
      ) : (
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Row gutter={[12, 12]}>
            <Col xs={12} md={6}>
              <Statistic title="Spot Mid" value={basis.spotMid ?? 0} precision={2} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title="Futures Mid" value={basis.futuresMid ?? 0} precision={2} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('价差', 'Basis')} value={basis.basis ?? 0} precision={2} valueStyle={{ color }} />
            </Col>
            <Col xs={12} md={6}>
              <Statistic title={byLang('价差比例', 'Basis pct')} value={basisPct == null ? '-' : signedPercent(basisPct, 3)} valueStyle={{ color }} />
            </Col>
          </Row>
          <Progress
            percent={Math.min(100, Math.round(Math.abs(basisPct ?? 0) * 5000))}
            showInfo={false}
            strokeColor={color}
          />
          <Typography.Text type="secondary">
            {byLang(
              '期现价差使用公开 Spot 和 USD-M Futures 中间价计算，用于监测期现结构变化，不构成交易建议。',
              'Basis is computed from public Spot and USD-M Futures mid prices to monitor structure changes; it is not trading advice.',
            )}
          </Typography.Text>
        </Space>
      )}
    </Card>
  );
}

function OpenInterestPanel({ venue }: { venue?: MarketIntelVenueSnapshot }) {
  const windows = venue?.derivatives?.openInterestWindows ?? [];
  const [period, setPeriod] = useState<OiPeriod>('15m');
  const selected = windows.find((item) => item.period === period) ?? windows[0];
  const points = useMemo(() => selected?.points ?? [], [selected]);
  const latestPoint = points.at(-1);
  const latestOpenInterest = selected?.latest ?? latestPoint?.openInterest ?? null;
  const latestChange = selected?.changePct ?? latestPoint?.changePct ?? null;
  const latestOpenInterestValue = latestPoint?.openInterestValue ?? null;
  const fundingRate = venue?.derivatives?.fundingRate;
  const option = useMemo(() => {
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const item = Array.isArray(params) ? params[0] : params;
          const point = points[item?.dataIndex];
          if (!point) return '';
          return [
            formatTs(point.ts),
            `${byLang('持仓量', 'Open interest')}: ${formatNumber(point.openInterest, 2)}`,
            `${byLang('变化', 'Change')}: ${point.changePct == null ? '-' : signedPercent(point.changePct, 3)}`,
            `${byLang('名义价值', 'Notional value')}: ${point.openInterestValue == null ? '-' : formatNumber(point.openInterestValue, 0)}`,
          ].join('<br/>');
        },
      },
      grid: { left: 56, right: 18, top: 18, bottom: 34 },
      xAxis: {
        type: 'category',
        data: points.map((point) => point.ts),
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: string) => formatTs(v, period === '1d' ? 'MM-DD' : 'MM-DD HH:mm') },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
        axisTick: { show: false },
      },
      yAxis: {
        type: 'value',
        scale: true,
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: number) => formatNumber(v, 0) },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      series: [
        {
          name: 'OI',
          type: 'line',
          data: points.map((point) => point.openInterest),
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2, color: '#faad14' },
          areaStyle: { opacity: 0.1, color: '#faad14' },
        },
      ],
    };
  }, [period, points]);

  return (
    <Card
      title={
        <Space wrap>
          <Typography.Text>{byLang('合约持仓量变化', 'Futures open interest change')}</Typography.Text>
          <Tag>OI</Tag>
        </Space>
      }
      extra={
        <Segmented
          value={period}
          options={OI_PERIODS.map((item) => ({ value: item, label: item }))}
          onChange={(value) => setPeriod(value as OiPeriod)}
        />
      }
    >
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Typography.Text type="secondary">
          {byLang(
            '这里按 15m、30m、1h、4h、1d 展示 Binance USD-M Futures 持仓量变化，用于监测合约仓位结构，不构成交易建议。',
            'This tracks Binance USD-M Futures open interest across 15m, 30m, 1h, 4h and 1d windows to monitor positioning structure, not trading advice.',
          )}
        </Typography.Text>
        {windows.length === 0 ? (
          <Empty description={byLang('暂无持仓量历史数据', 'No open interest history yet')} />
        ) : (
          <>
            <Row gutter={[12, 12]}>
              <Col xs={12} md={8} xl={4}>
                <Statistic title={byLang('当前周期', 'Period')} value={selected?.period ?? period} />
              </Col>
              <Col xs={12} md={8} xl={4}>
                <Statistic title={byLang('最新持仓量', 'Latest OI')} value={latestOpenInterest == null ? '-' : formatNumber(latestOpenInterest, 2)} />
              </Col>
              <Col xs={12} md={8} xl={4}>
                <Statistic
                  title={byLang('最近变化', 'Latest change')}
                  value={latestChange == null ? '-' : signedPercent(latestChange, 3)}
                  valueStyle={{ color: latestChange == null ? undefined : barColor(latestChange) }}
                />
              </Col>
              <Col xs={12} md={8} xl={4}>
                <Statistic title={byLang('样本数', 'Samples')} value={points.length} />
              </Col>
              <Col xs={12} md={8} xl={4}>
                <Statistic title={byLang('最新名义价值', 'Latest notional')} value={latestOpenInterestValue == null ? '-' : formatNumber(latestOpenInterestValue, 0)} />
              </Col>
              <Col xs={12} md={8} xl={4}>
                <Statistic title={byLang('资金费率', 'Funding')} value={fundingRate == null ? '-' : formatPercent(fundingRate, 4)} />
              </Col>
            </Row>
            <Typography.Text type="secondary">
              {byLang(
                'OI 上升或下降只表示合约持仓规模变化，不能直接说明多空方向。',
                'OI rising or falling only shows positioning size changes; it does not directly identify long or short direction.',
              )}
            </Typography.Text>
            <ReactECharts option={option} style={{ height: 260 }} notMerge lazyUpdate />
            <Table
              size="small"
              pagination={false}
              dataSource={windows.map((item) => ({ ...item, key: item.period }))}
              columns={[
                { title: byLang('周期', 'Period'), dataIndex: 'period' },
                { title: byLang('最新持仓量', 'Latest OI'), dataIndex: 'latest', render: (v: number | null) => v == null ? '-' : formatNumber(v, 2) },
                { title: byLang('最近变化', 'Latest change'), dataIndex: 'changePct', render: (v: number | null) => v == null ? '-' : signedPercent(v, 3) },
                { title: byLang('样本', 'Samples'), dataIndex: 'points', render: (v: unknown[]) => v.length },
              ]}
            />
          </>
        )}
      </Space>
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

function SessionResearchPanel({
  venue,
  timezone,
  lookbackBars,
}: {
  venue?: MarketIntelVenueSnapshot;
  timezone: SessionTimezone;
  lookbackBars: number;
}) {
  const sessionRows = useMemo(() => {
    const buckets = new Map<number, { hour: number; count: number; avgReturnPct: number; avgVolume: number }>();
    for (const row of venue?.sessionEffect ?? []) {
      const hour = displayHour(row.hourUtc, timezone);
      const existing = buckets.get(hour);
      if (!existing) {
        buckets.set(hour, { hour, count: row.count, avgReturnPct: row.avgReturnPct, avgVolume: row.avgVolume });
      } else {
        const totalCount = existing.count + row.count;
        buckets.set(hour, {
          hour,
          count: totalCount,
          avgReturnPct: totalCount > 0 ? ((existing.avgReturnPct * existing.count) + (row.avgReturnPct * row.count)) / totalCount : 0,
          avgVolume: totalCount > 0 ? ((existing.avgVolume * existing.count) + (row.avgVolume * row.count)) / totalCount : 0,
        });
      }
    }
    return Array.from({ length: 24 }, (_, hour) => buckets.get(hour) ?? { hour, count: 0, avgReturnPct: 0, avgVolume: 0 });
  }, [timezone, venue?.sessionEffect]);
  const totalBars = sessionRows.reduce((sum, row) => sum + row.count, 0);
  const avgVolume = totalBars > 0 ? sessionRows.reduce((sum, row) => sum + row.avgVolume * row.count, 0) / totalBars : 0;
  const activeHours = [...sessionRows].filter((row) => row.count > 0).sort((a, b) => b.avgVolume - a.avgVolume).slice(0, 4);
  const positiveHours = [...sessionRows].filter((row) => row.count > 0).sort((a, b) => b.avgReturnPct - a.avgReturnPct).slice(0, 3);
  const negativeHours = [...sessionRows].filter((row) => row.count > 0).sort((a, b) => a.avgReturnPct - b.avgReturnPct).slice(0, 3);
  const sampleCoverage = totalBars > 0 ? Math.min(1, totalBars / Math.max(1, lookbackBars)) : 0;
  const option = useMemo(() => ({
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      formatter: (params: any) => {
        const items = Array.isArray(params) ? params : [params];
        const idx = Number(items[0]?.dataIndex ?? 0);
        const row = sessionRows[idx];
        return [
          `${String(row?.hour ?? 0).padStart(2, '0')}:00 ${timezoneLabel(timezone)}`,
          `${byLang('均量', 'Avg volume')}: ${formatNumber(row?.avgVolume ?? 0, 2)}`,
          `${byLang('均值收益', 'Avg return')}: ${signedPercent(row?.avgReturnPct ?? 0, 3)}`,
          `${byLang('样本', 'Bars')}: ${row?.count ?? 0}`,
        ].join('<br/>');
      },
    },
    legend: { top: 0, textStyle: { color: 'rgba(215,226,240,0.72)' } },
    grid: { left: 52, right: 42, top: 36, bottom: 34 },
    xAxis: {
      type: 'category',
      data: sessionRows.map((row) => `${String(row.hour).padStart(2, '0')}:00`),
      axisLabel: { color: 'rgba(215,226,240,0.72)' },
      axisLine: { lineStyle: { color: 'rgba(255,255,255,0.14)' } },
      axisTick: { show: false },
    },
    yAxis: [
      {
        type: 'value',
        name: byLang('均量', 'Avg volume'),
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: number) => formatNumber(v, 0) },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } },
      },
      {
        type: 'value',
        name: byLang('收益', 'Return'),
        axisLabel: { color: 'rgba(215,226,240,0.72)', formatter: (v: number) => `${(v * 100).toFixed(1)}%` },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: byLang('均量', 'Avg volume'),
        type: 'bar',
        data: sessionRows.map((row) => row.avgVolume),
        itemStyle: { color: '#faad14' },
      },
      {
        name: byLang('均值收益', 'Avg return'),
        type: 'line',
        yAxisIndex: 1,
        smooth: true,
        data: sessionRows.map((row) => row.avgReturnPct),
        lineStyle: { width: 2, color: '#1677ff' },
      },
    ],
  }), [sessionRows, timezone]);

  return (
    <Card title={byLang('时间段效应研究', 'Session effect research')}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Row gutter={[12, 12]}>
          <Col xs={12} md={6}>
            <Statistic title={byLang('回看样本', 'Lookback bars')} value={lookbackBars} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('有效样本', 'Covered bars')} value={totalBars} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('样本覆盖', 'Sample coverage')} value={formatPercent(sampleCoverage)} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('全时段均量', 'All-hour avg volume')} value={formatNumber(avgVolume, 2)} />
          </Col>
        </Row>
        <Space wrap>
          <Typography.Text type="secondary">{byLang('活跃时段', 'Active hours')}:</Typography.Text>
          {activeHours.map((row) => (
            <Tag key={`active-${row.hour}`} color="gold">
              {String(row.hour).padStart(2, '0')}:00 {formatNumber(row.avgVolume, 0)}
            </Tag>
          ))}
        </Space>
        <Space wrap>
          <Typography.Text type="secondary">{byLang('收益偏正', 'Positive return bias')}:</Typography.Text>
          {positiveHours.map((row) => (
            <Tag key={`positive-${row.hour}`} color="red">
              {String(row.hour).padStart(2, '0')}:00 {signedPercent(row.avgReturnPct, 3)}
            </Tag>
          ))}
          <Typography.Text type="secondary">{byLang('收益偏负', 'Negative return bias')}:</Typography.Text>
          {negativeHours.map((row) => (
            <Tag key={`negative-${row.hour}`} color="blue">
              {String(row.hour).padStart(2, '0')}:00 {signedPercent(row.avgReturnPct, 3)}
            </Tag>
          ))}
        </Space>
        {totalBars === 0 ? (
          <Empty description={byLang('暂无足够样本进行时间段研究', 'Not enough samples for session research')} />
        ) : (
          <ReactECharts option={option} style={{ height: 300 }} notMerge lazyUpdate />
        )}
        <Typography.Text type="secondary">
          {byLang(
            '时间段效应用历史 15m K 线聚合，样本越长越适合观察稳定模式；样本少时只作为监测参考。',
            'Session effects aggregate historical 15m klines; longer samples are better for stable patterns, while sparse samples are monitoring context only.',
          )}
        </Typography.Text>
      </Space>
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
  const openConnections = connectionRows.filter((row) => row.status === 'open').length;
  const errorConnections = connectionRows.filter((row) => row.status === 'error').length;
  const updatedMs = stream?.updatedAt ? Date.parse(stream.updatedAt) : Number.NaN;
  const updateLagSeconds = Number.isFinite(updatedMs) ? Math.max(0, Math.round((Date.now() - updatedMs) / 1000)) : null;
  const isStale = updateLagSeconds != null && updateLagSeconds > 180;
  const runtimeType = errorConnections > 0 || stream?.status === 'stopped' || isStale ? 'warning' : 'info';
  const runtimeMessage = errorConnections > 0
    ? byLang('部分实时连接异常', 'Some live connections have errors')
    : stream?.status === 'running' && openConnections > 0
      ? byLang('实时采集器运行中', 'Live collector is running')
      : byLang('等待实时采集器连接', 'Waiting for live collector connections');

  return (
    <Card title={byLang('采集器状态', 'Collector status')}>
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Alert
          type={runtimeType}
          showIcon
          message={runtimeMessage}
          description={byLang(
            '正常空状态、样本积累、REST 局部失败和 WebSocket 连接错误分开显示，避免把无爆仓或刚启动误判为故障。',
            'Normal empty states, sample accumulation, partial REST failures and WebSocket connection errors are shown separately so no-liquidation or startup states are not treated as faults.',
          )}
        />
        <Row gutter={[12, 12]}>
          <Col xs={12} md={6}>
            <Statistic title={byLang('实时流', 'Stream')} value={stream?.status ?? 'stopped'} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('订阅流数量', 'Subscribed streams')} value={connectionRows.reduce((sum, row) => sum + (row.streams ?? 0), 0)} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('打开连接', 'Open connections')} value={openConnections} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('错误连接', 'Error connections')} value={errorConnections} />
          </Col>
          <Col xs={24} md={6}>
            <Statistic title={byLang('启动时间', 'Started')} value={stream?.startedAt ? formatTs(stream.startedAt) : '-'} />
          </Col>
          <Col xs={24} md={6}>
            <Statistic title={byLang('最近更新', 'Updated')} value={stream?.updatedAt ? formatTs(stream.updatedAt) : '-'} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('更新延迟', 'Update lag')} value={updateLagSeconds == null ? '-' : `${updateLagSeconds}s`} />
          </Col>
          <Col xs={12} md={6}>
            <Statistic title={byLang('最近错误数', 'Recent errors')} value={errorRows.length} />
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

function ExternalFeedGuide() {
  const newsRows = [
    {
      key: 'news-source',
      step: byLang('选择真实来源', 'Choose real sources'),
      detail: byLang('RSS、交易所公告、研究摘要或你自己的本地新闻 feed；每条必须带 source、url、publishedAt。', 'RSS, exchange announcements, research summaries or your own local news feed; each row needs source, url and publishedAt.'),
    },
    {
      key: 'news-normalize',
      step: byLang('先标准化再打分', 'Normalize before scoring'),
      detail: byLang('后端先保存标题、摘要、来源、时间和去重键，再调用 NLP；模型输出只能标为情绪估计。', 'Backend stores title, summary, source, time and dedupe key first, then calls NLP; model output is only a sentiment estimate.'),
    },
    {
      key: 'news-display',
      step: byLang('前端显示来源和时间', 'Show source and time'),
      detail: byLang('列表展示新闻来源、发布时间、相关资产、情绪分数和置信度；不要把情绪当成事实或交易信号。', 'List source, publish time, related assets, sentiment score and confidence; do not treat sentiment as fact or a trading signal.'),
    },
  ];
  const onchainRows = [
    {
      key: 'chain-source',
      step: byLang('接入聚合源或节点', 'Use an aggregator or node'),
      detail: byLang('可用链上 API、索引器或自建节点；密钥只放服务器环境变量或密钥文件，不进入 GitHub。', 'Use on-chain APIs, indexers or a self-hosted node; keys stay in server environment variables or secret files and never enter GitHub.'),
    },
    {
      key: 'chain-aggregate',
      step: byLang('存轻量聚合', 'Store light aggregates'),
      detail: byLang('优先保存小时级或 5 分钟级聚合，例如交易所净流入、稳定币流量、活跃地址、gas；不默认写入高频原始事件。', 'Prefer hourly or 5-minute aggregates such as exchange netflow, stablecoin flow, active addresses and gas; do not write high-frequency raw events by default.'),
    },
    {
      key: 'chain-quality',
      step: byLang('标明覆盖范围', 'Mark coverage'),
      detail: byLang('每个指标显示 chain、asset、source、updatedAt 和缺失原因，避免把未覆盖链误解为零值。', 'Every metric should show chain, asset, source, updatedAt and missing-data reason so uncovered chains are not confused with zero values.'),
    },
  ];

  return (
    <Row gutter={[16, 16]}>
      <Col xs={24} lg={12}>
        <Card title={byLang('新闻 NLP / 情绪接入', 'News NLP / sentiment integration')}>
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Alert
              type="info"
              showIcon
              message={byLang('当前未配置新闻源', 'No news source configured')}
              description={byLang(
                '接入前保持 source_not_configured；配置真实 RSS 或本地 feed 后再展示新闻和模型情绪。',
                'Keep source_not_configured before integration; show news and model sentiment only after a real RSS or local feed is configured.',
              )}
            />
            <Table
              size="small"
              pagination={false}
              dataSource={newsRows}
              columns={[
                { title: byLang('步骤', 'Step'), dataIndex: 'step' },
                { title: byLang('要求', 'Requirement'), dataIndex: 'detail' },
              ]}
            />
          </Space>
        </Card>
      </Col>
      <Col xs={24} lg={12}>
        <Card title={byLang('链上数据接入', 'On-chain data integration')}>
          <Space direction="vertical" size={12} style={{ width: '100%' }}>
            <Alert
              type="info"
              showIcon
              message={byLang('当前未配置链上数据源', 'No on-chain source configured')}
              description={byLang(
                '先接入真实 provider 或节点，再展示聚合指标；未覆盖的链和资产必须显示未配置或数据不足。',
                'Integrate a real provider or node before showing aggregates; uncovered chains and assets must show not configured or insufficient data.',
              )}
            />
            <Table
              size="small"
              pagination={false}
              dataSource={onchainRows}
              columns={[
                { title: byLang('步骤', 'Step'), dataIndex: 'step' },
                { title: byLang('要求', 'Requirement'), dataIndex: 'detail' },
              ]}
            />
          </Space>
        </Card>
      </Col>
    </Row>
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
              { title: byLang('价格', 'Price'), dataIndex: 'price', responsive: ['md'], render: (v: number) => formatNumber(v, 2) },
              { title: byLang('数量', 'Qty'), dataIndex: 'qty', responsive: ['lg'], render: (v: number) => formatNumber(v, 4) },
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
  const [lookbackBars, setLookbackBars] = useState<MarketLookbackBars>(672);
  const [sessionTimezone, setSessionTimezone] = useState<SessionTimezone>('utc');
  const query = useMarketIntelSummaryQuery(symbol, streamWindowSeconds, lookbackBars);
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
  const lookbackOptions = MARKET_LOOKBACK_OPTIONS.map((value) => ({
    value,
    label: lookbackLabel(value),
  }));

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
          <Select
            style={{ width: isMobile ? '100%' : 132 }}
            value={lookbackBars}
            options={lookbackOptions}
            onChange={(value) => setLookbackBars(Number(value) as MarketLookbackBars)}
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

      <MarketContextPanel
        data={data}
        primaryVenue={primaryVenue}
        streamWindowLabel={streamWindowOptions.find((item) => item.value === (data?.stream?.windowSeconds ?? streamWindowSeconds))?.label}
        loading={query.isPending}
        refreshing={query.isFetching}
        onRefresh={() => void query.refetch()}
      />

      <StructureInsightPanel
        selectedVenue={selectedVenue}
        secondaryVenue={secondaryVenue}
        basis={data?.basis}
        stream={data?.stream}
        liquidationRows={liquidationRows}
        correlationBreaks={data?.correlation.breaks}
        streamWindowSeconds={streamWindowSeconds}
      />

      <MetricDirectory
        selectedVenue={selectedVenue}
        liquidationCount={liquidationRows.length}
        rollingCount={data?.correlation.rolling?.length ?? 0}
        basisOk={data?.basis?.ok}
      />

      <SignalOverviewPanel
        selectedVenue={selectedVenue}
        basis={data?.basis}
        liquidationCount={liquidationRows.length}
        rollingCount={data?.correlation.rolling?.length ?? 0}
      />

      <StructureConsistencyPanel
        selectedVenue={selectedVenue}
        secondaryVenue={secondaryVenue}
        basis={data?.basis}
      />

      <MarketSection
        id="market-futures-positioning"
        title={byLang('合约持仓与期现结构', 'Futures positioning and basis')}
        description={byLang(
          '这里集中查看合约持仓量、资金费率和期现价差；持仓量支持 15m、30m、1h、4h、1d 多周期横坐标。',
          'Use this section for open interest, funding and basis; OI supports 15m, 30m, 1h, 4h and 1d time axes.',
        )}
      >
        <Row gutter={[16, 16]}>
          <Col xs={24} xl={14}>
            <OpenInterestPanel venue={data?.venues.futures} />
          </Col>
          <Col xs={24} xl={10}>
            <BasisPanel basis={data?.basis} />
          </Col>
        </Row>
      </MarketSection>

      <MarketSection
        id="market-live-flow"
        title={byLang('实时盘口与主动流', 'Live book and taker flow')}
        description={byLang(
          '核心查看 Taker ratio、OFI、Level 2 订单薄和 Spot/Futures 主辅视角。',
          'Use this core section for Taker ratio, OFI, Level 2 order book and Spot/Futures primary-secondary views.',
        )}
      >
        <MicrostructureFocusPanel
          venue={selectedVenue}
          secondaryVenue={secondaryVenue}
          streamWindowSeconds={streamWindowSeconds}
        />
        <Row gutter={[16, 16]}>
          <Col xs={24} xl={14}>
            <VenueCard venue={selectedVenue} isPrimary streamWindowSeconds={streamWindowSeconds} />
          </Col>
          <Col xs={24} xl={10}>
            <VenueCard venue={secondaryVenue} isPrimary={false} streamWindowSeconds={streamWindowSeconds} />
          </Col>
        </Row>
      </MarketSection>

      <MarketSection
        id="market-level2"
        title={byLang('Level 2 订单薄', 'Level 2 order book')}
        description={byLang(
          '这里只放主视角订单薄快照，方便查看买卖盘价格、数量和名义额。',
          'This section only shows the primary-view book snapshot for bid/ask price, quantity and notional.',
        )}
      >
        <OrderbookTable venue={selectedVenue} />
      </MarketSection>

      <MarketSection
        id="market-liquidations"
        title={byLang('爆仓流', 'Liquidation flow')}
        description={byLang(
          '这里集中查看最近强平记录、多空名义额和最近 5 分钟比例；没有爆仓不是错误。',
          'Use this section for recent forced orders, long/short notional and last-5m ratio; no liquidation is not an error.',
        )}
      >
        <LiquidationPanel rows={liquidationRows} status={data?.liquidations.status} apiAggregate={data?.liquidations.aggregate} />
      </MarketSection>

      <MarketSection
        id="market-time-structure"
        title={byLang('时间结构', 'Time structure')}
        description={byLang(
          '按小时和星期观察收益与成交量的历史分布；样本较少时只作为监测参考。',
          'Monitor historical return and volume distribution by hour and weekday; sparse samples are reference context only.',
        )}
      >
        <Space wrap style={{ justifyContent: 'space-between', width: '100%' }}>
          <Typography.Text type="secondary">
            {byLang('显示时区', 'Display timezone')}: {timezoneLabel(sessionTimezone)} · {byLang('回看', 'Lookback')}: {lookbackLabel(data?.lookbackBars ?? lookbackBars)}
          </Typography.Text>
          <Segmented
            value={sessionTimezone}
            options={[
              { value: 'utc', label: 'UTC' },
              { value: 'asia-shanghai', label: 'Asia-Shanghai' },
            ]}
            onChange={(value) => setSessionTimezone(value as SessionTimezone)}
          />
        </Space>
        <SessionResearchPanel venue={selectedVenue} timezone={sessionTimezone} lookbackBars={data?.lookbackBars ?? lookbackBars} />
        <Row gutter={[16, 16]}>
          <Col xs={24} xl={10}>
            <SessionEffectTable venue={selectedVenue} timezone={sessionTimezone} />
          </Col>
          <Col xs={24} xl={14}>
            <SessionHeatmap rows={selectedVenue?.sessionHeatmap} timezone={sessionTimezone} />
          </Col>
        </Row>
      </MarketSection>

      <MarketSection
        id="market-cross-asset"
        title={byLang('跨资产结构', 'Cross-asset structure')}
        description={byLang(
          '滚动相关展示联动关系的变化过程，矩阵保留当前截面；相关性变化只是结构监测。',
          'Rolling correlation shows how relationships evolve, while the matrix keeps the current cross-section; correlation changes are structure monitors.',
        )}
      >
        <RollingCorrelationPanel rows={data?.correlation.rolling} breaks={data?.correlation.breaks} />
        <CorrelationHeatmap data={data?.correlation.matrix} />
      </MarketSection>

      <MarketSection
        id="market-runtime"
        title={byLang('运行状态与未配置数据源', 'Runtime and unconfigured feeds')}
        description={byLang(
          '检查采集器连接、爆仓流和暂未接入的数据源，区分正常空状态与数据源问题。',
          'Check collector connections, liquidation flow and unconfigured feeds, separating normal empty states from source issues.',
        )}
      >
        <StreamObservability stream={data?.stream} />
        <ExternalFeedGuide />
      </MarketSection>
    </div>
  );
}
