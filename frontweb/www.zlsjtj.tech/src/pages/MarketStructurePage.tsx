import { useEffect, useMemo, useState } from 'react';
import { Alert, Card, Col, Empty, Grid, Progress, Row, Select, Space, Statistic, Table, Tag, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';

import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useMarketIntelSummaryQuery } from '@/hooks/queries/market';
import { byLang } from '@/i18n';
import type { MarketIntelLevel, MarketIntelLiquidation, MarketIntelSessionEffect, MarketIntelVenueSnapshot } from '@/types/api';
import { formatNumber, formatPercent, formatTs } from '@/utils/format';

function compactSymbol(symbol: string) {
  return symbol.replace('/USDT:USDT', '').replace('/USDT', '').replace('USDT', '');
}

function signedPercent(value?: number | null, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return '-';
  const sign = value > 0 ? '+' : '';
  return `${sign}${formatPercent(value, digits)}`;
}

function barColor(value: number) {
  if (value >= 0.15) return '#ff4d4f';
  if (value <= -0.15) return '#1677ff';
  return '#52c41a';
}

function VenueCard({ venue }: { venue?: MarketIntelVenueSnapshot }) {
  if (!venue) return null;
  const ob = venue.orderbook;
  const flow = venue.flow;
  const deriv = venue.derivatives;
  const stream = venue.stream;
  return (
    <Card
      title={
        <Space wrap>
          <Typography.Text strong>{venue.venue === 'spot' ? byLang('现货', 'Spot') : byLang('合约', 'Futures')}</Typography.Text>
          <Tag>{compactSymbol(venue.symbol)}</Tag>
        </Space>
      }
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
        <Typography.Text type="secondary">
          {byLang('订单薄偏斜：左侧偏卖盘，右侧偏买盘。', 'Book skew: left leans ask-heavy, right leans bid-heavy.')}
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
    <Card title={byLang('Level 2 订单薄', 'Level 2 order book')}>
      {rows.length === 0 ? (
        <Empty description={byLang('暂无订单薄快照', 'No book snapshot')} />
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

function SessionEffectTable({ rows }: { rows?: MarketIntelSessionEffect[] }) {
  const topRows = [...(rows ?? [])]
    .sort((a, b) => Math.abs(b.avgReturnPct) - Math.abs(a.avgReturnPct))
    .slice(0, 8)
    .sort((a, b) => a.hourUtc - b.hourUtc);
  return (
    <Card title={byLang('时间段效应', 'Session effect')}>
      {topRows.length === 0 ? (
        <Empty description={byLang('暂无分时统计', 'No session stats')} />
      ) : (
        <Table
          size="small"
          pagination={false}
          dataSource={topRows.map((row) => ({ ...row, key: row.hourUtc }))}
          columns={[
            { title: byLang('UTC 小时', 'UTC hour'), dataIndex: 'hourUtc', render: (v: number) => `${String(v).padStart(2, '0')}:00` },
            { title: byLang('均值收益', 'Avg return'), dataIndex: 'avgReturnPct', render: (v: number) => signedPercent(v, 3) },
            { title: byLang('均量', 'Avg volume'), dataIndex: 'avgVolume', render: (v: number) => formatNumber(v, 2) },
            { title: byLang('样本', 'Bars'), dataIndex: 'count' },
          ]}
        />
      )}
    </Card>
  );
}

export function MarketStructurePage() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const [symbol, setSymbol] = useState<string | undefined>(undefined);
  const query = useMarketIntelSummaryQuery(symbol);
  const data = query.data;
  const selectedVenue = data?.venues.futures?.ok ? data.venues.futures : data?.venues.spot;
  const liquidationRows = (data?.liquidations.rows ?? []) as MarketIntelLiquidation[];

  useEffect(() => {
    if (!symbol && data?.selectedSymbol) setSymbol(data.selectedSymbol);
  }, [data?.selectedSymbol, symbol]);

  const options = (data?.symbols ?? ['BTC/USDT:USDT', 'ETH/USDT:USDT']).map((item) => ({
    value: item,
    label: item,
  }));

  return (
    <div className="page-shell">
      <div className="page-header">
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            {byLang('市场结构', 'Market Structure')}
          </Typography.Title>
          <Typography.Text type="secondary">
            {byLang('现货和合约分开看；资金费率、持仓量、爆仓只属于合约侧。', 'Spot and futures are separate; funding, OI and liquidations belong to derivatives.')}
          </Typography.Text>
        </div>
        <Select
          style={{ width: isMobile ? '100%' : 240 }}
          value={symbol ?? data?.selectedSymbol}
          options={options}
          onChange={(value) => setSymbol(value)}
        />
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
          <Typography.Text type="secondary">{byLang('更新时间', 'Updated')}: {formatTs(data?.ts)}</Typography.Text>
          <Typography.Text type="secondary">{byLang('周期', 'Interval')}: {data?.interval ?? '15m'}</Typography.Text>
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
        <Col xs={24} xl={12}>
          <VenueCard venue={data?.venues.spot} />
        </Col>
        <Col xs={24} xl={12}>
          <VenueCard venue={data?.venues.futures} />
        </Col>
      </Row>

      <Row gutter={[16, 16]}>
        <Col xs={24} xl={12}>
          <OrderbookTable venue={selectedVenue} />
        </Col>
        <Col xs={24} xl={12}>
          <SessionEffectTable rows={selectedVenue?.sessionEffect} />
        </Col>
      </Row>

      <CorrelationHeatmap data={data?.correlation.matrix} />

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={12}>
          <Card title={byLang('爆仓流', 'Liquidations')}>
            {liquidationRows.length === 0 ? (
              <Typography.Text type="secondary">
                {data?.liquidations.status === 'running'
                  ? byLang('实时流已连接；只有发生强平时这里才会出现记录。', 'Stream is connected; rows appear only when a liquidation occurs.')
                  : byLang('爆仓实时流未运行。', 'Liquidation stream is not running.')}
              </Typography.Text>
            ) : (
              <Table
                size="small"
                pagination={false}
                dataSource={liquidationRows.slice(0, 8).map((row, idx) => ({ ...row, key: `${row.ts}-${idx}` }))}
                columns={[
                  { title: byLang('时间', 'Time'), dataIndex: 'ts', render: (v: string) => formatTs(v) },
                  { title: byLang('标的', 'Symbol'), dataIndex: 'symbol' },
                  { title: byLang('方向', 'Side'), dataIndex: 'side' },
                  { title: byLang('名义额', 'Notional'), dataIndex: 'notional', render: (v: number) => formatNumber(v, 0) },
                ]}
              />
            )}
          </Card>
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
