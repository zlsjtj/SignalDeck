import { useEffect, useMemo } from 'react';
import { Button, Card, Col, Descriptions, Empty, Grid, Row, Space, Statistic, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { Link, useNavigate, useParams } from 'react-router-dom';

import { DrawdownChart } from '@/components/charts/DrawdownChart';
import { EquityChart } from '@/components/charts/EquityChart';
import { BacktestLogsPanel } from '@/components/backtests/BacktestLogsPanel';
import { BacktestStatusTag } from '@/components/backtests/BacktestStatusTag';
import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useBacktestLogsQuery, useBacktestQuery } from '@/hooks/queries/backtests';
import { byLang, useI18n } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import type { BacktestTrade } from '@/types/api';
import { downloadCsv } from '@/utils/csv';
import { formatNumber, formatPercent, formatTs } from '@/utils/format';

export function BacktestDetailPage() {
  const navigate = useNavigate();
  const { t } = useI18n();
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { id } = useParams();
  const btQuery = useBacktestQuery(id);
  const logsQuery = useBacktestLogsQuery(id, true);

  const detail = btQuery.data;

  useEffect(() => {
    if (!detail?.strategyId) return;
    setSelectedLiveStrategyId(detail.strategyId);
  }, [detail?.strategyId, setSelectedLiveStrategyId]);

  const equityPoints = useMemo(
    () => (detail?.equityCurve ?? []).map((p) => ({ ts: p.ts, equity: p.equity })),
    [detail?.equityCurve],
  );

  const drawdownPoints = useMemo(
    () => (detail?.drawdownCurve ?? []).map((p) => ({ ts: p.ts, dd: p.dd })),
    [detail?.drawdownCurve],
  );

  const tradeColumns = useMemo<ColumnsType<BacktestTrade>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('symbol'), dataIndex: 'symbol', width: 120 },
      {
        title: t('side'),
        dataIndex: 'side',
        width: 100,
        filters: [
          { text: 'buy', value: 'buy' },
          { text: 'sell', value: 'sell' },
        ],
        onFilter: (value, record) => record.side === value,
      },
      { title: t('qty'), dataIndex: 'qty', width: 100, render: (v: number) => formatNumber(v, 4) },
      { title: t('priceLabel'), dataIndex: 'price', width: 120, render: (v: number) => formatNumber(v, 2) },
      { title: t('fee'), dataIndex: 'fee', width: 100, render: (v: number) => formatNumber(v, 2) },
      {
        title: t('pnlTotal'),
        dataIndex: 'pnl',
        width: 120,
        sorter: (a, b) => a.pnl - b.pnl,
        render: (v: number) => (
          <Typography.Text style={{ color: v >= 0 ? '#00b96b' : '#ff4d4f' }}>
            {formatNumber(v, 2)}
          </Typography.Text>
        ),
      },
    ],
    [t],
  );

  const exportTrades = () => {
    const rows = (detail?.trades ?? []).map((t) => ({
      ts: t.ts,
      symbol: t.symbol,
      side: t.side,
      qty: t.qty,
      price: t.price,
      fee: t.fee,
      pnl: t.pnl,
      orderId: t.orderId ?? '',
    }));
    downloadCsv(`backtest_${id}_trades.csv`, rows, ['ts', 'symbol', 'side', 'qty', 'price', 'fee', 'pnl', 'orderId']);
  };

  return (
    <div className="page-shell">
      <div className="page-header">
        <Typography.Title level={3} style={{ margin: 0 }}>
          {t('backtestDetail')}
        </Typography.Title>
        <Space wrap className="page-actions">
          <Button>
            <Link to="/backtests">{t('backToList')}</Link>
          </Button>
          {detail?.strategyId ? (
            <Button
              onClick={() => {
                setSelectedLiveStrategyId(detail.strategyId);
                navigate(`/strategies/${detail.strategyId}`);
              }}
            >
              {t('strategy')}
            </Button>
          ) : null}
          <Button
            onClick={() => {
              if (detail?.strategyId) setSelectedLiveStrategyId(detail.strategyId);
              navigate('/live');
            }}
          >
            {t('liveMonitor')}
          </Button>
          <Button
            onClick={() => {
              if (detail?.strategyId) setSelectedLiveStrategyId(detail.strategyId);
              navigate('/risk');
            }}
          >
            {t('risk')}
          </Button>
          {detail ? <BacktestStatusTag status={detail.status} /> : null}
        </Space>
      </div>
      <NonTechGuideCard
        title={byLang('回测结果怎么解读', 'How to read backtest result')}
        summary={byLang(
          '先看风险，再看收益。最大回撤过大时，不建议直接实盘。',
          'Check risk first, then return. Do not move to live trading with excessive drawdown.',
        )}
        steps={[
          byLang('先看“最大回撤”是否可接受', 'Check max drawdown first'),
          byLang('再看总盈亏、胜率、交易次数', 'Then inspect PnL, win rate, trade count'),
          byLang('必要时导出交易明细做复盘', 'Export trades for deeper review'),
        ]}
      />

      <Card loading={btQuery.isPending}>
        {btQuery.isError || !detail ? (
          <ActionableErrorAlert
            title={byLang(`回测详情加载失败：${id ?? '-'}`, `Failed to load backtest detail: ${id ?? '-'}`)}
            steps={[
              byLang('返回回测列表确认任务仍存在', 'Return to list and confirm this task still exists'),
              byLang('点击“重试”重新加载详情', 'Click Retry to load detail again'),
              byLang('仍失败时进入日志中心查看系统错误', 'If still failing, check system logs in Logs Center'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void btQuery.refetch()}
            secondaryActionText={byLang('打开日志中心', 'Open Logs Center')}
            onSecondaryAction={() => navigate('/logs')}
          />
        ) : (
          <Descriptions column={isMobile ? 1 : 2} bordered size="small">
            <Descriptions.Item label={t('id')}>{detail.id}</Descriptions.Item>
            <Descriptions.Item label={t('strategy')}>{detail.strategyName}</Descriptions.Item>
            <Descriptions.Item label={t('symbol')}>{detail.symbol}</Descriptions.Item>
            <Descriptions.Item label={t('status')}>
              <BacktestStatusTag status={detail.status} />
              {detail.status === 'running' ? ` (${Math.round(detail.progress ?? 0)}%)` : null}
            </Descriptions.Item>
            <Descriptions.Item label={t('range')} span={2}>
              {formatTs(detail.startAt)} ~ {formatTs(detail.endAt)}
            </Descriptions.Item>
            <Descriptions.Item label={t('initialCapital')}>{formatNumber(detail.initialCapital, 2)}</Descriptions.Item>
            <Descriptions.Item label={t('feeSlippage')}>
              {formatPercent(detail.feeRate, 4)} / {formatPercent(detail.slippage, 4)}
            </Descriptions.Item>
          </Descriptions>
        )}
      </Card>

      {detail ? (
        <Row gutter={[16, 16]}>
          <Col xs={24} md={12} lg={8}>
            <Card>
              <Statistic title={t('cagr')} value={formatPercent(detail.metrics.cagr)} />
            </Card>
          </Col>
          <Col xs={24} md={12} lg={8}>
            <Card>
              <Statistic title={t('sharpe')} value={detail.metrics.sharpe} precision={2} />
            </Card>
          </Col>
          <Col xs={24} md={12} lg={8}>
            <Card>
              <Statistic title={t('calmar')} value={detail.metrics.calmar} precision={2} />
            </Card>
          </Col>
          <Col xs={24} md={12} lg={8}>
            <Card>
              <Statistic title={t('maxdd')} value={formatPercent(detail.metrics.maxDrawdown)} valueStyle={{ color: '#ff4d4f' }} />
            </Card>
          </Col>
          <Col xs={24} md={12} lg={8}>
            <Card>
              <Statistic title={t('winRate')} value={formatPercent(detail.metrics.winRate)} />
            </Card>
          </Col>
          <Col xs={24} md={12} lg={8}>
            <Card>
              <Statistic
                title={t('pnlTotal')}
                value={detail.metrics.pnlTotal}
                precision={2}
                valueStyle={{ color: detail.metrics.pnlTotal >= 0 ? '#00b96b' : '#ff4d4f' }}
                suffix={`(${detail.metrics.trades} ${t('trades')})`}
              />
            </Card>
          </Col>
        </Row>
      ) : null}

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card title={t('equityCurve')}>
            <EquityChart data={equityPoints} height={300} />
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <Card title={t('drawdownCurve')}>
            <DrawdownChart data={drawdownPoints} height={300} />
          </Card>
        </Col>
      </Row>

      <Card
        title={
          <div className="page-header">
            <Typography.Text strong>{t('tradesDetail')}</Typography.Text>
            <Button size="small" onClick={exportTrades} disabled={!detail || detail.trades.length === 0}>
              {t('exportCsv')}
            </Button>
          </div>
        }
      >
        {detail && detail.trades.length === 0 ? (
          <Empty description={t('noTrades')} />
        ) : (
          <Table
            rowKey="id"
            size="small"
            loading={btQuery.isPending}
            columns={tradeColumns}
            dataSource={detail?.trades ?? []}
            pagination={{ pageSize: 10, showSizeChanger: true }}
            scroll={{ x: 860 }}
          />
        )}
      </Card>

      <BacktestLogsPanel logs={logsQuery.data ?? []} loading={logsQuery.isPending} />
    </div>
  );
}
