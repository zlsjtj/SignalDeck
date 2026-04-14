import { useEffect, useMemo, useRef } from 'react';
import { Button, Card, DatePicker, Empty, Form, Grid, Input, InputNumber, Select, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import type { Dayjs } from 'dayjs';
import { useNavigate } from 'react-router-dom';
import { PlusOutlined } from '@ant-design/icons';

import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { BacktestStatusTag } from '@/components/backtests/BacktestStatusTag';
import { useCreateBacktestMutation, useBacktestsQuery } from '@/hooks/queries/backtests';
import { useStrategiesQuery } from '@/hooks/queries/strategies';
import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';
import type { Backtest, CreateBacktestRequest } from '@/types/api';
import { formatNumber, formatTs } from '@/utils/format';

type BacktestFormValues = {
  strategyId: string;
  symbol: string;
  range: [Dayjs, Dayjs];
  initialCapital: number;
  feeRate: number;
  slippage: number;
};

function normalizeBacktestsListStrategyFilter(value: unknown): string | 'all' {
  if (typeof value !== 'string') return 'all';
  const normalized = value.trim();
  return normalized ? normalized : 'all';
}

function normalizeBacktestsListPageSize(value: unknown): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed)) return 10;
  const normalized = Math.round(parsed);
  return normalized === 10 || normalized === 20 || normalized === 50 || normalized === 100 ? normalized : 10;
}

function normalizeBacktestsCreateSymbol(value: unknown): string {
  if (typeof value !== 'string') return 'BTCUSDT';
  const normalized = value.trim().toUpperCase();
  return normalized || 'BTCUSDT';
}

function normalizeBacktestsCreateNumber(value: unknown, fallback: number, min: number, max?: number): number {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  const lowered = Math.max(min, parsed);
  if (typeof max === 'number') return Math.min(max, lowered);
  return lowered;
}

function normalizeBacktestsCreateRange(startValue: unknown, endValue: unknown): [Dayjs, Dayjs] {
  const defaultEnd = dayjs();
  const defaultStart = defaultEnd.subtract(7, 'day');
  if (typeof startValue !== 'string' || typeof endValue !== 'string') {
    return [defaultStart, defaultEnd];
  }
  const start = dayjs(startValue);
  const end = dayjs(endValue);
  if (!start.isValid() || !end.isValid() || start.isAfter(end)) {
    return [defaultStart, defaultEnd];
  }
  return [start, end];
}

export function BacktestsPage() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const isGuest = useAppStore((s) => s.isGuest);
  const selectedStrategyId = useAppStore((s) => s.selectedLiveStrategyId);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const backtestsFilters = useAppStore((s) => s.backtestsFilters);
  const setBacktestsFilters = useAppStore((s) => s.setBacktestsFilters);
  const navigate = useNavigate();
  const { t } = useI18n();
  const { data: strategies } = useStrategiesQuery();
  const { data, isPending, isError, refetch } = useBacktestsQuery();
  const createMutation = useCreateBacktestMutation();
  const [form] = Form.useForm<BacktestFormValues>();
  const saveFiltersTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const savedFilters =
    backtestsFilters && typeof backtestsFilters === 'object' && !Array.isArray(backtestsFilters)
      ? (backtestsFilters as Record<string, unknown>)
      : {};
  const listStrategyFilter = normalizeBacktestsListStrategyFilter(savedFilters.listStrategyId);
  const listPageSize = normalizeBacktestsListPageSize(savedFilters.listPageSize);
  const createSymbolDefault = normalizeBacktestsCreateSymbol(savedFilters.createSymbol);
  const createInitialCapitalDefault = normalizeBacktestsCreateNumber(savedFilters.createInitialCapital, 100_000, 0);
  const createFeeRateDefault = normalizeBacktestsCreateNumber(savedFilters.createFeeRate, 0.0006, 0, 0.1);
  const createSlippageDefault = normalizeBacktestsCreateNumber(savedFilters.createSlippage, 0.0002, 0, 0.1);
  const [createRangeStartDefault, createRangeEndDefault] = useMemo(
    () => normalizeBacktestsCreateRange(savedFilters.createStartAt, savedFilters.createEndAt),
    [savedFilters.createStartAt, savedFilters.createEndAt],
  );

  const columns = useMemo<ColumnsType<Backtest>>(
    () => [
      {
        title: t('strategy'),
        dataIndex: 'strategyName',
        render: (_, r) => (
          <Button type="link" onClick={() => navigate(`/strategies/${r.strategyId}`)} style={{ padding: 0 }}>
            {r.strategyName}
          </Button>
        ),
      },
      { title: t('symbol'), dataIndex: 'symbol', width: 120 },
      {
        title: t('status'),
        dataIndex: 'status',
        width: 120,
        filters: [
          { text: 'running', value: 'running' },
          { text: 'success', value: 'success' },
          { text: 'failed', value: 'failed' },
        ],
        onFilter: (value, record) => record.status === value,
        render: (v) => <BacktestStatusTag status={v} />,
      },
      {
        title: t('progress'),
        dataIndex: 'progress',
        width: 120,
        render: (v: number | undefined, r) => (r.status === 'running' ? `${Math.round(v ?? 0)}%` : '-'),
      },
      {
        title: t('createdAt'),
        dataIndex: 'createdAt',
        width: 180,
        sorter: (a, b) => (a.createdAt < b.createdAt ? -1 : 1),
        defaultSortOrder: 'descend',
        render: (v: string) => formatTs(v),
      },
      {
        title: t('actions'),
        key: 'actions',
        width: 120,
        render: (_, r) => (
          <Button
            size="small"
            onClick={() => {
              setSelectedLiveStrategyId(r.strategyId);
              navigate(`/backtests/${r.id}`);
            }}
          >
            {t('details')}
          </Button>
        ),
      },
    ],
    [navigate, setSelectedLiveStrategyId, t],
  );

  const defaultStrategyId = selectedStrategyId || strategies?.[0]?.id;

  useEffect(() => {
    if (!selectedStrategyId) {
      const first = (strategies ?? []).find((s) => !!s.id);
      if (first?.id) setSelectedLiveStrategyId(first.id);
    }
  }, [selectedStrategyId, setSelectedLiveStrategyId, strategies]);

  useEffect(() => {
    const cur = form.getFieldValue('strategyId');
    if (!cur && defaultStrategyId) form.setFieldValue('strategyId', defaultStrategyId);
  }, [defaultStrategyId, form]);

  useEffect(() => {
    const curSymbol = String(form.getFieldValue('symbol') ?? '').trim();
    if (!curSymbol) form.setFieldValue('symbol', createSymbolDefault);

    const curRange = form.getFieldValue('range');
    const rangeValid =
      Array.isArray(curRange) &&
      curRange.length === 2 &&
      dayjs(curRange[0]).isValid() &&
      dayjs(curRange[1]).isValid() &&
      !dayjs(curRange[0]).isAfter(dayjs(curRange[1]));
    if (!rangeValid) form.setFieldValue('range', [createRangeStartDefault, createRangeEndDefault]);

    const curInitialCapital = form.getFieldValue('initialCapital');
    if (typeof curInitialCapital !== 'number' || !Number.isFinite(curInitialCapital) || curInitialCapital < 0) {
      form.setFieldValue('initialCapital', createInitialCapitalDefault);
    }

    const curFeeRate = form.getFieldValue('feeRate');
    if (typeof curFeeRate !== 'number' || !Number.isFinite(curFeeRate) || curFeeRate < 0 || curFeeRate > 0.1) {
      form.setFieldValue('feeRate', createFeeRateDefault);
    }

    const curSlippage = form.getFieldValue('slippage');
    if (typeof curSlippage !== 'number' || !Number.isFinite(curSlippage) || curSlippage < 0 || curSlippage > 0.1) {
      form.setFieldValue('slippage', createSlippageDefault);
    }
  }, [
    createFeeRateDefault,
    createInitialCapitalDefault,
    createRangeEndDefault,
    createRangeStartDefault,
    createSlippageDefault,
    createSymbolDefault,
    form,
  ]);

  const filteredData = useMemo(() => {
    const rows = data ?? [];
    if (listStrategyFilter === 'all') return rows;
    return rows.filter((r) => r.strategyId === listStrategyFilter);
  }, [data, listStrategyFilter]);

  useEffect(() => {
    return () => {
      if (!saveFiltersTimerRef.current) return;
      clearTimeout(saveFiltersTimerRef.current);
      saveFiltersTimerRef.current = null;
    };
  }, []);

  const scheduleBacktestsFiltersSave = (nextFilters: Record<string, unknown>) => {
    const currentJson = JSON.stringify(savedFilters);
    const nextJson = JSON.stringify(nextFilters);
    if (nextJson === currentJson) return;
    if (saveFiltersTimerRef.current) {
      clearTimeout(saveFiltersTimerRef.current);
      saveFiltersTimerRef.current = null;
    }
    saveFiltersTimerRef.current = setTimeout(() => {
      setBacktestsFilters(nextFilters);
      saveFiltersTimerRef.current = null;
    }, 600);
  };

  const handleFormValuesChange = (_: unknown, allValues: Partial<BacktestFormValues>) => {
    const nextFilters: Record<string, unknown> = { ...savedFilters };
    if (listStrategyFilter === 'all') {
      delete nextFilters.listStrategyId;
    } else {
      nextFilters.listStrategyId = listStrategyFilter;
    }

    const symbolText = typeof allValues.symbol === 'string' ? allValues.symbol.trim() : '';
    if (symbolText) {
      nextFilters.createSymbol = normalizeBacktestsCreateSymbol(symbolText);
    }

    const initialCapital = allValues.initialCapital;
    if (typeof initialCapital === 'number' && Number.isFinite(initialCapital) && initialCapital >= 0) {
      nextFilters.createInitialCapital = normalizeBacktestsCreateNumber(initialCapital, 100_000, 0);
    }

    const feeRate = allValues.feeRate;
    if (typeof feeRate === 'number' && Number.isFinite(feeRate) && feeRate >= 0 && feeRate <= 0.1) {
      nextFilters.createFeeRate = normalizeBacktestsCreateNumber(feeRate, 0.0006, 0, 0.1);
    }

    const slippage = allValues.slippage;
    if (typeof slippage === 'number' && Number.isFinite(slippage) && slippage >= 0 && slippage <= 0.1) {
      nextFilters.createSlippage = normalizeBacktestsCreateNumber(slippage, 0.0002, 0, 0.1);
    }

    const range = allValues.range;
    if (
      Array.isArray(range) &&
      range.length === 2 &&
      dayjs(range[0]).isValid() &&
      dayjs(range[1]).isValid() &&
      !dayjs(range[0]).isAfter(dayjs(range[1]))
    ) {
      nextFilters.createStartAt = dayjs(range[0]).toISOString();
      nextFilters.createEndAt = dayjs(range[1]).toISOString();
    }

    scheduleBacktestsFiltersSave(nextFilters);
  };

  const applyListStrategyFilter = (nextValue: string | 'all') => {
    const nextFilters: Record<string, unknown> = { ...savedFilters };
    if (nextValue === 'all') {
      delete nextFilters.listStrategyId;
    } else {
      nextFilters.listStrategyId = nextValue;
    }
    setBacktestsFilters(nextFilters);
  };

  const applyListPageSize = (nextValue: number | undefined) => {
    const nextPageSize = normalizeBacktestsListPageSize(nextValue);
    if (nextPageSize === listPageSize) return;
    setBacktestsFilters({
      ...savedFilters,
      listPageSize: nextPageSize,
    });
  };

  const handleCreate = async () => {
    const values = await form.validateFields();
    const [start, end] = values.range;
    const symbol = normalizeBacktestsCreateSymbol(values.symbol);
    const startAt = start.toISOString();
    const endAt = end.toISOString();
    const req: CreateBacktestRequest = {
      strategyId: values.strategyId,
      symbol,
      startAt,
      endAt,
      initialCapital: values.initialCapital,
      feeRate: values.feeRate,
      slippage: values.slippage,
    };

    const nextFilters: Record<string, unknown> = {
      ...savedFilters,
      createSymbol: symbol,
      createStartAt: startAt,
      createEndAt: endAt,
      createInitialCapital: values.initialCapital,
      createFeeRate: values.feeRate,
      createSlippage: values.slippage,
    };
    if (listStrategyFilter === 'all') {
      delete nextFilters.listStrategyId;
    } else {
      nextFilters.listStrategyId = listStrategyFilter;
    }
    setBacktestsFilters(nextFilters);

    const bt = await createMutation.mutateAsync(req);
    navigate(`/backtests/${bt.id}`);
  };

  return (
    <div className="page-shell">
      <Typography.Title level={3} style={{ margin: 0 }}>
        {t('backtests')}
      </Typography.Title>
      <NonTechGuideCard
        title={byLang('回测建议流程', 'Recommended backtest flow')}
        summary={byLang(
          '回测用于先验证策略再投入运行。新手优先用默认资金与手续费参数。',
          'Use backtests to validate before live usage. Defaults are recommended for beginners.',
        )}
        steps={[
          byLang('选择策略与时间区间，先用默认初始资金', 'Pick strategy/date range and keep default capital'),
          byLang('点击“创建并查看”，等待状态完成', 'Create and wait until status completes'),
          byLang('在详情页重点看总盈亏、最大回撤、胜率', 'Review PnL, max drawdown and win rate in details'),
        ]}
        tip={byLang('同样参数重复提交会自动幂等处理。', 'Repeated same requests are handled idempotently.')}
      />

      <Card
        title={
          <Space>
            <PlusOutlined />
            <Typography.Text strong>{t('createBacktestTask')}</Typography.Text>
          </Space>
        }
      >
        {isGuest ? <Typography.Text type="warning">{t('guestBacktestNotice')}</Typography.Text> : null}
        <Form
          form={form}
          layout="vertical"
          disabled={isGuest}
          onValuesChange={handleFormValuesChange}
          initialValues={{
            strategyId: defaultStrategyId,
            symbol: createSymbolDefault,
            range: [createRangeStartDefault, createRangeEndDefault],
            initialCapital: createInitialCapitalDefault,
            feeRate: createFeeRateDefault,
            slippage: createSlippageDefault,
          }}
        >
          <div
            style={{
              width: '100%',
              display: 'grid',
              gridTemplateColumns: isMobile ? '1fr' : 'repeat(auto-fit, minmax(220px, 1fr))',
              gap: 12,
            }}
          >
            <Form.Item
              name="strategyId"
              label={t('strategy')}
              extra={byLang('先选择你要验证的策略。', 'Select which strategy you want to validate first.')}
              rules={[{ required: true, message: t('strategy') }]}
            >
              <Select
                className="strategy-select"
                popupClassName="strategy-select-dropdown"
                placeholder={t('strategy')}
                options={(strategies ?? []).map((s) => ({
                  value: s.id,
                  label: (
                    <span className="strategy-option-label" title={`${s.name} (${s.id})`}>
                      {s.name}
                    </span>
                  ),
                }))}
                onChange={(v: string) => setSelectedLiveStrategyId(v)}
              />
            </Form.Item>

            <Form.Item
              name="symbol"
              label={t('symbol')}
              extra={byLang(
                '输入交易对，例如 BTCUSDT；不确定时可先用默认值。',
                'Enter a trading pair like BTCUSDT; keep default if unsure.',
              )}
              rules={[{ required: true, message: t('symbol') }]}
            >
              <Input placeholder="e.g. BTCUSDT" />
            </Form.Item>

            <Form.Item
              name="range"
              label={t('timeRange')}
              extra={byLang('建议先测近 7-30 天，便于快速验证。', 'Start with the last 7-30 days for quick validation.')}
              rules={[
                { required: true, message: t('timeRange') },
                {
                  validator: async (_, v) => {
                    if (!v || !Array.isArray(v) || v.length !== 2) throw new Error(t('timeRange'));
                    if (v[0].isAfter(v[1])) throw new Error(t('startBeforeEnd'));
                  },
                },
              ]}
            >
              <DatePicker.RangePicker showTime style={{ width: '100%' }} />
            </Form.Item>
          </div>

          <div
            style={{
              width: '100%',
              display: 'grid',
              gridTemplateColumns: isMobile ? '1fr' : 'repeat(auto-fit, minmax(180px, 1fr))',
              gap: 12,
            }}
          >
            <Form.Item
              name="initialCapital"
              label={t('initialCapital')}
              extra={byLang(
                '这是模拟资金，不是实盘资金；新手可保持默认值。',
                'This is simulated capital, not live funds; beginners can keep the default.',
              )}
              rules={[{ required: true, message: t('initialCapital') }, { type: 'number', min: 0, message: t('mustGte0') }]}
            >
              <InputNumber style={{ width: '100%' }} min={0} step={1000} />
            </Form.Item>

            <Form.Item
              name="feeRate"
              label={t('feeRate')}
              extra={byLang('默认值接近主流交易所费率，无特殊需求可不改。', 'Default is close to common exchange fee levels.')}
              rules={[{ required: true, message: t('feeRate') }, { type: 'number', min: 0, max: 0.1, message: t('range01') }]}
            >
              <InputNumber style={{ width: '100%' }} min={0} max={0.1} step={0.0001} />
            </Form.Item>

            <Form.Item
              name="slippage"
              label={t('slippage')}
              extra={byLang(
                '表示成交偏差，值越大代表成交越不理想；先用默认值更稳妥。',
                'Represents execution drift; larger means worse fills. Default is safer to start.',
              )}
              rules={[{ required: true, message: t('slippage') }, { type: 'number', min: 0, max: 0.1, message: t('range01') }]}
            >
              <InputNumber style={{ width: '100%' }} min={0} max={0.1} step={0.0001} />
            </Form.Item>

            <Form.Item label=" ">
              <Button type="primary" loading={createMutation.isPending} onClick={() => void handleCreate()} disabled={isGuest}>
                {t('createAndView')}
              </Button>
            </Form.Item>
          </div>
          <Typography.Text type="secondary">{`${t('backtestNote')}: ${formatNumber(100_000)}.`}</Typography.Text>
        </Form>
      </Card>

      <Card title={t('backtestList')}>
        <div style={{ marginBottom: 12 }}>
          <Select
            className="strategy-select"
            popupClassName="strategy-select-dropdown"
            value={listStrategyFilter}
            onChange={(v: string) => applyListStrategyFilter(v as string | 'all')}
            options={[
              { value: 'all', label: `${t('all')} ${t('strategy')}` },
              ...(strategies ?? []).map((s) => ({
                value: s.id,
                label: (
                  <span className="strategy-option-label" title={`${s.name} (${s.id})`}>
                    {s.name}
                  </span>
                ),
              })),
            ]}
          />
        </div>
        {isError ? (
          <ActionableErrorAlert
            title={byLang('回测列表加载失败', 'Failed to load backtest list')}
            steps={[
              byLang('点击“重试”重新加载回测任务', 'Click Retry to reload backtest tasks'),
              byLang('确认当前账号有查看权限', 'Confirm your account has view permissions'),
              byLang('若仍失败，去日志中心查看系统错误', 'If still failing, check system logs in Logs Center'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void refetch()}
            secondaryActionText={byLang('打开日志中心', 'Open Logs Center')}
            onSecondaryAction={() => navigate('/logs')}
          />
        ) : filteredData.length === 0 ? (
          <Empty description={t('noBacktest')} />
        ) : (
          <Table
            rowKey="id"
            loading={isPending}
            columns={columns}
            dataSource={filteredData}
            pagination={{ pageSize: listPageSize, showSizeChanger: true }}
            onChange={(pagination) => applyListPageSize(pagination?.pageSize)}
            scroll={{ x: 860 }}
          />
        )}
      </Card>
    </div>
  );
}
