import { useEffect } from 'react';
import { Button, Card, Descriptions, Empty, Form, Grid, InputNumber, Popconfirm, Select, Space, Switch, Typography } from 'antd';

import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useRiskQuery, useUpdateRiskMutation } from '@/hooks/queries/risk';
import { useStrategiesQuery } from '@/hooks/queries/strategies';
import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';
import type { UpdateRiskRequest } from '@/types/api';
import { formatPercent, formatTs } from '@/utils/format';

export function RiskPage() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const isGuest = useAppStore((s) => s.isGuest);
  const selectedStrategyId = useAppStore((s) => s.selectedLiveStrategyId);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const { data: strategies } = useStrategiesQuery();
  const { language, t } = useI18n();
  const { data, isPending, isError, refetch } = useRiskQuery(selectedStrategyId);
  const updateMutation = useUpdateRiskMutation();
  const [form] = Form.useForm<UpdateRiskRequest>();

  useEffect(() => {
    if (selectedStrategyId) return;
    const first = (strategies ?? []).find((s) => !!s.id);
    if (first?.id) setSelectedLiveStrategyId(first.id);
  }, [selectedStrategyId, setSelectedLiveStrategyId, strategies]);

  useEffect(() => {
    if (!data) return;
    form.setFieldsValue({
      enabled: data.enabled,
      maxDrawdownPct: data.maxDrawdownPct,
      maxPositionPct: data.maxPositionPct,
      maxRiskPerTradePct: data.maxRiskPerTradePct,
      maxLeverage: data.maxLeverage,
      dailyLossLimitPct: data.dailyLossLimitPct,
    });
  }, [data, form]);

  const onSave = async () => {
    const values = await form.validateFields();
    await updateMutation.mutateAsync({ req: values, strategyId: selectedStrategyId });
  };

  return (
    <div className="page-shell">
      <Typography.Title level={3} style={{ margin: 0 }}>
        {t('risk')}
      </Typography.Title>
      <NonTechGuideCard
        title={byLang('风控参数如何设置', 'How to set risk parameters')}
        summary={byLang(
          '风控决定最大可承受损失，建议先保守设置，再根据回测结果逐步放宽。',
          'Risk settings define your maximum loss tolerance. Start conservative and relax gradually.',
        )}
        steps={[
          byLang('先开启风控开关', 'Enable risk control first'),
          byLang('优先设置最大回撤与单日亏损限制', 'Set max drawdown and daily loss first'),
          byLang('保存后观察触发记录是否频繁', 'Save and monitor trigger records'),
        ]}
        tip={byLang(
          '建议：先在回测验证，再应用到实盘。',
          'Recommendation: validate in backtest before live use.',
        )}
      />

      <Card size="small">
        <Space wrap>
          <Typography.Text>{t('strategy')}</Typography.Text>
          <Select
            className="strategy-select"
            popupClassName="strategy-select-dropdown"
            value={selectedStrategyId}
            onChange={(v: string) => setSelectedLiveStrategyId(v)}
            options={(strategies ?? []).map((s) => ({
              value: s.id,
              label: (
                <span className="strategy-option-label" title={`${s.name} (${s.id})`}>
                  {s.name}
                </span>
              ),
            }))}
            placeholder={t('strategy')}
          />
        </Space>
      </Card>

      <Card loading={isPending} title={t('riskParams')}>
        {isError || !data ? (
          <ActionableErrorAlert
            title={byLang('风控参数加载失败', 'Failed to load risk parameters')}
            steps={[
              byLang('先确认已选择策略', 'Confirm a strategy is selected'),
              byLang('点击“重试”重新拉取风控配置', 'Click Retry to fetch risk settings again'),
              byLang('仍失败时进入日志中心查看错误', 'If it still fails, open Logs for details'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void refetch()}
          />
        ) : (
          <>
            <Descriptions size="small" bordered column={isMobile ? 1 : 2} style={{ marginBottom: 12 }}>
              <Descriptions.Item label={t('updatedAt')}>{formatTs(data.updatedAt)}</Descriptions.Item>
              <Descriptions.Item label={t('enabled')}>{data.enabled ? 'true' : 'false'}</Descriptions.Item>
              <Descriptions.Item label={t('triggerStatus')} span={2}>
                {data.triggered.length > 0 ? `${data.triggered.length} ${t('rulesTriggered')}` : t('noTriggerShort')}
              </Descriptions.Item>
            </Descriptions>

            <Form form={form} layout="vertical" disabled={isGuest}>
              {isGuest ? <Typography.Text type="warning">{t('guestRiskNotice')}</Typography.Text> : null}
              <Form.Item
                name="enabled"
                label={t('enableRiskControl')}
                valuePropName="checked"
                extra={byLang('建议保持开启，避免异常行情下损失扩大。', 'Keep this on to limit losses in abnormal markets.')}
              >
                <Switch />
              </Form.Item>

              <div style={{ width: '100%', display: 'grid', gridTemplateColumns: isMobile ? '1fr' : 'repeat(auto-fit, minmax(220px, 1fr))', gap: 12 }}>
                <Form.Item
                  name="maxDrawdownPct"
                  label={t('maxDrawdown01')}
                  extra={byLang('组合最大可承受亏损比例，例如 0.2 表示最多亏 20%。', 'Max portfolio loss tolerance. Example: 0.2 means up to 20% drawdown.')}
                  rules={[{ required: true, message: t('risk') }, { type: 'number', min: 0, max: 1, message: t('range01') }]}
                >
                  <InputNumber style={{ width: '100%' }} min={0} max={1} step={0.01} />
                </Form.Item>

                <Form.Item
                  name="maxPositionPct"
                  label={t('maxPosition01')}
                  extra={byLang('单标的最大仓位占比，避免单一资产风险过高。', 'Max allocation per symbol to prevent concentration risk.')}
                  rules={[{ required: true, message: t('risk') }, { type: 'number', min: 0, max: 1, message: t('range01') }]}
                >
                  <InputNumber style={{ width: '100%' }} min={0} max={1} step={0.01} />
                </Form.Item>

                <Form.Item
                  name="maxRiskPerTradePct"
                  label={t('riskPerTrade01')}
                  extra={byLang('单次交易最大风险比例，值越小越保守。', 'Max risk per trade. Smaller values are more conservative.')}
                  rules={[{ required: true, message: t('risk') }, { type: 'number', min: 0, max: 1, message: t('range01') }]}
                >
                  <InputNumber style={{ width: '100%' }} min={0} max={1} step={0.001} />
                </Form.Item>

                <Form.Item
                  name="maxLeverage"
                  label={t('maxLeverage1')}
                  extra={byLang('杠杆越高波动越大，新手建议从低杠杆开始。', 'Higher leverage means higher volatility; start low if you are new.')}
                  rules={[{ required: true, message: t('risk') }, { type: 'number', min: 1, max: 20, message: t('range1to20') }]}
                >
                  <InputNumber style={{ width: '100%' }} min={1} max={20} step={0.5} />
                </Form.Item>

                <Form.Item
                  name="dailyLossLimitPct"
                  label={t('dailyLossLimit01')}
                  extra={byLang('单日亏损达到该阈值后将触发保护限制。', 'Protection triggers when daily loss reaches this threshold.')}
                  rules={[{ required: true, message: t('risk') }, { type: 'number', min: 0, max: 1, message: t('range01') }]}
                >
                  <InputNumber style={{ width: '100%' }} min={0} max={1} step={0.01} />
                </Form.Item>
              </div>

              <Popconfirm
                title={byLang('确认保存风控参数？', 'Save risk parameters now?')}
                description={byLang(
                  '保存后会立即影响策略风控行为，建议先确认阈值是否符合预期。',
                  'Saved values take effect immediately for risk control behavior.',
                )}
                okText={byLang('确认保存', 'Save')}
                cancelText={byLang('取消', 'Cancel')}
                disabled={isGuest}
                okButtonProps={{ loading: updateMutation.isPending }}
                onConfirm={() => void onSave()}
              >
                <Button type="primary" loading={updateMutation.isPending} disabled={isGuest}>
                  {t('save')}
                </Button>
              </Popconfirm>
            </Form>

            <Typography.Text type="secondary" style={{ display: 'block', marginTop: 10 }}>
              {language === 'en'
                ? `Current thresholds: MaxDD ${formatPercent(data.maxDrawdownPct)}; MaxPos ${formatPercent(data.maxPositionPct)}; Risk/Trade ${formatPercent(data.maxRiskPerTradePct)}; DailyLoss ${formatPercent(data.dailyLossLimitPct)}.`
                : `当前阈值: MaxDD ${formatPercent(data.maxDrawdownPct)}; MaxPos ${formatPercent(data.maxPositionPct)}; Risk/Trade ${formatPercent(data.maxRiskPerTradePct)}; DailyLoss ${formatPercent(data.dailyLossLimitPct)}.`}
            </Typography.Text>
          </>
        )}
      </Card>

      <Card title={t('triggerRecords')}>
        {!data ? (
          <Typography.Text type="secondary">-</Typography.Text>
        ) : data.triggered.length === 0 ? (
          <Empty description={t('noTrigger')} />
        ) : (
          data.triggered.map((tr) => (
            <div key={`${tr.rule}_${tr.ts}`} style={{ padding: '6px 0' }}>
              <Typography.Text strong>{tr.rule}</Typography.Text>
              <Typography.Text type="secondary" style={{ marginLeft: 10 }}>{formatTs(tr.ts)}</Typography.Text>
              <div>
                <Typography.Text>{tr.message}</Typography.Text>
              </div>
            </div>
          ))
        )}
      </Card>
    </div>
  );
}
