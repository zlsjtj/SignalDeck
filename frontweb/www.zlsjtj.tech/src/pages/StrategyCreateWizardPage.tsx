import { useEffect, useMemo, useState } from 'react';
import { Alert, Button, Card, Descriptions, Form, Input, Select, Space, Steps, Switch, Typography } from 'antd';
import { Link, useNavigate } from 'react-router-dom';

import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { byLang, useI18n } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import type { StrategyType } from '@/types/api';
import { useCreateStrategyMutation } from '@/hooks/queries/strategies';
import {
  buildAutoParams,
  COMMON_SYMBOLS,
  normalizeSymbols,
  prettyJson,
  type StrategyRiskPreset,
} from '@/components/strategies/strategyPreset';

type WizardFormValues = {
  name: string;
  type: StrategyType;
  symbols: string[];
  timeframe: '1m' | '5m' | '15m' | '1h' | '1d';
  riskPreset: StrategyRiskPreset;
  enableAdvanced: boolean;
  paramsJson: string;
};

const INITIAL_VALUES: WizardFormValues = {
  name: '',
  type: 'mean_reversion',
  symbols: ['BTCUSDT', 'ETHUSDT'],
  timeframe: '15m',
  riskPreset: 'balanced',
  enableAdvanced: false,
  paramsJson: prettyJson(buildAutoParams('mean_reversion', 'balanced')),
};

export function StrategyCreateWizardPage() {
  const { t } = useI18n();
  const navigate = useNavigate();
  const isGuest = useAppStore((s) => s.isGuest);
  const [form] = Form.useForm<WizardFormValues>();
  const [step, setStep] = useState(0);
  const createMutation = useCreateStrategyMutation();

  const selectedType = Form.useWatch('type', form) ?? INITIAL_VALUES.type;
  const selectedRiskPreset = Form.useWatch('riskPreset', form) ?? INITIAL_VALUES.riskPreset;
  const advancedMode = Form.useWatch('enableAdvanced', form) ?? INITIAL_VALUES.enableAdvanced;
  const paramsJson = Form.useWatch('paramsJson', form) ?? INITIAL_VALUES.paramsJson;

  useEffect(() => {
    if (advancedMode) return;
    const autoParams = prettyJson(buildAutoParams(selectedType, selectedRiskPreset));
    if (form.getFieldValue('paramsJson') !== autoParams) {
      form.setFieldValue('paramsJson', autoParams);
    }
  }, [advancedMode, form, selectedRiskPreset, selectedType]);

  const typeOptions = useMemo(
    () => [
      { value: 'mean_reversion' as const, label: t('strategyTypeMeanReversion') },
      { value: 'trend_following' as const, label: t('strategyTypeTrendFollowing') },
      { value: 'market_making' as const, label: t('strategyTypeMarketMaking') },
      { value: 'custom' as const, label: t('strategyTypeCustom') },
    ],
    [t],
  );

  const riskOptions = useMemo(
    () => [
      { value: 'conservative' as const, label: t('strategyRiskConservative') },
      { value: 'balanced' as const, label: t('strategyRiskBalanced') },
      { value: 'aggressive' as const, label: t('strategyRiskAggressive') },
    ],
    [t],
  );

  const typeHint =
    selectedType === 'mean_reversion'
      ? t('strategyTypeHintMeanReversion')
      : selectedType === 'trend_following'
        ? t('strategyTypeHintTrendFollowing')
        : selectedType === 'market_making'
          ? t('strategyTypeHintMarketMaking')
          : t('strategyTypeHintCustom');

  const riskHint =
    selectedRiskPreset === 'conservative'
      ? t('strategyRiskHintConservative')
      : selectedRiskPreset === 'aggressive'
        ? t('strategyRiskHintAggressive')
        : t('strategyRiskHintBalanced');

  const paramsForPreview = useMemo(() => {
    if (!advancedMode) return buildAutoParams(selectedType, selectedRiskPreset);
    try {
      const parsed = paramsJson?.trim() ? JSON.parse(paramsJson) : {};
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') return {};
      return parsed;
    } catch {
      return {};
    }
  }, [advancedMode, paramsJson, selectedRiskPreset, selectedType]);

  const canGoNext = step < 2;
  const canGoPrev = step > 0;

  const applySymbolSet = (symbols: string[]) => {
    form.setFieldValue('symbols', normalizeSymbols(symbols));
  };

  const validateCurrentStep = async () => {
    if (step === 0) {
      await form.validateFields(['name', 'type', 'symbols', 'timeframe']);
      return;
    }
    if (step === 1) {
      if (advancedMode) {
        await form.validateFields(['riskPreset', 'enableAdvanced', 'paramsJson']);
      } else {
        await form.validateFields(['riskPreset', 'enableAdvanced']);
      }
    }
  };

  const onNext = async () => {
    try {
      await validateCurrentStep();
      setStep((prev) => Math.min(prev + 1, 2));
    } catch {
      // validation errors are shown by Form
    }
  };

  const onPrev = () => {
    setStep((prev) => Math.max(prev - 1, 0));
  };

  const onCreate = async () => {
    const values = await form.validateFields();
    const symbols = normalizeSymbols(values.symbols ?? []);
    if (symbols.length === 0) {
      form.setFields([{ name: 'symbols', errors: [t('selectAtLeastOneSymbol')] }]);
      return;
    }

    let params = buildAutoParams(values.type, values.riskPreset);
    if (values.enableAdvanced) {
      const parsed = values.paramsJson?.trim() ? JSON.parse(values.paramsJson) : {};
      if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
        form.setFields([{ name: 'paramsJson', errors: [t('invalidJsonFormat')] }]);
        return;
      }
      params = parsed as Record<string, number | string | boolean>;
    }

    const created = await createMutation.mutateAsync({
      name: values.name.trim(),
      type: values.type,
      config: {
        symbols,
        timeframe: values.timeframe,
        params,
      },
    });
    navigate(`/strategies/${created.id}`);
  };

  if (isGuest) {
    return (
      <div className="page-shell">
        <Typography.Title level={3} style={{ margin: 0 }}>
          {t('strategyWizardTitle')}
        </Typography.Title>
        <Alert type="warning" showIcon message={t('guestReadonly')} description={t('guestStrategyNotice')} />
        <Space>
          <Button>
            <Link to="/strategies">{t('backToList')}</Link>
          </Button>
        </Space>
      </div>
    );
  }

  return (
    <div className="page-shell">
      <div className="page-header">
        <Typography.Title level={3} style={{ margin: 0 }}>
          {t('strategyWizardTitle')}
        </Typography.Title>
        <Space>
          <Button>
            <Link to="/strategies">{t('cancel')}</Link>
          </Button>
        </Space>
      </div>
      <NonTechGuideCard
        title={byLang('新手创建策略建议', 'Beginner strategy setup')}
        summary={byLang(
          '如果你不熟悉参数，直接使用“平衡”风险档位并关闭高级模式即可。',
          'If you are not familiar with parameters, keep Balanced preset and Advanced mode off.',
        )}
        steps={[
          byLang('基础信息：填写名称、选择策略类型、标的、周期', 'Basic: name, type, symbols, timeframe'),
          byLang('风险选择：优先“平衡（推荐）”', 'Risk: choose Balanced first'),
          byLang('确认创建：先运行观察，再按结果逐步微调', 'Confirm: run first, tune later'),
        ]}
        tip={byLang(
          '建议先跑回测验证，再用于实盘监控。',
          'Run backtests before relying on live monitoring.',
        )}
      />

      <Card>
        <Typography.Text type="secondary">{t('strategyWizardDesc')}</Typography.Text>
        <Steps
          current={step}
          style={{ marginTop: 12 }}
          items={[
            { title: t('strategyWizardStepBasic'), description: t('strategyWizardStepBasicDesc') },
            { title: t('strategyWizardStepRisk'), description: t('strategyWizardStepRiskDesc') },
            { title: t('strategyWizardStepConfirm'), description: t('strategyWizardStepConfirmDesc') },
          ]}
        />
      </Card>

      <Card>
        <Form form={form} layout="vertical" initialValues={INITIAL_VALUES}>
          {step === 0 ? (
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Form.Item
                name="name"
                label={t('strategyName')}
                extra={byLang('建议用“策略目的 + 市场”命名，便于后续识别。', 'Name it by purpose + market for easier recognition later.')}
                rules={[{ required: true, message: t('pleaseEnterStrategyName') }]}
              >
                <Input placeholder={t('strategyNameExample')} />
              </Form.Item>

              <Form.Item
                name="type"
                label={t('strategyType')}
                extra={byLang('不确定时可先选“均值回归”或“趋势跟随”。', 'If unsure, start with Mean Reversion or Trend Following.')}
                rules={[{ required: true, message: t('pleaseSelectStrategyType') }]}
              >
                <Select options={typeOptions} />
              </Form.Item>
              <Typography.Text type="secondary">{typeHint}</Typography.Text>

              <Form.Item
                name="symbols"
                label={t('symbol')}
                extra={byLang(
                  '可直接输入交易对（如 BTCUSDT），多个标的可用空格或逗号分隔。',
                  'Enter symbols like BTCUSDT; use spaces or commas for multiple symbols.',
                )}
                rules={[
                  { required: true, message: t('selectAtLeastOneSymbol') },
                  {
                    validator: async (_, v: unknown) => {
                      if (!Array.isArray(v) || normalizeSymbols(v as string[]).length === 0) {
                        throw new Error(t('selectAtLeastOneSymbol'));
                      }
                    },
                  },
                ]}
              >
                <Select
                  mode="tags"
                  options={COMMON_SYMBOLS.map((symbol) => ({ value: symbol, label: symbol }))}
                  tokenSeparators={[',', ' ']}
                  placeholder={t('enterOrSelectSymbol')}
                />
              </Form.Item>

              <Form.Item label={t('strategySymbolsQuickPick')}>
                <Space wrap>
                  <Button size="small" onClick={() => applySymbolSet(['BTCUSDT'])}>
                    {t('strategyPresetSingleBTC')}
                  </Button>
                  <Button size="small" onClick={() => applySymbolSet(['BTCUSDT', 'ETHUSDT'])}>
                    {t('strategyPresetMainstream2')}
                  </Button>
                  <Button size="small" onClick={() => applySymbolSet(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'])}>
                    {t('strategyPresetMainstream3')}
                  </Button>
                </Space>
              </Form.Item>

              <Form.Item
                name="timeframe"
                label={t('timeframe')}
                extra={byLang('新手建议先用 15m 或 1h，噪音更低。', 'Beginners should start with 15m or 1h to reduce noise.')}
                rules={[{ required: true, message: t('pleaseSelectTimeframe') }]}
              >
                <Select
                  options={[
                    { value: '1m', label: '1m' },
                    { value: '5m', label: '5m' },
                    { value: '15m', label: '15m' },
                    { value: '1h', label: '1h' },
                    { value: '1d', label: '1d' },
                  ]}
                />
              </Form.Item>
              <Typography.Text type="secondary">{t('strategyTimeframeHint')}</Typography.Text>
            </Space>
          ) : null}

          {step === 1 ? (
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Form.Item
                name="riskPreset"
                label={t('strategyRiskPreset')}
                extra={byLang('建议先选“平衡（推荐）”，后续再按回测结果调整。', 'Start with Balanced and adjust later based on backtests.')}
              >
                <Select options={riskOptions} />
              </Form.Item>
              <Typography.Text type="secondary">{riskHint}</Typography.Text>

              <Form.Item
                name="enableAdvanced"
                label={t('strategyAdvancedMode')}
                valuePropName="checked"
                extra={byLang('不了解 JSON 参数时请保持关闭。', 'Keep this off if you are not familiar with JSON params.')}
              >
                <Switch />
              </Form.Item>
              <Typography.Text type="secondary">{t('strategyAdvancedModeHelp')}</Typography.Text>

              {advancedMode ? (
                <Form.Item
                  name="paramsJson"
                  label={t('paramsJson')}
                  extra={byLang(
                    '仅建议有经验用户修改，错误格式会导致创建失败。',
                    'Recommended for advanced users only. Invalid format will fail creation.',
                  )}
                  rules={[
                    { required: true, message: t('pleaseEnterParamsJson') },
                    {
                      validator: async (_, v: unknown) => {
                        if (typeof v !== 'string') throw new Error(t('paramsMustBeJsonString'));
                        try {
                          JSON.parse(v);
                        } catch {
                          throw new Error(t('invalidJsonFormat'));
                        }
                      },
                    },
                  ]}
                >
                  <Input.TextArea autoSize={{ minRows: 8, maxRows: 16 }} spellCheck={false} />
                </Form.Item>
              ) : (
                <Form.Item label={t('strategyAutoParamsPreview')}>
                  <Input.TextArea value={prettyJson(paramsForPreview)} autoSize={{ minRows: 8, maxRows: 16 }} readOnly />
                </Form.Item>
              )}
            </Space>
          ) : null}

          {step === 2 ? (
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Alert type="info" showIcon message={t('strategyWizardConfirmHint')} />
              <Descriptions bordered column={1} size="small">
                <Descriptions.Item label={t('strategyName')}>{form.getFieldValue('name') || '-'}</Descriptions.Item>
                <Descriptions.Item label={t('strategyType')}>
                  {typeOptions.find((item) => item.value === form.getFieldValue('type'))?.label ?? '-'}
                </Descriptions.Item>
                <Descriptions.Item label={t('symbol')}>
                  {normalizeSymbols(form.getFieldValue('symbols') ?? []).join(', ') || '-'}
                </Descriptions.Item>
                <Descriptions.Item label={t('timeframe')}>{form.getFieldValue('timeframe') || '-'}</Descriptions.Item>
                <Descriptions.Item label={t('strategyRiskPreset')}>
                  {riskOptions.find((item) => item.value === form.getFieldValue('riskPreset'))?.label ?? '-'}
                </Descriptions.Item>
                <Descriptions.Item label={t('strategyAdvancedMode')}>
                  {form.getFieldValue('enableAdvanced') ? t('diagBoolTrue') : t('diagBoolFalse')}
                </Descriptions.Item>
              </Descriptions>

              <Form.Item label={t('strategyWizardFinalParams')}>
                <Input.TextArea value={prettyJson(paramsForPreview)} autoSize={{ minRows: 8, maxRows: 16 }} readOnly />
              </Form.Item>
            </Space>
          ) : null}
        </Form>

        <Space style={{ marginTop: 16 }}>
          {canGoPrev ? (
            <Button onClick={onPrev}>{t('strategyWizardPrev')}</Button>
          ) : (
            <Button onClick={() => navigate('/strategies')}>{t('cancel')}</Button>
          )}
          {canGoNext ? (
            <Button type="primary" onClick={() => void onNext()}>
              {t('strategyWizardNext')}
            </Button>
          ) : (
            <Button type="primary" loading={createMutation.isPending} onClick={() => void onCreate()}>
              {t('strategyWizardCreateNow')}
            </Button>
          )}
        </Space>
      </Card>
    </div>
  );
}
