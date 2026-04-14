import { useEffect, useMemo } from 'react';
import { Alert, Button, Form, Input, Modal, Select, Space, Switch, Typography } from 'antd';

import { byLang, useI18n } from '@/i18n';
import type { CreateStrategyRequest, Strategy, StrategyType } from '@/types/api';
import {
  buildAutoParams,
  COMMON_SYMBOLS,
  normalizeSymbols,
  prettyJson,
  type StrategyRiskPreset,
} from '@/components/strategies/strategyPreset';

type Props = {
  open: boolean;
  loading?: boolean;
  initial?: Strategy | null;
  onCancel: () => void;
  onSubmit: (req: CreateStrategyRequest) => Promise<void> | void;
};

type FormValues = {
  name: string;
  type: StrategyType;
  symbols: string[];
  timeframe: '1m' | '5m' | '15m' | '1h' | '1d';
  riskPreset: StrategyRiskPreset;
  enableAdvanced: boolean;
  paramsJson: string;
};

export function StrategyFormModal({ open, loading, initial, onCancel, onSubmit }: Props) {
  const { t } = useI18n();
  const [form] = Form.useForm<FormValues>();

  const title = initial ? t('edit') : t('createStrategy');
  const initialValues = useMemo<FormValues>(() => {
    const defaultType: StrategyType = initial?.type ?? 'mean_reversion';
    const defaultRiskPreset: StrategyRiskPreset = 'balanced';
    return {
      name: initial?.name ?? '',
      type: defaultType,
      symbols: normalizeSymbols(initial?.config.symbols ?? ['BTCUSDT', 'ETHUSDT']),
      timeframe: initial?.config.timeframe ?? '15m',
      riskPreset: defaultRiskPreset,
      enableAdvanced: Boolean(initial),
      paramsJson: prettyJson(initial?.config.params ?? buildAutoParams(defaultType, defaultRiskPreset)),
    };
  }, [initial]);

  useEffect(() => {
    if (!open) return;
    form.setFieldsValue(initialValues);
  }, [form, initialValues, open]);

  const selectedType = Form.useWatch('type', form) ?? initialValues.type;
  const selectedRiskPreset = Form.useWatch('riskPreset', form) ?? initialValues.riskPreset;
  const advancedMode = Form.useWatch('enableAdvanced', form) ?? initialValues.enableAdvanced;
  const paramsPreview = Form.useWatch('paramsJson', form) ?? initialValues.paramsJson;

  useEffect(() => {
    if (!open || advancedMode) return;
    const autoParamsJson = prettyJson(buildAutoParams(selectedType, selectedRiskPreset));
    if (form.getFieldValue('paramsJson') !== autoParamsJson) {
      form.setFieldValue('paramsJson', autoParamsJson);
    }
  }, [advancedMode, form, open, selectedRiskPreset, selectedType]);

  const strategyTypeOptions = useMemo(
    () => [
      { value: 'mean_reversion' as const, label: t('strategyTypeMeanReversion') },
      { value: 'trend_following' as const, label: t('strategyTypeTrendFollowing') },
      { value: 'market_making' as const, label: t('strategyTypeMarketMaking') },
      { value: 'custom' as const, label: t('strategyTypeCustom') },
    ],
    [t],
  );

  const riskPresetOptions = useMemo(
    () => [
      { value: 'conservative' as const, label: t('strategyRiskConservative') },
      { value: 'balanced' as const, label: t('strategyRiskBalanced') },
      { value: 'aggressive' as const, label: t('strategyRiskAggressive') },
    ],
    [t],
  );

  const strategyTypeHint =
    selectedType === 'mean_reversion'
      ? t('strategyTypeHintMeanReversion')
      : selectedType === 'trend_following'
        ? t('strategyTypeHintTrendFollowing')
        : selectedType === 'market_making'
          ? t('strategyTypeHintMarketMaking')
          : t('strategyTypeHintCustom');

  const riskPresetHint =
    selectedRiskPreset === 'conservative'
      ? t('strategyRiskHintConservative')
      : selectedRiskPreset === 'aggressive'
        ? t('strategyRiskHintAggressive')
        : t('strategyRiskHintBalanced');

  const symbolOptions = useMemo(
    () => COMMON_SYMBOLS.map((symbol) => ({ value: symbol, label: symbol })),
    [],
  );

  const applySymbolSet = (symbols: string[]) => {
    form.setFieldValue('symbols', normalizeSymbols(symbols));
  };

  const handleOk = async () => {
    const values = await form.validateFields();
    const symbols = normalizeSymbols(values.symbols ?? []);
    if (symbols.length === 0) {
      form.setFields([{ name: 'symbols', errors: [t('selectAtLeastOneSymbol')] }]);
      return;
    }

    let params = buildAutoParams(values.type, values.riskPreset);
    if (values.enableAdvanced) {
      try {
        const parsed = values.paramsJson?.trim() ? JSON.parse(values.paramsJson) : {};
        if (!parsed || Array.isArray(parsed) || typeof parsed !== 'object') {
          throw new Error(t('invalidJsonFormat'));
        }
        params = parsed as Record<string, number | string | boolean>;
      } catch {
        form.setFields([{ name: 'paramsJson', errors: [t('paramsMustBeValidJson')] }]);
        return;
      }
    }

    await onSubmit({
      name: values.name.trim(),
      type: values.type,
      config: {
        symbols,
        timeframe: values.timeframe,
        params,
      },
    });
  };

  return (
    <Modal
      open={open}
      title={title}
      okText={t('save')}
      cancelText={t('cancel')}
      confirmLoading={loading}
      onCancel={onCancel}
      onOk={() => void handleOk().catch(() => void 0)}
      destroyOnHidden
    >
      <Form form={form} layout="vertical" initialValues={initialValues}>
        {!initial ? (
          <Alert
            type="info"
            showIcon
            style={{ marginBottom: 16 }}
            message={t('strategyCreateSimpleHintTitle')}
            description={t('strategyCreateSimpleHintDesc')}
          />
        ) : null}

        <Form.Item
          name="name"
          label={t('strategyName')}
          extra={byLang('建议用“策略目的 + 市场”命名。', 'Use purpose + market naming for easy recognition.')}
          rules={[{ required: true, message: t('pleaseEnterStrategyName') }]}
        >
          <Input placeholder={t('strategyNameExample')} />
        </Form.Item>

        <Form.Item
          name="type"
          label={t('strategyType')}
          extra={byLang('不确定时可先用均值回归或趋势跟随。', 'If unsure, start with mean reversion or trend following.')}
          rules={[{ required: true, message: t('pleaseSelectStrategyType') }]}
        >
          <Select options={strategyTypeOptions} />
        </Form.Item>
        <Typography.Text type="secondary">{strategyTypeHint}</Typography.Text>

        <Form.Item
          name="symbols"
          label={t('symbol')}
          extra={byLang('可输入多个交易对，使用逗号或空格分隔。', 'You can enter multiple symbols separated by comma or space.')}
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
          <Select mode="tags" options={symbolOptions} tokenSeparators={[',', ' ']} placeholder={t('enterOrSelectSymbol')} />
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
          extra={byLang('新手建议 15m 或 1h。', 'Beginners are recommended to start from 15m or 1h.')}
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

        <Form.Item
          name="riskPreset"
          label={t('strategyRiskPreset')}
          extra={byLang('先用“平衡”，根据回测逐步调整。', 'Start with Balanced and tune based on backtests.')}
        >
          <Select options={riskPresetOptions} />
        </Form.Item>
        <Typography.Text type="secondary">{riskPresetHint}</Typography.Text>

        <Form.Item
          name="enableAdvanced"
          label={t('strategyAdvancedMode')}
          valuePropName="checked"
          extra={byLang('不熟悉参数时建议关闭高级模式。', 'Keep advanced mode off if unfamiliar with parameters.')}
        >
          <Switch />
        </Form.Item>
        <Typography.Text type="secondary">{t('strategyAdvancedModeHelp')}</Typography.Text>

        {advancedMode ? (
          <Form.Item
            name="paramsJson"
            label={t('paramsJson')}
            extra={byLang('仅建议高级用户编辑 JSON 参数。', 'Editing JSON params is recommended for advanced users only.')}
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
            <Input.TextArea autoSize={{ minRows: 6, maxRows: 12 }} spellCheck={false} />
          </Form.Item>
        ) : (
          <Form.Item label={t('strategyAutoParamsPreview')}>
            <Input.TextArea value={paramsPreview} autoSize={{ minRows: 6, maxRows: 12 }} readOnly />
          </Form.Item>
        )}
      </Form>
    </Modal>
  );
}
