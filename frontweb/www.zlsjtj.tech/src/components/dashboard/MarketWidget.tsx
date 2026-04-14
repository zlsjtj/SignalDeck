import { useEffect, useMemo, useState } from 'react';
import { Card, Grid, Select, Space, Typography } from 'antd';

import { KlineMiniChart } from '@/components/charts/KlineMiniChart';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useMarketKlinesQuery, useMarketTicksQuery } from '@/hooks/queries/market';
import { useLiveStore } from '@/store/liveStore';
import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';
import { env } from '@/utils/env';
import { formatPriceBySymbol, formatTs } from '@/utils/format';

export function MarketWidget() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { t } = useI18n();
  const ticksBySymbol = useLiveStore((s) => s.ticksBySymbol);
  const candlesBySymbol = useLiveStore((s) => s.candlesBySymbol);
  const pushTick = useLiveStore((s) => s.pushTick);
  const setCandles = useLiveStore((s) => s.setCandles);
  const guestReadonly = useAppStore((s) => Boolean(s.isGuest) && !Boolean(s.isAuthenticated));
  const restTicksQuery = useMarketTicksQuery(true);

  useEffect(() => {
    const rows = restTicksQuery.data ?? [];
    if (rows.length === 0) return;
    rows.forEach((t) => pushTick(t));
  }, [pushTick, restTicksQuery.data]);

  const symbols = useMemo(() => Object.keys(ticksBySymbol).sort(), [ticksBySymbol]);
  const [symbol, setSymbol] = useState<string>(() => symbols[0] ?? 'BTCUSDT');

  useEffect(() => {
    if (symbols.length === 0) return;
    if (!symbols.includes(symbol)) setSymbol(symbols[0]!);
  }, [symbol, symbols]);

  const tick = ticksBySymbol[symbol];
  const candles = candlesBySymbol[symbol] ?? [];
  const klinesQuery = useMarketKlinesQuery(symbol, Boolean(symbol));
  const waitingText = guestReadonly || (!env.wsEnabled && !env.useMock) ? t('waitingRest') : t('waitingWs');

  useEffect(() => {
    const rows = klinesQuery.data ?? [];
    if (!symbol || rows.length === 0) return;
    setCandles(symbol, rows);
  }, [klinesQuery.data, setCandles, symbol]);

  return (
    <Card
      title={
        <Space wrap>
          <Typography.Text strong>{t('realtimeMarket')}</Typography.Text>
          <Select
            size="small"
            style={{ width: isMobile ? 132 : 160 }}
            value={symbol}
            onChange={(v) => setSymbol(v)}
            options={(symbols.length ? symbols : ['BTCUSDT', 'ETHUSDT', 'AAPL']).map((s) => ({
              value: s,
              label: s,
            }))}
          />
        </Space>
      }
    >
      <div
        style={{
          marginBottom: 12,
          display: 'grid',
          gridTemplateColumns: isMobile ? '1fr' : 'repeat(3, minmax(0, 1fr))',
          gap: 8,
          minWidth: 0,
        }}
      >
        <div style={{ minWidth: 0 }}>
          <Typography.Text type="secondary">{t('tsLabel')}: </Typography.Text>
          <Typography.Text
            style={{
              display: 'inline-block',
              maxWidth: 'calc(100% - 28px)',
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              verticalAlign: 'bottom',
            }}
          >
            {formatTs(tick?.ts)}
          </Typography.Text>
        </div>
        <div style={{ minWidth: 0 }}>
          <Typography.Text type="secondary">{t('priceLabel')}: </Typography.Text>
          <Typography.Text>{formatPriceBySymbol(symbol, tick?.price)}</Typography.Text>
        </div>
        <div style={{ minWidth: 0 }}>
          <Typography.Text type="secondary">{t('bidAskLabel')}: </Typography.Text>
          <Typography.Text>
            {formatPriceBySymbol(symbol, tick?.bid)} / {formatPriceBySymbol(symbol, tick?.ask)}
          </Typography.Text>
        </div>
      </div>
      <KlineMiniChart
        key={symbol}
        symbol={symbol}
        candles={candles}
        height={220}
        showSeconds={env.marketPollMs <= 1000}
      />
      {!tick ? (
        <Typography.Text type="secondary">{waitingText}</Typography.Text>
      ) : null}
      {restTicksQuery.isError ? (
        <div style={{ marginTop: 8 }}>
          <ActionableErrorAlert
            title={t('marketFetchError')}
            steps={[
              byLang('点击重试，重新拉取最新行情。', 'Click Retry to fetch latest ticks again.'),
              byLang('若持续失败，检查行情服务连接状态。', 'If it still fails, check market service connectivity.'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void restTicksQuery.refetch()}
          />
        </div>
      ) : null}
      {klinesQuery.isError ? (
        <div style={{ marginTop: 8 }}>
          <ActionableErrorAlert
            title={t('klineFetchError')}
            steps={[
              byLang('点击重试，重新加载K线图。', 'Click Retry to reload kline chart.'),
              byLang('若持续失败，可先切换其他交易对确认。', 'If it still fails, switch symbol to verify.'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void klinesQuery.refetch()}
          />
        </div>
      ) : null}
    </Card>
  );
}
