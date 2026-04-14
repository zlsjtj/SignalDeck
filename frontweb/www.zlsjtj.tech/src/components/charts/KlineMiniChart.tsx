import { useCallback, useEffect, useMemo, useRef } from 'react';
import {
  CrosshairMode,
  createChart,
  type IChartApi,
  type ISeriesApi,
  type UTCTimestamp,
} from 'lightweight-charts';

import type { Candle } from '@/store/liveStore';
import { useAppStore } from '@/store/appStore';
import { priceDigitsBySymbol } from '@/utils/format';

type Props = {
  symbol?: string;
  candles: Candle[];
  height?: number;
  showSeconds?: boolean;
};

type LightweightCandle = Candle & { time: UTCTimestamp };

export function KlineMiniChart({ symbol, candles, height = 220, showSeconds = false }: Props) {
  const theme = useAppStore((s) => s.theme);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const seriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const latestDataRef = useRef<LightweightCandle[]>([]);
  const hasDataRef = useRef(false);
  const priceDigits = priceDigitsBySymbol(symbol);

  const formatLocalTick = useCallback((value?: number) => {
    if (!value) return '';
    const date = new Date(value * 1000);
    if (Number.isNaN(date.getTime())) return '';
    return date.toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      ...(showSeconds ? { second: '2-digit' } : {}),
      hour12: false,
    });
  }, [showSeconds]);

  const chartColors = useMemo(() => {
    const dark = theme === 'dark';
    return {
      layout: {
        background: { color: dark ? '#0b0f14' : '#ffffff' },
        textColor: dark ? '#d7e2f0' : '#334155',
      },
      grid: {
        vertLines: { color: dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)' },
        horzLines: { color: dark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.06)' },
      },
    };
  }, [theme]);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const chart = createChart(el, {
      width: el.clientWidth || 320,
      height,
      layout: chartColors.layout,
      grid: chartColors.grid,
      rightPriceScale: { borderVisible: false },
      timeScale: {
        borderVisible: false,
        timeVisible: true,
        secondsVisible: showSeconds,
      },
      crosshair: { mode: CrosshairMode.Normal },
    });
    chartRef.current = chart;

    const series = chart.addCandlestickSeries({
      upColor: '#00b96b',
      downColor: '#ff4d4f',
      borderVisible: false,
      wickUpColor: '#00b96b',
      wickDownColor: '#ff4d4f',
      priceFormat: {
        type: 'price',
        precision: priceDigits,
        minMove: 1 / 10 ** priceDigits,
      },
    });
    seriesRef.current = series;

    const resize = () => {
      const w = el.clientWidth;
      chart.applyOptions({ width: w, height });
    };
    chart.timeScale().applyOptions({
      // Force local-time label rendering, avoid browser UTC display differences.
      tickMarkFormatter: (time: unknown) => formatLocalTick(typeof time === 'number' ? time : undefined),
    } as unknown as Record<string, unknown>);
    const resizeObserver =
      typeof ResizeObserver !== 'undefined'
        ? new ResizeObserver(() => {
            resize();
          })
        : null;
    if (resizeObserver) {
      resizeObserver.observe(el);
    } else {
      window.addEventListener('resize', resize);
    }
    resize();

    return () => {
      if (resizeObserver) {
        resizeObserver.disconnect();
      } else {
        window.removeEventListener('resize', resize);
      }
      chart.remove();
      chartRef.current = null;
      seriesRef.current = null;
      latestDataRef.current = [];
      hasDataRef.current = false;
    };
  }, [chartColors.grid, chartColors.layout, height, showSeconds, formatLocalTick, priceDigits]);

  useEffect(() => {
    const series = seriesRef.current;
    const chart = chartRef.current;
    if (!series || !chart) return;

    const normalized: LightweightCandle[] = candles.map((c) => ({ ...c, time: c.time as UTCTimestamp }));
    const prev = latestDataRef.current;

    if (normalized.length === 0) {
      latestDataRef.current = [];
      hasDataRef.current = false;
      series.setData([]);
      return;
    }

    if (!hasDataRef.current) {
      latestDataRef.current = normalized;
      hasDataRef.current = true;
      series.setData(normalized);
      chart.timeScale().fitContent();
      return;
    }

    const prevLen = prev.length;
    const nextLen = normalized.length;
    const nextLast = normalized[nextLen - 1];
    const prevLast = prev[prevLen - 1];

    // Most updates only touch the last bar; update incrementally to avoid full re-render.
    if (nextLen === prevLen && prevLast && nextLast && prevLast.time === nextLast.time) {
      prev[prevLen - 1] = nextLast;
      series.update(nextLast);
      return;
    }

    // New bar appended
    if (nextLen === prevLen + 1) {
      const appended = nextLast;
      if (appended) {
        prev.push(appended);
        if (prev.length > 200) {
          prev.shift();
        }
        latestDataRef.current = prev;
        series.update(appended);
        if (nextLen % 10 === 0) {
          chart.timeScale().fitContent();
        }
        return;
      }
    }

    // Fallback for occasional resets (e.g., websocket reconnect).
    latestDataRef.current = normalized;
    series.setData(normalized);
    chart.timeScale().fitContent();
  }, [candles]);

  return <div ref={containerRef} className="lw-chart" style={{ height }} />;
}
