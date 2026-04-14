import { memo, useMemo } from 'react';
import ReactECharts from 'echarts-for-react';

import type { ISODateString } from '@/types/api';
import { formatTs } from '@/utils/format';

export type EquitySeriesPoint = { ts: ISODateString; equity: number };

type Props = {
  data: EquitySeriesPoint[];
  height?: number;
  title?: string;
};

export const EquityChart = memo(function EquityChart({ data, height = 280, title }: Props) {
  const option = useMemo(() => {
    const xs = data.map((p) => p.ts);
    const ys = data.map((p) => p.equity);
    return {
      backgroundColor: 'transparent',
      title: title ? { text: title, left: 'center', textStyle: { color: '#d7e2f0' } } : undefined,
      tooltip: {
        trigger: 'axis',
        valueFormatter: (v: unknown) => (typeof v === 'number' ? v.toFixed(2) : String(v)),
        formatter: (params: any) => {
          const p = Array.isArray(params) ? params[0] : params;
          const t = p?.axisValue;
          const v = p?.data;
          return `${formatTs(t)}<br/>Equity: ${typeof v === 'number' ? v.toFixed(2) : v}`;
        },
      },
      grid: { left: 44, right: 18, top: title ? 44 : 20, bottom: 34 },
      xAxis: {
        type: 'category',
        data: xs,
        axisLabel: {
          color: 'rgba(215,226,240,0.75)',
          formatter: (v: string) => formatTs(v, 'HH:mm'),
        },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.12)' } },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: 'rgba(215,226,240,0.75)' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } },
      },
      series: [
        {
          name: 'Equity',
          type: 'line',
          data: ys,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2, color: '#00b96b' },
          areaStyle: { opacity: 0.15, color: '#00b96b' },
        },
      ],
    };
  }, [data, title]);

  return <ReactECharts option={option} style={{ height }} notMerge lazyUpdate />;
});

