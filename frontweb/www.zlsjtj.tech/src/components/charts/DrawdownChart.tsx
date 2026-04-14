import { memo, useMemo } from 'react';
import ReactECharts from 'echarts-for-react';

import type { ISODateString } from '@/types/api';
import { formatPercent, formatTs } from '@/utils/format';

export type DrawdownPoint = { ts: ISODateString; dd: number }; // ratio, e.g. 0.08

type Props = {
  data: DrawdownPoint[];
  height?: number;
  title?: string;
};

export const DrawdownChart = memo(function DrawdownChart({ data, height = 220, title }: Props) {
  const option = useMemo(() => {
    const xs = data.map((p) => p.ts);
    const ys = data.map((p) => p.dd);
    return {
      backgroundColor: 'transparent',
      title: title ? { text: title, left: 'center', textStyle: { color: '#d7e2f0' } } : undefined,
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const p = Array.isArray(params) ? params[0] : params;
          const t = p?.axisValue;
          const v = p?.data as number;
          return `${formatTs(t)}<br/>DD: ${formatPercent(v)}`;
        },
      },
      grid: { left: 44, right: 18, top: title ? 44 : 20, bottom: 34 },
      xAxis: {
        type: 'category',
        data: xs,
        axisLabel: { color: 'rgba(215,226,240,0.75)', formatter: (v: string) => formatTs(v, 'HH:mm') },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.12)' } },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: 'rgba(215,226,240,0.75)', formatter: (v: number) => formatPercent(v) },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } },
      },
      series: [
        {
          name: 'Drawdown',
          type: 'bar',
          data: ys,
          barWidth: '60%',
          itemStyle: { color: '#ff4d4f', opacity: 0.65 },
        },
      ],
    };
  }, [data, title]);

  return <ReactECharts option={option} style={{ height }} notMerge lazyUpdate />;
});

