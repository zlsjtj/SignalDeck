import { memo, useMemo } from 'react';
import ReactECharts from 'echarts-for-react';

import { useLiveStore } from '@/store/liveStore';
import { formatTs } from '@/utils/format';

type Props = {
  height?: number;
};

export const RealtimeEquityChart = memo(function RealtimeEquityChart({ height = 260 }: Props) {
  const series = useLiveStore((s) => s.equitySeries);

  const option = useMemo(() => {
    const xs = series.map((p) => p.ts);
    const ys = series.map((p) => p.equity);
    return {
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        formatter: (params: any) => {
          const p = Array.isArray(params) ? params[0] : params;
          return `${formatTs(p?.axisValue)}<br/>Equity: ${Number(p?.data ?? 0).toFixed(2)}`;
        },
      },
      grid: { left: 44, right: 18, top: 18, bottom: 34 },
      xAxis: {
        type: 'category',
        data: xs,
        axisLabel: { color: 'rgba(215,226,240,0.75)', formatter: (v: string) => formatTs(v, 'HH:mm:ss') },
        axisLine: { lineStyle: { color: 'rgba(255,255,255,0.12)' } },
      },
      yAxis: {
        type: 'value',
        axisLabel: { color: 'rgba(215,226,240,0.75)' },
        splitLine: { lineStyle: { color: 'rgba(255,255,255,0.06)' } },
      },
      series: [
        {
          type: 'line',
          data: ys,
          smooth: true,
          showSymbol: false,
          lineStyle: { width: 2, color: '#1677ff' },
          areaStyle: { opacity: 0.12, color: '#1677ff' },
        },
      ],
    };
  }, [series]);

  return <ReactECharts option={option} style={{ height }} notMerge lazyUpdate />;
});

