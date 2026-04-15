import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import { env } from '@/utils/env';
import { useAppStore } from '@/store/appStore';

export function useMarketTicksQuery(enabled = true) {
  const wsStatus = useAppStore((s) => s.wsStatus);
  const shouldUseRest = !env.useMock && wsStatus !== 'open';

  return useQuery({
    queryKey: queryKeys.marketTicks,
    queryFn: api.getMarketTicks,
    enabled: enabled && shouldUseRest,
    refetchInterval: env.marketPollMs,
    refetchIntervalInBackground: true,
    staleTime: 0,
    retry: 1,
  });
}

export function useMarketKlinesQuery(symbol: string, enabled = true) {
  return useQuery({
    queryKey: queryKeys.marketKlines(symbol),
    queryFn: () => api.getMarketKlines(symbol),
    enabled: enabled && !env.useMock && Boolean(symbol),
    refetchInterval: 60_000,
    refetchIntervalInBackground: true,
    staleTime: 30_000,
    retry: 1,
  });
}

export function useMarketIntelSummaryQuery(symbol?: string, streamWindowSeconds = 300, enabled = true) {
  return useQuery({
    queryKey: queryKeys.marketIntelSummary(symbol, streamWindowSeconds),
    queryFn: () => api.getMarketIntelSummary(symbol, streamWindowSeconds),
    enabled: enabled && !env.useMock,
    refetchInterval: 60_000,
    refetchIntervalInBackground: true,
    staleTime: 20_000,
    retry: 1,
  });
}
