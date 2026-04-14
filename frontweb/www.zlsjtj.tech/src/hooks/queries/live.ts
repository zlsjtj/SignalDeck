import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import type { StrategyDiagnostics } from '@/types/api';

export function usePositionsQuery(enabled = true, strategyId?: string) {
  return useQuery({
    queryKey: queryKeys.positions(strategyId),
    queryFn: () => api.getPositions(strategyId),
    enabled,
    refetchInterval: 8000,
  });
}

export function useOrdersQuery(enabled = true, strategyId?: string) {
  return useQuery({
    queryKey: queryKeys.orders(strategyId),
    queryFn: () => api.getOrders(strategyId),
    enabled,
    refetchInterval: 8000,
  });
}

export function useFillsQuery(enabled = true, strategyId?: string) {
  return useQuery({
    queryKey: queryKeys.fills(strategyId),
    queryFn: () => api.getFills(strategyId),
    enabled,
    refetchInterval: 8000,
  });
}

export function useStrategyDiagnosticsQuery(
  enabled = true,
  strategyId?: string,
  refreshMs = 60_000,
) {
  return useQuery<StrategyDiagnostics>({
    queryKey: queryKeys.strategyDiagnostics(strategyId),
    queryFn: () => api.getStrategyDiagnostics(strategyId),
    enabled: enabled && Boolean(strategyId),
    refetchInterval: refreshMs,
    refetchIntervalInBackground: true,
    staleTime: Math.max(0, refreshMs - 1000),
    retry: 1,
  });
}
