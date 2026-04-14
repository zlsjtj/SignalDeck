import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import type { RiskEventsQueryParams } from '@/types/api';

export function useRiskEventsQuery(params: RiskEventsQueryParams, enabled = true) {
  return useQuery({
    queryKey: queryKeys.riskEvents(params),
    queryFn: () => api.getRiskEvents(params),
    refetchInterval: 5000,
    enabled,
  });
}
