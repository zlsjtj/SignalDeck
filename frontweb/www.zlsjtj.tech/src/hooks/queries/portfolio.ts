import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';

export function usePortfolioQuery(strategyId?: string) {
  return useQuery({
    queryKey: queryKeys.portfolio(strategyId),
    queryFn: () => api.getPortfolio(strategyId),
    refetchInterval: 5000,
  });
}
