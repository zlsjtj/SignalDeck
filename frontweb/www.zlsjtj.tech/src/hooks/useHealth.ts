import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';

export function useHealthQuery() {
  return useQuery({
    queryKey: queryKeys.health,
    queryFn: api.health,
    refetchInterval: 10_000,
  });
}
