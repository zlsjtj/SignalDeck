import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import type { LogsQueryParams } from '@/types/api';

export function useLogsQuery(params: LogsQueryParams, enabled = true) {
  return useQuery({
    queryKey: queryKeys.logs(params),
    queryFn: () => api.getLogs(params),
    refetchInterval: 5000,
    enabled,
  });
}
