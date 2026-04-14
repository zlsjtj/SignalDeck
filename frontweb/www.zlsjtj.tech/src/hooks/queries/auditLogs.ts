import { useQuery } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import type { AuditLogsQueryParams } from '@/types/api';

export function useAuditLogsQuery(params: AuditLogsQueryParams, enabled = true) {
  return useQuery({
    queryKey: queryKeys.auditLogs(params),
    queryFn: () => api.getAuditLogs(params),
    refetchInterval: 5000,
    enabled,
  });
}
