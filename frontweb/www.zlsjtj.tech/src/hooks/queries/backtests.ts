import { message } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import { byLang } from '@/i18n';
import type { CreateBacktestRequest } from '@/types/api';
import { errMsg, isAxiosErr } from '@/utils/error';

export function useBacktestsQuery() {
  return useQuery({
    queryKey: queryKeys.backtests,
    queryFn: api.listBacktests,
  });
}

export function useBacktestQuery(id: string | undefined) {
  return useQuery({
    queryKey: id ? queryKeys.backtest(id) : ['backtests', 'missing-id'],
    queryFn: () => api.getBacktest(id!),
    enabled: Boolean(id),
    refetchInterval: (q) => (q.state.data?.status === 'running' ? 2000 : false),
  });
}

export function useBacktestLogsQuery(id: string | undefined, enabled = true) {
  return useQuery({
    queryKey: id ? queryKeys.backtestLogs(id) : ['backtests', 'missing-id', 'logs'],
    queryFn: () => api.getBacktestLogs(id!),
    enabled: Boolean(id) && enabled,
    refetchInterval: 1500,
  });
}

export function useCreateBacktestMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateBacktestRequest) => api.createBacktest(req),
    onSuccess: async () => {
      message.success(byLang('回测任务已创建', 'Backtest task created'));
      await qc.invalidateQueries({ queryKey: queryKeys.backtests });
    },
    onError: (e) => {
      if (!isAxiosErr(e)) message.error(errMsg(e, byLang('创建失败', 'Create failed')));
    },
  });
}
