import { message } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import { byLang } from '@/i18n';
import type { CreateStrategyRequest, UpdateStrategyRequest } from '@/types/api';
import { errMsg, isAxiosErr } from '@/utils/error';

export function useStrategiesQuery() {
  return useQuery({
    queryKey: queryKeys.strategies,
    queryFn: api.listStrategies,
  });
}

export function useStrategyQuery(id: string | undefined) {
  return useQuery({
    queryKey: id ? queryKeys.strategy(id) : ['strategies', 'missing-id'],
    queryFn: () => api.getStrategy(id!),
    enabled: Boolean(id),
  });
}

export function useCreateStrategyMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateStrategyRequest) => api.createStrategy(req),
    onSuccess: async () => {
      message.success(byLang('策略已创建', 'Strategy created'));
      await qc.invalidateQueries({ queryKey: queryKeys.strategies });
    },
    onError: (e) => {
      if (!isAxiosErr(e)) message.error(errMsg(e, byLang('创建失败', 'Create failed')));
    },
  });
}

export function useUpdateStrategyMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ id, req }: { id: string; req: UpdateStrategyRequest }) => api.updateStrategy(id, req),
    onSuccess: async (_, vars) => {
      message.success(byLang('策略已更新', 'Strategy updated'));
      await qc.invalidateQueries({ queryKey: queryKeys.strategies });
      await qc.invalidateQueries({ queryKey: queryKeys.strategy(vars.id) });
    },
    onError: (e) => {
      if (!isAxiosErr(e)) message.error(errMsg(e, byLang('更新失败', 'Update failed')));
    },
  });
}

export function useStartStrategyMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.startStrategy(id),
    onSuccess: async (_, id) => {
      message.success(byLang('策略已启动', 'Strategy started'));
      await qc.invalidateQueries({ queryKey: queryKeys.strategies });
      await qc.invalidateQueries({ queryKey: queryKeys.strategy(id) });
    },
    onError: (e) => {
      if (!isAxiosErr(e)) message.error(errMsg(e, byLang('启动失败', 'Start failed')));
    },
  });
}

export function useStopStrategyMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => api.stopStrategy(id),
    onSuccess: async (_, id) => {
      message.success(byLang('策略已停止', 'Strategy stopped'));
      await qc.invalidateQueries({ queryKey: queryKeys.strategies });
      await qc.invalidateQueries({ queryKey: queryKeys.strategy(id) });
    },
    onError: (e) => {
      if (!isAxiosErr(e)) message.error(errMsg(e, byLang('停止失败', 'Stop failed')));
    },
  });
}
