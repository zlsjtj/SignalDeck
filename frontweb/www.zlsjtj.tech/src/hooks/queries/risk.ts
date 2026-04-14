import { message } from 'antd';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';

import { api } from '@/api';
import { queryKeys } from '@/api/queryKeys';
import { byLang } from '@/i18n';
import type { UpdateRiskRequest } from '@/types/api';
import { errMsg, isAxiosErr } from '@/utils/error';

export function useRiskQuery(strategyId?: string) {
  return useQuery({
    queryKey: queryKeys.risk(strategyId),
    queryFn: () => api.getRisk(strategyId),
  });
}

export function useUpdateRiskMutation() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ req, strategyId }: { req: UpdateRiskRequest; strategyId?: string }) => api.updateRisk(req, strategyId),
    onSuccess: async (_, vars) => {
      message.success(byLang('风控参数已保存', 'Risk params saved'));
      await qc.invalidateQueries({ queryKey: queryKeys.risk(vars.strategyId) });
    },
    onError: (e) => {
      if (!isAxiosErr(e)) message.error(errMsg(e, byLang('保存失败', 'Save failed')));
    },
  });
}
