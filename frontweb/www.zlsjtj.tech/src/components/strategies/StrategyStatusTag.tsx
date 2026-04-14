import { Tag } from 'antd';

import type { StrategyStatus } from '@/types/api';
import { useI18n } from '@/i18n';

export function StrategyStatusTag({ status }: { status: StrategyStatus }) {
  const { t } = useI18n();
  if (status === 'running') return <Tag color="green">{t('statusRunning')}</Tag>;
  return <Tag color="default">{t('statusStopped')}</Tag>;
}
