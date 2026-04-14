import { Tag } from 'antd';

import type { BacktestStatus } from '@/types/api';
import { useI18n } from '@/i18n';

export function BacktestStatusTag({ status }: { status: BacktestStatus }) {
  const { t } = useI18n();
  if (status === 'running') return <Tag color="processing">{t('statusRunning')}</Tag>;
  if (status === 'success') return <Tag color="green">{t('statusSuccess')}</Tag>;
  return <Tag color="red">{t('statusFailed')}</Tag>;
}
