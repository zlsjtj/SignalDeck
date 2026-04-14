import { Tag } from 'antd';

import type { LogLevel } from '@/types/api';
import { useI18n } from '@/i18n';

export function LevelTag({ level }: { level: LogLevel }) {
  const { t } = useI18n();
  if (level === 'error') return <Tag color="red">{t('levelError')}</Tag>;
  if (level === 'warn') return <Tag color="gold">{t('levelWarn')}</Tag>;
  return <Tag color="blue">{t('levelInfo')}</Tag>;
}
