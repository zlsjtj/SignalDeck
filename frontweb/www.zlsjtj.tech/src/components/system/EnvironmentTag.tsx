import { Tag } from 'antd';

import { env } from '@/utils/env';
import { useI18n } from '@/i18n';

export function EnvironmentTag() {
  const { t } = useI18n();
  if (env.useMock) return <Tag color="gold">{t('mock')}</Tag>;
  return <Tag color="green">{t('prod')}</Tag>;
}
