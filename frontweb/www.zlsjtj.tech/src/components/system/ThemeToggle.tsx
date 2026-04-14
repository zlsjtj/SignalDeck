import { Switch } from 'antd';

import { useAppStore } from '@/store/appStore';
import { useI18n } from '@/i18n';

export function ThemeToggle() {
  const theme = useAppStore((s) => s.theme);
  const toggleTheme = useAppStore((s) => s.toggleTheme);
  const { t } = useI18n();

  return (
    <Switch
      checked={theme === 'dark'}
      checkedChildren={t('dark')}
      unCheckedChildren={t('light')}
      onChange={toggleTheme}
      aria-label="Toggle theme"
    />
  );
}
