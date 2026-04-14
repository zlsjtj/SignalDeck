import { Button, Space, Typography } from 'antd';
import { MenuOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';

import { BackendStatusIndicator } from '@/components/system/BackendStatusIndicator';
import { EnvironmentTag } from '@/components/system/EnvironmentTag';
import { ThemeToggle } from '@/components/system/ThemeToggle';
import { useAppStore } from '@/store/appStore';
import { useI18n } from '@/i18n';

type TopBarProps = {
  isMobile?: boolean;
  onOpenNav?: () => void;
};

export function TopBar({ isMobile = false, onOpenNav }: TopBarProps) {
  const navigate = useNavigate();
  const logout = useAppStore((s) => s.logout);
  const authUser = useAppStore((s) => s.authUser);
  const isGuest = useAppStore((s) => s.isGuest);
  const { t } = useI18n();

  const onLogout = () => {
    logout();
    navigate('/login', { replace: true });
  };

  return (
    <div className="topbar">
      <Space size={8} wrap>
        {isMobile ? <Button type="text" icon={<MenuOutlined />} aria-label={t('openNavigation')} onClick={onOpenNav} /> : null}
        <Typography.Text strong style={{ fontSize: isMobile ? 14 : 16 }}>
          {isMobile ? t('consoleShort') : t('consoleFull')}
        </Typography.Text>
      </Space>

      <Space size={8} wrap className="topbar-actions">
        {!isMobile ? (
          <Typography.Text type="secondary">
            {isGuest ? t('guestMode') : authUser ? `${t('userPrefix')}: ${authUser}` : ''}
          </Typography.Text>
        ) : null}
        <EnvironmentTag />
        <BackendStatusIndicator compact={isMobile} />
        <ThemeToggle />
        <Button size="small" onClick={onLogout}>
          {isGuest ? t('exitGuest') : t('logout')}
        </Button>
      </Space>
    </div>
  );
}
