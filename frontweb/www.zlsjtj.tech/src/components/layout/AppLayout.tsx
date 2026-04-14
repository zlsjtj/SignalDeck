import { useMemo, useState } from 'react';
import { Link, Outlet, useLocation } from 'react-router-dom';
import { Drawer, Grid, Layout, Menu, Select, Typography } from 'antd';
import type { MenuProps } from 'antd';
import {
  BarChartOutlined,
  BugOutlined,
  DashboardOutlined,
  ExperimentOutlined,
  QuestionCircleOutlined,
  RadarChartOutlined,
  SettingOutlined,
} from '@ant-design/icons';

import { TopBar } from '@/components/layout/TopBar';
import { useAppWebSocket } from '@/hooks/useAppWebSocket';
import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';
import { env } from '@/utils/env';

const { Sider, Header, Content } = Layout;

function useSelectedKey(): string {
  const { pathname } = useLocation();
  if (pathname.startsWith('/getting-started')) return '/getting-started';
  if (pathname.startsWith('/strategies')) return '/strategies';
  if (pathname.startsWith('/backtests')) return '/backtests';
  return pathname === '/' ? '/dashboard' : pathname;
}

export function AppLayout() {
  const guestReadonly = useAppStore((s) => Boolean(s.isGuest) && !Boolean(s.isAuthenticated));
  useAppWebSocket({ enabled: env.wsEnabled && !guestReadonly });

  const selectedKey = useSelectedKey();
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.lg;
  const [collapsed, setCollapsed] = useState(false);
  const [mobileNavOpen, setMobileNavOpen] = useState(false);
  const theme = useAppStore((s) => s.theme);
  const language = useAppStore((s) => s.language);
  const setLanguage = useAppStore((s) => s.setLanguage);
  const { t } = useI18n();

  const items = useMemo<MenuProps['items']>(
    () => [
      { key: '/getting-started', icon: <QuestionCircleOutlined />, label: <Link to="/getting-started">{byLang('新手开始', 'Getting Started')}</Link> },
      { key: '/dashboard', icon: <DashboardOutlined />, label: <Link to="/dashboard">{t('dashboard')}</Link> },
      { key: '/strategies', icon: <ExperimentOutlined />, label: <Link to="/strategies">{t('strategies')}</Link> },
      { key: '/backtests', icon: <BarChartOutlined />, label: <Link to="/backtests">{t('backtests')}</Link> },
      { key: '/live', icon: <RadarChartOutlined />, label: <Link to="/live">{t('liveMonitor')}</Link> },
      { key: '/risk', icon: <SettingOutlined />, label: <Link to="/risk">{t('risk')}</Link> },
      { key: '/logs', icon: <BugOutlined />, label: <Link to="/logs">{t('logsCenter')}</Link> },
    ],
    [t],
  );

  const languageSelector = (
    <div style={{ padding: '12px 16px' }}>
      <Typography.Text style={{ display: 'block', marginBottom: 6 }}>{t('language')}</Typography.Text>
      <Select
        size="small"
        value={language}
        onChange={(v) => setLanguage(v)}
        style={{ width: '100%' }}
        options={[
          { value: 'zh', label: '中文' },
          { value: 'en', label: 'English' },
        ]}
      />
    </div>
  );

  return (
    <Layout style={{ minHeight: '100vh' }}>
      {!isMobile ? (
        <Sider
          collapsible
          collapsed={collapsed}
          onCollapse={setCollapsed}
          breakpoint="lg"
          style={{
            borderRight: '1px solid rgba(255,255,255,0.06)',
            background: theme === 'dark' ? '#0f1621' : '#ffffff',
          }}
        >
          <div
            style={{
              height: 56,
              display: 'flex',
              alignItems: 'center',
              padding: '0 16px',
              color: theme === 'dark' ? 'rgba(255,255,255,0.92)' : 'rgba(0,0,0,0.88)',
              fontWeight: 700,
              letterSpacing: 0.2,
            }}
          >
            {collapsed ? <RadarChartOutlined style={{ fontSize: 18 }} /> : t('appName')}
          </div>
          <Menu theme={theme === 'dark' ? 'dark' : 'light'} mode="inline" selectedKeys={[selectedKey]} items={items} />
          {!collapsed ? languageSelector : null}
        </Sider>
      ) : (
        <Drawer
          title={t('appName')}
          placement="left"
          open={mobileNavOpen}
          onClose={() => setMobileNavOpen(false)}
          bodyStyle={{ padding: 0 }}
          width={260}
        >
          <Menu
            theme={theme === 'dark' ? 'dark' : 'light'}
            mode="inline"
            selectedKeys={[selectedKey]}
            items={items}
            onClick={() => setMobileNavOpen(false)}
          />
          {languageSelector}
        </Drawer>
      )}

      <Layout>
        <Header
          className="app-header"
          style={{
            padding: isMobile ? '0 12px' : '0 16px',
            background: 'transparent',
            borderBottom: '1px solid rgba(255,255,255,0.06)',
          }}
        >
          <TopBar isMobile={isMobile} onOpenNav={() => setMobileNavOpen(true)} />
        </Header>
        <Content className="app-content" style={{ padding: isMobile ? 12 : 16 }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
