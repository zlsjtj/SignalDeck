import { useState } from 'react';
import { Alert, Button, Card, Form, Input, Typography, message } from 'antd';
import { useLocation, useNavigate } from 'react-router-dom';

import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';

type LoginForm = {
  username: string;
  password: string;
};

export function LoginPage() {
  const [submitting, setSubmitting] = useState(false);
  const login = useAppStore((s) => s.login);
  const enterGuest = useAppStore((s) => s.enterGuest);
  const { t } = useI18n();
  const navigate = useNavigate();
  const location = useLocation();

  const redirectTo = (location.state as { from?: string } | null)?.from ?? '/dashboard';

  const onFinish = async (values: LoginForm) => {
    setSubmitting(true);
    try {
      const ok = await login(values.username, values.password);
      if (!ok) {
        message.error(t('invalidCredential'));
        return;
      }
      navigate(redirectTo, { replace: true });
    } finally {
      setSubmitting(false);
    }
  };

  const onGuestEnter = () => {
    enterGuest();
    navigate(redirectTo, { replace: true });
  };

  return (
    <div
      style={{
        minHeight: '100vh',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        padding: 16,
        background:
          'radial-gradient(1200px 600px at 20% -10%, rgba(22,119,255,0.16), transparent), radial-gradient(900px 500px at 100% 0%, rgba(0,185,107,0.12), transparent), #0a121a',
      }}
    >
      <Card style={{ width: 360, borderRadius: 14 }}>
        <Typography.Title level={4} style={{ marginTop: 0, marginBottom: 6 }}>
          {t('loginTitle')}
        </Typography.Title>
        <Typography.Text type="secondary">{t('loginDesc')}</Typography.Text>
        <Alert
          style={{ marginTop: 12 }}
          showIcon
          type="info"
          message={byLang('第一次使用建议', 'Recommended for first-time users')}
          description={byLang(
            '先用账号登录查看完整功能；如果只是参观页面，可点击“游客进入（只读）”。',
            'Sign in for full features. Use Guest mode only if you want a read-only tour.',
          )}
        />

        <Form<LoginForm> layout="vertical" onFinish={onFinish} style={{ marginTop: 18 }}>
          <Form.Item name="username" label={t('username')} rules={[{ required: true, message: t('username') }]}>
            <Input placeholder={t('username')} autoComplete="username" />
          </Form.Item>
          <Form.Item name="password" label={t('password')} rules={[{ required: true, message: t('password') }]}>
            <Input.Password placeholder={t('password')} autoComplete="current-password" />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={submitting}>
            {t('login')}
          </Button>
          <Button style={{ marginTop: 8 }} block onClick={onGuestEnter}>
            {t('guestEnter')}
          </Button>
        </Form>
      </Card>
    </div>
  );
}
