import { Button, Result } from 'antd';
import { Link } from 'react-router-dom';
import { useI18n } from '@/i18n';

export function NotFoundPage() {
  const { language, t } = useI18n();
  return (
    <Result
      status="404"
      title="404"
      subTitle={language === 'en' ? 'Page not found' : '页面不存在'}
      extra={
        <Button type="primary">
          <Link to="/dashboard">{t('dashboard')}</Link>
        </Button>
      }
    />
  );
}
