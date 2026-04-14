import { Button, Space, Typography } from 'antd';
import { CopyOutlined } from '@ant-design/icons';

import { copyText } from '@/utils/copy';
import { useI18n } from '@/i18n';

type Props = {
  value: unknown;
  title?: string;
};

export function JsonBlock({ value, title }: Props) {
  const { t } = useI18n();
  const text = (() => {
    try {
      return JSON.stringify(value ?? {}, null, 2);
    } catch {
      return String(value);
    }
  })();

  return (
    <div>
      <Space style={{ marginBottom: 8 }}>
        {title ? <Typography.Text strong>{title}</Typography.Text> : null}
        <Button size="small" icon={<CopyOutlined />} onClick={() => void copyText(text)}>
          {t('copy')}
        </Button>
      </Space>
      <pre
        style={{
          margin: 0,
          padding: 12,
          borderRadius: 10,
          background: 'rgba(255,255,255,0.06)',
          overflow: 'auto',
          maxHeight: 360,
        }}
      >
        <code style={{ fontSize: 12 }}>{text}</code>
      </pre>
    </div>
  );
}
