import { useEffect, useMemo, useRef, useState } from 'react';
import { Button, Card, Space, Typography } from 'antd';
import { PauseCircleOutlined, PlayCircleOutlined } from '@ant-design/icons';

import { LevelTag } from '@/components/common/LevelTag';
import { useI18n } from '@/i18n';
import type { LogEntry } from '@/types/api';
import { formatTs } from '@/utils/format';

type Props = {
  logs: LogEntry[];
  loading?: boolean;
};

export function BacktestLogsPanel({ logs, loading }: Props) {
  const { t } = useI18n();
  const [paused, setPaused] = useState(false);
  const boxRef = useRef<HTMLDivElement | null>(null);

  const lines = useMemo(() => logs.slice(-500), [logs]);

  useEffect(() => {
    if (paused) return;
    const el = boxRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [lines, paused]);

  return (
    <Card
      title={
        <Space>
          <Typography.Text strong>{t('logsCenter')}</Typography.Text>
          <Button
            size="small"
            icon={paused ? <PlayCircleOutlined /> : <PauseCircleOutlined />}
            onClick={() => setPaused((v) => !v)}
          >
            {paused ? t('resume') : t('pause')}
          </Button>
          <Typography.Text type="secondary">{loading ? t('loading') : `${lines.length} ${t('lines')}`}</Typography.Text>
        </Space>
      }
    >
      <div
        ref={boxRef}
        style={{
          height: 260,
          overflow: 'auto',
          padding: 12,
          borderRadius: 10,
          background: 'rgba(255,255,255,0.06)',
          fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace',
          fontSize: 12,
        }}
      >
        {lines.map((l) => (
          <div key={l.id} style={{ display: 'flex', gap: 10, alignItems: 'baseline', padding: '2px 0' }}>
            <span style={{ width: 160, color: 'rgba(215,226,240,0.85)' }}>{formatTs(l.ts)}</span>
            <span style={{ width: 70 }}>
              <LevelTag level={l.level} />
            </span>
            <span style={{ color: 'rgba(215,226,240,0.92)' }}>{l.message}</span>
          </div>
        ))}
        {lines.length === 0 ? <Typography.Text type="secondary">{t('noLogs')}</Typography.Text> : null}
      </div>
    </Card>
  );
}
