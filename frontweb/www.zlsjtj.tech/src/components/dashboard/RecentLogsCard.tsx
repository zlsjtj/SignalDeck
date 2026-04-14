import { useEffect, useMemo, useState } from 'react';
import { Button, Card, Empty, Select, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { useNavigate } from 'react-router-dom';

import { LevelTag } from '@/components/common/LevelTag';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useLogsQuery } from '@/hooks/queries/logs';
import { useStrategiesQuery } from '@/hooks/queries/strategies';
import { byLang, useI18n } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import type { LogEntry, LogLevel, LogType } from '@/types/api';
import { formatTs } from '@/utils/format';

export function RecentLogsCard() {
  const navigate = useNavigate();
  const { t } = useI18n();
  const selectedLiveStrategyId = useAppStore((s) => s.selectedLiveStrategyId);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const { data: strategies } = useStrategiesQuery();
  const [type, setType] = useState<LogType>('system');
  const [level, setLevel] = useState<LogLevel | 'all'>('all');
  const [strategyId, setStrategyId] = useState<string | undefined>(undefined);

  useEffect(() => {
    if (!selectedLiveStrategyId || strategyId) return;
    setStrategyId(selectedLiveStrategyId);
  }, [selectedLiveStrategyId, strategyId]);

  const { data, isPending, isError, refetch } = useLogsQuery({
    type,
    level: level === 'all' ? undefined : level,
    strategyId,
    limit: 20,
  });

  const columns = useMemo<ColumnsType<LogEntry>>(
    () => [
      {
        title: t('time'),
        dataIndex: 'ts',
        width: 180,
        render: (v: string) => <Typography.Text>{formatTs(v)}</Typography.Text>,
      },
      {
        title: t('level'),
        dataIndex: 'level',
        width: 100,
        render: (v: LogLevel) => <LevelTag level={v} />,
      },
      {
        title: t('message'),
        dataIndex: 'message',
        ellipsis: true,
      },
      {
        title: t('strategy'),
        dataIndex: 'strategyId',
        width: 160,
        render: (v?: string) =>
          v ? (
            <Button
              size="small"
              type="link"
              style={{ padding: 0 }}
              onClick={() => {
                setSelectedLiveStrategyId(v);
                navigate(`/strategies/${v}`);
              }}
            >
              {v}
            </Button>
          ) : (
            '-'
          ),
      },
    ],
    [navigate, setSelectedLiveStrategyId, t],
  );

  return (
    <Card
      title={
        <Space wrap>
          <Typography.Text strong>{t('recentLogs')}</Typography.Text>
          <Select
            size="small"
            value={type}
            onChange={(v) => setType(v)}
            options={[
              { value: 'system', label: t('system') },
              { value: 'strategy', label: t('strategy') },
            ]}
            style={{ width: 120 }}
          />
          <Select
            size="small"
            value={level}
            onChange={(v) => setLevel(v)}
            options={[
              { value: 'all', label: t('all') },
              { value: 'info', label: t('levelInfo') },
              { value: 'warn', label: t('levelWarn') },
              { value: 'error', label: t('levelError') },
            ]}
            style={{ width: 120 }}
          />
          <Select
            size="small"
            value={strategyId}
            onChange={(v) => setStrategyId(v || undefined)}
            allowClear
            className="strategy-select"
            popupClassName="strategy-select-dropdown"
            options={(strategies ?? []).map((s) => ({
              value: s.id,
              label: (
                <span className="strategy-option-label" title={`${s.name} (${s.id})`}>
                  {s.name}
                </span>
              ),
            }))}
            placeholder={t('strategy')}
            style={{ width: 200 }}
          />
        </Space>
      }
    >
      {isError ? (
        <ActionableErrorAlert
          title={byLang('最近日志加载失败', 'Recent logs failed to load')}
          steps={[
            byLang('保持筛选条件不变后点击重试', 'Keep filters and click Retry'),
            byLang('必要时切换日志级别（如 error）缩小范围', 'Switch log level (e.g. error) if needed'),
          ]}
          retryText={t('refresh')}
          onRetry={() => void refetch()}
          secondaryActionText={byLang('打开日志中心', 'Open Logs Center')}
          onSecondaryAction={() => navigate('/logs')}
        />
      ) : data && data.length === 0 ? (
        <Empty description={t('noLogs')} />
      ) : (
        <Table
          rowKey="id"
          size="small"
          loading={isPending}
          columns={columns}
          dataSource={data ?? []}
          pagination={false}
          scroll={{ x: 720 }}
        />
      )}
    </Card>
  );
}
