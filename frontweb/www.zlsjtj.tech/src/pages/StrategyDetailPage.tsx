import { useEffect, useMemo, useState } from 'react';
import { Button, Card, Descriptions, Empty, Grid, Popconfirm, Select, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { Link, useNavigate, useParams } from 'react-router-dom';

import { JsonBlock } from '@/components/common/JsonBlock';
import { LevelTag } from '@/components/common/LevelTag';
import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { StrategyStatusTag } from '@/components/strategies/StrategyStatusTag';
import { useLogsQuery } from '@/hooks/queries/logs';
import { useStartStrategyMutation, useStopStrategyMutation, useStrategyQuery } from '@/hooks/queries/strategies';
import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';
import type { LogEntry, LogLevel } from '@/types/api';
import { formatTs } from '@/utils/format';

export function StrategyDetailPage() {
  const isGuest = useAppStore((s) => s.isGuest);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { id } = useParams();
  const navigate = useNavigate();
  const { t } = useI18n();
  const { data, isPending, isError, refetch } = useStrategyQuery(id);
  const startMutation = useStartStrategyMutation();
  const stopMutation = useStopStrategyMutation();

  const [level, setLevel] = useState<LogLevel | 'all'>('all');
  const logsQuery = useLogsQuery({ type: 'strategy', strategyId: id, level: level === 'all' ? undefined : level, limit: 80 });

  useEffect(() => {
    if (!id) return;
    setSelectedLiveStrategyId(id);
  }, [id, setSelectedLiveStrategyId]);

  const relatedLogs = useMemo(() => {
    const logs = logsQuery.data ?? [];
    if (!id) return logs;
    const filtered = logs.filter((l) => l.strategyId === id);
    return filtered.length > 0 ? filtered : logs;
  }, [id, logsQuery.data]);

  const columns = useMemo<ColumnsType<LogEntry>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('level'), dataIndex: 'level', width: 100, render: (v: LogLevel) => <LevelTag level={v} /> },
      { title: t('message'), dataIndex: 'message', ellipsis: true },
    ],
    [t],
  );

  return (
    <div className="page-shell">
      <div className="page-header">
        <Typography.Title level={3} style={{ margin: 0 }}>{t('strategyDetail')}</Typography.Title>
        <Space wrap className="page-actions">
          <Button><Link to="/strategies">{t('backToList')}</Link></Button>
          {data?.status === 'running' ? (
            <Popconfirm
              title={byLang('确认停止当前策略？', 'Stop this strategy now?')}
              description={byLang(
                '停止后不会再发出新指令，已持仓不会自动平仓。',
                'No new orders will be sent after stopping. Existing positions are not auto-closed.',
              )}
              okText={byLang('确认停止', 'Stop')}
              cancelText={byLang('取消', 'Cancel')}
              disabled={isGuest || !id}
              okButtonProps={{ loading: stopMutation.isPending }}
              onConfirm={() => id && stopMutation.mutate(id)}
            >
              <Button danger disabled={isGuest || !id} loading={stopMutation.isPending}>{t('stop')}</Button>
            </Popconfirm>
          ) : (
            <Popconfirm
              title={byLang('确认启动当前策略？', 'Start this strategy now?')}
              description={byLang(
                '启动后会根据策略条件自动发送交易指令。',
                'After start, trade orders can be sent automatically when conditions match.',
              )}
              okText={byLang('确认启动', 'Start')}
              cancelText={byLang('取消', 'Cancel')}
              disabled={isGuest || !id}
              okButtonProps={{ loading: startMutation.isPending }}
              onConfirm={() => id && startMutation.mutate(id)}
            >
              <Button type="primary" disabled={isGuest || !id} loading={startMutation.isPending}>{t('start')}</Button>
            </Popconfirm>
          )}
        </Space>
      </div>
      <NonTechGuideCard
        title={byLang('本页怎么用', 'How to use this page')}
        summary={byLang(
          '这里用于查看单个策略的状态、配置与相关日志。',
          'Use this page to inspect one strategy: status, config and related logs.',
        )}
        steps={[
          byLang('先看状态：运行中/已停止', 'Check running status first'),
          byLang('再看配置是否符合预期', 'Verify config matches expectation'),
          byLang('最后看相关日志定位问题', 'Use related logs to troubleshoot'),
        ]}
      />

      <Card loading={isPending}>
        {isError || !data ? (
          <ActionableErrorAlert
            title={byLang(`策略详情加载失败：${id ?? '-'}`, `Failed to load strategy detail: ${id ?? '-'}`)}
            steps={[
              byLang('先返回列表确认该策略仍存在', 'Return to list and confirm strategy still exists'),
              byLang('点击“刷新”重试加载', 'Click Refresh to retry loading'),
              byLang('若仍失败，进入日志中心查看错误', 'If it still fails, open Logs for details'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void refetch()}
          />
        ) : (
          <Descriptions column={isMobile ? 1 : 2} bordered size="small">
            <Descriptions.Item label={t('id')}>{data.id}</Descriptions.Item>
            <Descriptions.Item label={t('name')}>{data.name}</Descriptions.Item>
            <Descriptions.Item label={t('type')}>{data.type}</Descriptions.Item>
            <Descriptions.Item label={t('status')}><StrategyStatusTag status={data.status} /></Descriptions.Item>
            <Descriptions.Item label={t('createdAt')}>{formatTs(data.createdAt)}</Descriptions.Item>
            <Descriptions.Item label={t('updatedAt')}>{formatTs(data.updatedAt)}</Descriptions.Item>
            <Descriptions.Item label={t('symbol')} span={2}>{data.config.symbols.join(', ')}</Descriptions.Item>
          </Descriptions>
        )}
      </Card>

      {data ? <Card title={t('config')}><JsonBlock value={data.config} /></Card> : null}

      <Card
        title={
          <Space>
            <Typography.Text strong>{t('relatedLogs')}</Typography.Text>
            <Select
              size="small"
              value={level}
              style={{ width: 120 }}
              onChange={(v) => setLevel(v)}
              options={[
                { value: 'all', label: t('all') },
                { value: 'info', label: t('levelInfo') },
                { value: 'warn', label: t('levelWarn') },
                { value: 'error', label: t('levelError') },
              ]}
            />
          </Space>
        }
      >
        {logsQuery.isError ? (
          <ActionableErrorAlert
            title={byLang('相关日志加载失败', 'Failed to load related logs')}
            steps={[
              byLang('保持当前筛选并点击重试', 'Keep filters and click Retry'),
              byLang('必要时切换日志级别后再试', 'Try switching log level if needed'),
              byLang('若持续失败，进入日志中心全量查看', 'If failure persists, open Logs Center for full query'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void logsQuery.refetch()}
            secondaryActionText={byLang('打开日志中心', 'Open Logs Center')}
            onSecondaryAction={() => navigate('/logs')}
          />
        ) : relatedLogs.length === 0 ? (
          <Empty description={t('noLogs')} />
        ) : (
          <Table rowKey="id" size="small" loading={logsQuery.isPending} columns={columns} dataSource={relatedLogs} pagination={{ pageSize: 10 }} scroll={{ x: 760 }} />
        )}
      </Card>
    </div>
  );
}
