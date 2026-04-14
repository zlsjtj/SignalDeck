import { useMemo, useState } from 'react';
import { Button, Card, Empty, Grid, Popconfirm, Space, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { Link, useNavigate } from 'react-router-dom';
import { PlusOutlined, ReloadOutlined } from '@ant-design/icons';

import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { StrategyFormModal } from '@/components/strategies/StrategyFormModal';
import { StrategyStatusTag } from '@/components/strategies/StrategyStatusTag';
import {
  useStartStrategyMutation,
  useStopStrategyMutation,
  useStrategiesQuery,
  useUpdateStrategyMutation,
} from '@/hooks/queries/strategies';
import { useAppStore } from '@/store/appStore';
import { byLang, useI18n } from '@/i18n';
import type { CreateStrategyRequest, Strategy } from '@/types/api';
import { formatTs } from '@/utils/format';

export function StrategiesPage() {
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const isGuest = useAppStore((s) => s.isGuest);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const navigate = useNavigate();
  const { t } = useI18n();
  const { data, isPending, isError, isFetching, refetch, dataUpdatedAt } = useStrategiesQuery();
  const lastUpdatedIso = dataUpdatedAt ? new Date(dataUpdatedAt).toISOString() : undefined;

  const [modalOpen, setModalOpen] = useState(false);
  const [editing, setEditing] = useState<Strategy | null>(null);

  const updateMutation = useUpdateStrategyMutation();
  const startMutation = useStartStrategyMutation();
  const stopMutation = useStopStrategyMutation();

  const columns = useMemo<ColumnsType<Strategy>>(
    () => [
      {
        title: t('name'),
        dataIndex: 'name',
        sorter: (a, b) => a.name.localeCompare(b.name),
        render: (_, r) => (
          <Link className="strategy-name-link" to={`/strategies/${r.id}`}>
            {r.name}
          </Link>
        ),
      },
      {
        title: t('type'),
        dataIndex: 'type',
        width: isMobile ? 120 : 160,
        filters: [
          { text: 'mean_reversion', value: 'mean_reversion' },
          { text: 'trend_following', value: 'trend_following' },
          { text: 'market_making', value: 'market_making' },
          { text: 'custom', value: 'custom' },
        ],
        onFilter: (value, record) => record.type === value,
        render: (value: string) => <span className="strategy-type-text">{value}</span>,
      },
      {
        title: t('status'),
        dataIndex: 'status',
        width: 120,
        filters: [
          { text: 'running', value: 'running' },
          { text: 'stopped', value: 'stopped' },
        ],
        onFilter: (value, record) => record.status === value,
        render: (v) => <StrategyStatusTag status={v} />,
      },
      {
        title: t('updatedAt'),
        dataIndex: 'updatedAt',
        width: 200,
        sorter: (a, b) => (a.updatedAt < b.updatedAt ? -1 : 1),
        defaultSortOrder: 'descend',
        render: (v: string) => formatTs(v),
      },
      {
        title: t('actions'),
        key: 'actions',
        width: 220,
        render: (_, r) => (
          <Space>
            <Button
              size="small"
              disabled={isGuest}
              onClick={() => {
                setEditing(r);
                setModalOpen(true);
              }}
            >
              {t('edit')}
            </Button>
            {r.status === 'running' ? (
              <Popconfirm
                title={byLang('确认停止该策略？', 'Stop this strategy?')}
                description={byLang(
                  '停止后不会再产生新下单请求，已持仓不会自动平仓。',
                  'No new orders will be sent after stopping. Existing positions are not auto-closed.',
                )}
                okText={byLang('确认停止', 'Stop')}
                cancelText={byLang('取消', 'Cancel')}
                disabled={isGuest}
                okButtonProps={{ loading: stopMutation.isPending && stopMutation.variables === r.id }}
                onConfirm={() => stopMutation.mutate(r.id)}
              >
                <Button
                  size="small"
                  danger
                  disabled={isGuest}
                  loading={stopMutation.isPending && stopMutation.variables === r.id}
                >
                  {t('stop')}
                </Button>
              </Popconfirm>
            ) : (
              <Popconfirm
                title={byLang('确认启动该策略？', 'Start this strategy?')}
                description={byLang(
                  '启动后系统会按照策略条件开始发送交易指令。',
                  'Once started, the system can begin sending orders when conditions match.',
                )}
                okText={byLang('确认启动', 'Start')}
                cancelText={byLang('取消', 'Cancel')}
                disabled={isGuest}
                okButtonProps={{ loading: startMutation.isPending && startMutation.variables === r.id }}
                onConfirm={() => startMutation.mutate(r.id)}
              >
                <Button
                  size="small"
                  type="primary"
                  disabled={isGuest}
                  loading={startMutation.isPending && startMutation.variables === r.id}
                >
                  {t('start')}
                </Button>
              </Popconfirm>
            )}
            <Button
              size="small"
              onClick={() => {
                setSelectedLiveStrategyId(r.id);
                navigate(`/strategies/${r.id}`);
              }}
            >
              {t('details')}
            </Button>
          </Space>
        ),
      },
    ],
    [isGuest, isMobile, navigate, setSelectedLiveStrategyId, startMutation, stopMutation, t],
  );

  const openCreate = () => {
    navigate('/strategies/new');
  };

  const handleSubmit = async (req: CreateStrategyRequest) => {
    if (!editing) return;
    await updateMutation.mutateAsync({ id: editing.id, req });
    setModalOpen(false);
  };

  return (
    <div className="page-shell">
      <div className="page-header">
        <Typography.Title level={3} style={{ margin: 0 }}>
          {t('strategies')}
        </Typography.Title>
        <Space size={12} className="page-actions" wrap>
          {!isMobile ? <Typography.Text type="secondary">{t('latestUpdate')}: {formatTs(lastUpdatedIso)}</Typography.Text> : null}
          <Button icon={<ReloadOutlined />} loading={isFetching} disabled={isPending} onClick={() => void refetch()}>
            {t('refresh')}
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={openCreate} disabled={isGuest}>
            {t('createStrategy')}
          </Button>
        </Space>
      </div>
      <NonTechGuideCard
        title={byLang('策略管理推荐顺序', 'Recommended strategy workflow')}
        summary={byLang(
          '非技术用户建议先创建策略，再启动运行，最后进入详情观察日志与状态。',
          'For non-technical users: create first, start second, inspect details and logs third.',
        )}
        steps={[
          byLang('点击“新建策略”，按向导使用默认推荐参数', 'Create strategy with wizard defaults'),
          byLang('在列表里点击“启动”并观察状态变为“运行中”', 'Start and verify status is running'),
          byLang('点击“详情”查看配置与相关日志', 'Open details to check config and logs'),
        ]}
        tip={byLang('若你是游客模式，本页仅可查看，不能修改。', 'Guest mode is read-only on this page.')}
      />

      {isGuest ? <Typography.Text type="warning">{t('guestStrategyNotice')}</Typography.Text> : null}

      <Card>
        {isError ? (
          <ActionableErrorAlert
            title={byLang('策略列表加载失败', 'Failed to load strategy list')}
            steps={[
              byLang('点击“刷新”重试拉取数据', 'Click Refresh to fetch again'),
              byLang('确认网络正常且账号有权限', 'Confirm network and account permissions'),
              byLang('若持续失败，进入日志中心查看报错', 'If it still fails, open Logs to inspect errors'),
            ]}
            retryText={t('refresh')}
            onRetry={() => void refetch()}
            secondaryActionText={byLang('打开日志中心', 'Open Logs')}
            onSecondaryAction={() => navigate('/logs')}
          />
        ) : data && data.length === 0 ? (
          <Empty description={t('noStrategy')} />
        ) : (
          <Table
            className="strategies-table"
            rowKey="id"
            loading={isPending}
            columns={columns}
            dataSource={data ?? []}
            pagination={{ pageSize: 10, showSizeChanger: !isMobile }}
            scroll={{ x: 920 }}
          />
        )}
      </Card>

      <StrategyFormModal
        open={modalOpen}
        loading={updateMutation.isPending}
        initial={editing}
        onCancel={() => setModalOpen(false)}
        onSubmit={handleSubmit}
      />
    </div>
  );
}
