import { useEffect, useMemo, useState } from 'react';
import { Button, Card, Empty, Grid, Input, Select, Table, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { CopyOutlined, DownloadOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';

import { LevelTag } from '@/components/common/LevelTag';
import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { ActionableErrorAlert } from '@/components/common/ActionableErrorAlert';
import { useAuditLogsQuery } from '@/hooks/queries/auditLogs';
import { useLogsQuery } from '@/hooks/queries/logs';
import { useRiskEventsQuery } from '@/hooks/queries/riskEvents';
import { useStrategiesQuery } from '@/hooks/queries/strategies';
import { byLang, useI18n } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import type { AuditLogEntry, LogEntry, LogLevel, LogType, RiskEventEntry } from '@/types/api';
import { downloadCsv } from '@/utils/csv';
import { copyText } from '@/utils/copy';
import { formatTs } from '@/utils/format';

type LogsView = 'runtime' | 'audit' | 'riskEvents';

function normalizeView(raw: unknown): LogsView {
  return raw === 'audit' || raw === 'riskEvents' || raw === 'runtime' ? raw : 'runtime';
}

function normalizeType(raw: unknown): LogType {
  return raw === 'strategy' ? 'strategy' : 'system';
}

function normalizeLevel(raw: unknown): LogLevel | 'all' {
  return raw === 'info' || raw === 'warn' || raw === 'error' || raw === 'all' ? raw : 'all';
}

function normalizeText(raw: unknown): string {
  return typeof raw === 'string' ? raw : '';
}

function normalizeOptionalText(raw: unknown): string | undefined {
  const text = normalizeText(raw).trim();
  return text || undefined;
}

function normalizePageSize(raw: unknown): number {
  const parsed = typeof raw === 'number' ? raw : Number(raw);
  if (!Number.isFinite(parsed)) return 12;
  const normalized = Math.round(parsed);
  return normalized === 10 || normalized === 12 || normalized === 20 || normalized === 50 || normalized === 100
    ? normalized
    : 12;
}

export function LogsPage() {
  const navigate = useNavigate();
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { t } = useI18n();
  const selectedLiveStrategyId = useAppStore((s) => s.selectedLiveStrategyId);
  const setSelectedLiveStrategyId = useAppStore((s) => s.setSelectedLiveStrategyId);
  const logsFilters = useAppStore((s) => s.logsFilters);
  const setLogsFilters = useAppStore((s) => s.setLogsFilters);
  const savedFilters =
    logsFilters && typeof logsFilters === 'object' && !Array.isArray(logsFilters)
      ? (logsFilters as Record<string, unknown>)
      : {};
  const [view, setView] = useState<LogsView>(() => normalizeView(savedFilters.view));
  const [type, setType] = useState<LogType>(() => normalizeType(savedFilters.type));
  const [level, setLevel] = useState<LogLevel | 'all'>(() => normalizeLevel(savedFilters.level));
  const [strategyId, setStrategyId] = useState<string | undefined>(() => normalizeOptionalText(savedFilters.strategyId));
  const [q, setQ] = useState(() => normalizeText(savedFilters.q));
  const [auditAction, setAuditAction] = useState(() => normalizeText(savedFilters.auditAction));
  const [auditEntity, setAuditEntity] = useState(() => normalizeText(savedFilters.auditEntity));
  const [auditOwner, setAuditOwner] = useState(() => normalizeText(savedFilters.auditOwner));
  const [auditStart, setAuditStart] = useState(() => normalizeText(savedFilters.auditStart));
  const [auditEnd, setAuditEnd] = useState(() => normalizeText(savedFilters.auditEnd));
  const [riskStrategyId, setRiskStrategyId] = useState<string | undefined>(() => normalizeOptionalText(savedFilters.riskStrategyId));
  const [riskEventType, setRiskEventType] = useState(() => normalizeText(savedFilters.riskEventType));
  const [riskOwner, setRiskOwner] = useState(() => normalizeText(savedFilters.riskOwner));
  const [riskStart, setRiskStart] = useState(() => normalizeText(savedFilters.riskStart));
  const [riskEnd, setRiskEnd] = useState(() => normalizeText(savedFilters.riskEnd));
  const [runtimePageSize, setRuntimePageSize] = useState(() => normalizePageSize(savedFilters.runtimePageSize));
  const [auditPageSize, setAuditPageSize] = useState(() => normalizePageSize(savedFilters.auditPageSize));
  const [riskEventsPageSize, setRiskEventsPageSize] = useState(() => normalizePageSize(savedFilters.riskEventsPageSize));
  const { data: strategies } = useStrategiesQuery();

  useEffect(() => {
    if (!selectedLiveStrategyId) return;
    if (!strategyId) setStrategyId(selectedLiveStrategyId);
    if (!riskStrategyId) setRiskStrategyId(selectedLiveStrategyId);
  }, [selectedLiveStrategyId, strategyId, riskStrategyId]);

  useEffect(() => {
    setLogsFilters({
      view,
      type,
      level,
      strategyId: strategyId || '',
      q,
      auditAction,
      auditEntity,
      auditOwner,
      auditStart,
      auditEnd,
      riskStrategyId: riskStrategyId || '',
      riskEventType,
      riskOwner,
      riskStart,
      riskEnd,
      runtimePageSize,
      auditPageSize,
      riskEventsPageSize,
    });
  }, [
    view,
    type,
    level,
    strategyId,
    q,
    auditAction,
    auditEntity,
    auditOwner,
    auditStart,
    auditEnd,
    riskStrategyId,
    riskEventType,
    riskOwner,
    riskStart,
    riskEnd,
    runtimePageSize,
    auditPageSize,
    riskEventsPageSize,
    setLogsFilters,
  ]);

  const handlePageSizeChange = (target: LogsView, pageSize?: number) => {
    const normalized = normalizePageSize(pageSize);
    if (target === 'runtime') {
      if (normalized !== runtimePageSize) setRuntimePageSize(normalized);
      return;
    }
    if (target === 'audit') {
      if (normalized !== auditPageSize) setAuditPageSize(normalized);
      return;
    }
    if (normalized !== riskEventsPageSize) setRiskEventsPageSize(normalized);
  };

  const runtimeQuery = useLogsQuery(
    {
      type,
      level: level === 'all' ? undefined : level,
      strategyId,
      q: q.trim() ? q.trim() : undefined,
      limit: 200,
    },
    view === 'runtime',
  );
  const auditQuery = useAuditLogsQuery(
    {
      limit: 200,
      action: auditAction.trim() || undefined,
      entity: auditEntity.trim() || undefined,
      owner: auditOwner.trim() || undefined,
      start: auditStart.trim() || undefined,
      end: auditEnd.trim() || undefined,
    },
    view === 'audit',
  );
  const riskEventsQuery = useRiskEventsQuery(
    {
      limit: 200,
      strategyId: riskStrategyId,
      eventType: riskEventType.trim() || undefined,
      owner: riskOwner.trim() || undefined,
      start: riskStart.trim() || undefined,
      end: riskEnd.trim() || undefined,
    },
    view === 'riskEvents',
  );

  const runtimeData = runtimeQuery.data ?? [];
  const auditData = auditQuery.data ?? [];
  const riskEventsData = riskEventsQuery.data ?? [];
  const activeCount = view === 'runtime' ? runtimeData.length : view === 'audit' ? auditData.length : riskEventsData.length;
  const isPending =
    view === 'runtime' ? runtimeQuery.isPending : view === 'audit' ? auditQuery.isPending : riskEventsQuery.isPending;
  const isError = view === 'runtime' ? runtimeQuery.isError : view === 'audit' ? auditQuery.isError : riskEventsQuery.isError;

  const runtimeColumns = useMemo<ColumnsType<LogEntry>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('level'), dataIndex: 'level', width: 100, render: (v: LogLevel) => <LevelTag level={v} /> },
      { title: t('source'), dataIndex: 'source', width: 120 },
      {
        title: t('strategy'),
        dataIndex: 'strategyId',
        width: 200,
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
      { title: t('message'), dataIndex: 'message', ellipsis: true },
      {
        title: t('actions'),
        key: 'actions',
        width: 90,
        render: (_, r) => (
          <Button
            size="small"
            icon={<CopyOutlined />}
            onClick={() => void copyText(`${r.ts} [${r.level}] (${r.source}) ${r.message}`)}
          >
            {t('copy')}
          </Button>
        ),
      },
    ],
    [navigate, setSelectedLiveStrategyId, t],
  );

  const auditColumns = useMemo<ColumnsType<AuditLogEntry>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('owner'), dataIndex: 'owner', width: 140 },
      { title: t('action'), dataIndex: 'action', width: 180 },
      { title: t('entity'), dataIndex: 'entity', width: 130 },
      { title: t('id'), dataIndex: 'entityId', width: 180, render: (v?: string) => v || '-' },
      {
        title: t('detailJson'),
        dataIndex: 'detail',
        ellipsis: true,
        render: (detail: Record<string, unknown>) => {
          const text = JSON.stringify(detail ?? {});
          return <span title={text}>{text}</span>;
        },
      },
      {
        title: t('actions'),
        key: 'actions',
        width: 90,
        render: (_, r) => (
          <Button size="small" icon={<CopyOutlined />} onClick={() => void copyText(JSON.stringify(r.detail ?? {}))}>
            {t('copy')}
          </Button>
        ),
      },
    ],
    [t],
  );

  const riskEventColumns = useMemo<ColumnsType<RiskEventEntry>>(
    () => [
      { title: t('time'), dataIndex: 'ts', width: 180, render: (v: string) => formatTs(v) },
      { title: t('owner'), dataIndex: 'owner', width: 140 },
      { title: t('strategyKey'), dataIndex: 'strategyKey', width: 210 },
      { title: t('riskEventType'), dataIndex: 'eventType', width: 140 },
      { title: t('rule'), dataIndex: 'rule', width: 160 },
      { title: t('message'), dataIndex: 'message', ellipsis: true },
      {
        title: t('detailJson'),
        dataIndex: 'detail',
        ellipsis: true,
        render: (detail: Record<string, unknown>) => {
          const text = JSON.stringify(detail ?? {});
          return <span title={text}>{text}</span>;
        },
      },
      {
        title: t('actions'),
        key: 'actions',
        width: 90,
        render: (_, r) => (
          <Button size="small" icon={<CopyOutlined />} onClick={() => void copyText(JSON.stringify(r.detail ?? {}))}>
            {t('copy')}
          </Button>
        ),
      },
    ],
    [t],
  );

  const exportLogs = () => {
    if (view === 'runtime') {
      const rows = runtimeData.map((l) => ({
        ts: l.ts,
        level: l.level,
        source: l.source,
        message: l.message,
        strategyId: l.strategyId ?? '',
        backtestId: l.backtestId ?? '',
      }));
      downloadCsv(`logs_${type}.csv`, rows, ['ts', 'level', 'source', 'message', 'strategyId', 'backtestId']);
      return;
    }

    if (view === 'audit') {
      const rows = auditData.map((l) => ({
        id: l.id,
        ts: l.ts,
        owner: l.owner,
        action: l.action,
        entity: l.entity,
        entityId: l.entityId,
        detailJson: JSON.stringify(l.detail ?? {}),
      }));
      downloadCsv('audit_logs.csv', rows, ['id', 'ts', 'owner', 'action', 'entity', 'entityId', 'detailJson']);
      return;
    }

    const rows = riskEventsData.map((l) => ({
      id: l.id,
      ts: l.ts,
      owner: l.owner,
      strategyKey: l.strategyKey,
      eventType: l.eventType,
      rule: l.rule,
      message: l.message,
      detailJson: JSON.stringify(l.detail ?? {}),
    }));
    downloadCsv('risk_events.csv', rows, ['id', 'ts', 'owner', 'strategyKey', 'eventType', 'rule', 'message', 'detailJson']);
  };

  const retryActiveView = () => {
    if (view === 'runtime') {
      void runtimeQuery.refetch();
      return;
    }
    if (view === 'audit') {
      void auditQuery.refetch();
      return;
    }
    void riskEventsQuery.refetch();
  };

  return (
    <div className="page-shell">
      <Typography.Title level={3} style={{ margin: 0 }}>
        {t('logsCenter')}
      </Typography.Title>
      <NonTechGuideCard
        title={byLang('日志排查建议顺序', 'Recommended troubleshooting order')}
        summary={byLang(
          '先看运行日志，再看审计日志，最后看风控事件，可以更快定位问题来源。',
          'Check runtime logs first, then audit logs, then risk events for faster diagnosis.',
        )}
        steps={[
          byLang('运行日志：看系统是否报错', 'Runtime logs: look for direct errors'),
          byLang('审计日志：看谁在什么时候做了什么操作', 'Audit logs: who did what and when'),
          byLang('风控事件：看是否被风控规则拦截', 'Risk events: check rule-triggered blocks'),
        ]}
        tip={byLang('可先筛选策略，再用关键词缩小范围。', 'Filter by strategy first, then use keyword search.')}
      />

      <Card
        title={
          <div
            style={{
              display: 'grid',
              gridTemplateColumns:
                isMobile || view === 'runtime'
                  ? isMobile
                    ? '1fr'
                    : 'auto auto auto auto minmax(220px, 320px) auto'
                  : 'auto minmax(120px, 160px) minmax(120px, 160px) minmax(120px, 160px) minmax(160px, 200px) minmax(160px, 200px) auto',
              gap: 8,
              alignItems: 'center',
            }}
          >
            <Typography.Text strong>{t('filter')}</Typography.Text>
            <Select
              value={view}
              onChange={(v: LogsView) => setView(v)}
              options={[
                { value: 'runtime', label: t('runtimeLogs') },
                { value: 'audit', label: t('auditLogs') },
                { value: 'riskEvents', label: t('riskEvents') },
              ]}
              style={{ width: isMobile ? '100%' : 140 }}
            />
            {view === 'runtime' ? (
              <>
                <Select
                  value={type}
                  onChange={(v) => setType(v)}
                  options={[
                    { value: 'system', label: t('system') },
                    { value: 'strategy', label: t('strategy') },
                  ]}
                  style={{ width: isMobile ? '100%' : 140 }}
                />
                <Select
                  value={level}
                  onChange={(v) => setLevel(v)}
                  options={[
                    { value: 'all', label: t('all') },
                    { value: 'info', label: t('levelInfo') },
                    { value: 'warn', label: t('levelWarn') },
                    { value: 'error', label: t('levelError') },
                  ]}
                  style={{ width: isMobile ? '100%' : 140 }}
                />
                <Select
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
                  style={{ width: isMobile ? '100%' : 180 }}
                />
                <Input
                  value={q}
                  onChange={(e) => setQ(e.target.value)}
                  placeholder={t('keywordPlaceholder')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
              </>
            ) : view === 'audit' ? (
              <>
                <Input
                  value={auditAction}
                  onChange={(e) => setAuditAction(e.target.value)}
                  placeholder={t('action')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={auditEntity}
                  onChange={(e) => setAuditEntity(e.target.value)}
                  placeholder={t('entity')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={auditOwner}
                  onChange={(e) => setAuditOwner(e.target.value)}
                  placeholder={t('owner')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={auditStart}
                  onChange={(e) => setAuditStart(e.target.value)}
                  placeholder={t('startIso')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={auditEnd}
                  onChange={(e) => setAuditEnd(e.target.value)}
                  placeholder={t('endIso')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
              </>
            ) : (
              <>
                <Select
                  value={riskStrategyId}
                  onChange={(v) => setRiskStrategyId(v || undefined)}
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
                  style={{ width: isMobile ? '100%' : 180 }}
                />
                <Input
                  value={riskEventType}
                  onChange={(e) => setRiskEventType(e.target.value)}
                  placeholder={t('riskEventType')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={riskOwner}
                  onChange={(e) => setRiskOwner(e.target.value)}
                  placeholder={t('owner')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={riskStart}
                  onChange={(e) => setRiskStart(e.target.value)}
                  placeholder={t('startIso')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
                <Input
                  value={riskEnd}
                  onChange={(e) => setRiskEnd(e.target.value)}
                  placeholder={t('endIso')}
                  allowClear
                  style={{ width: '100%', maxWidth: '100%' }}
                />
              </>
            )}
            <Button icon={<DownloadOutlined />} onClick={exportLogs} disabled={activeCount === 0}>
              {t('export')}
            </Button>
          </div>
        }
      >
        {isError ? (
          <ActionableErrorAlert
            title={byLang('日志加载失败', 'Failed to load logs')}
            steps={[
              byLang('保持当前筛选条件后点击“重试”', 'Keep current filters and click Retry'),
              byLang('时间筛选建议使用 ISO 格式，例如 2026-03-07T00:00:00Z', 'Use ISO time format, e.g. 2026-03-07T00:00:00Z'),
              byLang('若仍失败，先切换到运行日志查看基础错误', 'If it still fails, switch to Runtime logs first'),
            ]}
            retryText={t('refresh')}
            onRetry={retryActiveView}
          />
        ) : activeCount === 0 ? (
          <Empty description={view === 'riskEvents' ? t('noRiskEvents') : t('noLogs')} />
        ) : (
          <>
            {view === 'runtime' ? (
              <Table
                rowKey="id"
                size="small"
                loading={isPending}
                columns={runtimeColumns}
                dataSource={runtimeData}
                pagination={{ pageSize: runtimePageSize, showSizeChanger: true }}
                onChange={(pagination) => handlePageSizeChange('runtime', pagination?.pageSize)}
                scroll={{ x: 860 }}
              />
            ) : view === 'audit' ? (
              <Table
                rowKey={(r) => String(r.id)}
                size="small"
                loading={isPending}
                columns={auditColumns}
                dataSource={auditData}
                pagination={{ pageSize: auditPageSize, showSizeChanger: true }}
                onChange={(pagination) => handlePageSizeChange('audit', pagination?.pageSize)}
                scroll={{ x: 980 }}
              />
            ) : (
              <Table
                rowKey={(r) => String(r.id)}
                size="small"
                loading={isPending}
                columns={riskEventColumns}
                dataSource={riskEventsData}
                pagination={{ pageSize: riskEventsPageSize, showSizeChanger: true }}
                onChange={(pagination) => handlePageSizeChange('riskEvents', pagination?.pageSize)}
                scroll={{ x: 1200 }}
              />
            )}
          </>
        )}
      </Card>
    </div>
  );
}
