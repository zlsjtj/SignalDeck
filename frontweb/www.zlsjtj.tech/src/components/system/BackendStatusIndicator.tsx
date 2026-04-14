import { Badge, Tooltip, Typography } from 'antd';

import { env } from '@/utils/env';
import { useHealthQuery } from '@/hooks/useHealth';
import { useAppStore } from '@/store/appStore';
import { useI18n } from '@/i18n';

function statusToBadge(healthOk: boolean | null, wsStatus: string) {
  const wsBad = wsStatus === 'closed' || wsStatus === 'error';
  const wsWarn = wsStatus === 'connecting' || wsStatus === 'reconnecting' || wsStatus === 'idle';

  if (healthOk === false || wsBad) return { status: 'error' as const, key: 'disconnected' as const };
  if (healthOk === null || wsWarn) return { status: 'warning' as const, key: 'connecting' as const };
  return { status: 'success' as const, key: 'connected' as const };
}

type BackendStatusIndicatorProps = {
  compact?: boolean;
};

export function BackendStatusIndicator({ compact = false }: BackendStatusIndicatorProps) {
  const { data, isPending, isError } = useHealthQuery();
  const wsStatus = useAppStore((s) => s.wsStatus);
  const wsLastError = useAppStore((s) => s.wsLastError);
  const isGuestReadonly = useAppStore((s) => Boolean(s.isGuest) && !Boolean(s.isAuthenticated));
  const { t } = useI18n();

  const healthOk = env.useMock ? true : isPending ? null : isError ? false : Boolean(data?.ok);
  const wsStatusForBadge = env.wsEnabled ? (env.useMock || isGuestReadonly ? 'open' : wsStatus) : 'open';
  const badge = statusToBadge(healthOk, wsStatusForBadge);
  const dbStatus = data?.db ?? (isPending ? 'unknown' : 'n/a');
  const dbError = data?.db_error;
  const dbRuntimeFailures = data?.db_runtime_failures;
  const dbTooltipSuffix = env.useMock
    ? ''
    : `; DB: ${dbStatus}${dbRuntimeFailures && dbRuntimeFailures > 0 ? `; dbFailures: ${dbRuntimeFailures}` : ''}${
        dbError ? `; dbError: ${dbError}` : ''
      }`;

  const tooltip = env.useMock
    ? 'Mock mode: REST/WS are mocked locally'
    : isGuestReadonly
      ? `HTTP: ${healthOk === null ? 'unknown' : healthOk ? 'ok' : 'down'}; WS: disabled in guest mode (REST fallback)${dbTooltipSuffix}`
    : env.wsEnabled
      ? `HTTP: ${healthOk === null ? 'unknown' : healthOk ? 'ok' : 'down'}; WS: ${wsStatus}${wsLastError ? `; lastError: ${wsLastError}` : ''}${dbTooltipSuffix}`
      : `HTTP: ${healthOk === null ? 'unknown' : healthOk ? 'ok' : 'down'}; WS: disabled (${env.apiProfile})${dbTooltipSuffix}`;

  return (
    <Tooltip title={tooltip}>
      <span style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
        <Badge status={badge.status} />
        {!compact ? <Typography.Text>{t(badge.key)}</Typography.Text> : null}
      </span>
    </Tooltip>
  );
}
