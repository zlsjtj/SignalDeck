import { useCallback, useEffect, useRef } from 'react';
import { notification } from 'antd';

import { env } from '@/utils/env';
import { useAppStore } from '@/store/appStore';
import { startMockWs } from '@/mocks/wsMock';
import { wsMessageSchema } from '@/types/ws';
import { useLiveStore } from '@/store/liveStore';
import type { EquityMessage, LogMessage, OrderMessage, PositionMessage, TickMessage } from '@/types/ws';

export type UseAppWebSocketOptions = {
  enabled: boolean;
};

function safeJsonParse(data: string): unknown {
  try {
    return JSON.parse(data);
  } catch {
    return null;
  }
}

export function useAppWebSocket(options: UseAppWebSocketOptions) {
  const setWsStatus = useAppStore((s) => s.setWsStatus);
  const setWsLastError = useAppStore((s) => s.setWsLastError);
  const pushTick = useLiveStore((s) => s.pushTick);
  const pushEquity = useLiveStore((s) => s.pushEquity);
  const upsertPosition = useLiveStore((s) => s.upsertPosition);
  const upsertOrder = useLiveStore((s) => s.upsertOrder);
  const pushLog = useLiveStore((s) => s.pushLog);
  const selectedLiveStrategyId = useAppStore((s) => s.selectedLiveStrategyId);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectRef = useRef<{ attempt: number; timer?: number }>({ attempt: 0 });
  const aliveRef = useRef(true);
  const pendingRef = useRef<{
    ticks: Map<string, TickMessage>;
    equities: EquityMessage[];
    positions: Map<string, PositionMessage>;
    orders: Map<string, OrderMessage>;
    logs: LogMessage[];
    flushScheduled: boolean;
  }>({
    ticks: new Map(),
    equities: [],
    positions: new Map(),
    orders: new Map(),
    logs: [],
    flushScheduled: false,
  });

  const flushPending = useCallback(() => {
    const pending = pendingRef.current;
    pendingRef.current = {
      ticks: new Map(),
      equities: [],
      positions: new Map(),
      orders: new Map(),
      logs: pending.logs.slice(-80),
      flushScheduled: false,
    };

    // Keep the latest tick for each symbol and merge duplicate stream updates.
    pending.ticks.forEach((tick) => {
      pushTick(tick);
    });

    // Only push the latest few equity points in one frame.
    pending.equities.forEach((eq) => {
      pushEquity(eq);
    });

    pending.positions.forEach((position) => {
      upsertPosition(position);
    });

    pending.orders.forEach((order) => {
      upsertOrder(order);
    });

    pending.logs.forEach((log) => {
      pushLog(log);
    });
  }, [pushEquity, pushLog, pushTick, upsertOrder, upsertPosition]);

  const scheduleFlush = useCallback(() => {
    if (!aliveRef.current) return;
    if (pendingRef.current.flushScheduled) return;
    pendingRef.current.flushScheduled = true;

    if (typeof requestAnimationFrame === 'function') {
      requestAnimationFrame(() => {
        flushPending();
      });
    } else {
      window.setTimeout(flushPending, 16);
    }
  }, [flushPending]);

  useEffect(() => {
    aliveRef.current = true;
    return () => {
      aliveRef.current = false;
    };
  }, []);

  useEffect(() => {
    const localReconnect = reconnectRef;
    const wsRefreshMs = Math.max(200, Math.min(10_000, Math.round(env.marketPollMs)));
    const connectorUrl = (() => {
      const params = [`refresh_ms=${wsRefreshMs}`];
      if (env.marketConfigPath) {
        params.push(`config_path=${encodeURIComponent(env.marketConfigPath)}`);
      }
      if (selectedLiveStrategyId) {
        params.push(`strategy_id=${encodeURIComponent(selectedLiveStrategyId)}`);
      }
      const suffix = params.join('&');
      return env.wsUrl.includes('?') ? `${env.wsUrl}&${suffix}` : `${env.wsUrl}?${suffix}`;
    })();

    if (!options.enabled) {
      setWsStatus('idle');
      setWsLastError(undefined);
      return;
    }

    const handleMessage = (raw: string) => {
      const parsed = safeJsonParse(raw);
      const res = wsMessageSchema.safeParse(parsed);
      if (!res.success) return;

      const msg = res.data;
      const { isGuest, isAuthenticated } = useAppStore.getState();
      const guestReadonly = Boolean(isGuest) && !Boolean(isAuthenticated);
      switch (msg.type) {
        case 'tick':
          pendingRef.current.ticks.set(msg.symbol, msg);
          scheduleFlush();
          return;
        case 'equity':
          if (guestReadonly) return;
          pendingRef.current.equities.push(msg);
          if (pendingRef.current.equities.length > 12) {
            pendingRef.current.equities = pendingRef.current.equities.slice(-12);
          }
          scheduleFlush();
          return;
        case 'position':
          if (guestReadonly) return;
          pendingRef.current.positions.set(msg.symbol, msg);
          scheduleFlush();
          return;
        case 'order':
          if (guestReadonly) return;
          pendingRef.current.orders.set(msg.id, msg);
          scheduleFlush();
          return;
        case 'log':
          if (guestReadonly) return;
          pendingRef.current.logs.push(msg);
          if (pendingRef.current.logs.length > 80) {
            pendingRef.current.logs = pendingRef.current.logs.slice(-80);
          }
          scheduleFlush();
          return;
      }
    };

    if (env.useMock) {
      setWsStatus('open');
      setWsLastError(undefined);
      const stop = startMockWs({
        onMessage: handleMessage,
      });
      return () => stop();
    }

    let closedByUser = false;

    const connect = () => {
      setWsStatus(reconnectRef.current.attempt > 0 ? 'reconnecting' : 'connecting');
      setWsLastError(undefined);

      const ws = new WebSocket(connectorUrl);
      wsRef.current = ws;

      ws.onopen = () => {
        reconnectRef.current.attempt = 0;
        setWsStatus('open');
      };

      ws.onmessage = (ev) => {
        if (typeof ev.data !== 'string') return;
        handleMessage(ev.data);
      };

      ws.onerror = () => {
        setWsLastError('ws error');
        setWsStatus('error');
      };

      ws.onclose = () => {
        wsRef.current = null;
        if (closedByUser || !aliveRef.current) {
          setWsStatus('closed');
          return;
        }

        // Exponential backoff reconnect (max ~30s)
        reconnectRef.current.attempt += 1;
        const ms = Math.min(30_000, 500 * 2 ** reconnectRef.current.attempt);
        setWsStatus('reconnecting');
        notification.warning({
          message: 'WebSocket disconnected',
          description: `Reconnecting in ${Math.round(ms / 1000)}s...`,
          duration: 2,
        });
        reconnectRef.current.timer = window.setTimeout(connect, ms);
      };
    };

    connect();

    return () => {
      closedByUser = true;
      if (localReconnect.current.timer) window.clearTimeout(localReconnect.current.timer);
      pendingRef.current.ticks.clear();
      pendingRef.current.equities = [];
      pendingRef.current.positions.clear();
      pendingRef.current.orders.clear();
      pendingRef.current.logs = [];
      pendingRef.current.flushScheduled = false;
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, [
    options.enabled,
    scheduleFlush,
    pushEquity,
    pushLog,
    pushTick,
    upsertOrder,
    setWsLastError,
    setWsStatus,
    selectedLiveStrategyId,
    upsertPosition,
  ]);
}
