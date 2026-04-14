import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { env } from '@/utils/env';

export type ThemeMode = 'light' | 'dark';
export type WsStatus = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed' | 'error';
export type AppLanguage = 'zh' | 'en';
export type LogsFilters = Record<string, unknown>;
export type BacktestsFilters = Record<string, unknown>;
export type LiveFilters = Record<string, unknown>;

const authBase = env.apiBaseUrl.replace(/\/+$/, '');

type AuthStatusResponse = {
  authenticated?: boolean;
  username?: string;
};

type UserPreferencesResponse = {
  theme?: string;
  language?: string;
  selectedLiveStrategyId?: string;
  logsFilters?: Record<string, unknown>;
  backtestsFilters?: Record<string, unknown>;
  liveFilters?: Record<string, unknown>;
};

async function fetchAuthStatus(): Promise<AuthStatusResponse | null> {
  try {
    const resp = await fetch(`${authBase}/auth/status`, {
      method: 'GET',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    });
    if (!resp.ok) return null;
    return (await resp.json()) as AuthStatusResponse;
  } catch {
    return null;
  }
}

async function postAuthLogin(username: string, password: string): Promise<AuthStatusResponse | null> {
  try {
    const resp = await fetch(`${authBase}/auth/login`, {
      method: 'POST',
      credentials: 'include',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ username, password }),
    });
    if (!resp.ok) return null;
    return (await resp.json()) as AuthStatusResponse;
  } catch {
    return null;
  }
}

async function postAuthGuest(): Promise<AuthStatusResponse | null> {
  try {
    const resp = await fetch(`${authBase}/auth/guest`, {
      method: 'POST',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    });
    if (!resp.ok) return null;
    return (await resp.json()) as AuthStatusResponse;
  } catch {
    return null;
  }
}

async function fetchUserPreferences(): Promise<UserPreferencesResponse | null> {
  try {
    const resp = await fetch(`${authBase}/user/preferences`, {
      method: 'GET',
      credentials: 'include',
      headers: { Accept: 'application/json' },
    });
    if (!resp.ok) return null;
    return (await resp.json()) as UserPreferencesResponse;
  } catch {
    return null;
  }
}

async function putUserPreferences(payload: UserPreferencesResponse): Promise<boolean> {
  try {
    const resp = await fetch(`${authBase}/user/preferences`, {
      method: 'PUT',
      credentials: 'include',
      headers: {
        Accept: 'application/json',
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
    });
    return resp.ok;
  } catch {
    return false;
  }
}

function normalizeTheme(theme?: string): ThemeMode {
  return theme === 'light' || theme === 'dark' ? theme : 'dark';
}

function normalizeLanguage(language?: string): AppLanguage {
  return language === 'en' || language === 'zh' ? language : 'zh';
}

function normalizeLogsFilters(logsFilters: unknown): LogsFilters {
  if (!logsFilters || typeof logsFilters !== 'object' || Array.isArray(logsFilters)) return {};
  return { ...(logsFilters as Record<string, unknown>) };
}

function normalizeBacktestsFilters(backtestsFilters: unknown): BacktestsFilters {
  if (!backtestsFilters || typeof backtestsFilters !== 'object' || Array.isArray(backtestsFilters)) return {};
  return { ...(backtestsFilters as Record<string, unknown>) };
}

function normalizeLiveFilters(liveFilters: unknown): LiveFilters {
  if (!liveFilters || typeof liveFilters !== 'object' || Array.isArray(liveFilters)) return {};
  return { ...(liveFilters as Record<string, unknown>) };
}

function syncCurrentPreferences(
  state: Pick<AppState, 'authUser' | 'theme' | 'language' | 'selectedLiveStrategyId' | 'logsFilters' | 'backtestsFilters' | 'liveFilters'>,
): void {
  const authUser = (state.authUser || '').trim();
  if (!authUser) return;
  void putUserPreferences({
    theme: state.theme,
    language: state.language,
    selectedLiveStrategyId: state.selectedLiveStrategyId || '',
    logsFilters: normalizeLogsFilters(state.logsFilters),
    backtestsFilters: normalizeBacktestsFilters(state.backtestsFilters),
    liveFilters: normalizeLiveFilters(state.liveFilters),
  });
}

function postAuthLogout(): void {
  void fetch(`${authBase}/auth/logout`, {
    method: 'POST',
    credentials: 'include',
    headers: { Accept: 'application/json' },
  }).catch(() => undefined);
}

type AppState = {
  theme: ThemeMode;
  wsStatus: WsStatus;
  wsLastError?: string;
  language: AppLanguage;
  isAuthenticated: boolean;
  isGuest: boolean;
  authUser?: string;
  selectedLiveStrategyId?: string;
  logsFilters: LogsFilters;
  backtestsFilters: BacktestsFilters;
  liveFilters: LiveFilters;
  setTheme: (theme: ThemeMode) => void;
  toggleTheme: () => void;
  setWsStatus: (status: WsStatus) => void;
  setWsLastError: (message?: string) => void;
  setLanguage: (language: AppLanguage) => void;
  setSelectedLiveStrategyId: (strategyId?: string) => void;
  setLogsFilters: (filters: LogsFilters) => void;
  setBacktestsFilters: (filters: BacktestsFilters) => void;
  setLiveFilters: (filters: LiveFilters) => void;
  login: (username: string, password: string) => Promise<boolean>;
  restoreAuth: () => Promise<void>;
  enterGuest: () => void;
  logout: () => void;
};

export const useAppStore = create<AppState>()(
  persist(
    (set, get) => ({
      theme: 'dark',
      wsStatus: 'idle',
      wsLastError: undefined,
      language: 'zh',
      isAuthenticated: false,
      isGuest: false,
      authUser: undefined,
      selectedLiveStrategyId: undefined,
      logsFilters: {},
      backtestsFilters: {},
      liveFilters: {},
      setTheme: (theme) => {
        set({ theme });
        syncCurrentPreferences(get());
      },
      toggleTheme: () => {
        set({ theme: get().theme === 'dark' ? 'light' : 'dark' });
        syncCurrentPreferences(get());
      },
      setWsStatus: (wsStatus) => set({ wsStatus }),
      setWsLastError: (wsLastError) => set({ wsLastError }),
      setLanguage: (language) => {
        set({ language });
        syncCurrentPreferences(get());
      },
      setSelectedLiveStrategyId: (selectedLiveStrategyId) => {
        set({ selectedLiveStrategyId });
        syncCurrentPreferences(get());
      },
      setLogsFilters: (logsFilters) => {
        set({ logsFilters: normalizeLogsFilters(logsFilters) });
        syncCurrentPreferences(get());
      },
      setBacktestsFilters: (backtestsFilters) => {
        set({ backtestsFilters: normalizeBacktestsFilters(backtestsFilters) });
        syncCurrentPreferences(get());
      },
      setLiveFilters: (liveFilters) => {
        set({ liveFilters: normalizeLiveFilters(liveFilters) });
        syncCurrentPreferences(get());
      },
      login: async (username, password) => {
        const user = username.trim();
        if (!user || !password) return false;

        const result = await postAuthLogin(user, password);
        const ok = Boolean(result?.authenticated);
        if (ok) {
          const authUser = (result?.username || user).trim() || user;
          set({
            isAuthenticated: true,
            isGuest: false,
            authUser,
          });
          const prefs = await fetchUserPreferences();
          if (prefs) {
            set({
              theme: normalizeTheme(prefs.theme),
              language: normalizeLanguage(prefs.language),
              selectedLiveStrategyId: prefs.selectedLiveStrategyId?.trim() || undefined,
              logsFilters: normalizeLogsFilters(prefs.logsFilters),
              backtestsFilters: normalizeBacktestsFilters(prefs.backtestsFilters),
              liveFilters: normalizeLiveFilters(prefs.liveFilters),
            });
          } else {
            syncCurrentPreferences({
              authUser,
              theme: get().theme,
              language: get().language,
              selectedLiveStrategyId: get().selectedLiveStrategyId,
              logsFilters: get().logsFilters,
              backtestsFilters: get().backtestsFilters,
              liveFilters: get().liveFilters,
            });
          }
        }
        return ok;
      },
      restoreAuth: async () => {
        const result = await fetchAuthStatus();
        const ok = Boolean(result?.authenticated);
        const username = result?.username?.trim() || undefined;
        const isGuestUser = username === 'guest';
        if (ok) {
          set({
            isAuthenticated: !isGuestUser,
            isGuest: isGuestUser,
            authUser: username,
          });
          const prefs = await fetchUserPreferences();
          if (prefs) {
            set({
              theme: normalizeTheme(prefs.theme),
              language: normalizeLanguage(prefs.language),
              selectedLiveStrategyId: prefs.selectedLiveStrategyId?.trim() || undefined,
              logsFilters: normalizeLogsFilters(prefs.logsFilters),
              backtestsFilters: normalizeBacktestsFilters(prefs.backtestsFilters),
              liveFilters: normalizeLiveFilters(prefs.liveFilters),
            });
          } else if (username) {
            syncCurrentPreferences({
              authUser: username,
              theme: get().theme,
              language: get().language,
              selectedLiveStrategyId: get().selectedLiveStrategyId,
              logsFilters: get().logsFilters,
              backtestsFilters: get().backtestsFilters,
              liveFilters: get().liveFilters,
            });
          }
          return;
        }

        const state = get();
        if (state.isAuthenticated) {
          set({
            isAuthenticated: false,
            authUser: undefined,
          });
        }
      },
      enterGuest: () => {
        set({ isAuthenticated: false, isGuest: true, authUser: 'guest' });
        void postAuthGuest().then((result) => {
          const username = result?.username?.trim() || 'guest';
          set({ isAuthenticated: false, isGuest: true, authUser: username });
          void fetchUserPreferences().then((prefs) => {
            if (prefs) {
              set({
                theme: normalizeTheme(prefs.theme),
                language: normalizeLanguage(prefs.language),
                selectedLiveStrategyId: prefs.selectedLiveStrategyId?.trim() || undefined,
                logsFilters: normalizeLogsFilters(prefs.logsFilters),
                backtestsFilters: normalizeBacktestsFilters(prefs.backtestsFilters),
                liveFilters: normalizeLiveFilters(prefs.liveFilters),
              });
            } else {
              syncCurrentPreferences({
                authUser: username,
                theme: get().theme,
                language: get().language,
                selectedLiveStrategyId: get().selectedLiveStrategyId,
                logsFilters: get().logsFilters,
                backtestsFilters: get().backtestsFilters,
                liveFilters: get().liveFilters,
              });
            }
          });
        });
      },
      logout: () => {
        postAuthLogout();
        set({
          isAuthenticated: false,
          isGuest: false,
          authUser: undefined,
          wsStatus: 'idle',
          wsLastError: undefined,
          selectedLiveStrategyId: undefined,
          logsFilters: {},
          backtestsFilters: {},
          liveFilters: {},
        });
      },
    }),
    {
      name: 'quant_frontweb_app',
      partialize: (s) => ({
        theme: s.theme,
        language: s.language,
        isAuthenticated: s.isAuthenticated,
        isGuest: s.isGuest,
        authUser: s.authUser,
        selectedLiveStrategyId: s.selectedLiveStrategyId,
        logsFilters: s.logsFilters,
        backtestsFilters: s.backtestsFilters,
        liveFilters: s.liveFilters,
      }),
    },
  ),
);
