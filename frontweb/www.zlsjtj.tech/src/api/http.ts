import axios from 'axios';
import { notification } from 'antd';

import { byLang } from '@/i18n';
import { useAppStore } from '@/store/appStore';
import { env } from '@/utils/env';

export const http = axios.create({
  baseURL: env.apiBaseUrl,
  timeout: 10_000,
  withCredentials: true,
  headers: env.apiToken ? { 'X-API-Key': env.apiToken } : undefined,
});

http.interceptors.response.use(
  (res) => res,
  (error) => {
    const status: number | undefined = error?.response?.status;
    const state = useAppStore.getState();
    const isGuestReadonly = Boolean(state.isGuest) && !Boolean(state.isAuthenticated);

    if (isGuestReadonly && (status === 401 || status === 403)) {
      return Promise.reject(error);
    }

    const description: string =
      error?.response?.data?.detail ??
      error?.response?.data?.message ??
      error?.message ??
      byLang('网络请求失败', 'Network request failed');

    if (status === 401) {
      notification.error({ message: byLang('401 未授权', '401 Unauthorized'), description });
    } else if (status && status >= 500) {
      notification.error({ message: `${status} ${byLang('服务器错误', 'Server Error')}`, description });
    } else if (error?.code === 'ECONNABORTED') {
      notification.error({ message: byLang('请求超时', 'Request Timeout'), description });
    } else {
      notification.error({ message: byLang('请求错误', 'Request Error'), description });
    }

    return Promise.reject(error);
  },
);
