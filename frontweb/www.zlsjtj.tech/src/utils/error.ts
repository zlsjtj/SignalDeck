import axios from 'axios';
import { byLang } from '@/i18n';

export function isAxiosErr(e: unknown): boolean {
  return axios.isAxiosError(e);
}

export function errMsg(e: unknown, fallback = byLang('操作失败', 'Action failed')) {
  if (e instanceof Error && e.message) return e.message;
  return fallback;
}
