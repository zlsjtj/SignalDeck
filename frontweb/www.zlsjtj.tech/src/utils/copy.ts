import { message } from 'antd';
import { useAppStore } from '@/store/appStore';

function tip(zh: string, en: string) {
  return useAppStore.getState().language === 'en' ? en : zh;
}

export async function copyText(text: string) {
  try {
    await navigator.clipboard.writeText(text);
    message.success(tip('已复制', 'Copied'));
  } catch {
    // Fallback for older browsers / insecure contexts
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.position = 'fixed';
    ta.style.opacity = '0';
    document.body.appendChild(ta);
    ta.focus();
    ta.select();
    try {
      document.execCommand('copy');
      message.success(tip('已复制', 'Copied'));
    } catch {
      message.error(tip('复制失败', 'Copy failed'));
    } finally {
      ta.remove();
    }
  }
}
