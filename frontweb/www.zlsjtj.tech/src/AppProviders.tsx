import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ConfigProvider, theme as antdTheme } from 'antd';
import { useEffect } from 'react';
import { BrowserRouter } from 'react-router-dom';

import App from '@/App';
import { useAppStore } from '@/store/appStore';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      refetchOnWindowFocus: false,
      staleTime: 10_000,
    },
    mutations: {
      retry: 0,
    },
  },
});

export function AppProviders() {
  const themeMode = useAppStore((s) => s.theme);
  const algorithm =
    themeMode === 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm;

  useEffect(() => {
    document.documentElement.dataset.theme = themeMode;
  }, [themeMode]);

  return (
    <ConfigProvider
      theme={{
        algorithm,
        token: {
          colorPrimary: '#1677ff',
          borderRadius: 8,
        },
      }}
    >
      <QueryClientProvider client={queryClient}>
        <BrowserRouter>
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </ConfigProvider>
  );
}
