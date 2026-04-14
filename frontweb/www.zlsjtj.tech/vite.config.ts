import path from 'node:path';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vitest/config';

export default defineConfig({
  plugins: [react()],
  build: {
    chunkSizeWarningLimit: 1300,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes('node_modules')) {
            return undefined;
          }
          if (
            id.includes('/react/') ||
            id.includes('/react-dom/') ||
            id.includes('/react-router-dom/') ||
            id.includes('/scheduler/')
          ) {
            return 'vendor-react';
          }
          if (
            id.includes('/antd/') ||
            id.includes('/@ant-design/') ||
            id.includes('/@rc-component/') ||
            id.includes('/rc-')
          ) {
            return 'vendor-antd';
          }
          if (
            id.includes('/echarts-for-react/') ||
            id.includes('/lightweight-charts/') ||
            id.includes('/zrender/') ||
            id.includes('/echarts/')
          ) {
            return 'vendor-charts';
          }
          if (
            id.includes('/@tanstack/') ||
            id.includes('/axios/') ||
            id.includes('/dayjs/') ||
            id.includes('/zod/') ||
            id.includes('/zustand/')
          ) {
            return 'vendor-data';
          }
          return undefined;
        },
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    strictPort: true,
  },
  test: {
    environment: 'jsdom',
    globals: true,
    css: true,
    setupFiles: ['./src/setupTests.ts'],
    restoreMocks: true,
    clearMocks: true,
  },
});
