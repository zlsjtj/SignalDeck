import { afterEach, describe, expect, it, vi } from 'vitest';

describe('env ws url defaults', () => {
  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
    vi.resetModules();
  });

  it('uses wss for https pages by default', async () => {
    vi.stubGlobal('location', { protocol: 'https:', host: 'unit.test' });
    const { env } = await import('./env');
    expect(env.wsUrl).toBe('wss://unit.test/ws');
  });

  it('uses provided VITE_WS_URL when set', async () => {
    vi.stubGlobal('location', { protocol: 'https:', host: 'unit.test' });
    vi.stubEnv('VITE_WS_URL', 'wss://custom.example/ws');
    const { env } = await import('./env');
    expect(env.wsUrl).toBe('wss://custom.example/ws');
  });
});
