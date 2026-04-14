import { describe, expect, it, vi } from 'vitest';
import type { AxiosRequestConfig, AxiosResponse, InternalAxiosRequestConfig } from 'axios';

const { notificationError } = vi.hoisted(() => ({
  notificationError: vi.fn(),
}));

vi.mock('antd', () => ({
  notification: {
    error: notificationError,
  },
}));

import { http } from './http';

function failingAdapter(errorObj: unknown) {
  return async (config: InternalAxiosRequestConfig): Promise<AxiosResponse> => {
    void config;
    throw errorObj;
  };
}

describe('http interceptor', () => {
  it('prefers FastAPI detail field for error description', async () => {
    notificationError.mockReset();
    const error = {
      response: {
        status: 400,
        data: {
          detail: 'invalid payload',
          message: 'fallback message',
        },
      },
      message: 'axios error',
    };

    await expect(
      http.get('/any', {
        adapter: failingAdapter(error) as AxiosRequestConfig['adapter'],
      }),
    ).rejects.toEqual(error);

    expect(notificationError).toHaveBeenCalledWith(
      expect.objectContaining({
        description: 'invalid payload',
      }),
    );
  });
});
