import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { StrategyFormModal } from '@/components/strategies/StrategyFormModal';
import type { Strategy } from '@/types/api';

function buildStrategy(): Strategy {
  const ts = new Date().toISOString();
  return {
    id: 'stg_1',
    name: 'Demo Strategy',
    type: 'custom',
    status: 'stopped',
    config: {
      symbols: ['BTCUSDT'],
      timeframe: '1m',
      params: { lookback: 20 },
    },
    createdAt: ts,
    updatedAt: ts,
  };
}

describe('StrategyFormModal', () => {
  it('blocks submit on invalid params JSON', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();
    render(
      <StrategyFormModal
        open
        initial={buildStrategy()}
        onCancel={() => void 0}
        onSubmit={onSubmit}
      />,
    );

    const textarea = screen.getByLabelText('参数（JSON）') as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: '{bad json' } });

    await user.click(screen.getByRole('button', { name: /保\s*存/ }));

    expect(onSubmit).not.toHaveBeenCalled();
    expect(await screen.findByText('JSON 格式不合法')).toBeInTheDocument();
  });

  it('submits parsed params JSON', async () => {
    const user = userEvent.setup();
    const onSubmit = vi.fn();
    render(
      <StrategyFormModal
        open
        initial={buildStrategy()}
        onCancel={() => void 0}
        onSubmit={onSubmit}
      />,
    );

    const textarea = screen.getByLabelText('参数（JSON）') as HTMLTextAreaElement;
    fireEvent.change(textarea, { target: { value: '{"lookback": 30, "enabled": true}' } });

    await user.click(screen.getByRole('button', { name: /保\s*存/ }));

    await waitFor(() => expect(onSubmit).toHaveBeenCalledTimes(1));
    expect(onSubmit.mock.calls[0]?.[0]).toMatchObject({
      name: 'Demo Strategy',
      type: 'custom',
      config: {
        symbols: ['BTCUSDT'],
        timeframe: '1m',
        params: { lookback: 30, enabled: true },
      },
    });
  });
});
