import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';

import { ThemeToggle } from '@/components/system/ThemeToggle';
import { useAppStore } from '@/store/appStore';

describe('ThemeToggle', () => {
  beforeEach(() => {
    localStorage.clear();
    useAppStore.setState({ theme: 'dark' });
  });

  it('toggles theme in zustand store', async () => {
    const user = userEvent.setup();
    render(<ThemeToggle />);

    const sw = screen.getByRole('switch', { name: 'Toggle theme' });
    expect(useAppStore.getState().theme).toBe('dark');
    expect(sw).toBeChecked();

    await user.click(sw);
    expect(useAppStore.getState().theme).toBe('light');
    expect(sw).not.toBeChecked();
  });
});
