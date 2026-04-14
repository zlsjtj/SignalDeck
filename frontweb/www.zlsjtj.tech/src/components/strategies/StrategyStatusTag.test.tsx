import { render, screen } from '@testing-library/react';

import { StrategyStatusTag } from '@/components/strategies/StrategyStatusTag';

describe('StrategyStatusTag', () => {
  it('renders running', () => {
    render(<StrategyStatusTag status="running" />);
    expect(screen.getByText(/运行中|Running/i)).toBeInTheDocument();
  });

  it('renders stopped', () => {
    render(<StrategyStatusTag status="stopped" />);
    expect(screen.getByText(/已停止|Stopped/i)).toBeInTheDocument();
  });
});
