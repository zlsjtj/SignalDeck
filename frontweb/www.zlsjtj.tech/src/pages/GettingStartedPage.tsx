import { Button, Card, Space, Typography } from 'antd';
import { Link } from 'react-router-dom';

import { NonTechGuideCard } from '@/components/common/NonTechGuideCard';
import { byLang } from '@/i18n';

export function GettingStartedPage() {
  return (
    <div className="page-shell">
      <Typography.Title level={3} style={{ margin: 0 }}>
        {byLang('新手开始', 'Getting Started')}
      </Typography.Title>

      <NonTechGuideCard
        title={byLang('3步完成首次上手', 'Complete your first run in 3 steps')}
        summary={byLang(
          '不需要技术背景，按下面步骤就可以完成从策略到回测的完整流程。',
          'No technical background required. Follow these steps to complete your first full workflow.',
        )}
        steps={[
          byLang('在“策略”页面创建一个新策略（建议默认参数）', 'Create a strategy with default settings'),
          byLang('在“回测”页面运行回测并查看结果', 'Run a backtest and inspect results'),
          byLang('在“实盘监控”观察状态与风险提示', 'Observe status and risk hints in Live Monitor'),
        ]}
        tip={byLang(
          '如果你目前只想浏览功能，可先使用游客模式。',
          'If you only want to explore, Guest mode is enough.',
        )}
      />

      <Card title={byLang('常见问题', 'Common Questions')}>
        <Typography.Paragraph style={{ marginTop: 0 }}>
          {byLang('1) 看不到实时数据？先确认策略是否已启动。', '1) No realtime data? Check strategy is running.')}
        </Typography.Paragraph>
        <Typography.Paragraph>
          {byLang('2) 收益为负怎么办？先看回测最大回撤与风控设置。', '2) Negative return? Check backtest drawdown and risk settings.')}
        </Typography.Paragraph>
        <Typography.Paragraph style={{ marginBottom: 0 }}>
          {byLang('3) 不知道报错含义？去日志中心按时间筛选后查看。', '3) Error unclear? Go to Logs and filter by time.')}
        </Typography.Paragraph>
      </Card>

      <Card title={byLang('下一步', 'Next Actions')}>
        <Space wrap>
          <Button type="primary">
            <Link to="/strategies">{byLang('去创建策略', 'Create Strategy')}</Link>
          </Button>
          <Button>
            <Link to="/backtests">{byLang('去跑回测', 'Run Backtest')}</Link>
          </Button>
          <Button>
            <Link to="/live">{byLang('去看实盘监控', 'Open Live Monitor')}</Link>
          </Button>
        </Space>
      </Card>
    </div>
  );
}
