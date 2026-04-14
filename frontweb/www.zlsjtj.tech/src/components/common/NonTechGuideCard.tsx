import { Alert, Card, List, Typography } from 'antd';

type NonTechGuideCardProps = {
  title: string;
  summary: string;
  steps: string[];
  tip?: string;
};

export function NonTechGuideCard({ title, summary, steps, tip }: NonTechGuideCardProps) {
  return (
    <Card size="small" style={{ borderRadius: 10 }}>
      <Alert
        type="info"
        showIcon
        message={title}
        description={
          <div style={{ marginTop: 4 }}>
            <Typography.Paragraph style={{ marginBottom: 10 }}>{summary}</Typography.Paragraph>
            <List
              size="small"
              dataSource={steps}
              renderItem={(item, idx) => (
                <List.Item style={{ padding: '4px 0' }}>
                  <Typography.Text>{`${idx + 1}. ${item}`}</Typography.Text>
                </List.Item>
              )}
            />
            {tip ? (
              <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8 }}>
                {tip}
              </Typography.Text>
            ) : null}
          </div>
        }
      />
    </Card>
  );
}
