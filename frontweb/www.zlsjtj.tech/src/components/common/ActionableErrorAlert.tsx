import { Alert, Button, Space, Typography } from 'antd';

type ActionableErrorAlertProps = {
  title: string;
  steps: string[];
  retryText?: string;
  onRetry?: () => void;
  secondaryActionText?: string;
  onSecondaryAction?: () => void;
};

export function ActionableErrorAlert({
  title,
  steps,
  retryText,
  onRetry,
  secondaryActionText,
  onSecondaryAction,
}: ActionableErrorAlertProps) {
  return (
    <Alert
      type="error"
      showIcon
      message={title}
      description={
        <div style={{ marginTop: 4 }}>
          {steps.map((step, idx) => (
            <Typography.Text key={`${idx + 1}-${step}`} style={{ display: 'block' }}>
              {`${idx + 1}. ${step}`}
            </Typography.Text>
          ))}
          {onRetry || onSecondaryAction ? (
            <Space wrap style={{ marginTop: 10 }}>
              {onRetry && retryText ? (
                <Button size="small" onClick={() => onRetry()}>
                  {retryText}
                </Button>
              ) : null}
              {onSecondaryAction && secondaryActionText ? (
                <Button size="small" type="link" onClick={() => onSecondaryAction()}>
                  {secondaryActionText}
                </Button>
              ) : null}
            </Space>
          ) : null}
        </div>
      }
    />
  );
}
