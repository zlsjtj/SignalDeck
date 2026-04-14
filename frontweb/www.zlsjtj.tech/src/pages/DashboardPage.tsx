import { useEffect, useMemo } from "react";
import {
  Card,
  Col,
  Grid,
  Row,
  Select,
  Skeleton,
  Space,
  Statistic,
  Typography,
} from "antd";

import { EquityChart } from "@/components/charts/EquityChart";
import { MarketWidget } from "@/components/dashboard/MarketWidget";
import { RecentLogsCard } from "@/components/dashboard/RecentLogsCard";
import { NonTechGuideCard } from "@/components/common/NonTechGuideCard";
import { ActionableErrorAlert } from "@/components/common/ActionableErrorAlert";
import { byLang, useI18n } from "@/i18n";
import { usePortfolioQuery } from "@/hooks/queries/portfolio";
import { useStrategiesQuery } from "@/hooks/queries/strategies";
import { useAppStore } from "@/store/appStore";
import { formatNumber, formatPercent } from "@/utils/format";
import { useNavigate } from "react-router-dom";

export function DashboardPage() {
  const navigate = useNavigate();
  const screens = Grid.useBreakpoint();
  const isMobile = !screens.md;
  const { t } = useI18n();
  const { data: strategies } = useStrategiesQuery();
  const selectedStrategyId = useAppStore((s) => s.selectedLiveStrategyId);
  const setSelectedLiveStrategyId = useAppStore(
    (s) => s.setSelectedLiveStrategyId,
  );
  const validStrategyIds = useMemo(
    () => new Set((strategies ?? []).map((s) => s.id).filter(Boolean)),
    [strategies],
  );
  const defaultStrategyId = useMemo(() => {
    const list = strategies ?? [];
    const running = list.find((s) => !!s.id && s.status === 'running')?.id;
    if (running) return running;
    return list.find((s) => !!s.id)?.id;
  }, [strategies]);
  const effectiveStrategyId =
    selectedStrategyId && validStrategyIds.has(selectedStrategyId)
      ? selectedStrategyId
      : defaultStrategyId;
  const { data, isPending, isError, refetch } = usePortfolioQuery(effectiveStrategyId);

  useEffect(() => {
    if (!defaultStrategyId) return;
    if (selectedStrategyId && validStrategyIds.has(selectedStrategyId)) return;
    if (selectedStrategyId !== defaultStrategyId) {
      setSelectedLiveStrategyId(defaultStrategyId);
    }
  }, [
    defaultStrategyId,
    selectedStrategyId,
    setSelectedLiveStrategyId,
    validStrategyIds,
  ]);

  return (
    <div className="page-shell">
      <Typography.Title level={3} style={{ margin: 0 }}>
        {t("dashboard")}
      </Typography.Title>
      <NonTechGuideCard
        title={byLang("如何看这个页面", "How to read this page")}
        summary={byLang(
          "这里用于快速判断系统当前是否健康，先看收益与回撤，再看图表趋势，最后看最近日志。",
          "Use this page for quick health checks: PnL/Drawdown first, trend chart second, recent logs last.",
        )}
        steps={[
          byLang("先确认策略是否选对（上方策略选择器）", "Confirm selected strategy first"),
          byLang("看“今日收益/本周收益/最大回撤”是否在可接受范围", "Check PnL and drawdown are acceptable"),
          byLang("若异常，进入“日志中心”定位原因", "If abnormal, go to Logs for root cause"),
        ]}
        tip={byLang(
          "提示：策略未运行时会显示历史快照，这是正常现象。",
          "Tip: historical snapshot is expected when strategy is not running.",
        )}
      />
      <Card size="small">
        <Space wrap>
          <Typography.Text>{t("strategy")}</Typography.Text>
          <div style={{ width: isMobile ? "100%" : 320, minWidth: 0 }}>
            <Select
              className="strategy-select"
              popupClassName="strategy-select-dropdown"
              value={effectiveStrategyId}
              onChange={(v: string) => setSelectedLiveStrategyId(v)}
              options={(strategies ?? []).map((s) => ({
                value: s.id,
                label: (
                  <span
                    className="strategy-option-label"
                    title={`${s.name} (${s.id})`}
                  >
                    {s.name}
                  </span>
                ),
              }))}
              placeholder={t("strategy")}
            />
          </div>
        </Space>
      </Card>
      <Row gutter={[16, 16]}>
        <Col xs={24} md={12} lg={8}>
          <Card>
            {isPending ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic
                title={t("pnlToday")}
                value={data?.pnlToday ?? 0}
                precision={2}
                valueStyle={{
                  color: (data?.pnlToday ?? 0) >= 0 ? "#00b96b" : "#ff4d4f",
                }}
              />
            )}
          </Card>
        </Col>
        <Col xs={24} md={12} lg={8}>
          <Card>
            {isPending ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic
                title={t("pnlWeek")}
                value={data?.pnlWeek ?? 0}
                precision={2}
                valueStyle={{
                  color: (data?.pnlWeek ?? 0) >= 0 ? "#00b96b" : "#ff4d4f",
                }}
              />
            )}
          </Card>
        </Col>
        <Col xs={24} md={12} lg={8}>
          <Card>
            {isPending ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic
                title={t("equity")}
                value={data?.equity ?? 0}
                precision={2}
              />
            )}
          </Card>
        </Col>
        <Col xs={24} md={12} lg={8}>
          <Card>
            {isPending ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic
                title={t("maxdd")}
                value={formatPercent(data?.maxDrawdown ?? 0)}
                valueStyle={{ color: "#ff4d4f" }}
              />
            )}
          </Card>
        </Col>
        <Col xs={24} md={12} lg={8}>
          <Card>
            {isPending ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic
                title={t("winRate")}
                value={formatPercent(data?.winRate ?? 0)}
              />
            )}
          </Card>
        </Col>
        <Col xs={24} md={12} lg={8}>
          <Card>
            {isPending ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic
                title={t("tradesTodayWeek")}
                value={`${data?.tradesToday ?? 0} / ${data?.tradesWeek ?? 0}`}
              />
            )}
          </Card>
        </Col>
      </Row>

      {isError ? (
        <Card title={t("portfolio")}>
          <ActionableErrorAlert
            title={byLang("组合数据加载失败", "Portfolio data failed to load")}
            steps={[
              byLang("确认已选择一个策略", "Confirm a strategy is selected"),
              byLang("点击“重试”重新拉取数据", "Click Retry to fetch data again"),
              byLang("仍失败时去日志中心查看错误详情", "If it still fails, check Logs for details"),
            ]}
            retryText={t("refresh")}
            onRetry={() => void refetch()}
            secondaryActionText={byLang("打开日志中心", "Open Logs")}
            onSecondaryAction={() => navigate("/logs")}
          />
        </Card>
      ) : null}
      {data?.stale ? (
        <Typography.Text type="secondary">
          {byLang("当前为历史快照（策略未运行）", "Current data is a historical snapshot (strategy not running)")}
        </Typography.Text>
      ) : null}

      <Row gutter={[16, 16]}>
        <Col xs={24} lg={14}>
          <Card title={t("equityCurve")}>
            <EquityChart data={data?.equityCurve ?? []} height={280} />
            <Typography.Text type="secondary">
              {t("equity")}: {formatNumber(data?.equity, 2)}; {t("cash")}:{" "}
              {formatNumber(data?.cash, 2)}
            </Typography.Text>
          </Card>
        </Col>
        <Col xs={24} lg={10}>
          <MarketWidget />
        </Col>
      </Row>

      <RecentLogsCard />
    </div>
  );
}
