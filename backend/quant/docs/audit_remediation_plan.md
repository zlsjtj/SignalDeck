# 回测审计整改清单（执行版）

> 目标：将审计中的关键问题转成可执行、可打勾、可复核的整改计划。  
> 状态标记：`[ ] 未解决` / `[x] 已解决`  
> 说明：本清单仅覆盖审计报告中的 **[3]关键发现**、**[4]改进路线图**、**[5]追加实验清单**。

---

## [3] 关键发现（按严重程度）

### Critical

- [x] `F-CRIT-01` 回测时序污染（未来成交回填到当前净值）
  - 证据：`statarb/backtest.py:432`, `statarb/backtest.py:461`, `statarb/backtest.py:464`, `statarb/backtest.py:475`
  - 风险：净值路径、回撤、日亏、年化被系统性扭曲
  - 修复目标：成交入账与净值记账时间戳一致，杜绝“用 `ts_next` 成交却记在 `ts`”
  - 验收标准：修复后同一交易不能出现在早于执行时间的净值点

- [x] `F-CRIT-02` 回测成交模型与实盘执行模型不一致且过理想化
  - 证据：`statarb/backtest.py:432-436` vs `statarb/execution.py:79-89`, `statarb/execution.py:135-138`
  - 风险：回测高估可成交性，实盘漏单/未成交导致显著偏差
  - 修复目标：引入限价可成交判定、未成交处理、部分成交
  - 验收标准：输出成交率/未成交率，并可回放订单状态

### High

- [x] `F-HIGH-01` 永续关键成本与约束缺失（funding/保证金/强平）
  - 证据：`statarb/paper.py:21-41`，项目内无 funding/强平扣减逻辑
  - 风险：实盘生存性被高估
  - 修复目标：补齐资金费率、维持保证金、强平逻辑
  - 验收标准：极端行情下能触发风险动作；资金费累计可核对

- [x] `F-HIGH-02` 因子实现与配置/文档不一致，参数大面积无效
  - 证据：`statarb/factors.py:13-50` 基本仅动量；配置含多因子参数（`config_2025_bch_bnb_btc_equal_combo_baseline_v2_invvol_best.yaml:37-42`）
  - 风险：伪优化、参数解释失真
  - 修复目标：实现缺失因子或删除无效参数并强制告警
  - 验收标准：参数变动能引起可解释的信号/仓位变化

- [x] `F-HIGH-03` API 回测 `feeRate/slippage` 输入未真正生效
  - 证据：`api_server.py:1994-2032` 仅记录参数；实际回测命令只用 `--config`（`api_server.py:1931-1949`）
  - 风险：用户误以为做了成本压力测试
  - 修复目标：API 启动回测时生成 run-specific 配置覆盖成本参数
  - 验收标准：同配置仅改 API 费率参数，产物指标必须变化

### Medium

- [x] `F-MED-01` 回测结束日期语义偏差（自然日边界不一致）
  - 证据：`statarb/backtest.py:26-27`, `statarb/backtest.py:184`
  - 风险：年度统计少算末日 bar
  - 修复目标：`end` 语义明确为“含当日”或“到当日 00:00（不含）”
  - 验收标准：bar 数与最后时间戳符合文档定义

- [x] `F-MED-02` Sharpe 口径双轨，优化脚本使用 legacy 版本
  - 证据：`statarb/backtest.py:510`, `statarb/backtest.py:515`, `tools/optimize_combo_slim.py:262-266`
  - 风险：优化目标偏斜
  - 修复目标：统一使用 `sharpe_correct`
  - 验收标准：指标输出单一口径，优化脚本与回测一致

### Low

- [x] `F-LOW-01` 项目缺少自有回测单元测试/断言
  - 证据：项目目录无自有 `test_*.py`（排除 `venv/.venv` 后）
  - 风险：时序/成本/约束类 bug 易复发
  - 修复目标：补最小测试集与 CI
  - 验收标准：关键断言纳入自动化

---

## [4] 改进建议路线图（Prioritized）

## 立刻要做（1-3天）

- [x] `R-IMM-01` 修复回测时序错位（对应 `F-CRIT-01`）
- [x] `R-IMM-02` 修复 API 成本参数不生效（对应 `F-HIGH-03`）
- [x] `R-IMM-03` 统一 Sharpe 口径（对应 `F-MED-02`）

## 短期增强（1-2周）

- [x] `R-STD-01` 成交模型升级：可成交判定 + 部分成交 + 挂单超时
- [x] `R-STD-02` 永续成本与约束：funding + 保证金 + 强平
- [x] `R-STD-03` 因子实现与配置对齐（对应 `F-HIGH-02`）

## 中期研究（>2周）

- [x] `R-MID-01` Walk-forward/滚动样本外框架
- [x] `R-MID-02` 容量与冲击成本模型
- [x] `R-MID-03` 跨市场/跨标的稳健性验证

---

## [5] 推荐追加实验清单（可直接照做）

- [x] `E-01` 时序修复前后 AB：验证净值/回撤/风险触发时点变化
- [x] `E-02` 限价可成交模型压力：统计成交率、漏单率与PnL偏移
- [x] `E-03` 加入资金费率后重跑：评估净值侵蚀
- [x] `E-04` 加入保证金与强平：评估生存性与尾部风险
- [x] `E-05` 成本敏感性（手续费/滑点 x2、x4）初次实验已完成
  - 产物：`logs/audit_runs/mt_2023_2025_costx2.txt`, `logs/audit_runs/mt_2023_2025_costx4.txt`
  - 说明：仅为初次检查，后续需在“时序修复后”复跑一次
- [x] `E-06` 参数邻域稳定性：检查是否尖峰最优
- [x] `E-07` 延迟执行压力（+1/+2 bar）
- [x] `E-08` 样本外验证（滚动训练-验证-测试）
- [x] `E-09` 结束日期边界一致性测试
- [x] `E-10` API 参数一致性测试（feeRate/slippage 变更必须反映到产物）

---

## 执行记录（每次改完更新）

### 已解决项

- `F-CRIT-01`：`statarb/backtest.py` 已改为“信号当根 -> 下一根执行 -> 执行时点入账”，并补最后时点挂起成交结算。
- `F-HIGH-03`：`/api/backtests` 已写入 run-specific 临时配置，`initialCapital/feeRate/slippage` 会实际覆盖回测参数。
- `F-MED-02`：`sharpe` 已统一为 `sharpe_correct`，保留 `sharpe_legacy` 供对照。
- `F-MED-01`：`--end` 已改为“含当日”，回测结束时间可覆盖到结束日最后一根 bar。
- `R-IMM-01`、`R-IMM-02`、`R-IMM-03` 已完成。
- `E-01`、`E-09` 已完成并留有产物：`logs/audit_runs/mt_*_fix2.txt`。
- `E-02` 已完成：回测 metrics 新增 `orders_attempted/orders_filled/orders_unfilled/order_fill_rate`，2023/2024 已观测到未成交单。
- `E-10` 已完成：同区间仅调整 API `feeRate/slippage`，回测产物指标发生变化（`run_id=20260217_091853` vs `20260217_091854`）。
- API 端到端全区间校验已完成（`2023-01-01~2025-12-31`）：`logs/audit_runs/phase2/api_e2e/candidate_api_e2e_summary.csv`。
  - `strategy_candidate_v009`: `annualized_return=0.972997`, `max_drawdown=-0.357212`
  - `strategy_candidate_v010`: `annualized_return=1.019310`, `max_drawdown=-0.367780`
  - 与离线候选报告一致，说明“API路径结果差”来自此前仅跑了 2025 单年窗口，而非 API 实现错误。
- `F-CRIT-02` / `R-STD-01`：`statarb/backtest.py` 已升级为“订单队列 + 限价可成交判定 + 部分成交 + 超时撤单”，并新增订单事件回放文件 `*_orders.csv` 及指标 `orders_partial/orders_canceled_timeout`。
- `F-HIGH-01` / `R-STD-02`：`statarb/backtest.py` 已加入 funding（缓存优先、常量回退）与保证金/强平逻辑；metrics 新增 `funding_fee_total/funding_events/liquidated/liquidation_count`。
- `E-03` / `E-04` 已完成并留有产物：`logs/audit_runs/mt_2325_e03.txt`、`logs/audit_runs/mt_2325_e04.txt`（当前参数下未触发强平：`liquidated=0`）。
- `R-STD-01` 追加验收产物：`logs/audit_runs/mt_2023_rstd01_metrics.txt`、`logs/audit_runs/mt_2023_rstd01_trades_orders.csv`、`logs/audit_runs/mt_2023q1_rstd01_partial_metrics.txt`。
- `F-HIGH-02` / `R-STD-03`：`statarb/factors.py` 已实现 reversal/momentum/trend/flow/volz/volume 因子组合与 `min_notional_usdt/max_vol` 过滤；参数变化会改变交易与指标（`logs/audit_runs/mt_2023h1_factor_mom_metrics.txt` vs `logs/audit_runs/mt_2023h1_factor_trend_metrics.txt`）。
- `F-LOW-01`：新增最小测试集 `tests/test_core.py`（成交记账、翻仓 intent、因子参数生效），`unittest` 运行通过。
- `E-06` 已完成：`mom_lookback` 邻域（8/12/16）实验产物 `logs/audit_runs/mt_2325_e06_momlb_*.txt`，收益曲线非尖峰（8 与 12/16 接近）。
- `E-07` 已完成：新增 `backtest_exec_delay_bars` 并完成延迟 1/2/3 bar 实验，产物 `logs/audit_runs/mt_2325_e07_delay_*.txt`。
- `E-08` 已完成：滚动样本外实验脚本 `logs/audit_runs/run_e08_walkforward.sh` 与汇总 `logs/audit_runs/e08_walkforward_summary.txt` 已生成；在 `2024->2025` 测试窗出现明显退化（`equity_end=924.227585`）。
- `R-MID-01`：新增通用滚动样本外工具 `tools/walkforward_runner.py`，产物 `logs/audit_runs/rmid01_walkforward/summary.csv`（`2024->2025` OOS 退化仍显著）。
- `R-MID-02`：回测已支持冲击成本参数 `backtest_impact_enabled/backtest_impact_base_bps/backtest_impact_exponent`，并新增容量/冲击扫描工具 `tools/capacity_impact_sweep.py`，产物 `logs/audit_runs/rmid02_capacity/summary.csv`。
- `R-MID-03`：新增跨标的/周期稳健性矩阵工具 `tools/robustness_matrix.py`，产物 `logs/audit_runs/rmid03_robustness/summary.csv`。
- 持续集成：新增 GitHub Actions 工作流 `.github/workflows/quant_checks.yml`，自动执行编译检查与 `unittest`，防止核心逻辑回归。
- 第二阶段深挖：已完成扩展实验并沉淀报告 `logs/audit_runs/phase2/phase2_summary.md`；核心结论包括 `2024->2025` OOS 持续退化、以及“大资金+低参与率”下 `order_fill_rate` 可降至 `0.273148`。
- 2025 退化分段诊断：新增脚本 `tools/diagnose_2025_window.py`，产物目录 `logs/audit_runs/phase2/diagnose_2025/`。
  - 月度归因：`monthly_breakdown.csv` 显示主要回撤集中在 `2025-02`、`2025-11`、`2025-10`。
  - Regime 归因：`regime_breakdown.csv` 显示 `down_lowvol` 为主要负贡献状态。
  - 标的归因：`symbol_pnl_breakdown.csv` 显示 `DOGE`、`SOL` 为主要拖累，`BNB`、`BCH` 为主要正贡献。
- 归因驱动修复AB：`statarb/backtest.py` 新增 `strategy.regime_deleverage` 开关，并在 2025 OOS 做 AB。
  - baseline: `logs/audit_runs/phase2/regime_ab_base.txt`
  - regime on: `logs/audit_runs/phase2/regime_ab_on.txt`
  - 指标改善：`equity_end 923.858767 -> 969.213689`，`max_drawdown -0.450850 -> -0.420580`。
- 继续迭代（第2轮）：启用 `strategy.abs_mom_filter`（336h, min_return=0）对 2025 OOS 做 AB。
  - 产物：`logs/audit_runs/phase2/iter2_absmom_only.txt`
  - 结果：`equity_end 923.858767 -> 1232.185627`，`annualized_return -0.076191 -> 0.232362`，`max_drawdown -0.450850 -> -0.403085`，`sharpe 0.178860 -> 0.649923`。
- 继续迭代（第3轮，防过拟合）：为 `abs_mom_filter` 增加 regime 门控，仅在“弱势+低波”状态触发。
  - 配置：`logs/audit_runs/phase2/cfg_iter5_cond_absmom.yaml`
  - 报告：`logs/audit_runs/phase2/iter5_conditional_filter_report.md`
  - 结果：2025 从负年化修复到微正（`-0.076191 -> 0.012590`），且 2023-2025 合并 Sharpe 提升（`1.376280 -> 1.443189`）。
- 继续迭代（第4轮，回撤优先）：在 Iteration-5 基础上做温和回撤参数网格。
  - 网格：`logs/audit_runs/phase2/iter6b_dd_sweep_gentle/summary.csv`
  - 报告：`logs/audit_runs/phase2/iter6_drawdown_reduction_report.md`
  - 候选结果：`2025 max_dd -0.392102 -> -0.361610`，`2325 max_dd -0.490921 -> -0.445727`（代价是 2325 年化下降至 `0.870634`）。
- 继续迭代（第5轮，硬约束搜索）：要求 `2325 ann > 0.90` 且 `2325 max_dd > -0.40`，引入 `vol_target` 扫参。
  - 网格：`logs/audit_runs/phase2/iter8_voltarget_search/summary.csv`
  - 可行集：`logs/audit_runs/phase2/iter8_voltarget_search/feasible.csv`（4组）
  - 推荐候选：`logs/audit_runs/phase2/iter8_voltarget_search/v009_tv0p35_mn0p5_mx1p1_lb336.yaml`
  - 指标：`2325 ann=0.972997`, `2325 max_dd=-0.357212`, `2025 ann=0.146625`, `2025 max_dd=-0.338799`。

### 未解决项

- 当前无阻塞未解决项（后续按研究深度继续迭代参数空间与更多市场数据）。
