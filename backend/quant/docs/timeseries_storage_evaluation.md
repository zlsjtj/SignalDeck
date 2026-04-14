# 行情时序存储评估（SQLite -> 专用时序存储）

## 1. 评估背景

- 当前实现已将行情落库到 SQLite：
  - `market_ticks(symbol, ts_utc, price, bid, ask, volume, ...)`
  - `market_klines(symbol, timeframe, ts_utc, open, high, low, close, volume, ...)`
- 目标：在不破坏现有 API 接口的前提下，评估中长期迁移到专用时序存储（TimescaleDB/ClickHouse）的必要性与路径。

## 2. 关键需求

- 高频写入：多 symbol、多 timeframe 持续写入。
- 时间窗查询：按 symbol/timeframe + 时间区间拉取。
- 保留与归档：支持冷热分层与 TTL。
- 运维可控：备份、恢复、监控、权限与变更成本可接受。

## 3. 方案对比

### 3.1 继续使用 SQLite（当前）

- 优点：
  - 部署最简单、无额外服务依赖。
  - 已有脚本体系完整（备份/恢复/维护/清理/压测/故障注入）。
- 风险：
  - 单节点写入与并发扩展上限较低。
  - 大规模时序聚合查询（多维统计）能力有限。

### 3.2 TimescaleDB（PostgreSQL 扩展）

- 优点：
  - 与关系模型兼容度高，迁移 SQL 心智成本低。
  - 原生分区（hypertable）、连续聚合、压缩与保留策略成熟。
  - 便于与“可插拔 PostgreSQL”路线统一。
- 风险：
  - 运维复杂度高于 SQLite（服务化部署、监控、升级）。
  - 成本高于单文件数据库。

### 3.3 ClickHouse

- 优点：
  - 面向大规模分析查询性能强，压缩率高。
  - 适合大批量聚合、报表、长时间窗口分析。
- 风险：
  - 事务与 OLTP 场景不如 PostgreSQL 系。
  - 团队运维与查询模型学习成本更高。

## 4. 结论与阶段决策

- 当前结论：短中期继续使用 SQLite；暂不立即迁移专用时序库。
- 触发迁移阈值（任一满足即启动迁移项目）：
  - `market_ticks + market_klines` 总量长期超过 5,000 万行；
  - 时序写入 p95 > 200ms 且持续 7 天以上；
  - 业务侧出现跨月/跨季度高频聚合报表需求且 SQLite 查询 SLA 不达标；
  - 需要多副本高可用与跨节点读写扩展。
- 首选迁移目标：TimescaleDB（优先于 ClickHouse），以兼容后续 PostgreSQL 可插拔路线。

## 5. 迁移预案（保留接口兼容）

1. 新增时序 Repository 抽象层（SQLite/TimescaleDB 双实现）。
2. 双写阶段：保持现有 API 不变，写入 SQLite + TimescaleDB。
3. 对账阶段：按 symbol/timeframe/时间窗核对条数与关键指标。
4. 读切换阶段：报表类查询优先走 TimescaleDB，异常可回切 SQLite。
5. 稳定后降级 SQLite 时序写入为兜底或归档。
