# 数据库开发文档（SQLite 持久化与审计）

> 适用项目：`backend/quant`  
> 文档目标：沉淀本次数据库建设方案、当前实现状态、测试验证结果与后续优化方向。  
> 状态约定：每项均显式标注 `已完成` 或 `未完成`。

---

## 1. 建设目标与范围

- [x] 已完成：明确目标为“长期运行、多人使用、保留策略/回测历史、支持风控审计”。
- [x] 已完成：确定一期方案采用 `SQLite`，优先实现最小可用持久化与审计。
- [x] 已完成：确定持久化对象范围为策略、回测、风控状态、审计日志。
- [x] 已完成：覆盖行情全量时序数据（K 线/tick）入库（`market_ticks` + `market_klines`）。

---

## 2. 技术方案总览

### 2.1 架构选择

- [x] 已完成：采用 `SQLiteStore` 独立模块封装数据库访问，避免在 `api_server.py` 中散落 SQL。
- [x] 已完成：数据库文件默认放置于 `logs/quant_api.db`，便于与现有日志目录统一管理。
- [x] 已完成：启用 `WAL` 模式与 `busy_timeout`，提升并发读写稳定性。
- [x] 已完成：抽象统一 Repository/Service 分层（`db_repository.py` + `db_service.py`）。

### 2.2 配置项

- [x] 已完成：支持 `API_DB_ENABLED`（默认 `true`）。
- [x] 已完成：支持 `API_DB_PATH`（默认 `logs/quant_api.db`）。
- [x] 已完成：健康检查输出数据库状态（`db`、`db_error`）。
- [x] 已完成：支持运行时热切换数据库配置（`POST /api/admin/db/reload`）。

---

## 3. 数据模型（一期）

> 代码位置：`backend/quant/db_store.py`

### 3.1 `strategies`

- [x] 已完成：主键 `strategy_key`（兼容现有作用域策略键）。
- [x] 已完成：冗余索引字段 `owner`、`status`。
- [x] 已完成：`updated_at` 时间索引（`idx_strategies_updated_at`）。
- [x] 已完成：完整业务对象以 `record_json` 存储，保障兼容当前数据结构。
- [x] 已完成：策略配置 JSON 结构化拆表（`strategy_params`）。

### 3.2 `backtests`

- [x] 已完成：主键 `run_id`。
- [x] 已完成：索引字段 `owner`、`status`。
- [x] 已完成：`created_at` 时间索引（`idx_backtests_created_at`）。
- [x] 已完成：完整业务对象以 `record_json` 持久化。
- [x] 已完成：指标结果（`metric_return/metric_sharpe/metric_calmar/metric_max_drawdown`）列式化存储并自动回填历史记录。

### 3.3 `risk_states`

- [x] 已完成：复合主键 `(owner, strategy_key)`。
- [x] 已完成：状态对象 `state_json` 持久化。
- [x] 已完成：风控变更历史版本化（`risk_state_history`）。

### 3.4 `audit_logs`

- [x] 已完成：自增主键 `id` + `ts_utc`。
- [x] 已完成：字段覆盖 `owner/action/entity/entity_id/detail_json`。
- [x] 已完成：索引覆盖 `owner+ts`、`action`、`entity`。
- [x] 已完成：审计日志防篡改（`prev_hash` + `row_hash` 哈希链）。

---

## 4. 后端接入实现（一期）

> 代码主入口：`backend/quant/api_server.py`

### 4.1 启动与状态回载

- [x] 已完成：服务启动时初始化数据库连接与建表。
- [x] 已完成：服务启动时回载策略、回测、风控状态到内存 Store。
- [x] 已完成：数据库不可用时的降级告警上报（初始化失败错误日志 + 可选 webhook 告警）。
- [x] 已完成：运行期持久化失败计数与 health 降级（`db=degraded` + failure detail）。
- [x] 已完成：运行期持久化失败告警接入（错误日志 + webhook 告警 + `/api/metrics` 指标暴露）。

### 4.2 策略数据持久化

- [x] 已完成：策略从配置生成时写库。
- [x] 已完成：策略创建/更新时写库。
- [x] 已完成：策略启动/停止后状态同步写库。
- [x] 已完成：策略删除接口与对应数据库删除（`DELETE /api/strategies/{id}`）。

### 4.3 回测数据持久化

- [x] 已完成：回测启动时写库。
- [x] 已完成：回测创建（业务接口）时写库。
- [x] 已完成：回测状态可通过状态同步流程更新（查询触发）。
- [x] 已完成：回测进程退出时即时回写最终状态（无需查询触发）。
- [x] 已完成：回测中间进度（百分比）周期性持久化（基于回测进度日志，运行中定期写库）。

### 4.4 风控状态持久化

- [x] 已完成：首次从配置生成风控状态时写库。
- [x] 已完成：风控参数更新时写库。
- [x] 已完成：风控触发事件（`triggered/recovered/manual_update`）独立事件表 `risk_events` 落地。

### 4.5 审计日志接入

- [x] 已完成：登录成功/失败/限流/登出审计。
- [x] 已完成：策略创建/更新/启动/停止审计。
- [x] 已完成：回测创建/启动/停止审计。
- [x] 已完成：风控更新审计。
- [x] 已完成：审计覆盖只读查询行为（`/api/config` 配置读取审计 `config.read`）。

---

## 5. 新增/变更接口

### 5.1 健康检查

- [x] 已完成：`GET /api/health` 增加数据库状态字段。
- [x] 已完成：健康检查输出数据库容量、碎片率、最近写入耗时（`db_storage` + `db_last_write_*`）。

### 5.2 审计日志查询

- [x] 已完成：新增 `GET /api/audit/logs`。
- [x] 已完成：支持 `limit/action/entity/owner` 过滤。
- [x] 已完成：权限控制为“管理员可查全量，普通用户仅查本人”。
- [x] 已完成：支持时间区间过滤（`start`/`end`，ISO datetime）。
- [x] 已完成：支持游标分页（`cursor`，基于 `id` 递减游标）。

### 5.3 风控事件查询

- [x] 已完成：新增 `GET /api/risk/events`。
- [x] 已完成：支持 `strategy_id/event_type/owner` 过滤（含管理员全量、普通用户仅本人）。
- [x] 已完成：支持时间区间过滤（`start`/`end`，ISO datetime）。
- [x] 已完成：支持游标分页（`cursor`，基于 `id` 递减游标）。

### 5.4 数据库运行时管理

- [x] 已完成：新增 `GET /api/admin/db/config`（管理员查看当前 DB 配置与状态）。
- [x] 已完成：新增 `POST /api/admin/db/reload`（管理员运行时切换 DB 路径/启停/内存状态迁移）。

### 5.5 审计链校验与结构化报表

- [x] 已完成：新增 `GET /api/audit/verify`（哈希链完整性校验，支持 owner/start_id/end_id/limit）。
- [x] 已完成：新增 `GET /api/reports/db/summary`（审计、风控、时序落库的结构化汇总报表）。

### 5.6 风控历史查询

- [x] 已完成：新增 `GET /api/risk/history`。
- [x] 已完成：支持 `strategy_id/owner/cursor/limit` 过滤与权限隔离。

---

## 6. 文档与配置更新

- [x] 已完成：`README.md` 增加数据库配置项说明。
- [x] 已完成：`docs/api_backend.md` 增加数据库配置与审计接口说明。
- [x] 已完成：补充本开发文档（当前文件）。
- [x] 已完成：补充 `.env.example` 模板文件。

---

## 7. 测试与验证

### 7.1 已完成验证

- [x] 已完成：`api_server.py`、`db_store.py`、`test_db_store.py` 语法编译通过（`py_compile`）。
- [x] 已完成：新增 `tests/test_db_store.py`，覆盖策略/回测/风控/审计基本读写与过滤。
- [x] 已完成：新增 `tests/test_api_db_runtime_alerting.py`，覆盖运行期失败计数、告警阈值、慢操作/锁冲突指标与 health/metrics 输出。
- [x] 已完成：审计日志时间区间/游标分页测试覆盖（store + API）。
- [x] 已完成：新增 `tests/test_sqlite_backup_tool.py`，覆盖备份可用性与保留策略。
- [x] 已完成：新增 `tests/test_sqlite_retention_cleanup_tool.py`，覆盖审计日志/回测元数据 TTL 清理。
- [x] 已完成：新增 `tests/test_api_idempotency.py`，覆盖策略/回测重复请求幂等行为。
- [x] 已完成：新增 `tests/test_sqlite_cold_backup_tool.py`，覆盖冷备脚本与保留策略。
- [x] 已完成：新增 `tests/test_sqlite_restore_drill_tool.py`，覆盖恢复演练脚本校验链路。
- [x] 已完成：新增 `tests/test_api_strategy_delete_and_read_audit.py`，覆盖策略删除与配置读取审计。
- [x] 已完成：新增 `tests/test_backtest_progress_persistence.py`，覆盖回测进度解析、运行中回写与退出收敛。
- [x] 已完成：新增 `tests/test_api_risk_events.py`，覆盖风控事件 `triggered/recovered/manual_update` 写入与权限过滤。
- [x] 已完成：新增 `tests/test_api_db_runtime_reload_and_reports.py`，覆盖 DB 热切换、审计链校验接口、汇总报表与风控历史权限隔离。
- [x] 已完成：新增 `tests/test_sqlite_fault_injection_tool.py`，覆盖故障注入脚本。
- [x] 已完成：新增 `tests/test_sqlite_verify_audit_chain_tool.py`，覆盖审计链校验脚本与篡改检测。
- [x] 已完成：新增 `tests/test_api_portfolio_drawdown_metrics.py`，覆盖最大回撤来源于真实 paper 曲线（非停机置零）。
- [x] 已完成：`tests/test_core.py` 显式纳入 CI 自动执行并新增基线校验（`tools/check_test_core_baseline.py` + `tests/baselines/test_core_cases.txt`）。
- [x] 已完成：新增端到端联调自动化脚本 `tools/e2e_frontend_backend_sqlite.py`（前端 typecheck + 后端 API + SQLite 联调）。
- [x] 已完成：新增 `tests/test_e2e_frontend_backend_sqlite_tool.py`，覆盖联调脚本（快速模式）可用性。
- [x] 已完成：新增真实 PostgreSQL 联调脚本 `tools/e2e_backend_postgres.py`（连接、迁移、读写、运行时切换链路）。
- [x] 已完成：新增 `tests/test_e2e_backend_postgres_tool.py`（失败路径必跑 + `QUANT_E2E_POSTGRES_DSN` 实库联调自动执行）。
- [x] 已完成：新增 PostgreSQL 报表回归测试 `tests/test_postgres_store_report_summary.py`（覆盖 `owner=None` 汇总查询）。
- [x] 已完成：CI 增加 `postgres-integration` 任务（PostgreSQL service + 回归测试 + 联调测试）。

### 7.2 补充验证（本轮已完成）

- [x] 已完成：端到端联调测试（前端 + 后端 + SQLite）自动化脚本。
- [x] 已完成：并发压测（多用户并发创建策略、回测）（`tools/sqlite_concurrency_stress.py` + `tests/test_sqlite_concurrency_stress_tool.py`）。
- [x] 已完成：故障注入（磁盘满、数据库锁冲突、I/O 抖动）测试（`tools/sqlite_fault_injection.py` + `tests/test_sqlite_fault_injection_tool.py`）。
- [x] 已完成：将 `tests/test_core.py` 纳入持续集成自动化执行并固化验收基线。
- [x] 已完成：权限/隔离自动化测试（`/api/audit/logs` owner 过滤、跨用户数据不可见）。
- [x] 已完成：真实 PostgreSQL 实例联调（连接、迁移、读写、运行时切换）验证（`tools/e2e_backend_postgres.py` + `tests/test_e2e_backend_postgres_tool.py`）。

---

## 8. 运维与发布建议

### 8.1 运行建议

- [x] 已完成：默认启用数据库，可开箱持久化。
- [x] 已完成：数据库路径支持配置化，允许独立挂载目录。
- [x] 已完成：提供 SQLite 维护脚本 `tools/sqlite_maintenance.py`（checkpoint/analyze/vacuum/碎片统计）。
- [x] 已完成：提供 SQLite 自动维护任务脚本 `tools/sqlite_maintenance_job.py` 与运维文档 `docs/sqlite_maintenance_ops.md`（可直接接入 cron）。
- [x] 已完成：提供 cron 幂等安装脚本 `tools/install_sqlite_maintenance_cron.sh`。
- [x] 已完成：提供 Prometheus 抓取与告警规则模板（`ops/prometheus/*.yml`）及接入文档。
- [x] 已完成：生产环境数据库文件定期备份策略文档化（`tools/sqlite_backup.py` + `docs/sqlite_backup_restore.md`）。
- [x] 已完成：归档/清理策略（审计日志、历史回测元数据）自动化任务（`tools/sqlite_retention_cleanup.py`）。

### 8.2 备份建议（建议执行）

- [x] 已完成：每日冷备（停写窗口内复制 `quant_api.db`）脚本与 cron 安装器已提供。
- [x] 已完成：热备方案（在线 `backup` API）落地（`tools/sqlite_backup.py`）。
- [x] 已完成：恢复演练（从备份恢复并验证业务可读写）脚本化（`tools/sqlite_restore_drill.py`）。

---

## 9. 后续优化方向（Roadmap）

> 每项均标注状态；包含已落地项与后续规划项。

### P0（优先级最高）

- [x] 已完成：引入 `schema_version` 迁移机制（`db_store.py` 内置版本化迁移）。
- [x] 已完成：补充事务边界与幂等保护（策略启动/回测创建重复请求返回既有运行态或既有 run）。
- [x] 已完成：审计日志增加时间区间过滤与分页（`start`/`end`/`cursor`）。
- [x] 已完成：提供后台清理任务（审计日志 TTL、历史回测元数据清理）。
- [x] 已完成：前端 `quant-api-server` 适配层已对齐 `/api/backtests`、`/api/strategies`（避免绕过 DB 持久化链路）。
- [x] 已完成：运行期持久化失败观测闭环（错误日志、失败计数、webhook 告警、health 降级）。
- [x] 已完成：回测进程退出时即时回写最终状态（而非仅在查询时同步）。

### P1（中优先级）

- [x] 已完成：将关键字段结构化（策略名、symbol、回测区间、收益指标）以提升查询能力（`schema_version v3` + 新索引）。
- [x] 已完成：新增“风控事件表”记录触发、恢复、人工干预闭环（`risk_events` + `/api/risk/events`）。
- [x] 已完成：新增数据库观测指标（慢查询计数、写入失败计数、锁等待时间）。
- [x] 已完成：前端新增审计日志页面与筛选能力（运行日志/审计日志双视图，支持 action/entity/owner/start/end 过滤）。
- [x] 已完成：补齐索引现状（新增 `idx_strategies_updated_at`、`idx_backtests_created_at`）。
- [x] 已完成：前端状态指示组件显示 `db` / `db_error`（TopBar `BackendStatusIndicator`）。
- [x] 已完成：补充权限/隔离自动化测试（`/api/audit/logs` owner 过滤、跨用户数据不可见）。

### P2（中长期）

- [x] 已完成：可插拔数据库后端（PostgreSQL）并保持接口兼容（`postgres_store.py` + `API_DB_BACKEND`）。
- [x] 已完成：引入数据签名/哈希链增强审计不可抵赖性（`audit_logs` 哈希链 + 校验接口/脚本）。
- [x] 已完成：将行情/时序数据迁移到专用时序存储（如 TimescaleDB/ClickHouse 等）评估（`docs/timeseries_storage_evaluation.md`）。
- [x] 已完成：SQLite 运维任务自动化（`tools/sqlite_maintenance_job.py` + `docs/sqlite_maintenance_ops.md`）。

---

## 10. 里程碑验收清单（按阶段）

### 里程碑 M1：最小可用持久化

- [x] 已完成：策略、回测、风控状态持久化可用。
- [x] 已完成：审计日志写入与查询接口可用。
- [x] 已完成：后端健康检查可反映 DB 状态。
- [x] 已完成：基础单测覆盖数据库核心行为。

### 里程碑 M2：生产可运维

- [x] 已完成：迁移机制上线。
- [x] 已完成：备份恢复流程自动化并演练（脚本 + 自动化测试）。
- [x] 已完成：并发压测达标并形成报告（`docs/sqlite_concurrency_stress_report.md`）。
- [x] 已完成：日志清理归档策略上线。

### 里程碑 M3：高可靠与可扩展

- [x] 已完成：可平滑切换 PostgreSQL（`POST /api/admin/db/reload` 支持 `backend/postgresDsn` + 内存状态迁移）。
- [x] 已完成：结构化分析查询能力上线（`/api/reports/db/summary` + 审计/风控筛查接口）。
- [x] 已完成：前端审计与风控事件看板上线。

---

## 11. 相关文件索引

- [x] 已完成：数据库模块  
  `backend/quant/db_store.py`
- [x] 已完成：PostgreSQL 后端模块  
  `backend/quant/postgres_store.py`
- [x] 已完成：数据库 Repository 抽象  
  `backend/quant/db_repository.py`
- [x] 已完成：数据库 Service 层  
  `backend/quant/db_service.py`
- [x] 已完成：后端接入点  
  `backend/quant/api_server.py`
- [x] 已完成：后端主 README  
  `backend/quant/README.md`
- [x] 已完成：接口文档  
  `backend/quant/docs/api_backend.md`
- [x] 已完成：数据库单测  
  `backend/quant/tests/test_db_store.py`
- [x] 已完成：审计权限测试  
  `backend/quant/tests/test_api_audit_permissions.py`
- [x] 已完成：运行期告警链路测试  
  `backend/quant/tests/test_api_db_runtime_alerting.py`
- [x] 已完成：SQLite 运维脚本  
  `backend/quant/tools/sqlite_maintenance.py`
- [x] 已完成：SQLite 自动任务脚本  
  `backend/quant/tools/sqlite_maintenance_job.py`
- [x] 已完成：SQLite 运维文档  
  `backend/quant/docs/sqlite_maintenance_ops.md`
- [x] 已完成：环境变量模板  
  `backend/quant/.env.example`
- [x] 已完成：PostgreSQL 可选依赖清单  
  `backend/quant/requirements-postgres.txt`
- [x] 已完成：cron 安装脚本  
  `backend/quant/tools/install_sqlite_maintenance_cron.sh`
- [x] 已完成：SQLite 备份脚本  
  `backend/quant/tools/sqlite_backup.py`
- [x] 已完成：SQLite 备份 cron 安装脚本  
  `backend/quant/tools/install_sqlite_backup_cron.sh`
- [x] 已完成：SQLite 冷备脚本  
  `backend/quant/tools/sqlite_cold_backup.py`
- [x] 已完成：SQLite 冷备 cron 安装脚本  
  `backend/quant/tools/install_sqlite_cold_backup_cron.sh`
- [x] 已完成：SQLite 恢复演练脚本  
  `backend/quant/tools/sqlite_restore_drill.py`
- [x] 已完成：Prometheus 抓取配置  
  `backend/quant/ops/prometheus/scrape_quant_api.yml`
- [x] 已完成：Prometheus 告警规则  
  `backend/quant/ops/prometheus/alerts_quant_db.yml`
- [x] 已完成：Prometheus 接入文档  
  `backend/quant/docs/prometheus_db_alerting.md`
- [x] 已完成：SQLite 备份恢复文档  
  `backend/quant/docs/sqlite_backup_restore.md`
- [x] 已完成：SQLite TTL 清理脚本  
  `backend/quant/tools/sqlite_retention_cleanup.py`
- [x] 已完成：SQLite TTL 清理 cron 安装脚本  
  `backend/quant/tools/install_sqlite_retention_cleanup_cron.sh`
- [x] 已完成：SQLite TTL 清理运维文档  
  `backend/quant/docs/sqlite_retention_ops.md`
- [x] 已完成：幂等保护测试  
  `backend/quant/tests/test_api_idempotency.py`
- [x] 已完成：TTL 清理测试  
  `backend/quant/tests/test_sqlite_retention_cleanup_tool.py`
- [x] 已完成：冷备脚本测试  
  `backend/quant/tests/test_sqlite_cold_backup_tool.py`
- [x] 已完成：恢复演练脚本测试  
  `backend/quant/tests/test_sqlite_restore_drill_tool.py`
- [x] 已完成：策略删除/只读审计测试  
  `backend/quant/tests/test_api_strategy_delete_and_read_audit.py`
- [x] 已完成：回测进度持久化测试  
  `backend/quant/tests/test_backtest_progress_persistence.py`
- [x] 已完成：风控事件闭环测试  
  `backend/quant/tests/test_api_risk_events.py`
- [x] 已完成：组合回撤真实性测试  
  `backend/quant/tests/test_api_portfolio_drawdown_metrics.py`
- [x] 已完成：DB 热切换/汇总报表/风控历史权限测试  
  `backend/quant/tests/test_api_db_runtime_reload_and_reports.py`
- [x] 已完成：SQLite 并发压测脚本  
  `backend/quant/tools/sqlite_concurrency_stress.py`
- [x] 已完成：SQLite 并发压测报告  
  `backend/quant/docs/sqlite_concurrency_stress_report.md`
- [x] 已完成：SQLite 故障注入脚本  
  `backend/quant/tools/sqlite_fault_injection.py`
- [x] 已完成：SQLite 故障注入测试  
  `backend/quant/tests/test_sqlite_fault_injection_tool.py`
- [x] 已完成：SQLite 审计链校验脚本  
  `backend/quant/tools/sqlite_verify_audit_chain.py`
- [x] 已完成：SQLite 审计链校验测试  
  `backend/quant/tests/test_sqlite_verify_audit_chain_tool.py`
- [x] 已完成：时序存储评估文档  
  `backend/quant/docs/timeseries_storage_evaluation.md`
- [x] 已完成：前端日志中心（运行日志/审计日志）  
  `frontweb/www.zlsjtj.tech/src/pages/LogsPage.tsx`
- [x] 已完成：前端风控事件查询 Hook  
  `frontweb/www.zlsjtj.tech/src/hooks/queries/riskEvents.ts`
- [x] 已完成：前端审计日志查询 Hook  
  `frontweb/www.zlsjtj.tech/src/hooks/queries/auditLogs.ts`
- [x] 已完成：前端审计日志 API 适配  
  `frontweb/www.zlsjtj.tech/src/api/quantApi.ts`
- [x] 已完成：前端后端状态指示组件（展示 `db` / `db_error`）  
  `frontweb/www.zlsjtj.tech/src/components/system/BackendStatusIndicator.tsx`
- [x] 已完成：`test_core` 基线校验脚本  
  `backend/quant/tools/check_test_core_baseline.py`
- [x] 已完成：端到端联调自动化脚本  
  `backend/quant/tools/e2e_frontend_backend_sqlite.py`
- [x] 已完成：端到端联调脚本测试  
  `backend/quant/tests/test_e2e_frontend_backend_sqlite_tool.py`
- [x] 已完成：PostgreSQL 联调自动化脚本  
  `backend/quant/tools/e2e_backend_postgres.py`
- [x] 已完成：PostgreSQL 联调自动化测试  
  `backend/quant/tests/test_e2e_backend_postgres_tool.py`
- [x] 已完成：PostgreSQL 报表回归测试  
  `backend/quant/tests/test_postgres_store_report_summary.py`
- [x] 已完成：PostgreSQL schema v18（JSONB/GIN + TIMESTAMPTZ）迁移资产测试  
  `backend/quant/tests/test_postgres_typed_mirror_migration_assets.py`
- [x] 已完成：策略自动编译链路测试  
  `backend/quant/tests/test_api_strategy_auto_compile.py`
- [x] 已完成：多账号隔离测试  
  `backend/quant/tests/test_api_multi_account_isolation.py`
- [x] 已完成：会话持久化测试  
  `backend/quant/tests/test_auth_session_persistence.py`
- [x] 已完成：13.3 SQLite/PostgreSQL 一致性测试  
  `backend/quant/tests/test_postgres_sqlite_feature_parity_for_13_3.py`
- [x] 已完成：PostgreSQL 联调验收基线文档  
  `backend/quant/docs/postgres_e2e_acceptance.md`
- [x] 已完成：PostgreSQL 联调实跑报告（2026-03-04）  
  `backend/quant/docs/postgres_e2e_report_2026-03-04.json`
- [x] 已完成：CI 工作流（含 PostgreSQL 集成任务）  
  `backend/quant/.github/workflows/quant_checks.yml`
- [x] 已完成：前端游客后端会话接入（`auth/guest`）  
  `frontweb/www.zlsjtj.tech/src/store/appStore.ts`
- [x] 已完成：用户偏好 API（`/api/user/preferences`）  
  `backend/quant/api_server.py`
- [x] 已完成：用户偏好数据库化测试  
  `backend/quant/tests/test_api_user_preferences.py`
- [x] 已完成：运行日志结构化入库（`runtime_logs` + `/api/logs` DB 查询）  
  `backend/quant/api_server.py`  
  `backend/quant/db_store.py`  
  `backend/quant/postgres_store.py`
- [x] 已完成：策略诊断快照时序化（`strategy_diagnostics_snapshots`）  
  `backend/quant/api_server.py`  
  `backend/quant/db_store.py`  
  `backend/quant/postgres_store.py`  
  `backend/quant/tests/test_api_strategy_diagnostics_snapshots.py`
- [x] 已完成：历史 owner 补写脚本（含 dry-run）  
  `backend/quant/tools/backfill_owner_columns.py`
- [x] 已完成：历史 owner 补写脚本测试  
  `backend/quant/tests/test_backfill_owner_columns_tool.py`

---

## 12. 当前结论

- [x] 已完成：本次数据库建设已满足“一期可用”目标（持久化 + 审计 + 可查询）。
- [x] 已完成：系统已具备长期运行所需的基础数据留存能力。
- [x] 已完成：达到“真实 PostgreSQL 已联调”的严格生产验收基线（`tools/e2e_backend_postgres.py` + `tests/test_e2e_backend_postgres_tool.py` + `docs/postgres_e2e_acceptance.md`）。

---

## 13. 数据库进一步优化建议（下一阶段候选）

> 说明：以下为“可继续做得更好”的增强项，不影响第 12 节的一期完成结论。

- [x] 已完成：统一时间字段为原生时区类型（PostgreSQL `TIMESTAMPTZ`）的兼容迁移基线。  
  已落地 schema v18：为核心时间列新增 `*_tz` 生成列（`TIMESTAMPTZ`）与索引，保留原 `TEXT` 字段以平滑过渡（`postgres_store.py`）。
- [x] 已完成：将 `record_json/detail_json/state_json` 在 PostgreSQL 侧升级为 `JSONB` 并补充 GIN 索引。  
  已落地 schema v18：新增 JSONB 生成列（`record_jsonb/detail_jsonb/state_jsonb`）与 GIN 索引，兼容历史文本字段（`postgres_store.py` + `tests/test_postgres_typed_mirror_migration_assets.py`）。
- [x] 已完成：为 `audit_logs/risk_events/market_ticks/market_klines` 引入按时间分区（按月/按周），降低大表膨胀后的查询与清理成本。  
  已落地 PostgreSQL schema v20：新增月分区资产（`quant_ensure_monthly_partition`）与四张表路由触发器，支持按月子表自动创建（上月/本月/下月预热 + 新月份按写入时自动补建）并保留现有 API/查询接口兼容（`postgres_store.py`）。
- [x] 已完成：补充 PostgreSQL 备份与恢复体系（`pg_dump` 基线备份 + WAL 归档 + PITR 演练）并脚本化验收（`tools/postgres_backup_pitr_drill.py` + `tests/test_postgres_backup_pitr_drill_tool.py` + `docs/postgres_backup_pitr_ops.md`）。
- [x] 已完成：增加 PostgreSQL 专项性能基线（写入 TPS、分页查询 P95、报表聚合 P95）并纳入 CI 定期回归（`tools/postgres_performance_baseline.py` + `tests/test_postgres_performance_baseline_tool.py` + `docs/postgres_performance_baseline.md` + `.github/workflows/quant_checks.yml` `postgres-integration` 步骤与 `schedule` 触发）。
- [x] 已完成：接入连接池（如 PgBouncer）与连接上限治理，避免高并发场景下连接震荡。  
  已落地 PostgreSQL 内置连接池参数化接入（`PostgresStore` + `API_DB_POSTGRES_POOL_*`），支持最小/最大连接数与取连接超时治理，并在 `health/metrics` 与 `GET /api/admin/db/config` 暴露池状态观测字段。
- [x] 已完成：新增数据库约束强化（枚举约束、`CHECK`、关键字段唯一性）并回放历史数据校验脚本（`schema v17`：SQLite 触发器约束 + PostgreSQL `CHECK` 约束 + 活跃 API Token 名称唯一索引；`tools/validate_db_constraints_replay.py` + `tests/test_validate_db_constraints_replay_tool.py`）。
- [x] 已完成：引入“Outbox + 异步投递”模式承接 webhook/外部通知，避免业务写事务与外部网络调用耦合。  
  已落地 `alert_outbox` 异步投递 worker（`api_server.py`），支持启动/关闭生命周期管理、DB 热切换后的 worker 重建、入队失败同步兜底，以及 `health/metrics` 可观测字段与自动化测试覆盖（`tests/test_api_alert_deliveries_persistence.py`）。
- [x] 已完成：完善 PostgreSQL 权限模型（只读报表账号、最小权限账号、凭据轮换），并固化为部署模板（`ops/postgres/postgres_permission_model_template.sql` + `docs/postgres_permission_model.md` + `tests/test_postgres_permission_model_assets.py`）。
- [x] 已完成：建立跨库一致性巡检（SQLite/PostgreSQL 同口径报表对账）与自动化差异告警（`tools/reconcile_db_report_summary.py`，支持 webhook 差异告警；`tests/test_reconcile_db_report_summary_tool.py` 覆盖无差异/有差异/allow-diff 场景）。

### 13.1 基于前后端现有功能的数据库化增强

- [x] 已完成：用户偏好入库（`user_preferences`）  
  已覆盖前端核心本地持久化项：`theme/language/selectedLiveStrategyId`；并已接入 `logsFilters/backtestsFilters/liveFilters` 结构化字段与 API，支持日志页、回测页与实时页筛选/刷新偏好跨会话恢复（其中 `logsFilters` 已覆盖日志多视图分页大小；`backtestsFilters` 已覆盖回测列表筛选、分页大小、创建表单默认参数与编辑时自动保存）。
- [x] 已完成：会话与登录限流持久化（`auth_sessions` + `auth_login_attempts`）  
  将当前进程内登录限流状态升级为数据库持久化，支持多实例共享风控窗口、会话撤销与安全审计追溯。
- [x] 已完成：运行日志结构化入库（`runtime_logs`）  
  已对接 `/api/logs` 数据源，支持按策略/级别/关键字/时间检索；并在 `tools/sqlite_retention_cleanup.py` 增加 `runtime_logs` TTL 清理能力，避免仅依赖文件 tail。  
  同时已接入 Live 载荷 DB 优先读取：`/api/positions`、`/api/orders`、`/api/fills` 在 DB 可用时优先消费 `runtime_logs`，无数据再回退进程日志 tail（`api_server.py` + `tests/test_api_live_payload_db_runtime_logs.py`）。
- [x] 已完成：策略诊断快照时序化（`strategy_diagnostics_snapshots`）  
  已在 `/api/strategy/diagnostics` 读取链路中落地快照并新增 `/api/strategy/diagnostics/history` 查询，覆盖连接状态、异常计数、过滤原因等字段，支持趋势分析与回放。
- [x] 已完成：回测明细列式化入库（`backtest_trades` + `backtest_equity_points`）  
  已落地 schema v11（SQLite/PostgreSQL 同步迁移）、`get_backtest` DB 优先读取 + CSV 回填、进程退出自动落库；支持跨重启保留回测成交与权益明细查询能力。
- [x] 已完成：告警投递记录表（`alert_deliveries`）  
  已落地 schema v12（SQLite/PostgreSQL 同步迁移），DB 告警 webhook 发送结果/重试次数/错误信息可持久化；新增 `/api/alerts/deliveries` 查询与报表聚合字段，支持失败补偿筛查。
- [x] 已完成：WebSocket 连接质量事件入库（`ws_connection_events`）  
  已落地 schema v13（SQLite/PostgreSQL 同步迁移），记录连接/断开/发送异常/鉴权拒绝等事件；新增 `/api/ws/connection-events` 查询与报表聚合字段，支持连接稳定性排查与 SLA 统计。

### 13.2 账号数据数据库化优化（auth/account）

- [x] 已完成：账号主数据表（`users`）  
  统一管理 `username/status/display_name/created_at/last_login_at`，替代纯配置文件用户源，支持用户生命周期管理。
- [x] 已完成：凭据安全表（`user_credentials`）  
  仅存储密码哈希（当前为 PBKDF2-SHA256）与算法版本、密码更新时间，禁止明文凭据落盘。
- [x] 已完成：会话持久化表（`auth_sessions`）  
  将当前签名 Cookie 会话扩展为可撤销会话（`session_id/user_id/issued_at/expires_at/revoked_at/ip/ua`），支持强制下线与并发会话治理。
- [x] 已完成：登录尝试与封禁表（`auth_login_attempts` + `auth_lockouts`）  
  将登录失败计数、窗口限流、临时封禁持久化，支撑多实例共享限流状态与风控规则统一。
- [x] 已完成：RBAC 权限模型（`roles/permissions/user_roles/role_permissions`）  
  已落地 schema v15（SQLite/PostgreSQL 同步迁移），内置角色与权限矩阵并支持用户角色绑定；新增 `GET /api/auth/roles`、`GET /api/auth/permissions`、`GET /api/auth/user-roles`、`PUT /api/auth/user-roles`，并将 `audit.read.all/risk.write/strategy.execute/ops.admin.db/auth.token.manage/security.read.all` 接入关键接口鉴权。
- [x] 已完成：账号安全事件表（`account_security_events`）  
  已落地 schema v14（SQLite/PostgreSQL 同步迁移），登录成功/失败/限流拦截/游客进入/登出及 token 创建/吊销均入库；新增 `GET /api/auth/security-events`（管理员全量、普通用户仅本人）。
  记录登录成功/失败、密码修改、会话撤销、权限变更等关键事件，支持安全审计与异常追踪。
- [x] 已完成：API 访问凭据表（`api_tokens`）  
  已落地 schema v14（SQLite/PostgreSQL 同步迁移），支持 token 哈希存储、作用域、过期时间、最后使用时间、吊销状态；新增 `POST /api/auth/tokens`、`GET /api/auth/tokens`、`POST /api/auth/tokens/{token_id}/revoke`，并接入 `X-API-Key/Bearer` 鉴权。
  对 API token 建立哈希存储、作用域、过期时间、最后使用时间与吊销状态，支持轮换与最小权限控制。
- [x] 已完成：业务数据 `owner` 外键化（`owner -> users.id`）  
  已落地 schema v16（SQLite/PostgreSQL 同步迁移）：为核心业务表新增 `owner_user_id` 并回填历史数据，建立到 `users.id` 的外键关联与索引；新增触发器在写入/更新时自动归一化 `owner` 并维护 `owner_user_id`，降低脏数据与孤儿数据风险。

### 13.3 用户提出需求的落地方案（2026-03-05）

- [x] 已完成：策略“入库即可执行”自动化流水线（`strategy_compiler_jobs` + `strategy_scripts`）  
  目标：前端新建策略后，后端基于数据库中的策略参数自动生成策略脚本（或模板化配置脚本）并启动运行。  
  建议链路：`/api/strategies` 写库 -> 触发编译任务 -> 生成脚本文件与版本记录 -> `ManagedProcess` 按产物启动。  
  验收口径：策略创建后无需手工改 YAML，即可自动完成“入库 -> 产物 -> 运行”闭环，并可追踪每次脚本生成版本与运行日志。  
  补充：已支持服务重启后的编译任务恢复（`strategy_compiler_jobs` 中 `pending/running` 任务自动回收并重新入队，避免重启后卡单）。
- [x] 已完成：多账号数据强隔离（`guest/admin/lsm_test`）  
  当前状态：`admin/lsm_test` 已通过 `owner` + `strategy_key` 作用域隔离大部分业务数据；游客模式当前主要是前端只读态。  
  目标状态：游客账号也成为后端真实账号（如 `guest`），其策略/回测/风控/审计数据与 `admin/lsm_test` 在数据库中完全隔离。  
  建议改造：启用账号表与会话持久化、为游客发放受限会话、所有业务表 `owner` 外键化并强制按会话用户过滤。  
  验收口径：`guest/admin/lsm_test` 三账号分别创建策略后，互相在 `/api/strategies`、`/api/backtests`、`/api/risk*`、`/api/audit/logs` 均不可见。
- [x] 已完成：隔离与自动化联调测试补齐  
  新增自动化测试覆盖上述两个目标：  
  1) 策略自动生成与运行链路（API + DB + 进程）。  
  2) `guest/admin/lsm_test` 跨账号访问拒绝与 owner 精确过滤。  
  3) PostgreSQL 与 SQLite 双后端一致行为验证。

### 13.4 13.3 的工程化拆解（可直接开发）

#### 13.4.1 数据表草案

- [x] 已完成：新增 `strategy_compiler_jobs`  
  建议字段：`id/strategy_key/owner/status/error_message/created_at/updated_at/started_at/finished_at`。  
  说明：记录“策略入库后自动生成脚本”任务状态（`pending/running/success/failed`）。
- [x] 已完成：新增 `strategy_scripts`  
  建议字段：`id/strategy_key/owner/version/script_type/script_path/script_hash/source_config_json/created_at`。  
  说明：记录每次生成的策略脚本产物与版本；可追溯、可回滚。
- [x] 已完成：新增 `users` / `user_credentials` / `auth_sessions` / `auth_login_attempts` / `auth_lockouts`。  
  说明：支撑 `guest/admin/lsm_test` 后端账号化与数据库级会话/限流治理。

#### 13.4.2 API 与流程改造

- [x] 已完成：`POST /api/strategies` 增加“自动编译任务入队”。  
  建议：策略写库成功后插入 `strategy_compiler_jobs`（`pending`），由后台 worker 异步执行脚本生成。
- [x] 已完成：新增 `POST /api/strategies/{id}/compile`（手工重编译）与 `GET /api/strategies/{id}/scripts`（脚本版本查询）。
- [x] 已完成：策略启动逻辑优先读取 `strategy_scripts` 最新成功版本。  
  回退策略：若无可用脚本，保持当前 runtime config 启动路径并返回明确错误/提示。
- [x] 已完成：游客模式改为后端真实账号（`guest`）会话。  
  要求：游客不再只是前端态；后端所有 owner 过滤逻辑均基于会话用户执行。

#### 13.4.3 迁移与兼容策略

- [x] 已完成：迁移版本递增（SQLite/PostgreSQL 同步）。  
  建议新增 schema version（如 v7+），两套 store 保持等价迁移能力。
- [x] 已完成：历史数据补写脚本。  
  新增 `tools/backfill_owner_columns.py`（支持 SQLite/PostgreSQL、dry-run、owner 规范化、`users` 回填），可将现有 `owner` 为空或旧格式数据修复为明确 owner，并支持 `admin` 映射规则。
- [x] 已完成：双写灰度期。  
  在自动编译功能开启初期保留“旧启动路径”兜底，待稳定后切主新链路。

#### 13.4.4 测试矩阵（必须新增）

- [x] 已完成：`tests/test_api_strategy_auto_compile.py`  
  覆盖：创建策略后生成编译任务、任务成功后可启动、失败可观测。
- [x] 已完成：`tests/test_api_multi_account_isolation.py`  
  覆盖：`guest/admin/lsm_test` 三账号在 `strategies/backtests/risk/audit` 的全链路隔离。
- [x] 已完成：`tests/test_auth_session_persistence.py`  
  覆盖：会话撤销、并发会话、登录失败限流、锁定恢复。
- [x] 已完成：`tests/test_postgres_sqlite_feature_parity_for_13_3.py`  
  覆盖：SQLite/PostgreSQL 在 13.3 新功能上的行为一致性。

#### 13.4.5 验收门槛（Definition of Done）

- [x] 已完成：用户新建策略后，系统可在无手工 YAML 修改的情况下自动完成“入库 -> 编译 -> 启动”。
- [x] 已完成：`guest/admin/lsm_test` 在数据库层与 API 层均完成强隔离（含审计日志读取隔离）。
- [x] 已完成：CI 中新增对应测试并稳定通过（含 PostgreSQL 集成任务）。
