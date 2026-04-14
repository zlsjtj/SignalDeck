# Quant API Backend（接口速查）

> 完整文档请优先查看：`backend/quant/README.md`

## 安装与启动

```bash
cd backend/quant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

PostgreSQL 为必选后端：

```bash
pip install -r requirements-postgres.txt
```

- Health: `http://localhost:8000/api/health`
- OpenAPI: `http://localhost:8000/docs`
- Health 响应包含数据库状态：`db=ok|degraded|error|disabled`。
- 初始化异常时可见 `db_error`；运行期持久化失败时可见 `db_runtime_failures` 与 `db_runtime_failure_detail`。
- 另包含 DB 观测字段：`db_last_write_*`、`db_write_ops_*`、`db_read_ops_*`、`db_lock_*`、`db_last_slow_*`。
- 另包含容量字段（`db_storage.*`）。
- 另包含 `db_path` 与 `db_runtime_reload_supported`（DB 运行时重载能力标记）。

## 联调要点（与前端一致）

前端应配置：

```env
VITE_API_BASE_URL=http://localhost:8000/api
VITE_WS_URL=ws://localhost:8000/ws
VITE_USE_MOCK=false
VITE_API_PROFILE=quant-api-server
VITE_MARKET_CONFIG_PATH=config_market.yaml
VITE_MARKET_POLL_MS=1000
```

关键点：`VITE_API_BASE_URL` 必须包含 `/api`。

## 后端环境变量

- `DEFAULT_STRATEGY_CONFIG_PATH`
- `API_AUTH_REQUIRED`（默认 `false`）
- `API_AUTH_TOKEN`（当鉴权开启时必填）
- `API_AUTH_TOKEN_FILE`（从文件读取 token）
- `DASHBOARD_AUTH_FILE`（从文件读取 username:password）
- `DASHBOARD_LOGIN_USERNAME` / `DASHBOARD_LOGIN_PASSWORD`
- `DASHBOARD_GUEST_USERNAME`（游客后端真实账号名，默认 `guest`）
- `API_SESSION_SECRET` / `API_SESSION_SECRET_FILE`
- `API_SESSION_COOKIE_NAME` / `API_SESSION_TTL_SECONDS`
- `API_SESSION_COOKIE_SECURE` / `API_SESSION_COOKIE_SAMESITE`
- `API_LOGIN_RATE_LIMIT_WINDOW_SECONDS`
- `API_LOGIN_RATE_LIMIT_MAX_ATTEMPTS`
- `API_LOGIN_LOCKOUT_SECONDS`
- `CORS_ORIGINS`（默认 `*`）
- `API_DB_ENABLED`（默认 `true`）
- `API_DB_BACKEND`（当前强制 `postgres`）
- `API_DB_PATH`（兼容字段，PostgreSQL 模式下建议 `/dev/null`）
- `API_DB_POSTGRES_DSN`（`postgres` 后端连接串）
- `API_DB_POSTGRES_POOL_ENABLED`（默认 `true`，PostgreSQL 连接池开关）
- `API_DB_POSTGRES_POOL_MIN_SIZE`（默认 `1`，PostgreSQL 连接池最小连接数）
- `API_DB_POSTGRES_POOL_MAX_SIZE`（默认 `10`，PostgreSQL 连接池最大连接数）
- `API_DB_POSTGRES_POOL_TIMEOUT_SECONDS`（默认 `5`，连接池取连接超时秒）
- `API_DB_ALERT_WEBHOOK_URL`（可选，DB 失败告警 webhook）
- `API_DB_ALERT_THRESHOLD`（默认 `1`）
- `API_DB_ALERT_COOLDOWN_SECONDS`（默认 `300`）
- `API_DB_ALERT_TIMEOUT_SECONDS`（默认 `3`）
- `API_DB_ALERT_MAX_RETRIES`（默认 `0`，告警 webhook 重试次数）
- `API_DB_ALERT_RETRY_BACKOFF_MS`（默认 `200`，告警 webhook 重试间隔毫秒）
- `API_DB_ALERT_OUTBOX_ENABLED`（默认 `true`，告警异步 outbox 投递开关）
- `API_DB_ALERT_OUTBOX_POLL_SECONDS`（默认 `1`，outbox worker 轮询间隔秒）
- `API_DB_ALERT_OUTBOX_BATCH_SIZE`（默认 `20`，outbox worker 单批处理条数）
- `API_DB_HEALTH_STATS_TTL_SECONDS`（默认 `30`）
- `API_DB_SLOW_OP_THRESHOLD_MS`（默认 `200`，慢读写判定阈值）
- `API_BACKTEST_CREATE_DEDUP_TTL_SECONDS`（默认 `30`，`/api/backtests` 重复请求幂等窗口）
- `API_BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS`（默认 `10`，回测进度最小持久化间隔）
- `API_BACKTEST_PROGRESS_MIN_DELTA_PCT`（默认 `1`，回测进度最小持久化增量）
- `API_STRATEGY_COMPILE_WAIT_SECONDS`（默认 `0.25`，编译 worker 空闲轮询间隔）

示例：

```bash
export API_AUTH_REQUIRED=true
export API_AUTH_TOKEN=your-token
export CORS_ORIGINS=http://localhost:5173
export API_DB_ENABLED=true
export API_DB_BACKEND=postgres
export API_DB_PATH=/dev/null
export API_DB_POSTGRES_DSN='postgresql://quant_user:strong-password@127.0.0.1:5432/quant_db'
export DASHBOARD_LOGIN_USERNAME=admin
export DASHBOARD_LOGIN_PASSWORD='your-strong-password'
export API_SESSION_SECRET='your-long-random-secret'
```

模板文件：`backend/quant/.env.example`

## API 路由

### 系统
- `GET /api/health`
- `GET /api/metrics`（Prometheus 文本格式）
- `GET /api/config`
- `GET /api/admin/db/config`（需 `ops.admin.db`）
- `POST /api/admin/db/reload`（需 `ops.admin.db`，支持 `enabled/dbPath/backend/postgresDsn/preserveState`）

### 鉴权
- `POST /api/auth/login`
- `POST /api/auth/guest`（创建游客后端会话）
- `GET /api/auth/status`
- `POST /api/auth/logout`
- `GET /api/auth/security-events`
  - 支持查询参数：`owner`（需 `security.read.all` 才可跨账号）、`event_type`、`start`、`end`、`cursor`、`limit`
- `POST /api/auth/tokens`（需 `auth.token.manage`）
  - 请求体：`owner/tokenName/scopes/expiresAt`
  - 返回明文 token（仅创建时返回）与元数据（owner/scopes/expiresAt/revokedAt/lastUsedAt）
- `GET /api/auth/tokens`
  - 支持查询参数：`owner`（需 `auth.token.manage` 才可跨账号）、`include_revoked`、`limit`
- `POST /api/auth/tokens/{token_id}/revoke`（需 `auth.token.manage`）
- `GET /api/auth/roles`（需 `rbac.manage`）
- `GET /api/auth/permissions`（需 `rbac.manage`）
- `GET /api/auth/user-roles`（查询他人需 `rbac.manage`，查询本人无需）
- `PUT /api/auth/user-roles`（需 `rbac.manage`）
- `GET /api/user/preferences`（登录用户读取偏好）
- `PUT /api/user/preferences`（登录用户更新偏好）

### 行情
- `GET /api/market/ticks`
- `GET /api/market/klines`

### 策略
- `GET /api/strategies`
- `POST /api/strategies`
  - 自动入队策略编译任务（`strategy_compiler_jobs`）
- `GET /api/strategies/{id}`
- `PUT /api/strategies/{id}`
- `DELETE /api/strategies/{id}`
- `POST /api/strategies/{id}/compile`（手工重编译）
- `GET /api/strategies/{id}/scripts`（查看脚本版本）
- `POST /api/strategies/{id}/start`（需 `strategy.execute`）
- `POST /api/strategies/{id}/stop`（需 `strategy.execute`）
- `POST /api/strategy/start`（需 `strategy.execute`）
- `POST /api/strategy/stop`（需 `strategy.execute`）
- `GET /api/strategy/status`
- `GET /api/strategy/logs`
- `GET /api/strategy/diagnostics`
  - 支持查询参数：`strategy_id`，可选 `path`（手动指定诊断文件）
  - 每次读取会落地一条 `strategy_diagnostics_snapshots` 记录
- `GET /api/strategy/diagnostics/history`
  - 支持查询参数：`strategy_id`、`owner`（仅管理员）、`start`、`end`、`cursor`、`include_snapshot`、`limit`

### 回测
- `GET /api/backtests`
- `POST /api/backtests`（重复同参请求在短窗口内返回同一 run）
  - 运行中回测会按进度日志周期性持久化 `progress`（不依赖查询触发）
- `GET /api/backtests/{run_id}`
  - 优先读取 `backtest_trades/backtest_equity_points`，缺失时自动回退 CSV 并回填数据库
- `GET /api/backtests/{run_id}/logs`
- `POST /api/backtest/start`（同参数且任务运行中时幂等返回）
- `POST /api/backtest/stop`
- `GET /api/backtest/status`
- `GET /api/backtest/artifacts`
- `GET /api/backtest/file/{artifact_name}`

### 账户与风控
- `GET /api/paper/equity`
- `GET /api/portfolio`
  - `maxDrawdown` 基于 `paper_equity.csv` 真实权益曲线计算；策略未运行时返回 `stale=true`
- `GET /api/positions`
- `GET /api/orders`
- `GET /api/fills`
- `GET /api/logs`
  - 支持查询参数：`type`、`level`、`q`、`strategy_id`、`start`、`end`、`cursor`、`limit`
  - 当 DB 可用时优先从 `runtime_logs` 历史表读取（支持跨进程重启后检索）
- `GET /api/audit/logs`
  - 支持查询参数：`limit`、`action`、`entity`、`owner`、`start`、`end`、`cursor`
  - 仅 `audit.read.all` 可跨账号查询 `owner`
- `GET /api/alerts/deliveries`
  - 支持查询参数：`owner`、`event`、`status`、`start`、`end`、`cursor`、`limit`
  - 非管理员仅可读取自身告警投递记录
- `GET /api/ws/connection-events`
  - 支持查询参数：`owner`、`event_type`、`strategy_id`、`start`、`end`、`cursor`、`limit`
  - 非管理员仅可读取自身连接事件记录
- `GET /api/audit/verify`
  - 支持查询参数：`owner`、`start_id`、`end_id`、`limit`
  - 仅 `audit.read.all` 可跨账号校验
- `GET /api/reports/db/summary`
  - 支持查询参数：`owner`、`start`、`end`、`limit_top`
- `GET /api/risk`
- `PUT /api/risk`
  - 需 `risk.write`
- `GET /api/risk/history`
  - 支持查询参数：`strategy_id`、`owner`、`cursor`、`limit`
- `GET /api/risk/events`
  - 支持查询参数：`strategy_id`、`event_type`、`owner`、`start`、`end`、`cursor`、`limit`

### WebSocket
- `WS /ws?config_path=<yaml路径>&strategy_id=<策略ID>&refresh_ms=1000`

## 运维补充

- SQLite 自动维护文档：`backend/quant/docs/sqlite_maintenance_ops.md`
- Prometheus 接入文档：`backend/quant/docs/prometheus_db_alerting.md`
- cron 安装脚本：`backend/quant/tools/install_sqlite_maintenance_cron.sh`
- SQLite 备份恢复文档：`backend/quant/docs/sqlite_backup_restore.md`
- SQLite 备份脚本：`backend/quant/tools/sqlite_backup.py`
- SQLite 备份 cron 安装脚本：`backend/quant/tools/install_sqlite_backup_cron.sh`
- SQLite 冷备脚本：`backend/quant/tools/sqlite_cold_backup.py`
- SQLite 冷备 cron 安装脚本：`backend/quant/tools/install_sqlite_cold_backup_cron.sh`
- SQLite 恢复演练脚本：`backend/quant/tools/sqlite_restore_drill.py`
- SQLite TTL 清理文档：`backend/quant/docs/sqlite_retention_ops.md`
- SQLite TTL 清理脚本：`backend/quant/tools/sqlite_retention_cleanup.py`
- owner 历史补写脚本：`backend/quant/tools/backfill_owner_columns.py`
- SQLite TTL 清理 cron 安装脚本：`backend/quant/tools/install_sqlite_retention_cleanup_cron.sh`
- SQLite 并发压测脚本：`backend/quant/tools/sqlite_concurrency_stress.py`
- SQLite 故障注入脚本：`backend/quant/tools/sqlite_fault_injection.py`
- SQLite 审计链校验脚本：`backend/quant/tools/sqlite_verify_audit_chain.py`
- 并发压测报告：`backend/quant/docs/sqlite_concurrency_stress_report.md`
- 故障注入报告：`backend/quant/docs/sqlite_fault_injection_report.md`
- 时序存储评估：`backend/quant/docs/timeseries_storage_evaluation.md`

## 请求示例

### 启动策略

```http
POST /api/strategy/start
Content-Type: application/json

{
  "config_path": "config.yaml"
}
```

### 启动回测

```http
POST /api/backtest/start
Content-Type: application/json

{
  "start": "2025-01-01",
  "end": "2025-12-31",
  "config_path": "config.yaml"
}
```

### 下载回测文件

```http
GET /api/backtest/file/metrics_txt
```

## 快速自检

```bash
curl http://localhost:8000/api/health
curl http://localhost:8000/api/strategies
curl "http://localhost:8000/api/market/ticks?config_path=config.yaml"
```

前后端+SQLite 一键联调（默认包含前端 typecheck）：

```bash
cd backend/quant
python tools/e2e_frontend_backend_sqlite.py
```

后端+真实 PostgreSQL 一键联调（连接/迁移/读写/运行时切换）：

```bash
cd backend/quant
export QUANT_E2E_POSTGRES_DSN='postgresql://user:password@127.0.0.1:5432/quant_e2e'
python tools/e2e_backend_postgres.py
```

若本机安装了 Docker，也可临时拉起 PostgreSQL：

```bash
cd backend/quant
python tools/e2e_backend_postgres.py --use-docker-postgres
```

验收基线说明见：

- `backend/quant/docs/postgres_e2e_acceptance.md`
- `backend/quant/docs/postgres_permission_model.md`
- `backend/quant/ops/postgres/postgres_permission_model_template.sql`
- `backend/quant/docs/postgres_backup_pitr_ops.md`
- `backend/quant/tools/postgres_backup_pitr_drill.py`
- `backend/quant/docs/postgres_performance_baseline.md`
- `backend/quant/tools/postgres_performance_baseline.py`
