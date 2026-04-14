# 量化交易后端（Quant API Backend）

本项目是基于 `FastAPI + ccxt + pandas` 的量化后端服务，提供：

- 策略进程管理（启动/停止/状态/日志）
- 回测任务管理（启动/停止/详情/日志/产物下载）
- 组合与风控数据接口
- 行情接口（REST）与实时推送（WebSocket）
- 纸交易（paper）权益/持仓/成交数据输出

对应前端目录：`frontweb/www.zlsjtj.tech`

## 目录结构

```text
backend/quant/
├─ api_server.py                 # FastAPI 服务入口
├─ main.py                       # 策略主循环入口
├─ config.yaml                   # 默认策略配置
├─ config_*.yaml                 # 其他策略配置
├─ requirements.txt
├─ statarb/
│  ├─ backtest.py                # 回测 CLI（python -m statarb.backtest）
│  └─ ...
├─ logs/                         # 运行日志、回测产物、paper 曲线
└─ README.md
```

## 环境要求

- Python `3.11+`（建议）
- 可访问交易所行情接口（如 Binance USD-M）

## 安装

```bash
cd backend/quant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

PostgreSQL 为必选后端，请安装：

```bash
pip install -r requirements-postgres.txt
```

## 启动 API 服务

```bash
cd backend/quant
source .venv/bin/activate
uvicorn api_server:app --host 0.0.0.0 --port 8000 --reload
```

启动后可访问：

- 健康检查：`http://localhost:8000/api/health`
- OpenAPI：`http://localhost:8000/docs`
- Health 返回 `db` 字段（`ok/degraded/error/disabled`）：
  - `error`：数据库初始化失败；
  - `degraded`：数据库初始化成功，但运行期出现持久化失败。
- 初始化失败时返回 `db_error`；运行期失败时返回 `db_runtime_failures` 与 `db_runtime_failure_detail`。
- 运行时会返回 `db_path` 与 `db_runtime_reload_supported`（当前恒为 `true`）。
- Health 还会返回：
  - `db_last_write_kind/db_last_write_ms/db_last_write_at/db_max_write_ms`
  - `db_write_ops_total/db_write_ops_slow_total/db_read_ops_total/db_read_ops_slow_total`
  - `db_lock_contention_total/db_lock_wait_ms_total/db_last_slow_*`
  - `db_storage`（`db_size_bytes/free_bytes/fragmentation_pct`）

## 与前端联调

前端建议环境变量（与当前前端 README 对齐）：

```env
VITE_API_BASE_URL=http://localhost:8000/api
VITE_WS_URL=ws://localhost:8000/ws
VITE_USE_MOCK=false
VITE_API_PROFILE=quant-api-server
VITE_MARKET_CONFIG_PATH=config_market.yaml
VITE_MARKET_POLL_MS=1000
```

注意：`VITE_API_BASE_URL` 必须包含 `/api` 前缀。

## 后端环境变量

`api_server.py` 当前支持：

- `DEFAULT_STRATEGY_CONFIG_PATH`
  - 默认策略配置文件（默认：`config_2025_bch_bnb_btc_equal_combo_baseline_v2_invvol_best.yaml`）
- `API_AUTH_REQUIRED`
  - 是否开启 API 鉴权（默认 `false`）
- `API_AUTH_TOKEN`
  - 鉴权 token（当 `API_AUTH_REQUIRED=true` 时必填）
- `API_AUTH_TOKEN_FILE`
  - 从文件读取鉴权 token（优先于明文变量）
- `DASHBOARD_AUTH_FILE`
  - 从文件读取仪表盘账号密码（`username:password` 或多行）
- `DASHBOARD_LOGIN_USERNAME` / `DASHBOARD_LOGIN_PASSWORD`
  - 直接通过环境变量配置登录账号密码
- `DASHBOARD_GUEST_USERNAME`
  - 游客后端真实账号名（默认 `guest`）
- `API_SESSION_SECRET` / `API_SESSION_SECRET_FILE`
  - 会话签名密钥（登录 Cookie 必需）
- `API_SESSION_COOKIE_NAME` / `API_SESSION_TTL_SECONDS`
  - 会话 Cookie 名称与有效期（默认 43200 秒）
- `API_SESSION_COOKIE_SECURE` / `API_SESSION_COOKIE_SAMESITE`
  - 会话 Cookie 安全策略
- `API_LOGIN_RATE_LIMIT_WINDOW_SECONDS`
- `API_LOGIN_RATE_LIMIT_MAX_ATTEMPTS`
- `API_LOGIN_LOCKOUT_SECONDS`
  - 登录限流与锁定策略
- `CORS_ORIGINS`
  - 允许跨域来源，逗号分隔（默认 `*`）
- `API_DB_ENABLED`
  - 数据库持久化开关（当前强制启用，固定为 `true`）
- `API_DB_BACKEND`
  - 数据库后端类型（当前强制 `postgres`）
- `API_DB_PATH`
  - 兼容字段；PostgreSQL 模式下不使用（建议保持 `/dev/null`）
- `API_DB_POSTGRES_DSN`
  - PostgreSQL 连接串（必填，例如 `postgresql://user:password@host:5432/dbname`）
- `API_DB_POSTGRES_POOL_ENABLED`
  - 是否启用 PostgreSQL 连接池（默认 `true`）
- `API_DB_POSTGRES_POOL_MIN_SIZE`
  - PostgreSQL 连接池最小连接数（默认 `1`）
- `API_DB_POSTGRES_POOL_MAX_SIZE`
  - PostgreSQL 连接池最大连接数（默认 `10`，并作为连接上限治理基线）
- `API_DB_POSTGRES_POOL_TIMEOUT_SECONDS`
  - PostgreSQL 连接池取连接超时秒数（默认 `5`）
- `API_DB_ALERT_WEBHOOK_URL`
  - 运行期 DB 持久化失败告警 webhook（可选）
- `API_DB_ALERT_THRESHOLD`
  - 触发告警的失败计数阈值（默认 `1`）
- `API_DB_ALERT_COOLDOWN_SECONDS`
  - 告警冷却时间（默认 `300` 秒）
- `API_DB_ALERT_TIMEOUT_SECONDS`
  - 告警 webhook 超时秒数（默认 `3`）
- `API_DB_ALERT_MAX_RETRIES`
  - 告警 webhook 失败重试次数（默认 `0`）
- `API_DB_ALERT_RETRY_BACKOFF_MS`
  - 告警 webhook 重试间隔毫秒（默认 `200`）
- `API_DB_ALERT_OUTBOX_ENABLED`
  - 是否启用 outbox 异步投递（默认 `true`，建议生产开启）
- `API_DB_ALERT_OUTBOX_POLL_SECONDS`
  - outbox worker 轮询间隔秒数（默认 `1`）
- `API_DB_ALERT_OUTBOX_BATCH_SIZE`
  - outbox worker 单批处理条数（默认 `20`）
- `API_DB_SLOW_OP_THRESHOLD_MS`
  - DB 慢操作阈值（毫秒，默认 `200`；用于慢读写计数）
- `API_BACKTEST_CREATE_DEDUP_TTL_SECONDS`
  - `/api/backtests` 重复请求幂等窗口（秒，默认 `30`）
- `API_BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS`
  - 回测运行中进度持久化最小时间间隔（秒，默认 `10`）
- `API_BACKTEST_PROGRESS_MIN_DELTA_PCT`
  - 回测进度持久化最小百分比增量（默认 `1`）
- `API_STRATEGY_COMPILE_WAIT_SECONDS`
  - 策略自动编译 worker 空闲轮询间隔（秒，默认 `0.25`）

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

可直接参考模板：`backend/quant/.env.example`

## 配置文件说明

默认策略配置在 `config.yaml`，关键字段包括：

- `exchange`：交易所（如 `binanceusdm`）
- `paper`：是否纸交易
- `symbols`：交易对列表
- `timeframe` / `lookback_hours` / `rebalance_every_minutes`
- `strategy`：打分和过滤参数
- `portfolio`：杠杆、权重、最小下单额、交易费用等
- `risk`：回撤/日亏/冷静期等风控参数
- `execution`：下单方式与偏移
- `keys`：实盘 API Key/Secret（纸交易可留空）

安全建议：

- 不要在仓库中明文保存真实 `apiKey/secret`。
- 生产环境请使用独立配置文件，并通过访问控制限制读取权限。

## 常用运行方式

1) 仅跑策略主循环（不经过 API）：

```bash
cd backend/quant
source .venv/bin/activate
python main.py --config config.yaml
```

2) 直接命令行跑回测：

```bash
cd backend/quant
source .venv/bin/activate
python -m statarb.backtest \
  --start 2025-01-01 \
  --end 2025-12-31 \
  --config config.yaml \
  --out logs/backtest_equity.csv \
  --trades logs/backtest_trades.csv \
  --metrics logs/backtest_metrics.txt \
  --plot logs/backtest_equity.png
```

## API 概览

主要接口（路径前缀 `/api`）：

- 系统
  - `GET /health`
  - `GET /metrics`（Prometheus 文本格式）
  - `GET /config`
  - `GET /admin/db/config`（管理员）
  - `POST /admin/db/reload`（管理员，支持运行时切换 backend/path/dsn/启停）
- 鉴权
  - `POST /auth/login`
  - `POST /auth/guest`（游客后端会话）
  - `GET /auth/status`
  - `POST /auth/logout`
  - `GET /user/preferences`
  - `PUT /user/preferences`
- 行情
  - `GET /market/ticks`
  - `GET /market/klines`
- 策略
  - `GET /strategies`
  - `POST /strategies`（自动入队编译任务）
  - `GET /strategies/{id}`
  - `PUT /strategies/{id}`
  - `DELETE /strategies/{id}`
  - `POST /strategies/{id}/compile`
  - `GET /strategies/{id}/scripts`
  - `POST /strategies/{id}/start`
  - `POST /strategies/{id}/stop`
  - `POST /strategy/start`
  - `POST /strategy/stop`
  - `GET /strategy/status`
  - `GET /strategy/logs`
  - `GET /strategy/diagnostics`
  - `GET /strategy/diagnostics/history`（支持时序回放，`include_snapshot=true` 可返回完整快照）
- 回测
  - `GET /backtests`
  - `POST /backtests`（重复同参请求在短窗口内幂等返回同一 run）
  - 运行中任务会根据进度日志周期性回写 `progress` 到数据库（无需查询触发）
  - `GET /backtests/{run_id}`
    - 优先从 `backtest_trades/backtest_equity_points` 读取；回测 CSV/TXT/PNG 会同步入库，文件缺失时可从数据库回读
  - `GET /backtests/{run_id}/logs`
  - `POST /backtest/start`（同参数且任务在运行时幂等返回）
  - `POST /backtest/stop`
  - `GET /backtest/status`
  - `GET /backtest/artifacts`
  - `GET /backtest/file/{artifact_name}`
- 账户与风控
  - `GET /paper/equity`
  - `GET /portfolio`
    - 返回 `maxDrawdown` 基于真实纸交易权益曲线计算（文件会同步入库，缺失时走数据库回读）；当策略未运行时返回 `stale=true`
  - `GET /positions`
  - `GET /orders`
  - `GET /fills`
  - `GET /logs`（支持 `type/level/q/strategy_id/start/end/cursor/limit`，DB 可用时走 `runtime_logs` 历史检索）
  - `GET /audit/logs`（支持 `limit/action/entity/owner/start/end/cursor`）
  - `GET /alerts/deliveries`（支持 `owner/event/status/start/end/cursor/limit`，非管理员仅可见自身）
  - `GET /ws/connection-events`（支持 `owner/event_type/strategy_id/start/end/cursor/limit`，非管理员仅可见自身）
  - `GET /audit/verify`（审计哈希链校验，支持 `owner/start_id/end_id/limit`）
  - `GET /reports/db/summary`（结构化报表：审计/风控/行情/告警投递/WS 连接事件汇总）
  - `GET /risk`
  - `PUT /risk`
  - `GET /risk/history`（支持 `strategy_id/owner/cursor/limit`）

WebSocket：

- `WS /ws?config_path=<yaml路径>&strategy_id=<策略ID>&refresh_ms=1000`

## SQLite 运维自动化

- 手工维护：`tools/sqlite_maintenance.py`
- 定时任务：`tools/sqlite_maintenance_job.py`
- cron 安装脚本：`tools/install_sqlite_maintenance_cron.sh`
- 运维文档：`docs/sqlite_maintenance_ops.md`
- 在线备份脚本：`tools/sqlite_backup.py`
- 备份 cron 安装脚本：`tools/install_sqlite_backup_cron.sh`
- 冷备脚本（停写窗口）：`tools/sqlite_cold_backup.py`
- 冷备 cron 安装脚本：`tools/install_sqlite_cold_backup_cron.sh`
- 备份恢复文档：`docs/sqlite_backup_restore.md`
- 恢复演练脚本：`tools/sqlite_restore_drill.py`
- TTL 清理脚本：`tools/sqlite_retention_cleanup.py`（含 `runtime_logs`）
- TTL 清理 cron 安装脚本：`tools/install_sqlite_retention_cleanup_cron.sh`
- TTL 清理文档：`docs/sqlite_retention_ops.md`
- owner 历史补写脚本：`tools/backfill_owner_columns.py`
- 并发压测脚本：`tools/sqlite_concurrency_stress.py`
- 故障注入脚本：`tools/sqlite_fault_injection.py`
- 审计链校验脚本：`tools/sqlite_verify_audit_chain.py`
- 并发压测报告（样例）：`docs/sqlite_concurrency_stress_report.md`
- 故障注入报告（样例）：`docs/sqlite_fault_injection_report.md`
- 时序存储评估：`docs/timeseries_storage_evaluation.md`

## Prometheus 监控接入

- 抓取配置示例：`ops/prometheus/scrape_quant_api.yml`
- 告警规则示例：`ops/prometheus/alerts_quant_db.yml`
- 接入文档：`docs/prometheus_db_alerting.md`

## 快速自检

```bash
curl http://localhost:8000/api/health
curl http://localhost:8000/api/strategies
curl "http://localhost:8000/api/market/ticks?config_path=config.yaml"
```

## 前后端+SQLite 联调自动化

提供一键联调脚本（默认包含前端 typecheck）：

```bash
cd backend/quant
python tools/e2e_frontend_backend_sqlite.py
```

常用快速模式（跳过前端检查，仅验证后端+SQLite）：

```bash
cd backend/quant
python tools/e2e_frontend_backend_sqlite.py --skip-frontend-check
```

## 后端+真实 PostgreSQL 联调自动化

使用已有 PostgreSQL（推荐）：

```bash
cd backend/quant
source .venv/bin/activate
export QUANT_E2E_POSTGRES_DSN='postgresql://user:password@127.0.0.1:5432/quant_e2e'
python tools/e2e_backend_postgres.py
```

如需一键临时拉起 Docker PostgreSQL（本机需可用 `docker`）：

```bash
cd backend/quant
source .venv/bin/activate
python tools/e2e_backend_postgres.py --use-docker-postgres
```

PostgreSQL 联调验收基线文档：

- `docs/postgres_e2e_acceptance.md`
- `docs/postgres_permission_model.md`
- `ops/postgres/postgres_permission_model_template.sql`
- `docs/postgres_backup_pitr_ops.md`
- `tools/postgres_backup_pitr_drill.py`
- `docs/postgres_performance_baseline.md`
- `tools/postgres_performance_baseline.py`

CI 已包含 PostgreSQL 集成任务（GitHub Actions `postgres-integration`）：

- `.github/workflows/quant_checks.yml`

## 常见问题

- `401 Unauthorized`
  - 若使用会话登录：先调用 `/api/auth/login`，并确认会话未过期（可用 `/api/auth/status` 检查）。
  - 若使用 token 鉴权：检查 `API_AUTH_REQUIRED=true`，并确认请求头 `X-API-Key` 与 `API_AUTH_TOKEN` 一致。
- 前端请求 404
  - 检查前端 `VITE_API_BASE_URL` 是否是 `http://localhost:8000/api`（含 `/api`）。
- `config file not found`
  - 检查 `config_path` 是否在项目根目录下且为 `.yaml/.yml`。
- 行情接口偶发慢
  - `/api/market/ticks` 已有短间隔缓存与并发保护，建议前端 `refresh_ms` 不低于 200ms。
