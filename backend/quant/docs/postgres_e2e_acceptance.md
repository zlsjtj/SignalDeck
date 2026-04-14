# PostgreSQL 联调验收基线

## 1. 目标

验证后端在真实 PostgreSQL 场景下具备以下能力：

1. 可连接真实 PostgreSQL 并自动完成迁移（`schema_version >= 13`）。
2. 通过 API 完成策略/风控/审计读写并落盘 PostgreSQL。
3. 运行时热切换链路可用：`postgres -> sqlite -> postgres` 且保留内存状态。

---

## 2. 前置条件

```bash
cd backend/quant
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-postgres.txt
```

---

## 3. 执行方式

### 3.1 使用现有 PostgreSQL 实例（推荐）

```bash
cd backend/quant
export QUANT_E2E_POSTGRES_DSN='postgresql://user:password@127.0.0.1:5432/quant_e2e'
python tools/e2e_backend_postgres.py
```

### 3.2 一键临时拉起 Docker PostgreSQL

> 需要本机可用 `docker`。

```bash
cd backend/quant
python tools/e2e_backend_postgres.py --use-docker-postgres
```

---

## 4. 通过标准

脚本输出 JSON 中同时满足以下条件即通过：

1. `ok = true`
2. `postgres.schema_version_max >= 13`
3. `backend.steps` 包含关键步骤：
   - `postgres.ready`
   - `health.postgres.ok`
   - `postgres.migration.ok`
   - `strategy.create`
   - `risk.update`
   - `risk.events.read`
   - `audit.read`
   - `db.reload.sqlite`
   - `health.sqlite.ok`
   - `strategy.read.after.sqlite_reload`
   - `db.reload.postgres`
   - `health.postgres.reloaded`
   - `strategy.read.after.postgres_reload`
   - `reports.summary.read`
   - `strategy.delete`

---

## 5. 自动化测试

```bash
cd backend/quant
.venv/bin/python -m unittest tests.test_e2e_backend_postgres_tool
```

说明：

1. `test_requires_postgres_dsn_or_docker_mode` 默认必跑（校验脚本可用性与失败路径）。
2. `test_real_postgres_smoke_and_runtime_reload` 在设置 `QUANT_E2E_POSTGRES_DSN` 时自动执行真实 PostgreSQL 联调。
3. `test_postgres_store_report_summary` 覆盖 PostgreSQL 报表在 `owner=None` 场景的回归。
4. `test_postgres_sqlite_feature_parity_for_13_3` 覆盖 13.3 新能力在 PostgreSQL/SQLite 的行为一致性。

CI 已接入 PostgreSQL 集成任务：`backend/quant/.github/workflows/quant_checks.yml`（job: `postgres-integration`）。

---

## 6. 最近一次实跑记录

- 执行日期（UTC）：2026-03-04
- 结果：通过（历史记录，`ok=true`，`schema_version_max=6`，`postgres -> sqlite -> postgres` 切换完成）
- 报告文件：`backend/quant/docs/postgres_e2e_report_2026-03-04.json`
