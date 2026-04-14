# PostgreSQL 性能基线（写入 TPS / 分页 P95 / 报表 P95）

## 1. 目标

建立可重复执行的 PostgreSQL 性能基线，覆盖：

- 写入吞吐：`runtime_logs` 写入 `TPS`
- 分页查询：`audit_logs` 分页查询 `P95` 延迟
- 报表聚合：`build_db_report_summary` `P95` 延迟

脚本：

- `tools/postgres_performance_baseline.py`

## 2. 手工执行

```bash
cd backend/quant
export QUANT_E2E_POSTGRES_DSN='postgresql://user:pass@127.0.0.1:5432/quant'
python tools/postgres_performance_baseline.py \
  --seed-rows 2000 \
  --write-ops 1200 \
  --pagination-queries 200 \
  --report-queries 80
```

输出为 JSON，包含：

- `metrics.write.tps`
- `metrics.pagination.latencyMs.p95`
- `metrics.report.latencyMs.p95`
- `criteria` + `checks`

## 3. 通过阈值

可通过参数覆盖阈值：

- `--min-write-tps`
- `--max-pagination-p95-ms`
- `--max-report-p95-ms`

若需要在探索阶段仅产出报告不阻断，可加：

- `--allow-threshold-fail`

## 4. CI 回归

该基线已接入 CI `postgres-integration` 任务，并随工作流定时触发进行周期回归。
