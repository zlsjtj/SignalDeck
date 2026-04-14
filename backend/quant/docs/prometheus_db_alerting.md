# Prometheus 监控接入（DB）

> 适用目录：`backend/quant`

## 1. 指标端点

- 地址：`GET /api/metrics`
- 格式：Prometheus text exposition

关键指标：
- `quant_db_status{state="ok|degraded|error|disabled"}`
- `quant_db_runtime_failures_total`
- `quant_db_runtime_failure_total{kind="..."}`
- `quant_db_last_write_duration_ms`
- `quant_db_max_write_duration_ms`
- `quant_db_write_ops_total`
- `quant_db_write_ops_slow_total`
- `quant_db_read_ops_total`
- `quant_db_read_ops_slow_total`
- `quant_db_lock_contention_total`
- `quant_db_lock_wait_ms_total`
- `quant_db_slow_op_threshold_ms`
- `quant_db_last_slow_duration_ms`
- `quant_db_size_bytes`
- `quant_db_free_bytes`
- `quant_db_fragmentation_percent`

## 2. Prometheus 抓取配置

可直接复用示例文件：
- `ops/prometheus/scrape_quant_api.yml`

将内容合并进你的 Prometheus 主配置。

## 3. 告警规则

可直接复用规则文件：
- `ops/prometheus/alerts_quant_db.yml`

包含告警：
- `QuantDbUnavailable`
- `QuantDbDegraded`
- `QuantDbRuntimeFailuresIncreasing`
- `QuantDbAlertingDisabled`
- `QuantDbFragmentationHigh`
- `QuantDbFreeBytesHigh`
- `QuantDbWriteLatencyHigh`
- `QuantDbSlowOpsIncreasing`
- `QuantDbLockContentionIncreasing`

## 4. 最小自检

```bash
curl -s http://127.0.0.1:8000/api/metrics | rg '^quant_db_'
```

确认 Prometheus 侧：
- Targets 页面 `quant_api` 为 `UP`
- Rules 页面可见 `quant_db_alerts` 规则组
