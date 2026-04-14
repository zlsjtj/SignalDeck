# SQLite 清理归档任务（TTL）

> 适用目录：`backend/quant`

## 1. 脚本

- 清理脚本：`tools/sqlite_retention_cleanup.py`

能力：
- 按 TTL 删除 `audit_logs` 历史数据
- 按 TTL 删除 `runtime_logs` 历史数据
- 按 TTL 删除终态 `backtests` 元数据（可配置状态集）
- 支持 `--dry-run` 先看命中量，再正式删除

## 2. 推荐 Cron

每天 03:35 运行（示例）：

```cron
35 3 * * * cd backend/quant && /usr/bin/python3 tools/sqlite_retention_cleanup.py \
  --db-path logs/quant_api.db \
  --audit-ttl-days 180 \
  --runtime-log-ttl-days 30 \
  --backtest-ttl-days 90 \
  --backtest-final-statuses finished,failed,stopped,cancelled \
  >> logs/sqlite_retention_cleanup_cron.log 2>&1
```

也可以使用安装脚本（幂等更新同一条任务）：

```bash
cd backend/quant
./tools/install_sqlite_retention_cleanup_cron.sh
```

仅预览最终 crontab 内容（不安装）：

```bash
cd backend/quant
./tools/install_sqlite_retention_cleanup_cron.sh --dry-run
```

## 3. 手工执行与预演

仅预演（不删数据）：

```bash
cd backend/quant
/usr/bin/python3 tools/sqlite_retention_cleanup.py \
  --db-path logs/quant_api.db \
  --audit-ttl-days 180 \
  --runtime-log-ttl-days 30 \
  --backtest-ttl-days 90 \
  --dry-run
```

正式执行：

```bash
cd backend/quant
/usr/bin/python3 tools/sqlite_retention_cleanup.py \
  --db-path logs/quant_api.db \
  --audit-ttl-days 180 \
  --runtime-log-ttl-days 30 \
  --backtest-ttl-days 90 \
  --backtest-final-statuses finished,failed,stopped,cancelled
```
