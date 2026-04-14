# SQLite 备份与恢复（Runbook）

> 适用目录：`backend/quant`

## 1. 在线备份脚本

脚本：`tools/sqlite_backup.py`

能力：
- 使用 SQLite `backup` API 在线备份（无需停服务）
- 支持完整性校验（`PRAGMA integrity_check`）
- 支持保留策略（自动删除过旧备份）

示例：

```bash
cd backend/quant
/usr/bin/python3 tools/sqlite_backup.py \
  --db-path logs/quant_api.db \
  --backup-dir logs/db_backups \
  --prefix quant_api \
  --retain 14 \
  --verify
```

## 2. 冷备脚本（停写窗口）

脚本：`tools/sqlite_cold_backup.py`

说明：
- 适用于“已停写/已停服务”的时间窗口
- 直接复制数据库文件（可选复制同名 `-wal/-shm` sidecar）

示例：

```bash
cd backend/quant
/usr/bin/python3 tools/sqlite_cold_backup.py \
  --db-path logs/quant_api.db \
  --backup-dir logs/db_cold_backups \
  --prefix quant_api_cold \
  --retain 7 \
  --verify
```

## 3. 推荐 Cron

每天 03:10 做一次备份：

```cron
10 3 * * * cd backend/quant && /usr/bin/python3 tools/sqlite_backup.py \
  --db-path logs/quant_api.db \
  --backup-dir logs/db_backups \
  --prefix quant_api \
  --retain 14 \
  --verify \
  >> logs/sqlite_backup_cron.log 2>&1
```

## 4. 恢复步骤（最小流程）

1. 停止 API 进程（避免恢复期间写入冲突）。
2. 选取目标备份文件（例如：`logs/db_backups/quant_api_20260304_031000.db`）。
3. 覆盖恢复：

```bash
cd backend/quant
cp logs/db_backups/quant_api_YYYYMMDD_HHMMSS.db logs/quant_api.db
```

4. 启动 API 服务并执行自检：

```bash
curl -s http://127.0.0.1:8000/api/health
curl -s http://127.0.0.1:8000/api/metrics | rg '^quant_db_'
```

## 5. 恢复演练自动化

脚本：`tools/sqlite_restore_drill.py`

示例：

```bash
cd backend/quant
/usr/bin/python3 tools/sqlite_restore_drill.py \
  --backup-file logs/db_backups/quant_api_YYYYMMDD_HHMMSS.db \
  --output-dir logs/restore_drills \
  --cleanup
```

脚本会自动执行：
- `integrity_check`
- 核心表结构检查
- 读写探针（插入/删除审计行）

## 6. 恢复演练建议

- 至少每月一次在预发环境执行恢复演练。
- 演练验收点：
  - `/api/health` 返回 `db=ok|degraded`（非 `error`）
  - `/api/strategies`、`/api/backtests`、`/api/audit/logs` 可读
  - 关键用户数据抽样一致
