# PostgreSQL 备份 / WAL / PITR 演练指引

## 1. 目标

为 PostgreSQL 提供可脚本化验收的三件事：

- `pg_dump` 基线备份
- WAL 归档配置检查（`wal_level/archive_mode/archive_command`）
- PITR 演练探针（`pg_create_restore_point`）

工具脚本：

- `tools/postgres_backup_pitr_drill.py`

## 2. 最小执行示例

```bash
cd backend/quant
export QUANT_E2E_POSTGRES_DSN='postgresql://user:pass@127.0.0.1:5432/quant'
python tools/postgres_backup_pitr_drill.py \
  --backup-dir logs/postgres_backups \
  --prefix quant_pg \
  --retain 14
```

脚本成功后会输出 JSON 报告，包含：

- `wal`：WAL 配置检查结果
- `backup`：`pg_dump` 执行结果、备份文件、保留策略清理结果
- `pitr`：restore point 创建结果

## 3. 常用参数

- `--skip-pg-dump`：只做 WAL + PITR 检查，不执行 `pg_dump`
- `--skip-pitr-drill`：只做 `pg_dump` + WAL 检查
- `--allow-wal-unconfigured`：WAL 未配置时不置失败（灰度阶段可用）
- `--allow-pitr-fail`：restore point 失败时不置失败（只读实例可用）

## 4. 自动化建议

可将脚本接入 CI 或定时任务：

- 每日执行 `pg_dump` 基线备份
- 每次发布前执行一次完整 drill
- 将 JSON 结果上传到制品库或观测系统做留档
