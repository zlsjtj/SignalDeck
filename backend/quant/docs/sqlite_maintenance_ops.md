# SQLite 运维任务（自动化）

> 适用目录：`backend/quant`

## 1. 脚本

- 手工维护：`tools/sqlite_maintenance.py`
- 自动任务：`tools/sqlite_maintenance_job.py`

`sqlite_maintenance_job.py` 会按策略执行：
- `WAL checkpoint`（每次都执行）
- `ANALYZE`（按间隔执行）
- `VACUUM`（达到碎片/空闲阈值时执行，或强制执行）
- 产出状态文件与最新报告

## 2. 推荐 Cron

每 15 分钟运行（示例）：

```cron
*/15 * * * * cd backend/quant && /usr/bin/python3 tools/sqlite_maintenance_job.py \
  --db-path logs/quant_api.db \
  --state-path logs/sqlite_maintenance_state.json \
  --report-path logs/sqlite_maintenance_latest.json \
  --checkpoint PASSIVE \
  --analyze-every-hours 24 \
  --vacuum-fragmentation-threshold 20 \
  --vacuum-free-bytes-threshold 268435456 \
  --alert-fragmentation-threshold 35 \
  --alert-free-bytes-threshold 536870912 \
  >> logs/sqlite_maintenance_cron.log 2>&1
```

也可以使用安装脚本（幂等更新同一条任务）：

```bash
cd backend/quant
./tools/install_sqlite_maintenance_cron.sh
```

仅预览最终 crontab 内容（不安装）：

```bash
cd backend/quant
./tools/install_sqlite_maintenance_cron.sh --dry-run
```

## 3. 报警/退出码

- `exit 0`：任务正常，阈值未超标
- `exit 2`：任务完成但仍触发容量/碎片告警阈值（建议接入监控告警）
- 其他非零：脚本执行失败

报告文件默认输出到 `logs/sqlite_maintenance_latest.json`，可直接被监控采集。

## 4. 手工执行

```bash
cd backend/quant
/usr/bin/python3 tools/sqlite_maintenance_job.py --db-path logs/quant_api.db --force-vacuum
```
