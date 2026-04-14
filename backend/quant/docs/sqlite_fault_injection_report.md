# SQLite 故障注入验证报告

## 1. 执行命令

```bash
cd backend/quant
python tools/sqlite_fault_injection.py --jitter-writes 20 --jitter-sleep-ms 20
```

## 2. 结果（2026-03-04）

- 总体：`ok=true`
- 场景结果：
  - `lock_conflict`：`ok=true`，耗时 `50.943ms`，捕获 `database is locked`
  - `disk_full_simulated`：`ok=true`，耗时 `0.0087ms`，捕获 `database or disk is full`
  - `io_jitter`：`ok=true`，`writes=20`，`sleep=20ms`，`p95=23.8333ms`

## 3. 结论

- 已覆盖数据库锁冲突、磁盘满、I/O 抖动三类故障注入场景。
- 当前脚本与自动化测试（`tests/test_sqlite_fault_injection_tool.py`）可作为发布前回归项。
