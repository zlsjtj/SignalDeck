# combo_slim 稳健性验证与参数敏感性分析

## 结论先行
- drift_threshold 的稳定区间在 0.02~0.05：满足 2023/2024 约束，同时 2025 Sharpe 与年化更优。
- inv_vol 试验未能降低 2024/2025 回撤，且交易成本上升；不建议替代 baseline_v2。
- risk-off(btc_trend) 单机制能明显降低 2024/2025 回撤，但交易成本上升、2025 年化略降。

## 复现命令（已有）

### 2025 baseline trades
```bash
.\venv\Scripts\python statarb/backtest.py --start 2025-01-01 --end 2025-12-31 --config config_2025_bch_bnb_btc_equal_combo.yaml --out logs/backtest_equity_2025_combo_base.csv --trades logs/backtest_trades_2025_combo_base.csv --metrics logs/backtest_metrics_2025_combo_base.txt --plot logs/backtest_equity_2025_combo_base.png
```

### 2025 optimized equity
```bash
.\venv\Scripts\python statarb/backtest.py --start 2025-01-01 --end 2025-12-31 --config config_2025_bch_bnb_btc_equal_combo_optimized_v2.yaml --out logs/backtest_equity_2025_combo_opt.csv --trades logs/backtest_trades_2025_combo_opt.csv --metrics logs/backtest_metrics_2025_combo_opt.txt --plot logs/backtest_equity_2025_combo_opt.png
```

### 2025 base vs opt 对比报告
```bash
.\venv\Scripts\python tools\analyze_artifacts.py --base_equity logs\backtest_equity_2025_combo_base.csv --base_trades logs\backtest_trades_2025_combo_base.csv --opt_equity logs\backtest_equity_2025_combo_opt.csv --opt_trades logs\backtest_trades_2025_combo_opt.csv --timeframe 6h --out results\report_2025_base_vs_opt.csv
```

### drift 敏感性扫描
```bash
.\venv\Scripts\python tools\drift_sweep.py
```

### inv_vol 搜索
```bash
.\venv\Scripts\python tools\optimize_invvol.py --max_trials 300 --seed 42
```

## drift 阈值的成本 vs 风险权衡（数据证明）

来自：`results/drift_sweep_report.csv`

| drift | 2025 ann | 2025 mdd | sharpe_legacy | sharpe_correct | turnover | total_fee |
|---:|---:|---:|---:|---:|---:|---:|
| 0.00 | 0.2213 | -0.3437 | 1.5719 | 0.6417 | 5.6242 | 3.7061 |
| 0.02 | 0.2297 | -0.3431 | 1.6038 | 0.6547 | 2.4516 | 1.5485 |
| 0.05 | 0.2277 | -0.3344 | 1.5962 | 0.6517 | 1.7187 | 1.0975 |
| 0.08 | 0.1985 | -0.3303 | 1.4980 | 0.6115 | 1.1376 | 0.7139 |

推荐区间：0.02~0.05

## inv_vol 搜索结果（对比 baseline_v2）

来自：`results/before_after_invvol.csv`

| label | year | ann | mdd | sharpe_correct | trades_count | total_fee | turnover |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline_v2 | 2024 | 1.2808 | -0.4169 | 1.6469 | 29 | 1.8960 | 2.1427 |
| invvol_best | 2024 | 1.4059 | -0.4130 | 1.7042 | 39 | 2.5834 | 2.7675 |
| baseline_v2 | 2025 | 0.2346 | -0.3380 | 0.6623 | 21 | 1.1591 | 1.7973 |
| invvol_best | 2025 | 0.2392 | -0.3463 | 0.6649 | 33 | 1.4637 | 2.3619 |

结论：
- inv_vol 并未降低 2024/2025 回撤（2025 回撤反而更深），且交易成本更高。
- 因此该机制暂不推荐作为下一阶段稳健化方案。

---

## 风险-off（btc_trend）单机制优化

触发逻辑（小白版）：
- 用 BTC 的 6h K 线算一条均线（SMA）。
- 当 BTC 收盘价跌破 SMA，进入风险-off：把所有目标仓位统一缩小为 `risk_off_scale`（比如 0.95），剩余仓位视为现金。
- 当 BTC 收盘价回到 SMA 之上，并超过 `hysteresis`（例如 3%），退出风险-off。
- 每次进入/退出风险-off 都会触发一次强制再平衡，避免 14 天再平衡反应过慢。

最佳配置（基于 baseline_v2）：
- `risk_off_scale=0.95`
- `btc_sma_bars=320`（6h*320 ≈ 80 天）
- `hysteresis=0.03`
- `cooldown_bars=0`

对比结果（来自 `results/before_after_riskoff.csv`）：

| label | year | ann | mdd | sharpe_correct | trades | total_fee | turnover | riskoff_active_ratio | riskoff_toggle_count |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| baseline_v2 | 2023 | 1.3445 | -0.2663 | 1.9126 | 26 | 1.6386 | 2.0280 | 0.0000 | 0 |
| riskoff_best | 2023 | 1.2955 | -0.2510 | 1.9355 | 46 | 2.2463 | 2.6773 | 0.2852 | 6 |
| baseline_v2 | 2024 | 1.2808 | -0.4169 | 1.6469 | 29 | 1.8960 | 2.1427 | 0.0000 | 0 |
| riskoff_best | 2024 | 1.2868 | -0.3935 | 1.6814 | 73 | 3.1681 | 3.3966 | 0.3125 | 12 |
| baseline_v2 | 2025 | 0.2346 | -0.3380 | 0.6623 | 21 | 1.1591 | 1.7973 | 0.0000 | 0 |
| riskoff_best | 2025 | 0.2185 | -0.3278 | 0.6449 | 35 | 1.3519 | 2.1707 | 0.5168 | 5 |

结论：
- 回撤在 2024/2025 明显下降（更“稳”），且满足 2023/2024 约束。
- 成本上升（交易笔数、turnover、手续费都更高），这也是 2025 年化略降的主要原因之一。

## 复现命令（本次新增）

### baseline_v2 三年
```bash
.\venv\Scripts\python statarb/backtest.py --start 2023-01-01 --end 2023-12-31 --config config_2025_bch_bnb_btc_equal_combo_baseline_v2.yaml --out logs/backtest_equity_2023_combo_baseline_v2.csv --trades logs/backtest_trades_2023_combo_baseline_v2.csv --metrics logs/backtest_metrics_2023_combo_baseline_v2.txt --plot logs/backtest_equity_2023_combo_baseline_v2.png
.\venv\Scripts\python statarb/backtest.py --start 2024-01-01 --end 2024-12-31 --config config_2025_bch_bnb_btc_equal_combo_baseline_v2.yaml --out logs/backtest_equity_2024_combo_baseline_v2.csv --trades logs/backtest_trades_2024_combo_baseline_v2.csv --metrics logs/backtest_metrics_2024_combo_baseline_v2.txt --plot logs/backtest_equity_2024_combo_baseline_v2.png
.\venv\Scripts\python statarb/backtest.py --start 2025-01-01 --end 2025-12-31 --config config_2025_bch_bnb_btc_equal_combo_baseline_v2.yaml --out logs/backtest_equity_2025_combo_baseline_v2.csv --trades logs/backtest_trades_2025_combo_baseline_v2.csv --metrics logs/backtest_metrics_2025_combo_baseline_v2.txt --plot logs/backtest_equity_2025_combo_baseline_v2.png
```

### riskoff_best 三年
```bash
.\venv\Scripts\python statarb/backtest.py --start 2023-01-01 --end 2023-12-31 --config config_2025_bch_bnb_btc_equal_combo_baseline_v2_riskoff_best.yaml --out logs/backtest_equity_2023_combo_riskoff_best.csv --trades logs/backtest_trades_2023_combo_riskoff_best.csv --metrics logs/backtest_metrics_2023_combo_riskoff_best.txt --plot logs/backtest_equity_2023_combo_riskoff_best.png
.\venv\Scripts\python statarb/backtest.py --start 2024-01-01 --end 2024-12-31 --config config_2025_bch_bnb_btc_equal_combo_baseline_v2_riskoff_best.yaml --out logs/backtest_equity_2024_combo_riskoff_best.csv --trades logs/backtest_trades_2024_combo_riskoff_best.csv --metrics logs/backtest_metrics_2024_combo_riskoff_best.txt --plot logs/backtest_equity_2024_combo_riskoff_best.png
.\venv\Scripts\python statarb/backtest.py --start 2025-01-01 --end 2025-12-31 --config config_2025_bch_bnb_btc_equal_combo_baseline_v2_riskoff_best.yaml --out logs/backtest_equity_2025_combo_riskoff_best.csv --trades logs/backtest_trades_2025_combo_riskoff_best.csv --metrics logs/backtest_metrics_2025_combo_riskoff_best.txt --plot logs/backtest_equity_2025_combo_riskoff_best.png
```

### riskoff 搜索（400 trials）
```bash
.\venv\Scripts\python tools\optimize_riskoff.py --max_trials 400 --seed 42 --resume
```

### 汇总对比表
```bash
.\venv\Scripts\python - << 'PY'
import csv
from pathlib import Path

def read_metrics(path):
    out = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or '=' not in line:
                continue
            k, v = line.split('=', 1)
            out[k] = v
    return {
        'ann': float(out.get('annualized_return', 0.0)),
        'mdd': float(out.get('max_drawdown', 0.0)),
        'sharpe_correct': float(out.get('sharpe_correct', out.get('sharpe', 0.0))),
        'riskoff_active_ratio': float(out.get('riskoff_active_ratio', 0.0)),
        'riskoff_toggle_count': float(out.get('riskoff_toggle_count', 0.0)),
    }

def read_trades(path):
    stats = {'trades': 0, 'total_fee': 0.0, 'turnover': 0.0}
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats['trades'] += 1
            try:
                stats['total_fee'] += float(row.get('fee', 0.0))
            except Exception:
                pass
            try:
                eq_before = float(row.get('equity_before', 0.0))
            except Exception:
                eq_before = 0.0
            try:
                notional = float(row.get('notional', 0.0))
            except Exception:
                notional = 0.0
            if eq_before > 0:
                stats['turnover'] += notional / eq_before
    return stats

rows = []
for label, tag in [('baseline_v2', 'baseline_v2'), ('riskoff_best', 'riskoff_best')]:
    for year in (2023, 2024, 2025):
        metrics = read_metrics(f'logs/backtest_metrics_{year}_combo_{tag}.txt')
        trades = read_trades(f'logs/backtest_trades_{year}_combo_{tag}.csv')
        rows.append({'label': label, 'year': year, **metrics, **trades})

out_path = Path('results/before_after_riskoff.csv')
out_path.parent.mkdir(parents=True, exist_ok=True)
with out_path.open('w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
print('wrote', out_path)
PY
```
