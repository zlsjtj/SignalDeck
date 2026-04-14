import csv
import os
import subprocess
import sys

import pandas as pd
import yaml

BASE = 'config_2025_bch_bnb_btc_equal_combo.yaml'
SWEEP = [0.00, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15]


def timeframe_to_minutes(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == 'm':
        return val
    if unit == 'h':
        return val * 60
    if unit == 'd':
        return val * 1440
    return 0


def compute_metrics_from_equity(equity_csv: str, tf_minutes: int):
    df = pd.read_csv(equity_csv)
    df['ts_utc'] = pd.to_datetime(df['ts_utc'])
    equity = df['equity']
    returns = equity.pct_change().dropna()

    max_dd = (equity / equity.cummax() - 1.0).min()

    total_hours = 0.0
    if len(df['ts_utc']) >= 2:
        total_hours = (df['ts_utc'].iloc[-1] - df['ts_utc'].iloc[0]).total_seconds() / 3600.0
    ann_factor_legacy = 365.0 * 24.0
    if total_hours > 0 and equity.iloc[0] > 0:
        ann_return = (equity.iloc[-1] / equity.iloc[0]) ** (ann_factor_legacy / total_hours) - 1.0
    else:
        ann_return = 0.0

    if returns.std() and returns.std() > 0:
        sharpe_legacy = returns.mean() / returns.std() * (ann_factor_legacy ** 0.5)
    else:
        sharpe_legacy = 0.0

    periods_per_year = (365.0 * 24.0 * 60.0) / max(1, tf_minutes)
    if returns.std() and returns.std() > 0:
        sharpe_correct = returns.mean() / returns.std() * (periods_per_year ** 0.5)
    else:
        sharpe_correct = 0.0

    return {
        'ann': float(ann_return),
        'mdd': float(max_dd),
        'sharpe_legacy': float(sharpe_legacy),
        'sharpe_correct': float(sharpe_correct),
    }


def trade_stats(trades_csv: str):
    stats = {
        'trades_count': 0,
        'total_notional': 0.0,
        'total_fee': 0.0,
        'turnover': 0.0,
    }
    if not os.path.exists(trades_csv):
        return stats
    with open(trades_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            stats['trades_count'] += 1
            try:
                notional = float(row.get('notional', 0.0))
            except Exception:
                notional = 0.0
            try:
                fee = float(row.get('fee', 0.0))
            except Exception:
                fee = 0.0
            try:
                eq_before = float(row.get('equity_before', 0.0))
            except Exception:
                eq_before = 0.0
            stats['total_notional'] += notional
            stats['total_fee'] += fee
            if eq_before > 0:
                stats['turnover'] += notional / eq_before
    return stats


def run_backtest(py_exe, cfg_path, start, end, tag):
    out_dir = os.path.join('logs', 'opt_runs')
    os.makedirs(out_dir, exist_ok=True)
    equity = os.path.join(out_dir, f'equity_{tag}.csv')
    trades = os.path.join(out_dir, f'trades_{tag}.csv')
    cmd = [
        py_exe,
        'statarb/backtest.py',
        '--start',
        start,
        '--end',
        end,
        '--config',
        cfg_path,
        '--metrics',
        os.path.join(out_dir, f'metrics_{tag}.txt'),
        '--out',
        equity,
        '--trades',
        trades,
        '--plot',
        os.path.join(out_dir, f'equity_{tag}.png'),
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return equity, trades


with open(BASE, 'r', encoding='utf-8') as f:
    base = yaml.safe_load(f)

os.makedirs('results', exist_ok=True)
rows = []

for drift in SWEEP:
    cfg = dict(base)
    cfg.setdefault('portfolio', {})['drift_threshold'] = drift
    cfg_path = os.path.join('logs', 'opt_configs', f'drift_{drift:.2f}.yaml')
    with open(cfg_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(cfg, f, allow_unicode=False, sort_keys=False)

    tf_minutes = timeframe_to_minutes(cfg['timeframe'])

    for year in (2023, 2024, 2025):
        equity_csv, trades_csv = run_backtest(sys.executable, cfg_path, f'{year}-01-01', f'{year}-12-31', f'drift_{drift:.2f}_{year}')
        metrics = compute_metrics_from_equity(equity_csv, tf_minutes)
        stats = trade_stats(trades_csv)
        rows.append(
            {
                'drift_threshold': drift,
                'year': year,
                'ann': metrics['ann'],
                'mdd': metrics['mdd'],
                'sharpe_legacy': metrics['sharpe_legacy'],
                'sharpe_correct': metrics['sharpe_correct'],
                'trades_count': stats['trades_count'],
                'total_fee': stats['total_fee'],
                'turnover': stats['turnover'],
            }
        )

path = 'results/drift_sweep_report.csv'
with open(path, 'w', newline='', encoding='utf-8') as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)

print('wrote', path)
