import argparse
import copy
import csv
import os
import random
import subprocess
import sys

import pandas as pd
import yaml


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base', default='config_2025_bch_bnb_btc_equal_combo_baseline_v2.yaml')
    ap.add_argument('--out', default='results/invvol_opt_results.csv')
    ap.add_argument('--top', type=int, default=10)
    ap.add_argument('--max_trials', type=int, default=300)
    ap.add_argument('--seed', type=int, default=42)
    ap.add_argument('--tol_ret', type=float, default=0.05)
    ap.add_argument('--tol_dd', type=float, default=0.05)
    args = ap.parse_args()

    random.seed(args.seed)
    py_exe = sys.executable
    os.makedirs('results', exist_ok=True)
    os.makedirs('logs/opt_configs', exist_ok=True)

    with open(args.base, 'r', encoding='utf-8') as f:
        base = yaml.safe_load(f)

    vol_lookbacks = [120, 240, 480, 960]
    max_weights = [0.45, 0.55, 0.65]
    gross_leverages = [1.0, 1.05, 1.1]

    candidates = []
    for lb in vol_lookbacks:
        for mw in max_weights:
            for gl in gross_leverages:
                candidates.append({'vol_lookback_hours': lb, 'max_weight': mw, 'gross_leverage': gl})

    if args.max_trials > len(candidates):
        candidates = [random.choice(candidates) for _ in range(args.max_trials)]
    else:
        random.shuffle(candidates)
        candidates = candidates[: args.max_trials]

    tf_minutes = timeframe_to_minutes(base['timeframe'])

    fieldnames = [
        'trial',
        'score',
        'vol_lookback_hours',
        'max_weight',
        'gross_leverage',
        'ann_2023',
        'mdd_2023',
        'sharpe_correct_2023',
        'turnover_2023',
        'total_fee_2023',
        'ann_2024',
        'mdd_2024',
        'sharpe_correct_2024',
        'turnover_2024',
        'total_fee_2024',
        'ann_2025',
        'mdd_2025',
        'sharpe_correct_2025',
        'turnover_2025',
        'total_fee_2025',
        'config_path',
    ]

    rows = []
    best = None

    for i, cand in enumerate(candidates):
        cfg = copy.deepcopy(base)
        cfg.setdefault('strategy', {})['inv_vol'] = {'enabled': True, 'lookback_hours': cand['vol_lookback_hours']}
        cfg['portfolio']['max_weight_per_symbol'] = cand['max_weight']
        cfg['portfolio']['gross_leverage'] = cand['gross_leverage']

        cfg_path = os.path.join('logs', 'opt_configs', f'invvol_{i}.yaml')
        with open(cfg_path, 'w', encoding='utf-8') as f:
            yaml.safe_dump(cfg, f, allow_unicode=False, sort_keys=False)

        equity_2023, trades_2023 = run_backtest(py_exe, cfg_path, '2023-01-01', '2023-12-31', f'invvol_{i}_2023')
        metrics_2023 = compute_metrics_from_equity(equity_2023, tf_minutes)
        stats_2023 = trade_stats(trades_2023)

        if metrics_2023['ann'] < 1.322159 * (1 - args.tol_ret):
            continue
        if abs(metrics_2023['mdd']) > 0.262046 * (1 + args.tol_dd):
            continue

        equity_2024, trades_2024 = run_backtest(py_exe, cfg_path, '2024-01-01', '2024-12-31', f'invvol_{i}_2024')
        metrics_2024 = compute_metrics_from_equity(equity_2024, tf_minutes)
        stats_2024 = trade_stats(trades_2024)

        if metrics_2024['ann'] < 1.275071 * (1 - args.tol_ret):
            continue
        if abs(metrics_2024['mdd']) > 0.415700 * (1 + args.tol_dd):
            continue

        equity_2025, trades_2025 = run_backtest(py_exe, cfg_path, '2025-01-01', '2025-12-31', f'invvol_{i}_2025')
        metrics_2025 = compute_metrics_from_equity(equity_2025, tf_minutes)
        stats_2025 = trade_stats(trades_2025)

        def score_y(metrics, stats):
            return metrics['ann'] - 0.8 * abs(metrics['mdd']) + 20 * metrics['sharpe_correct'] - 0.2 * stats['turnover']

        score = 0.2 * score_y(metrics_2023, stats_2023) + 0.3 * score_y(metrics_2024, stats_2024) + 0.5 * score_y(metrics_2025, stats_2025)

        row = {
            'trial': i,
            'score': score,
            'vol_lookback_hours': cand['vol_lookback_hours'],
            'max_weight': cand['max_weight'],
            'gross_leverage': cand['gross_leverage'],
            'ann_2023': metrics_2023['ann'],
            'mdd_2023': metrics_2023['mdd'],
            'sharpe_correct_2023': metrics_2023['sharpe_correct'],
            'turnover_2023': stats_2023['turnover'],
            'total_fee_2023': stats_2023['total_fee'],
            'ann_2024': metrics_2024['ann'],
            'mdd_2024': metrics_2024['mdd'],
            'sharpe_correct_2024': metrics_2024['sharpe_correct'],
            'turnover_2024': stats_2024['turnover'],
            'total_fee_2024': stats_2024['total_fee'],
            'ann_2025': metrics_2025['ann'],
            'mdd_2025': metrics_2025['mdd'],
            'sharpe_correct_2025': metrics_2025['sharpe_correct'],
            'turnover_2025': stats_2025['turnover'],
            'total_fee_2025': stats_2025['total_fee'],
            'config_path': cfg_path,
        }
        rows.append(row)
        if best is None or score > best['score']:
            best = row

    rows = sorted(rows, key=lambda r: r['score'], reverse=True)
    with open(args.out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    top_path = 'results/invvol_opt_results_top10.csv'
    with open(top_path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows[: args.top])

    if best:
        with open(best['config_path'], 'r', encoding='utf-8') as f:
            best_cfg = yaml.safe_load(f)
        out_cfg = 'config_2025_bch_bnb_btc_equal_combo_baseline_v2_invvol_best.yaml'
        with open(out_cfg, 'w', encoding='utf-8') as f:
            yaml.safe_dump(best_cfg, f, allow_unicode=False, sort_keys=False)
        print('Best config:', out_cfg)
    else:
        print('No valid configs met constraints.')


if __name__ == '__main__':
    main()
