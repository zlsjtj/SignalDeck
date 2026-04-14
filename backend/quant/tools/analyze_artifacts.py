import argparse
import csv

import pandas as pd


def compute_metrics(equity_csv: str, tf_minutes: int):
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

    dd_series = equity / equity.cummax() - 1.0
    trough_idx = dd_series.idxmin()
    peak_value = equity.iloc[: trough_idx + 1].max()
    peak_time = df['ts_utc'].iloc[: trough_idx + 1][equity.iloc[: trough_idx + 1].idxmax()]
    trough_time = df['ts_utc'].iloc[trough_idx]
    recovery_time = ''
    recovery_duration_hours = ''
    after = df.iloc[trough_idx:]
    recovered = after[after['equity'] >= peak_value]
    if not recovered.empty:
        recovery_time = recovered['ts_utc'].iloc[0].isoformat()
        recovery_duration_hours = (recovered['ts_utc'].iloc[0] - trough_time).total_seconds() / 3600.0

    return {
        'ann': float(ann_return),
        'mdd': float(max_dd),
        'sharpe_legacy': float(sharpe_legacy),
        'sharpe_correct': float(sharpe_correct),
        'dd_peak_time': peak_time.isoformat(),
        'dd_trough_time': trough_time.isoformat(),
        'dd_recovery_time': recovery_time,
        'dd_recovery_hours': recovery_duration_hours,
    }


def trade_stats(trades_csv: str):
    stats = {'trades_count': 0, 'total_notional': 0.0, 'total_fee': 0.0, 'turnover': 0.0}
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


def parse_metrics(metrics_path: str):
    if not metrics_path:
        return {}
    try:
        with open(metrics_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception:
        return {}
    out = {}
    for line in lines:
        line = line.strip()
        if not line or '=' not in line:
            continue
        k, v = line.split('=', 1)
        out[k] = v
    parsed = {}
    for key in ('riskoff_active_ratio', 'riskoff_toggle_count'):
        if key in out:
            try:
                parsed[key] = float(out[key])
            except Exception:
                parsed[key] = out[key]
    for key in ('riskoff_first_ts', 'riskoff_last_ts'):
        if key in out:
            parsed[key] = out[key]
    return parsed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--base_equity', required=True)
    ap.add_argument('--base_trades', required=True)
    ap.add_argument('--opt_equity', required=True)
    ap.add_argument('--opt_trades', required=True)
    ap.add_argument('--base_metrics', default='')
    ap.add_argument('--opt_metrics', default='')
    ap.add_argument('--timeframe', default='6h')
    ap.add_argument('--out', default='results/report_2025_base_vs_opt.csv')
    args = ap.parse_args()

    unit = args.timeframe[-1]
    val = int(args.timeframe[:-1])
    tf_minutes = val if unit == 'm' else val * 60 if unit == 'h' else val * 1440

    base_metrics = compute_metrics(args.base_equity, tf_minutes)
    opt_metrics = compute_metrics(args.opt_equity, tf_minutes)
    base_trades = trade_stats(args.base_trades)
    opt_trades = trade_stats(args.opt_trades)
    base_riskoff = parse_metrics(args.base_metrics)
    opt_riskoff = parse_metrics(args.opt_metrics)

    rows = [
        {'label': 'base', **base_metrics, **base_trades, **base_riskoff},
        {'label': 'opt', **opt_metrics, **opt_trades, **opt_riskoff},
    ]

    with open(args.out, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print('wrote', args.out)


if __name__ == '__main__':
    main()
