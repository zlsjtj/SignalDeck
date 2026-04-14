param(
  [ValidateSet('paper','live','backtest')]
  [string]$Mode = 'paper',
  [string]$Start = '2023-01-01',
  [string]$End = '2025-12-31'
)

$python = '.\venv\Scripts\python'
if (-not (Test-Path $python)) {
  $python = 'python'
}

if ($Mode -eq 'backtest') {
  & $python 'statarb/backtest.py' --start $Start --end $End --config 'config_2025_bch_bnb_btc_equal_combo.yaml' --out 'logs/backtest_equity_combo.csv' --trades 'logs/backtest_trades_combo.csv' --metrics 'logs/backtest_metrics_combo.txt' --plot 'logs/backtest_equity_combo.png'
  exit $LASTEXITCODE
}

& $python 'main.py'
exit $LASTEXITCODE
