#!/usr/bin/env python3
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _fmt_ts(ts: str) -> str:
    if not ts:
        return "-"
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return str(ts)


def _find_snapshot_path(root: Path, strategy_id: str, explicit_path: str) -> Path:
    if explicit_path:
        path = Path(explicit_path).expanduser()
        if not path.is_absolute():
            path = root / path
        return path.resolve()

    diag_dir = (root / "logs" / "diagnostics").resolve()
    if strategy_id:
        candidate = diag_dir / f"{strategy_id}.json"
        if candidate.exists():
            return candidate

    if not diag_dir.exists():
        raise FileNotFoundError(f"diagnostics dir not found: {diag_dir}")

    candidates = sorted(diag_dir.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise FileNotFoundError(f"no diagnostics snapshot json found under {diag_dir}")
    return candidates[0].resolve()


def _print_section(title: str) -> None:
    print()
    print(title)
    print("-" * len(title))


def _print_exceptions(last_20: List[Dict[str, Any]]) -> None:
    if not last_20:
        print("recent stacks: none")
        return
    print("recent stacks (up to 20):")
    for idx, item in enumerate(last_20, start=1):
        print(f"[{idx}] ts={_fmt_ts(str(item.get('ts', '')))} where={item.get('where', '')} type={item.get('type', '')}")
        stack = str(item.get("stack", "")).rstrip()
        if not stack:
            stack = str(item.get("message", ""))
        print(stack)
        print()


def print_report(path: Path, payload: Dict[str, Any]) -> None:
    process = payload.get("process", {})
    config = payload.get("config", {})
    market = payload.get("market_data", {})
    exchange = payload.get("exchange_connection", {})
    state = payload.get("strategy_state", {})
    pos_orders = payload.get("positions_and_orders", {})
    signal = payload.get("signal_evaluation", {})
    stops = payload.get("stop_take_trailing", {})
    order = payload.get("last_order_attempt", {})
    logging_info = payload.get("logging", {})
    exc = payload.get("exceptions", {})

    print("=" * 88)
    print(f"Diagnostics Snapshot: {path}")
    print(f"Generated At: {_fmt_ts(str(payload.get('generated_at', '')))}")
    print("=" * 88)

    _print_section("1) Process / Version / Config Summary")
    print(f"start_time: {_fmt_ts(str(process.get('started_at', '')))}")
    print(f"pid: {process.get('pid', '-')}")
    print(f"uptime_seconds: {process.get('uptime_seconds', '-')}")
    print(f"version: {process.get('version', '-')}")
    print(f"commit_id: {process.get('commit_id', '-')}")
    print(f"config_path: {process.get('config_path', config.get('path', '-'))}")
    print(f"config_summary: {json.dumps(config.get('summary', {}), ensure_ascii=False)}")

    _print_section("2) Market Data Health")
    print(f"last_tick_time: {_fmt_ts(str(market.get('last_tick_time', '')))}")
    print(f"last_bar_time: {_fmt_ts(str(market.get('last_bar_time', '')))}")
    print(f"data_lag_seconds: {market.get('data_lag_seconds', '-')}")
    print(f"data_source_status: {market.get('data_source_status', '-')}")
    print(f"data_source_detail: {market.get('data_source_detail', '-')}")

    _print_section("3) Exchange Connectivity")
    print(f"fetch_balance: {json.dumps(exchange.get('fetch_balance', {}), ensure_ascii=False)}")
    print(f"fetch_positions: {json.dumps(exchange.get('fetch_positions', {}), ensure_ascii=False)}")
    print(f"fetch_open_orders: {json.dumps(exchange.get('fetch_open_orders', {}), ensure_ascii=False)}")
    print(f"last_api_error: {json.dumps(exchange.get('last_api_error', {}), ensure_ascii=False)}")

    _print_section("4) Strategy State Machine")
    print(f"state: {state.get('state', '-')}")
    print(f"last_switch_reason: {state.get('last_switch_reason', '-')}")
    print(f"last_switch_time: {_fmt_ts(str(state.get('last_switch_time', '')))}")

    _print_section("5) Positions And Open Orders")
    print(f"positions: {json.dumps(pos_orders.get('positions', []), ensure_ascii=False)}")
    print(f"open_orders: {json.dumps(pos_orders.get('open_orders', []), ensure_ascii=False)}")

    _print_section("6) Latest Signal Evaluation")
    print(f"at: {_fmt_ts(str(signal.get('at', '')))}")
    print(f"conditions: {json.dumps(signal.get('conditions', []), ensure_ascii=False)}")
    print(f"entry_signal: {signal.get('entry_signal', False)}")
    print(f"filter_reasons: {json.dumps(signal.get('filter_reasons', []), ensure_ascii=False)}")
    print(f"details: {json.dumps(signal.get('details', {}), ensure_ascii=False)}")

    _print_section("7) Stop Loss / Take Profit / Trailing Stop")
    print(f"sl: {stops.get('sl', None)}")
    print(f"tp: {stops.get('tp', None)}")
    print(f"ts: {stops.get('ts', None)}")
    print(f"price_source: {stops.get('price_source', '-')}")
    print(f"last_updated: {_fmt_ts(str(stops.get('last_updated', '')))}")
    print(f"note: {stops.get('note', '-')}")

    _print_section("8) Last Order Attempt")
    print(f"attempt: {json.dumps(order, ensure_ascii=False)}")

    _print_section("9) Logging System")
    print(f"logging: {json.dumps(logging_info, ensure_ascii=False)}")

    _print_section("10) Exception Stats (Past 10 Days)")
    print(f"window_days: {exc.get('window_days', '-')}")
    print(f"total_count: {exc.get('total_count', '-')}")
    print(f"counts_by_day: {json.dumps(exc.get('counts_by_day', {}), ensure_ascii=False)}")
    _print_exceptions(exc.get("last_20", []))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--strategy-id", default="", help="strategy id, e.g. strategy_candidate_v010")
    parser.add_argument("--path", default="", help="explicit diagnostics json path")
    parser.add_argument("--follow", action="store_true", help="print heartbeat continuously")
    parser.add_argument("--heartbeat-minutes", type=int, default=1, help="follow interval in minutes (1-5)")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve()
    interval_minutes = max(1, min(5, int(args.heartbeat_minutes)))

    while True:
        snapshot_path = _find_snapshot_path(root, args.strategy_id.strip(), args.path.strip())
        payload = json.loads(snapshot_path.read_text(encoding="utf-8"))
        print_report(snapshot_path, payload)
        if not args.follow:
            break
        time.sleep(interval_minutes * 60)


if __name__ == "__main__":
    main()
