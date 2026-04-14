import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta

from db_store import SQLiteStore


class SQLiteStoreTests(unittest.TestCase):
    def test_schema_version_migrations_applied(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            with store._connect() as conn:
                versions = [int(row[0]) for row in conn.execute("SELECT version FROM schema_version ORDER BY version ASC")]
                self.assertEqual(versions, list(range(1, 22)))
                idempotency_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='idempotency_records'"
                ).fetchone()
                self.assertIsNotNone(idempotency_table)
                risk_events_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='risk_events'"
                ).fetchone()
                self.assertIsNotNone(risk_events_table)
                strategy_params_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='strategy_params'"
                ).fetchone()
                self.assertIsNotNone(strategy_params_table)
                risk_state_history_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='risk_state_history'"
                ).fetchone()
                self.assertIsNotNone(risk_state_history_table)
                market_ticks_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='market_ticks'"
                ).fetchone()
                self.assertIsNotNone(market_ticks_table)
                market_klines_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='market_klines'"
                ).fetchone()
                self.assertIsNotNone(market_klines_table)
                backtest_trades_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_trades'"
                ).fetchone()
                self.assertIsNotNone(backtest_trades_table)
                backtest_equity_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='backtest_equity_points'"
                ).fetchone()
                self.assertIsNotNone(backtest_equity_table)
                alert_deliveries_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_deliveries'"
                ).fetchone()
                self.assertIsNotNone(alert_deliveries_table)
                alert_outbox_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='alert_outbox'"
                ).fetchone()
                self.assertIsNotNone(alert_outbox_table)
                ws_events_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='ws_connection_events'"
                ).fetchone()
                self.assertIsNotNone(ws_events_table)
                account_security_events_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='account_security_events'"
                ).fetchone()
                self.assertIsNotNone(account_security_events_table)
                api_tokens_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='api_tokens'"
                ).fetchone()
                self.assertIsNotNone(api_tokens_table)
                roles_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='roles'"
                ).fetchone()
                self.assertIsNotNone(roles_table)
                permissions_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='permissions'"
                ).fetchone()
                self.assertIsNotNone(permissions_table)
                user_roles_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='user_roles'"
                ).fetchone()
                self.assertIsNotNone(user_roles_table)
                role_permissions_table = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='role_permissions'"
                ).fetchone()
                self.assertIsNotNone(role_permissions_table)
                strategies_columns = {
                    str(row[1]) for row in conn.execute("PRAGMA table_info(strategies)").fetchall()
                }
                self.assertIn("owner_user_id", strategies_columns)

    def test_roundtrip_for_core_tables(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            strategy_record = {
                "id": "strategy_1",
                "name": "s1",
                "status": "running",
                "createdAt": "2026-01-01T00:00:00+00:00",
                "updatedAt": "2026-01-01T00:00:01+00:00",
                "owner": "alice",
                "config": {"symbols": ["BTCUSDT"]},
            }
            store.upsert_strategy("usr__alice__strategy_1", "alice", strategy_record)
            strategies = store.load_strategies()
            self.assertEqual(len(strategies), 1)
            self.assertEqual(strategies[0]["strategy_key"], "usr__alice__strategy_1")
            self.assertEqual(strategies[0]["record"]["name"], "s1")

            backtest_record = {
                "id": "bt_1",
                "owner": "alice",
                "strategyId": "strategy_1",
                "strategyName": "s1",
                "symbol": "BTCUSDT",
                "startAt": "2026-01-01",
                "endAt": "2026-01-31",
                "status": "running",
                "createdAt": "2026-01-01T00:00:00+00:00",
                "updatedAt": "2026-01-01T00:00:02+00:00",
                "initialCapital": 10000.0,
                "metrics": {
                    "pnlTotal": 500.0,
                    "sharpe": 1.2,
                    "calmar": 0.8,
                    "maxDrawdown": 0.12,
                },
            }
            store.upsert_backtest("bt_1", "alice", backtest_record)
            backtests = store.load_backtests()
            self.assertEqual(len(backtests), 1)
            self.assertEqual(backtests[0]["run_id"], "bt_1")

            risk_state = {
                "enabled": True,
                "maxDrawdownPct": 0.2,
                "updatedAt": "2026-01-01T00:00:03+00:00",
            }
            store.upsert_risk_state("alice", "usr__alice__strategy_1", risk_state)
            risks = store.load_risk_states()
            self.assertEqual(len(risks), 1)
            self.assertEqual(risks[0]["strategy_key"], "usr__alice__strategy_1")

            with store._connect() as conn:
                strategy_row = conn.execute(
                    """
                    SELECT strategy_name, primary_symbol, timeframe
                    FROM strategies
                    WHERE strategy_key = ?
                    """,
                    ("usr__alice__strategy_1",),
                ).fetchone()
                self.assertIsNotNone(strategy_row)
                self.assertEqual(str(strategy_row["strategy_name"]), "s1")
                self.assertEqual(str(strategy_row["primary_symbol"]), "BTCUSDT")
                self.assertEqual(str(strategy_row["timeframe"]), "")

                param_rows = conn.execute(
                    "SELECT param_key, param_value_text, value_type FROM strategy_params WHERE strategy_key = ? ORDER BY param_key ASC",
                    ("usr__alice__strategy_1",),
                ).fetchall()
                self.assertGreaterEqual(len(param_rows), 0)

                backtest_row = conn.execute(
                    """
                    SELECT strategy_id, strategy_name, symbol, start_at, end_at, metric_return, metric_sharpe, metric_calmar, metric_max_drawdown
                    FROM backtests
                    WHERE run_id = ?
                    """,
                    ("bt_1",),
                ).fetchone()
                self.assertIsNotNone(backtest_row)
                self.assertEqual(str(backtest_row["strategy_id"]), "strategy_1")
                self.assertEqual(str(backtest_row["strategy_name"]), "s1")
                self.assertEqual(str(backtest_row["symbol"]), "BTCUSDT")
                self.assertEqual(str(backtest_row["start_at"]), "2026-01-01")
                self.assertEqual(str(backtest_row["end_at"]), "2026-01-31")
                self.assertAlmostEqual(float(backtest_row["metric_return"]), 0.05, places=6)
                self.assertAlmostEqual(float(backtest_row["metric_sharpe"]), 1.2, places=6)
                self.assertAlmostEqual(float(backtest_row["metric_calmar"]), 0.8, places=6)
                self.assertAlmostEqual(float(backtest_row["metric_max_drawdown"]), 0.12, places=6)

    def test_strategy_params_split_table_populates_config_params(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.upsert_strategy(
                "usr__alice__strategy_2",
                "alice",
                {
                    "id": "strategy_2",
                    "name": "s2",
                    "status": "stopped",
                    "createdAt": "2026-01-01T00:00:00+00:00",
                    "updatedAt": "2026-01-01T00:00:00+00:00",
                    "owner": "alice",
                    "config": {
                        "symbols": ["ETH/USDT:USDT"],
                        "timeframe": "1h",
                        "params": {
                            "alpha": 0.123,
                            "enabled": True,
                            "mode": "paper",
                        },
                    },
                },
            )
            with store._connect() as conn:
                rows = conn.execute(
                    "SELECT param_key, param_value_text, value_type FROM strategy_params WHERE strategy_key = ? ORDER BY param_key ASC",
                    ("usr__alice__strategy_2",),
                ).fetchall()
            self.assertEqual([str(row["param_key"]) for row in rows], ["alpha", "enabled", "mode"])
            self.assertEqual(str(rows[0]["value_type"]), "number")
            self.assertEqual(str(rows[1]["value_type"]), "bool")
            self.assertEqual(str(rows[2]["value_type"]), "string")

    def test_audit_filters(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_audit_log(
                owner="alice",
                action="strategy.create",
                entity="strategy",
                entity_id="strategy_1",
                detail={"name": "s1"},
            )
            store.append_audit_log(
                owner="bob",
                action="risk.update",
                entity="risk",
                entity_id="strategy_2",
                detail={"maxDrawdownPct": 0.1},
            )

            alice_rows = store.list_audit_logs(owner="alice", limit=50)
            self.assertEqual(len(alice_rows), 1)
            self.assertEqual(alice_rows[0]["owner"], "alice")

            risk_rows = store.list_audit_logs(entity="risk", limit=50)
            self.assertEqual(len(risk_rows), 1)
            self.assertEqual(risk_rows[0]["action"], "risk.update")

    def test_audit_time_range_and_cursor(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_audit_log(owner="alice", action="a1", entity="strategy", entity_id="s1", detail={})
            store.append_audit_log(owner="alice", action="a2", entity="strategy", entity_id="s2", detail={})
            store.append_audit_log(owner="alice", action="a3", entity="strategy", entity_id="s3", detail={})

            rows_desc = store.list_audit_logs(owner="alice", limit=10)
            self.assertEqual(len(rows_desc), 3)
            id_newest = rows_desc[0]["id"]
            id_mid = rows_desc[1]["id"]
            id_oldest = rows_desc[2]["id"]

            t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            ts_old = t0.isoformat()
            ts_mid = (t0 + timedelta(minutes=1)).isoformat()
            ts_new = (t0 + timedelta(minutes=2)).isoformat()
            with store._connect() as conn:
                conn.execute("UPDATE audit_logs SET ts_utc = ? WHERE id = ?", (ts_new, id_newest))
                conn.execute("UPDATE audit_logs SET ts_utc = ? WHERE id = ?", (ts_mid, id_mid))
                conn.execute("UPDATE audit_logs SET ts_utc = ? WHERE id = ?", (ts_old, id_oldest))
                conn.commit()

            range_rows = store.list_audit_logs(
                owner="alice",
                start_ts=(t0 + timedelta(seconds=30)).isoformat(),
                end_ts=(t0 + timedelta(minutes=1, seconds=30)).isoformat(),
                limit=10,
            )
            self.assertEqual(len(range_rows), 1)
            self.assertEqual(range_rows[0]["id"], id_mid)

            first_page = store.list_audit_logs(owner="alice", limit=1)
            self.assertEqual(len(first_page), 1)
            cursor_id = first_page[0]["id"]
            next_page = store.list_audit_logs(owner="alice", cursor_id=cursor_id, limit=10)
            self.assertEqual(len(next_page), 2)
            self.assertTrue(all(int(row["id"]) < int(cursor_id) for row in next_page))

    def test_runtime_logs_filters_and_cursor(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
            old_ts = t0.isoformat()
            mid_ts = (t0 + timedelta(minutes=1)).isoformat()
            new_ts = (t0 + timedelta(minutes=2)).isoformat()

            store.append_runtime_log(
                owner="alice",
                log_type="strategy",
                level="info",
                source="stdout",
                message="alpha warmup",
                strategy_id="s1",
                ts_utc=old_ts,
            )
            store.append_runtime_log(
                owner="alice",
                log_type="strategy",
                level="error",
                source="stderr",
                message="alpha failed",
                strategy_id="s1",
                ts_utc=mid_ts,
            )
            store.append_runtime_log(
                owner="bob",
                log_type="system",
                level="warn",
                source="system",
                message="maintenance warning",
                strategy_id="",
                ts_utc=new_ts,
            )

            alice_rows = store.list_runtime_logs(owner="alice", log_type="strategy", limit=10)
            self.assertEqual(len(alice_rows), 2)
            self.assertEqual(str(alice_rows[0]["level"]), "error")
            self.assertEqual(str(alice_rows[1]["level"]), "info")

            q_rows = store.list_runtime_logs(owner="alice", q="failed", limit=10)
            self.assertEqual(len(q_rows), 1)
            self.assertEqual(str(q_rows[0]["message"]), "alpha failed")

            range_rows = store.list_runtime_logs(
                owner="alice",
                start_ts=(t0 + timedelta(seconds=30)).isoformat(),
                end_ts=(t0 + timedelta(minutes=1, seconds=30)).isoformat(),
                limit=10,
            )
            self.assertEqual(len(range_rows), 1)
            self.assertEqual(str(range_rows[0]["message"]), "alpha failed")

            first_page = store.list_runtime_logs(owner="alice", limit=1)
            self.assertEqual(len(first_page), 1)
            cursor_id = int(first_page[0]["cursorId"])
            second_page = store.list_runtime_logs(owner="alice", cursor_id=cursor_id, limit=10)
            self.assertEqual(len(second_page), 1)
            self.assertEqual(str(second_page[0]["message"]), "alpha warmup")

    def test_strategy_diagnostics_snapshots_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            snapshot = {
                "generated_at": "2026-03-05T00:00:00+00:00",
                "strategy_state": {"state": "RUNNING"},
                "market_data": {"data_source_status": "ok"},
                "signal_evaluation": {"entry_signal": True, "filter_reasons": ["r1", "r2"]},
                "exceptions": {"total_count": 3},
            }
            store.append_strategy_diagnostics_snapshot(
                owner="alice",
                strategy_id="s1",
                source_path="/tmp/s1.json",
                snapshot=snapshot,
                ts_utc="2026-03-05T00:00:10+00:00",
            )
            store.append_strategy_diagnostics_snapshot(
                owner="alice",
                strategy_id="s1",
                source_path="/tmp/s1.json",
                snapshot={
                    "generated_at": "2026-03-05T00:01:00+00:00",
                    "strategy_state": {"state": "STOPPED"},
                    "market_data": {"data_source_status": "stale"},
                    "signal_evaluation": {"entry_signal": False, "filter_reasons": []},
                    "exceptions": {"total_count": 0},
                },
                ts_utc="2026-03-05T00:01:10+00:00",
            )

            rows = store.list_strategy_diagnostics_snapshots(owner="alice", strategy_id="s1", limit=10)
            self.assertEqual(len(rows), 2)
            self.assertEqual(str(rows[0]["strategyState"]), "STOPPED")
            self.assertEqual(str(rows[1]["strategyState"]), "RUNNING")
            self.assertEqual(int(rows[1]["exceptionTotalCount"]), 3)
            self.assertEqual(rows[1]["filterReasons"], ["r1", "r2"])

            rows_with_snapshot = store.list_strategy_diagnostics_snapshots(
                owner="alice",
                strategy_id="s1",
                include_snapshot=True,
                limit=1,
            )
            self.assertEqual(len(rows_with_snapshot), 1)
            self.assertTrue(isinstance(rows_with_snapshot[0].get("snapshot"), dict))

            first_page = store.list_strategy_diagnostics_snapshots(owner="alice", strategy_id="s1", limit=1)
            cursor_id = int(first_page[0]["cursorId"])
            second_page = store.list_strategy_diagnostics_snapshots(
                owner="alice",
                strategy_id="s1",
                cursor_id=cursor_id,
                limit=10,
            )
            self.assertEqual(len(second_page), 1)
            self.assertEqual(str(second_page[0]["strategyState"]), "RUNNING")

    def test_backtest_detail_rows_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            trades_count = store.replace_backtest_trades(
                run_id="bt_detail_1",
                owner="alice",
                rows=[
                    {
                        "id": "t1",
                        "ts": "2026-03-05T00:00:00+00:00",
                        "symbol": "BTC/USDT:USDT",
                        "side": "buy",
                        "qty": 1.2,
                        "price": 100.5,
                        "fee": 0.1,
                        "pnl": 0.0,
                        "orderId": "o1",
                        "extra": {"source": "unit"},
                    },
                    {
                        "id": "t2",
                        "ts": "2026-03-05T00:05:00+00:00",
                        "symbol": "BTC/USDT:USDT",
                        "side": "sell",
                        "qty": 1.2,
                        "price": 101.0,
                        "fee": 0.1,
                        "pnl": 0.5,
                        "orderId": "o2",
                    },
                ],
            )
            self.assertEqual(trades_count, 2)

            equity_count = store.replace_backtest_equity_points(
                run_id="bt_detail_1",
                owner="alice",
                rows=[
                    {
                        "ts": "2026-03-05T00:00:00+00:00",
                        "equity": 1000.0,
                        "pnl": 0.0,
                        "dd": 0.0,
                    },
                    {
                        "ts": "2026-03-05T00:05:00+00:00",
                        "equity": 1001.5,
                        "pnl": 1.5,
                        "dd": 0.0,
                    },
                ],
            )
            self.assertEqual(equity_count, 2)

            trades = store.list_backtest_trades(run_id="bt_detail_1", owner="alice", limit=10)
            self.assertEqual(len(trades), 2)
            self.assertEqual(str(trades[0]["id"]), "t1")
            self.assertEqual(str(trades[1]["side"]), "sell")
            self.assertEqual(str(((trades[0].get("extra") or {}).get("source"))), "unit")

            equity_points = store.list_backtest_equity_points(run_id="bt_detail_1", owner="alice", limit=10)
            self.assertEqual(len(equity_points), 2)
            self.assertAlmostEqual(float(equity_points[-1]["equity"]), 1001.5, places=6)

    def test_alert_deliveries_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_alert_delivery(
                owner="admin",
                event="db_runtime_persistence_failure",
                severity="critical",
                message="db write failed",
                webhook_url="https://example.invalid/hook",
                status="failed",
                retry_count=1,
                http_status=500,
                error_message="http_status=500",
                payload={"component": "db"},
                response_body="server error",
                ts_utc="2026-03-05T00:00:00+00:00",
                duration_ms=120.5,
            )
            store.append_alert_delivery(
                owner="admin",
                event="db_init_failure",
                severity="critical",
                message="db init failed",
                webhook_url="https://example.invalid/hook",
                status="sent",
                retry_count=0,
                payload={"component": "db"},
                ts_utc="2026-03-05T00:01:00+00:00",
                duration_ms=80.0,
            )

            rows = store.list_alert_deliveries(owner="admin", limit=10)
            self.assertEqual(len(rows), 2)
            self.assertEqual(str(rows[0]["status"]), "sent")
            self.assertEqual(str(rows[1]["status"]), "failed")
            self.assertEqual(int(rows[1]["retryCount"]), 1)

            failed_rows = store.list_alert_deliveries(owner="admin", status="failed", limit=10)
            self.assertEqual(len(failed_rows), 1)
            self.assertEqual(str(failed_rows[0]["event"]), "db_runtime_persistence_failure")
            self.assertEqual(str((failed_rows[0].get("payload") or {}).get("component")), "db")

    def test_alert_outbox_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            outbox_id = store.enqueue_alert_outbox(
                owner="guest",
                event="db_runtime_persistence_failure",
                severity="critical",
                message="db write failed",
                webhook_url="https://example.invalid/hook",
                payload={"source": "unit"},
                max_retries=2,
                available_at="2026-03-05T00:00:00+00:00",
            )
            self.assertGreater(int(outbox_id), 0)

            due_rows = store.list_due_alert_outbox(now_ts="2026-03-05T00:00:00+00:00", limit=10)
            self.assertEqual(len(due_rows), 1)
            self.assertEqual(int(due_rows[0]["id"]), int(outbox_id))
            self.assertEqual(int(due_rows[0]["maxRetries"]), 2)
            self.assertEqual(str((due_rows[0].get("payload") or {}).get("source")), "unit")

            store.finalize_alert_outbox(
                outbox_id,
                status="pending",
                retry_count=1,
                available_at="2026-03-05T00:10:00+00:00",
                http_status=500,
                error_message="http_status=500",
                response_body="error",
            )
            not_due = store.list_due_alert_outbox(now_ts="2026-03-05T00:05:00+00:00", limit=10)
            self.assertEqual(len(not_due), 0)

            due_rows_after = store.list_due_alert_outbox(now_ts="2026-03-05T00:10:00+00:00", limit=10)
            self.assertEqual(len(due_rows_after), 1)
            self.assertEqual(int(due_rows_after[0]["retryCount"]), 1)

            store.finalize_alert_outbox(
                outbox_id,
                status="sent",
                retry_count=1,
                http_status=200,
                response_body="ok",
                dispatched_at="2026-03-05T00:10:01+00:00",
            )
            no_rows = store.list_due_alert_outbox(now_ts="2026-03-05T00:11:00+00:00", limit=10)
            self.assertEqual(len(no_rows), 0)

    def test_ws_connection_events_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_ws_connection_event(
                owner="guest",
                event_type="connected",
                connection_id="ws_1",
                strategy_id="strategy_1",
                config_path="config.yaml",
                refresh_ms=1000,
                client_ip="127.0.0.1",
                user_agent="unit-test",
                detail={"phase": "open"},
                ts_utc="2026-03-05T00:00:00+00:00",
            )
            store.append_ws_connection_event(
                owner="guest",
                event_type="disconnected",
                connection_id="ws_1",
                strategy_id="strategy_1",
                config_path="config.yaml",
                refresh_ms=1000,
                client_ip="127.0.0.1",
                user_agent="unit-test",
                detail={"reason": "client_disconnect"},
                ts_utc="2026-03-05T00:01:00+00:00",
            )

            rows = store.list_ws_connection_events(owner="guest", limit=10)
            self.assertEqual(len(rows), 2)
            self.assertEqual(str(rows[0]["eventType"]), "disconnected")
            self.assertEqual(str(rows[1]["eventType"]), "connected")
            self.assertEqual(str(rows[1]["connectionId"]), "ws_1")
            self.assertEqual(str((rows[0].get("detail") or {}).get("reason")), "client_disconnect")

            connected_rows = store.list_ws_connection_events(owner="guest", event_type="connected", limit=10)
            self.assertEqual(len(connected_rows), 1)
            self.assertEqual(str(connected_rows[0]["eventType"]), "connected")

    def test_audit_hash_chain_verification(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_audit_log(owner="alice", action="a1", entity="strategy", entity_id="s1", detail={"v": 1})
            store.append_audit_log(owner="alice", action="a2", entity="strategy", entity_id="s2", detail={"v": 2})
            rows = store.list_audit_logs(owner="alice", limit=10)
            self.assertTrue(all(str(row.get("rowHash") or "") for row in rows))
            self.assertTrue(all(str(row.get("prevHash") or "") for row in rows))
            verify_ok = store.verify_audit_hash_chain(owner="alice", limit=100)
            self.assertTrue(bool(verify_ok.get("ok")))

            row_id = int(rows[-1]["id"])
            with store._connect() as conn:
                conn.execute("UPDATE audit_logs SET detail_json = ? WHERE id = ?", ('{"tampered":true}', row_id))
                conn.commit()
            verify_fail = store.verify_audit_hash_chain(owner="alice", limit=100)
            self.assertFalse(bool(verify_fail.get("ok")))
            self.assertGreaterEqual(len(verify_fail.get("mismatchedRows") or []), 1)

    def test_risk_state_history_versions(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            strategy_key = "usr__alice__strategy_3"
            store.upsert_risk_state("alice", strategy_key, {"enabled": True, "maxDrawdownPct": 0.1, "updatedAt": "2026-01-01T00:00:00+00:00"})
            store.upsert_risk_state("alice", strategy_key, {"enabled": True, "maxDrawdownPct": 0.2, "updatedAt": "2026-01-01T00:01:00+00:00"})
            store.delete_risk_state("alice", strategy_key)

            rows = store.list_risk_state_history(owner="alice", strategy_key=strategy_key, limit=10)
            self.assertEqual(len(rows), 3)
            versions = sorted(int(row["version"]) for row in rows)
            self.assertEqual(versions, [1, 2, 3])
            change_types = [str(row["changeType"]) for row in rows]
            self.assertIn("upsert", change_types)
            self.assertIn("delete", change_types)

    def test_market_timeseries_upsert(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            inserted_ticks = store.upsert_market_ticks(
                [
                    {
                        "symbol": "BTC/USDT:USDT",
                        "ts_utc": "2026-01-01T00:00:00+00:00",
                        "price": 100.0,
                        "bid": 99.5,
                        "ask": 100.5,
                        "volume": 10.0,
                    }
                ],
                source_config_path="config.yaml",
            )
            self.assertEqual(inserted_ticks, 1)

            inserted_klines = store.upsert_market_klines(
                [
                    {
                        "symbol": "BTC/USDT:USDT",
                        "time": 1767225600,
                        "ts_utc": "2026-01-01T00:00:00+00:00",
                        "open": 100.0,
                        "high": 110.0,
                        "low": 90.0,
                        "close": 105.0,
                        "volume": 123.0,
                    }
                ],
                timeframe="15m",
                source_config_path="config.yaml",
            )
            self.assertEqual(inserted_klines, 1)

            with store._connect() as conn:
                tick_count = int(conn.execute("SELECT COUNT(1) FROM market_ticks").fetchone()[0])
                kline_count = int(conn.execute("SELECT COUNT(1) FROM market_klines").fetchone()[0])
            self.assertEqual(tick_count, 1)
            self.assertEqual(kline_count, 1)

    def test_risk_events_filters_and_cursor(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_risk_event(
                owner="alice",
                strategy_key="usr__alice__strategy_1",
                event_type="triggered",
                rule="max_drawdown",
                message="max drawdown threshold breached",
                detail={"threshold": 0.2, "actual": 0.31},
                ts_utc="2026-01-01T00:00:00+00:00",
            )
            store.append_risk_event(
                owner="alice",
                strategy_key="usr__alice__strategy_1",
                event_type="recovered",
                rule="max_drawdown",
                message="drawdown back to safe range",
                detail={"threshold": 0.2, "actual": 0.1},
                ts_utc="2026-01-01T01:00:00+00:00",
            )

            triggered_rows = store.list_risk_events(owner="alice", event_type="triggered", limit=10)
            self.assertEqual(len(triggered_rows), 1)
            self.assertEqual(triggered_rows[0]["rule"], "max_drawdown")

            ranged_rows = store.list_risk_events(
                owner="alice",
                start_ts="2026-01-01T00:30:00+00:00",
                end_ts="2026-01-01T01:30:00+00:00",
                limit=10,
            )
            self.assertEqual(len(ranged_rows), 1)
            self.assertEqual(ranged_rows[0]["eventType"], "recovered")

            page1 = store.list_risk_events(owner="alice", limit=1)
            self.assertEqual(len(page1), 1)
            cursor_id = page1[0]["id"]
            page2 = store.list_risk_events(owner="alice", cursor_id=cursor_id, limit=10)
            self.assertEqual(len(page2), 1)
            self.assertTrue(all(int(row["id"]) < int(cursor_id) for row in page2))

    def test_user_preferences_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            self.assertIsNone(store.get_user_preferences("alice"))
            store.upsert_user_preferences(
                "alice",
                {
                    "theme": "light",
                    "language": "en",
                    "selectedLiveStrategyId": "strategy_1",
                    "logsFilters": {"view": "audit"},
                },
            )
            row = store.get_user_preferences("alice")
            self.assertIsNotNone(row)
            prefs = (row or {}).get("preferences") or {}
            self.assertEqual(str(prefs.get("theme")), "light")
            self.assertEqual(str(prefs.get("language")), "en")
            self.assertEqual(str(prefs.get("selectedLiveStrategyId")), "strategy_1")
            self.assertEqual(str(((prefs.get("logsFilters") or {}).get("view"))), "audit")

            store.upsert_user_preferences("alice", {"theme": "dark"})
            row2 = store.get_user_preferences("alice")
            self.assertIsNotNone(row2)
            prefs2 = (row2 or {}).get("preferences") or {}
            self.assertEqual(str(prefs2.get("theme")), "dark")

    def test_account_security_events_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.append_account_security_event(
                owner="alice",
                event_type="login_success",
                severity="info",
                message="login succeeded",
                detail={"client_ip": "127.0.0.1"},
                ts_utc="2026-03-05T00:00:00+00:00",
            )
            store.append_account_security_event(
                owner="alice",
                event_type="login_failed",
                severity="warn",
                message="invalid credential",
                detail={"client_ip": "127.0.0.1"},
                ts_utc="2026-03-05T00:01:00+00:00",
            )

            rows = store.list_account_security_events(owner="alice", limit=10)
            self.assertEqual(len(rows), 2)
            self.assertEqual(str(rows[0]["eventType"]), "login_failed")
            self.assertEqual(str(rows[1]["eventType"]), "login_success")

            failed_rows = store.list_account_security_events(owner="alice", event_type="login_failed", limit=10)
            self.assertEqual(len(failed_rows), 1)
            self.assertEqual(str(failed_rows[0]["severity"]), "warn")

    def test_api_tokens_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            row = store.create_api_token(
                owner="alice",
                token_name="ci-token",
                token_prefix="qat_abc",
                token_hash="hash_1",
                scopes=["read", "write", "read"],
                expires_at="2099-01-01T00:00:00+00:00",
                created_by="admin",
            )
            self.assertGreater(int(row.get("id") or 0), 0)
            self.assertEqual(str(row.get("owner")), "alice")
            self.assertEqual(row.get("scopes"), ["read", "write"])

            listed = store.list_api_tokens(owner="alice", include_revoked=False, limit=10)
            self.assertEqual(len(listed), 1)
            token_id = int(listed[0]["id"])
            self.assertEqual(str(listed[0]["tokenPrefix"]), "qat_abc")

            active = store.get_active_api_token_by_hash(token_hash="hash_1", now_ts="2026-03-05T00:00:00+00:00")
            self.assertIsNotNone(active)
            self.assertEqual(str((active or {}).get("tokenName")), "ci-token")

            store.touch_api_token_last_used(token_id, last_used_at="2026-03-05T00:01:00+00:00")
            listed_after_touch = store.list_api_tokens(owner="alice", include_revoked=False, limit=10)
            self.assertEqual(str(listed_after_touch[0]["lastUsedAt"]), "2026-03-05T00:01:00+00:00")

            store.revoke_api_token(token_id, revoked_at="2026-03-05T00:02:00+00:00", revoked_by="admin")
            listed_active = store.list_api_tokens(owner="alice", include_revoked=False, limit=10)
            self.assertEqual(len(listed_active), 0)

            listed_all = store.list_api_tokens(owner="alice", include_revoked=True, limit=10)
            self.assertEqual(len(listed_all), 1)
            self.assertEqual(str(listed_all[0]["revokedBy"]), "admin")
            self.assertFalse(bool((store.get_active_api_token_by_hash(token_hash="hash_1", now_ts="2026-03-05T00:03:00+00:00"))))

    def test_rbac_roles_permissions_and_bindings_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            role_codes = {str(item.get("roleCode") or "") for item in store.list_roles()}
            permission_codes = {str(item.get("permissionCode") or "") for item in store.list_permissions()}
            self.assertIn("admin", role_codes)
            self.assertIn("auditor", role_codes)
            self.assertIn("audit.read.all", permission_codes)
            self.assertIn("risk.write", permission_codes)

            store.ensure_user("alice", role="user", display_name="alice")
            roles_initial = store.list_user_roles("alice")
            self.assertIn("user", roles_initial)
            self.assertTrue(store.user_has_permission("alice", "strategy.execute"))
            self.assertTrue(store.user_has_permission("alice", "risk.write"))
            self.assertFalse(store.user_has_permission("alice", "audit.read.all"))

            roles_updated = store.replace_user_roles("alice", ["auditor"])
            self.assertEqual(roles_updated, ["auditor"])
            self.assertEqual(store.list_user_roles("alice"), ["auditor"])
            self.assertTrue(store.user_has_permission("alice", "audit.read.all"))
            self.assertTrue(store.user_has_permission("alice", "security.read.all"))
            self.assertFalse(store.user_has_permission("alice", "strategy.execute"))

            with self.assertRaises(ValueError):
                store.replace_user_roles("alice", ["unknown_role"])

    def test_owner_user_id_is_populated_for_owner_scoped_tables(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            strategy_key = "usr__alice__strategy_owner_fk"
            store.upsert_strategy(
                strategy_key,
                "alice",
                {
                    "id": "strategy_owner_fk",
                    "name": "owner-fk",
                    "status": "stopped",
                    "createdAt": "2026-03-05T00:00:00+00:00",
                    "updatedAt": "2026-03-05T00:00:00+00:00",
                    "owner": "alice",
                    "config": {"symbols": ["BTCUSDT"], "params": {}},
                },
            )
            store.append_audit_log(
                owner="alice",
                action="strategy.create",
                entity="strategy",
                entity_id="strategy_owner_fk",
                detail={},
            )
            store.append_runtime_log(
                owner="alice",
                log_type="system",
                level="info",
                source="unit",
                message="owner fk test",
                strategy_id="strategy_owner_fk",
                ts_utc="2026-03-05T00:01:00+00:00",
            )
            store.create_api_token(
                owner="alice",
                token_name="owner-fk-token",
                token_prefix="qat_ownerfk",
                token_hash="owner_fk_hash",
                scopes=["read"],
                created_by="admin",
            )
            store.upsert_user_preferences("alice", {"theme": "dark"})

            with store._connect() as conn:
                user_row = conn.execute("SELECT id FROM users WHERE username = 'alice' LIMIT 1").fetchone()
                self.assertIsNotNone(user_row)
                owner_user_id = int(user_row["id"])

                checks = [
                    ("strategies", "strategy_key = ?", (strategy_key,)),
                    ("audit_logs", "owner = ? AND action = ?", ("alice", "strategy.create")),
                    ("runtime_logs", "owner = ? AND message = ?", ("alice", "owner fk test")),
                    ("api_tokens", "token_hash = ?", ("owner_fk_hash",)),
                    ("user_preferences", "owner = ?", ("alice",)),
                ]
                for table_name, where_sql, params in checks:
                    row = conn.execute(
                        f"SELECT owner_user_id FROM {table_name} WHERE {where_sql} LIMIT 1",
                        params,
                    ).fetchone()
                    self.assertIsNotNone(row, msg=f"missing row in {table_name}")
                    self.assertEqual(
                        int(row["owner_user_id"] or 0),
                        owner_user_id,
                        msg=f"owner_user_id mismatch in {table_name}",
                    )

    def test_runtime_log_level_constraint_rejects_invalid_level(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            with store._connect() as conn:
                with self.assertRaises(sqlite3.IntegrityError):
                    conn.execute(
                        """
                        INSERT INTO runtime_logs (
                            ts_utc, owner, log_type, level, source, message, strategy_id, backtest_id, detail_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            "2026-03-05T00:00:00+00:00",
                            "guest",
                            "system",
                            "fatal",
                            "unit",
                            "invalid level",
                            "",
                            "",
                            "{}",
                        ),
                    )

    def test_api_token_active_name_unique_per_owner(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            first = store.create_api_token(
                owner="alice",
                token_name="trading-main",
                token_prefix="qat_tok_a",
                token_hash="hash_a",
                scopes=["read"],
                created_by="admin",
            )
            self.assertGreater(int(first.get("id") or 0), 0)
            with self.assertRaises(sqlite3.IntegrityError):
                store.create_api_token(
                    owner="alice",
                    token_name="trading-main",
                    token_prefix="qat_tok_b",
                    token_hash="hash_b",
                    scopes=["read"],
                    created_by="admin",
                )
            store.revoke_api_token(int(first.get("id") or 0), revoked_by="admin")
            second = store.create_api_token(
                owner="alice",
                token_name="trading-main",
                token_prefix="qat_tok_c",
                token_hash="hash_c",
                scopes=["trade"],
                created_by="admin",
            )
            self.assertGreater(int(second.get("id") or 0), 0)


if __name__ == "__main__":
    unittest.main()
