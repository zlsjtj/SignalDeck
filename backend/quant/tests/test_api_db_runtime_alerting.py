import unittest
from copy import deepcopy

import api_server


class DbRuntimeAlertingTests(unittest.TestCase):
    def setUp(self):
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_db_alert_threshold = api_server._DB_ALERT_THRESHOLD
        self._orig_db_alert_cooldown = api_server._DB_ALERT_COOLDOWN_SECONDS
        self._orig_db_alert_webhook = api_server._DB_ALERT_WEBHOOK_URL
        self._orig_db_alert_timeout = api_server._DB_ALERT_TIMEOUT_SECONDS
        self._orig_db_runtime_stats = deepcopy(api_server._DB_RUNTIME_STATS)
        self._orig_db_runtime_alert_state = deepcopy(api_server._DB_RUNTIME_ALERT_STATE)
        self._orig_emit_db_alert = api_server._emit_db_alert

        api_server._DB_ENABLED = True
        api_server._DB_READY = True
        api_server._DB_INIT_ERROR = ""
        api_server._DB_ALERT_THRESHOLD = 2
        api_server._DB_ALERT_COOLDOWN_SECONDS = 3600
        api_server._DB_ALERT_WEBHOOK_URL = "http://example.invalid/hook"
        api_server._DB_ALERT_TIMEOUT_SECONDS = 1.0
        api_server._DB_RUNTIME_STATS = {
            "strategy_write_failures": 0,
            "backtest_write_failures": 0,
            "risk_write_failures": 0,
            "audit_write_failures": 0,
            "audit_read_failures": 0,
            "write_ops_total": 0,
            "write_ops_slow_total": 0,
            "read_ops_total": 0,
            "read_ops_slow_total": 0,
            "lock_contention_total": 0,
            "lock_wait_ms_total": 0.0,
            "last_slow_kind": "",
            "last_slow_ms": 0.0,
            "last_slow_at": "",
            "last_error": "",
            "last_error_at": "",
            "last_write_kind": "",
            "last_write_ms": 0.0,
            "last_write_at": "",
            "max_write_ms": 0.0,
        }
        api_server._DB_RUNTIME_ALERT_STATE = {
            "last_alert_at": "",
            "last_alert_total": 0,
            "last_alert_error": "",
            "last_alert_epoch": 0.0,
            "last_webhook_status": "",
        }

        self._alerts = []

        def _fake_emit(event: str, severity: str, message: str, detail=None):
            self._alerts.append(
                {
                    "event": event,
                    "severity": severity,
                    "message": message,
                    "detail": detail or {},
                }
            )
            return True, ""

        api_server._emit_db_alert = _fake_emit

    def tearDown(self):
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._DB_ALERT_THRESHOLD = self._orig_db_alert_threshold
        api_server._DB_ALERT_COOLDOWN_SECONDS = self._orig_db_alert_cooldown
        api_server._DB_ALERT_WEBHOOK_URL = self._orig_db_alert_webhook
        api_server._DB_ALERT_TIMEOUT_SECONDS = self._orig_db_alert_timeout
        api_server._DB_RUNTIME_STATS = self._orig_db_runtime_stats
        api_server._DB_RUNTIME_ALERT_STATE = self._orig_db_runtime_alert_state
        api_server._emit_db_alert = self._orig_emit_db_alert

    def test_runtime_failure_alert_threshold_and_cooldown(self):
        api_server._record_db_runtime_failure("strategy_write", RuntimeError("db fail 1"))
        self.assertEqual(len(self._alerts), 0)
        api_server._record_db_runtime_failure("strategy_write", RuntimeError("db fail 2"))
        self.assertEqual(len(self._alerts), 1)
        api_server._record_db_runtime_failure("strategy_write", RuntimeError("db fail 3"))
        self.assertEqual(len(self._alerts), 1)

        self.assertEqual(api_server._db_runtime_failures_total(), 3)
        self.assertEqual(self._alerts[0]["event"], "db_runtime_persistence_failure")
        self.assertEqual(self._alerts[0]["severity"], "critical")
        self.assertEqual(self._alerts[0]["detail"].get("failure_total"), 2)

    def test_health_and_metrics_reflect_runtime_failures(self):
        api_server._record_db_runtime_failure("strategy_write", RuntimeError("db fail"))
        health_payload = api_server.health()
        self.assertEqual(health_payload.get("db"), "degraded")
        self.assertEqual(health_payload.get("db_runtime_failures"), 1)
        self.assertIsInstance(health_payload.get("db_alerting"), dict)

        metrics_resp = api_server.metrics()
        self.assertEqual(getattr(metrics_resp, "status_code", 200), 200)
        body = metrics_resp.body.decode("utf-8")
        self.assertIn("quant_db_runtime_failures_total 1", body)
        self.assertIn('quant_db_runtime_failure_total{kind="strategy_write"} 1', body)
        self.assertIn('quant_db_status{state="degraded"} 1', body)

    def test_health_and_metrics_include_write_latency(self):
        api_server._record_db_write_success("strategy_write", 12.34)
        health_payload = api_server.health()
        self.assertEqual(health_payload.get("db_last_write_kind"), "strategy_write")
        self.assertGreater(float(health_payload.get("db_last_write_ms", 0.0)), 0.0)
        self.assertTrue(bool(health_payload.get("db_last_write_at")))
        self.assertIn("db_storage", health_payload)

        metrics_resp = api_server.metrics()
        body = metrics_resp.body.decode("utf-8")
        self.assertIn("quant_db_last_write_duration_ms", body)
        self.assertIn("quant_db_max_write_duration_ms", body)
        self.assertIn("quant_db_size_bytes", body)

    def test_health_and_metrics_include_slow_and_lock_observability(self):
        api_server._record_db_write_success("strategy_write", api_server._DB_SLOW_OP_THRESHOLD_MS + 8.0)
        api_server._record_db_read_success("audit_read", api_server._DB_SLOW_OP_THRESHOLD_MS + 5.0)
        api_server._record_db_runtime_failure(
            "strategy_write",
            RuntimeError("database is locked"),
            elapsed_ms=321.5,
        )

        health_payload = api_server.health()
        self.assertGreaterEqual(int(health_payload.get("db_write_ops_total", 0)), 2)
        self.assertGreaterEqual(int(health_payload.get("db_write_ops_slow_total", 0)), 1)
        self.assertGreaterEqual(int(health_payload.get("db_read_ops_total", 0)), 1)
        self.assertGreaterEqual(int(health_payload.get("db_read_ops_slow_total", 0)), 1)
        self.assertGreaterEqual(int(health_payload.get("db_lock_contention_total", 0)), 1)
        self.assertGreater(float(health_payload.get("db_lock_wait_ms_total", 0.0)), 0.0)
        self.assertEqual(float(health_payload.get("db_slow_op_threshold_ms", 0.0)), float(api_server._DB_SLOW_OP_THRESHOLD_MS))

        metrics_resp = api_server.metrics()
        body = metrics_resp.body.decode("utf-8")
        self.assertIn("quant_db_write_ops_total", body)
        self.assertIn("quant_db_write_ops_slow_total", body)
        self.assertIn("quant_db_read_ops_total", body)
        self.assertIn("quant_db_read_ops_slow_total", body)
        self.assertIn("quant_db_lock_contention_total", body)
        self.assertIn("quant_db_lock_wait_ms_total", body)
        self.assertIn("quant_db_slow_op_threshold_ms", body)
        self.assertIn("quant_db_last_slow_duration_ms", body)


if __name__ == "__main__":
    unittest.main()
