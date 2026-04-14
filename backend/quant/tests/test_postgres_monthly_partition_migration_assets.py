import unittest
from pathlib import Path


class PostgresMonthlyPartitionMigrationAssetsTests(unittest.TestCase):
    def test_v20_migration_registered(self):
        src = (Path(__file__).resolve().parents[1] / "postgres_store.py").read_text(encoding="utf-8")
        self.assertIn(
            '(20, "add_monthly_time_partitions", self._migration_v20_add_monthly_time_partitions)',
            src,
        )

    def test_v20_contains_partition_routing_assets(self):
        src = (Path(__file__).resolve().parents[1] / "postgres_store.py").read_text(encoding="utf-8")
        self.assertIn("def _migration_v20_add_monthly_time_partitions", src)
        self.assertIn("quant_ensure_monthly_partition", src)
        self.assertIn("INHERITS (", src)
        self.assertIn("quant_route_audit_logs_monthly_partition", src)
        self.assertIn("quant_route_risk_events_monthly_partition", src)
        self.assertIn("quant_route_market_ticks_monthly_partition", src)
        self.assertIn("quant_route_market_klines_monthly_partition", src)
        self.assertIn("trg_route_audit_logs_monthly_partition", src)
        self.assertIn("trg_route_risk_events_monthly_partition", src)
        self.assertIn("trg_route_market_ticks_monthly_partition", src)
        self.assertIn("trg_route_market_klines_monthly_partition", src)


if __name__ == "__main__":
    unittest.main()

