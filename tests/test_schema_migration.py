from pathlib import Path
import sqlite3
import tempfile
import unittest

from qmt_side import qmt_adapter_qmt as bridge


class OrderStoreMigrationTests(unittest.TestCase):
    def test_old_orders_table_is_backfilled_with_payload_hash(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "bridge.db"
            connection = sqlite3.connect(str(db_path))
            connection.executescript(
                """
                CREATE TABLE orders (
                    client_order_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL UNIQUE,
                    wire_order_tag TEXT NOT NULL UNIQUE,
                    account_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity INTEGER NOT NULL,
                    price_type TEXT NOT NULL,
                    limit_price TEXT,
                    user_remark TEXT,
                    qmt_remark TEXT NOT NULL,
                    qmt_order_id TEXT,
                    status TEXT NOT NULL,
                    raw_json TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
            connection.execute(
                "INSERT INTO orders VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "CLIENT-OLD-0001",
                    "REQUEST-OLD-0001",
                    "QAWIREOLD",
                    "SIM-STOCK-001",
                    "600000.SH",
                    "BUY",
                    100,
                    "LIMIT",
                    "10.2500",
                    "old-order",
                    "QAWIREOLD:old-order",
                    "QMT-OLD-0001",
                    "SUBMITTED",
                    None,
                    "2026-08-19T00:00:00.000000Z",
                    "2026-08-19T00:00:01.000000Z",
                ),
            )
            connection.commit()
            connection.close()

            store = bridge.OrderStore(str(db_path))
            try:
                columns = {
                    row[1]
                    for row in store.conn.execute("PRAGMA table_info(orders)")
                }
                row = store.get_order("CLIENT-OLD-0001")
                self.assertIn("payload_hash", columns)
                self.assertIn("order_kind", columns)
                self.assertIn("metadata_json", columns)
                tables = {
                    item[0]
                    for item in store.conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    )
                }
                self.assertIn("algo_orders", tables)
                self.assertIn("algo_children", tables)
                self.assertIn("trades", tables)
                self.assertEqual(len(row["payload_hash"]), 64)
                self.assertEqual(row["order_kind"], "STOCK")
                self.assertEqual(
                    row["payload_hash"],
                    bridge._order_payload_hash(
                        "SIM-STOCK-001",
                        "600000.SH",
                        "BUY",
                        100,
                        "LIMIT",
                        "10.25",
                        "old-order",
                    ),
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
