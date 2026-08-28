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

    def test_qmt_order_id_can_be_reused_on_a_later_trading_day(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "bridge.db"
            bootstrap_store = bridge.OrderStore(str(db_path))
            bootstrap_store.close()
            legacy_connection = sqlite3.connect(str(db_path))
            legacy_connection.executescript(
                """
                DROP INDEX IF EXISTS ix_orders_qmt_id;
                CREATE UNIQUE INDEX uq_orders_qmt_id
                ON orders(account_id, qmt_order_id)
                WHERE qmt_order_id IS NOT NULL;
                """
            )
            legacy_connection.commit()
            legacy_connection.close()

            store = bridge.OrderStore(str(db_path))
            try:
                old_payload = {
                    "client_order_id": "CLIENT-OLD-DAY",
                    "account_id": "SIM-STOCK-001",
                    "instrument": "600000.SH",
                    "side": "BUY",
                    "quantity": 100,
                    "price_type": "LIMIT",
                    "limit_price": "10.00",
                    "remark": "old-day",
                }
                new_payload = dict(old_payload)
                new_payload.update(
                    {
                        "client_order_id": "CLIENT-NEW-DAY",
                        "instrument": "000001.SZ",
                        "limit_price": "11.00",
                        "remark": "new-day",
                    }
                )
                store.insert_order(
                    "REQUEST-OLD-DAY",
                    old_payload,
                    "QAWIREOLD",
                    "QAWIREOLD:old-day",
                    bridge._order_payload_hash(
                        old_payload["account_id"],
                        old_payload["instrument"],
                        old_payload["side"],
                        old_payload["quantity"],
                        old_payload["price_type"],
                        old_payload["limit_price"],
                        old_payload["remark"],
                    ),
                )
                store.update_order(
                    "CLIENT-OLD-DAY",
                    status="FILLED",
                    qmt_order_id="REUSED-QMT-ID",
                )
                store.conn.execute(
                    "UPDATE orders SET created_at=? WHERE client_order_id=?",
                    ("2026-08-27T01:00:00.000000Z", "CLIENT-OLD-DAY"),
                )
                store.conn.commit()

                store.insert_order(
                    "REQUEST-NEW-DAY",
                    new_payload,
                    "QAWIRENEW",
                    "QAWIRENEW:new-day",
                    bridge._order_payload_hash(
                        new_payload["account_id"],
                        new_payload["instrument"],
                        new_payload["side"],
                        new_payload["quantity"],
                        new_payload["price_type"],
                        new_payload["limit_price"],
                        new_payload["remark"],
                    ),
                )
                store.update_order(
                    "CLIENT-NEW-DAY",
                    status="SUBMITTED",
                    qmt_order_id="REUSED-QMT-ID",
                )

                rows = store.conn.execute(
                    "SELECT client_order_id FROM orders "
                    "WHERE account_id=? AND qmt_order_id=? ORDER BY created_at",
                    ("SIM-STOCK-001", "REUSED-QMT-ID"),
                ).fetchall()
                latest = store.get_order_by_qmt_id(
                    "SIM-STOCK-001", "REUSED-QMT-ID"
                )
                runtime = object.__new__(bridge.BridgeRuntime)
                runtime.store = store
                trade_owner = runtime._order_for_trade_raw(
                    {
                        "m_strOrderSysID": "REUSED-QMT-ID",
                        "m_strRemark": "QAWIRENEW:new-day",
                    },
                    "SIM-STOCK-001",
                )
                old_trade_owner = runtime._order_for_trade_raw(
                    {
                        "m_strOrderSysID": "REUSED-QMT-ID",
                        "m_strRemark": "QAWIREOLD:old-day",
                    },
                    "SIM-STOCK-001",
                )
                fallback_trade_owner = runtime._order_for_trade_raw(
                    {
                        "m_strOrderSysID": "REUSED-QMT-ID",
                        "m_strRemark": "",
                    },
                    "SIM-STOCK-001",
                )
                indexes = {
                    row[1]: row[2]
                    for row in store.conn.execute("PRAGMA index_list(orders)")
                }

                self.assertEqual(
                    [row["client_order_id"] for row in rows],
                    ["CLIENT-OLD-DAY", "CLIENT-NEW-DAY"],
                )
                self.assertEqual(latest["client_order_id"], "CLIENT-NEW-DAY")
                self.assertEqual(
                    trade_owner["client_order_id"], "CLIENT-NEW-DAY"
                )
                self.assertEqual(
                    old_trade_owner["client_order_id"], "CLIENT-OLD-DAY"
                )
                self.assertEqual(
                    fallback_trade_owner["client_order_id"], "CLIENT-NEW-DAY"
                )
                self.assertNotIn("uq_orders_qmt_id", indexes)
                self.assertEqual(indexes["ix_orders_qmt_id"], 0)

                store.conn.execute(
                    "UPDATE orders SET created_at=? WHERE client_order_id=?",
                    ("2026-08-27T01:00:00.000000Z", "CLIENT-NEW-DAY"),
                )
                store.conn.commit()
                tied_latest = store.get_order_by_qmt_id(
                    "SIM-STOCK-001", "REUSED-QMT-ID"
                )
                self.assertEqual(
                    tied_latest["client_order_id"], "CLIENT-NEW-DAY"
                )
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
