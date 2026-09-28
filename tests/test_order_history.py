"""委托日期过滤与跨表历史清理的持久化测试。"""

from pathlib import Path
import tempfile
import unittest

from qmt_side import qmt_adapter_qmt as bridge


ACCOUNT_ID = "SIM-STOCK-001"
OLD = "2026-09-27T15:59:59.000000Z"
NEW = "2026-09-27T16:00:00.000000Z"


class OrderHistoryTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = bridge.OrderStore(str(Path(self.temp_dir.name) / "bridge.db"))

    def tearDown(self):
        self.store.close()
        self.temp_dir.cleanup()

    def insert_order(self, order_id, created_at, status="SUBMITTED"):
        self.store.insert_order(
            "request-" + order_id,
            {
                "client_order_id": order_id,
                "account_id": ACCOUNT_ID,
                "instrument": "600000.SH",
                "side": "BUY",
                "quantity": 100,
                "price_type": "LIMIT",
                "limit_price": "10.00",
            },
            "wire-" + order_id,
            "remark-" + order_id,
            "hash-" + order_id,
        )
        self.store.conn.execute(
            "UPDATE orders SET created_at=?,updated_at=?,status=? "
            "WHERE client_order_id=?",
            (created_at, created_at, status, order_id),
        )
        self.store.conn.commit()

    def insert_trade(self, trade_id, order_id, created_at):
        self.store.conn.execute(
            "INSERT INTO trades(trade_key,account_id,trade_id,qmt_order_id,"
            "client_order_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
            (trade_id, ACCOUNT_ID, trade_id, trade_id, order_id,
             created_at, created_at),
        )
        self.store.conn.commit()

    def test_china_date_bounds_and_account_filter(self):
        self.insert_order("old", OLD)
        self.insert_order("new", NEW)
        self.insert_order("other", NEW)
        self.store.conn.execute(
            "UPDATE orders SET account_id='OTHER' WHERE client_order_id='other'"
        )
        self.store.conn.commit()
        start = bridge._china_date_utc_bound("2026-09-28", "start_date")
        end = bridge._china_date_utc_bound("2026-09-29", "end_date")
        self.assertEqual(
            [row["client_order_id"] for row in
             self.store.list_orders(ACCOUNT_ID, start, end)],
            ["new"],
        )
        self.assertEqual(
            [row["client_order_id"] for row in
             self.store.list_orders(ACCOUNT_ID, end_utc=start)],
            ["old"],
        )
        with self.assertRaises(bridge.BridgeError):
            bridge._china_date_utc_bound("2026-02-30", "start_date")

    def test_cleanup_removes_active_old_records_and_cross_date_children(self):
        self.insert_order("old-active", OLD, "SUBMITTED")
        self.insert_order("new-independent", NEW, "SUBMITTED")
        self.insert_order("new-child", NEW, "SUBMITTED")
        self.insert_trade("old-trade", "old-active", OLD)
        self.insert_trade("new-child-trade", "new-child", NEW)
        self.insert_trade("new-independent-trade", "new-independent", NEW)
        self.store.conn.execute(
            "INSERT INTO algo_orders(algo_order_id,request_id,payload_hash,"
            "account_id,instrument,side,algorithm,resolved_quantity,status,"
            "params_json,created_at,updated_at) "
            "VALUES('old-algo','old-algo-request','hash',?,?,?, ?,100,"
            "'PLACING','{}',?,?)",
            (ACCOUNT_ID, "600000.SH", "BUY", "BOOK_LIQUIDITY_WEIGHTED", OLD, OLD),
        )
        self.store.conn.execute(
            "INSERT INTO algo_children(algo_order_id,client_order_id,attempt,"
            "child_index,price,quantity,source,created_at) "
            "VALUES('old-algo','new-child',0,1,'10.00',100,'test',?)",
            (NEW,),
        )
        self.store.conn.commit()

        cutoff = bridge._china_date_utc_bound("2026-09-28", "before_date")
        counts, wire_tags = self.store.cleanup_history(cutoff)

        self.assertEqual(
            counts,
            {"orders": 2, "trades": 2, "algo_orders": 1,
             "algo_children": 1},
        )
        self.assertEqual(set(wire_tags), {"wire-old-active", "wire-new-child"})
        self.assertEqual(
            [row["client_order_id"] for row in self.store.list_orders()],
            ["new-independent"],
        )
        self.assertEqual(
            [row[0] for row in self.store.conn.execute(
                "SELECT trade_key FROM trades").fetchall()],
            ["new-independent-trade"],
        )
        self.assertEqual(self.store.cleanup_history(cutoff)[0]["orders"], 0)


if __name__ == "__main__":
    unittest.main()
