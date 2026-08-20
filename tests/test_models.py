import unittest

from qmt_adapter import AlgoOrderRequest, OrderRequest, ValidationError


class OrderRequestTests(unittest.TestCase):
    def test_generated_client_order_id_is_stable(self):
        order = OrderRequest(
            account_id="SIM001",
            instrument="600000.SH",
            side="BUY",
            quantity=100,
        )

        first = order.to_payload()["client_order_id"]
        second = order.to_payload()["client_order_id"]

        self.assertTrue(first)
        self.assertEqual(first, second)

    def test_limit_buy_payload(self):
        payload = OrderRequest(
            account_id="SIM001",
            instrument="600000.sh",
            side="buy",
            quantity=100,
            price_type="limit",
            limit_price="10.25",
            remark="test",
            client_order_id="CLIENT-1",
        ).to_payload()

        self.assertEqual(payload["instrument"], "600000.SH")
        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["quantity"], 100)
        self.assertEqual(payload["limit_price"], "10.25")
        self.assertEqual(payload["client_order_id"], "CLIENT-1")

    def test_limit_price_is_required(self):
        with self.assertRaises(ValidationError):
            OrderRequest(
                account_id="SIM001",
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                price_type="LIMIT",
            ).to_payload()

    def test_only_stock_cash_shares_are_accepted(self):
        with self.assertRaises(ValidationError):
            OrderRequest(
                account_id="SIM001",
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                business_type="CREDIT",
            ).to_payload()

    def test_blank_client_order_id_is_rejected(self):
        with self.assertRaises(ValidationError):
            OrderRequest(
                account_id="SIM001",
                instrument="600000.SH",
                side="BUY",
                quantity=100,
                client_order_id=" ",
            ).to_payload()

    def test_native_market_order_defaults_protection_price_to_zero(self):
        payload = OrderRequest(
            account_id="SIM001",
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="MARKET_SH_CONVERT_5_CANCEL",
        ).to_payload()

        self.assertEqual(payload["price_type"], "MARKET_SH_CONVERT_5_CANCEL")
        self.assertEqual(payload["limit_price"], "0")

    def test_native_market_order_accepts_protection_price(self):
        payload = OrderRequest(
            account_id="SIM001",
            instrument="600000.SH",
            side="BUY",
            quantity=100,
            price_type="MARKET_PEER_PRICE_FIRST",
            limit_price="18.2500",
        ).to_payload()

        self.assertEqual(payload["limit_price"], "18.2500")

    def test_native_market_order_rejects_wrong_exchange(self):
        invalid_cases = (
            ("600000.SH", "MARKET_SZ_INSTBUSI_RESTCANCEL"),
            ("000001.SZ", "MARKET_SH_CONVERT_5_CANCEL"),
            ("430047.BJ", "MARKET_SZ_FULL_OR_CANCEL"),
        )
        for instrument, price_type in invalid_cases:
            with self.subTest(instrument=instrument, price_type=price_type):
                with self.assertRaises(ValidationError):
                    OrderRequest(
                        account_id="SIM001",
                        instrument=instrument,
                        side="BUY",
                        quantity=100,
                        price_type=price_type,
                    ).to_payload()

    def test_native_market_order_rejects_invalid_protection_price(self):
        for value in ("-0.01", "10000", "NaN"):
            with self.subTest(value=value):
                with self.assertRaises(ValidationError):
                    OrderRequest(
                        account_id="SIM001",
                        instrument="600000.SH",
                        side="BUY",
                        quantity=100,
                        price_type="MARKET_MINE_PRICE_FIRST",
                        limit_price=value,
                    ).to_payload()


class AlgoOrderRequestTests(unittest.TestCase):
    def test_target_amount_payload_and_stable_parent_id(self):
        order = AlgoOrderRequest(
            account_id="SIM001",
            instrument="601919.sh",
            side="buy",
            target_amount="10000000",
        )

        first = order.to_payload()
        second = order.to_payload()

        self.assertEqual(first["instrument"], "601919.SH")
        self.assertEqual(first["target_amount"], "10000000")
        self.assertEqual(first["algo_order_id"], second["algo_order_id"])

    def test_exactly_one_target_is_required(self):
        invalid = (
            AlgoOrderRequest(
                account_id="SIM001", instrument="601919.SH", side="BUY"
            ),
            AlgoOrderRequest(
                account_id="SIM001",
                instrument="601919.SH",
                side="BUY",
                target_amount="100000",
                quantity=100,
            ),
        )
        for order in invalid:
            with self.subTest(order=order):
                with self.assertRaises(ValidationError):
                    order.to_payload()

    def test_reserved_algorithm_identifier_is_accepted_by_client_model(self):
        payload = AlgoOrderRequest(
            account_id="SIM001",
            instrument="601919.SH",
            side="BUY",
            quantity=100,
            algorithm="TWAP",
        ).to_payload()

        self.assertEqual(payload["algorithm"], "TWAP")

if __name__ == "__main__":
    unittest.main()
