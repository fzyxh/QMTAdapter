import decimal
import unittest

from qmt_side import qmt_adapter_qmt as bridge


def depth(levels):
    return {
        "instrument": "601919.SH",
        "book_side": "ASK",
        "volume_unit": "LOTS",
        "lot_size": 100,
        "price_limits": {"upper_limit": "20.00", "lower_limit": "5.00"},
        "price_cage": {
            "buy_reference": "10.00",
            "buy_maximum": "20.00",
            "sell_reference": "9.99",
            "sell_minimum": "5.00",
            "price_tick": "0.01",
        },
        "levels": [
            {
                "level": index + 1,
                "price": str(price),
                "volume_lots": lots,
                "visible_quantity": lots * 100,
            }
            for index, (price, lots) in enumerate(levels)
        ],
    }


class BookLiquidityPlannerTests(unittest.TestCase):
    def params(self, **overrides):
        values = dict(bridge.BOOK_LIQUIDITY_DEFAULTS)
        values.update(overrides)
        return bridge._normalize_book_params(values)

    def test_top_three_weights_use_original_target(self):
        children = bridge._plan_book_quantity(
            10000,
            depth(((10.00, 50), (10.01, 30), (10.02, 20))),
            "BUY",
            self.params(big_order_threshold="1"),
        )
        by_source = {}
        for child in children:
            by_source[child["source"]] = (
                by_source.get(child["source"], 0) + child["quantity"]
            )

        self.assertEqual(by_source["DEPTH_1"], 5000)
        self.assertEqual(by_source["DEPTH_2"], 3000)
        self.assertEqual(by_source["DEPTH_3"], 2000)
        self.assertEqual(sum(item["quantity"] for item in children), 10000)

    def test_max_child_applies_to_chase_and_quantity_is_conserved(self):
        children = bridge._plan_book_quantity(
            200000,
            depth(
                (
                    (10.00, 1),
                    (10.01, 1),
                    (10.02, 1),
                    (10.03, 1),
                    (10.04, 1),
                )
            ),
            "BUY",
            self.params(big_order_threshold="1", max_child_notional="500000"),
        )

        self.assertEqual(sum(item["quantity"] for item in children), 200000)
        self.assertTrue(any(item["source"] == "CHASE" for item in children))
        self.assertTrue(
            all(
                decimal.Decimal(item["estimated_notional"])
                <= decimal.Decimal("500000")
                for item in children
            )
        )

    def test_target_amount_never_exceeds_buy_budget(self):
        amount = decimal.Decimal("10000000")
        quantity, children = bridge._plan_book_target(
            str(amount),
            None,
            depth(
                (
                    (13.00, 1000),
                    (13.01, 1000),
                    (13.02, 1000),
                    (13.03, 1000),
                    (13.04, 1000),
                )
            ),
            "BUY",
            self.params(),
        )

        self.assertGreater(quantity, 0)
        self.assertEqual(sum(item["quantity"] for item in children), quantity)
        self.assertLessEqual(bridge._plan_notional(children), amount)

    def test_full_tick_volume_is_normalized_from_lots_to_shares(self):
        normalized = bridge._normalize_depth_tick(
            {
                "lastPrice": 10.0,
                "lastClose": 9.9,
                "askPrice": [10.0, 10.01],
                "askVol": [50, 30],
                "bidPrice": [9.99, 9.98],
                "bidVol": [20, 10],
            },
            "601919.SH",
            "BUY",
        )

        self.assertEqual(normalized["volume_unit"], "LOTS")
        self.assertEqual(normalized["levels"][0]["volume_lots"], 50)
        self.assertEqual(normalized["levels"][0]["visible_quantity"], 5000)
        self.assertEqual(len(normalized["quote"]["ask_levels"]), 5)
        self.assertEqual(len(normalized["quote"]["bid_levels"]), 5)
        self.assertEqual(normalized["quote"]["ask_levels"][0]["price"], "10")
        self.assertIsNone(normalized["quote"]["ask_levels"][4]["price"])
        self.assertEqual(normalized["last_price"], "10")
        self.assertEqual(normalized["last_close"], "9.9")

    def test_filled_and_terminal_status_are_derived_from_qmt_fields(self):
        raw = {
            "m_nOrderStatus": 56,
            "m_nVolumeTotalOriginal": 500,
            "m_nVolumeTraded": 500,
            "m_nVolumeTotal": 0,
        }

        self.assertEqual(bridge._derive_order_status(raw, "SUBMITTED"), "FILLED")
        self.assertEqual(bridge._order_filled_quantity(raw), 500)

    def test_stale_qmt_status_does_not_downgrade_cancel_pending(self):
        submitted = {
            "m_nOrderStatus": 50,
            "m_nVolumeTotalOriginal": 500,
            "m_nVolumeTraded": 0,
            "m_nVolumeTotal": 500,
        }
        partial = {
            "m_nOrderStatus": 55,
            "m_nVolumeTotalOriginal": 500,
            "m_nVolumeTraded": 100,
            "m_nVolumeTotal": 400,
        }
        canceled = {
            "m_nOrderStatus": 53,
            "m_nVolumeTotalOriginal": 500,
            "m_nVolumeTraded": 100,
            "m_nVolumeTotal": 0,
        }

        self.assertEqual(
            bridge._derive_order_status(submitted, "CANCEL_PENDING"),
            "CANCEL_PENDING",
        )
        self.assertEqual(
            bridge._derive_order_status(partial, "CANCEL_PENDING"),
            "CANCEL_PENDING",
        )
        self.assertEqual(
            bridge._derive_order_status(canceled, "CANCEL_PENDING"),
            "CANCELED",
        )

    def test_chase_price_is_clamped_to_daily_limit(self):
        market_depth = depth(((16.64, 1),))
        market_depth["price_limits"] = {
            "upper_limit": "16.64",
            "lower_limit": "13.62",
        }

        children = bridge._plan_book_quantity(
            1000,
            market_depth,
            "BUY",
            self.params(big_order_threshold="1"),
        )

        self.assertTrue(
            any(
                child["source"] == "CHASE_DAILY_LIMIT_CLAMPED"
                for child in children
            )
        )
        self.assertTrue(all(child["price"] == "16.64" for child in children))

    def test_chase_price_is_clamped_to_dynamic_price_cage(self):
        market_depth = depth(((10.00, 1),))
        market_depth["price_cage"]["buy_maximum"] = "10.01"

        children = bridge._plan_book_quantity(
            1000,
            market_depth,
            "BUY",
            self.params(big_order_threshold="1", chase_ticks=2),
        )

        self.assertTrue(
            any(
                child["source"] == "CHASE_PRICE_CAGE_CLAMPED"
                for child in children
            )
        )
        self.assertTrue(all(child["price"] <= "10.01" for child in children))

    def test_chase_uses_qmt_instrument_price_tick(self):
        market_depth = depth(((10.00, 1),))
        market_depth["price_cage"]["price_tick"] = "0.001"

        children = bridge._plan_book_quantity(
            1000,
            market_depth,
            "BUY",
            self.params(big_order_threshold="1", chase_ticks=2),
        )

        chase_children = [
            child for child in children if child["source"] == "CHASE"
        ]
        self.assertTrue(chase_children)
        self.assertTrue(all(child["price"] == "10.002" for child in chase_children))


if __name__ == "__main__":
    unittest.main()
