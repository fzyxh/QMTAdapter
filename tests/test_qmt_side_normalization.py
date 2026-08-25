import decimal
import sys
import unittest

from qmt_side import qmt_adapter_qmt as bridge


class OptionalThreeDecimalTextTests(unittest.TestCase):
    def test_normal_price_is_rounded_half_up(self):
        self.assertEqual(bridge._optional_three_decimal_text("10.2345"), "10.235")

    def test_non_finite_values_are_omitted(self):
        for value in (
            float("nan"),
            float("inf"),
            float("-inf"),
            decimal.Decimal("NaN"),
            decimal.Decimal("Infinity"),
            decimal.Decimal("-Infinity"),
        ):
            with self.subTest(value=value):
                self.assertIsNone(bridge._optional_three_decimal_text(value))

    def test_extreme_finite_values_are_omitted(self):
        for value in (sys.float_info.max, -sys.float_info.max):
            with self.subTest(value=value):
                self.assertIsNone(bridge._optional_three_decimal_text(value))

        with decimal.localcontext() as context:
            context.traps[decimal.InvalidOperation] = False
            self.assertIsNone(
                bridge._optional_three_decimal_text(sys.float_info.max)
            )

    def test_position_keeps_extreme_raw_price_for_diagnostics(self):
        raw = {
            "m_strInstrumentID": "131990",
            "m_strExchangeID": "SZ",
            "m_strInstrumentName": "national-standard-bond",
            "m_nVolume": 4420,
            "m_nCanUseVolume": 4420,
            "m_nFrozenVolume": 0,
            "m_dOpenPrice": 0.0,
            "m_dLastPrice": sys.float_info.max,
            "m_dMarketValue": float("inf"),
            "m_dPositionProfit": 0.0,
        }

        result = bridge._normalize_position(
            raw, "SIM-STOCK-001", include_raw=True
        )

        self.assertEqual(result["instrument"], "131990.SZ")
        self.assertIsNone(result["current_price"])
        self.assertIsNone(result["market_value"])
        self.assertEqual(result["raw"]["m_dLastPrice"], sys.float_info.max)


if __name__ == "__main__":
    unittest.main()
