import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qmt_adapter import OrderRequest, QmtAdapterError, QmtClient


CONFIRM_TEXT = "PLACE_STOCK_ORDER"


def main():
    parser = argparse.ArgumentParser(description="Place one QMT stock order.")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--instrument", required=True, help="Example: 600000.SH")
    parser.add_argument("--side", required=True, choices=("BUY", "SELL"))
    parser.add_argument("--quantity", required=True, type=int)
    parser.add_argument(
        "--price-type",
        default="LIMIT",
        choices=(
            "LIMIT",
            "LATEST",
            "ASK5",
            "ASK4",
            "ASK3",
            "ASK2",
            "ASK1",
            "BID1",
            "BID2",
            "BID3",
            "BID4",
            "BID5",
            "LIMIT_UP_DOWN",
            "QUEUE",
            "COUNTERPARTY",
            "MARKET_SH_CONVERT_5_CANCEL",
            "MARKET_SH_CONVERT_5_LIMIT",
            "MARKET_PEER_PRICE_FIRST",
            "MARKET_MINE_PRICE_FIRST",
            "MARKET_SZ_INSTBUSI_RESTCANCEL",
            "MARKET_SZ_CONVERT_5_CANCEL",
            "MARKET_SZ_FULL_OR_CANCEL",
        ),
    )
    parser.add_argument("--limit-price")
    parser.add_argument("--remark", default="")
    parser.add_argument("--config")
    parser.add_argument("--confirm", required=True)
    args = parser.parse_args()

    if args.confirm != CONFIRM_TEXT:
        print("Refusing order: --confirm must equal %s" % CONFIRM_TEXT, file=sys.stderr)
        return 2

    try:
        with QmtClient(config_path=args.config, client_id="stock-order-cli") as client:
            receipt = client.place_order(
                OrderRequest(
                    account_id=args.account_id,
                    account_type="STOCK",
                    instrument=args.instrument,
                    side=args.side,
                    quantity=args.quantity,
                    price_type=args.price_type,
                    limit_price=args.limit_price,
                    remark=args.remark,
                ),
                wait_for="BROKER_ID",
                timeout=10.0,
            )
            print(json.dumps(receipt.raw, ensure_ascii=False, indent=2))
            return 0
    except QmtAdapterError as exc:
        print("%s: %s" % (exc.code, exc), file=sys.stderr)
        if exc.data:
            print(json.dumps(exc.data, ensure_ascii=False, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
