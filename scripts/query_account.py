import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from qmt_adapter import QmtClient, QmtAdapterError


def main():
    parser = argparse.ArgumentParser(
        description="Query one configured QMT stock account and its positions."
    )
    parser.add_argument("--account-id", help="Configured QMT stock account id")
    parser.add_argument("--config", help="Path to bridge_config.json")
    args = parser.parse_args()

    try:
        with QmtClient(config_path=args.config, client_id="account-query") as client:
            configured = client.hello.get("accounts", [])
            account_id = args.account_id
            if not account_id:
                if len(configured) != 1:
                    print(
                        "Specify --account-id. The bridge currently has %d configured accounts."
                        % len(configured),
                        file=sys.stderr,
                    )
                    return 2
                account_id = configured[0]["account_id"]

            result = {
                "health": client.health(),
                "account": client.get_account(account_id),
                "positions": client.list_positions(account_id),
            }
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0
    except QmtAdapterError as exc:
        print("%s: %s" % (exc.code, exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
