"""Command-line interface for QMTAdapter deployment."""

import argparse
import sys
from typing import List, Optional

from .deploy import DEFAULT_INSTALL_ROOT, deploy


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qmt-adapter")
    commands = parser.add_subparsers(dest="command")
    deploy_parser = commands.add_parser(
        "deploy", help="deploy or upgrade the QMT-side bridge"
    )
    deploy_parser.add_argument(
        "--root",
        default=str(DEFAULT_INSTALL_ROOT),
        help="absolute deployment directory (default: C:\\QMTAdapter)",
    )
    deploy_parser.add_argument(
        "--account-id",
        action="append",
        default=[],
        help="stock account ID; required on first deployment",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """Run the ``qmt-adapter`` command-line interface."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.command != "deploy":
        parser.print_help()
        return 2
    try:
        result = deploy(root=args.root, account_ids=args.account_id)
    except (OSError, RuntimeError, ValueError) as exc:
        print("qmt-adapter deploy failed: %s" % exc, file=sys.stderr)
        return 1

    print("QMT bridge: %s" % result["bridge_path"])
    print("QMT loader: %s" % result["loader_path"])
    if result["config_created"]:
        print("Config created: %s" % result["config_path"])
    else:
        print("Config updated: %s" % result["config_path"])
    print(
        "Database is created by QMT on first bridge startup: %s"
        % result["database_path"]
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
