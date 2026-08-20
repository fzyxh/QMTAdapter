"""Run one Python source file and persist a pythonw traceback for diagnostics."""

from __future__ import print_function

import runpy
import sys
import traceback


def main():
    source_path = sys.argv[1]
    result_path = sys.argv[2]
    try:
        runpy.run_path(source_path, run_name="__qmt_runtime_check__")
        result = "PASS\n"
        exit_code = 0
    except Exception:
        result = traceback.format_exc()
        exit_code = 1
    with open(result_path, "w") as handle:
        handle.write(result)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
