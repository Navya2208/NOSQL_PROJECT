"""Runs the test-only reference pipeline through the real controller and loader.

Use it to get trusted results to check a real pipeline against:

    .venv/bin/python tests/run_reference.py --input tests/golden/sample.log --batch-size 50
    .venv/bin/etl run --pipeline mongodb --input tests/golden/sample.log --batch-size 50
    .venv/bin/etl compare --runs <reference run id> <mongodb run id>

The reference pipeline is plain Python. It is not one of the tool's four
pipelines and must never be used in place of one.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from etl import controller  # noqa: E402
from reference_pipeline import ReferencePipeline  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batch-size", type=int, required=True, metavar="N")
    p.add_argument("--input", nargs="+", metavar="FILE",
                   help="input files (default: the two NASA files in data/)")
    args = p.parse_args()
    controller.run(batch_size=args.batch_size, inputs=args.input, pipeline=ReferencePipeline())
    return 0


if __name__ == "__main__":
    sys.exit(main())
