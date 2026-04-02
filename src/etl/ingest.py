"""Backward-compatible ETL entrypoint."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

SRC_ROOT = Path(__file__).parents[1]
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl.pipeline import run_etl  # noqa: E402
from utils.config import load_app_config  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run MovieLens ingest/transform.")
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional path to app_config.yaml",
    )
    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_args()
    app_config = load_app_config(cli_args.config) if cli_args.config else load_app_config()
    result = run_etl(app_config=app_config)
    print("Ingest/transform completed:", result)
