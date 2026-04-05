"""Run the FastAPI server."""

from __future__ import annotations

import sys
from pathlib import Path

import uvicorn

PROJECT_ROOT = Path(__file__).parent
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from utils.config import load_app_config  # noqa: E402


def main() -> None:
    config = load_app_config()
    api_cfg = config.get("api", {})
    host = str(api_cfg.get("host", "127.0.0.1"))
    port = int(api_cfg.get("port", 8000))

    uvicorn.run("api.main:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
