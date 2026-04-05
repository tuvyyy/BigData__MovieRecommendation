"""Logging utilities for the movie recommendation project."""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

from utils.config import PROJECT_ROOT

DEFAULT_LOG_FORMAT = (
    "%(asctime)s | %(levelname)s | %(name)s | event=%(message)s"
)
_CONFIGURED_LOGGER_KEYS: set[str] = set()


def get_logger(
    name: str,
    level: int = logging.INFO,
    log_file: Optional[str | Path] = None,
    log_format: Optional[str] = None,
) -> logging.Logger:
    """Return a structured logger with stream and optional file output."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    file_key = str(log_file) if log_file else "stdout_only"
    key = f"{name}|{file_key}"
    if key in _CONFIGURED_LOGGER_KEYS:
        return logger

    formatter = logging.Formatter(log_format or DEFAULT_LOG_FORMAT)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_file:
        file_path = Path(log_file)
        if not file_path.is_absolute():
            file_path = PROJECT_ROOT / file_path
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = RotatingFileHandler(
            filename=file_path,
            maxBytes=5_000_000,
            backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.propagate = False
    _CONFIGURED_LOGGER_KEYS.add(key)
    return logger
