"""YAML configuration utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml

PROJECT_ROOT = Path(__file__).parents[2]
DEFAULT_SPARK_CONFIG_PATH = PROJECT_ROOT / "configs" / "spark_config.yaml"
DEFAULT_APP_CONFIG_PATH = PROJECT_ROOT / "configs" / "app_config.yaml"


def resolve_project_path(path_value: str | Path) -> Path:
    """Resolve relative paths against the project root."""
    path = Path(path_value)
    if path.is_absolute():
        return path
    return PROJECT_ROOT / path


def load_yaml(path: str | Path) -> Dict[str, Any]:
    """Load YAML file into a dictionary."""
    resolved_path = resolve_project_path(path)
    if not resolved_path.exists():
        raise FileNotFoundError(f"Config file not found: {resolved_path}")

    with resolved_path.open("r", encoding="utf-8") as file:
        payload = yaml.safe_load(file) or {}
    return payload


def load_app_config(config_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Load app configuration."""
    return load_yaml(config_path or DEFAULT_APP_CONFIG_PATH)


def load_spark_config(config_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """Load Spark configuration section."""
    payload = load_yaml(config_path or DEFAULT_SPARK_CONFIG_PATH)
    return payload.get("spark", payload)
