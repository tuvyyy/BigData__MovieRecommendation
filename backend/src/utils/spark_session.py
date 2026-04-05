"""Spark session management utilities."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import ctypes
from pathlib import Path
from typing import Any, Optional

from pyspark.sql import SparkSession

from utils.config import load_spark_config


def _normalize_conf_value(value: Any) -> str:
    """Normalize Spark conf values into canonical string form."""
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def _parse_java_major(version_text: str) -> Optional[int]:
    match = re.search(r'"(\d+)(?:\.(\d+))?', version_text)
    if not match:
        return None
    first = int(match.group(1))
    second = int(match.group(2)) if match.group(2) else None
    if first == 1 and second is not None:
        return second
    return first


def _java_major_from_executable(java_executable: str) -> Optional[int]:
    try:
        proc = subprocess.run(
            [java_executable, "-version"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    return _parse_java_major(proc.stderr or proc.stdout)


def _current_java_major() -> Optional[int]:
    return _java_major_from_executable("java")


def _candidate_java_homes() -> list[Path]:
    local_app_data = Path(os.environ.get("LOCALAPPDATA", ""))
    candidates: list[Path] = [
        local_app_data / "Programs" / "Microsoft",
        local_app_data / "Programs" / "Eclipse Adoptium",
        Path("C:/Program Files/Microsoft"),
        Path("C:/Program Files/Eclipse Adoptium"),
        Path("C:/Program Files/Java"),
    ]

    homes: list[Path] = []
    for base in candidates:
        if not base.exists():
            continue
        for child in base.iterdir():
            if child.is_dir() and "jdk" in child.name.lower() and (child / "bin" / "java.exe").exists():
                homes.append(child)
    return sorted(homes, reverse=True)


def _ensure_compatible_java() -> None:
    existing_java_home = os.environ.get("JAVA_HOME")
    if existing_java_home:
        java_bin = Path(existing_java_home) / "bin" / "java.exe"
        if java_bin.exists() and (_java_major_from_executable(str(java_bin)) or 0) >= 8:
            os.environ["PATH"] = f"{java_bin.parent}{os.pathsep}{os.environ.get('PATH', '')}"
            return

    if (_current_java_major() or 0) >= 8:
        return

    for candidate_home in _candidate_java_homes():
        java_bin = candidate_home / "bin"
        candidate_java = java_bin / "java.exe"
        if not candidate_java.exists():
            continue
        if (_java_major_from_executable(str(candidate_java)) or 0) >= 8:
            os.environ["JAVA_HOME"] = str(candidate_home)
            os.environ["PATH"] = f"{java_bin}{os.pathsep}{os.environ.get('PATH', '')}"
            return


def _is_ascii(text: str) -> bool:
    return all(ord(char) < 128 for char in text)


def _windows_short_path(path: Path) -> Optional[Path]:
    """Return Windows 8.3 short path when available."""
    if os.name != "nt":
        return None
    try:
        buffer = ctypes.create_unicode_buffer(32768)
        size = ctypes.windll.kernel32.GetShortPathNameW(str(path), buffer, len(buffer))
        if size == 0:
            return None
        candidate = Path(buffer.value)
        if candidate.exists():
            return candidate
    except Exception:  # pylint: disable=broad-except
        return None
    return None


def _prefer_ascii_path(path: Path) -> Path:
    """Prefer ASCII-safe path variant on Windows for Spark launcher scripts."""
    if _is_ascii(str(path)):
        return path
    short_path = _windows_short_path(path)
    if short_path is not None and _is_ascii(str(short_path)):
        return short_path
    return path


def _ensure_pyspark_env() -> None:
    """Force stable Spark launcher env from current interpreter location."""
    cwd_python = Path.cwd() / ".venv" / "Scripts" / "python.exe"
    if cwd_python.exists():
        python_path = cwd_python
    else:
        python_path = Path(sys.executable)

    python_path = _prefer_ascii_path(python_path)
    python_path_str = str(python_path)
    if not _is_ascii(python_path_str):
        # Fallback to command lookup if executable path still non-ASCII.
        python_path_str = shutil.which("python") or shutil.which("py") or python_path_str

    os.environ["PYSPARK_PYTHON"] = python_path_str
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_path_str

    cwd_spark_home = Path.cwd() / ".venv" / "Lib" / "site-packages" / "pyspark"
    if cwd_spark_home.exists():
        candidate_spark_home = cwd_spark_home
    else:
        candidate_spark_home = python_path.parent.parent / "Lib" / "site-packages" / "pyspark"
    if not candidate_spark_home.exists():
        return

    candidate_spark_home = _prefer_ascii_path(candidate_spark_home)

    existing_spark_home = os.environ.get("SPARK_HOME", "")
    candidate_text = str(candidate_spark_home)
    should_override = (
        not existing_spark_home
        or (not _is_ascii(existing_spark_home) and _is_ascii(candidate_text))
    )

    if should_override and _is_ascii(candidate_text):
        os.environ["SPARK_HOME"] = candidate_text

    # Pip-installed Spark bundles use Scala 2.12.
    os.environ.setdefault("SPARK_SCALA_VERSION", "2.12")


def _ensure_windows_hadoop_home() -> None:
    """Configure HADOOP_HOME on Windows when local winutils binaries are available."""
    if os.name != "nt":
        return

    existing_hadoop_home = os.environ.get("HADOOP_HOME")
    hadoop_home: Optional[Path] = Path(existing_hadoop_home) if existing_hadoop_home else None

    if hadoop_home is None:
        candidate = Path.cwd() / "tools" / "hadoop"
        if (candidate / "bin" / "winutils.exe").exists() and (candidate / "bin" / "hadoop.dll").exists():
            hadoop_home = candidate
            os.environ["HADOOP_HOME"] = str(candidate)

    if hadoop_home is None:
        return

    hadoop_bin = hadoop_home / "bin"
    if hadoop_bin.exists():
        os.environ["PATH"] = f"{hadoop_bin}{os.pathsep}{os.environ.get('PATH', '')}"
        os.environ["hadoop.home.dir"] = str(hadoop_home)


def build_spark_session(
    app_name: Optional[str] = None,
    config_path: Optional[str | Path] = None,
) -> SparkSession:
    """Create a SparkSession based on YAML configuration."""
    _ensure_compatible_java()
    _ensure_pyspark_env()
    _ensure_windows_hadoop_home()
    spark_config = load_spark_config(config_path=config_path)

    resolved_app_name = app_name or spark_config.get(
        "app_name",
        "MovieRecommendationSystem",
    )
    builder = SparkSession.builder.appName(resolved_app_name)

    master = spark_config.get("master")
    if master:
        builder = builder.master(master)

    for key, value in (spark_config.get("configs") or {}).items():
        builder = builder.config(key, _normalize_conf_value(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_config.get("log_level", "WARN"))
    return spark
