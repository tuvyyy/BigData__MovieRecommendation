"""User behavior event tracking for recommendation product analytics."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

from utils.config import resolve_project_path

ALLOWED_EVENT_TYPES = {"click", "view", "rate", "skip"}


def append_user_event(
    events_root: str | Path,
    event_type: str,
    user_id: int,
    movie_id: int | None = None,
    metadata: Dict[str, Any] | None = None,
) -> Path:
    """Append one user behavior event into daily-partitioned JSONL logs."""
    normalized_type = event_type.strip().lower()
    if normalized_type not in ALLOWED_EVENT_TYPES:
        raise ValueError(f"unsupported event_type='{event_type}'")

    now = datetime.now(timezone.utc)
    event_day = now.strftime("%Y-%m-%d")
    event_ts = now.isoformat()

    root = resolve_project_path(events_root)
    partition_dir = root / f"dt={event_day}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    target_file = partition_dir / "events.jsonl"

    payload = {
        "timestamp": event_ts,
        "event_type": normalized_type,
        "userId": int(user_id),
        "movieId": int(movie_id) if movie_id is not None else None,
        "metadata": metadata or {},
    }

    with target_file.open("a", encoding="utf-8") as file:
        file.write(json.dumps(payload, ensure_ascii=False) + "\n")

    return target_file

