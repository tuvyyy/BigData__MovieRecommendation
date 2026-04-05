"""Optional Redis cache wrapper for recommendation serving."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional


class RedisRecommendationCache:
    """Small safe wrapper around Redis (optional dependency)."""

    def __init__(self, enabled: bool, redis_url: str, ttl_seconds: int) -> None:
        self.enabled = bool(enabled)
        self.redis_url = redis_url
        self.ttl_seconds = int(ttl_seconds)
        self._client = None

        if not self.enabled:
            return
        try:
            import redis  # type: ignore

            client = redis.Redis.from_url(redis_url, decode_responses=True)
            client.ping()
            self._client = client
        except Exception:
            self.enabled = False
            self._client = None

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.enabled or self._client is None:
            return None
        raw = self._client.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return None

    def set(self, key: str, value: Dict[str, Any]) -> None:
        if not self.enabled or self._client is None:
            return
        self._client.setex(name=key, time=self.ttl_seconds, value=json.dumps(value, ensure_ascii=False))

