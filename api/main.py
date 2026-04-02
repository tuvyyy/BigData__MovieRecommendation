"""FastAPI backend for movie recommendations."""

from __future__ import annotations

import sys
import traceback
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, ConfigDict, Field

PROJECT_ROOT = Path(__file__).parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from model.feedback import retrain_with_feedback  # noqa: E402
from service.recommendation_service import RecommendationService  # noqa: E402
from utils.config import load_app_config  # noqa: E402
from utils.logger import get_logger  # noqa: E402

APP_CONFIG = load_app_config()
app = FastAPI(title="Movie Recommendation API", version="1.0.0")
SERVICE: RecommendationService | None = None
API_LOGGER = get_logger("api", log_file="logs/app.log")

cors_origins = APP_CONFIG.get("api", {}).get(
    "cors_origins",
    ["http://localhost:5173", "http://127.0.0.1:5173"],
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[str(origin) for origin in cors_origins],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class RateRequest(BaseModel):
    """Payload for feedback submission."""

    model_config = ConfigDict(populate_by_name=True)

    user_id: int = Field(alias="userId")
    movie_id: int = Field(alias="movieId")
    rating: float
    retrain: bool = True


class EventRequest(BaseModel):
    """Payload for behavior tracking."""

    model_config = ConfigDict(populate_by_name=True)

    event_type: str = Field(alias="eventType")
    user_id: int = Field(alias="userId")
    movie_id: Optional[int] = Field(default=None, alias="movieId")
    metadata: Dict[str, Any] = Field(default_factory=dict)


def _require_service() -> RecommendationService:
    if SERVICE is None:
        raise RuntimeError("Recommendation service is not initialized")
    return SERVICE


def _reset_service(reason: str) -> None:
    global SERVICE
    if SERVICE is not None:
        try:
            SERVICE.close()
        except Exception:  # pylint: disable=broad-except
            pass
    SERVICE = RecommendationService(config=APP_CONFIG)
    API_LOGGER.warning("event=service_reset reason=%s", reason)


def _is_recoverable_spark_assertion(exc: Exception) -> bool:
    if isinstance(exc, AssertionError):
        return True
    text = str(exc)
    return ("AssertionError" in text) or ("sc is not None" in text)


def _run_background_retrain() -> None:
    service = _require_service()
    retrain_with_feedback(service.config)
    service.invalidate_runtime_cache()


@app.on_event("startup")
def startup() -> None:
    global SERVICE
    SERVICE = RecommendationService(config=APP_CONFIG)


@app.on_event("shutdown")
def shutdown() -> None:
    if SERVICE is not None:
        SERVICE.close()


@app.get("/health")
def health() -> dict:
    """Health endpoint."""
    try:
        return _require_service().health()
    except Exception as exc:  # pylint: disable=broad-except
        if _is_recoverable_spark_assertion(exc):
            _reset_service(reason="health_assertion_recover")
            return _require_service().health()
        raise


@app.get("/recommend/{user_id}")
def recommend(
    user_id: int,
    top_n: int = 10,
    genre: Optional[str] = None,
) -> dict:
    """Recommendation endpoint."""
    try:
        return _require_service().recommend(user_id=user_id, top_n=top_n, preferred_genre=genre)
    except Exception as exc:  # pylint: disable=broad-except
        if _is_recoverable_spark_assertion(exc):
            try:
                _reset_service(reason="recommend_assertion_recover")
                return _require_service().recommend(user_id=user_id, top_n=top_n, preferred_genre=genre)
            except Exception as retry_exc:  # pylint: disable=broad-except
                exc = retry_exc
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error(
            "event=api_recommend_failed user_id=%s top_n=%s genre=%s detail=%s traceback=%s",
            user_id,
            top_n,
            genre,
            detail,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=detail) from exc


@app.post("/rate")
def rate(payload: RateRequest, background_tasks: BackgroundTasks) -> dict:
    """Rating feedback endpoint."""
    if payload.rating < 0.5 or payload.rating > 5.0:
        raise HTTPException(status_code=400, detail="rating must be between 0.5 and 5.0")

    try:
        result = _require_service().add_rating(
            user_id=payload.user_id,
            movie_id=payload.movie_id,
            rating=payload.rating,
            trigger_retrain=False,
        )
        if payload.retrain:
            background_tasks.add_task(_run_background_retrain)
            result["retrain_status"] = "scheduled"
        else:
            result["retrain_status"] = "skipped"
        return result
    except Exception as exc:  # pylint: disable=broad-except
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error(
            "event=api_rate_failed user_id=%s movie_id=%s rating=%s detail=%s traceback=%s",
            payload.user_id,
            payload.movie_id,
            payload.rating,
            detail,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=detail) from exc


@app.post("/event")
def event(payload: EventRequest) -> dict:
    """Behavior tracking endpoint: click/view/rate/skip."""
    try:
        return _require_service().add_event(
            event_type=payload.event_type,
            user_id=payload.user_id,
            movie_id=payload.movie_id,
            metadata=payload.metadata,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pylint: disable=broad-except
        detail = str(exc).strip() or f"{type(exc).__name__}: no detail message"
        API_LOGGER.error(
            "event=api_event_failed event_type=%s user_id=%s movie_id=%s detail=%s traceback=%s",
            payload.event_type,
            payload.user_id,
            payload.movie_id,
            detail,
            traceback.format_exc(),
        )
        raise HTTPException(status_code=500, detail=detail) from exc
