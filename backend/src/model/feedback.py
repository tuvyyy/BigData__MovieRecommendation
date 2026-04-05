"""Feedback ingestion and retraining hooks."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from model.pipeline import run_training
from utils.config import load_app_config, resolve_project_path
from utils.logger import get_logger
from utils.spark_session import build_spark_session

FEEDBACK_LOGGER = get_logger("feedback", log_file="logs/app.log")


def append_feedback_rating(
    user_id: int,
    movie_id: int,
    rating: float,
    app_config: dict | None = None,
) -> Dict[str, str | int | float]:
    """Append new rating feedback into CSV storage."""
    if rating < 0.5 or rating > 5.0:
        raise ValueError("rating must be between 0.5 and 5.0")

    config = app_config or load_app_config()
    feedback_path = resolve_project_path(config["paths"]["feedback_path"])
    feedback_path.parent.mkdir(parents=True, exist_ok=True)

    timestamp = int(datetime.now(timezone.utc).timestamp())
    row = {
        "userId": int(user_id),
        "movieId": int(movie_id),
        "rating": float(rating),
        "timestamp": timestamp,
    }

    file_exists = feedback_path.exists()
    with feedback_path.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["userId", "movieId", "rating", "timestamp"])
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    FEEDBACK_LOGGER.info(
        "event=feedback_appended user_id=%s movie_id=%s rating=%.2f",
        user_id,
        movie_id,
        rating,
    )
    return row


def retrain_with_feedback(app_config: dict | None = None) -> Dict[str, str | int | float]:
    """Trigger model retraining after feedback append."""
    config = app_config or load_app_config()
    spark = build_spark_session(app_name="MovieRecommendationRetrain")
    try:
        return run_training(
            app_config=config,
            spark=spark,
            retrain_reason="feedback_triggered",
        )
    finally:
        spark.stop()
