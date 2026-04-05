"""Recommendation service with hybrid-precomputed routing, fallback, and events."""

from __future__ import annotations

import csv
from pathlib import Path
from typing import Any, Dict, List, Optional

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from model.feedback import append_feedback_rating, retrain_with_feedback
from model.fallback import build_fallback_recommendations, save_fallback_recommendations
from serving.cache import RedisRecommendationCache
from serving.events import append_user_event
from utils.config import load_app_config, resolve_project_path
from utils.logger import get_logger
from utils.spark_session import build_spark_session


class RecommendationService:
    """High-level API for hybrid recommendations, fallback, and feedback loop."""

    def __init__(self, config: dict | None = None, spark: SparkSession | None = None) -> None:
        self.config = config or load_app_config()
        self.spark = spark or build_spark_session(app_name="MovieRecommendationService")
        self._owns_spark = spark is None
        self.logger = get_logger("app", log_file="logs/app.log")

        self.paths = self.config["paths"]
        self.default_top_n = int(self.config["app"].get("top_n", 10))

        self._movies_df: DataFrame | None = None
        self._ratings_df: DataFrame | None = None
        self._fallback_df: DataFrame | None = None
        self._precomputed_df: DataFrame | None = None
        self._model: ALSModel | None = None

        cache_cfg = self.config.get("cache", {})
        self.cache = RedisRecommendationCache(
            enabled=bool(cache_cfg.get("redis_enabled", False)),
            redis_url=str(cache_cfg.get("redis_url", "redis://localhost:6379/0")),
            ttl_seconds=int(cache_cfg.get("redis_ttl_sec", 300)),
        )

    def close(self) -> None:
        """Close owned Spark resources."""
        if self._owns_spark:
            self.spark.stop()

    def _best_model_exists(self) -> bool:
        model_path = resolve_project_path(self.paths["best_model_path"])
        return model_path.exists()

    def _get_model(self) -> ALSModel | None:
        if self._model is not None:
            return self._model
        if not self._best_model_exists():
            return None

        model_path = resolve_project_path(self.paths["best_model_path"])
        try:
            self._model = ALSModel.load(str(model_path))
        except Exception as exc:  # pylint: disable=broad-except
            # Model loading must be best-effort; serving can still fallback safely.
            self.logger.warning("event=model_load_failed path=%s error=%s", model_path, exc)
            self._model = None
        return self._model

    def _get_movies_df(self) -> DataFrame:
        if self._movies_df is None:
            movies_path = resolve_project_path(self.paths["silver_movies_path"])
            self._movies_df = self.spark.read.parquet(str(movies_path)).cache()
            self._movies_df.count()
        return self._movies_df

    def _get_ratings_df(self) -> DataFrame:
        if self._ratings_df is None:
            ratings_path = resolve_project_path(self.paths["silver_ratings_path"])
            self._ratings_df = self.spark.read.parquet(str(ratings_path)).cache()
            self._ratings_df.count()
        return self._ratings_df

    def _get_fallback_df(self) -> DataFrame:
        if self._fallback_df is None:
            fallback_path = resolve_project_path(self.paths["gold_fallback_path"])
            try:
                if not fallback_path.exists():
                    self.logger.warning("event=fallback_missing path=%s action=rebuild", fallback_path)
                    self._rebuild_fallback_artifact(fallback_path=fallback_path)

                self._fallback_df = self.spark.read.parquet(str(fallback_path)).cache()
                self._fallback_df.count()
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning(
                    "event=fallback_read_failed path=%s action=rebuild error=%s",
                    fallback_path,
                    exc,
                )
                self._rebuild_fallback_artifact(fallback_path=fallback_path)
                self._fallback_df = self.spark.read.parquet(str(fallback_path)).cache()
                self._fallback_df.count()
        return self._fallback_df

    def _get_precomputed_df(self) -> DataFrame:
        if self._precomputed_df is None:
            precomputed_path = resolve_project_path(
                self.paths.get("gold_precomputed_path", "data/gold/recommendations_precomputed")
            )
            if not precomputed_path.exists():
                raise FileNotFoundError(f"Precomputed recommendations not found: {precomputed_path}")
            self._precomputed_df = self.spark.read.parquet(str(precomputed_path)).cache()
            self._precomputed_df.count()
        return self._precomputed_df

    def _rebuild_fallback_artifact(self, fallback_path: Path) -> None:
        fallback_cfg = self.config.get("fallback", {})
        top_n = int(fallback_cfg.get("top_n", self.default_top_n))
        min_rating_count = int(fallback_cfg.get("min_rating_count", 20))

        fallback_df = build_fallback_recommendations(
            ratings_df=self._get_ratings_df(),
            movies_df=self._get_movies_df(),
            top_n=top_n,
            min_rating_count=min_rating_count,
        )
        save_fallback_recommendations(fallback_df, fallback_path)

    def _user_has_history(self, user_id: int) -> bool:
        ratings_df = self._get_ratings_df()
        return ratings_df.filter(F.col("userId") == F.lit(int(user_id))).limit(1).count() > 0

    @staticmethod
    def _rows_to_dict(rows: List) -> List[Dict[str, object]]:
        return [row.asDict(recursive=True) for row in rows]

    @staticmethod
    def _normalize_recommendation_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            normalized.append(
                {
                    "movieId": int(row.get("movieId")),
                    "title": row.get("title"),
                    "genres": row.get("genres"),
                    "score": float(row.get("score", 0.0)),
                    "als_score": float(row.get("als_score", 0.0)) if row.get("als_score") is not None else None,
                    "content_score": float(row.get("content_score", 0.0)) if row.get("content_score") is not None else None,
                    "rank": int(row.get("rank", 0)) if row.get("rank") is not None else None,
                    "explain": row.get("explain") or "",
                }
            )
        return normalized

    def _hybrid_recommendations_from_precompute(
        self,
        user_id: int,
        top_n: int,
        preferred_genre: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        precomputed_df = self._get_precomputed_df()
        filtered = precomputed_df.filter(F.col("userId") == F.lit(int(user_id)))
        if preferred_genre:
            filtered = filtered.filter(F.lower(F.col("genres")).contains(preferred_genre.lower()))

        rows = (
            filtered.orderBy(F.asc("rank"))
            .limit(int(top_n))
            .select("movieId", "title", "genres", "score", "als_score", "content_score", "rank", "explain")
            .collect()
        )
        return self._normalize_recommendation_rows(self._rows_to_dict(rows))

    def _legacy_als_recommendations(self, user_id: int, top_n: int) -> List[Dict[str, Any]]:
        model = self._get_model()
        if model is None:
            return []

        user_df = self.spark.createDataFrame([(int(user_id),)], ["userId"])
        recommendations_df = model.recommendForUserSubset(user_df, int(top_n))
        if recommendations_df.count() == 0:
            return []

        exploded_df = recommendations_df.select(
            F.explode("recommendations").alias("rec"),
        ).select(
            F.col("rec.movieId").cast("int").alias("movieId"),
            F.col("rec.rating").cast("float").alias("score"),
        )
        joined_df = exploded_df.join(
            F.broadcast(self._get_movies_df().select("movieId", "title", "genres")),
            on="movieId",
            how="left",
        ).orderBy(F.desc("score"))

        rows = joined_df.limit(int(top_n)).collect()
        payload = self._rows_to_dict(rows)
        for row in payload:
            row["als_score"] = row.get("score")
            row["content_score"] = 0.0
            row["rank"] = None
            row["explain"] = "Users similar to you liked this."
        return self._normalize_recommendation_rows(payload)

    def _fallback_recommendations(
        self,
        top_n: int,
        preferred_genre: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        fallback_df = self._get_fallback_df()
        if preferred_genre:
            genre_df = (
                fallback_df.filter(
                    (F.col("fallback_type") == F.lit("genre"))
                    & (F.lower(F.col("genre")) == F.lit(preferred_genre.lower()))
                )
                .orderBy(F.asc("rank"))
                .limit(top_n)
            )
            genre_rows = genre_df.select(
                "movieId",
                "title",
                "genres",
                F.col("popularity_score").alias("score"),
            ).collect()
            if genre_rows:
                payload = self._rows_to_dict(genre_rows)
                for row in payload:
                    row["als_score"] = None
                    row["content_score"] = None
                    row["explain"] = f"Matches requested genre: {preferred_genre}"
                return self._normalize_recommendation_rows(payload)

        global_rows = (
            fallback_df.filter(F.col("fallback_type") == F.lit("global"))
            .orderBy(F.asc("rank"))
            .limit(top_n)
            .select(
                "movieId",
                "title",
                "genres",
                F.col("popularity_score").alias("score"),
            )
            .collect()
        )
        payload = self._rows_to_dict(global_rows)
        for row in payload:
            row["als_score"] = None
            row["content_score"] = None
            row["explain"] = "Top trending fallback recommendation."
        return self._normalize_recommendation_rows(payload)

    def _top_trending(self, top_n: int) -> List[Dict[str, Any]]:
        fallback_df = self._get_fallback_df()
        rows = (
            fallback_df.filter(F.col("fallback_type") == F.lit("global"))
            .orderBy(F.asc("rank"))
            .limit(int(top_n))
            .select("movieId", "title", "genres", F.col("popularity_score").alias("score"))
            .collect()
        )
        payload = self._rows_to_dict(rows)
        for row in payload:
            row["explain"] = "Top trending by global popularity."
        return self._normalize_recommendation_rows(payload)

    def _top_by_genre(self, genre: str, top_n: int) -> List[Dict[str, Any]]:
        fallback_df = self._get_fallback_df()
        rows = (
            fallback_df.filter(
                (F.col("fallback_type") == F.lit("genre"))
                & (F.lower(F.col("genre")) == F.lit(genre.lower()))
            )
            .orderBy(F.asc("rank"))
            .limit(int(top_n))
            .select("movieId", "title", "genres", F.col("popularity_score").alias("score"))
            .collect()
        )
        payload = self._rows_to_dict(rows)
        for row in payload:
            row["explain"] = f"Popular in genre: {genre}"
        return self._normalize_recommendation_rows(payload)

    @staticmethod
    def _cache_key(user_id: int, top_n: int, preferred_genre: Optional[str]) -> str:
        normalized_genre = (preferred_genre or "").strip().lower() or "all"
        return f"recommendations:user={int(user_id)}:top={int(top_n)}:genre={normalized_genre}"

    def _build_response(
        self,
        route: str,
        user_id: int,
        top_n: int,
        recommendations: List[Dict[str, Any]],
        preferred_genre: Optional[str] = None,
    ) -> Dict[str, Any]:
        trending_top_n = min(10, max(3, int(top_n)))
        selected_genre = preferred_genre or ""
        if not selected_genre and recommendations:
            first_genres = str(recommendations[0].get("genres", ""))
            selected_genre = first_genres.split("|")[0].strip() if first_genres else ""

        top_by_genre = self._top_by_genre(selected_genre, trending_top_n) if selected_genre else []
        because_you_watched = [
            {"movieId": row["movieId"], "title": row["title"], "explain": row.get("explain", "")}
            for row in recommendations[: min(5, len(recommendations))]
        ]
        score_rows = [
            {
                "movieId": row.get("movieId"),
                "score": row.get("score"),
                "als_score": row.get("als_score"),
                "content_score": row.get("content_score"),
            }
            for row in recommendations
        ]
        return {
            "route": route,
            "user_id": int(user_id),
            "top_n": int(top_n),
            "recommendations": recommendations,
            "scores": score_rows,
            "because_you_watched": because_you_watched,
            "top_trending": self._top_trending(trending_top_n),
            "top_by_genre": top_by_genre,
        }

    def recommend(
        self,
        user_id: int,
        top_n: Optional[int] = None,
        preferred_genre: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Return recommendations via hybrid-precomputed router, fallback for cold-start."""
        resolved_top_n = int(top_n or self.default_top_n)
        cache_key = self._cache_key(user_id=user_id, top_n=resolved_top_n, preferred_genre=preferred_genre)
        cached_payload = self.cache.get(cache_key)
        if cached_payload:
            cached_payload["cache_hit"] = True
            return cached_payload

        route = "fallback"
        recommendations: List[Dict[str, Any]] = []
        serving_cfg = self.config.get("serving", {})
        allow_legacy_als = bool(serving_cfg.get("allow_als_legacy", False))

        has_history = False
        try:
            has_history = self._user_has_history(user_id)
        except Exception as exc:  # pylint: disable=broad-except
            self.logger.warning("event=user_history_check_failed user_id=%s error=%s", user_id, exc)
            has_history = False

        if has_history:
            try:
                recommendations = self._hybrid_recommendations_from_precompute(
                    user_id=user_id,
                    top_n=resolved_top_n,
                    preferred_genre=preferred_genre,
                )
                if recommendations:
                    route = "hybrid"
            except Exception as exc:  # pylint: disable=broad-except
                self.logger.warning(
                    "event=precompute_route_failed user_id=%s top_n=%s error=%s",
                    user_id,
                    resolved_top_n,
                    exc,
                )

            if not recommendations and allow_legacy_als:
                recommendations = self._legacy_als_recommendations(user_id=user_id, top_n=resolved_top_n)
                if recommendations:
                    route = "als_legacy"

        if not recommendations:
            recommendations = self._fallback_recommendations(resolved_top_n, preferred_genre)
            route = "fallback"

        payload = self._build_response(
            route=route,
            user_id=user_id,
            top_n=resolved_top_n,
            recommendations=recommendations,
            preferred_genre=preferred_genre,
        )
        payload["cache_hit"] = False
        self.cache.set(cache_key, payload)
        return payload

    def add_event(
        self,
        event_type: str,
        user_id: int,
        movie_id: int | None = None,
        metadata: Dict[str, Any] | None = None,
    ) -> Dict[str, Any]:
        """Persist user behavior event to log storage."""
        events_path = self.paths.get("user_events_path", "data/logs/user_events")
        file_path = append_user_event(
            events_root=events_path,
            event_type=event_type,
            user_id=user_id,
            movie_id=movie_id,
            metadata=metadata or {},
        )
        self.logger.info(
            "event=user_event_logged type=%s user_id=%s movie_id=%s path=%s",
            event_type,
            user_id,
            movie_id,
            file_path,
        )
        return {
            "status": "ok",
            "event_type": event_type,
            "user_id": int(user_id),
            "movie_id": int(movie_id) if movie_id is not None else None,
            "path": str(file_path),
        }

    def add_rating(
        self,
        user_id: int,
        movie_id: int,
        rating: float,
        trigger_retrain: bool = False,
    ) -> Dict[str, object]:
        """Append rating feedback and optionally retrain model."""
        payload = append_feedback_rating(
            user_id=user_id,
            movie_id=movie_id,
            rating=rating,
            app_config=self.config,
        )
        response: Dict[str, object] = {"status": "ok", "saved_rating": payload}
        self.add_event(
            event_type="rate",
            user_id=user_id,
            movie_id=movie_id,
            metadata={"rating": float(rating)},
        )

        if trigger_retrain:
            self.logger.info("event=retrain_triggered user_id=%s movie_id=%s", user_id, movie_id)
            retrain_summary = retrain_with_feedback(app_config=self.config)
            self._model = None
            self._ratings_df = None
            self._fallback_df = None
            self._precomputed_df = None
            response["retrain_summary"] = retrain_summary

        return response

    def invalidate_runtime_cache(self) -> None:
        """Force lazy reload of model/data artifacts."""
        self._model = None
        self._ratings_df = None
        self._fallback_df = None
        self._precomputed_df = None

    def _artifact_exists(self, path_key: str) -> bool:
        path_value = self.paths.get(path_key)
        if not path_value:
            return False
        return resolve_project_path(path_value).exists()

    def _latest_model_version(self) -> Optional[str]:
        marker_path = resolve_project_path(self.paths["models_dir"]) / "latest_model_version.txt"
        if not marker_path.exists():
            return None
        content = marker_path.read_text(encoding="utf-8").strip()
        if not content:
            return None
        return Path(content).name

    def _best_rmse_summary(self) -> Dict[str, object]:
        metrics_path = resolve_project_path(self.paths["rmse_results_path"])
        if not metrics_path.exists():
            return {"best_rmse": None, "best_params": None, "trial_count": 0}

        rows: List[Dict[str, str]] = []
        with metrics_path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            for row in reader:
                if row.get("rmse"):
                    rows.append(row)

        if not rows:
            return {"best_rmse": None, "best_params": None, "trial_count": 0}

        best_row = min(rows, key=lambda row: float(row["rmse"]))
        return {
            "best_rmse": float(best_row["rmse"]),
            "best_params": {
                "rank": int(float(best_row["rank"])),
                "regParam": float(best_row["regParam"]),
                "maxIter": int(float(best_row["maxIter"])),
            },
            "trial_count": len(rows),
        }

    def _ranking_metrics_summary(self) -> Dict[str, Any]:
        metrics_path = resolve_project_path(
            self.paths.get("ranking_metrics_path", "metrics/ranking_metrics.csv")
        )
        if not metrics_path.exists():
            return {
                "precision_at_k": None,
                "recall_at_k": None,
                "ndcg_at_k": None,
                "k": None,
                "evaluated_users": 0,
            }

        with metrics_path.open("r", encoding="utf-8", newline="") as file:
            reader = csv.DictReader(file)
            rows = [row for row in reader]
        if not rows:
            return {
                "precision_at_k": None,
                "recall_at_k": None,
                "ndcg_at_k": None,
                "k": None,
                "evaluated_users": 0,
            }

        row = rows[-1]
        return {
            "precision_at_k": float(row.get("precision_at_k", 0.0)),
            "recall_at_k": float(row.get("recall_at_k", 0.0)),
            "ndcg_at_k": float(row.get("ndcg_at_k", 0.0)),
            "k": int(float(row.get("k", 0))),
            "evaluated_users": int(float(row.get("evaluated_users", 0))),
            "algorithm": row.get("algorithm"),
            "timestamp_utc": row.get("timestamp_utc"),
        }

    def health(self) -> Dict[str, object]:
        """Health payload for API checks."""
        metrics_summary = self._best_rmse_summary()
        ranking_summary = self._ranking_metrics_summary()
        return {
            "status": "ok",
            "model_ready": self._best_model_exists(),
            "spark_app_name": self.spark.sparkContext.appName,
            "spark_ui_hint": "http://localhost:4040",
            "model_version": self._latest_model_version(),
            "cache_enabled": bool(self.cache.enabled),
            "metrics": {
                **metrics_summary,
                "ranking": ranking_summary,
            },
            "artifacts": {
                "silver_ratings_ready": self._artifact_exists("silver_ratings_path"),
                "silver_movies_ready": self._artifact_exists("silver_movies_path"),
                "fallback_ready": self._artifact_exists("gold_fallback_path"),
                "precomputed_ready": self._artifact_exists("gold_precomputed_path"),
                "ranking_metrics_ready": self._artifact_exists("ranking_metrics_path"),
                "user_events_ready": self._artifact_exists("user_events_path"),
                "etl_explain_ready": self._artifact_exists("etl_explain_path"),
                "train_explain_before_ready": self._artifact_exists("train_explain_before_path"),
                "train_explain_after_ready": self._artifact_exists("train_explain_after_path"),
            },
        }
