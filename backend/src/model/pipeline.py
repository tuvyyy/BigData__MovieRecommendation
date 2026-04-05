"""Model training pipeline for ALS recommendations."""

from __future__ import annotations

import csv
import io
import time
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.schemas import MOVIES_SCHEMA, RATINGS_SCHEMA
from model.evaluate import evaluate_ranking_metrics, evaluate_rmse
from model.fallback import build_fallback_recommendations, save_fallback_recommendations
from model.precompute import (
    als_candidates_for_user_subset,
    build_hybrid_from_candidates,
    build_hybrid_precomputed_recommendations,
    save_precomputed_recommendations,
)
from model.train_als import ALSConfig, train_als_model
from utils.config import load_app_config, resolve_project_path
from utils.logger import get_logger
from utils.spark_session import build_spark_session

TRAIN_LOGGER = get_logger("train", log_file="logs/train.log")


def _capture_explain(df: DataFrame, section_name: str) -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(mode="formatted")
    return f"=== {section_name} ===\n{buffer.getvalue().strip()}\n"


def _save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _save_rmse_results(path: Path, rows: List[Dict[str, float | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["rank", "regParam", "maxIter", "rmse"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _save_ranking_metrics(path: Path, row: Dict[str, float | int | str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "timestamp_utc",
                "algorithm",
                "k",
                "relevant_threshold",
                "precision_at_k",
                "recall_at_k",
                "ndcg_at_k",
                "evaluated_users",
            ],
        )
        writer.writeheader()
        writer.writerow(row)


def _load_feedback_ratings(spark: SparkSession, feedback_path: Path) -> Optional[DataFrame]:
    if not feedback_path.exists():
        return None

    return (
        spark.read.format("csv")
        .option("header", True)
        .schema(RATINGS_SCHEMA)
        .load(str(feedback_path))
    )


def _load_training_inputs(
    spark: SparkSession,
    ratings_path: Path,
    movies_path: Path,
    feedback_path: Path,
) -> Tuple[DataFrame, DataFrame]:
    ratings_df = spark.read.schema(RATINGS_SCHEMA).parquet(str(ratings_path))
    movies_df = spark.read.schema(MOVIES_SCHEMA).parquet(str(movies_path))
    feedback_df = _load_feedback_ratings(spark, feedback_path)

    ratings_core = ratings_df.select(
        F.col("userId").cast("int").alias("userId"),
        F.col("movieId").cast("int").alias("movieId"),
        F.col("rating").cast("float").alias("rating"),
        F.col("timestamp").cast("long").alias("timestamp"),
    ).dropna(subset=["userId", "movieId", "rating"])

    if feedback_df is not None:
        feedback_core = feedback_df.select(
            F.col("userId").cast("int").alias("userId"),
            F.col("movieId").cast("int").alias("movieId"),
            F.col("rating").cast("float").alias("rating"),
            F.col("timestamp").cast("long").alias("timestamp"),
        ).dropna(subset=["userId", "movieId", "rating"])
        combined_source = ratings_core.unionByName(feedback_core)
    else:
        combined_source = ratings_core

    combined_ratings = (
        combined_source.filter((F.col("rating") >= F.lit(0.5)) & (F.col("rating") <= F.lit(5.0)))
        .dropDuplicates(["userId", "movieId", "timestamp"])
        .cache()
    )
    movies_cached = movies_df.cache()

    combined_ratings.count()
    movies_cached.count()

    return combined_ratings, movies_cached


def _save_model_versions(
    model: ALSModel,
    models_dir: Path,
    best_model_path: Path,
) -> Path:
    models_dir.mkdir(parents=True, exist_ok=True)
    best_model_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    version_path = models_dir / "versions" / f"als_{timestamp}"
    version_path.parent.mkdir(parents=True, exist_ok=True)

    model.write().overwrite().save(str(version_path))
    model.write().overwrite().save(str(best_model_path))

    (models_dir / "latest_model_version.txt").write_text(str(version_path), encoding="utf-8")
    return version_path


def _save_optimization_explains(
    model: ALSModel,
    movies_df: DataFrame,
    top_n: int,
    before_path: Path,
    after_path: Path,
) -> None:
    recommendations = model.recommendForAllUsers(top_n).select(
        "userId",
        F.explode("recommendations").alias("rec"),
    ).select(
        "userId",
        F.col("rec.movieId").cast("int").alias("movieId"),
        F.col("rec.rating").cast("float").alias("pred_rating"),
    )

    before_df = recommendations.join(
        movies_df.select("movieId", "title", "genres"),
        on="movieId",
        how="left",
    )
    after_df = recommendations.join(
        F.broadcast(movies_df.select("movieId", "title", "genres")),
        on="movieId",
        how="left",
    )

    _save_text(before_path, _capture_explain(before_df, "recommendations_join_before_optimization"))
    _save_text(after_path, _capture_explain(after_df, "recommendations_join_after_broadcast"))


def run_training(
    app_config: dict | None = None,
    spark: SparkSession | None = None,
    retrain_reason: str = "standard",
) -> Dict[str, str | int | float]:
    """Train ALS with hyperparameter tuning and save artifacts."""
    config = app_config or load_app_config()
    paths = config["paths"]
    training_cfg = config["training"]
    fallback_cfg = config["fallback"]
    hybrid_cfg = config.get("hybrid", {})
    precompute_cfg = config.get("precompute", {})
    evaluation_cfg = config.get("evaluation", {})

    ratings_path = resolve_project_path(paths["silver_ratings_path"])
    movies_path = resolve_project_path(paths["silver_movies_path"])
    feedback_path = resolve_project_path(paths["feedback_path"])
    models_dir = resolve_project_path(paths["models_dir"])
    best_model_path = resolve_project_path(paths["best_model_path"])
    rmse_results_path = resolve_project_path(paths["rmse_results_path"])
    explain_before_path = resolve_project_path(paths["train_explain_before_path"])
    explain_after_path = resolve_project_path(paths["train_explain_after_path"])
    fallback_output_path = resolve_project_path(paths["gold_fallback_path"])
    precomputed_output_path = resolve_project_path(
        paths.get("gold_precomputed_path", "data/gold/recommendations_precomputed")
    )
    ranking_metrics_path = resolve_project_path(
        paths.get("ranking_metrics_path", "metrics/ranking_metrics.csv")
    )

    owns_spark = spark is None
    spark_session = spark or build_spark_session(app_name="MovieRecommendationALS")

    try:
        shuffle_partitions = spark_session.conf.get("spark.sql.shuffle.partitions", "unknown")
        TRAIN_LOGGER.info(
            "event=training_start reason=%s shuffle_partitions=%s",
            retrain_reason,
            shuffle_partitions,
        )

        io_start = time.perf_counter()
        ratings_df, movies_df = _load_training_inputs(
            spark_session,
            ratings_path=ratings_path,
            movies_path=movies_path,
            feedback_path=feedback_path,
        )
        TRAIN_LOGGER.info(
            "event=stage_time stage=load_inputs seconds=%.3f ratings_count=%s movies_count=%s",
            time.perf_counter() - io_start,
            ratings_df.count(),
            movies_df.count(),
        )

        split_start = time.perf_counter()
        seed = int(training_cfg.get("seed", 42))
        split_ratio = training_cfg.get("split_ratio", [0.8, 0.2])
        train_df, test_df = ratings_df.randomSplit(split_ratio, seed=seed)
        train_df = train_df.cache()
        test_df = test_df.cache()
        train_count = train_df.count()
        test_count = test_df.count()
        TRAIN_LOGGER.info(
            "event=stage_time stage=train_test_split seconds=%.3f train_count=%s test_count=%s",
            time.perf_counter() - split_start,
            train_count,
            test_count,
        )

        rmse_rows: List[Dict[str, float | int]] = []
        best_model: ALSModel | None = None
        best_rmse = float("inf")
        best_params: Dict[str, float | int] = {}

        for rank in training_cfg.get("ranks", [20, 40]):
            for reg_param in training_cfg.get("reg_params", [0.05, 0.1]):
                for max_iter in training_cfg.get("max_iters", [10, 15]):
                    run_start = time.perf_counter()
                    als_config = ALSConfig(
                        rank=int(rank),
                        reg_param=float(reg_param),
                        max_iter=int(max_iter),
                        seed=seed,
                        cold_start_strategy="drop",
                        nonnegative=bool(training_cfg.get("nonnegative", True)),
                        implicit_prefs=bool(training_cfg.get("implicit_prefs", False)),
                    )

                    model = train_als_model(train_df, als_config)
                    predictions = model.transform(test_df)
                    rmse = evaluate_rmse(predictions)

                    row = {
                        "rank": int(rank),
                        "regParam": float(reg_param),
                        "maxIter": int(max_iter),
                        "rmse": float(rmse),
                    }
                    rmse_rows.append(row)
                    TRAIN_LOGGER.info(
                        "event=als_trial rank=%s regParam=%s maxIter=%s rmse=%.5f seconds=%.3f",
                        rank,
                        reg_param,
                        max_iter,
                        rmse,
                        time.perf_counter() - run_start,
                    )

                    if rmse < best_rmse:
                        best_rmse = rmse
                        best_model = model
                        best_params = row

        if best_model is None:
            raise RuntimeError("ALS training failed to produce a model.")

        _save_rmse_results(rmse_results_path, rmse_rows)
        model_version_path = _save_model_versions(best_model, models_dir, best_model_path)

        ranking_start = time.perf_counter()
        ranking_k = int(evaluation_cfg.get("ranking_k", config["app"].get("top_n", 10)))
        relevant_threshold = float(evaluation_cfg.get("relevant_threshold", 4.0))
        candidate_top_k = max(int(hybrid_cfg.get("candidate_top_k", 50)), ranking_k)

        test_users_df = test_df.select("userId").distinct()
        eval_candidates_df = als_candidates_for_user_subset(
            model=best_model,
            users_df=test_users_df,
            candidate_top_k=candidate_top_k,
        )
        eval_hybrid_df = build_hybrid_from_candidates(
            candidates_df=eval_candidates_df,
            ratings_df=train_df,
            movies_df=movies_df,
            hybrid_cfg=hybrid_cfg,
            top_n=ranking_k,
        )
        ranking_metrics = evaluate_ranking_metrics(
            recommendations_df=eval_hybrid_df.select("userId", "movieId", "rank"),
            test_df=test_df,
            k=ranking_k,
            relevant_threshold=relevant_threshold,
        )
        _save_ranking_metrics(
            ranking_metrics_path,
            {
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "algorithm": "hybrid_rerank",
                "k": int(ranking_metrics["k"]),
                "relevant_threshold": float(ranking_metrics["relevant_threshold"]),
                "precision_at_k": float(ranking_metrics["precision_at_k"]),
                "recall_at_k": float(ranking_metrics["recall_at_k"]),
                "ndcg_at_k": float(ranking_metrics["ndcg_at_k"]),
                "evaluated_users": int(ranking_metrics["evaluated_users"]),
            },
        )
        TRAIN_LOGGER.info(
            "event=stage_time stage=ranking_metrics seconds=%.3f precision@%s=%.5f recall@%s=%.5f ndcg@%s=%.5f users=%s",
            time.perf_counter() - ranking_start,
            ranking_k,
            ranking_metrics["precision_at_k"],
            ranking_k,
            ranking_metrics["recall_at_k"],
            ranking_k,
            ranking_metrics["ndcg_at_k"],
            ranking_metrics["evaluated_users"],
        )

        explain_start = time.perf_counter()
        _save_optimization_explains(
            model=best_model,
            movies_df=movies_df,
            top_n=int(config["app"].get("top_n", 10)),
            before_path=explain_before_path,
            after_path=explain_after_path,
        )
        TRAIN_LOGGER.info(
            "event=stage_time stage=explain_optimization seconds=%.3f",
            time.perf_counter() - explain_start,
        )

        precompute_start = time.perf_counter()
        precompute_top_n = int(precompute_cfg.get("top_n", max(candidate_top_k, int(config["app"].get("top_n", 10)))))
        precomputed_df = build_hybrid_precomputed_recommendations(
            model=best_model,
            ratings_df=ratings_df,
            movies_df=movies_df,
            hybrid_cfg=hybrid_cfg,
            top_n=precompute_top_n,
        ).cache()
        precomputed_count = precomputed_df.count()
        save_precomputed_recommendations(
            recommendations_df=precomputed_df,
            output_path=precomputed_output_path,
            partition_buckets=int(precompute_cfg.get("partition_buckets", 64)),
        )
        TRAIN_LOGGER.info(
            "event=stage_time stage=precompute_hybrid seconds=%.3f rows=%s path=%s",
            time.perf_counter() - precompute_start,
            precomputed_count,
            precomputed_output_path,
        )

        fallback_start = time.perf_counter()
        fallback_df = build_fallback_recommendations(
            ratings_df=ratings_df,
            movies_df=movies_df,
            top_n=int(fallback_cfg.get("top_n", 10)),
            min_rating_count=int(fallback_cfg.get("min_rating_count", 20)),
        )
        fallback_count = fallback_df.count()
        save_fallback_recommendations(fallback_df, fallback_output_path)
        TRAIN_LOGGER.info(
            "event=stage_time stage=fallback_build seconds=%.3f fallback_rows=%s",
            time.perf_counter() - fallback_start,
            fallback_count,
        )

        TRAIN_LOGGER.info(
            "event=training_complete best_rmse=%.5f model_path=%s",
            best_rmse,
            best_model_path,
        )

        return {
            "best_rmse": float(best_rmse),
            "best_rank": int(best_params["rank"]),
            "best_reg_param": float(best_params["regParam"]),
            "best_max_iter": int(best_params["maxIter"]),
            "best_model_path": str(best_model_path),
            "model_version_path": str(model_version_path),
            "rmse_results_path": str(rmse_results_path),
            "fallback_output_path": str(fallback_output_path),
            "precomputed_output_path": str(precomputed_output_path),
            "ranking_metrics_path": str(ranking_metrics_path),
            "precision_at_k": float(ranking_metrics["precision_at_k"]),
            "recall_at_k": float(ranking_metrics["recall_at_k"]),
            "ndcg_at_k": float(ranking_metrics["ndcg_at_k"]),
            "train_count": int(train_count),
            "test_count": int(test_count),
        }
    finally:
        if owns_spark:
            spark_session.stop()
