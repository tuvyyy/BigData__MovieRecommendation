"""Production ETL pipeline for MovieLens data."""

from __future__ import annotations

import io
from contextlib import redirect_stdout
from pathlib import Path
from typing import Dict, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl.schemas import MOVIES_SCHEMA, RATINGS_SCHEMA
from utils.config import load_app_config, resolve_project_path
from utils.logger import get_logger
from utils.spark_session import build_spark_session

NUM_USER_BUCKETS = 64
ETL_LOGGER = get_logger("etl", log_file="logs/etl.log")


def _capture_explain(df: DataFrame, section_name: str) -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        df.explain(mode="formatted")
    return f"=== {section_name} ===\n{buffer.getvalue().strip()}\n"


def _save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _resolve_raw_file(raw_dir: Path, base_name: str) -> Path:
    direct_candidates = [
        raw_dir / f"{base_name}.csv",
        raw_dir / f"{base_name}.dat",
    ]
    for candidate in direct_candidates:
        if candidate.exists():
            return candidate

    recursive_candidates = sorted(raw_dir.rglob(f"{base_name}.csv")) + sorted(
        raw_dir.rglob(f"{base_name}.dat")
    )
    if recursive_candidates:
        return recursive_candidates[0]

    raise FileNotFoundError(
        f"Cannot find {base_name}.csv or {base_name}.dat under: {raw_dir}"
    )


def _csv_has_header(path: Path) -> bool:
    with path.open("r", encoding="utf-8", errors="ignore") as file:
        first_line = file.readline().strip().lower()
    return "userid" in first_line or "movieid" in first_line


def _read_ratings(spark: SparkSession, ratings_path: Path) -> DataFrame:
    if ratings_path.suffix.lower() == ".dat":
        return (
            spark.read.format("csv")
            .option("header", False)
            .option("delimiter", "::")
            .schema(RATINGS_SCHEMA)
            .load(str(ratings_path))
        )

    return (
        spark.read.format("csv")
        .option("header", _csv_has_header(ratings_path))
        .schema(RATINGS_SCHEMA)
        .load(str(ratings_path))
    )


def _read_movies(spark: SparkSession, movies_path: Path) -> DataFrame:
    if movies_path.suffix.lower() == ".dat":
        return (
            spark.read.format("csv")
            .option("header", False)
            .option("delimiter", "::")
            .option("encoding", "ISO-8859-1")
            .schema(MOVIES_SCHEMA)
            .load(str(movies_path))
        )

    return (
        spark.read.format("csv")
        .option("header", _csv_has_header(movies_path))
        .schema(MOVIES_SCHEMA)
        .load(str(movies_path))
    )


def _clean_ratings(ratings_df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    before_count = ratings_df.count()
    cleaned_df = (
        ratings_df.select(
            F.col("userId").cast("int").alias("userId"),
            F.col("movieId").cast("int").alias("movieId"),
            F.col("rating").cast("float").alias("rating"),
            F.col("timestamp").cast("long").alias("timestamp"),
        )
        .dropna(subset=["userId", "movieId", "rating", "timestamp"])
        .filter((F.col("rating") >= F.lit(0.5)) & (F.col("rating") <= F.lit(5.0)))
        .dropDuplicates(["userId", "movieId", "timestamp"])
        .withColumn("user_bucket", F.pmod(F.col("userId"), F.lit(NUM_USER_BUCKETS)).cast("int"))
        .repartition(NUM_USER_BUCKETS, "user_bucket")
    )
    after_count = cleaned_df.count()
    return cleaned_df, {"before": before_count, "after": after_count}


def _clean_movies(movies_df: DataFrame) -> Tuple[DataFrame, Dict[str, int]]:
    before_count = movies_df.count()
    cleaned_df = (
        movies_df.select(
            F.col("movieId").cast("int").alias("movieId"),
            F.col("title").cast("string").alias("title"),
            F.col("genres").cast("string").alias("genres"),
        )
        .dropna(subset=["movieId", "title", "genres"])
        .dropDuplicates(["movieId"])
    )
    after_count = cleaned_df.count()
    return cleaned_df, {"before": before_count, "after": after_count}


def run_etl(
    app_config: dict | None = None,
    spark: SparkSession | None = None,
) -> Dict[str, str | int]:
    """Run end-to-end ETL and write Silver datasets."""
    config = app_config or load_app_config()
    paths = config["paths"]

    raw_dir = resolve_project_path(paths["raw_dir"])
    ratings_output_path = resolve_project_path(paths["silver_ratings_path"])
    movies_output_path = resolve_project_path(paths["silver_movies_path"])
    explain_output_path = resolve_project_path(paths["etl_explain_path"])

    ratings_path = _resolve_raw_file(raw_dir, "ratings")
    movies_path = _resolve_raw_file(raw_dir, "movies")
    ETL_LOGGER.info("event=raw_input_resolved ratings=%s movies=%s", ratings_path, movies_path)

    owns_spark = spark is None
    spark_session = spark or build_spark_session(app_name="MovieRecommendationETL")

    try:
        ratings_raw_df = _read_ratings(spark_session, ratings_path)
        movies_raw_df = _read_movies(spark_session, movies_path)

        ratings_clean_df, ratings_stats = _clean_ratings(ratings_raw_df)
        movies_clean_df, movies_stats = _clean_movies(movies_raw_df)

        ETL_LOGGER.info(
            "event=ratings_cleaned before=%s after=%s dropped=%s partitions=%s",
            ratings_stats["before"],
            ratings_stats["after"],
            ratings_stats["before"] - ratings_stats["after"],
            ratings_clean_df.rdd.getNumPartitions(),
        )
        ETL_LOGGER.info(
            "event=movies_cleaned before=%s after=%s dropped=%s",
            movies_stats["before"],
            movies_stats["after"],
            movies_stats["before"] - movies_stats["after"],
        )

        ratings_output_path.parent.mkdir(parents=True, exist_ok=True)
        movies_output_path.parent.mkdir(parents=True, exist_ok=True)

        ratings_clean_df.write.mode("overwrite").partitionBy("user_bucket").parquet(
            str(ratings_output_path)
        )
        movies_clean_df.write.mode("overwrite").parquet(str(movies_output_path))

        explain_content = "\n".join(
            [
                _capture_explain(ratings_clean_df, "ratings_cleaned_repartitioned"),
                _capture_explain(movies_clean_df, "movies_cleaned"),
            ]
        )
        _save_text(explain_output_path, explain_content)
        ETL_LOGGER.info("event=etl_explain_saved path=%s", explain_output_path)
        ETL_LOGGER.info(
            "event=etl_write_complete ratings_path=%s movies_path=%s",
            ratings_output_path,
            movies_output_path,
        )

        return {
            "ratings_before": ratings_stats["before"],
            "ratings_after": ratings_stats["after"],
            "movies_before": movies_stats["before"],
            "movies_after": movies_stats["after"],
            "ratings_output_path": str(ratings_output_path),
            "movies_output_path": str(movies_output_path),
            "explain_output_path": str(explain_output_path),
        }
    finally:
        if owns_spark:
            spark_session.stop()

