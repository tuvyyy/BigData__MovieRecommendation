"""Offline hybrid recommendation precomputation utilities."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

from pyspark.ml.recommendation import ALSModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from hybrid.content_based import (
    build_user_anchor_movies,
    build_user_genre_profile,
    score_content_similarity,
)
from hybrid.explain import attach_explanations
from hybrid.rerank import build_movie_popularity, rerank_hybrid_candidates


def als_candidates_for_all_users(model: ALSModel, candidate_top_k: int) -> DataFrame:
    """Generate ALS candidates for every known user."""
    return (
        model.recommendForAllUsers(int(candidate_top_k))
        .select("userId", F.explode("recommendations").alias("rec"))
        .select(
            F.col("userId").cast("int").alias("userId"),
            F.col("rec.movieId").cast("int").alias("movieId"),
            F.col("rec.rating").cast("float").alias("als_score"),
        )
    )


def als_candidates_for_user_subset(model: ALSModel, users_df: DataFrame, candidate_top_k: int) -> DataFrame:
    """Generate ALS candidates for a specific subset of users."""
    return (
        model.recommendForUserSubset(users_df.select("userId").distinct(), int(candidate_top_k))
        .select("userId", F.explode("recommendations").alias("rec"))
        .select(
            F.col("userId").cast("int").alias("userId"),
            F.col("rec.movieId").cast("int").alias("movieId"),
            F.col("rec.rating").cast("float").alias("als_score"),
        )
    )


def build_hybrid_from_candidates(
    candidates_df: DataFrame,
    ratings_df: DataFrame,
    movies_df: DataFrame,
    hybrid_cfg: Dict[str, float | int | bool],
    top_n: int,
) -> DataFrame:
    """Build hybrid recommendations from ALS candidates + content + re-ranking."""
    ratings_core = ratings_df.select(
        F.col("userId").cast("int").alias("userId"),
        F.col("movieId").cast("int").alias("movieId"),
        F.col("rating").cast("float").alias("rating"),
        F.col("timestamp").cast("long").alias("timestamp"),
    )
    movies_core = movies_df.select(
        F.col("movieId").cast("int").alias("movieId"),
        F.col("title").cast("string").alias("title"),
        F.col("genres").cast("string").alias("genres"),
    )

    seen_pairs = ratings_core.select("userId", "movieId").dropDuplicates()
    fresh_candidates = candidates_df.join(seen_pairs, on=["userId", "movieId"], how="left_anti")

    user_profile_df = build_user_genre_profile(ratings_core, movies_core).cache()
    user_anchor_df = build_user_anchor_movies(ratings_core, movies_core)
    popularity_df = build_movie_popularity(ratings_core).cache()
    user_profile_df.count()
    popularity_df.count()

    content_scored_df = score_content_similarity(
        candidates_df=fresh_candidates,
        movies_df=movies_core,
        user_genre_profile_df=user_profile_df,
    )
    reranked_df = rerank_hybrid_candidates(
        scored_candidates_df=content_scored_df,
        movies_df=movies_core,
        movie_popularity_df=popularity_df,
        hybrid_cfg=hybrid_cfg,
        top_n=int(top_n),
    )
    explained_df = attach_explanations(
        recommendations_df=reranked_df,
        user_genre_profile_df=user_profile_df,
        user_anchor_movies_df=user_anchor_df,
        movie_popularity_df=popularity_df,
    )
    return explained_df


def build_hybrid_precomputed_recommendations(
    model: ALSModel,
    ratings_df: DataFrame,
    movies_df: DataFrame,
    hybrid_cfg: Dict[str, float | int | bool],
    top_n: int,
) -> DataFrame:
    """Generate full-user hybrid recommendations ready for online serving."""
    candidate_top_k = max(int(hybrid_cfg.get("candidate_top_k", 50)), int(top_n))
    candidates_df = als_candidates_for_all_users(model=model, candidate_top_k=candidate_top_k)
    hybrid_df = build_hybrid_from_candidates(
        candidates_df=candidates_df,
        ratings_df=ratings_df,
        movies_df=movies_df,
        hybrid_cfg=hybrid_cfg,
        top_n=int(top_n),
    )
    return (
        hybrid_df.withColumn("route", F.lit("hybrid"))
        .withColumn("score", F.col("rerank_score"))
        .withColumn("user_bucket", F.pmod(F.col("userId"), F.lit(64)).cast("int"))
    )


def save_precomputed_recommendations(
    recommendations_df: DataFrame,
    output_path: Path,
    partition_buckets: int = 64,
) -> None:
    """Persist precomputed recommendations in Gold layer."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    to_write = recommendations_df.withColumn(
        "user_bucket",
        F.pmod(F.col("userId"), F.lit(int(partition_buckets))).cast("int"),
    )
    to_write.write.mode("overwrite").partitionBy("user_bucket").parquet(str(output_path))

