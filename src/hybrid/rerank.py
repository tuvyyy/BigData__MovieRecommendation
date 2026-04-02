"""Re-ranking utilities for hybrid recommendation serving."""

from __future__ import annotations

from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_movie_popularity(ratings_df: DataFrame) -> DataFrame:
    """Compute a normalized popularity signal for optional re-ranking boost."""
    raw = ratings_df.groupBy("movieId").agg(
        F.count("*").alias("rating_count"),
        F.avg("rating").alias("avg_rating"),
    )
    scored = raw.withColumn(
        "popularity_score",
        F.col("avg_rating") * F.log1p(F.col("rating_count")),
    )
    norm_window = Window.partitionBy()
    max_score = F.max("popularity_score").over(norm_window)
    return scored.withColumn(
        "popularity_norm",
        F.when(max_score > 0, F.col("popularity_score") / max_score).otherwise(F.lit(0.0)),
    ).select("movieId", "rating_count", "avg_rating", "popularity_score", "popularity_norm")


def rerank_hybrid_candidates(
    scored_candidates_df: DataFrame,
    movies_df: DataFrame,
    movie_popularity_df: DataFrame,
    hybrid_cfg: Dict[str, float | int | bool],
    top_n: int,
) -> DataFrame:
    """Blend ALS and content scores, then apply diversity-aware re-ranking."""
    als_weight = float(hybrid_cfg.get("als_weight", 0.7))
    content_weight = float(hybrid_cfg.get("content_weight", 0.3))
    genre_boost = float(hybrid_cfg.get("genre_boost", 0.12))
    diversity_penalty = float(hybrid_cfg.get("diversity_penalty", 0.08))
    popularity_boost = float(hybrid_cfg.get("popularity_boost", 0.08))
    recency_boost = float(hybrid_cfg.get("recency_boost", 0.03))

    movies_features = movies_df.select(
        "movieId",
        "title",
        "genres",
        F.element_at(F.split(F.col("genres"), "\\|"), 1).alias("primary_genre"),
        F.regexp_extract(F.col("title"), r"\((\d{4})\)\s*$", 1).cast("int").alias("release_year"),
    )

    base = (
        scored_candidates_df.join(movies_features, on="movieId", how="left")
        .join(movie_popularity_df, on="movieId", how="left")
        .fillna({"content_score": 0.0, "max_genre_weight": 0.0, "genre_match_count": 0, "popularity_norm": 0.0})
        .withColumn(
            "als_norm",
            F.greatest(F.lit(0.0), F.least(F.lit(1.0), (F.col("als_score") - F.lit(0.5)) / F.lit(4.5))),
        )
        # Hybrid blend is the main score: collaborative + content signal.
        .withColumn(
            "hybrid_score",
            (F.col("als_norm") * F.lit(als_weight)) + (F.col("content_score") * F.lit(content_weight)),
        )
    )

    all_window = Window.partitionBy()
    min_year = F.min("release_year").over(all_window)
    max_year = F.max("release_year").over(all_window)

    with_recency = base.withColumn(
        "recency_norm",
        F.when(
            (F.col("release_year").isNotNull()) & (max_year > min_year),
            (F.col("release_year") - min_year) / (max_year - min_year),
        ).otherwise(F.lit(0.0)),
    )

    genre_window = Window.partitionBy("userId", "primary_genre").orderBy(
        F.desc("hybrid_score"),
        F.desc("popularity_norm"),
        F.asc("movieId"),
    )

    with_diversity = with_recency.withColumn("genre_rank", F.row_number().over(genre_window))
    reranked = with_diversity.withColumn(
        "rerank_score",
        F.col("hybrid_score")
        + (F.col("max_genre_weight") * F.lit(genre_boost))
        + (F.col("popularity_norm") * F.lit(popularity_boost))
        + (F.col("recency_norm") * F.lit(recency_boost))
        - ((F.col("genre_rank") - F.lit(1)) * F.lit(diversity_penalty)),
    )

    final_rank_window = Window.partitionBy("userId").orderBy(
        F.desc("rerank_score"),
        F.desc("hybrid_score"),
        F.desc("popularity_norm"),
        F.asc("movieId"),
    )
    return (
        reranked.withColumn("rank", F.row_number().over(final_rank_window))
        .filter(F.col("rank") <= F.lit(int(top_n)))
    )

