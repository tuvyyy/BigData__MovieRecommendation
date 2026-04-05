"""Content-based scoring utilities for hybrid recommendations."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_user_genre_profile(ratings_df: DataFrame, movies_df: DataFrame) -> DataFrame:
    """Create normalized per-user genre preferences from watch/rating history."""
    movies_genre = movies_df.select(
        "movieId",
        F.explode(F.split(F.col("genres"), "\\|")).alias("genre"),
    ).filter(F.length(F.trim(F.col("genre"))) > F.lit(0))

    user_genre_raw = (
        ratings_df.join(movies_genre, on="movieId", how="inner")
        .groupBy("userId", "genre")
        .agg(
            F.count("*").alias("genre_interactions"),
            F.avg("rating").alias("genre_avg_rating"),
            F.sum("rating").alias("genre_sum_rating"),
        )
        # Blend engagement count and average rating to avoid sparse bias.
        .withColumn(
            "raw_weight",
            F.col("genre_sum_rating") + (F.col("genre_interactions") * F.lit(0.25)),
        )
    )

    user_window = Window.partitionBy("userId")
    rank_window = Window.partitionBy("userId").orderBy(
        F.desc("genre_weight"),
        F.desc("genre_interactions"),
        F.asc("genre"),
    )
    max_weight = F.max("raw_weight").over(user_window)

    return (
        user_genre_raw.withColumn("genre_weight", F.when(max_weight > 0, F.col("raw_weight") / max_weight).otherwise(F.lit(0.0)))
        .withColumn("favorite_rank", F.row_number().over(rank_window))
        .select(
            "userId",
            "genre",
            "genre_interactions",
            "genre_avg_rating",
            "genre_weight",
            "favorite_rank",
        )
    )


def build_user_anchor_movies(ratings_df: DataFrame, movies_df: DataFrame) -> DataFrame:
    """Find representative movies users liked in each genre for explanations."""
    movies_genre = movies_df.select(
        "movieId",
        "title",
        F.explode(F.split(F.col("genres"), "\\|")).alias("genre"),
    ).filter(F.length(F.trim(F.col("genre"))) > F.lit(0))

    window_spec = Window.partitionBy("userId", "genre").orderBy(
        F.desc("rating"),
        F.desc("timestamp"),
        F.asc("movieId"),
    )
    return (
        ratings_df.join(movies_genre, on="movieId", how="inner")
        .withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == F.lit(1))
        .select(
            "userId",
            "genre",
            F.col("movieId").alias("anchor_movieId"),
            F.col("title").alias("anchor_title"),
            F.col("rating").alias("anchor_rating"),
        )
    )


def score_content_similarity(
    candidates_df: DataFrame,
    movies_df: DataFrame,
    user_genre_profile_df: DataFrame,
) -> DataFrame:
    """Score candidate movies by genre overlap with user content profile."""
    movie_genres = movies_df.select(
        "movieId",
        F.explode(F.split(F.col("genres"), "\\|")).alias("genre"),
    ).filter(F.length(F.trim(F.col("genre"))) > F.lit(0))

    scored = (
        candidates_df.join(movie_genres, on="movieId", how="left")
        .join(
            user_genre_profile_df.select("userId", "genre", "genre_weight"),
            on=["userId", "genre"],
            how="left",
        )
        .groupBy("userId", "movieId", "als_score")
        .agg(
            F.avg(F.coalesce(F.col("genre_weight"), F.lit(0.0))).alias("content_score"),
            F.max(F.coalesce(F.col("genre_weight"), F.lit(0.0))).alias("max_genre_weight"),
            F.sum(F.when(F.col("genre_weight").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("genre_match_count"),
        )
    )
    return scored

