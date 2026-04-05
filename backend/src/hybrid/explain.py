"""Explainability helpers for recommendation responses."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def attach_explanations(
    recommendations_df: DataFrame,
    user_genre_profile_df: DataFrame,
    user_anchor_movies_df: DataFrame,
    movie_popularity_df: DataFrame,
) -> DataFrame:
    """Attach user-facing explanation text to each recommendation row."""
    favorite_genre_df = (
        user_genre_profile_df.filter(F.col("favorite_rank") == F.lit(1))
        .select(
            "userId",
            F.col("genre").alias("favorite_genre"),
            F.col("genre_weight").alias("favorite_genre_weight"),
        )
    )

    anchor_df = user_anchor_movies_df.select(
        F.col("userId").alias("anchor_user_id"),
        F.col("genre").alias("anchor_genre"),
        "anchor_title",
    )

    with_context = (
        recommendations_df.join(favorite_genre_df, on="userId", how="left")
        .join(
            anchor_df,
            (F.col("userId") == F.col("anchor_user_id"))
            & (F.col("primary_genre") == F.col("anchor_genre")),
            how="left",
        )
        .drop("anchor_user_id", "anchor_genre")
        .join(
            movie_popularity_df.select("movieId", "rating_count"),
            on="movieId",
            how="left",
        )
    )

    # Explanation pattern:
    # - "Because you liked X"
    # - "Users similar to you liked this"
    # - "Matches your favorite genre: Y"
    return (
        with_context.withColumn(
            "explain_because",
            F.concat(
                F.lit("Because you liked "),
                F.coalesce(F.col("anchor_title"), F.lit("similar movies")),
            ),
        )
        .withColumn(
            "explain_social",
            F.concat(
                F.lit("Users similar to you liked this ("),
                F.coalesce(F.col("rating_count"), F.lit(0)).cast("string"),
                F.lit(" ratings)"),
            ),
        )
        .withColumn(
            "explain_genre",
            F.concat(
                F.lit("Matches your favorite genre: "),
                F.coalesce(F.col("primary_genre"), F.col("favorite_genre"), F.lit("Mixed")),
            ),
        )
        .withColumn(
            "explain",
            F.concat_ws("; ", F.col("explain_because"), F.col("explain_social"), F.col("explain_genre")),
        )
    )
