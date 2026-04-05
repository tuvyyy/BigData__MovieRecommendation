"""Cold-start fallback recommendation builders."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_fallback_recommendations(
    ratings_df: DataFrame,
    movies_df: DataFrame,
    top_n: int,
    min_rating_count: int,
) -> DataFrame:
    """Build global and genre-level Top-N fallback recommendations."""
    movie_stats = (
        ratings_df.groupBy("movieId")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .filter(F.col("rating_count") >= F.lit(min_rating_count))
        .withColumn(
            "popularity_score",
            F.col("avg_rating") * F.log1p(F.col("rating_count")),
        )
    )

    ranked_global = (
        movie_stats.join(movies_df, on="movieId", how="inner")
        .orderBy(F.desc("popularity_score"), F.desc("rating_count"), F.asc("movieId"))
        .limit(top_n)
        .withColumn(
            "rank",
            F.row_number().over(Window.orderBy(F.desc("popularity_score"), F.desc("rating_count"))),
        )
        .withColumn("fallback_type", F.lit("global"))
        .withColumn("genre", F.lit(None).cast("string"))
    )

    movies_with_genre = movies_df.withColumn("genre", F.explode(F.split(F.col("genres"), "\\|")))
    genre_stats = (
        ratings_df.join(movies_with_genre, on="movieId", how="inner")
        .groupBy("genre", "movieId", "title", "genres")
        .agg(
            F.count("*").alias("rating_count"),
            F.avg("rating").alias("avg_rating"),
        )
        .filter(F.col("rating_count") >= F.lit(min_rating_count))
        .withColumn(
            "popularity_score",
            F.col("avg_rating") * F.log1p(F.col("rating_count")),
        )
    )
    genre_window = Window.partitionBy("genre").orderBy(
        F.desc("popularity_score"),
        F.desc("rating_count"),
        F.asc("movieId"),
    )
    ranked_by_genre = (
        genre_stats.withColumn("rank", F.row_number().over(genre_window))
        .filter(F.col("rank") <= F.lit(top_n))
        .withColumn("fallback_type", F.lit("genre"))
    )

    return ranked_global.select(
        "fallback_type",
        "genre",
        "rank",
        "movieId",
        "title",
        "genres",
        "rating_count",
        "avg_rating",
        "popularity_score",
    ).unionByName(
        ranked_by_genre.select(
            "fallback_type",
            "genre",
            "rank",
            "movieId",
            "title",
            "genres",
            "rating_count",
            "avg_rating",
            "popularity_score",
        )
    )


def save_fallback_recommendations(fallback_df: DataFrame, output_path: Path) -> None:
    """Persist fallback recommendations to Gold layer."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fallback_df.write.mode("overwrite").parquet(str(output_path))
