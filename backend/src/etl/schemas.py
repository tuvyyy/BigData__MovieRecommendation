"""Explicit Spark schemas for MovieLens datasets."""

from __future__ import annotations

from pyspark.sql.types import FloatType, IntegerType, LongType, StringType, StructField, StructType

RATINGS_SCHEMA = StructType(
    [
        StructField("userId", IntegerType(), True),
        StructField("movieId", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("timestamp", LongType(), True),
    ]
)

MOVIES_SCHEMA = StructType(
    [
        StructField("movieId", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True),
    ]
)
