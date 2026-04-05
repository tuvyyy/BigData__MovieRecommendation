"""Model evaluation utilities."""

from __future__ import annotations

import math

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def evaluate_rmse(
    predictions_df: DataFrame,
    label_col: str = "rating",
    prediction_col: str = "prediction",
) -> float:
    """Evaluate RMSE for regression-style recommendations."""
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol=label_col,
        predictionCol=prediction_col,
    )
    return float(evaluator.evaluate(predictions_df))


def evaluate_ranking_metrics(
    recommendations_df: DataFrame,
    test_df: DataFrame,
    k: int,
    relevant_threshold: float = 4.0,
) -> dict[str, float | int]:
    """Evaluate Precision@K, Recall@K and NDCG@K from ranked recommendations."""
    if k <= 0:
        raise ValueError("k must be positive")

    relevant_df = (
        test_df.filter(F.col("rating") >= F.lit(float(relevant_threshold)))
        .groupBy("userId")
        .agg(F.collect_set("movieId").alias("relevant_movie_ids"))
        .filter(F.size("relevant_movie_ids") > F.lit(0))
    )

    rec_grouped = (
        recommendations_df.filter(F.col("rank") <= F.lit(int(k)))
        .groupBy("userId")
        .agg(F.collect_list(F.struct(F.col("rank").alias("rank"), F.col("movieId").alias("movieId"))).alias("ranked_items"))
        .select(
            "userId",
            F.expr("transform(array_sort(ranked_items), x -> x.movieId)").alias("recommended_movie_ids"),
        )
    )

    base_df = (
        rec_grouped.join(relevant_df, on="userId", how="inner")
        .withColumn("hit_count", F.size(F.array_intersect("recommended_movie_ids", "relevant_movie_ids")))
        .withColumn("precision_at_k", F.col("hit_count") / F.lit(float(k)))
        .withColumn("recall_at_k", F.col("hit_count") / F.size("relevant_movie_ids"))
    )

    exploded_df = base_df.select(
        "userId",
        "relevant_movie_ids",
        F.posexplode("recommended_movie_ids").alias("position", "movieId"),
    ).withColumn(
        "is_relevant",
        F.when(F.array_contains(F.col("relevant_movie_ids"), F.col("movieId")), F.lit(1.0)).otherwise(F.lit(0.0)),
    )

    dcg_df = exploded_df.withColumn(
        "dcg_term",
        F.col("is_relevant") / F.log2(F.col("position") + F.lit(2.0)),
    ).groupBy("userId", "relevant_movie_ids").agg(F.sum("dcg_term").alias("dcg"))

    def _idcg_at_k(num_relevant: int) -> float:
        limit = min(int(num_relevant), int(k))
        if limit <= 0:
            return 0.0
        return float(sum(1.0 / math.log2(idx + 2.0) for idx in range(limit)))

    idcg_udf = F.udf(_idcg_at_k, DoubleType())
    ndcg_df = (
        dcg_df.withColumn("idcg", idcg_udf(F.size("relevant_movie_ids")))
        .withColumn("ndcg_at_k", F.when(F.col("idcg") > F.lit(0), F.col("dcg") / F.col("idcg")).otherwise(F.lit(0.0)))
        .select("userId", "ndcg_at_k")
    )

    merged_df = base_df.join(ndcg_df, on="userId", how="left").fillna({"ndcg_at_k": 0.0})
    summary = merged_df.agg(
        F.avg("precision_at_k").alias("precision_at_k"),
        F.avg("recall_at_k").alias("recall_at_k"),
        F.avg("ndcg_at_k").alias("ndcg_at_k"),
        F.count("*").alias("evaluated_users"),
    ).collect()[0]

    return {
        "precision_at_k": float(summary["precision_at_k"] or 0.0),
        "recall_at_k": float(summary["recall_at_k"] or 0.0),
        "ndcg_at_k": float(summary["ndcg_at_k"] or 0.0),
        "evaluated_users": int(summary["evaluated_users"] or 0),
        "k": int(k),
        "relevant_threshold": float(relevant_threshold),
    }
