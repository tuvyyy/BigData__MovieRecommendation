"""Run full ETL + ALS training pipeline."""

from __future__ import annotations

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from etl.pipeline import run_etl  # noqa: E402
from model.pipeline import run_training  # noqa: E402
from utils.config import load_app_config  # noqa: E402
from utils.logger import get_logger  # noqa: E402
from utils.spark_session import build_spark_session  # noqa: E402


def main() -> None:
    app_logger = get_logger("app", log_file="logs/app.log")
    config = load_app_config()
    spark = build_spark_session(app_name="MovieRecommendationPipeline")

    pipeline_start = time.perf_counter()
    try:
        app_logger.info("event=pipeline_start")

        etl_start = time.perf_counter()
        etl_summary = run_etl(app_config=config, spark=spark)
        app_logger.info(
            "event=stage_complete stage=etl seconds=%.3f details=%s",
            time.perf_counter() - etl_start,
            etl_summary,
        )

        train_start = time.perf_counter()
        train_summary = run_training(app_config=config, spark=spark)
        app_logger.info(
            "event=stage_complete stage=train seconds=%.3f details=%s",
            time.perf_counter() - train_start,
            train_summary,
        )

        print("\nPipeline completed successfully.")
        print(f"Final best RMSE: {train_summary['best_rmse']:.5f}")
        print(
            "Ranking metrics: "
            f"Precision@K={train_summary.get('precision_at_k', 0.0):.5f}, "
            f"Recall@K={train_summary.get('recall_at_k', 0.0):.5f}, "
            f"NDCG@K={train_summary.get('ndcg_at_k', 0.0):.5f}"
        )
        print(f"Best model saved to: {train_summary['best_model_path']}")
        print(f"Versioned model saved to: {train_summary['model_version_path']}")
        print(f"RMSE table saved to: {train_summary['rmse_results_path']}")
        print(f"Ranking metrics saved to: {train_summary.get('ranking_metrics_path', 'metrics/ranking_metrics.csv')}")
        print(
            "Precomputed hybrid recommendations saved to: "
            f"{train_summary.get('precomputed_output_path', 'data/gold/recommendations_precomputed')}"
        )
        print("Spark UI: http://localhost:4040 (available while Spark job is running)")
        print("For detailed stages/shuffles, open Spark UI > SQL/DataFrame tabs.")
    finally:
        total_seconds = time.perf_counter() - pipeline_start
        app_logger.info("event=pipeline_end total_seconds=%.3f", total_seconds)
        spark.stop()


if __name__ == "__main__":
    main()
