"""ALS model training primitives."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.ml.recommendation import ALS, ALSModel
from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ALSConfig:
    """Configuration holder for ALS hyperparameters."""

    user_col: str = "userId"
    item_col: str = "movieId"
    rating_col: str = "rating"
    rank: int = 20
    max_iter: int = 10
    reg_param: float = 0.1
    seed: int = 42
    cold_start_strategy: str = "drop"
    nonnegative: bool = True
    implicit_prefs: bool = False


def create_als_estimator(config: ALSConfig) -> ALS:
    """Build an ALS estimator from configuration."""
    return ALS(
        userCol=config.user_col,
        itemCol=config.item_col,
        ratingCol=config.rating_col,
        rank=config.rank,
        maxIter=config.max_iter,
        regParam=config.reg_param,
        seed=config.seed,
        coldStartStrategy=config.cold_start_strategy,
        nonnegative=config.nonnegative,
        implicitPrefs=config.implicit_prefs,
    )


def train_als_model(training_df: DataFrame, config: ALSConfig) -> ALSModel:
    """Fit ALS model for a given configuration."""
    estimator = create_als_estimator(config)
    return estimator.fit(training_df)
