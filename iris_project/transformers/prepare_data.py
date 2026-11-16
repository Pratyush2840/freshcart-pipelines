"""Mage transformer block splitting Iris samples into 80/20 train/test partitions."""

from __future__ import annotations

import pandas as pd
from sklearn.model_selection import train_test_split

if "transformer" not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        transformer = lambda fn: fn  # noqa: E731

if "test" not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        test = lambda fn: fn  # noqa: E731


@transformer
def transform(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Tag each record with a deterministic 'train' or 'test' split label."""
    holdout_ratio = float(kwargs.get("test_size", 0.2))
    train_part, test_part = train_test_split(
        df, test_size=holdout_ratio, random_state=42
    )

    labeled_train = train_part.assign(split="train")
    labeled_test = test_part.assign(split="test")
    combined = pd.concat([labeled_train, labeled_test], axis=0)

    print(
        f"[prepare_data] Partitioned dataset -> train={len(labeled_train)}, test={len(labeled_test)}"
    )
    return combined


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """Confirm split column presence and exact 120/30 row counts."""
    assert "split" in output.columns, "Expected 'split' column is missing"
    train_rows = int((output["split"] == "train").sum())
    test_rows = int((output["split"] == "test").sum())
    assert train_rows == 120, f"Expected 120 train rows, found {train_rows}"
    assert test_rows == 30, f"Expected 30 test rows, found {test_rows}"
