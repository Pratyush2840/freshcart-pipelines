"""Mage transformer block validating completeness and class cardinality."""

from __future__ import annotations

import pandas as pd

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

EXPECTED_CLASS_COUNT = 3


@transformer
def check_data(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Audit null values and distinct target classes before feature splitting."""
    null_cells = int(df.isna().sum().sum())
    class_count = int(df["target"].nunique())
    print(
        f"[check_no_missing] null_cells={null_cells} | distinct_targets={class_count}"
    )
    return df


@test
def test_data_quality(output: pd.DataFrame, *args) -> None:
    """Block downstream steps when missing cells or unexpected class counts appear."""
    null_cells = int(output.isna().sum().sum())
    assert null_cells == 0, f"Dataset contains {null_cells} missing values!"
    distinct_classes = int(output["target"].nunique())
    assert (
        distinct_classes == EXPECTED_CLASS_COUNT
    ), f"Expected {EXPECTED_CLASS_COUNT} classes, got {distinct_classes}"
