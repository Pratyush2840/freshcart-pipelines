"""Mage data loader block for the Iris classification dataset."""

from __future__ import annotations

import pandas as pd
from sklearn.datasets import load_iris

if "data_loader" not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_loader
    except ImportError:
        data_loader = lambda fn: fn  # noqa: E731

if "test" not in globals():
    try:
        from mage_ai.data_preparation.decorators import test
    except ImportError:
        test = lambda fn: fn  # noqa: E731


@data_loader
def load_data(*args, **kwargs) -> pd.DataFrame:
    """Fetch the scikit-learn Iris dataset as a pandas DataFrame."""
    dataset = load_iris(as_frame=True)
    frame = dataset.frame.copy()
    print(f"[load_iris] Loaded {len(frame)} records with {frame.shape[1]} columns.")
    return frame


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """Verify the Iris loader yields the expected 150 records."""
    assert len(output) == 150, f"Expected 150 Iris rows, got {len(output)}"
