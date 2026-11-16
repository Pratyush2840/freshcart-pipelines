"""Mage data exporter block persisting scored test predictions to CSV."""

from __future__ import annotations

import pandas as pd

if "data_exporter" not in globals():
    try:
        from mage_ai.data_preparation.decorators import data_exporter
    except ImportError:
        data_exporter = lambda fn: fn  # noqa: E731

PREDICTIONS_CSV = "iris_predictions.csv"


@data_exporter
def export_data(df: pd.DataFrame, *args, **kwargs) -> None:
    """Write scored predictions DataFrame to disk as CSV."""
    df.to_csv(PREDICTIONS_CSV, index=False)
    print(f"[save_results] Exported {len(df)} predictions to {PREDICTIONS_CSV}")
