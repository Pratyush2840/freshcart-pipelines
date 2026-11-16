"""Mage transformer block computing confusion matrix and macro classification metrics."""

from __future__ import annotations

import pandas as pd
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score

if "transformer" not in globals():
    try:
        from mage_ai.data_preparation.decorators import transformer
    except ImportError:
        transformer = lambda fn: fn  # noqa: E731


@transformer
def evaluate(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Compute confusion matrix and macro-averaged classification scores."""
    actual = df["target"]
    predicted = df["predicted"]

    confusion = pd.crosstab(actual, predicted, rownames=["actual"], colnames=["predicted"])
    print("Confusion matrix:")
    print(confusion)

    metric_calculators = (
        ("accuracy", lambda: accuracy_score(actual, predicted)),
        ("precision", lambda: precision_score(actual, predicted, average="macro")),
        ("recall", lambda: recall_score(actual, predicted, average="macro")),
        ("f1_score", lambda: f1_score(actual, predicted, average="macro")),
    )
    report_df = pd.DataFrame(
        [
            {"metric": label, "value": round(float(fn()), 3)}
            for label, fn in metric_calculators
        ]
    )
    print("\nClassification Metrics:")
    print(report_df)
    return report_df
