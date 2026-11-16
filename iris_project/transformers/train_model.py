"""Mage transformer block training a DecisionTreeClassifier with an accuracy gate."""

from __future__ import annotations

import joblib
import pandas as pd
from sklearn.tree import DecisionTreeClassifier

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

FEATURES: list[str] = [
    "sepal length (cm)",
    "sepal width (cm)",
    "petal length (cm)",
    "petal width (cm)",
]
MIN_ACCURACY_THRESHOLD = 0.80
MODEL_ARTIFACT_PATH = "iris_model.pkl"


@transformer
def train(df: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    """Fit a DecisionTreeClassifier on the training partition and score the test split."""
    depth_limit = int(kwargs.get("max_depth", 3))
    train_rows = df.loc[df["split"] == "train"]
    test_rows = df.loc[df["split"] == "test"].copy()

    classifier = DecisionTreeClassifier(max_depth=depth_limit, random_state=42)
    classifier.fit(train_rows[FEATURES], train_rows["target"])

    test_rows["predicted"] = classifier.predict(test_rows[FEATURES])
    score = float((test_rows["predicted"] == test_rows["target"]).mean())
    print(f"[train_model] max_depth={depth_limit} | test_accuracy={score:.3f}")

    joblib.dump(classifier, MODEL_ARTIFACT_PATH)
    return test_rows


@test
def test_accuracy(output: pd.DataFrame, *args) -> None:
    """Gate downstream exporters unless test accuracy meets the 80% minimum."""
    score = float((output["predicted"] == output["target"]).mean())
    assert score >= MIN_ACCURACY_THRESHOLD, (
        f"Model accuracy {score:.3f} is below required {MIN_ACCURACY_THRESHOLD:.0%}"
    )
