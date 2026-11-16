"""Unit tests for the Iris Mage preparation and validation blocks."""

from __future__ import annotations

from sklearn.datasets import load_iris

from iris_project.transformers.check_no_missing import check_data, test_data_quality
from iris_project.transformers.prepare_data import test_output, transform


def test_split_is_80_20() -> None:
    dataset = load_iris(as_frame=True).frame
    split_df = transform(dataset)
    assert (split_df["split"] == "train").sum() == 120
    assert (split_df["split"] == "test").sum() == 30


def test_split_column_exists() -> None:
    dataset = load_iris(as_frame=True).frame
    split_df = transform(dataset)
    assert "split" in split_df.columns


def test_no_rows_lost() -> None:
    dataset = load_iris(as_frame=True).frame
    split_df = transform(dataset)
    assert len(split_df) == len(dataset) == 150


def test_quality_and_split_gates_pass() -> None:
    dataset = load_iris(as_frame=True).frame
    checked = check_data(dataset)
    test_data_quality(checked)
    split_df = transform(checked)
    test_output(split_df)
