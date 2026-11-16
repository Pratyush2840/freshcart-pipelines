"""
iris_project/run_pipeline.py
----------------------------
Standalone DAG runner mirroring Mage AI block order and @test quality gates.
"""

from __future__ import annotations

import argparse
import os
from typing import Any

from iris_project.data_exporters.save_results import export_data
from iris_project.data_loaders.load_iris import load_data, test_output as verify_load
from iris_project.transformers.check_no_missing import (
    check_data,
    test_data_quality as verify_quality,
)
from iris_project.transformers.evaluate_model import evaluate
from iris_project.transformers.prepare_data import transform as split_dataset, test_output as verify_split
from iris_project.transformers.train_model import test_accuracy as verify_accuracy, train as fit_and_score

PREDICTIONS_FILE = "iris_predictions.csv"


def execute_pipeline(
    max_depth: int = 3,
    include_custom_block: bool = True,
    include_eval_branch: bool = True,
) -> dict[str, Any]:
    """Run the full Iris classification DAG with in-pipeline @test gates."""
    banner = "=" * 60
    print(f"{banner}\nExecuting Mage Pipeline: iris_classifier (max_depth={max_depth})\n{banner}")

    print("\n[1/5] Data Loader -> load_iris")
    raw_df = load_data()
    verify_load(raw_df)
    print("  -> load_iris @test passed.")

    if include_custom_block:
        print("\n[2/5] Transformer -> check_no_missing (Part C4)")
        raw_df = check_data(raw_df)
        verify_quality(raw_df)
        print("  -> check_no_missing @test passed.")

    print("\n[3/5] Transformer -> prepare_data")
    prepared_df = split_dataset(raw_df)
    verify_split(prepared_df)
    print("  -> prepare_data @test passed.")

    print(f"\n[4/5] Transformer -> train_model (max_depth={max_depth})")
    scored_df = fit_and_score(prepared_df, max_depth=max_depth)
    accuracy_val = round(float((scored_df["predicted"] == scored_df["target"]).mean()), 3)

    try:
        verify_accuracy(scored_df)
        print("  -> train_model @test test_accuracy PASSED (accuracy >= 0.80)")
    except AssertionError as exc:
        print(f"  -> [GATE BLOCKED] train_model @test FAILED: {exc}")
        print("  -> Downstream blocks (save_results, evaluate_model) skipped.")
        return {
            "max_depth": max_depth,
            "accuracy": accuracy_val,
            "test_passed": False,
            "csv_created": os.path.exists(PREDICTIONS_FILE),
        }

    print("\n[5a/5] Data Exporter -> save_results")
    export_data(scored_df)
    csv_ready = os.path.exists(PREDICTIONS_FILE)
    print(f"  -> save_results completed (csv_exists={csv_ready})")

    if include_eval_branch:
        print("\n[5b/5] Transformer -> evaluate_model (Part C3)")
        evaluate(scored_df)
        print("  -> evaluate_model completed.")

    print(f"\n{banner}\nPIPELINE EXECUTION COMPLETED SUCCESSFULLY!\n{banner}")
    return {
        "max_depth": max_depth,
        "accuracy": accuracy_val,
        "test_passed": True,
        "csv_created": csv_ready,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute the Iris Mage ML pipeline.")
    parser.add_argument("--max-depth", type=int, default=3, help="DecisionTree max_depth parameter")
    cli_args = parser.parse_args()
    execute_pipeline(max_depth=cli_args.max_depth)


if __name__ == "__main__":
    main()
