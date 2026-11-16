"""
iris_project/run_c2_experiments.py
----------------------------------
Part C2 experiment runner sweeping max_depth in [1..5] to verify @test gating.
"""

from __future__ import annotations

import os
import pandas as pd

from iris_project.run_pipeline import PREDICTIONS_FILE, execute_pipeline

SUMMARY_CSV = "c2_experiment_results.csv"


def _ensure_clean_output(path: str) -> None:
    if os.path.exists(path):
        os.remove(path)
        print(f"[PRE-CHECK] Removed stale {path}")
    else:
        print(f"[PRE-CHECK] Verified {path} does not exist prior to run.")
    assert not os.path.exists(path), f"{path} must be absent before experiment starts!"


def run_experiments(depths: tuple[int, ...] = (1, 2, 3, 4, 5)) -> pd.DataFrame:
    """Run the Iris DAG across multiple tree depths and record gate outcomes."""
    records: list[dict[str, object]] = []

    for depth in depths:
        print("\n" + "#" * 65)
        print(f"# EXPERIMENT FOR max_depth = {depth}")
        print("#" * 65)

        _ensure_clean_output(PREDICTIONS_FILE)
        outcome = execute_pipeline(
            max_depth=depth,
            include_custom_block=False,
            include_eval_branch=False,
        )
        csv_present = os.path.exists(PREDICTIONS_FILE)

        records.append(
            {
                "max_depth": depth,
                "Test accuracy": outcome["accuracy"],
                "test_accuracy passed?": "YES" if outcome["test_passed"] else "NO (Failed)",
                "iris_predictions.csv created?": "YES" if csv_present else "NO (Gated/Skipped)",
            }
        )

    summary_df = pd.DataFrame(records)
    print("\n" + "=" * 65)
    print("FINAL EXPERIMENT RESULTS (Part C2):")
    print("=" * 65)
    print(summary_df.to_string(index=False))
    summary_df.to_csv(SUMMARY_CSV, index=False)
    return summary_df


if __name__ == "__main__":
    run_experiments()
