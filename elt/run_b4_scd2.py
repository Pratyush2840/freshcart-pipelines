"""Part B4 experiment: Customer SCD Type 2 update and Gold MERGE idempotency proof."""

from __future__ import annotations

import pandas as pd

from elt.pipeline_runner import (
    CRM_TIER_EVENTS_BASE,
    fetch_dataframe,
    fetch_rows,
    open_snowflake_connection,
    run_pipeline,
    use_freshcart_context,
)


def run_b4() -> None:
    """Append Arjun's Gold tier event, rerun ELT, and verify only ORD-1002 updates in Gold."""
    print("=" * 66)
    print("  PART B4: SCD TYPE 2 CHANGE & IDEMPOTENCY EXECUTION")
    print("=" * 66)

    conn = open_snowflake_connection()
    try:
        cur = conn.cursor()
        use_freshcart_context(cur)

        gold_snapshot_sql = (
            "SELECT order_id, tier, multiplier, loyalty_points, row_hash, published_at "
            "FROM gold.fct_orders ORDER BY order_id"
        )

        print("\n--- 1. Capturing published_at timestamps BEFORE B4 run ---")
        gold_before = fetch_dataframe(cur, gold_snapshot_sql)
        print(gold_before.to_string(index=False))

        arjun_upgrade = {
            "cust_id": "CUST-02",
            "email": "arjun.k@yahoo.in",
            "tier": "Gold",
            "effective_from": "2026-08-15",
        }
        augmented_crm = [*CRM_TIER_EVENTS_BASE, arjun_upgrade]

        print("\n--- 2. Executing pipeline with new CRM tier event for Arjun ---")
        print(f"Added event: {arjun_upgrade}")
        run_pipeline(
            conn,
            crm_events=augmented_crm,
            include_unsupported_currency=True,
            include_sixth_check=False,
        )

        print("\n--- 3. Querying dim.customers_scd2 for CUST-02 ---")
        scd2_arjun = fetch_dataframe(
            cur,
            "SELECT cust_id, email, tier, valid_from, valid_to, is_current "
            "FROM dim.customers_scd2 WHERE cust_id = 'CUST-02' ORDER BY valid_from",
        )
        print(scd2_arjun.to_string(index=False))

        print("\n--- 4. Querying ORD-1002 in gold.fct_orders ---")
        ord_1002_df = fetch_dataframe(
            cur,
            "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, "
            "fx_rate, amount_inr, loyalty_points FROM gold.fct_orders WHERE order_id = 'ORD-1002'",
        )
        print(ord_1002_df.to_string(index=False))

        print("\n--- 5. Querying Gold totals ---")
        orders, rev, points = fetch_rows(
            cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders"
        )[0]
        print(f"Gold Totals: {orders} orders, revenue ₹{rev:,.2f}, {points} loyalty points")

        print("\n--- 6. Inspecting published_at changes across all Gold orders ---")
        gold_after = fetch_dataframe(cur, gold_snapshot_sql)
        print(gold_after.to_string(index=False))

        ts_diff = pd.merge(
            gold_before[["ORDER_ID", "PUBLISHED_AT"]],
            gold_after[["ORDER_ID", "PUBLISHED_AT"]],
            on="ORDER_ID",
            suffixes=("_BEFORE", "_AFTER"),
        )
        ts_diff["CHANGED"] = ts_diff["PUBLISHED_AT_BEFORE"] != ts_diff["PUBLISHED_AT_AFTER"]
        print("\n[published_at comparison]:")
        print(ts_diff.to_string(index=False))

        comparison_rows = [
            ("Rows for CUST-02 in dim.customers_scd2", "2", str(len(scd2_arjun))),
            ("Tier used for ORD-1002", "Gold", str(ord_1002_df["TIER"].iloc[0])),
            ("Loyalty points for ORD-1002", "70", str(int(ord_1002_df["LOYALTY_POINTS"].iloc[0]))),
            ("Total loyalty points in Gold", "161", str(int(points))),
            ("Number of Gold rows whose published_at changed", "1", str(int(ts_diff["CHANGED"].sum()))),
        ]
        comparison_df = pd.DataFrame(
            comparison_rows, columns=["Quantity", "Predicted Value", "Actual Value"]
        )
        print("\n" + "=" * 66)
        print("  PREDICTIONS VS ACTUAL RESULTS (Part B4)")
        print("=" * 66)
        print(comparison_df.to_string(index=False))
        print("=" * 66 + "\n")
    finally:
        conn.close()


if __name__ == "__main__":
    run_b4()
