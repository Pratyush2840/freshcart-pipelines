"""Part B1 baseline ELT execution and verification script."""

from __future__ import annotations

import pandas as pd

from elt.pipeline_runner import (
    DDL_BOOTSTRAP,
    execute_sql_batch,
    fetch_dataframe,
    fetch_rows,
    open_snowflake_connection,
    run_pipeline,
    use_freshcart_context,
)


def _print_banner(title: str) -> None:
    bar = "=" * 66
    print(f"{bar}\n  {title}\n{bar}")


def run_b1() -> list[tuple[str, str, str]]:
    """Reset the FRESHCART database, run the baseline ELT, and print verification metrics."""
    _print_banner("PART B1: FRESHCART BASELINE ELT EXECUTION (SNOWFLAKE)")

    conn = open_snowflake_connection()
    try:
        cur = conn.cursor()

        print("\n--- 1. Resetting database to clean baseline state ---")
        cur.execute("DROP DATABASE IF EXISTS FRESHCART")
        execute_sql_batch(conn, DDL_BOOTSTRAP)
        use_freshcart_context(cur)

        print("\n--- 2. Executing run_pipeline() ---")
        run_pipeline(conn, include_unsupported_currency=False, include_sixth_check=False)

        print("\n" + "=" * 66)
        print("  PART B1 BASELINE VERIFICATION RESULTS")
        print("=" * 66)

        raw_total = fetch_rows(cur, "SELECT COUNT(*) FROM bronze.orders_raw")[0][0]
        web_total = fetch_rows(cur, "SELECT COUNT(*) FROM bronze.orders_raw WHERE source = 'web'")[0][0]
        mobile_total = fetch_rows(cur, "SELECT COUNT(*) FROM bronze.orders_raw WHERE source = 'mobile'")[0][0]
        clean_total = fetch_rows(cur, "SELECT COUNT(*) FROM silver.orders_clean")[0][0]
        quarantine_total = fetch_rows(cur, "SELECT COUNT(*) FROM quarantine.orders")[0][0]
        quarantined_ids = [
            row[0] for row in fetch_rows(cur, "SELECT order_id FROM quarantine.orders ORDER BY order_id")
        ]
        dedup_total = fetch_rows(cur, "SELECT COUNT(*) FROM silver.orders_dedup")[0][0]
        gold_orders, gold_revenue, gold_points = fetch_rows(
            cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders"
        )[0]
        rejected_share = fetch_rows(
            cur,
            "SELECT COUNT_IF(ARRAY_SIZE(reasons) > 0) / NULLIF(COUNT(*), 0) FROM silver.orders_checked",
        )[0][0]

        verification_rows = [
            ("Raw rows in bronze.orders_raw", "8 (6 web + 2 mobile)", f"{raw_total} ({web_total} web + {mobile_total} mobile)"),
            ("Rows passing cleaning in silver.orders_clean", "6", str(clean_total)),
            ("Quarantined rows", "2 (ORD-1004, ORD-1006)", f"{quarantine_total} ({', '.join(quarantined_ids)})"),
            ("Rows after deduplication", "4", str(dedup_total)),
            ("Gold orders", "4", str(gold_orders)),
            ("Gold revenue", "₹7,785.00", f"₹{gold_revenue:,.2f}"),
            ("Gold loyalty points", "126", str(gold_points)),
            ("Rejected share", "25%", f"{float(rejected_share):.0%}"),
        ]

        summary_df = pd.DataFrame(verification_rows, columns=["Check", "Expected Result", "Actual Result"])
        print(summary_df.to_string(index=False))
        print("=" * 66 + "\n")

        table_queries = [
            (
                "[bronze.orders_raw]:",
                "SELECT ingest_seq, source, raw:order_id::STRING AS order_id, raw:amount::STRING AS amount "
                "FROM bronze.orders_raw ORDER BY ingest_seq",
            ),
            (
                "[quarantine.orders]:",
                "SELECT run_id, ingest_seq, source, order_id, reasons FROM quarantine.orders ORDER BY order_id",
            ),
            (
                "[silver.orders_dedup]:",
                "SELECT order_id, ingest_seq, source, email, order_date, amount, currency, updated_at "
                "FROM silver.orders_dedup ORDER BY order_id",
            ),
            (
                "[gold.fct_orders]:",
                "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, fx_rate, "
                "amount_inr, loyalty_points FROM gold.fct_orders ORDER BY order_id",
            ),
        ]
        print("--- Detailed Table Dumps ---")
        for heading, sql in table_queries:
            print(f"\n{heading}")
            print(fetch_dataframe(cur, sql).to_string(index=False))

        return verification_rows
    finally:
        conn.close()


if __name__ == "__main__":
    run_b1()
