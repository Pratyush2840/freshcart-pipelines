"""Part B2 lineage tracer for ORD-1003 and Priya's SCD2 orders (ORD-1001 vs ORD-1005)."""

from __future__ import annotations

from elt.pipeline_runner import fetch_dataframe, fetch_rows, open_snowflake_connection, use_freshcart_context


def trace_records() -> None:
    """Inspect Bronze-to-Gold lineage for ORD-1003 and CUST-01 SCD2 tier history."""
    conn = open_snowflake_connection()
    try:
        cur = conn.cursor()
        use_freshcart_context(cur)

        print("=== ORD-1003 IN BRONZE ===")
        bronze_rows = fetch_rows(
            cur,
            "SELECT ingest_seq, source, raw FROM bronze.orders_raw "
            "WHERE raw:order_id::STRING = 'ORD-1003' OR raw:orderId::STRING = 'ORD-1003' "
            "ORDER BY ingest_seq",
        )
        for seq, source, raw_payload in bronze_rows:
            print(f"ingest_seq={seq} | source={source} | raw={raw_payload}")

        lineage_queries = [
            (
                "=== ORD-1003 IN SILVER.ORDERS_CHECKED ===",
                "SELECT ingest_seq, source, order_id, email, name, order_date, amount, currency, "
                "pincode, updated_at, reasons FROM silver.orders_checked "
                "WHERE order_id = 'ORD-1003' ORDER BY ingest_seq",
            ),
            (
                "=== ORD-1003 IN SILVER.ORDERS_DEDUP ===",
                "SELECT ingest_seq, source, order_id, email, name, order_date, amount, currency, "
                "pincode, updated_at FROM silver.orders_dedup WHERE order_id = 'ORD-1003'",
            ),
            (
                "=== ORD-1003 IN GOLD ===",
                "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, "
                "fx_rate, amount_inr, loyalty_points FROM gold.fct_orders WHERE order_id = 'ORD-1003'",
            ),
            (
                "=== PRIYA TIER HISTORY IN DIM.CUSTOMERS_SCD2 ===",
                "SELECT cust_id, email, tier, valid_from, valid_to, is_current "
                "FROM dim.customers_scd2 WHERE cust_id = 'CUST-01' ORDER BY valid_from",
            ),
            (
                "=== PRIYA ORDERS IN GOLD (ORD-1001 vs ORD-1005) ===",
                "SELECT order_id, order_date, cust_id, tier, multiplier, amount, currency, "
                "fx_rate, amount_inr, loyalty_points FROM gold.fct_orders "
                "WHERE cust_id = 'CUST-01' ORDER BY order_date",
            ),
        ]

        for heading, sql in lineage_queries:
            print(f"\n{heading}")
            print(fetch_dataframe(cur, sql).to_string(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    trace_records()
