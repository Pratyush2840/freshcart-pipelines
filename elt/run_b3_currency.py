"""Part B3 experiment: enable unsupported_currency quarantine validation."""

from __future__ import annotations

from elt.pipeline_runner import (
    fetch_dataframe,
    fetch_rows,
    open_snowflake_connection,
    run_pipeline,
    use_freshcart_context,
)


def _gold_snapshot(cur) -> tuple[int, float, int]:
    return fetch_rows(
        cur, "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders"
    )[0]


def run_b3() -> None:
    """Capture ORD-1006 quarantine state before and after enabling the currency rule."""
    print("=" * 66)
    print("  PART B3: ADD UNSUPPORTED_CURRENCY VALIDATION")
    print("=" * 66)

    conn = open_snowflake_connection()
    try:
        cur = conn.cursor()
        use_freshcart_context(cur)

        print("\n--- 1. BEFORE State: Inspecting ORD-1006 in Quarantine ---")
        reasons_before = fetch_rows(
            cur, "SELECT reasons FROM quarantine.orders WHERE order_id = 'ORD-1006'"
        )[0][0]
        q_count_before = fetch_rows(cur, "SELECT COUNT(*) FROM quarantine.orders")[0][0]
        g_orders, g_rev, g_pts = _gold_snapshot(cur)
        print(f"Quarantine rows: {q_count_before}")
        print(f"ORD-1006 reasons BEFORE: {reasons_before}")
        print(f"Gold: {g_orders} orders, revenue ₹{g_rev}, {g_pts} points")

        print("\n--- 2. Executing Pipeline with unsupported_currency Validation Rule ---")
        run_pipeline(conn, include_unsupported_currency=True, include_sixth_check=False)

        print("\n--- 3. AFTER State: Inspecting Quarantine & Gold ---")
        reasons_after = fetch_rows(
            cur, "SELECT reasons FROM silver.orders_checked WHERE order_id = 'ORD-1006'"
        )[0][0]
        q_count_after = fetch_rows(cur, "SELECT COUNT(*) FROM quarantine.orders")[0][0]
        g_orders_after, g_rev_after, g_pts_after = _gold_snapshot(cur)
        quarantine_table = fetch_dataframe(
            cur,
            "SELECT run_id, ingest_seq, source, order_id, reasons "
            "FROM quarantine.orders ORDER BY order_id, ingest_seq",
        )

        print(f"Quarantine rows AFTER rerun: {q_count_after}")
        print(f"silver.orders_checked ORD-1006 reasons AFTER: {reasons_after}")
        print(f"Gold AFTER: {g_orders_after} orders, revenue ₹{g_rev_after}, {g_pts_after} points")
        print("\n[quarantine.orders table content]:")
        print(quarantine_table.to_string(index=False))
    finally:
        conn.close()


if __name__ == "__main__":
    run_b3()
