"""Part B5 experiment: sixth quality check (negative loyalty) with failure injection."""

from __future__ import annotations

from elt.pipeline_runner import (
    CORE_QUALITY_QUERIES,
    fetch_rows,
    open_snowflake_connection,
    use_freshcart_context,
)

SIXTH_CHECK_LABEL = "gold orders with negative loyalty (6th)"
SIXTH_CHECK_SQL = "SELECT COUNT_IF(loyalty_points < 0) FROM gold.fct_orders"


def _evaluate_all_checks(cur) -> list[tuple[str, int, str]]:
    checks = {**CORE_QUALITY_QUERIES, SIXTH_CHECK_LABEL: SIXTH_CHECK_SQL}
    results: list[tuple[str, int, str]] = []
    for label, sql in checks.items():
        value = fetch_rows(cur, sql)[0][0]
        status = "OK" if value == 0 else "PROBLEM"
        print(f"   {label:42s} {value}   {status}")
        results.append((label, value, status))
    return results


def run_b5() -> None:
    """Verify the 6th quality gate passes, catches injected negative loyalty, and recovers."""
    print("=" * 66)
    print("  PART B5: SIXTH QUALITY CHECK & ELT VS ETL EXPERIMENT")
    print("=" * 66)

    conn = open_snowflake_connection()
    try:
        cur = conn.cursor()
        use_freshcart_context(cur)

        print("\n--- 1. Baseline Quality Checks (with 6th check added) ---")
        _evaluate_all_checks(cur)
        normal_val = fetch_rows(cur, SIXTH_CHECK_SQL)[0][0]
        assert normal_val == 0, f"Expected 0, got {normal_val}"
        print(f"\n[Result 1] Normal Check Output: {normal_val} -> PASS (OK)")

        mock_id = "MOCK-FAIL-999"
        print(f"\n--- 2. Injecting temporary mock order '{mock_id}' with loyalty_points = -10 ---")
        cur.execute(
            f"""
            INSERT INTO gold.fct_orders (
                order_id, order_date, cust_id, email, tier, multiplier,
                amount, currency, fx_rate, amount_inr, loyalty_points,
                is_high_value, city, state, region, updated_at, row_hash, published_at
            ) VALUES (
                '{mock_id}', '2026-09-05', 'CUST-MOCK', 'mock@test.com', 'Silver', 1,
                -500.00, 'INR', 1.0, -500.00, -10,
                FALSE, 'Bengaluru', 'Karnataka', 'South', CURRENT_TIMESTAMP(), 999999, CURRENT_TIMESTAMP()
            )
            """
        )

        print("\n--- 3. Running Quality Checks with Mock Order Present ---")
        _evaluate_all_checks(cur)
        failure_val = fetch_rows(cur, SIXTH_CHECK_SQL)[0][0]
        assert failure_val > 0, f"Expected > 0, got {failure_val}"
        print(f"\n[Result 2] Intentional Failure Output: {failure_val} -> PROBLEM (Triggered as expected!)")

        print(f"\n--- 4. Removing temporary mock order '{mock_id}' ---")
        cur.execute(f"DELETE FROM gold.fct_orders WHERE order_id = '{mock_id}'")

        print("\n--- 5. Restoring and Verifying Quality Checks ---")
        _evaluate_all_checks(cur)
        restored_val = fetch_rows(cur, SIXTH_CHECK_SQL)[0][0]
        assert restored_val == 0, f"Expected 0, got {restored_val}"
        print(f"\n[Result 3] Restored Check Output: {restored_val} -> PASS (OK, Database Clean!)")

        remaining = fetch_rows(
            cur, f"SELECT COUNT(*) FROM gold.fct_orders WHERE order_id = '{mock_id}'"
        )[0][0]
        print(f"Mock orders remaining in database: {remaining}")
        assert remaining == 0
    finally:
        conn.close()


if __name__ == "__main__":
    run_b5()
