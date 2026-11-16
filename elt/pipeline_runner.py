"""
elt/pipeline_runner.py
----------------------
FreshCart Multi-Hop ELT Orchestration Engine (Snowflake).
Coordinates Extract -> Bronze Load -> Silver Cleanse/Dedup -> Dimensions/Ref -> Gold MERGE & QA.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from elt.sample_data import (
    CRM_TIER_EVENTS_BASE,
    FX_RATE_RESPONSES,
    MOBILE_ORDERS_SAMPLE,
    MOCK_CRM_TIER_EVENTS_BASE,
    MOCK_FX_RESPONSES,
    MOCK_MOBILE_ORDERS,
    MOCK_PINCODES,
    MOCK_WEB_ORDERS,
    WEB_ORDERS_SAMPLE,
    build_pincode_lookup_payload,
    mock_pincode_response,
)
from elt.snowflake_client import (
    copy_staged_to_bronze,
    execute_sql_batch,
    fetch_dataframe,
    fetch_rows,
    get_connection,
    load_file,
    open_snowflake_connection,
    query,
    run_sql,
    show,
    stage_ndjson_batch,
    use_freshcart_context,
    write_landing,
)
from elt.sql_scripts import (
    DDL_BOOTSTRAP,
    SQL_00_SETUP,
    SQL_20_ORDERS_DEDUP,
    SQL_30_CUSTOMERS_SCD2,
    SQL_31_FX_DAILY,
    SQL_32_PINCODE_GEO,
    SQL_40_GOLD,
    SQL_CUSTOMERS_SCD2,
    SQL_FX_DAILY_SPINE,
    SQL_GOLD_ENRICH_AND_MERGE,
    SQL_ORDERS_DEDUP,
    SQL_PINCODE_GEO_MERGE,
    build_silver_cleaning_sql,
    get_sql_10,
)

CORE_QUALITY_QUERIES: dict[str, str] = {
    "rows gained or lost while enriching": (
        "SELECT (SELECT COUNT(*) FROM silver.orders_enriched) - (SELECT COUNT(*) FROM silver.orders_dedup)"
    ),
    "duplicate orders in gold": "SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM gold.fct_orders",
    "orders with no customer match": "SELECT COUNT_IF(cust_id IS NULL) FROM silver.orders_enriched",
    "orders with an amount but no FX rate": (
        "SELECT COUNT_IF(amount IS NOT NULL AND fx_rate IS NULL) FROM silver.orders_enriched"
    ),
    "orders with no city": "SELECT COUNT_IF(city IS NULL) FROM silver.orders_enriched",
}


def run_quality_checks(cur, include_sixth_check: bool = False) -> dict[str, Any]:
    """Evaluate pipeline quality assertions and print a summary table."""
    active_checks = dict(CORE_QUALITY_QUERIES)
    if include_sixth_check:
        active_checks["gold orders with negative loyalty"] = (
            "SELECT COUNT_IF(loyalty_points < 0) FROM gold.fct_orders"
        )

    summary: dict[str, Any] = {}
    for label, sql in active_checks.items():
        metric_val = fetch_rows(cur, sql)[0][0]
        outcome = "OK" if metric_val == 0 else "PROBLEM"
        print(f"   {label:38s} {metric_val}   {outcome}")
        summary[label] = {"value": metric_val, "status": outcome}

    rejected_ratio = fetch_rows(
        cur,
        "SELECT COUNT_IF(ARRAY_SIZE(reasons) > 0) / NULLIF(COUNT(*), 0) FROM silver.orders_checked",
    )[0][0]
    rejected_pct = float(rejected_ratio or 0)
    print(f"   {'share of rows rejected':38s} {rejected_pct:.0%}")
    summary["share of rows rejected"] = f"{rejected_pct:.0%}"
    return summary


def execute_elt_transformations(
    conn,
    cur,
    run_id: str,
    include_unsupported_currency: bool = False,
    resolve_missing_pincodes: bool = True,
) -> None:
    """Run Silver, Dimension/Reference, and Gold transformations for an active run_id."""
    cur.execute(f"SET run_id = '{run_id}'")
    execute_sql_batch(conn, build_silver_cleaning_sql(include_unsupported_currency))
    execute_sql_batch(conn, SQL_ORDERS_DEDUP)

    if resolve_missing_pincodes:
        unmapped_pins = [
            row[0]
            for row in fetch_rows(
                cur,
                """
                SELECT DISTINCT o.pincode
                FROM silver.orders_dedup o
                WHERE NOT EXISTS (
                  SELECT 1 FROM ref.pincode_geo g WHERE g.pincode = o.pincode
                )
                """,
            )
        ]
        print("   new pincodes to look up:", unmapped_pins)
        if unmapped_pins:
            pin_payloads = [
                {"pincode": pin, "response": build_pincode_lookup_payload(pin)}
                for pin in unmapped_pins
            ]
            pin_file = stage_ndjson_batch("pincode", pin_payloads, run_id)
            copy_staged_to_bronze(cur, pin_file, "bronze.pincode_raw", f"pincode/{run_id}")

    execute_sql_batch(conn, SQL_CUSTOMERS_SCD2)
    execute_sql_batch(conn, SQL_FX_DAILY_SPINE)
    execute_sql_batch(conn, SQL_PINCODE_GEO_MERGE)
    execute_sql_batch(conn, SQL_GOLD_ENRICH_AND_MERGE)


def run_pipeline(
    conn,
    crm_events: list[dict[str, Any]] | None = None,
    include_unsupported_currency: bool = False,
    include_sixth_check: bool = False,
) -> dict[str, Any]:
    """Execute one full Extract -> Load -> Transform -> QA cycle on Snowflake."""
    cur = conn.cursor()
    use_freshcart_context(cur)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    print(f"\n>>> Running pipeline run_id: {run_id}")

    print("1. EXTRACT: sample data -> raw files")
    web_path = stage_ndjson_batch("web", WEB_ORDERS_SAMPLE, run_id)
    mobile_path = stage_ndjson_batch("mobile", MOBILE_ORDERS_SAMPLE, run_id)
    crm_payload = crm_events if crm_events is not None else CRM_TIER_EVENTS_BASE
    crm_path = stage_ndjson_batch("crm", crm_payload, run_id)
    fx_path = stage_ndjson_batch("fx", FX_RATE_RESPONSES, run_id)

    print("2. LOAD: raw files -> bronze tables")
    copy_staged_to_bronze(cur, web_path, "bronze.orders_raw", f"orders_web/{run_id}")
    copy_staged_to_bronze(cur, mobile_path, "bronze.orders_raw", f"orders_mobile/{run_id}")
    copy_staged_to_bronze(cur, crm_path, "bronze.customers_raw", f"crm/{run_id}")
    copy_staged_to_bronze(cur, fx_path, "bronze.fx_raw", f"fx/{run_id}")

    print("3. TRANSFORM: bronze -> silver -> gold")
    execute_elt_transformations(
        conn,
        cur,
        run_id=run_id,
        include_unsupported_currency=include_unsupported_currency,
        resolve_missing_pincodes=True,
    )

    print("4. CHECK")
    qa_results = run_quality_checks(cur, include_sixth_check)
    orders, revenue, points = fetch_rows(
        cur,
        "SELECT COUNT(*), SUM(amount_inr), SUM(loyalty_points) FROM gold.fct_orders",
    )[0]
    print(f"GOLD: {orders} orders, revenue ₹{revenue}, {points} loyalty points\n")
    return {"orders": orders, "revenue": revenue, "points": points, "checks": qa_results}
