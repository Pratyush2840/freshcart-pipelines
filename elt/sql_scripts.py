"""Snowflake DDL and multi-hop ELT SQL definitions for the FreshCart lakehouse."""

from __future__ import annotations

DDL_BOOTSTRAP = r"""
CREATE WAREHOUSE IF NOT EXISTS FRESHCART_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;
USE WAREHOUSE FRESHCART_WH;

CREATE DATABASE IF NOT EXISTS FRESHCART;
USE DATABASE FRESHCART;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS dim;
CREATE SCHEMA IF NOT EXISTS ref;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE FILE FORMAT IF NOT EXISTS bronze.ff_ndjson TYPE = JSON COMPRESSION = AUTO;
CREATE STAGE IF NOT EXISTS bronze.landing FILE_FORMAT = bronze.ff_ndjson;

CREATE TABLE IF NOT EXISTS bronze.orders_raw (
  ingest_seq   NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
  source       STRING,
  raw          VARIANT,
  run_id       STRING,
  extracted_at TIMESTAMP_TZ,
  src_file     STRING,
  src_row      NUMBER,
  loaded_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS bronze.customers_raw (
  ingest_seq   NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
  source       STRING,
  raw          VARIANT,
  run_id       STRING,
  extracted_at TIMESTAMP_TZ,
  src_file     STRING,
  src_row      NUMBER,
  loaded_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS bronze.fx_raw (
  ingest_seq   NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
  source       STRING,
  raw          VARIANT,
  run_id       STRING,
  extracted_at TIMESTAMP_TZ,
  src_file     STRING,
  src_row      NUMBER,
  loaded_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS bronze.pincode_raw (
  ingest_seq   NUMBER AUTOINCREMENT START 1 INCREMENT 1 ORDER,
  source       STRING,
  raw          VARIANT,
  run_id       STRING,
  extracted_at TIMESTAMP_TZ,
  src_file     STRING,
  src_row      NUMBER,
  loaded_at    TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE ref.tier_multiplier AS
SELECT * FROM VALUES ('Silver', 1), ('Gold', 2), ('Platinum', 3) AS v(tier, multiplier);

CREATE OR REPLACE TABLE ref.state_region AS
SELECT * FROM VALUES
  ('Tamil Nadu', 'South'), ('Karnataka', 'South'), ('Kerala', 'South'),
  ('Andhra Pradesh', 'South'), ('Telangana', 'South'), ('Puducherry', 'South'),
  ('Maharashtra', 'West'), ('Gujarat', 'West'), ('Goa', 'West'), ('Rajasthan', 'West'),
  ('Delhi', 'North'), ('Haryana', 'North'), ('Punjab', 'North'),
  ('Uttar Pradesh', 'North'), ('Himachal Pradesh', 'North'),
  ('Jammu and Kashmir', 'North'), ('Uttarakhand', 'North'),
  ('West Bengal', 'East'), ('Odisha', 'East'), ('Bihar', 'East'),
  ('Jharkhand', 'East'), ('Assam', 'East'),
  ('Madhya Pradesh', 'Central'), ('Chhattisgarh', 'Central')
  AS v(state, region);

CREATE TABLE IF NOT EXISTS ref.pincode_geo (
  pincode    STRING PRIMARY KEY,
  city       STRING,
  state      STRING,
  region     STRING,
  updated_at TIMESTAMP_LTZ
);

CREATE TABLE IF NOT EXISTS quarantine.orders (
  run_id         STRING,
  quarantined_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
  ingest_seq     NUMBER,
  source         STRING,
  order_id       STRING,
  reasons        ARRAY,
  row_data       VARIANT
);

CREATE TABLE IF NOT EXISTS gold.fct_orders (
  order_id       STRING PRIMARY KEY,
  order_date     DATE,
  cust_id        STRING,
  email          STRING,
  tier           STRING,
  multiplier     NUMBER,
  amount         NUMBER(12,2),
  currency       STRING,
  fx_rate        NUMBER(12,4),
  amount_inr     NUMBER(14,2),
  loyalty_points NUMBER,
  is_high_value  BOOLEAN,
  city           STRING,
  state          STRING,
  region         STRING,
  updated_at     TIMESTAMP_NTZ,
  row_hash       NUMBER,
  published_at   TIMESTAMP_LTZ
);
"""


def build_silver_cleaning_sql(enforce_supported_currency: bool = False) -> str:
    """Build the Bronze-to-Silver cleansing and quarantine SQL block."""
    currency_check = (
        "    IFF(currency NOT IN ('INR', 'USD'), 'unsupported_currency', NULL),\n"
        if enforce_supported_currency
        else ""
    )
    return rf"""
CREATE OR REPLACE TRANSIENT TABLE silver.orders_checked AS
WITH unified_sources AS (
  SELECT
    ingest_seq,
    source,
    raw:order_id::STRING   AS order_id,
    raw:cust_email::STRING AS cust_email,
    raw:cust_name::STRING  AS cust_name,
    raw:order_date::STRING AS order_date,
    raw:amount::STRING     AS amount,
    raw:currency::STRING   AS currency,
    raw:pincode::STRING    AS pincode,
    raw:updated_at::STRING AS updated_at
  FROM bronze.orders_raw
  WHERE source = 'web'
  UNION ALL
  SELECT
    ingest_seq,
    source,
    raw:orderId::STRING,
    raw:customer.email::STRING,
    raw:customer.name::STRING,
    raw:orderDate::STRING,
    raw:total.value::STRING,
    raw:total.currency::STRING,
    raw:deliveryPin::STRING,
    raw:updatedAt::STRING
  FROM bronze.orders_raw
  WHERE source = 'mobile'
),
standardized AS (
  SELECT
    ingest_seq,
    source,
    TRIM(order_id)                       AS order_id,
    LOWER(TRIM(cust_email))              AS email,
    INITCAP(RTRIM(TRIM(cust_name), '.')) AS name,
    COALESCE(
      TRY_TO_DATE(order_date, 'YYYY-MM-DD'),
      TRY_TO_DATE(order_date, 'DD/MM/YYYY'),
      TRY_TO_DATE(order_date, 'MON DD, YYYY')
    )                                    AS order_date,
    TRY_TO_DECIMAL(REGEXP_REPLACE(amount, '[^0-9.-]', ''), 12, 2) AS amount,
    UPPER(TRIM(currency))                AS currency,
    TRIM(pincode)                        AS pincode,
    TRY_TO_TIMESTAMP_NTZ(updated_at)     AS updated_at
  FROM unified_sources
)
SELECT
  *,
  ARRAY_CONSTRUCT_COMPACT(
    IFF(order_id IS NULL, 'missing_order_id', NULL),
    IFF(email IS NULL OR NOT REGEXP_LIKE(email, '^[^@\\s]+@[^@\\s]+\\.[a-z]{{2,}}$'), 'bad_email', NULL),
    IFF(pincode IS NULL OR NOT REGEXP_LIKE(pincode, '^[1-9][0-9]{{5}}$'), 'bad_pincode', NULL),
    IFF(order_date IS NULL, 'unparseable_date', NULL),
    IFF(updated_at IS NULL, 'unparseable_updated_at', NULL),
{currency_check}    IFF(amount <= 0, 'non_positive_amount', NULL)
  ) AS reasons
FROM standardized;

CREATE OR REPLACE TABLE silver.orders_clean AS
SELECT * EXCLUDE reasons, (amount IS NULL) AS amount_missing
FROM silver.orders_checked
WHERE ARRAY_SIZE(reasons) = 0;

INSERT INTO quarantine.orders (run_id, ingest_seq, source, order_id, reasons, row_data)
SELECT
  $run_id,
  chk.ingest_seq,
  chk.source,
  chk.order_id,
  chk.reasons,
  OBJECT_CONSTRUCT(*)
FROM silver.orders_checked chk
WHERE ARRAY_SIZE(chk.reasons) > 0
  AND NOT EXISTS (
    SELECT 1
    FROM quarantine.orders existing_q
    WHERE existing_q.source = chk.source
      AND EQUAL_NULL(existing_q.order_id, chk.order_id)
      AND EQUAL_NULL(existing_q.row_data:UPDATED_AT::TIMESTAMP_NTZ, chk.updated_at)
  )
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY chk.source, chk.order_id, chk.updated_at
  ORDER BY chk.ingest_seq DESC
) = 1;
"""


SQL_ORDERS_DEDUP = r"""
CREATE OR REPLACE TABLE silver.orders_dedup AS
SELECT *
FROM silver.orders_clean
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY order_id
  ORDER BY updated_at DESC, ingest_seq DESC
) = 1;
"""

SQL_CUSTOMERS_SCD2 = r"""
CREATE OR REPLACE TABLE dim.customers_scd2 AS
WITH dedup_events AS (
  SELECT
    raw:cust_id::STRING                     AS cust_id,
    LOWER(TRIM(raw:email::STRING))          AS email,
    INITCAP(TRIM(raw:tier::STRING))         AS tier,
    TRY_TO_DATE(raw:effective_from::STRING) AS valid_from,
    loaded_at,
    ingest_seq
  FROM bronze.customers_raw
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cust_id, valid_from
    ORDER BY loaded_at DESC, ingest_seq DESC
  ) = 1
)
SELECT
  cust_id,
  email,
  tier,
  valid_from,
  COALESCE(
    DATEADD(day, -1, LEAD(valid_from) OVER (PARTITION BY cust_id ORDER BY valid_from)),
    '9999-12-31'::DATE
  ) AS valid_to,
  LEAD(valid_from) OVER (PARTITION BY cust_id ORDER BY valid_from) IS NULL AS is_current
FROM dedup_events
WHERE cust_id IS NOT NULL
  AND valid_from IS NOT NULL;
"""

SQL_FX_DAILY_SPINE = r"""
CREATE OR REPLACE TABLE ref.fx_daily AS
WITH published_rates AS (
  SELECT
    r.raw:base::STRING        AS currency,
    f.key::DATE               AS rate_date,
    f.value:INR::NUMBER(12,4) AS rate_to_inr
  FROM bronze.fx_raw r,
       LATERAL FLATTEN(input => r.raw:rates) f
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY currency, rate_date
    ORDER BY r.loaded_at DESC, r.ingest_seq DESC
  ) = 1
),
date_bounds AS (
  SELECT MIN(rate_date) AS d0, GREATEST(MAX(rate_date), CURRENT_DATE()) AS d1
  FROM published_rates
),
date_spine AS (
  SELECT DATEADD(day, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, b.d0) AS d
  FROM date_bounds b, TABLE(GENERATOR(ROWCOUNT => 4000))
  QUALIFY d <= (SELECT d1 FROM date_bounds)
),
forward_filled AS (
  SELECT
    c.currency,
    s.d AS rate_date,
    LAST_VALUE(p.rate_to_inr) IGNORE NULLS OVER (
      PARTITION BY c.currency
      ORDER BY s.d
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS rate_to_inr,
    p.rate_to_inr IS NULL AS is_carried_forward
  FROM date_spine s
  CROSS JOIN (SELECT DISTINCT currency FROM published_rates) c
  LEFT JOIN published_rates p
    ON p.currency = c.currency AND p.rate_date = s.d
)
SELECT currency, rate_date, rate_to_inr, is_carried_forward FROM forward_filled
UNION ALL
SELECT 'INR', d, 1.0, FALSE FROM date_spine;
"""

SQL_PINCODE_GEO_MERGE = r"""
MERGE INTO ref.pincode_geo target
USING (
  SELECT latest.pincode, latest.city, latest.state, sr.region
  FROM (
    SELECT
      raw:pincode::STRING                            AS pincode,
      raw:response[0]:PostOffice[0]:District::STRING AS city,
      raw:response[0]:PostOffice[0]:State::STRING    AS state,
      loaded_at,
      ingest_seq
    FROM bronze.pincode_raw
    WHERE raw:response[0]:Status::STRING = 'Success'
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY pincode
      ORDER BY loaded_at DESC, ingest_seq DESC
    ) = 1
  ) latest
  LEFT JOIN ref.state_region sr ON sr.state = latest.state
) src
ON target.pincode = src.pincode
WHEN MATCHED AND (
  target.city <> src.city
  OR target.state <> src.state
  OR NVL(target.region, '') <> NVL(src.region, '')
) THEN
  UPDATE SET
    city = src.city,
    state = src.state,
    region = src.region,
    updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (pincode, city, state, region, updated_at)
  VALUES (src.pincode, src.city, src.state, src.region, CURRENT_TIMESTAMP());
"""

SQL_GOLD_ENRICH_AND_MERGE = r"""
CREATE OR REPLACE TABLE silver.orders_enriched AS
SELECT
  o.*,
  c.cust_id,
  c.tier,
  m.multiplier,
  fx.rate_to_inr                         AS fx_rate,
  ROUND(o.amount * fx.rate_to_inr, 2)    AS amount_inr,
  FLOOR(amount_inr / 100) * m.multiplier AS loyalty_points,
  amount_inr >= 2000                     AS is_high_value,
  g.city,
  g.state,
  g.region
FROM silver.orders_dedup o
LEFT JOIN dim.customers_scd2 c
  ON c.email = o.email
 AND o.order_date BETWEEN c.valid_from AND c.valid_to
LEFT JOIN ref.tier_multiplier m
  ON m.tier = c.tier
LEFT JOIN ref.fx_daily fx
  ON fx.currency = o.currency
 AND fx.rate_date = o.order_date
LEFT JOIN ref.pincode_geo g
  ON g.pincode = o.pincode;

MERGE INTO gold.fct_orders target
USING (
  SELECT
    order_id, order_date, cust_id, email, tier, multiplier, amount, currency,
    fx_rate, amount_inr, loyalty_points, is_high_value, city, state, region, updated_at,
    HASH(
      order_date, cust_id, email, tier, multiplier, amount, currency,
      fx_rate, amount_inr, loyalty_points, is_high_value, city, state, region, updated_at
    ) AS row_hash
  FROM silver.orders_enriched
) src
ON target.order_id = src.order_id
WHEN MATCHED AND target.row_hash <> src.row_hash THEN
  UPDATE SET
    order_date = src.order_date,
    cust_id = src.cust_id,
    email = src.email,
    tier = src.tier,
    multiplier = src.multiplier,
    amount = src.amount,
    currency = src.currency,
    fx_rate = src.fx_rate,
    amount_inr = src.amount_inr,
    loyalty_points = src.loyalty_points,
    is_high_value = src.is_high_value,
    city = src.city,
    state = src.state,
    region = src.region,
    updated_at = src.updated_at,
    row_hash = src.row_hash,
    published_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    order_id, order_date, cust_id, email, tier, multiplier, amount, currency,
    fx_rate, amount_inr, loyalty_points, is_high_value, city, state, region,
    updated_at, row_hash, published_at
  )
  VALUES (
    src.order_id, src.order_date, src.cust_id, src.email, src.tier, src.multiplier,
    src.amount, src.currency, src.fx_rate, src.amount_inr, src.loyalty_points,
    src.is_high_value, src.city, src.state, src.region, src.updated_at,
    src.row_hash, CURRENT_TIMESTAMP()
  );
"""

# Backwards-compatible aliases
SQL_00_SETUP = DDL_BOOTSTRAP
get_sql_10 = build_silver_cleaning_sql
SQL_20_ORDERS_DEDUP = SQL_ORDERS_DEDUP
SQL_30_CUSTOMERS_SCD2 = SQL_CUSTOMERS_SCD2
SQL_31_FX_DAILY = SQL_FX_DAILY_SPINE
SQL_32_PINCODE_GEO = SQL_PINCODE_GEO_MERGE
SQL_40_GOLD = SQL_GOLD_ENRICH_AND_MERGE
