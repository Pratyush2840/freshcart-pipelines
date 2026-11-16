# FreshCart Pipelines · End-to-End Data Engineering & ML Orchestration

A modular data engineering repository integrating CI/CD testing, a multi-hop Snowflake ELT lakehouse, a gated Mage AI / Scikit-Learn ML DAG, and a RabbitMQ topic-exchange streaming pipeline.

---

## 1. Architecture Overview

```text
+----------------------------------------------------------------------------------+
|                       FRESHCART PIPELINE ARCHITECTURE                            |
+----------------------------------------------------------------------------------+

  [1. Event Streaming] (stream/)
      dispatcher.py ──▶ RabbitMQ Topic Exchange ("rapido-dispatch") ──▶ driver.py / freshcart_consumer.py
                                                                              │ (60s batch -> NDJSON.gz)
                                                                              ▼
  [2. Snowflake Multi-Hop ELT] (elt/)
      bronze.orders_raw ──▶ silver.orders_clean ──▶ silver.orders_dedup ──▶ gold.fct_orders
      (NDJSON Stage)       (Regex + Quarantine)    (QUALIFY ROW_NUMBER)   (Hash Idempotent MERGE)
                                                          ▲
                                            dim.customers_scd2 + ref.fx_daily + ref.pincode_geo

  [3. Gated ML DAG] (iris_project/)
      load_iris ──▶ check_no_missing ──▶ prepare_data ──▶ train_model (@test >= 80%)
                                                               ├──▶ save_results (CSV)
                                                               └──▶ evaluate_model (Metrics)

  [4. CI/CD & Tests] (.github/workflows/ci.yml, tests/)
      Pytest unit suite covering split determinism, topic routing, and payload contracts
```

---

## 2. Repository Layout

```text
freshcart-pipelines/
├── .env.example                      # Environment template (copy to .env)
├── .github/workflows/ci.yml          # GitHub Actions pytest workflow (Python 3.12)
├── pytest.ini                        # Pytest path & discovery settings
├── requirements.txt                  # Python package dependencies
├── common/
│   └── settings.py                   # Centralized .env loader & Snowflake/AMQP config
├── elt/
│   ├── sample_data.py                # Synthetic web/mobile/CRM/FX/pincode fixtures
│   ├── sql_scripts.py                # Snowflake DDL and Bronze->Silver->Gold SQL
│   ├── snowflake_client.py           # Connection, NDJSON staging, and COPY INTO helpers
│   ├── pipeline_runner.py            # ELT orchestrator and data quality assertions
│   ├── run_b1_baseline.py            # Baseline ELT verification runner
│   ├── run_b3_currency.py            # Unsupported currency quarantine validation
│   ├── run_b4_scd2.py                # SCD Type 2 customer tier update & MERGE check
│   ├── run_b5_quality.py             # Negative loyalty quality gate & failure injection
│   └── trace_records.py              # End-to-end record lineage inspector
├── iris_project/
│   ├── metadata.yaml                 # Mage DAG configuration
│   ├── run_pipeline.py               # Standalone DAG runner with @test gates
│   ├── run_c2_experiments.py         # Depth sweep (max_depth = 1..5) gate verification
│   ├── data_loaders/load_iris.py
│   ├── transformers/{check_no_missing,prepare_data,train_model,evaluate_model}.py
│   └── data_exporters/save_results.py
├── stream/
│   ├── broker_client.py              # Shared RabbitMQ HTTP Management API client
│   ├── topic_routing.py              # AMQP 0-9-1 topic matcher (* and # wildcards)
│   ├── dispatcher.py                 # Periodic order publisher (malformed every 10th)
│   ├── driver.py                     # Topic-bound driver consumer with ACK/NACK checks
│   ├── freshcart_consumer.py         # 60s micro-batch consumer bridging to Snowflake
│   ├── run_stream_test.py            # Multi-driver streaming harness
│   └── run_bonus_experiment.py       # 2-pass replay idempotency verification
└── tests/
    ├── test_prepare_data.py
    ├── test_stream_routing.py
    └── test_stream_payloads.py
```

---

## 3. Environment Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure `.env` in the repository root (`.env` is git-ignored; use `.env.example` as reference):
   ```env
   SNOWFLAKE_ACCOUNT=<org-account>
   SNOWFLAKE_USER=<username>
   SNOWFLAKE_PASSWORD=<password>
   SNOWFLAKE_WAREHOUSE=FRESHCART_WH
   SNOWFLAKE_DATABASE=FRESHCART
   SNOWFLAKE_SCHEMA=PUBLIC
   SNOWFLAKE_ROLE=ACCOUNTADMIN

   AMQP_URL=amqp://guest:guest@localhost:5672/
   ```

---

## 4. Running Components

- **Unit Tests:**
  ```bash
  pytest tests/ -v
  ```
- **Snowflake ELT (Part B):**
  ```bash
  python -m elt.run_b1_baseline
  python -m elt.run_b3_currency
  python -m elt.run_b4_scd2
  python -m elt.run_b5_quality
  python -m elt.trace_records
  ```
- **Mage Iris ML Pipeline (Part C):**
  ```bash
  python -m iris_project.run_pipeline
  python -m iris_project.run_c2_experiments
  ```
- **RabbitMQ Streaming & Lakehouse Bridge (Part D & Bonus):**
  ```bash
  python stream/dispatcher.py
  python stream/driver.py Ravi "bangalore.koramangala.*"
  python -m stream.run_stream_test
  python -m stream.run_bonus_experiment
  ```
