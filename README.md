# FreshCart Data Engineering & ML Pipelines

| Field | Details |
|---|---|
| **Student Name** | Pratyush Singh |
| **Roll Number** | `23bcs200` |
| **Email** | `psingh08iph@gmail.com` |
| **GitHub Repository** | [https://github.com/Pratyush2840/freshcart-pipelines](https://github.com/Pratyush2840/freshcart-pipelines) |
| **Final Report PDF** | `report/23bcs200_Pratyush_Singh_Pipelines.pdf` |

---

## Pipeline Modules

| Track | Scope | Key Implementation |
|---|---|---|
| **Part A — Git & CI/CD** | Version control & automated pytest gates | `.github/workflows/ci.yml`, `tests/` |
| **Part B — Snowflake Lakehouse ELT** | Multi-hop Bronze → Silver → Gold + QA | `elt/sql_scripts.py`, `elt/snowflake_client.py`, `elt/pipeline_runner.py` |
| **Part C — Mage AI ML DAG** | Iris classification with `@test` accuracy gates | `iris_project/run_pipeline.py`, `iris_project/run_c2_experiments.py` |
| **Part D — RabbitMQ Event Stream** | Topic routing (`*`, `#`) & ACK/NACK handling | `stream/broker_client.py`, `stream/topic_routing.py`, `stream/dispatcher.py`, `stream/driver.py` |
| **Bonus — Stream to Lakehouse** | 60s micro-batch ingestion & replay idempotency | `stream/freshcart_consumer.py`, `stream/run_bonus_experiment.py` |
| **Part E — Documentation & Viva** | Academic report, evidence index & viva guide | `report/`, `evidence/`, `VIVA_PREPARATION.md` |

---

## End-to-End Flow

1. **Streaming (`stream/`)**: `dispatcher.py` emits `<city>.<zone>.<type>` orders to the `rapido-dispatch` RabbitMQ topic exchange (injecting a missing-`zone` payload every 10th event for NACK verification). `driver.py` filters via topic bindings and `freshcart_consumer.py` buffers `#.food` events over a 60s window.
2. **Lakehouse ELT (`elt/`)**: `stage_ndjson_batch()` writes gzipped NDJSON envelopes loaded via `PUT` + `COPY INTO` into `bronze.orders_raw`. SQL pipelines standardize, deduplicate (`QUALIFY ROW_NUMBER()`), enrich with SCD Type 2 customer tiers (`dim.customers_scd2`), FX spine (`ref.fx_daily`), and pincode geography (`ref.pincode_geo`), then idempotently `MERGE` into `gold.fct_orders`.
3. **ML Orchestration (`iris_project/`)**: `load_iris` → `check_no_missing` → `prepare_data` (80/20 split) → `train_model` (`@test` accuracy >= 80% gate) → `save_results` & `evaluate_model`.
4. **CI Automation (`.github/workflows/ci.yml`)**: Runs `pytest tests/ -v` on every push and pull request to `main` and `dev`.

---

## Project Structure

```text
freshcart-pipelines/
├── .env.example                              # Template for local .env (never commit .env)
├── .github/workflows/ci.yml                  # GitHub Actions CI workflow (Python 3.12)
├── pytest.ini
├── requirements.txt
├── common/
│   └── settings.py                           # Centralized .env loader & connection config
├── elt/
│   ├── sample_data.py                        # Synthetic web/mobile/CRM/FX/pincode fixtures
│   ├── sql_scripts.py                        # Snowflake DDL & Bronze->Silver->Gold SQL
│   ├── snowflake_client.py                   # Connection, NDJSON staging & COPY INTO helpers
│   ├── pipeline_runner.py                    # ELT orchestrator & quality assertions
│   ├── run_b1_baseline.py                    # Part B1 baseline verification
│   ├── run_b3_currency.py                    # Part B3 unsupported_currency validation
│   ├── run_b4_scd2.py                        # Part B4 SCD Type 2 & Gold MERGE idempotency
│   ├── run_b5_quality.py                     # Part B5 negative loyalty check & failure test
│   ├── trace_records.py                      # Part B2 record lineage tracer
│   └── freshcart_elt.ipynb
├── iris_project/
│   ├── metadata.yaml
│   ├── run_pipeline.py
│   ├── run_c2_experiments.py
│   ├── Mage_Iris_Pipeline.ipynb
│   ├── data_loaders/load_iris.py
│   ├── transformers/{check_no_missing,prepare_data,train_model,evaluate_model}.py
│   └── data_exporters/save_results.py
├── stream/
│   ├── broker_client.py                      # Shared RabbitMQ HTTP Management API client
│   ├── topic_routing.py                      # Iterative DP AMQP 0-9-1 topic matcher
│   ├── dispatcher.py                         # Order event publisher
│   ├── driver.py                             # Topic-bound driver consumer
│   ├── freshcart_consumer.py                 # 60s micro-batch Snowflake bridge
│   ├── run_stream_test.py                    # Multi-driver live harness
│   └── run_bonus_experiment.py               # 2-pass replay idempotency experiment
├── tests/
│   ├── test_prepare_data.py
│   ├── test_stream_routing.py
│   └── test_stream_payloads.py
├── report/                                   # HTML generator, part reports & compiled PDFs
├── evidence/                                 # Execution logs & evidence index
└── screenshots/                              # Verification screenshots (Part A-E & Bonus)
```

---

## Quick Setup

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure Local `.env`
Copy `.env.example` to `.env` (git-ignored) and fill in your Snowflake and RabbitMQ values:
```env
SNOWFLAKE_ACCOUNT=<your_snowflake_account>
SNOWFLAKE_USER=<your_snowflake_user>
SNOWFLAKE_PASSWORD=<your_snowflake_password>
SNOWFLAKE_WAREHOUSE=FRESHCART_WH
SNOWFLAKE_DATABASE=FRESHCART
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=ACCOUNTADMIN

AMQP_URL=amqp://guest:guest@localhost:5672/
```

---

## Execution Commands

```bash
# Unit Tests (Part A & D1)
pytest tests/ -v

# Snowflake ELT (Part B)
python -m elt.run_b1_baseline
python -m elt.trace_records
python -m elt.run_b3_currency
python -m elt.run_b4_scd2
python -m elt.run_b5_quality

# Mage ML Pipeline & Depth Sweep (Part C)
python -m iris_project.run_pipeline
python -m iris_project.run_c2_experiments

# RabbitMQ Streaming & Lakehouse Bonus (Part D & Bonus)
python -m stream.run_stream_test
python -m stream.run_bonus_experiment

# Compile Final Report PDF (Part E)
python report/generate_report_html.py
```
