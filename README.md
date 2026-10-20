# CFPB Complaints Lakehouse: Apache Spark + dbt

A production-grade, batch data-engineering lakehouse that transforms messy regulatory data from the **CFPB Consumer Complaint Database** (banking & consumer finance complaints, 2.4M+ records) into an auditable, high-performance dimensional warehouse and analytics platform.

Built with **Apache Spark (PySpark 4.x)** for distributed data cleaning, deduplication, entity resolution, and quarantine, and **dbt (data build tool) on DuckDB** (swappable to Snowflake) for Kimball star-schema dimensional modeling, incremental fact updates, SCD Type 2 snapshots, and automated semantic testing.

---

## Table of Contents
1. [Architecture & Data Flow](#architecture--data-flow)
2. [Layer Contracts & Directory Structure](#layer-contracts--directory-structure)
3. [Apache Spark Deep Dive](#apache-spark-deep-dive)
   - [Spark Execution Model & DAG](#spark-execution-model--dag)
   - [Narrow vs. Wide Transformations](#narrow-vs-wide-transformations)
   - [Broadcast Joins & Memory Optimization](#broadcast-joins--memory-optimization)
   - [Dynamic Partition Overwriting](#dynamic-partition-overwriting)
   - [Idempotent Incremental Watermarking](#idempotent-incremental-watermarking)
   - [Spark Web UI & Performance Tuning Guide](#spark-web-ui--performance-tuning-guide)
4. [dbt (Data Build Tool) Deep Dive](#dbt-data-build-tool-deep-dive)
   - [dbt Architecture & DuckDB Parquet Engine](#dbt-architecture--duckdb-parquet-engine)
   - [Model Lineage (Staging -> Intermediate -> Marts)](#model-lineage)
   - [Kimball Star Schema (Core Models)](#kimball-star-schema-core-models)
   - [Incremental Strategy & Lookback Windows](#incremental-strategy--lookback-windows)
   - [SCD Type 2 Snapshots](#scd-type-2-snapshots)
   - [dbt Macros & Schema Management](#dbt-macros--schema-management)
   - [Comprehensive Testing Suite](#comprehensive-testing-suite)
5. [Data Quality & Entity Resolution Framework](#data-quality--entity-resolution-framework)
   - [Quarantine Zone Architecture](#quarantine-zone-architecture)
   - [Company Entity Resolution (Rules + RapidFuzz)](#company-entity-resolution-rules--rapidfuzz)
   - [Regulatory Privacy Guards (PII / Narratives)](#regulatory-privacy-guards)
6. [How to Run (Windows & Linux/macOS)](#how-to-run-windows--linuxmacos)
7. [Production Verification & Benchmark Results](#production-verification--benchmark-results)

---

## Architecture & Data Flow

```
                      DATA INGESTION & STORAGE LAYERS
                      
  CFPB API (Daily Delta) ──┐
  Hugging Face (Parquet)  ──┼──► [ data/raw ] (CSV / Parquet / JSONL)
  Synthetic Messy CSV    ──┘          │
                                       ▼
                       [ BRONZE LAYER (Append-Only) ]
                         * Stored as Partitioned Parquet (_ingest_date)
                         * 100% strings, preserves raw data faithfully
                         * Lineage metadata: _batch_id, _ingested_at, _source_file
                                       │
                                       ▼
                     [ APACHE SPARK PROCESSING ENGINE ]
                         * Schema enforcement, date/type casting
                         * Regex-based whitespace & legal token cleaning
                         * Company entity resolution (Rules + RapidFuzz)
                         * Soft DQ Flags array attached to each row
                         * Hard rules split -> [ data/quarantine ]
                         * Dedup & Dynamic Partition Merge by Year-Month
                                       │
                                       ▼
                         [ SILVER LAYER (Clean Parquet) ]
                         * Hive-partitioned by: received_year_month
                         * Deduplicated & normalized (Grain: 1 row per complaint_id)
                         * Persisted company_map audit trail
                                       │
                                       ▼
                      [ dbt + DUCKDB WAREHOUSE LAYER ]
                         * Staging: 1:1 typed views over Silver Parquet
                         * Intermediate: Joins with reference seeds, analytical bucketing
                         * Core Star Schema: dim_company, dim_product, dim_issue,
                                             dim_geography, dim_date, fact_complaints (Incremental)
                         * SCD2 Snapshots: company_master_snapshot
                         * Analytics Marts: 5 aggregated reporting tables
                                       │
                                       ▼
                      [ BI DASHBOARDS & CONSUMPTION ]
                         * DuckDB / Snowflake / Metabase / Evidence
```

---

## Layer Contracts & Directory Structure

| Layer | Engine | Format / Schema | Storage Path | Mutation Rule | Responsibility |
|---|---|---|---|---|---|
| **Raw** | Python `ingest` | CSV, Parquet, JSONL | `data/raw/` | Read-only | Immutable landing zone. |
| **Bronze** | Spark | Partitioned Parquet | `data/bronze/` | Append-only | Preserves all incoming strings, adds `_batch_id`, `_ingested_at`, `_ingest_seq`, `_source_file`. |
| **Silver** | Spark | Hive Parquet (`received_year_month`) | `data/silver/complaints` | Idempotent Overwrite | Cleaned, deduplicated, entity-resolved, soft-flagged records. |
| **Quarantine**| Spark | Parquet | `data/quarantine/` | Append / Overwrite | Rows failing hard contracts with `quarantine_reason`. |
| **Audit Map** | Spark / RapidFuzz | Parquet | `data/silver/company_map` | Stable Append | Entity-resolution decision log with match method and score. |
| **Warehouse** | dbt (DuckDB) | `.duckdb` / Snowflake | `data/warehouse/cfpb.duckdb` | Managed by dbt | Staging, intermediate, dimensional star schema, analytics marts. |

```
cfpb-complaints-lakehouse/
├── pipeline/                   # Apache Spark (PySpark) & Python ETL
│   ├── bronze.py               # Raw -> Bronze ingest & schema standardization
│   ├── silver.py               # Bronze -> Silver transformation, dedup & merge
│   ├── cleaning.py             # Column-level Spark functions, DQ flags & quarantine rules
│   ├── company_resolution.py   # Rule-based + RapidFuzz entity resolution
│   ├── quality.py              # Automated Bronze vs. Silver DQ scorecard generator
│   ├── reference.py            # Single source of truth for taxonomy, states & seed generation
│   ├── ingest.py               # Hugging Face & CFPB REST API extractors
│   ├── spark_session.py        # SparkSession factory with optimized configurations
│   ├── config.py               # Directory paths, watermark states, thresholds
│   ├── generate_sample_data.py # Synthetic data generator (initial load + late deltas)
│   └── run_pipeline.py         # End-to-end orchestration runner
├── dbt_cfpb/                   # dbt Project (dbt-duckdb / dbt-snowflake)
│   ├── dbt_project.yml         # Project configurations, model materializations & vars
│   ├── profiles.yml            # DuckDB and Snowflake connection profiles
│   ├── macros/
│   │   └── generate_schema_name.sql  # Schema name override macro
│   ├── models/
│   │   ├── staging/            # stg_complaints, stg_company_map, _sources.yml
│   │   ├── intermediate/       # int_complaints_enriched, _intermediate.yml
│   │   └── marts/
│   │       ├── core/           # Star schema: fact_complaints, dim_company, dim_date, etc.
│   │       └── analytics/      # 5 business marts (trends, scorecard, performance, geo)
│   ├── snapshots/              # company_master_snapshot (SCD Type 2)
│   ├── seeds/                  # state_codes.csv, product_taxonomy.csv, company_category.csv
│   └── tests/                  # Custom generic & singular data quality tests
├── tests/                      # Pytest suite for Spark transformations & pipeline
├── airflow/dags/               # Production Airflow orchestration DAG
├── .github/workflows/          # CI/CD GitHub Actions workflow
├── Makefile                    # Automation shortcuts (Linux/macOS)
└── requirements.txt            # Python dependencies
```

---

## Apache Spark Deep Dive

### Spark Execution Model & DAG
Apache Spark operates on a **Driver-Executor** distributed architecture. In this project:
1. **Driver Program (`pipeline/silver.py`):** Instantiates the `SparkSession`, creates DataFrame plans, coordinates entity resolution with `rapidfuzz`, and manages partition overwrite commits.
2. **Catalyst Optimizer:** Compiles DataFrame operations into a Directed Acyclic Graph (DAG) of stages:
   * **Unresolved Logical Plan:** Validates syntax and table/column references.
   * **Logical Plan:** Applies predicate pushdown, projection pruning, and constant folding.
   * **Optimized Physical Plan:** Selects join algorithms (e.g., Broadcast Hash Join) and physical execution operators.
   * **WholeStageCodeGen:** Generates optimized Java bytecode on the fly for pipelined transformations.

### Narrow vs. Wide Transformations
Understanding the difference is critical for Spark performance tuning:

* **Narrow Transformations (Zero Shuffle across executors):**
  - Operations in `pipeline/cleaning.py`: `clean_text()`, `clean_token()`, `parse_date()`, `state_code()`, `zip3()`, `F.when().otherwise()`.
  - Data stays within executor CPU registers and L1/L2 cache; partitions are processed independently and pipelined together.
* **Wide Transformations (Network Shuffle required):**
  - Deduplication: `Window.partitionBy("complaint_id").orderBy(F.col("_ingested_at").desc())`. Spark hashes `complaint_id` across network partitions to bring duplicates to the same executor.
  - Aggregations: `groupBy("company_key", "company_raw").count()`.
  - Partitioning: `staged_merge.repartition("received_year_month")` to ensure uniform Hive-style Parquet partition files without fragmentation.

### Broadcast Joins & Memory Optimization
When joining the cleaned DataFrame with the entity resolution mapping table (`silver/company_map`), a standard shuffle join would redistribute millions of rows across executors.
Instead, we use **Broadcast Hash Join**:
```python
resolved = staged.join(F.broadcast(lookup), "company_key", "left")
```
* The driver collects the compact `lookup` DataFrame (~few thousand rows) and serializes it to all executors.
* Executors keep the lookup table in-memory as a hash map, executing the join locally with $O(1)$ complexity and **zero network shuffle**.

### Dynamic Partition Overwriting
In default Spark configurations, executing `.write.mode("overwrite").partitionBy("received_year_month")` will **completely wipe out the entire target directory**, destroying historical data!

To prevent this, `pipeline/spark_session.py` configures:
```python
.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
```
* **How it works:** Spark inspects only the partition keys present in the incoming micro-batch (e.g., `received_year_month = '2026-10'`).
* Only those specific partition folders (`received_year_month=2026-10/`) are overwritten atomically, leaving historical partitions (`2026-09/`, `2026-08/`, etc.) untouched.

### Idempotent Incremental Watermarking
The pipeline guarantees **idempotency**: running the pipeline multiple times or replaying the same batch produces the exact same state without duplicates.
1. Bronze batches are tagged with `_ingested_at` timestamp.
2. Silver records the maximum processed `_ingested_at` in microseconds in `data/state/silver_watermark.json`.
3. In `incremental` mode:
   * Spark filters bronze: `F.unix_micros("_ingested_at") > watermark`.
   * Re-reads existing Silver partitions touched by updated `complaint_id`s.
   * Unions and deduplicates via `row_number() over (partition by complaint_id order by _ingested_at desc)`.
   * Overwrites only touched partitions and advances watermark.
   * If re-run with no new data or same delta, it gracefully terminates as a no-op.

### Spark Web UI & Performance Tuning Guide
To visualize and debug Spark jobs, set `spark.ui.enabled = true` in `pipeline/spark_session.py` and open **`http://localhost:4040`** in your browser.

#### Key Tabs in Spark UI:
1. **Jobs Tab:**
   * Shows high-level execution progress, total duration, and how jobs are broken down into stages by shuffle boundaries.
2. **Stages Tab:**
   * **Data Skew Detection:** Look at the *Task Deserialization Time*, *Duration*, and *Shuffle Read Size* min/median/max values. If the 75th percentile is 2 seconds but max is 4 minutes, you have data skew on a partition key.
   * **Task Count:** Controlled by `spark.sql.shuffle.partitions`. For laptop/local testing, set to `8` or `16` to avoid scheduling overhead of the default 200 partitions.
3. **Storage Tab:**
   * Shows memory fraction used by cached DataFrames (`batch.cache()`). Confirms whether DataFrames are stored in deserialized RAM or serialized to disk.
4. **SQL / DataFrame Tab:**
   * Click on any query execution to view the **Physical Plan DAG**. Look for:
     * `WholeStageCodegen` blocks (green boxes indicating JIT-compiled native execution).
     * `BroadcastHashJoin` vs `SortMergeJoin`.
     * `Exchange` nodes (points where network shuffles occur).
5. **Executors Tab:**
   * Displays JVM garbage collection (GC) time vs computation time. If GC time exceeds 10%, increase driver/executor heap memory (`spark.driver.memory`).

---

## dbt (Data Build Tool) Deep Dive

### dbt Architecture & DuckDB Parquet Engine
Unlike traditional warehouses (Postgres, Redshift) where raw data must be loaded into tables before querying, **dbt-duckdb** queries Parquet files directly from disk via vectorized C++ SIMD instructions:
```sql
-- models/staging/_sources.yml
meta:
  external_location: >-
    read_parquet('{{ env_var("SILVER_PATH", "../data/silver/complaints") }}/**/*.parquet', 
                 hive_partitioning = true, 
                 hive_types_autocast = false)
```
* **Zero ETL ingestion lag:** The moment Spark writes Parquet files to `data/silver/`, dbt can immediately build models on top of them.
* **Portable:** The same dbt models can be switched from DuckDB to Snowflake simply by changing `profiles.yml` and replacing the `source` external location with a Snowflake external table / stage.

### Model Lineage
Data flows through three formal dbt modeling tiers:

```
                  ┌────────────────────────┐
                  │ stg_complaints (view)   │
                  └───────────┬────────────┘
                              │
                  ┌───────────▼────────────┐     ┌─────────────────────┐
                  │ int_complaints_enriched │◄────┤ Seeds (reference)   │
                  └───────────┬────────────┘     └─────────────────────┘
                              │
          ┌───────────────────┼───────────────────┐
          │                   │                   │
┌─────────▼────────┐  ┌───────▼────────┐  ┌───────▼────────┐
│ dim_company      │  │ fact_complaints│  │ dim_product    │ ... (dim_date, dim_geo, dim_issue)
└─────────┬────────┘  └───────┬────────┘  └────────────────┘
          │                   │
          └─────────┬─────────┘
                    ▼
┌──────────────────────────────────────────────┐
│ Analytics Marts                              │
│  - mart_complaints_by_company_month          │
│  - mart_data_quality_scorecard               │
│  - mart_resolution_performance               │
│  - mart_product_issue_trends                 │
│  - mart_geo_complaints                       │
└──────────────────────────────────────────────┘
```

### Kimball Star Schema (Core Models)
Designed for dimensional business intelligence:
* **`fact_complaints` (Incremental Fact):** Grain is one row per complaint. Contains foreign surrogate keys (`company_sk`, `product_sk`, `issue_sk`), additive/semi-additive metrics (`days_to_company`, `narrative_word_count`, `dq_flag_count`), and boolean outcome indicators (`is_relief`, `timely_response`, `consumer_disputed`).
* **`dim_company`:** Conformed dimension holding canonical company names, categories, and alias counts. Includes a synthetic `-1 / UNKNOWN` row to preserve referential integrity for missing keys.
* **`dim_product` & `dim_issue`:** Hierarchical dimensions grouping raw products into canonical product groups (Credit, Mortgages, Debt Collection) and issues into sub-issues.
* **`dim_geography`:** State-level dimension enriched with US Census regions.
* **`dim_date`:** Calendar dimension with year, month, quarter, day-of-week, and weekend flags.

### Incremental Strategy & Lookback Windows
In `dbt_cfpb/models/marts/core/fact_complaints.sql`:
```sql
{{
    config(
        materialized='incremental',
        unique_key='complaint_id',
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns'
    )
}}

select ... from {{ ref('int_complaints_enriched') }}
{% if is_incremental() %}
where silver_updated_at >= (
    select max(silver_updated_at) - interval '{{ var("lookback_days") }} days' from {{ this }}
)
{% endif %}
```
* **Why `delete+insert`?** In DuckDB, `delete+insert` provides atomic upserts on `complaint_id`. On Snowflake, switching `incremental_strategy='merge'` performs ANSI SQL `MERGE INTO`.
* **Lookback Window:** Set to `7` days by default. When banks retroactively submit corrections (e.g., updating `timely_response` or providing monetary relief), the lookback window captures and overwrites the existing fact records idempotently.

### SCD Type 2 Snapshots
Implemented in `dbt_cfpb/snapshots/company_master_snapshot.sql`:
```sql
{% snapshot company_master_snapshot %}
{{
    config(
        target_schema='snapshots',
        unique_key='company_sk',
        strategy='check',
        check_cols=['company_name_clean', 'company_category', 'alias_count']
    )
}}
select * from {{ ref('dim_company') }}
{% endsnapshot %}
```
* Tracks how company entity resolution evolves over time.
* If a new alias is discovered or a company's category changes, dbt automatically closes the active record (`dbt_valid_to = current_timestamp`) and inserts a new active version (`dbt_valid_to is null`).

### dbt Macros & Schema Management
By default, dbt concatenates the default schema with custom schemas (e.g., `cfpb_marts`). 
To keep clean, production-grade schemas (`staging`, `intermediate`, `marts`, `analytics`, `snapshots`), we implemented a custom macro in `dbt_cfpb/macros/generate_schema_name.sql`:
```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

### Comprehensive Testing Suite
The project enforces 77+ automated data quality checks across three tiers:
1. **Generic Tests (`_core.yml`, `_staging.yml`):**
   * `unique` & `not_null` on primary/surrogate keys.
   * `relationships` enforcing foreign key referential integrity from `fact_complaints` to all dimension tables.
   * `accepted_values` on statuses, categories, and channels.
2. **Custom Generic Tests (`tests/generic/`):**
   * `date_not_in_future`: Verifies dates never exceed `current_date()`.
   * `date_not_before`: Validates historical boundaries (CFPB database inception: `2011-07-21`).
   * `rate_between_0_and_1`: Validates percentage and rate metrics in analytics marts.
3. **Singular Business Rule Tests (`tests/`):**
   * `assert_fact_row_count_matches_staging.sql`: Ensures zero record loss between staging and marts.
   * `assert_no_narrative_without_consent.sql`: Verifies strict compliance with consumer privacy regulations.
   * `assert_all_companies_resolved.sql`: Guarantees every raw company key is assigned a valid canonical entity.

---

## Data Quality & Entity Resolution Framework

### Quarantine Zone Architecture
Most data pipelines either fail completely when encountering bad data or silently drop rows. This lakehouse implements an **enterprise quarantine pattern**:
* **Soft Failures (Logged & Flagged):** Missing states, unparseable ZIP codes, or out-of-order dates (`date_sent < date_received`) are retained in Silver. The pipeline populates a Spark `ArrayType(StringType)` column named `dq_flags`. dbt then aggregates these in `mart_data_quality_scorecard` to monitor upstream data drift.
* **Hard Failures (Quarantined):** Records with unparseable `complaint_id`, future `date_received`, or dates prior to 2011 are routed directly to `data/quarantine/` with a detailed `quarantine_reason`. They never contaminate Silver or the warehouse.

### Company Entity Resolution (Rules + RapidFuzz)
CFPB company names contain thousands of typos, abbreviations, and legal variations:
1. **Normalization:** Upper-case conversion, ampersand normalization (`&` -> `AND`), punctuation removal, and stripping of legal suffixes (`INC`, `LLC`, `NATIONAL ASSOCIATION`, `N.A.`, etc.).
2. **Deterministic Rules:** Regex matching against known parent banking institutions (`reference.COMPANY_RULES`).
3. **Fuzzy String Matching (`RapidFuzz`):**
   * Computes `token_sort_ratio` against canonical keys (cutoff threshold = 88).
   * **Compatibility Guards:** Prevents false merges by asserting that numbers within names match exactly and the first letter is identical.
4. **Audit Trail & Stability:** Every mapping is saved to `silver/company_map` with its resolution method (`rule`, `exact`, `fuzzy`, `new`) and confidence score. Once a key is mapped, its decision is permanently frozen for all future incremental runs.

### Regulatory Privacy Guards
Consumer complaints often contain sensitive personal narratives. 
* Under CFPB regulatory standards, narratives may only be shared if affirmative consent was granted.
* `pipeline/cleaning.py` checks `consumer_consent_provided`. If consent is `not_provided` or `withdrawn`, the narrative is forcefully set to `NULL` before writing to Silver, eliminating regulatory compliance risk.

---

## How to Run (Windows & Linux/macOS)

### Prerequisites
* **Python:** 3.10, 3.11, 3.12, or 3.13
* **Java:** JDK 11, 17, or 21 (Required for Apache Spark)

---

### Running on Windows (PowerShell)

#### Step 1: Create Virtual Environment & Install Dependencies
```powershell
cd d:\programining\cfpb-complaints-lakehouse
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

#### Step 2: Run Automated Unit & E2E Tests
```powershell
python -m pytest -v
```

#### Step 3: Run the Full End-to-End Synthetic Pipeline
```powershell
# Set environment variables for dbt
$env:LAKE_ROOT = "$pwd/data"
$env:DBT_PROFILES_DIR = "$pwd/dbt_cfpb"
$env:SILVER_PATH = "$pwd/data/silver/complaints"
$env:COMPANY_MAP_PATH = "$pwd/data/silver/company_map"
$env:DBT_DUCKDB_PATH = "$pwd/data/warehouse/cfpb.duckdb"

# 1. Generate synthetic messy data
python -m pipeline.generate_sample_data --rows 20000 --out data/raw/sample/initial.csv

# 2. Run Spark Full Rebuild (Bronze -> Silver -> Quarantine -> DQ Report)
python -m pipeline.run_pipeline --input data/raw/sample/initial.csv --format csv --mode full

# 3. Build dbt models, seeds, snapshots, and run tests
cd dbt_cfpb
dbt seed
dbt build
cd ..

# 4. Generate incremental delta (new complaints + corrections + late-arriving data)
python -m pipeline.generate_sample_data --delta --base data/raw/sample/initial.csv --out data/raw/sample/delta_1.csv

# 5. Run Spark Incremental Merge
python -m pipeline.run_pipeline --input data/raw/sample/delta_1.csv --format csv --mode incremental

# 6. Run dbt incremental build
cd dbt_cfpb
dbt build
cd ..
```

#### Step 4: Generate & View Documentation Lineage
```powershell
cd dbt_cfpb
dbt docs generate
dbt docs serve
```

---

### Running on Linux / macOS (Makefile)

```bash
make setup
source .venv/bin/activate
make test       # Runs pytest suite
make demo       # Runs full load -> dbt -> delta -> incremental -> dbt
make dbt-docs   # Serves interactive documentation and lineage graph
```

---

### Using Real Live CFPB Data (US Government API)

```powershell
# 1. Fetch real live complaints from the CFPB Elasticsearch API (with safety limit)
python -m pipeline.ingest api --since 2026-10-10 --until 2026-10-12 --limit 1000

# 2. Ingest real complaints through the automated Lakehouse pipeline
python -m pipeline.orchestrate --input "data/raw/api/complaints_2026-10-10_2026-10-12.jsonl" --format jsonl --mode incremental --target snowflake
```

---

## Unified End-to-End Orchestrator

Instead of executing stages manually, the production orchestrator (`pipeline/orchestrate.py`) manages the entire lifecycle with an automated **Circuit Breaker** data quality gate:

```powershell
# Run the automated pipeline on Snowflake
python -m pipeline.orchestrate --mode incremental --target snowflake

# Run locally on DuckDB (zero cloud cost)
python -m pipeline.orchestrate --mode full --target dev
```

### Circuit Breaker & Observability Card
* **Fault Tolerance:** If upstream dirty data exceeds the quarantine safety threshold (default: > 20%), the orchestrator automatically trips the circuit breaker and halts downstream load before reaching Snowflake or dbt.
* **Observability Summary:** Prints runtime duration, partition counts, and row metrics upon completion.

---

## Interactive Streamlit BI & Observability Dashboard

A multi-engine analytical web application built with Streamlit and Plotly that connects to your Snowflake warehouse (or local DuckDB):

```powershell
streamlit run app/dashboard.py
```

### Dashboard Features:
1. **Executive KPIs:** Real-time metrics for total complaints, resolved companies, timely response rate %, and monetary relief %.
2. **🗺️ Interactive US Choropleth Map:** Color-intensity map of complaint volumes across all 50 states with hover analytics.
3. **🏦 Company Resolution Performance:** League table of institutions ranked by complaint volume, timely response %, and consumer dispute %.
4. **📈 Product Trends Over Time:** Multi-line time series tracking canonical product hierarchy volume trajectories.
5. **🛡️ Data Quality Scorecard:** Visualizing data drift, missing state %, malformed ZIP %, and narrative consent compliance.

---

## Production Verification & Benchmark Results

The pipeline was verified under the following runtime matrix:
* **Operating System:** Windows 11 / Linux Ubuntu 22.04 LTS
* **Runtime:** Python 3.13, Java 21 LTS (HotSpot 64-Bit)
* **Engines:** PySpark 4.2.0, dbt-core 1.12.6, dbt-snowflake 1.12.3, dbt-duckdb 1.11.0, Snowflake Enterprise Cloud
* **Cloud Warehouse State:** 21,284 complaints, 96 canonical resolved entities, 77/77 tests passing green.

### Measured Data Quality Improvements (Synthetic + Live API Benchmark)
* **Distinct Company Name Variants:** 151 $\rightarrow$ 30 (Synthetic) | 96 resolved entities (Live API)
* **Product Labels:** 54 $\rightarrow$ 9 (Taxonomy normalized to canonical banking categories)
* **Duplicate Complaints:** 600 $\rightarrow$ 0 (Deterministic timestamp-based window deduplication)
* **Invalid State Abbreviations:** 438 $\rightarrow$ 0 (Normalized or flagged)
* **Corrupt / Future Dates:** 114 $\rightarrow$ 0 (Safely diverted to quarantine zone)
* **Privacy Compliance:** 172 unconsented narratives $\rightarrow$ 0 (Redacted prior to Silver)
* **dbt Test Coverage:** **77 out of 77** tests passing green across both full and incremental runs on Snowflake.

