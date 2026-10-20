# PROJECT_SPEC: CFPB Complaints Lakehouse (Spark + dbt)

> Audience: a human or an AI coding agent (e.g. Google Antigravity) picking this repo up cold.
> Read this file first. It states what the project is, how the pieces fit, the contracts between
> them, what is verified, and how to extend it safely.

## 1. One-paragraph summary

A batch data-engineering project that turns the messy public **CFPB Consumer Complaint Database**
(banking / consumer-finance complaints, ~2.4M+ rows, updated daily) into trustworthy analytics.
**Apache Spark** does the heavy cleaning (bronze -> silver: typing, normalisation, de-duplication,
entity resolution of company names, quarantine of bad rows, data-quality metrics). **dbt**
(on DuckDB, swappable to Snowflake) models the clean data into a star schema and analytics marts
with tests, snapshots, docs and lineage. Incremental loads, late-arriving data and CFPB
"corrections" are handled idempotently. Everything runs for free on a laptop.

Portfolio headline: *"Messy regulatory data in, measurable quality out"*. The pipeline writes a
before/after report (`data/reports/dq_report.md`): distinct company names 151 -> 30, product
labels 54 -> 9, invalid states 438 -> 0, duplicates removed, etc. (numbers from the bundled
synthetic sample).

## 2. Goals and non-goals

Goals
- Demonstrate Spark + dbt each doing what they are best at, with a clean contract between them.
- Show production habits: idempotency, incremental merge, quarantine, audit tables, tests, CI, orchestration.
- Be runnable offline with synthetic data, and switchable to real CFPB data.

Non-goals
- Not a real-time/streaming system (batch, daily).
- Not a statistical study of complaints (the CFPB data is not a representative sample of consumers).
- No cloud dependency required.

## 3. Architecture

```
 CFPB API (daily delta)  ─┐
 Hugging Face parquet     ├─►  data/raw  ─►  BRONZE  ─►  SILVER  ─►  dbt (DuckDB)  ─►  marts ─► dashboard
 CSV export / synthetic  ─┘     (landing)    (all strings,   (typed, clean,   staging → intermediate →
                                              append-only,    deduped,         core (star schema) +
                                              lineage cols)   flagged)         analytics marts
                                                   │              │
                                                   │              ├─► data/quarantine  (rows failing hard rules)
                                                   │              └─► silver/company_map (entity-resolution audit trail)
                                                   └─► data/reports/dq_report.{md,json}  (raw vs clean metrics)
 Orchestration: Airflow DAG (daily)        CI: GitHub Actions (pytest + dbt build, slim CI)
```

Layer contracts
| Layer | Written by | Format | Rule |
|---|---|---|---|
| raw | `pipeline.ingest` / user | csv / parquet / jsonl | Never modified. |
| bronze | `pipeline.bronze` | parquet, partitioned by `_ingest_date` | Append-only; every value is a STRING; adds `_batch_id`, `_ingested_at`, `_ingest_seq`, `_source_file`. Replayable. |
| silver | `pipeline.silver` | parquet, hive-partitioned by `received_year_month` | One row per `complaint_id`; typed; normalised; flagged. dbt reads this and never writes to it. |
| quarantine | `pipeline.silver` | parquet | Rows that fail hard rules, with `quarantine_reason`. |
| warehouse | dbt | DuckDB file `data/warehouse/cfpb.duckdb` | Schemas: `staging`, `intermediate`, `marts`, `analytics`, `reference`, `snapshots`. |

## 4. Repository layout

```
pipeline/                 Spark + Python package (run with python -m pipeline.<module>)
  config.py               lake paths (LAKE_ROOT), constants
  spark_session.py        SparkSession factory (local[*], ANSI off, dynamic partition overwrite)
  reference.py            single source of truth: states, product map, rules; writes dbt seeds
  cleaning.py             column-level cleaning rules (pure Spark Column functions) + dq flags + hard rules
  company_resolution.py   rules + rapidfuzz entity resolution (driver side, stable across runs)
  ingest.py               hf (parquet download) and api (CFPB search API, per-day paging)
  bronze.py               raw -> bronze
  silver.py               bronze -> silver (full | incremental merge), quarantine, watermark
  quality.py              raw-vs-clean report
  generate_sample_data.py synthetic messy data generator (+ delta generator)
  run_pipeline.py         bronze -> silver -> quality in one command
dbt_cfpb/                 dbt project (dbt-duckdb)
  models/staging          stg_complaints, stg_company_map (views over silver parquet)
  models/intermediate     int_complaints_enriched (joins seeds, derived columns)
  models/marts/core       dim_date, dim_company, dim_product, dim_issue, dim_geography, fact_complaints (incremental)
  models/marts/analytics  5 marts (see §8)
  snapshots               company_master_snapshot (SCD2)
  seeds                   state_codes, product_taxonomy, company_category (generated by pipeline.reference)
  tests                   3 custom generic tests + 3 singular tests
airflow/dags/cfpb_daily.py
.github/workflows/ci.yml
tests/                    pytest: cleaning rules, company resolution, end-to-end incremental + replay
Makefile  Dockerfile  docker-compose.yml  requirements.txt  ruff.toml
```

## 5. Data model

### 5.1 Source columns (CFPB CSV header names)
`Date received, Product, Sub-product, Issue, Sub-issue, Consumer complaint narrative,
Company public response, Company, State, ZIP code, Tags, Consumer consent provided?,
Submitted via, Date sent to company, Company response to consumer, Timely response?,
Consumer disputed?, Complaint ID`.
`bronze.standardise_columns` snake_cases names and maps aliases (`timely` -> `timely_response`,
`company_response` -> `company_response_to_consumer`, `complaint_what_happened` ->
`consumer_complaint_narrative`, ...). Missing columns become NULL; extra columns are dropped.

### 5.2 Cleaning rules (silver)
| Concern | Rule |
|---|---|
| Text | trim, collapse whitespace, empty -> NULL; placeholders (`N/A`, `NA`, `NULL`, `NONE`, `NAN`, `-`) -> NULL |
| Dates | accept `M/d/yyyy` and ISO (`yyyy-MM-dd[...]`); unparseable -> NULL |
| Company | `company_key` = upper, `&`->AND, punctuation removed, trailing legal suffixes removed (INC, LLC, N A, NATIONAL ASSOCIATION, ...), leading THE removed (never reduced to empty). Then resolved to `company_name_clean` (see §6) |
| Product | legacy/renamed labels mapped to canonical (`reference.PRODUCT_MAP`), case-insensitive; unknown labels kept as-is |
| State | upper-case 2-letter code, or full state name -> code; anything else -> NULL (`state_valid=false`) |
| ZIP | `\d{3}XX` = masked (valid), `\d{5}(-\d{4})?` = valid; `zip3` derived; others `zip_valid=false` |
| Consent | -> `provided / not_provided / withdrawn / other / unknown` |
| Narrative | **dropped when consent is `not_provided` or `withdrawn`** (privacy guard); whitespace collapsed; adds char length, word count, mask-token count (`X{2,}`) |
| Booleans | `timely_response`, `consumer_disputed`: Yes/No -> true/false, else NULL |
| Response / channel | normalised to canonical labels; unknown channel -> `Other` |
| Tags | flags `is_older_american`, `is_servicemember` |
| Dedup | one row per `complaint_id`; latest `_ingested_at`, then `_ingest_seq`, wins |

### 5.3 Hard rules -> quarantine (`quarantine_reason`)
`missing_or_non_numeric_complaint_id`, `date_received_unparseable`, `date_received_in_future`,
`date_received_before_cfpb_start` (before 2011-07-21).
Soft problems (invalid state, malformed ZIP, sent-before-received, ...) do NOT drop the row; they
are NULLed/flagged and listed in `dq_flags` (array<string>) so dbt can score them.

### 5.4 Silver schema (`cleaning.SILVER_COLUMNS`)
`complaint_id, date_received, received_year_month, date_sent_to_company, days_to_company,
date_order_valid, product_raw, product_clean, sub_product, issue, sub_issue, company_raw,
company_key, company_name_clean, company_canonical_key, company_resolution_method, state_raw,
state_code, state_valid, zip_raw, zip3, zip_masked, zip_valid, tags, is_older_american,
is_servicemember, consumer_consent_provided, has_narrative, narrative, narrative_char_len,
narrative_word_count, pii_mask_token_count, submitted_via, company_public_response,
company_response_to_consumer, is_relief, timely_response, consumer_disputed, dq_flags,
_batch_id, _ingested_at, _ingest_seq, silver_updated_at`.
Note: `received_year_month` is the hive partition column (not stored inside the parquet files).

## 6. Company entity resolution
1. Spark computes `company_key` (above).
2. `reference.COMPANY_RULES` roll known institutions up to a parent (Bank of America, Wells Fargo, ...).
3. Remaining keys are fuzzy-matched with `rapidfuzz.token_sort_ratio` (cutoff
   `COMPANY_FUZZY_THRESHOLD`, default 88) against canonical keys, most frequent first.
   Guards: identical digit sequences and same first letter.
4. Output table `silver/company_map`: `company_key, canonical_key, canonical_name, method(rule|exact|fuzzy|new), score`.
5. **Stability invariant:** a key mapped once is never re-mapped in later incremental runs.
Known trade-off: rules intentionally roll subsidiaries into the parent (e.g. "Capital One Auto Finance").
Tune `COMPANY_RULES` / threshold for your use case.

## 7. Incremental / late-arriving data (the senior-level part)
- Bronze is append-only; each batch has `_batch_id` and `_ingested_at`.
- Silver keeps a watermark (`data/state/silver_watermark.json`, max `_ingested_at` in micros).
- `silver --mode incremental`:
  1. read bronze rows newer than the watermark;
  2. clean, resolve companies, flag, split valid/quarantine, dedup inside the batch;
  3. find partitions to rewrite = months of the new rows + months currently holding the updated ids;
  4. re-read those partitions, union new rows, dedup by `complaint_id` (latest ingest wins);
  5. write to `data/_staging` first (Spark must not read and overwrite the same path), then
     dynamic-partition-overwrite only the affected months; advance the watermark.
- Replaying the same delta is a no-op for silver (tested). Running with nothing new is a no-op.
- `silver --mode full` deletes silver + quarantine and rebuilds everything, including the company map.
- dbt `fact_complaints`: incremental, `unique_key=complaint_id`, `delete+insert`, filter
  `silver_updated_at >= max(silver_updated_at) - lookback_days` (default 7). On Snowflake use `merge`.
- The Airflow DAG re-pulls the last `LOOKBACK_DAYS` (default 7) from the API every day for the same reason.

## 8. dbt models
- staging (views): `stg_complaints`, `stg_company_map`. Source = hive-partitioned parquet via
  dbt-duckdb `external_location` (env vars `SILVER_PATH`, `COMPANY_MAP_PATH`).
- intermediate: `int_complaints_enriched` (state name/region, product group, company category, send-lag bucket, received_month).
- core (tables): `dim_date`, `dim_company` (+Unknown member), `dim_product`, `dim_issue`, `dim_geography` (+Unknown), `fact_complaints`.
- analytics (tables):
  - `mart_complaints_by_company_month`: complaints, timely/relief/dispute/narrative rates, avg days, rank.
  - `mart_resolution_performance`: product group x year, avg / p50 / p90 days, outcome mix.
  - `mart_product_issue_trends`: month x product x issue, MoM change, rolling 3-month average.
  - `mart_geo_complaints`: state x month, share of month, rank.
  - `mart_data_quality_scorecard`: monthly DQ rates from `dq_flags`.
- snapshot: `company_master_snapshot` (SCD2 on name/category/alias_count).
- exposure: `complaints_dashboard`.
- Surrogate keys are `md5()` of natural keys (`company_canonical_key`, `product_clean`, `issue|sub_issue`).

### dbt tests (78 nodes incl. models)
- generic: `unique`, `not_null`, `accepted_values`, `relationships` (fact -> every dim, staging state -> seed).
- custom generic (`tests/generic`): `date_not_in_future`, `date_not_before`, `rate_between_0_and_1`.
- singular (`tests/`): fact row count equals staging; no narrative without consent; every company key resolved.
- source freshness on `silver.complaints` (`silver_updated_at`, warn after 3 days).

## 9. How to run

```bash
make setup                         # venv + deps (Java 11/17/21 required for Spark)
source .venv/bin/activate
make demo                          # synthetic data: full load -> dbt -> delta -> incremental -> dbt
make test                          # pytest (15 tests, ~2 min)
cd dbt_cfpb && DBT_PROFILES_DIR=. dbt docs generate && dbt docs serve
```
Real data:
```bash
python -m pipeline.ingest hf --repo Mouwiya/cfpb-consumer-complaints --max-files 3
python -m pipeline.run_pipeline --input "data/raw/hf/**/*.parquet" --format parquet --mode full
make dbt-build
# daily:
python -m pipeline.ingest api --since 2026-10-10 --until 2026-10-17
python -m pipeline.run_pipeline --input "data/raw/api/*.jsonl" --format jsonl --mode incremental
```
Environment: `LAKE_ROOT` (default `./data`), `SPARK_DRIVER_MEMORY`, `SPARK_SHUFFLE_PARTITIONS`,
`COMPANY_FUZZY_THRESHOLD`, `SILVER_PATH`, `COMPANY_MAP_PATH`, `DBT_DUCKDB_PATH`.

## 10. Verification status (be honest about it)
Verified in a sandbox with Python 3.13, Java 21, PySpark 4.2, dbt-core 1.12 + dbt-duckdb 1.11, DuckDB 1.5:
- 15/15 pytest tests pass (cleaning rules, entity resolution, full -> incremental -> replay -> no-op).
- `make demo` passes end to end: dbt 77/77 checks pass after the full load AND after the incremental load;
  fact row count == silver row count; corrections applied; snapshot recorded new versions.
- ruff clean.

NOT verified (no network access from the sandbox, or no Docker/Airflow installed):
- `pipeline.ingest hf` and `pipeline.ingest api` against the live Hugging Face / CFPB endpoints.
  The API field map (`ingest.FIELD_MAP`) and the HF column names are from documentation/memory; validate
  against https://cfpb.github.io/ccdb5-api/documentation/ and the repo's real files.
  `bronze.standardise_columns` is alias-tolerant, so small naming differences are usually absorbed.
- The Airflow DAG (compiles, not executed), `docker-compose.yml`, `Dockerfile`, and the GitHub Actions
  workflow (YAML parses, not executed).
- All numbers in this repo come from SYNTHETIC data. Re-run on real data before quoting any metric.
- `PRODUCT_MAP` / product canonical names should be checked against the current CFPB field reference.

## 11. Conventions for contributors / agents
- Cleaning logic lives in `pipeline/cleaning.py` as pure `Column -> Column` functions with a unit test each.
- Reference data lives ONLY in `pipeline/reference.py`; regenerate seeds with
  `python -m pipeline.reference --write-seeds`. Never hand-edit the seed CSVs.
- Spark session must keep `spark.sql.ansi.enabled=false` (Spark 4 defaults it to true, which makes bad
  dates/casts throw instead of becoming NULL).
- Never read and overwrite the same path in one Spark job (use `data/_staging`).
- Keep `data/_staging` and any scratch dir OUTSIDE `data/silver` so dbt/DuckDB globs don't pick it up.
- Do not use Jinja inside dbt `vars`; use `env_var()` directly in the YAML that needs it. dbt-duckdb formats
  `external_location` with Python `str.format_map`, so avoid literal `{` `}` in those strings.
- dbt-duckdb path env vars are relative to the directory dbt runs from (`dbt_cfpb/`); the Makefile exports absolute ones.
- Never log or print narrative text. Narratives are only retained when consent allows it.
- Every behaviour change needs a test; run `make test` and `make demo` before committing.

## 12. Roadmap / suggested next tasks (with acceptance criteria)
1. **Iceberg tables for silver** (Spark Iceberg runtime, local Hadoop/REST catalog): `silver --mode incremental`
   becomes `MERGE INTO`; acceptance = replay test still passes, time-travel query demonstrated.
2. **Snowflake target**: load silver to a stage/table, switch dbt source + profile, incremental `merge`;
   acceptance = `dbt build --target snowflake` green.
3. **Great Expectations / `dbt_expectations`** in addition to the custom tests; acceptance = failing expectation blocks the DAG.
4. **Per-capita analysis**: add a Census population seed + `mart_geo_per_capita`.
5. **Narrative NLP** in Spark (keyword topics, sentiment, language detection) -> `fct_complaint_text_features`.
6. **Dashboard**: Metabase/Superset on `analytics.*` marts; commit screenshots to the README.
7. **Benchmark**: same transformations in Spark SQL vs dbt-on-DuckDB vs dbt-on-Snowflake; publish a short write-up.
8. **Observability**: persist run metadata (rows in/out, quarantine counts, duration) to a `pipeline_runs` table and chart it.
