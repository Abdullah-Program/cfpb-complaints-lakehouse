"""Daily CFPB complaints pipeline.

    API delta (last N days) -> bronze -> silver (incremental merge) -> DQ report
        -> dbt seed -> dbt build (incl. tests) -> dbt docs

Each task shells out to the same commands you run locally, so the DAG stays a thin
orchestration layer and the logic stays in `pipeline/` and `dbt_cfpb/`.

Env (set on the Airflow worker):
    PROJECT_ROOT   absolute path of this repo (default /opt/airflow/project)
    LAKE_ROOT      lake location (default $PROJECT_ROOT/data)
    LOOKBACK_DAYS  how many days back to re-pull each run (default 7) - CFPB can
                   publish complaints late and update them after the fact, so we
                   always re-pull a window; silver merge makes it idempotent.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow/project")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

ENV = {
    "LAKE_ROOT": os.getenv("LAKE_ROOT", f"{PROJECT_ROOT}/data"),
    "DBT_PROFILES_DIR": f"{PROJECT_ROOT}/dbt_cfpb",
    "SILVER_PATH": os.getenv("LAKE_ROOT", f"{PROJECT_ROOT}/data") + "/silver/complaints",
    "COMPANY_MAP_PATH": os.getenv("LAKE_ROOT", f"{PROJECT_ROOT}/data") + "/silver/company_map",
    "DBT_DUCKDB_PATH": os.getenv("LAKE_ROOT", f"{PROJECT_ROOT}/data") + "/warehouse/cfpb.duckdb",
}

default_args = {
    "owner": "abdullah",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cfpb_complaints_daily",
    description="CFPB complaints: ingest -> Spark clean -> dbt model + test",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,  # the silver merge is not safe to run concurrently
    default_args=default_args,
    tags=["cfpb", "spark", "dbt"],
) as dag:
    cd = f"cd {PROJECT_ROOT}"

    ingest = BashOperator(
        task_id="ingest_api_delta",
        bash_command=(
            f"{cd} && python -m pipeline.ingest api "
            "--since {{ macros.ds_add(ds, -" + str(LOOKBACK_DAYS) + ") }} --until {{ ds }}"
        ),
        env=ENV,
        append_env=True,
    )

    bronze = BashOperator(
        task_id="spark_bronze",
        bash_command=(
            f"{cd} && python -m pipeline.bronze --input 'data/raw/api/complaints_*_{{{{ ds }}}}.jsonl' "
            "--format jsonl --batch-id {{ run_id | replace(':', '') | replace('+', '') }}"
        ),
        env=ENV,
        append_env=True,
    )

    silver = BashOperator(
        task_id="spark_silver_incremental",
        bash_command=f"{cd} && python -m pipeline.silver --mode incremental",
        env=ENV,
        append_env=True,
    )

    quality = BashOperator(
        task_id="data_quality_report",
        bash_command=f"{cd} && python -m pipeline.quality",
        env=ENV,
        append_env=True,
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{cd}/dbt_cfpb && dbt seed",
        env=ENV,
        append_env=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{cd}/dbt_cfpb && dbt build",
        env=ENV,
        append_env=True,
    )

    dbt_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{cd}/dbt_cfpb && dbt source freshness || true",
        env=ENV,
        append_env=True,
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs",
        bash_command=f"{cd}/dbt_cfpb && dbt docs generate",
        env=ENV,
        append_env=True,
    )

    ingest >> bronze >> silver >> quality >> dbt_seed >> dbt_build >> [dbt_freshness, dbt_docs]
