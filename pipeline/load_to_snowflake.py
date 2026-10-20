"""Loads local Silver parquet tables into Snowflake (CFPB.SILVER).

Run after Spark silver completes to sync clean data into Snowflake before dbt.
Supports MFA via authenticator='externalbrowser'.
"""

from __future__ import annotations

import os
from pathlib import Path
import duckdb
from dotenv import load_dotenv

load_dotenv()


def get_snowflake_conn():
    import snowflake.connector

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
    role = os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    conn_params = {
        "account": account,
        "user": user,
        "role": role,
        "warehouse": warehouse,
        "client_session_keep_alive": True,
    }

    if password:
        conn_params["password"] = password

    passcode = os.getenv("SNOWFLAKE_PASSCODE")
    if passcode:
        conn_params["passcode"] = passcode

    if authenticator and authenticator != "snowflake":
        conn_params["authenticator"] = authenticator

    return snowflake.connector.connect(**conn_params)


def main():
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    database = os.getenv("SNOWFLAKE_DATABASE", "CFPB")
    silver_schema = os.getenv("SNOWFLAKE_SILVER_SCHEMA", "SILVER")

    print("[snowflake] Connecting with MFA (browser will open if required)...")
    conn = get_snowflake_conn()
    cur = conn.cursor()

    print(f"[snowflake] Ensuring database {database} and schema {silver_schema} exist...")
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {database}.{silver_schema}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"USE SCHEMA {silver_schema}")

    # 1. Load complaints
    print("[snowflake] Reading local Silver complaints parquet...")
    df_complaints = duckdb.read_parquet("data/silver/complaints/**/*.parquet").df()
    # Snowflake uppercase column standard
    df_complaints.columns = [c.upper() for c in df_complaints.columns]

    print(f"[snowflake] Uploading {len(df_complaints)} complaints to {database}.{silver_schema}.COMPLAINTS...")
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df_complaints,
        table_name="COMPLAINTS",
        database=database,
        schema=silver_schema,
        auto_create_table=True,
        overwrite=True,
    )
    print(f"[snowflake] Uploaded complaints: success={success}, rows={nrows}")

    # 2. Load company map
    print("[snowflake] Reading local company_map parquet...")
    df_map = duckdb.read_parquet("data/silver/company_map/*.parquet").df()
    df_map.columns = [c.upper() for c in df_map.columns]

    print(f"[snowflake] Uploading {len(df_map)} rows to {database}.{silver_schema}.COMPANY_MAP...")
    success_map, _, nrows_map, _ = write_pandas(
        conn=conn,
        df=df_map,
        table_name="COMPANY_MAP",
        database=database,
        schema=silver_schema,
        auto_create_table=True,
        overwrite=True,
    )
    print(f"[snowflake] Uploaded company_map: success={success_map}, rows={nrows_map}")

    cur.close()
    conn.close()
    print("[snowflake] Sync completed successfully! Now you can run `dbt build --target snowflake`.")


if __name__ == "__main__":
    main()
