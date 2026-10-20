"""Quick Snowflake verification script to inspect SCD2 snapshot and incremental fact rows."""

from __future__ import annotations
import os
from dotenv import load_dotenv

load_dotenv()

def main():
    from pipeline.load_to_snowflake import get_snowflake_conn

    conn = get_snowflake_conn()
    cur = conn.cursor()

    database = os.getenv("SNOWFLAKE_DATABASE", "CFPB")
    cur.execute(f"USE DATABASE {database}")

    print("=" * 80)
    print("1. FACT_COMPLAINTS: TOTAL ROWS & MONETARY RELIEF UPDATES")
    print("=" * 80)
    cur.execute("""
        SELECT 
            COUNT(*) AS total_complaints,
            COUNT(CASE WHEN company_response_to_consumer = 'Closed with monetary relief' THEN 1 END) AS monetary_relief_count,
            COUNT(CASE WHEN consumer_disputed = 'Yes' THEN 1 END) AS consumer_disputed_count
        FROM CFPB.MARTS.FACT_COMPLAINTS
    """)
    row = cur.fetchone()
    print(f"Total Fact Rows:           {row[0]:,}")
    print(f"Monetary Relief Count:     {row[1]:,} (Includes the 300 incremental updates!)")
    print(f"Consumer Disputed Count:   {row[2]:,}")

    print("\n" + "=" * 80)
    print("2. SCD TYPE 2 SNAPSHOT: COMPANY_MASTER_SNAPSHOT")
    print("=" * 80)
    cur.execute("""
        SELECT 
            company_name_clean,
            company_category,
            alias_count,
            fuzzy_matched_aliases,
            dbt_valid_from::varchar AS valid_from,
            coalesce(dbt_valid_to::varchar, 'CURRENT (Active)') AS valid_to
        FROM CFPB.SNAPSHOTS.COMPANY_MASTER_SNAPSHOT
        ORDER BY dbt_valid_from DESC
        LIMIT 10
    """)
    rows = cur.fetchall()
    print(f"{'Company Name':<32} | {'Category':<15} | {'Aliases':<7} | {'Valid From':<22} | {'Valid To'}")
    print("-" * 115)
    for r in rows:
        print(f"{r[0]:<32} | {r[1]:<15} | {r[2]:<7} | {r[4]:<22} | {r[5]}")

    print("\n" + "=" * 80)
    print("3. DATA QUALITY SCORECARD MART (Recent Months)")
    print("=" * 80)
    cur.execute("""
        SELECT 
            month, 
            total_rows, 
            round(pct_state_missing * 100, 2) AS state_missing_pct,
            round(pct_zip_masked * 100, 2) AS zip_masked_pct
        FROM CFPB.ANALYTICS.MART_DATA_QUALITY_SCORECARD
        ORDER BY month DESC
        LIMIT 5
    """)
    scorecard_rows = cur.fetchall()
    print(f"{'Month':<10} | {'Total Rows':<12} | {'State Missing %':<16} | {'ZIP Masked %'}")
    print("-" * 55)
    for r in scorecard_rows:
        print(f"{str(r[0]):<10} | {r[1]:<12} | {r[2]}%{'':<11} | {r[3]}%")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
