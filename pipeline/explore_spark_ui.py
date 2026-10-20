"""Interactive script to explore Spark UI and Catalyst Optimizer plans on localhost:4040."""

from __future__ import annotations
import time
from pyspark.sql import functions as F
from pipeline.spark_session import get_spark

def main():
    print("=" * 80)
    print("STARTING SPARK SESSION WITH SPARK UI ENABLED...")
    print("=" * 80)
    
    spark = get_spark("spark-ui-masterclass")
    ui_url = spark.sparkContext.uiWebUrl or "http://localhost:4040"
    
    print(f"\n[SPARK UI ACTIVE] Navigate in your browser to: {ui_url}\n")
    
    # 1. Read Silver Complaints
    print("Step 1: Reading Silver Parquet dataset (20,784 rows)...")
    silver = spark.read.option("basePath", "data/silver/complaints").parquet("data/silver/complaints")
    
    # 2. Trigger Action 1: Count & Simple Filter
    print("Triggering Job 1: Complaints by Company (Aggregation & Shuffle)...")
    top_companies = (
        silver.groupBy("company_name_clean")
        .agg(
            F.count("*").alias("total_complaints"),
            F.count(F.when(F.col("has_narrative") == True, 1)).alias("with_narratives"),
            F.round(F.avg(F.when(F.col("timely_response") == "Yes", 1.0).otherwise(0.0)) * 100, 2).alias("timely_pct")
        )
        .orderBy(F.desc("total_complaints"))
    )
    top_companies.show(10, truncate=False)
    
    # 3. Trigger Action 2: Join with Company Map (Broadcast vs SortMerge Join)
    print("\nTriggering Job 2: Broadcast Join with Company Map...")
    company_map = spark.read.parquet("data/silver/company_map")
    joined = silver.join(F.broadcast(company_map), "company_key", "inner")
    print(f"Joined records count: {joined.count():,}")

    # 4. Print Catalyst Physical Plan
    print("\n" + "=" * 80)
    print("CATALYST OPTIMIZER: PHYSICAL EXECUTION PLAN (.explain(True))")
    print("=" * 80)
    top_companies.explain(mode="formatted")

    print("\n" + "=" * 80)
    print(f"🔥 SPARK UI IS LIVE AT: {ui_url}")
    print("Open the link in your browser to inspect:")
    print("  1. 'Jobs' Tab: View Job 0, Job 1, Job 2 and execution time.")
    print("  2. 'Stages' Tab: Click into a Stage to see the Visual DAG (WholeStageCodegen).")
    print("  3. 'Executors' Tab: See memory usage, GC time, and input/output bytes.")
    print("=" * 80)
    
    try:
        input("\nPress [ENTER] in this terminal when you are done to close Spark Session...")
    except (KeyboardInterrupt, EOFError):
        pass
    finally:
        print("\nStopping Spark session...")
        spark.stop()
        print("Spark stopped cleanly.")

if __name__ == "__main__":
    main()
