"""End-to-end: synthetic CSV -> bronze -> silver (full) -> delta -> silver (incremental)."""

from pyspark.sql import functions as F

from pipeline import bronze, quality, silver
from pipeline.config import get_paths
from pipeline.generate_sample_data import make_delta, make_initial
from pipeline.reference import STATES


def _silver(spark):
    return spark.read.parquet(str(get_paths().silver))


def test_full_then_incremental_then_replay(spark, lake):
    raw = lake / "initial.csv"
    delta = lake / "delta.csv"
    make_initial(rows=1500, seed=7, out=raw)

    bronze.run(str(raw), "csv", spark=spark)
    silver.run("full", spark=spark)

    s = _silver(spark)
    n_full = s.count()
    assert n_full == s.select("complaint_id").distinct().count(), "complaint_id must be unique"

    # data-quality invariants of the clean layer
    assert s.filter(F.col("date_received") > F.current_date()).count() == 0
    assert s.filter(F.col("state_code").isNotNull() & ~F.col("state_code").isin(*STATES)).count() == 0
    assert s.filter(F.col("consumer_consent_provided").isin("not_provided", "withdrawn") & F.col("has_narrative")).count() == 0
    assert s.filter(F.col("company_name_clean").isNull() & F.col("company_raw").isNotNull()).count() == 0
    assert s.select("product_clean").distinct().count() <= 12

    q = spark.read.parquet(str(get_paths().quarantine))
    assert q.count() > 0
    reasons = {r["quarantine_reason"] for r in q.select("quarantine_reason").distinct().collect()}
    assert reasons <= {
        "missing_or_non_numeric_complaint_id",
        "date_received_unparseable",
        "date_received_in_future",
        "date_received_before_cfpb_start",
    }

    # ---- incremental: 800 new + 150 late-arriving + 300 corrections ----
    make_delta(raw, seed=8, out=delta, new_rows=80, updates=30, late=15)
    corrected_ids = [
        int(r["complaint_id"])
        for r in spark.read.option("header", True).csv(str(delta))
        .filter(F.col("Company response to consumer") == "Closed with monetary relief")
        .filter(F.col("Consumer disputed?") == "Yes")
        .select("Complaint ID").withColumnRenamed("Complaint ID", "complaint_id").collect()
        if r["complaint_id"]
    ]
    before_relief = _silver(spark).filter(F.col("complaint_id").isin(corrected_ids) & F.col("is_relief")).count()

    bronze.run(str(delta), "csv", spark=spark)
    result = silver.run("incremental", spark=spark)
    assert result["new_rows"] > 0

    s2 = _silver(spark).cache()
    n_inc = s2.count()
    assert n_inc > n_full  # new + late rows landed
    assert n_inc == s2.select("complaint_id").distinct().count()  # still unique after merge
    after_relief = s2.filter(F.col("complaint_id").isin(corrected_ids) & F.col("is_relief")).count()
    assert after_relief == len(corrected_ids) > before_relief  # corrections applied in place

    # ---- replay the same delta as a new batch: silver must not change ----
    bronze.run(str(delta), "csv", spark=spark)
    silver.run("incremental", spark=spark)
    s3 = _silver(spark)
    assert s3.count() == n_inc
    assert (
        s3.filter(F.col("complaint_id").isin(corrected_ids) & F.col("is_relief")).count() == len(corrected_ids)
    )

    # nothing new -> no-op
    assert silver.run("incremental", spark=spark)["new_rows"] == 0

    # report is produced and shows the improvement
    m = quality.run(spark=spark)
    metrics = {name: (raw_v, clean_v) for name, raw_v, clean_v in m["metrics"]}
    assert metrics["Distinct company names"][1] < metrics["Distinct company names"][0]
    assert metrics["Rows with invalid state value"][1] == 0
    assert (get_paths().reports / "dq_report.md").exists()
