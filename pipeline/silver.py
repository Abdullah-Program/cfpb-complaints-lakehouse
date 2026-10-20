"""Bronze -> silver.

full         rebuild silver (and the quarantine + company map) from ALL of bronze
incremental  only process bronze batches newer than the stored watermark, then
             MERGE them into silver:
               * the partitions touched by new rows (and the partitions that
                 currently hold the complaint_ids being updated) are re-read,
               * unioned with the new rows,
               * de-duplicated by complaint_id (latest ingest wins),
               * and only those partitions are rewritten (dynamic overwrite).
             This is what makes late-arriving rows and CFPB "corrections"
             (a company response changing later) idempotent.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import cleaning
from .company_resolution import Resolution, build_company_map
from .config import get_paths
from .spark_session import get_spark

MAP_SCHEMA = "company_key string, canonical_key string, canonical_name string, method string, score double"


def _exists(p: Path) -> bool:
    return p.exists() and any(p.iterdir())


def _read_watermark(path: Path) -> int | None:
    if not path.exists():
        return None
    return json.loads(path.read_text()).get("last_ingested_at_micros")


def _write_watermark(path: Path, micros: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_ingested_at_micros": micros}))


def _load_existing_map(spark: SparkSession, path: Path) -> dict[str, Resolution]:
    if not _exists(path):
        return {}
    rows = spark.read.parquet(str(path)).collect()
    return {
        r["company_key"]: Resolution(r["canonical_key"], r["canonical_name"], r["method"], r["score"])
        for r in rows
    }


def _resolve_companies(spark: SparkSession, staged: DataFrame, map_path: Path, rebuild: bool) -> DataFrame:
    counts = (
        staged.filter(F.col("company_key").isNotNull())
        .groupBy("company_key", "company_raw")
        .count()
        .collect()
    )
    observed = [(r["company_key"], r["company_raw"], r["count"]) for r in counts]
    existing = {} if rebuild else _load_existing_map(spark, map_path)
    mapping = build_company_map(observed, existing)

    rows = [(k, v.canonical_key, v.canonical_name, v.method, float(v.score)) for k, v in mapping.items()]
    map_df = spark.createDataFrame(rows, schema=MAP_SCHEMA)

    # persist the full map (small) - it is also the audit trail
    tmp = map_path.parent / "_company_map_tmp"
    map_df.coalesce(1).write.mode("overwrite").parquet(str(tmp))
    if map_path.exists():
        shutil.rmtree(map_path)
    shutil.move(str(tmp), str(map_path))

    lookup = map_df.select(
        "company_key",
        F.col("canonical_name").alias("company_name_clean"),
        F.col("canonical_key").alias("company_canonical_key"),
        F.col("method").alias("company_resolution_method"),
    )
    return staged.join(F.broadcast(lookup), "company_key", "left")


def run(mode: str = "incremental", spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark("cfpb-silver")
    paths = get_paths()
    if not _exists(paths.bronze):
        raise SystemExit("bronze is empty - run pipeline.bronze first")

    bronze = spark.read.parquet(str(paths.bronze))
    incremental = mode == "incremental" and _exists(paths.silver)
    watermark = _read_watermark(paths.watermark_file) if incremental else None

    batch = bronze
    if incremental and watermark is not None:
        batch = bronze.filter(F.unix_micros("_ingested_at") > F.lit(watermark))
    batch = batch.cache()
    if batch.isEmpty():
        print("[silver] nothing new in bronze - nothing to do")
        batch.unpersist()
        return {"mode": mode, "new_rows": 0}

    new_max_micros = batch.agg(F.max(F.unix_micros("_ingested_at"))).first()[0]

    # 1. clean + resolve companies + flag
    staged = cleaning.clean_base(batch)
    resolved = _resolve_companies(spark, staged, paths.company_map, rebuild=not incremental)
    resolved = cleaning.add_dq_flags(resolved).withColumn("silver_updated_at", F.current_timestamp())

    # 2. hard rules -> quarantine
    valid, bad = cleaning.split_valid_quarantine(resolved)
    quarantine_cols = [
        "_batch_id",
        "_ingest_seq",
        "_complaint_id_raw",
        "_date_received_raw",
        "_date_sent_raw",
        "company_raw",
        "product_raw",
        "state_raw",
        "zip_raw",
        "quarantine_reason",
    ]
    bad_out = bad.select(*quarantine_cols).withColumn("_quarantined_at", F.current_timestamp())
    q_mode = "append" if incremental else "overwrite"
    if q_mode == "overwrite" and paths.quarantine.exists():
        shutil.rmtree(paths.quarantine)
    n_bad = bad_out.count()
    if n_bad:
        bad_out.write.mode("append").parquet(str(paths.quarantine))

    # 3. dedup inside the new batch (latest ingest wins)
    w = Window.partitionBy("complaint_id").orderBy(F.col("_ingested_at").desc(), F.col("_ingest_seq").desc())
    new_rows = (
        valid.select(*cleaning.SILVER_COLUMNS)
        .withColumn("_rn", F.row_number().over(w))
        .filter("_rn = 1")
        .drop("_rn")
    )

    # 4. merge with existing silver (incremental) or write fresh (full)
    if incremental:
        existing = spark.read.option("basePath", str(paths.silver)).parquet(str(paths.silver))
        new_ids = new_rows.select("complaint_id")
        touched_old = (
            existing.join(F.broadcast(new_ids), "complaint_id", "left_semi").select("received_year_month").distinct()
        )
        months = {r[0] for r in touched_old.collect()} | {
            r[0] for r in new_rows.select("received_year_month").distinct().collect()
        }
        existing_slice = existing.filter(F.col("received_year_month").isin(*months)).select(*cleaning.SILVER_COLUMNS)
        merged = (
            existing_slice.unionByName(new_rows)
            .withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1")
            .drop("_rn")
        )
    else:
        if paths.silver.exists():
            shutil.rmtree(paths.silver)
        merged = new_rows
        months = {r[0] for r in new_rows.select("received_year_month").distinct().collect()}

    # Materialise first: Spark must not read and overwrite the same path in one job.
    if paths.staging.exists():
        shutil.rmtree(paths.staging)
    merged.write.mode("overwrite").parquet(str(paths.staging / "silver_merge"))
    staged_merge = spark.read.parquet(str(paths.staging / "silver_merge"))
    n_written = staged_merge.count()
    (
        staged_merge.repartition("received_year_month")
        .write.mode("overwrite")
        .partitionBy("received_year_month")
        .parquet(str(paths.silver))
    )
    shutil.rmtree(paths.staging, ignore_errors=True)

    _write_watermark(paths.watermark_file, int(new_max_micros))
    n_new = batch.count()
    batch.unpersist()
    print(
        f"[silver] mode={'incremental' if incremental else 'full'} new_bronze_rows={n_new} "
        f"quarantined={n_bad} partitions_rewritten={len(months)} rows_in_rewritten_partitions={n_written}"
    )
    return {
        "mode": "incremental" if incremental else "full",
        "new_rows": n_new,
        "quarantined": n_bad,
        "partitions_rewritten": len(months),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Bronze -> silver cleaning")
    ap.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    args = ap.parse_args()
    run(args.mode)


if __name__ == "__main__":
    main()
