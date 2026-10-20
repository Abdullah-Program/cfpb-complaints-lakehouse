"""Raw files -> bronze.

Bronze keeps every incoming row, every value as a STRING (nothing is cast or
dropped), plus lineage columns. It is append-only, so you can always replay
silver from bronze.

Accepts the three shapes we meet in practice:
  * CFPB CSV export        ("Date received", "Consumer consent provided?", ...)
  * Hugging Face parquet   (column naming differs between repos)
  * CFPB API JSON lines    (written by pipeline.ingest)
"""

from __future__ import annotations

import argparse
import re
import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import get_paths
from .spark_session import get_spark

BRONZE_COLUMNS = [
    "complaint_id",
    "date_received",
    "product",
    "sub_product",
    "issue",
    "sub_issue",
    "consumer_complaint_narrative",
    "company_public_response",
    "company",
    "state",
    "zip_code",
    "tags",
    "consumer_consent_provided",
    "submitted_via",
    "date_sent_to_company",
    "company_response_to_consumer",
    "timely_response",
    "consumer_disputed",
]

# snake_case(source column) -> canonical bronze column
ALIASES = {
    "timely": "timely_response",
    "company_response": "company_response_to_consumer",
    "complaint_what_happened": "consumer_complaint_narrative",
    "consumer_consent_provided_": "consumer_consent_provided",
    "zipcode": "zip_code",
    "zip": "zip_code",
    "id": "complaint_id",
}


def snake(name: str) -> str:
    return re.sub(r"[^0-9a-zA-Z]+", "_", name.strip()).strip("_").lower()


def read_raw(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    fmt = fmt.lower()
    if fmt == "csv":
        return (
            spark.read.option("header", "true")
            .option("multiLine", "true")  # narratives contain newlines
            .option("quote", '"')
            .option("escape", '"')
            .option("inferSchema", "false")
            .csv(path)
        )
    if fmt == "parquet":
        return spark.read.parquet(path)
    if fmt in ("json", "jsonl"):
        return spark.read.json(path)
    raise ValueError(f"unsupported format: {fmt}")


def standardise_columns(df: DataFrame) -> DataFrame:
    """Rename to canonical snake_case, add missing columns as NULL, cast to string."""
    sources: dict[str, list[str]] = {c: [] for c in BRONZE_COLUMNS}
    for col in df.columns:
        if col.startswith("_"):  # e.g. _metadata
            continue
        s = snake(col)
        target = ALIASES.get(s, s)
        if target in sources:
            sources[target].append(col)

    exprs = []
    for target in BRONZE_COLUMNS:
        cols = [F.col(f"`{c}`").cast("string") for c in sources[target]]
        if not cols:
            exprs.append(F.lit(None).cast("string").alias(target))
        elif len(cols) == 1:
            exprs.append(cols[0].alias(target))
        else:
            exprs.append(F.coalesce(*cols).alias(target))
    return df.select(*exprs, F.col("_metadata.file_path").alias("_source_file"))


def run(input_path: str, fmt: str, batch_id: str | None = None, spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark("cfpb-bronze")
    paths = get_paths()
    batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]

    raw = read_raw(spark, input_path, fmt)
    df = (
        standardise_columns(raw)
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_seq", F.monotonically_increasing_id())
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
    )

    n = df.count()
    df.write.mode("append").partitionBy("_ingest_date").parquet(str(paths.bronze))
    print(f"[bronze] batch={batch_id} rows={n} -> {paths.bronze}")
    return {"batch_id": batch_id, "rows": n}


def main() -> None:
    ap = argparse.ArgumentParser(description="Load raw files into bronze (append-only)")
    ap.add_argument("--input", required=True, help="file / glob / directory")
    ap.add_argument("--format", required=True, choices=["csv", "parquet", "json", "jsonl"])
    ap.add_argument("--batch-id", default=None)
    args = ap.parse_args()
    run(args.input, args.format, args.batch_id)


if __name__ == "__main__":
    main()
