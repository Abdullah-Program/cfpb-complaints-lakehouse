"""One-shot runner: raw file(s) -> bronze -> silver -> quality report.

    python -m pipeline.run_pipeline --input "data/raw/sample/initial.csv" --format csv --mode full
    python -m pipeline.run_pipeline --input "data/raw/sample/delta_1.csv" --format csv --mode incremental
"""

from __future__ import annotations

import argparse

from . import bronze, quality, silver
from .spark_session import get_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--format", required=True, choices=["csv", "parquet", "json", "jsonl"])
    ap.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    ap.add_argument("--batch-id", default=None)
    args = ap.parse_args()

    spark = get_spark("cfpb-pipeline")
    bronze.run(args.input, args.format, args.batch_id, spark=spark)
    silver.run(args.mode, spark=spark)
    quality.run(spark=spark)
    spark.stop()


if __name__ == "__main__":
    main()
