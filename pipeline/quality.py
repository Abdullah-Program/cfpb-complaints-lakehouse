"""Before/after data-quality report: bronze (raw) vs silver (clean).

This is the portfolio headline: measurable improvement, not "I cleaned some data".
Writes data/reports/dq_report.json and data/reports/dq_report.md.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from . import cleaning
from .config import get_paths
from .spark_session import get_spark


def _pct(num: int, den: int) -> float:
    return round(100.0 * num / den, 2) if den else 0.0


def compute(spark: SparkSession) -> dict:
    p = get_paths()
    b = spark.read.parquet(str(p.bronze))
    s = spark.read.parquet(str(p.silver))
    q = spark.read.parquet(str(p.quarantine)) if p.quarantine.exists() else None

    nb, ns = b.count(), s.count()
    nq = q.count() if q is not None else 0

    # ---- raw-side expressions (reuse the exact cleaning rules) ----
    z = cleaning.zip_normalised(F.col("zip_code"))
    consent = cleaning.consent_code(F.col("consumer_consent_provided"))
    bagg = b.agg(
        F.countDistinct("complaint_id").alias("distinct_ids"),
        F.sum(F.col("complaint_id").isNull().cast("int")).alias("null_ids"),
        F.sum(cleaning.clean_token(F.col("state")).isNull().cast("int")).alias("state_missing"),
        F.sum((cleaning.clean_token(F.col("state")).isNotNull() & cleaning.state_code(F.col("state")).isNull()).cast("int")).alias("state_invalid"),
        F.sum(((F.upper(F.trim(F.col("state"))) != cleaning.state_code(F.col("state"))) & cleaning.state_code(F.col("state")).isNotNull()).cast("int")).alias("state_needs_normalising"),
        F.sum(z.isNull().cast("int")).alias("zip_missing"),
        F.sum((z.isNotNull() & ~cleaning.zip_is_valid(z)).cast("int")).alias("zip_invalid"),
        F.sum(cleaning.zip_is_masked(z).cast("int")).alias("zip_masked"),
        F.sum(cleaning.parse_date(F.col("date_received")).isNull().cast("int")).alias("date_received_unparseable"),
        F.sum((cleaning.parse_date(F.col("date_received")) > F.current_date()).cast("int")).alias("date_received_future"),
        F.sum(
            (cleaning.parse_date(F.col("date_sent_to_company")) < cleaning.parse_date(F.col("date_received"))).cast("int")
        ).alias("sent_before_received"),
        F.sum((consent.isin("not_provided", "withdrawn") & cleaning.clean_text(F.col("consumer_complaint_narrative")).isNotNull()).cast("int")).alias("narrative_without_consent"),
        F.countDistinct(F.trim(F.col("company"))).alias("distinct_company_names"),
        F.countDistinct(F.trim(F.col("product"))).alias("distinct_product_values"),
        F.countDistinct(F.lower(F.trim(F.col("company_response_to_consumer")))).alias("distinct_response_values_ci"),
        F.countDistinct(F.trim(F.col("company_response_to_consumer"))).alias("distinct_response_values_raw"),
        F.countDistinct(F.trim(F.col("submitted_via"))).alias("distinct_submitted_via_raw"),
    ).first()

    sagg = s.agg(
        F.countDistinct("complaint_id").alias("distinct_ids"),
        F.sum(F.col("state_code").isNull().cast("int")).alias("state_null"),
        F.sum((F.col("zip_valid") == F.lit(False)).cast("int")).alias("zip_invalid"),
        F.sum(F.col("zip_masked").cast("int")).alias("zip_masked"),
        F.sum((F.col("date_order_valid") == F.lit(False)).cast("int")).alias("sent_before_received"),
        F.sum(F.col("has_narrative").cast("int")).alias("with_narrative"),
        F.countDistinct("company_name_clean").alias("distinct_company_names"),
        F.countDistinct("product_clean").alias("distinct_product_values"),
        F.countDistinct("company_response_to_consumer").alias("distinct_response_values"),
        F.countDistinct("submitted_via").alias("distinct_submitted_via"),
    ).first()

    quarantine_reasons = {}
    if q is not None:
        quarantine_reasons = {r["quarantine_reason"]: r["count"] for r in q.groupBy("quarantine_reason").count().collect()}

    m = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": {"bronze": nb, "silver": ns, "quarantined": nq},
        "duplicate_rows_removed": nb - nq - ns,
        "metrics": [
            ("Duplicate / repeated complaint rows", nb - bagg["null_ids"] - bagg["distinct_ids"], 0),
            ("Rows with missing complaint_id", bagg["null_ids"], 0),
            ("Distinct company names", bagg["distinct_company_names"], sagg["distinct_company_names"]),
            ("Distinct product labels", bagg["distinct_product_values"], sagg["distinct_product_values"]),
            ("Distinct 'company response' labels (case/space variants)", bagg["distinct_response_values_raw"], sagg["distinct_response_values"]),
            ("Distinct 'submitted via' labels", bagg["distinct_submitted_via_raw"], sagg["distinct_submitted_via"]),
            ("Rows with invalid state value", bagg["state_invalid"], 0),
            ("Rows with state needing normalising (case/full name)", bagg["state_needs_normalising"], 0),
            ("Rows with malformed ZIP", bagg["zip_invalid"], sagg["zip_invalid"]),
            ("Rows with unparseable date_received", bagg["date_received_unparseable"], 0),
            ("Rows with future date_received", bagg["date_received_future"], 0),
            ("Rows with date_sent < date_received", bagg["sent_before_received"], sagg["sent_before_received"]),
            ("Narrative present although consent not given", bagg["narrative_without_consent"], 0),
        ],
        "informational": {
            "zip_masked_pct_bronze": _pct(bagg["zip_masked"], nb),
            "zip_masked_pct_silver": _pct(sagg["zip_masked"], ns),
            "state_missing_pct_bronze": _pct(bagg["state_missing"], nb),
            "state_null_pct_silver": _pct(sagg["state_null"], ns),
            "narrative_pct_silver": _pct(sagg["with_narrative"], ns),
        },
        "quarantine_reasons": quarantine_reasons,
    }
    return m


def to_markdown(m: dict) -> str:
    lines = [
        "# Data quality report: raw (bronze) vs clean (silver)",
        "",
        f"_Generated {m['generated_at']}_",
        "",
        f"* Bronze rows: **{m['rows']['bronze']:,}**",
        f"* Silver rows: **{m['rows']['silver']:,}**",
        f"* Quarantined rows: **{m['rows']['quarantined']:,}**",
        "",
        "| Metric | Raw (bronze) | Clean (silver) |",
        "|---|---:|---:|",
    ]
    for name, raw, clean in m["metrics"]:
        lines.append(f"| {name} | {raw:,} | {clean:,} |")
    lines += ["", "## Quarantine reasons", ""]
    if m["quarantine_reasons"]:
        lines += [f"* `{k}`: {v:,}" for k, v in sorted(m["quarantine_reasons"].items())]
    else:
        lines.append("_none_")
    lines += ["", "## Informational", ""]
    lines += [f"* {k}: {v}" for k, v in m["informational"].items()]
    return "\n".join(lines) + "\n"


def run(spark: SparkSession | None = None) -> dict:
    spark = spark or get_spark("cfpb-quality")
    paths = get_paths()
    m = compute(spark)
    paths.reports.mkdir(parents=True, exist_ok=True)
    serialisable = {**m, "metrics": [{"metric": n, "raw": r, "clean": c} for n, r, c in m["metrics"]]}
    (paths.reports / "dq_report.json").write_text(json.dumps(serialisable, indent=2, default=int))
    (paths.reports / "dq_report.md").write_text(to_markdown(m))
    print(f"[quality] report -> {paths.reports / 'dq_report.md'}")
    return m


def main() -> None:
    run()


if __name__ == "__main__":
    main()
