"""Production Lakehouse Pipeline Orchestrator.

Orchestrates the entire lifecycle:
    1. Validation & Environment Checks
    2. Spark Bronze Ingest
    3. Spark Silver Cleaning, Entity Resolution & Dynamic Partition Merge
    4. Data Quality Quarantine Guardrail (Circuit Breaker)
    5. Sync to Cloud Warehouse (Snowflake) if target=snowflake
    6. dbt Build & Test Execution (Topological DAG execution)
    7. Final Execution Observability Summary Card

Usage:
    python -m pipeline.orchestrate --mode incremental --target snowflake
    python -m pipeline.orchestrate --mode full --target dev
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

from pipeline import bronze, quality, silver
from pipeline.config import get_paths
from pipeline.spark_session import get_spark


class Colors:
    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"
    RESET = "\033[0m"


def print_banner(text: str) -> None:
    line = "=" * 80
    print(f"\n{Colors.BOLD}{Colors.CYAN}{line}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN} {text.upper()}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}{line}{Colors.RESET}\n")


def print_step(num: int, title: str) -> None:
    print(f"{Colors.BOLD}{Colors.BLUE}▶ STEP {num}: {title}{Colors.RESET}")


def run_command(cmd: list[str], cwd: Path | None = None) -> tuple[int, str]:
    """Runs a shell command and streams stdout while capturing output."""
    process = subprocess.Popen(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    output_lines = []
    for line in iter(process.stdout.readline, ""):
        sys.stdout.write(line)
        sys.stdout.flush()
        output_lines.append(line)
    process.stdout.close()
    return_code = process.wait()
    return return_code, "".join(output_lines)


def orchestrate(
    input_path: str,
    file_format: str,
    mode: str,
    target: str,
    max_quarantine_rate: float = 0.20,
) -> bool:
    start_time = time.time()
    paths = get_paths()
    pipeline_status = "SUCCESS"
    metrics: dict[str, str | int | float] = {
        "mode": mode,
        "target": target,
        "input_path": input_path,
        "raw_rows": 0,
        "silver_rows": 0,
        "quarantined_rows": 0,
        "quarantine_pct": 0.0,
        "partitions_rewritten": 0,
        "spark_duration_sec": 0.0,
        "sync_duration_sec": 0.0,
        "dbt_duration_sec": 0.0,
        "dbt_tests_passed": 0,
    }

    print_banner(f"CFPB Lakehouse Orchestrator | Mode: {mode.upper()} | Target: {target.upper()}")
    print(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    print(f"Input file: {input_path}")
    print(f"Format:     {file_format}\n")

    # --------------------------------------------------------------------------
    # Step 1: Pre-flight validation
    # --------------------------------------------------------------------------
    print_step(1, "Pre-Flight Checks & Target Validation")
    if not Path(input_path).exists():
        print(f"{Colors.RED}[ERROR] Input file does not exist: {input_path}{Colors.RESET}")
        return False
    print(f"{Colors.GREEN}✔ Input file verified: {input_path}{Colors.RESET}")

    if target == "snowflake":
        account = os.getenv("SNOWFLAKE_ACCOUNT")
        user = os.getenv("SNOWFLAKE_USER")
        if not account or not user:
            print(f"{Colors.RED}[ERROR] SNOWFLAKE_ACCOUNT or SNOWFLAKE_USER not set in .env{Colors.RESET}")
            return False
        print(f"{Colors.GREEN}✔ Snowflake credentials loaded for user '{user}' on account '{account}'{Colors.RESET}")
    print()

    # --------------------------------------------------------------------------
    # Step 2: Spark Bronze Ingest
    # --------------------------------------------------------------------------
    spark_t0 = time.time()
    print_step(2, "Spark Ingestion -> Bronze (Append-Only Raw Layer)")
    spark = get_spark("cfpb-orchestrator")
    try:
        bronze_result = bronze.run(input_path, file_format, spark=spark)
        metrics["raw_rows"] = bronze_result.get("rows", 0)
        print(f"{Colors.GREEN}✔ Bronze ingest complete: {metrics['raw_rows']:,} rows ingested.{Colors.RESET}\n")

        # ----------------------------------------------------------------------
        # Step 3: Spark Silver Cleaning, Entity Resolution & Dynamic Merge
        # ----------------------------------------------------------------------
        print_step(3, "Spark Silver Cleaning, Deduplication & Dynamic Partition Merge")
        silver_result = silver.run(mode, spark=spark)
        metrics["quarantined_rows"] = silver_result.get("quarantined", 0)
        metrics["partitions_rewritten"] = silver_result.get("partitions_rewritten", 0)

        # Quality Report
        quality.run(spark=spark)

        # Calculate quarantine rate
        if metrics["raw_rows"] > 0:
            metrics["quarantine_pct"] = round(
                (float(metrics["quarantined_rows"]) / float(metrics["raw_rows"])) * 100, 2
            )
    finally:
        spark.stop()
    metrics["spark_duration_sec"] = round(time.time() - spark_t0, 2)

    # --------------------------------------------------------------------------
    # Step 4: Circuit Breaker / Data Quality Gate
    # --------------------------------------------------------------------------
    print_step(4, "Data Quality Circuit Breaker Evaluation")
    print(f"Quarantined rows: {metrics['quarantined_rows']} ({metrics['quarantine_pct']}%)")
    if (metrics["quarantine_pct"] / 100.0) > max_quarantine_rate:
        print(
            f"{Colors.RED}[CIRCUIT BREAKER TRIGGERED] Quarantine rate {metrics['quarantine_pct']}% "
            f"exceeds safety threshold ({max_quarantine_rate * 100}%). Halting downstream load!{Colors.RESET}"
        )
        return False
    print(f"{Colors.GREEN}✔ Quality Gate Passed: Quarantine rate is within healthy boundaries.{Colors.RESET}\n")

    # --------------------------------------------------------------------------
    # Step 5: Cloud Warehouse Sync (Snowflake)
    # --------------------------------------------------------------------------
    if target == "snowflake":
        sync_t0 = time.time()
        print_step(5, "Syncing Silver Tables & Company Mappings to Snowflake")
        from pipeline import load_to_snowflake

        load_to_snowflake.main()
        metrics["sync_duration_sec"] = round(time.time() - sync_t0, 2)
        print(f"{Colors.GREEN}✔ Snowflake sync completed in {metrics['sync_duration_sec']}s.{Colors.RESET}\n")
    else:
        print_step(5, "Local Mode (Target=Dev DuckDB) - Skipping Cloud Sync\n")

    # --------------------------------------------------------------------------
    # Step 6: dbt Build & Verification (Topological Execution)
    # --------------------------------------------------------------------------
    dbt_t0 = time.time()
    print_step(6, f"Executing dbt Build & Data Tests on Target '{target}'")
    dbt_dir = Path(__file__).resolve().parent.parent / "dbt_cfpb"

    # Always ensure seeds are updated
    print(f"Running: dbt seed --target {target}...")
    rc_seed, _ = run_command(["dbt", "seed", "--target", target], cwd=dbt_dir)
    if rc_seed != 0:
        print(f"{Colors.RED}[ERROR] dbt seed failed with exit code {rc_seed}{Colors.RESET}")
        return False

    # Execute models, snapshots, and tests
    print(f"Running: dbt build --target {target}...")
    rc_build, build_out = run_command(["dbt", "build", "--target", target], cwd=dbt_dir)
    metrics["dbt_duration_sec"] = round(time.time() - dbt_t0, 2)

    if rc_build != 0:
        print(f"{Colors.RED}[ERROR] dbt build failed with exit code {rc_build}{Colors.RESET}")
        return False

    print(f"{Colors.GREEN}✔ dbt build & tests completed successfully in {metrics['dbt_duration_sec']}s.{Colors.RESET}\n")

    # --------------------------------------------------------------------------
    # Step 7: Final Execution Observability Summary Card
    # --------------------------------------------------------------------------
    total_time = round(time.time() - start_time, 2)
    print_banner("Pipeline Execution Summary & Observability Card")

    summary_card = f"""
┌──────────────────────────────────────────────────────────────────────────────┐
│ STATUS: {Colors.GREEN}{Colors.BOLD}COMPLETED SUCCESSFULLY 🟢{Colors.RESET}                                      │
├───────────────────────────────────┬──────────────────────────────────────────┤
│ Metric                            │ Value                                    │
├───────────────────────────────────┼──────────────────────────────────────────┤
│ Execution Mode                    │ {str(metrics['mode']).upper():<40} │
│ Target Warehouse                  │ {str(metrics['target']).upper():<40} │
│ Raw Rows Ingested                 │ {f"{metrics['raw_rows']:,}":<40} │
│ Quarantined Rows                  │ {f"{metrics['quarantined_rows']:,} ({metrics['quarantine_pct']}%)":<40} │
│ Monthly Partitions Touched        │ {str(metrics['partitions_rewritten']):<40} │
│ Spark Processing Time             │ {f"{metrics['spark_duration_sec']} seconds":<40} │
│ Snowflake Sync Time               │ {f"{metrics['sync_duration_sec']} seconds":<40} │
│ dbt Build & Tests Time            │ {f"{metrics['dbt_duration_sec']} seconds":<40} │
│ Total End-to-End Runtime          │ {f"{Colors.BOLD}{total_time} seconds{Colors.RESET}":<49} │
└───────────────────────────────────┴──────────────────────────────────────────┘
"""
    print(summary_card)
    return True


def main() -> None:
    ap = argparse.ArgumentParser(description="End-to-End CFPB Lakehouse Pipeline Orchestrator")
    ap.add_argument(
        "--input",
        default="data/raw/sample/delta_1.csv",
        help="Input data file (default: data/raw/sample/delta_1.csv)",
    )
    ap.add_argument(
        "--format",
        default="csv",
        choices=["csv", "parquet", "json", "jsonl"],
        help="Input format (default: csv)",
    )
    ap.add_argument(
        "--mode",
        default="incremental",
        choices=["incremental", "full"],
        help="Pipeline mode (default: incremental)",
    )
    ap.add_argument(
        "--target",
        default="snowflake",
        choices=["snowflake", "dev"],
        help="Warehouse target (default: snowflake)",
    )
    ap.add_argument(
        "--max-quarantine-rate",
        type=float,
        default=0.20,
        help="Circuit breaker threshold (default: 0.20 / 20%)",
    )
    args = ap.parse_args()

    success = orchestrate(
        input_path=args.input,
        file_format=args.format,
        mode=args.mode,
        target=args.target,
        max_quarantine_rate=args.max_quarantine_rate,
    )
    if not success:
        sys.exit(1)


if __name__ == "__main__":
    main()
