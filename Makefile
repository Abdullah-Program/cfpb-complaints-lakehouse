# Usage: make help
PYTHON ?= python
DBT    ?= dbt
export LAKE_ROOT ?= $(CURDIR)/data
export DBT_PROFILES_DIR ?= $(CURDIR)/dbt_cfpb
export SILVER_PATH ?= $(LAKE_ROOT)/silver/complaints
export COMPANY_MAP_PATH ?= $(LAKE_ROOT)/silver/company_map
export DBT_DUCKDB_PATH ?= $(LAKE_ROOT)/warehouse/cfpb.duckdb

SAMPLE_ROWS ?= 20000

.PHONY: help setup sample delta spark-full spark-incremental dbt-build dbt-docs demo test lint clean

help:
	@echo "setup              create venv and install dependencies"
	@echo "sample             generate synthetic messy CFPB-shaped CSV (initial load)"
	@echo "delta              generate an incremental delta (new + late-arriving + corrections)"
	@echo "spark-full         raw sample -> bronze -> silver (full rebuild) + DQ report"
	@echo "spark-incremental  delta -> bronze -> silver (incremental merge) + DQ report"
	@echo "dbt-build          dbt seed + build (models, snapshot, tests)"
	@echo "dbt-docs           generate and serve dbt docs"
	@echo "demo               full end-to-end run: initial load, dbt, delta, dbt again"
	@echo "test               pytest (Spark unit + end-to-end tests)"
	@echo "lint               ruff"
	@echo "clean              delete generated data"

setup:
	python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt

sample:
	$(PYTHON) -m pipeline.generate_sample_data --rows $(SAMPLE_ROWS) --out data/raw/sample/initial.csv

delta:
	$(PYTHON) -m pipeline.generate_sample_data --delta --base data/raw/sample/initial.csv --out data/raw/sample/delta_1.csv

spark-full:
	$(PYTHON) -m pipeline.run_pipeline --input data/raw/sample/initial.csv --format csv --mode full

spark-incremental:
	$(PYTHON) -m pipeline.run_pipeline --input data/raw/sample/delta_1.csv --format csv --mode incremental

dbt-build:
	mkdir -p $(LAKE_ROOT)/warehouse
	cd dbt_cfpb && $(DBT) seed && $(DBT) build

dbt-docs:
	cd dbt_cfpb && $(DBT) docs generate && $(DBT) docs serve

dashboard:
	streamlit run app/dashboard.py

demo: clean sample spark-full dbt-build delta spark-incremental
	cd dbt_cfpb && $(DBT) build
	@echo "---- DQ report ----" && cat data/reports/dq_report.md

test:
	$(PYTHON) -m pytest -q

lint:
	ruff check pipeline tests

clean:
	rm -rf data/bronze data/silver data/quarantine data/state data/reports data/warehouse data/_staging data/raw/sample dbt_cfpb/target dbt_cfpb/logs
