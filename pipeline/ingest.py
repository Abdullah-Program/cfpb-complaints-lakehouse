"""Landing-zone downloaders (raw files only, no transformation).

  hf   download parquet files of a Hugging Face dataset repo (initial / backfill load)
  api  pull a date window from the CFPB search API (daily delta, late corrections)

    python -m pipeline.ingest hf --repo Mouwiya/cfpb-consumer-complaints --max-files 2
    python -m pipeline.ingest api --since 2025-07-01 --until 2025-07-07

NOTE: the HF repo layout and the live CFPB API response shape can change.
Check https://cfpb.github.io/ccdb5-api/documentation/ and the repo's file list
if either step fails, then adjust FIELD_MAP / the glob below.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from .config import get_paths

API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"

# API _source field -> canonical bronze column name
FIELD_MAP = {
    "complaint_id": "complaint_id",
    "date_received": "date_received",
    "product": "product",
    "sub_product": "sub_product",
    "issue": "issue",
    "sub_issue": "sub_issue",
    "complaint_what_happened": "consumer_complaint_narrative",
    "company_public_response": "company_public_response",
    "company": "company",
    "state": "state",
    "zip_code": "zip_code",
    "tags": "tags",
    "consumer_consent_provided": "consumer_consent_provided",
    "submitted_via": "submitted_via",
    "date_sent_to_company": "date_sent_to_company",
    "company_response": "company_response_to_consumer",
    "timely": "timely_response",
    "consumer_disputed": "consumer_disputed",
}


def ingest_hf(repo: str, max_files: int | None) -> Path:
    from huggingface_hub import HfApi, hf_hub_download

    out = get_paths().raw / "hf" / repo.replace("/", "__")
    out.mkdir(parents=True, exist_ok=True)
    files = sorted(f for f in HfApi().list_repo_files(repo, repo_type="dataset") if f.endswith(".parquet"))
    if not files:
        raise SystemExit(f"no .parquet files found in {repo}; check the repo layout")
    if max_files:
        files = files[:max_files]
    for f in files:
        local = hf_hub_download(repo, f, repo_type="dataset", local_dir=out)
        print(f"downloaded {f} -> {local}")
    return out


def _fetch_day(session, day: date, page_size: int = 500, pause: float = 0.3, max_per_day: int = 5000) -> list[dict]:
    rows: list[dict] = []
    frm = 0
    while True:
        # Elasticsearch / CFPB API limit: frm + size cannot exceed 10,000
        if frm + page_size > 10000 or len(rows) >= max_per_day:
            break
        params = {
            "date_received_min": day.isoformat(),
            "date_received_max": day.isoformat(),
            "size": min(page_size, 10000 - frm),
            "frm": frm,
            "sort": "created_date_desc",
            "no_aggs": "true",
            "field": "all",
        }
        r = session.get(API_URL, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        hits = payload["hits"]["hits"] if isinstance(payload, dict) else payload
        for h in hits:
            src = h.get("_source", h)
            rows.append({dst: src.get(srcname) for srcname, dst in FIELD_MAP.items()})
        if len(hits) < page_size or len(rows) >= max_per_day:
            return rows
        frm += page_size
        time.sleep(pause)
    return rows


def ingest_api(since: date, until: date, max_per_day: int = 5000, limit: int | None = None) -> Path:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1.5, status_forcelist=[429, 500, 502, 503, 504])))
    out_dir = get_paths().raw / "api"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"complaints_{since.isoformat()}_{until.isoformat()}.jsonl"

    n = 0
    with open(out, "w", encoding="utf-8") as fh:
        day = since
        while day <= until:
            print(f"[api] Fetching complaints for {day}...")
            day_rows = _fetch_day(session, day, max_per_day=max_per_day)
            for row in day_rows:
                fh.write(json.dumps(row, default=str) + "\n")
                n += 1
                if limit and n >= limit:
                    print(f"reached limit of {limit} rows")
                    break
            if limit and n >= limit:
                break
            day += timedelta(days=1)
    print(f"fetched {n} complaints ({since}..{until}) -> {out}")
    return out


def _d(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    h = sub.add_parser("hf")
    h.add_argument("--repo", default="Mouwiya/cfpb-consumer-complaints")
    h.add_argument("--max-files", type=int, default=None)
    a = sub.add_parser("api")
    a.add_argument("--since", required=True, type=_d)
    a.add_argument("--until", default=date.today().isoformat(), type=_d)
    a.add_argument("--limit", type=int, default=None, help="Stop after fetching this many total rows")
    a.add_argument("--max-per-day", type=int, default=3000, help="Max rows per single day")
    args = ap.parse_args()
    if args.cmd == "hf":
        ingest_hf(args.repo, args.max_files)
    else:
        ingest_api(args.since, args.until, max_per_day=args.max_per_day, limit=args.limit)


if __name__ == "__main__":
    main()
