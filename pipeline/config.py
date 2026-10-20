"""Central place for lake paths and tunables.

Everything lives under LAKE_ROOT (default: ./data) so the whole project can be
pointed at a temp dir in tests, or swapped for an object-store URI later.

    data/
      raw/          landing zone (files exactly as downloaded)
      bronze/       raw rows as strings + lineage columns, append-only
      silver/       cleaned, deduplicated, typed (what dbt reads)
      quarantine/   rows that failed hard validation rules
      reports/      data-quality reports (JSON + Markdown)
      state/        small JSON files (incremental watermark)
      warehouse/    DuckDB file written by dbt
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def raw(self) -> Path:
        return self.root / "raw"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze" / "complaints"

    @property
    def silver(self) -> Path:
        return self.root / "silver" / "complaints"

    @property
    def company_map(self) -> Path:
        return self.root / "silver" / "company_map"

    @property
    def quarantine(self) -> Path:
        return self.root / "quarantine" / "complaints"

    @property
    def staging(self) -> Path:
        # Scratch space used while merging silver. Deliberately OUTSIDE the
        # silver tree so dbt/DuckDB globs never pick it up.
        return self.root / "_staging"

    @property
    def reports(self) -> Path:
        return self.root / "reports"

    @property
    def state(self) -> Path:
        return self.root / "state"

    @property
    def watermark_file(self) -> Path:
        return self.state / "silver_watermark.json"


def get_paths() -> Paths:
    """Resolve paths at call time so tests can override LAKE_ROOT."""
    return Paths(root=Path(os.getenv("LAKE_ROOT", "data")).resolve())


# CFPB started publishing complaints in July 2011; anything earlier is bad data.
MIN_VALID_DATE = "2011-07-21"

# Fuzzy company matching threshold (rapidfuzz token_sort_ratio, 0-100).
COMPANY_FUZZY_THRESHOLD = int(os.getenv("COMPANY_FUZZY_THRESHOLD", "88"))
