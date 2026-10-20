"""Company entity resolution.

CFPB company names are free-ish text: the same institution shows up as
"BANK OF AMERICA, NATIONAL ASSOCIATION", "Bank of America N.A.", "BANK OF AMERCIA"...

Strategy (cheap, explainable, auditable):
  1. Spark builds a normalised `company_key` (upper-case, punctuation and legal
     suffixes stripped) -> this alone collapses most formatting variants.
  2. Rules roll well-known institutions up to a parent name (COMPANY_RULES).
  3. Remaining keys are fuzzy-matched (rapidfuzz token_sort_ratio) against the
     canonical keys seen so far, most frequent first. Guards prevent merging
     names that differ in a number or in their first letter.
  4. The result is persisted as a mapping table (silver/company_map) and is
     STABLE across incremental runs: a key that was mapped once keeps its mapping.

The mapping table is also the audit trail: every row says *how* it was resolved
(`rule` | `exact` | `fuzzy` | `new`) and with what score.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass

from rapidfuzz import fuzz, process

from .config import COMPANY_FUZZY_THRESHOLD
from .reference import COMPANY_RULES

_RULES = [(re.compile(pattern), name) for pattern, name in COMPANY_RULES]


@dataclass
class Resolution:
    canonical_key: str
    canonical_name: str
    method: str  # rule | exact | fuzzy | new
    score: float


def apply_rules(key: str) -> str | None:
    for rx, name in _RULES:
        if rx.search(key):
            return name
    return None


def _digits(s: str) -> list[str]:
    return re.findall(r"\d+", s)


def _compatible(a: str, b: str) -> bool:
    """Guards against false merges at the fuzzy stage."""
    if _digits(a) != _digits(b):
        return False
    # typos inside the first word are common, so only require the same first letter
    return a[:1] == b[:1]


def build_company_map(
    observed: list[tuple[str, str, int]],
    existing: dict[str, Resolution] | None = None,
    threshold: int = COMPANY_FUZZY_THRESHOLD,
) -> dict[str, Resolution]:
    """observed: (company_key, display_name, n_rows). Returns key -> Resolution."""
    agg: dict[str, dict] = {}
    for key, display, n in observed:
        if not key:
            continue
        slot = agg.setdefault(key, {"n": 0, "displays": Counter()})
        slot["n"] += n
        if display:
            slot["displays"][display] += n

    result: dict[str, Resolution] = dict(existing or {})

    # canonical key -> canonical name (pre-seeded with rule parents so that
    # typos of a big bank match it even if the exact spelling never appears)
    canon: dict[str, str] = {name.upper(): name for _, name in COMPANY_RULES}
    for res in result.values():
        canon[res.canonical_key] = res.canonical_name
    choices = list(canon)

    new_keys = sorted((k for k in agg if k not in result), key=lambda k: (-agg[k]["n"], k))
    for key in new_keys:
        top_display = agg[key]["displays"].most_common(1)[0][0] if agg[key]["displays"] else key

        rule_name = apply_rules(key)
        if rule_name:
            result[key] = Resolution(rule_name.upper(), rule_name, "rule", 100.0)
            continue

        if key in canon:
            result[key] = Resolution(key, canon[key], "exact", 100.0)
            continue

        match = process.extractOne(
            key, choices, scorer=fuzz.token_sort_ratio, score_cutoff=threshold
        )
        if match and _compatible(key, match[0]):
            ckey, score, _ = match
            result[key] = Resolution(ckey, canon[ckey], "fuzzy", float(score))
            continue

        canon[key] = top_display
        choices.append(key)
        result[key] = Resolution(key, top_display, "new", 100.0)

    return result
