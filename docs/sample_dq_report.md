# Data quality report: raw (bronze) vs clean (silver)

_Generated 2026-10-17T17:56:33+00:00_

* Bronze rows: **21,550**
* Silver rows: **20,784**
* Quarantined rows: **167**

| Metric | Raw (bronze) | Clean (silver) |
|---|---:|---:|
| Duplicate / repeated complaint rows | 600 | 0 |
| Rows with missing complaint_id | 53 | 0 |
| Distinct company names | 151 | 30 |
| Distinct product labels | 54 | 9 |
| Distinct 'company response' labels (case/space variants) | 18 | 6 |
| Distinct 'submitted via' labels | 8 | 6 |
| Rows with invalid state value | 438 | 0 |
| Rows with state needing normalising (case/full name) | 409 | 0 |
| Rows with malformed ZIP | 292 | 281 |
| Rows with unparseable date_received | 65 | 0 |
| Rows with future date_received | 49 | 0 |
| Rows with date_sent < date_received | 159 | 109 |
| Narrative present although consent not given | 172 | 0 |

## Quarantine reasons

* `date_received_in_future`: 49
* `date_received_unparseable`: 65
* `missing_or_non_numeric_complaint_id`: 53

## Informational

* zip_masked_pct_bronze: 60.39
* zip_masked_pct_silver: 60.36
* state_missing_pct_bronze: 2.9
* state_null_pct_silver: 4.95
* narrative_pct_silver: 29.79
