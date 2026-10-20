-- Monthly data-quality scorecard built from the flags the Spark layer attached to every row.
-- Watch this over time: a sudden spike in any rate usually means an upstream schema or taxonomy change.
select
    received_month                                                          as month,
    count(*)                                                                as total_rows,
    avg(case when {{ array_contains_val('dq_flags', 'state_missing') }}       then 1.0 else 0.0 end) as pct_state_missing,
    avg(case when {{ array_contains_val('dq_flags', 'state_invalid') }}       then 1.0 else 0.0 end) as pct_state_invalid,
    avg(case when {{ array_contains_val('dq_flags', 'state_normalised') }}    then 1.0 else 0.0 end) as pct_state_normalised,
    avg(case when {{ array_contains_val('dq_flags', 'zip_masked') }}          then 1.0 else 0.0 end) as pct_zip_masked,
    avg(case when {{ array_contains_val('dq_flags', 'zip_invalid') }}         then 1.0 else 0.0 end) as pct_zip_invalid,
    avg(case when {{ array_contains_val('dq_flags', 'sent_before_received') }} then 1.0 else 0.0 end) as pct_sent_before_received,
    avg(case when {{ array_contains_val('dq_flags', 'company_renamed') }}     then 1.0 else 0.0 end) as pct_company_renamed,
    avg(case when {{ array_contains_val('dq_flags', 'product_remapped') }}    then 1.0 else 0.0 end) as pct_product_remapped,
    avg(case when {{ array_contains_val('dq_flags', 'consent_unknown') }}     then 1.0 else 0.0 end) as pct_consent_unknown,
    avg(case when has_narrative                                                then 1.0 else 0.0 end) as pct_with_narrative
from {{ ref('int_complaints_enriched') }}
group by 1
