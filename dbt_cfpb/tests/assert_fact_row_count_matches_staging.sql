-- Reconciliation: the fact table must hold exactly one row per silver complaint.
-- Returns a row (= test fails) when the counts differ.
with s as (select count(*) as n from {{ ref('stg_complaints') }}),
     f as (select count(*) as n from {{ ref('fact_complaints') }})
select s.n as staging_rows, f.n as fact_rows
from s, f
where s.n <> f.n
