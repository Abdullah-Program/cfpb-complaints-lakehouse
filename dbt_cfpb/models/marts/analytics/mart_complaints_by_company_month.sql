-- Grain: company x month. Powers the "who gets complained about, and how do they respond" dashboard.
with f as (

    select * from {{ ref('fact_complaints') }}

),

c as (

    select company_sk, company_name_clean, company_category from {{ ref('dim_company') }}

),

monthly as (

    select
        c.company_name_clean,
        c.company_category,
        date_trunc('month', f.date_received)::date                         as month,
        count(*)                                                           as complaints,
        avg(case when f.timely_response then 1.0 when not f.timely_response then 0.0 end)  as timely_rate,
        avg(case when f.is_relief then 1.0 else 0.0 end)                   as relief_rate,
        avg(case when f.consumer_disputed then 1.0 when not f.consumer_disputed then 0.0 end) as dispute_rate,
        avg(case when f.has_narrative then 1.0 else 0.0 end)               as narrative_rate,
        avg(f.days_to_company)                                             as avg_days_to_company
    from f
    join c using (company_sk)
    group by 1, 2, 3

)

select
    *,
    rank() over (partition by month order by complaints desc) as complaint_rank_in_month
from monthly
