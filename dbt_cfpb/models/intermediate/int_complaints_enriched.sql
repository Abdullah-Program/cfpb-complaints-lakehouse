-- Enrich staging with reference data (seeds) and derived analytical columns.
with c as (

    select * from {{ ref('stg_complaints') }}

),

states as (

    select state_code, state_name, region from {{ ref('state_codes') }}

),

products as (

    select product_clean, product_group from {{ ref('product_taxonomy') }}

),

companies as (

    select company_name_clean, company_category from {{ ref('company_category') }}

)

select
    c.*,
    date_trunc('month', c.date_received)::date                      as received_month,
    coalesce(s.state_name, 'Unknown')                               as state_name,
    coalesce(s.region, 'Unknown')                                   as region,
    coalesce(p.product_group, 'Other')                              as product_group,
    coalesce(k.company_category, 'Other')                           as company_category,
    case
        when c.days_to_company is null then 'unknown'
        when c.days_to_company < 0     then 'invalid'
        when c.days_to_company = 0     then 'same day'
        when c.days_to_company <= 2    then '1-2 days'
        when c.days_to_company <= 5    then '3-5 days'
        else '6+ days'
    end                                                             as send_lag_bucket
from c
left join states   s on c.state_code         = s.state_code
left join products p on c.product_clean      = p.product_clean
left join companies k on c.company_name_clean = k.company_name_clean
