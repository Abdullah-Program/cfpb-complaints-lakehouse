-- Grain: product group x year. How quickly and how favourably complaints get resolved.
select
    p.product_group,
    extract(year from f.date_received)::int                                  as year,
    count(*)                                                                 as complaints,
    avg(f.days_to_company)                                                   as avg_days_to_company,
    {% if target.name == 'snowflake' %}
    percentile_cont(0.5) within group (order by f.days_to_company)            as p50_days_to_company,
    percentile_cont(0.9) within group (order by f.days_to_company)            as p90_days_to_company,
    {% else %}
    quantile_cont(f.days_to_company, 0.5)                                    as p50_days_to_company,
    quantile_cont(f.days_to_company, 0.9)                                    as p90_days_to_company,
    {% endif %}
    avg(case when f.timely_response then 1.0 when not f.timely_response then 0.0 end) as timely_rate,
    avg(case when f.is_relief then 1.0 else 0.0 end)                         as relief_rate,
    avg(case when f.company_response_to_consumer = 'Closed without relief' then 1.0 else 0.0 end)
                                                                             as closed_without_relief_rate,
    avg(case when f.company_response_to_consumer = 'Closed with explanation' then 1.0 else 0.0 end)
                                                                             as closed_with_explanation_rate
from {{ ref('fact_complaints') }} f
join {{ ref('dim_product') }} p using (product_sk)
where f.days_to_company >= 0
group by 1, 2
