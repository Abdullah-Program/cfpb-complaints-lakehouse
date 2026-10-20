-- Grain: month x product x issue, with month-over-month change and a 3-month rolling average.
with monthly as (

    select
        date_trunc('month', f.date_received)::date as month,
        p.product_clean,
        i.issue,
        count(*)                                   as complaints
    from {{ ref('fact_complaints') }} f
    join {{ ref('dim_product') }} p using (product_sk)
    join {{ ref('dim_issue') }}   i using (issue_sk)
    group by 1, 2, 3

)

select
    month,
    product_clean,
    issue,
    complaints,
    lag(complaints) over (partition by product_clean, issue order by month) as prev_month_complaints,
    case when lag(complaints) over (partition by product_clean, issue order by month) > 0
         then (complaints - lag(complaints) over (partition by product_clean, issue order by month)) * 1.0 / lag(complaints) over (partition by product_clean, issue order by month)
    end                                                                     as mom_change_pct,
    avg(complaints) over (partition by product_clean, issue order by month rows between 2 preceding and current row) as rolling_3m_avg
from monthly
