-- Grain: state x month. (Per-capita rates need Census population: see the README roadmap.)
with monthly as (

    select
        date_trunc('month', f.date_received)::date as month,
        g.state_code,
        g.state_name,
        g.region,
        count(*)                                   as complaints
    from {{ ref('fact_complaints') }} f
    join {{ ref('dim_geography') }} g using (state_code)
    group by 1, 2, 3, 4

)

select
    *,
    complaints * 1.0 / sum(complaints) over (partition by month)  as share_of_month,
    rank() over (partition by month order by complaints desc)     as state_rank_in_month
from monthly
