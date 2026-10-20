{% if target.name == 'snowflake' %}
with date_spine as (
    select dateadd(day, seq4(), '2011-07-01'::date) as d
    from table(generator(rowcount => 9000))
)
select
    d::date                                   as date_day,
    extract(year from d)::int                 as year,
    extract(quarter from d)::int              as quarter,
    extract(month from d)::int                as month,
    to_varchar(d, 'MMMM')                     as month_name,
    to_varchar(d, 'YYYY-MM')                  as year_month,
    date_trunc('month', d)::date              as month_start,
    date_trunc('week', d)::date               as week_start,
    dayofweekiso(d)                           as day_of_week,
    to_varchar(d, 'DY')                       as day_name,
    dayofweekiso(d) in (6, 7)                 as is_weekend
from date_spine
where d <= '2035-12-31'::date

{% else %}
select
    d::date                                   as date_day,
    extract(year from d)::int                 as year,
    extract(quarter from d)::int              as quarter,
    extract(month from d)::int                as month,
    strftime(d, '%B')                         as month_name,
    strftime(d, '%Y-%m')                      as year_month,
    date_trunc('month', d)::date              as month_start,
    date_trunc('week', d)::date               as week_start,
    isodow(d)                                 as day_of_week,
    strftime(d, '%A')                         as day_name,
    isodow(d) in (6, 7)                       as is_weekend
from (
    select unnest(generate_series(date '2011-07-01', date '2035-12-31', interval 1 day)) as d
)
{% endif %}
