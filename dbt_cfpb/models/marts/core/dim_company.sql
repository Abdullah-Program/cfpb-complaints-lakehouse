-- One row per canonical company (after Spark entity resolution).
with mapped as (

    select
        company_canonical_key,
        any_value(company_name_clean) as company_name_clean,
        count(*) as alias_count,
        sum(case when resolution_method = 'fuzzy' then 1 else 0 end) as fuzzy_matched_aliases
    from {{ ref('stg_company_map') }}
    group by company_canonical_key

),

categories as (

    select company_name_clean, company_category from {{ ref('company_category') }}

),

companies as (

    select
        md5(m.company_canonical_key) as company_sk,
        m.company_canonical_key,
        m.company_name_clean,
        coalesce(k.company_category, 'Other') as company_category,
        m.alias_count,
        m.fuzzy_matched_aliases
    from mapped m
    left join categories k on m.company_name_clean = k.company_name_clean

)

select * from companies

union all

select md5('UNKNOWN'), 'UNKNOWN', 'Unknown company', 'Other', 0, 0
