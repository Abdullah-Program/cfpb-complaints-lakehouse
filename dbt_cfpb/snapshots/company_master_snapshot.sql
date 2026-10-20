{% snapshot company_master_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='company_sk',
        strategy='check',
        check_cols=['company_name_clean', 'company_category', 'alias_count']
    )
}}

-- SCD Type 2 history of the company dimension: when entity resolution merges new
-- spelling variants into a company (alias_count grows) or a company is re-categorised,
-- the old version is closed and a new one opened.
select * from {{ ref('dim_company') }}

{% endsnapshot %}
