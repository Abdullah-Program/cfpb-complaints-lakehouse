-- One row per complaint, straight from the Spark silver layer.
-- Staging only renames / casts; business logic lives downstream.
select
    cast(complaint_id as bigint) as complaint_id,
    {% if target.name == 'snowflake' %}
    to_timestamp_ntz(date_received, 6)::date        as date_received,
    to_varchar(to_timestamp_ntz(date_received, 6), 'YYYY-MM') as received_year_month,
    to_timestamp_ntz(date_sent_to_company, 6)::date as date_sent_to_company,
    {% else %}
    date_received,
    received_year_month,
    date_sent_to_company,
    {% endif %}
    days_to_company,
    date_order_valid,
    product_raw,
    product_clean,
    cast(sub_product as varchar) as sub_product,
    cast(issue as varchar) as issue,
    cast(sub_issue as varchar) as sub_issue,
    company_raw,
    company_key,
    company_name_clean,
    company_canonical_key,
    company_resolution_method,
    state_code,
    state_valid,
    zip3,
    zip_masked,
    zip_valid,
    tags,
    is_older_american,
    is_servicemember,
    consumer_consent_provided,
    has_narrative,
    narrative_char_len,
    narrative_word_count,
    pii_mask_token_count,
    submitted_via,
    company_public_response,
    company_response_to_consumer,
    is_relief,
    timely_response,
    consumer_disputed,
    dq_flags,
    _batch_id,
    _ingested_at,
    {% if target.name == 'snowflake' %}
    to_timestamp_ntz(silver_updated_at, 6)           as silver_updated_at
    {% else %}
    silver_updated_at
    {% endif %}
from {{ source('silver', 'complaints') }}
