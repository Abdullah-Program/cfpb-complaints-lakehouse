{{
    config(
        materialized='incremental',
        unique_key='complaint_id',
        incremental_strategy='merge' if target.name == 'snowflake' else 'delete+insert',
        on_schema_change='append_new_columns'
    )
}}

-- Grain: one row per complaint.
-- Incremental logic: pick up rows whose silver_updated_at is within the lookback
-- window of what we already hold. That covers brand-new complaints, late-arriving
-- ones and CFPB corrections (same complaint_id, changed attributes) - delete+insert
-- on complaint_id makes re-runs idempotent.
-- (On Snowflake/Delta you can switch incremental_strategy to 'merge'.)

select
    complaint_id,
    date_received,
    date_sent_to_company,
    md5(coalesce(company_canonical_key, 'UNKNOWN')) as company_sk,
    md5(coalesce(product_clean, 'Unknown')) as product_sk,
    md5(coalesce(cast(issue as varchar), 'Unknown') || '|' || coalesce(cast(sub_issue as varchar), 'Unknown')) as issue_sk,
    coalesce(state_code, 'Unknown') as state_code,
    zip3,
    submitted_via,
    consumer_consent_provided,
    company_response_to_consumer,
    company_public_response,
    timely_response,
    consumer_disputed,
    is_relief,
    is_older_american,
    is_servicemember,
    days_to_company,
    send_lag_bucket,
    has_narrative,
    narrative_word_count,
    pii_mask_token_count,
    {% if target.name == 'snowflake' %}
    coalesce(array_size(dq_flags), 0)                                                  as dq_flag_count,
    {% else %}
    len(dq_flags)                                                                      as dq_flag_count,
    {% endif %}
    silver_updated_at
from {{ ref('int_complaints_enriched') }}

{% if is_incremental() %}
where silver_updated_at >= (
    select max(silver_updated_at) - interval '{{ var("lookback_days") }} days' from {{ this }}
)
{% endif %}
