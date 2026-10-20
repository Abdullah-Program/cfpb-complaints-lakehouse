select state_code, state_name, region from {{ ref('state_codes') }}

union all

select 'Unknown', 'Unknown', 'Unknown'
