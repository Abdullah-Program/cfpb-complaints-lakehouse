select
    company_key,
    canonical_key  as company_canonical_key,
    canonical_name as company_name_clean,
    method         as resolution_method,
    score          as resolution_score
from {{ source('silver', 'company_map') }}
