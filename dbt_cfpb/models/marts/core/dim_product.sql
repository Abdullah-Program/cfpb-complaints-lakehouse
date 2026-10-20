select distinct
    md5(coalesce(product_clean, 'Unknown')) as product_sk,
    coalesce(product_clean, 'Unknown') as product_clean,
    product_group
from {{ ref('int_complaints_enriched') }}
