select distinct
    md5(coalesce(cast(issue as varchar), 'Unknown') || '|' || coalesce(cast(sub_issue as varchar), 'Unknown')) as issue_sk,
    coalesce(cast(issue as varchar), 'Unknown')                                                                 as issue,
    coalesce(cast(sub_issue as varchar), 'Unknown')                                                             as sub_issue
from {{ ref('int_complaints_enriched') }}
