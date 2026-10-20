-- Every complaint with a company must have a row in the company map (entity resolution is complete).
select c.complaint_id, c.company_key
from {{ ref('stg_complaints') }} c
left join {{ ref('stg_company_map') }} m on c.company_key = m.company_key
where c.company_key is not null
  and m.company_key is null
