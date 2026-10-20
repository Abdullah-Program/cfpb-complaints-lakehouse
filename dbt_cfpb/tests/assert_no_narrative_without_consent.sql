-- Privacy guard: a narrative may only exist when the consumer consented (or consent is unrecorded).
select complaint_id, consumer_consent_provided
from {{ ref('stg_complaints') }}
where has_narrative
  and consumer_consent_provided in ('not_provided', 'withdrawn')
