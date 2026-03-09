-- Fails if any beneficiary age at claim is unreasonable
-- Age below 0 or above 125 indicates a birth date error
select
    claim_id,
    beneficiary_id,
    age_at_claim
from {{ ref('int_claims__with_beneficiary') }}
where age_at_claim < 0
   or age_at_claim > 125