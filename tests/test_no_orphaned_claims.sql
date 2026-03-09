-- Fails if any claim has no matching beneficiary
-- Every claim must be linked to a known beneficiary
select
    c.claim_id,
    c.claim_type,
    c.beneficiary_id
from {{ ref('int_claims__combined') }} c
left join {{ ref('stg_cms__beneficiaries') }} b
    on c.beneficiary_id = b.beneficiary_id
where b.beneficiary_id is null