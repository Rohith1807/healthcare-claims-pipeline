-- Fails if any claim date is in the future
-- A claim cannot be processed before it occurs
select
    claim_id,
    claim_type,
    claim_from_date
from {{ ref('int_claims__combined') }}
where claim_from_date > current_date()