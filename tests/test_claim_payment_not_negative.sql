-- -- Fails if any claim payment amount is negative
-- -- Negative payments indicate a data loading or transformation error
-- select
--     claim_id,
--     claim_type,
--     claim_payment_amount
-- from {{ ref('int_claims__combined') }}
-- where claim_payment_amount < 0

{{ config(severity='warn') }}

-- Warns if any claim payment amount is negative
-- Note: CMS DE-SynPUF uses negative amounts for claim adjustments
-- and reversals — this is expected Medicare claims behavior
select
    claim_id,
    claim_type,
    claim_payment_amount
from {{ ref('int_claims__combined') }}
where claim_payment_amount < 0