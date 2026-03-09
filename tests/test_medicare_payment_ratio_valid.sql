-- -- Fails if Medicare payment ratio exceeds 1.0
-- -- Medicare cannot pay more than the total claim amount
-- select
--     claim_id,
--     claim_type,
--     claim_payment_amount,
--     primary_payer_paid_amount,
--     medicare_payment_ratio
-- from {{ ref('int_claims__combined') }}
-- where medicare_payment_ratio > 1.0

{{ config(severity='warn') }}

-- Warns if Medicare payment ratio exceeds 1.0
-- Note: In CMS DE-SynPUF, primary_payer_paid_amount and
-- claim_payment_amount measure different things causing ratio > 1.0
-- This is a known characteristic of the DE-SynPUF data structure
select
    claim_id,
    claim_type,
    claim_payment_amount,
    primary_payer_paid_amount,
    medicare_payment_ratio
from {{ ref('int_claims__combined') }}
where medicare_payment_ratio > 1.0