with claims as (
    select * from {{ ref('int_claims__with_beneficiary') }}
),

final as (
    select
        -- time dimensions
        claim_year,
        claim_month,
        date_trunc('month', claim_from_date)    as claim_month_start,

        -- claim dimensions
        claim_type,
        age_group,
        risk_tier,
        gender,
        race,
        state_code,

        -- volume metrics
        count(*)                                as total_claims,
        count(distinct beneficiary_id)          as unique_patients,
        count(distinct provider_id)             as unique_providers,

        -- payment metrics
        sum(claim_payment_amount)               as total_medicare_payments,
        avg(claim_payment_amount)               as avg_medicare_payment,
        min(claim_payment_amount)               as min_claim_payment,
        max(claim_payment_amount)               as max_claim_payment,

        -- beneficiary cost sharing
        sum(beneficiary_out_of_pocket)          as total_beneficiary_out_of_pocket,
        avg(beneficiary_out_of_pocket)          as avg_beneficiary_out_of_pocket,

        -- primary payer
        sum(primary_payer_paid_amount)          as total_primary_payer_paid,

        -- duration metrics
        avg(duration_days)                      as avg_duration_days,
        max(duration_days)                      as max_duration_days,

        -- medicare coverage ratio
        avg(medicare_payment_ratio)             as avg_medicare_payment_ratio

    from claims
    where claim_from_date is not null
    group by
        claim_year,
        claim_month,
        date_trunc('month', claim_from_date),
        claim_type,
        age_group,
        risk_tier,
        gender,
        race,
        state_code
)

select * from final