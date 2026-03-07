with inpatient as (
    select
        claim_id,
        beneficiary_id,
        claim_from_date,
        claim_thru_date,
        provider_id,
        attending_physician_npi,
        claim_payment_amount,
        beneficiary_out_of_pocket,
        primary_payer_paid_amount,
        claim_type,
        length_of_stay_days       as duration_days,
        admitting_diagnosis_code  as primary_diagnosis_code,
        drg_code,
        null                      as part_b_deductible_amount
    from {{ ref('stg_cms__inpatient_claims') }}
),

outpatient as (
    select
        claim_id,
        beneficiary_id,
        claim_from_date,
        claim_thru_date,
        provider_id,
        attending_physician_npi,
        claim_payment_amount,
        beneficiary_out_of_pocket,
        primary_payer_paid_amount,
        claim_type,
        claim_duration_days       as duration_days,
        null                      as primary_diagnosis_code,
        null                      as drg_code,
        part_b_deductible_amount
    from {{ ref('stg_cms__outpatient_claims') }}
),

combined as (
    select * from inpatient
    union all
    select * from outpatient
),

final as (
    select
        -- generate a unique surrogate key across both claim types
        {{ dbt_utils.generate_surrogate_key(['claim_id', 'claim_type', 'claim_from_date']) }}
                                as claim_key,
        claim_id,
        beneficiary_id,
        claim_type,
        claim_from_date,
        claim_thru_date,
        year(claim_from_date)   as claim_year,
        month(claim_from_date)  as claim_month,
        provider_id,
        attending_physician_npi,
        primary_diagnosis_code,
        drg_code,
        duration_days,
        claim_payment_amount,
        primary_payer_paid_amount,
        beneficiary_out_of_pocket,
        part_b_deductible_amount,

        -- payment ratio: what fraction did Medicare cover
        case
            when claim_payment_amount > 0
            then round(primary_payer_paid_amount / claim_payment_amount, 4)
            else null
        end as medicare_payment_ratio

    from combined
)

select * from final