with claims as (
    select * from {{ ref('int_claims__combined') }}
),

beneficiaries as (
    select * from {{ ref('stg_cms__beneficiaries') }}
),

joined as (
    select
        -- claim fields
        c.claim_key,
        c.claim_id,
        c.claim_type,
        c.claim_from_date,
        c.claim_thru_date,
        c.claim_year,
        c.claim_month,
        c.provider_id,
        c.attending_physician_npi,
        c.primary_diagnosis_code,
        c.drg_code,
        c.duration_days,
        c.claim_payment_amount,
        c.primary_payer_paid_amount,
        c.beneficiary_out_of_pocket,
        c.medicare_payment_ratio,

        -- beneficiary demographic fields
        b.beneficiary_id,
        b.gender,
        b.race,
        b.state_code,
        b.birth_date,
        b.death_date,
        b.is_deceased,
        b.total_chronic_conditions,
        b.total_reimbursement,

        -- chronic condition flags
        b.has_diabetes,
        b.has_chf,
        b.has_cancer,
        b.has_copd,
        b.has_chronic_kidney,
        b.has_alzheimer,
        b.has_depression,
        b.has_ischemic_heart,
        b.has_osteoporosis,
        b.has_arthritis,
        b.has_stroke,

        -- calculate age at time of claim
        datediff(
            year,
            b.birth_date,
            c.claim_from_date
        ) as age_at_claim,

        -- age group segmentation
        case
            when datediff(year, b.birth_date, c.claim_from_date) < 18
                then 'Pediatric (0-17)'
            when datediff(year, b.birth_date, c.claim_from_date) between 18 and 44
                then 'Adult (18-44)'
            when datediff(year, b.birth_date, c.claim_from_date) between 45 and 64
                then 'Middle-Aged (45-64)'
            when datediff(year, b.birth_date, c.claim_from_date) between 65 and 79
                then 'Senior (65-79)'
            when datediff(year, b.birth_date, c.claim_from_date) >= 80
                then 'Elderly (80+)'
            else 'Unknown'
        end as age_group,

        -- chronic condition risk tier
        case
            when b.total_chronic_conditions = 0 then 'Low Risk'
            when b.total_chronic_conditions between 1 and 2 then 'Moderate Risk'
            when b.total_chronic_conditions between 3 and 5 then 'High Risk'
            when b.total_chronic_conditions > 5 then 'Very High Risk'
            else 'Unknown'
        end as risk_tier

    from claims c
    left join beneficiaries b
        on c.beneficiary_id = b.beneficiary_id
)

select * from joined