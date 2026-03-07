with beneficiaries as (
    select * from {{ ref('stg_cms__beneficiaries') }}
),

inpatient_summary as (
    select
        beneficiary_id,
        count(*)                            as total_inpatient_claims,
        sum(claim_payment_amount)           as total_inpatient_payments,
        avg(claim_payment_amount)           as avg_inpatient_payment,
        avg(length_of_stay_days)            as avg_length_of_stay,
        max(length_of_stay_days)            as max_length_of_stay,
        min(claim_from_date)                as first_inpatient_date,
        max(claim_from_date)                as last_inpatient_date
    from {{ ref('stg_cms__inpatient_claims') }}
    group by beneficiary_id
),

outpatient_summary as (
    select
        beneficiary_id,
        count(*)                            as total_outpatient_claims,
        sum(claim_payment_amount)           as total_outpatient_payments,
        avg(claim_payment_amount)           as avg_outpatient_payment,
        min(claim_from_date)                as first_outpatient_date,
        max(claim_from_date)                as last_outpatient_date
    from {{ ref('stg_cms__outpatient_claims') }}
    group by beneficiary_id
),

final as (
    select
        b.beneficiary_id,
        b.birth_date,
        b.death_date,
        b.gender,
        b.race,
        b.state_code,
        b.is_deceased,
        b.total_chronic_conditions,
        b.total_reimbursement,

        -- chronic flags
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

        -- current age
        datediff(year, b.birth_date, current_date()) as current_age,

        -- age group
        case
            when datediff(year, b.birth_date, current_date()) < 18
                then 'Pediatric (0-17)'
            when datediff(year, b.birth_date, current_date()) between 18 and 44
                then 'Adult (18-44)'
            when datediff(year, b.birth_date, current_date()) between 45 and 64
                then 'Middle-Aged (45-64)'
            when datediff(year, b.birth_date, current_date()) between 65 and 79
                then 'Senior (65-79)'
            when datediff(year, b.birth_date, current_date()) >= 80
                then 'Elderly (80+)'
            else 'Unknown'
        end as age_group,

        -- risk tier
        case
            when b.total_chronic_conditions = 0 then 'Low Risk'
            when b.total_chronic_conditions between 1 and 2 then 'Moderate Risk'
            when b.total_chronic_conditions between 3 and 5 then 'High Risk'
            when b.total_chronic_conditions > 5 then 'Very High Risk'
            else 'Unknown'
        end as risk_tier,

        -- inpatient stats
        coalesce(i.total_inpatient_claims,   0) as total_inpatient_claims,
        coalesce(i.total_inpatient_payments, 0) as total_inpatient_payments,
        coalesce(i.avg_inpatient_payment,    0) as avg_inpatient_payment,
        coalesce(i.avg_length_of_stay,       0) as avg_length_of_stay,
        coalesce(i.max_length_of_stay,       0) as max_length_of_stay,
        i.first_inpatient_date,
        i.last_inpatient_date,

        -- outpatient stats
        coalesce(o.total_outpatient_claims,   0) as total_outpatient_claims,
        coalesce(o.total_outpatient_payments, 0) as total_outpatient_payments,
        coalesce(o.avg_outpatient_payment,    0) as avg_outpatient_payment,
        o.first_outpatient_date,
        o.last_outpatient_date,

        -- combined totals
        coalesce(i.total_inpatient_claims,  0) +
        coalesce(o.total_outpatient_claims, 0)  as total_claims,

        coalesce(i.total_inpatient_payments,  0) +
        coalesce(o.total_outpatient_payments, 0) as total_claim_payments

    from beneficiaries b
    left join inpatient_summary  i on b.beneficiary_id = i.beneficiary_id
    left join outpatient_summary o on b.beneficiary_id = o.beneficiary_id
)

select * from final