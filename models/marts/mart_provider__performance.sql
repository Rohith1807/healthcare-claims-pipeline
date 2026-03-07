with providers as (
    select * from {{ ref('int_providers__summary') }}
),

claims as (
    select * from {{ ref('int_claims__with_beneficiary') }}
),

-- get per-provider patient mix details from claims
provider_patient_mix as (
    select
        provider_id,
        count(distinct case when age_group = 'Senior (65-79)'   then beneficiary_id end) as senior_patients,
        count(distinct case when age_group = 'Elderly (80+)'    then beneficiary_id end) as elderly_patients,
        count(distinct case when risk_tier = 'High Risk'        then beneficiary_id end) as high_risk_patients,
        count(distinct case when risk_tier = 'Very High Risk'   then beneficiary_id end) as very_high_risk_patients,
        count(distinct case when gender = 'Female'              then beneficiary_id end) as female_patients,
        count(distinct case when gender = 'Male'                then beneficiary_id end) as male_patients,
        avg(age_at_claim)                                                                as avg_patient_age,
        avg(total_chronic_conditions)                                                    as avg_patient_chronic_conditions,
        count(distinct claim_year)                                                       as years_active
    from claims
    group by provider_id
),

final as (
    select
        p.provider_id,

        -- volume metrics
        p.total_claim_count,
        p.inpatient_claim_count,
        p.outpatient_claim_count,
        p.total_unique_patients,
        p.inpatient_unique_patients,
        p.outpatient_unique_patients,

        -- payment metrics
        p.total_payments,
        p.inpatient_total_payments,
        p.outpatient_total_payments,
        p.inpatient_avg_payment,
        p.outpatient_avg_payment,

        -- efficiency metrics
        round(p.total_payments / nullif(p.total_unique_patients, 0), 2)
                                                        as revenue_per_patient,
        round(p.total_claim_count / nullif(p.total_unique_patients, 0), 2)
                                                        as claims_per_patient,
        p.avg_length_of_stay,

        -- patient mix
        m.avg_patient_age,
        m.avg_patient_chronic_conditions,
        m.senior_patients,
        m.elderly_patients,
        m.high_risk_patients,
        m.very_high_risk_patients,
        m.female_patients,
        m.male_patients,
        m.years_active,

        -- high risk patient rate
        round(
            (m.high_risk_patients + m.very_high_risk_patients) * 100.0
            / nullif(p.total_unique_patients, 0), 2
        )                                               as high_risk_patient_rate_pct,

        -- provider size tier
        case
            when p.total_claim_count >= 10000 then 'Large'
            when p.total_claim_count between 1000 and 9999 then 'Medium'
            when p.total_claim_count between 100 and 999 then 'Small'
            else 'Micro'
        end                                             as provider_size_tier

    from providers p
    left join provider_patient_mix m
        on p.provider_id = m.provider_id
)

select * from final