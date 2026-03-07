with beneficiaries as (
    select * from {{ ref('int_beneficiaries__enriched') }}
),

final as (
    select
        -- segment dimensions
        age_group,
        gender,
        race,
        state_code,
        risk_tier,

        -- population counts
        count(*)                                        as total_beneficiaries,
        count(case when is_deceased then 1 end)         as deceased_count,
        count(case when total_claims > 0 then 1 end)    as beneficiaries_with_claims,

        -- mortality rate
        round(
            count(case when is_deceased then 1 end) * 100.0
            / nullif(count(*), 0), 2
        )                                               as mortality_rate_pct,

        -- chronic condition prevalence
        count(case when has_diabetes        then 1 end) as diabetes_count,
        count(case when has_chf             then 1 end) as chf_count,
        count(case when has_cancer          then 1 end) as cancer_count,
        count(case when has_copd            then 1 end) as copd_count,
        count(case when has_chronic_kidney  then 1 end) as chronic_kidney_count,
        count(case when has_alzheimer       then 1 end) as alzheimer_count,
        count(case when has_depression      then 1 end) as depression_count,
        count(case when has_ischemic_heart  then 1 end) as ischemic_heart_count,
        count(case when has_osteoporosis    then 1 end) as osteoporosis_count,
        count(case when has_arthritis       then 1 end) as arthritis_count,
        count(case when has_stroke          then 1 end) as stroke_count,

        -- condition prevalence rates
        round(count(case when has_diabetes       then 1 end) * 100.0 / nullif(count(*), 0), 2) as diabetes_rate_pct,
        round(count(case when has_chf            then 1 end) * 100.0 / nullif(count(*), 0), 2) as chf_rate_pct,
        round(count(case when has_cancer         then 1 end) * 100.0 / nullif(count(*), 0), 2) as cancer_rate_pct,
        round(count(case when has_copd           then 1 end) * 100.0 / nullif(count(*), 0), 2) as copd_rate_pct,
        round(count(case when has_chronic_kidney then 1 end) * 100.0 / nullif(count(*), 0), 2) as chronic_kidney_rate_pct,

        -- average chronic conditions per beneficiary
        avg(total_chronic_conditions)                   as avg_chronic_conditions,
        max(total_chronic_conditions)                   as max_chronic_conditions,

        -- utilization metrics
        avg(total_claims)                               as avg_claims_per_beneficiary,
        avg(total_inpatient_claims)                     as avg_inpatient_claims,
        avg(total_outpatient_claims)                    as avg_outpatient_claims,
        sum(total_claims)                               as total_claims,

        -- financial metrics
        sum(total_claim_payments)                       as total_payments,
        avg(total_claim_payments)                       as avg_payments_per_beneficiary,
        avg(avg_length_of_stay)                         as avg_length_of_stay,
        max(max_length_of_stay)                         as max_length_of_stay

    from beneficiaries
    group by
        age_group,
        gender,
        race,
        state_code,
        risk_tier
)

select * from final