with beneficiaries as (
    select * from {{ ref('int_beneficiaries__enriched') }}
),

-- unpivot chronic conditions into rows for easier analysis
condition_rows as (
    select beneficiary_id, 'Diabetes'        as condition_name, has_diabetes        as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'CHF'             as condition_name, has_chf             as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Cancer'          as condition_name, has_cancer          as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'COPD'            as condition_name, has_copd            as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Chronic Kidney'  as condition_name, has_chronic_kidney  as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Alzheimer'       as condition_name, has_alzheimer       as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Depression'      as condition_name, has_depression      as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Ischemic Heart'  as condition_name, has_ischemic_heart  as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Osteoporosis'    as condition_name, has_osteoporosis    as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Arthritis'       as condition_name, has_arthritis       as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
    union all
    select beneficiary_id, 'Stroke'          as condition_name, has_stroke          as has_condition, total_claim_payments, avg_length_of_stay, total_claims, age_group, gender, race from beneficiaries
),

final as (
    select
        condition_name,
        age_group,
        gender,
        race,

        -- population split
        count(*)                                            as total_beneficiaries,
        count(case when has_condition then 1 end)           as beneficiaries_with_condition,
        count(case when not has_condition then 1 end)       as beneficiaries_without_condition,

        -- prevalence rate
        round(
            count(case when has_condition then 1 end) * 100.0
            / nullif(count(*), 0), 2
        )                                                   as condition_prevalence_pct,

        -- cost comparison: with vs without condition
        avg(case when has_condition
            then total_claim_payments end)                  as avg_payments_with_condition,
        avg(case when not has_condition
            then total_claim_payments end)                  as avg_payments_without_condition,

        -- cost impact = extra cost of having the condition
        avg(case when has_condition     then total_claim_payments end) -
        avg(case when not has_condition then total_claim_payments end)
                                                            as avg_cost_impact,

        -- utilization comparison
        avg(case when has_condition
            then total_claims end)                          as avg_claims_with_condition,
        avg(case when not has_condition
            then total_claims end)                          as avg_claims_without_condition,

        -- length of stay comparison
        avg(case when has_condition
            then avg_length_of_stay end)                    as avg_los_with_condition,
        avg(case when not has_condition
            then avg_length_of_stay end)                    as avg_los_without_condition

    from condition_rows
    group by
        condition_name,
        age_group,
        gender,
        race
)

select * from final