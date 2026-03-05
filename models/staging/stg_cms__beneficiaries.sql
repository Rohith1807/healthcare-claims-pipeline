with source as (
    select * from {{ source('cms', 'beneficiaries') }}
),

-- DE-SynPUF has one row per beneficiary per year (2008, 2009, 2010)
-- We keep the most recent year per beneficiary to ensure uniqueness
deduped as (
    select *,
        row_number() over (
            partition by beneficiary_id
            order by birth_date desc nulls last
        ) as rn
    from source
    where beneficiary_id is not null
),

cleaned as (
    select * from deduped where rn = 1
),

final as (
    select
        -- primary key
        beneficiary_id,

        -- dates: CMS stores as YYYYMMDD string → cast to proper DATE
        try_to_date(birth_date,  'YYYYMMDD') as birth_date,
        try_to_date(death_date,  'YYYYMMDD') as death_date,

        -- demographics
        case gender_code
            when '1' then 'Male'
            when '2' then 'Female'
            else 'Unknown'
        end as gender,

        case race_code
            when '1' then 'White'
            when '2' then 'Black'
            when '3' then 'Other'
            when '4' then 'Asian'
            when '5' then 'Hispanic'
            when '6' then 'North American Native'
            else 'Unknown'
        end as race,

        state_code,
        county_code,
        esrd_indicator,

        -- coverage months
        try_to_number(hi_coverage_months)      as hi_coverage_months,
        try_to_number(smi_coverage_months)     as smi_coverage_months,
        try_to_number(hmo_coverage_months)     as hmo_coverage_months,
        try_to_number(part_d_coverage_months)  as part_d_coverage_months,

        -- chronic condition flags (1 = Yes, 2 = No)
        case when alzheimer_flag    = '1' then true else false end as has_alzheimer,
        case when chf_flag          = '1' then true else false end as has_chf,
        case when chronic_kidney_flag = '1' then true else false end as has_chronic_kidney,
        case when cancer_flag       = '1' then true else false end as has_cancer,
        case when copd_flag         = '1' then true else false end as has_copd,
        case when depression_flag   = '1' then true else false end as has_depression,
        case when diabetes_flag     = '1' then true else false end as has_diabetes,
        case when ischemic_heart_flag = '1' then true else false end as has_ischemic_heart,
        case when osteoporosis_flag = '1' then true else false end as has_osteoporosis,
        case when arthritis_flag    = '1' then true else false end as has_arthritis,
        case when stroke_flag       = '1' then true else false end as has_stroke,

        -- calculate total chronic conditions
        (
            case when alzheimer_flag    = '1' then 1 else 0 end +
            case when chf_flag          = '1' then 1 else 0 end +
            case when chronic_kidney_flag = '1' then 1 else 0 end +
            case when cancer_flag       = '1' then 1 else 0 end +
            case when copd_flag         = '1' then 1 else 0 end +
            case when depression_flag   = '1' then 1 else 0 end +
            case when diabetes_flag     = '1' then 1 else 0 end +
            case when ischemic_heart_flag = '1' then 1 else 0 end +
            case when osteoporosis_flag = '1' then 1 else 0 end +
            case when arthritis_flag    = '1' then 1 else 0 end +
            case when stroke_flag       = '1' then 1 else 0 end
        ) as total_chronic_conditions,

        -- is the beneficiary deceased?
        case when death_date is not null then true else false end as is_deceased,

        -- reimbursement amounts
        try_to_number(inpatient_reimbursement,  18, 2) as inpatient_reimbursement,
        try_to_number(outpatient_reimbursement, 18, 2) as outpatient_reimbursement,
        try_to_number(carrier_reimbursement,    18, 2) as carrier_reimbursement,

        -- total reimbursement across all claim types
        coalesce(try_to_number(inpatient_reimbursement,  18, 2), 0) +
        coalesce(try_to_number(outpatient_reimbursement, 18, 2), 0) +
        coalesce(try_to_number(carrier_reimbursement,    18, 2), 0)
            as total_reimbursement

    from cleaned
)

select * from final