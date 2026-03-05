with source as (
    select * from {{ source('cms', 'inpatient_claims') }}
),

cleaned as (
    select
        -- primary key
        claim_id,
        beneficiary_id,
        segment,

        -- dates
        try_to_date(claim_from_date, 'YYYYMMDD') as claim_from_date,
        try_to_date(claim_thru_date, 'YYYYMMDD') as claim_thru_date,
        try_to_date(admission_date,  'YYYYMMDD') as admission_date,
        try_to_date(discharge_date,  'YYYYMMDD') as discharge_date,

        -- length of stay in days
        datediff(
            day,
            try_to_date(admission_date,  'YYYYMMDD'),
            try_to_date(discharge_date,  'YYYYMMDD')
        ) as length_of_stay_days,

        -- provider and physician info
        provider_id,
        attending_physician_npi,
        operating_physician_npi,
        other_physician_npi,

        -- diagnosis and DRG
        admitting_diagnosis_code,
        drg_code,

        -- financials
        try_to_number(claim_payment_amount,      18, 2) as claim_payment_amount,
        try_to_number(primary_payer_paid_amount, 18, 2) as primary_payer_paid_amount,
        try_to_number(inpatient_deductible_amount, 18, 2) as inpatient_deductible_amount,
        try_to_number(coinsurance_amount,        18, 2) as coinsurance_amount,
        try_to_number(per_diem_amount,           18, 2) as per_diem_amount,
        try_to_number(utilization_day_count)             as utilization_day_count,

        -- beneficiary out of pocket = deductible + coinsurance
        coalesce(try_to_number(inpatient_deductible_amount, 18, 2), 0) +
        coalesce(try_to_number(coinsurance_amount, 18, 2), 0)
            as beneficiary_out_of_pocket,

        -- claim type flag
        'inpatient' as claim_type

    from source
    where claim_id        is not null
      and beneficiary_id  is not null
)

select * from cleaned