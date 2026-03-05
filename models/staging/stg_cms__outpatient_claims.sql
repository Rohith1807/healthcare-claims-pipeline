with source as (
    select * from {{ source('cms', 'outpatient_claims') }}
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

        -- claim duration in days
        datediff(
            day,
            try_to_date(claim_from_date, 'YYYYMMDD'),
            try_to_date(claim_thru_date, 'YYYYMMDD')
        ) as claim_duration_days,

        -- provider and physician info
        provider_id,
        attending_physician_npi,
        operating_physician_npi,
        other_physician_npi,

        -- financials
        try_to_number(claim_payment_amount,       18, 2) as claim_payment_amount,
        try_to_number(primary_payer_paid_amount,  18, 2) as primary_payer_paid_amount,
        try_to_number(part_b_deductible_amount,   18, 2) as part_b_deductible_amount,
        try_to_number(part_b_coinsurance_amount,  18, 2) as part_b_coinsurance_amount,
        try_to_number(blood_deductible_amount,    18, 2) as blood_deductible_amount,

        -- beneficiary out of pocket
        coalesce(try_to_number(part_b_deductible_amount,  18, 2), 0) +
        coalesce(try_to_number(part_b_coinsurance_amount, 18, 2), 0) +
        coalesce(try_to_number(blood_deductible_amount,   18, 2), 0)
            as beneficiary_out_of_pocket,

        -- claim type flag
        'outpatient' as claim_type

    from source
    where claim_id       is not null
      and beneficiary_id is not null
)

select * from cleaned