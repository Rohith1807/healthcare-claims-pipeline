with claims as (
    select * from {{ ref('int_claims__combined') }}
),

inpatient_by_provider as (
    select
        provider_id,
        count(*)                        as inpatient_claim_count,
        sum(claim_payment_amount)       as inpatient_total_payments,
        avg(claim_payment_amount)       as inpatient_avg_payment,
        avg(duration_days)              as avg_length_of_stay,
        count(distinct beneficiary_id)  as inpatient_unique_patients
    from claims
    where claim_type = 'inpatient'
    group by provider_id
),

outpatient_by_provider as (
    select
        provider_id,
        count(*)                        as outpatient_claim_count,
        sum(claim_payment_amount)       as outpatient_total_payments,
        avg(claim_payment_amount)       as outpatient_avg_payment,
        count(distinct beneficiary_id)  as outpatient_unique_patients
    from claims
    where claim_type = 'outpatient'
    group by provider_id
),

combined as (
    select
        coalesce(i.provider_id, o.provider_id) as provider_id,

        -- inpatient metrics
        coalesce(i.inpatient_claim_count,     0) as inpatient_claim_count,
        coalesce(i.inpatient_total_payments,  0) as inpatient_total_payments,
        coalesce(i.inpatient_avg_payment,     0) as inpatient_avg_payment,
        coalesce(i.avg_length_of_stay,        0) as avg_length_of_stay,
        coalesce(i.inpatient_unique_patients, 0) as inpatient_unique_patients,

        -- outpatient metrics
        coalesce(o.outpatient_claim_count,     0) as outpatient_claim_count,
        coalesce(o.outpatient_total_payments,  0) as outpatient_total_payments,
        coalesce(o.outpatient_avg_payment,     0) as outpatient_avg_payment,
        coalesce(o.outpatient_unique_patients, 0) as outpatient_unique_patients,

        -- combined totals
        coalesce(i.inpatient_claim_count,  0) +
        coalesce(o.outpatient_claim_count, 0)   as total_claim_count,

        coalesce(i.inpatient_total_payments,  0) +
        coalesce(o.outpatient_total_payments, 0) as total_payments,

        -- total unique patients across both claim types
        coalesce(i.inpatient_unique_patients,  0) +
        coalesce(o.outpatient_unique_patients, 0) as total_unique_patients

    from inpatient_by_provider  i
    full outer join outpatient_by_provider o
        on i.provider_id = o.provider_id
)

select * from combined