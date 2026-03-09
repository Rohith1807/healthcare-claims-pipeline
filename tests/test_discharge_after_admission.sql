-- Fails if discharge date is before admission date
-- Indicates a date parsing or source data error
select
    claim_id,
    admission_date,
    discharge_date,
    length_of_stay_days
from {{ ref('stg_cms__inpatient_claims') }}
where discharge_date < admission_date