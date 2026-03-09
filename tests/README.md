# Data Quality Tests

## Custom Singular Tests

| Test | Model | What It Catches |
|---|---|---|
| test_no_future_claim_dates | int_claims__combined | Claims with dates in the future |
| test_claim_payment_not_negative | int_claims__combined | Negative payment amounts |
| test_discharge_after_admission | stg_cms__inpatient_claims | Discharge before admission date |
| test_beneficiary_age_reasonable | int_claims__with_beneficiary | Age below 0 or above 125 |
| test_medicare_payment_ratio_valid | int_claims__combined | Medicare paying more than claim total |
| test_no_orphaned_claims | int_claims__combined | Claims with no matching beneficiary |

## Generic Tests
Generic `not_null`, `unique`, `accepted_values`, and `relationships` 
tests are defined in `.yml` files alongside each model.

## Notes on WARN Tests
Two tests are set to severity='warn' rather than error because they
reflect known characteristics of the CMS DE-SynPUF data structure:

- Negative payments — CMS uses negative claim amounts to represent
adjustments and reversals. This is standard Medicare claims processing
behavior, not a pipeline error.

- Payment ratio > 1.0 — In DE-SynPUF, primary_payer_paid_amount
and claim_payment_amount measure slightly different things, causing
the ratio to exceed 1.0 for some claims. This is a known quirk of
the synthetic data structure.

## Running Tests
```bash
# Run all tests
dbt test

# Run only custom singular tests
dbt test --select test_type:singular

# Run only generic tests
dbt test --select test_type:generic

# Run tests for a specific model
dbt test --select int_claims__combined
```