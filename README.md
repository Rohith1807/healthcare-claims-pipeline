# 🏥 Healthcare Claims & Provider Analytics Pipeline

An end-to-end ELT pipeline transforming raw CMS Medicare claims into a
Power BI analytics dashboard — built using Python,
dbt Core, Snowflake, and SQL.

---

## What This Project Does

Ingests CMS DE-SynPUF synthetic Medicare data, transforms it through a
**Medallion Architecture** (Raw → Staging → Intermediate → Gold), and
delivers an interactive Power BI dashboard surfacing clinical,
financial, and operational insights across **116,352 beneficiaries**,
**2 claim types**, and **11 chronic conditions**.

---

## Architecture

```
CMS DE-SynPUF CSVs
        │
        ▼  Python (load_cms_data.py)
  RAW_DB (Snowflake)
  BENEFICIARIES · INPATIENT_CLAIMS · OUTPATIENT_CLAIMS
        │
        ▼  dbt Core — Staging
  stg_beneficiaries · stg_inpatient_claims · stg_outpatient_claims
        │
        ▼  dbt Core — Intermediate
  int_beneficiaries__enriched · int_claims__with_beneficiary · int_providers__summary
        │
        ▼  dbt Core — Gold Marts
  mart_claims__summary · mart_beneficiary__outcomes
  mart_provider__performance · mart_chronic_conditions__analysis
        │
        ▼  Power BI Desktop
  Interactive Dashboard
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python · Pandas · snowflake-connector |
| Warehouse | Snowflake |
| Transformation | dbt Core (local) |
| Data Quality | dbt Tests (not_null, unique, accepted_values) |
| Analysis | SQL (Snowflake) |
| Visualization | Power BI Desktop |
| Version Control | Git / GitHub |

---

## The 7 Phases

| Phase | What Was Built |
|---|---|
| 1 — Ingestion | Python script loads 3 CMS CSV files into Snowflake RAW_DB, renames 40+ CMS column codes to readable names, validates schema before writing |
| 2 — Staging | dbt models cast types, handle NULLs, deduplicate, standardize naming across all 3 source tables |
| 3 — Intermediate | Business logic layer — joins tables, derives age groups, computes risk tiers, calculates LOS and claim totals per beneficiary and provider |
| 4 — Gold Marts | 4 aggregated analytical marts purpose-built for BI — one per business question (claims trends, beneficiary outcomes, provider performance, condition cost impact) |
| 5 — Data Quality | dbt tests enforce integrity across all Gold models — pipeline fails loudly before bad data reaches the dashboard |
| 6 — SQL Analysis | Ad-hoc Snowflake queries validate Gold outputs and generate the key findings documented below |
| 7 — Power BI | Snowflake Gold views connected to Power BI Desktop — 35+ DAX measures, DateTable, 4-page dashboard |

<img width="1835" height="776" alt="Screenshot 2026-03-10 154116" src="https://github.com/user-attachments/assets/4417c15c-4f4a-444c-81ff-0b45767baffe" />

<img width="1895" height="975" alt="Screenshot 2026-03-10 154322" src="https://github.com/user-attachments/assets/08de6ddb-0b59-4746-9421-bde34bed8636" />


---

## Key Findings

### Chronic Condition Cost Impact
> How much more does Medicare spend on a patient WITH each condition vs WITHOUT?

| Condition | Prevalence | Cost Impact | LOS Impact |
|---|---|---|---|
| Stroke | 34.37% | **+$22,868/yr** | +3.16 days |
| Chronic Kidney | 14.48% | **+$21,534/yr** | +2.89 days |
| COPD | 11.70% | **+$18,984/yr** | +2.99 days |
| CHF | 25.32% | **+$14,891/yr** | +2.51 days |
| Cancer | 4.92% | **+$12,480/yr** | +1.87 days |
| Diabetes | 34.25% | **+$12,593/yr** | +2.07 days |

> **Cancer outlier:** Lowest prevalence (4.92%) but second highest avg cost
> with condition ($18,753) — highest cost risk per capita of all 11 conditions.

> **Ischemic Heart volume driver:** Most prevalent (36.16%) — still adds
> $10,929 per patient, making it the largest total cost driver in absolute terms.

---

### Risk Model Validation
> Does the pipeline's risk stratification produce accurate cost predictions?

| Risk Tier | Avg Payment/Claim | Avg Chronic Conditions | Mortality Rate |
|---|---|---|---|
| Very High Risk | **$3,459** | 7.03 | 1.48% |
| High Risk | **$2,941** | 3.90 | 1.06% |
| Moderate Risk | **$2,195** | 1.48 | 1.45% |
| Low Risk | **$1,817** | 0.00 | 0.00% |

> Confirmed **1.9x cost multiplier** from Low → Very High Risk. Risk tiers
> scale predictably — validating the pipeline's chronic condition burden
> scoring logic.

---

### High-Burden Population Segments
> Which demographic segments carry the highest disease and cost burden?

| Segment | Avg Chronic Conditions | Avg Payments |
|---|---|---|
| Adult (18-44) · Female · Black · Very High Risk | **8.00** | $44,600 |
| Middle-Aged (45-64) · Male · Black · Very High Risk | **7.27** | $36,158 |
| Senior (65-79) · Female · Hispanic · Very High Risk | **7.69** | $30,564 |

> Young high-risk patients (18-44) with extreme comorbidity burden represent
> the highest-ROI early intervention opportunity — costs can be intercepted
> before they escalate to the $28,000+ range seen in older cohorts.

---

### Provider Efficiency Outliers
> Which providers have LOS 50%+ above the cohort average?

Top flagged providers averaged **24–26 day LOS** vs the cohort average of
~1.6 days. High chronic condition scores (3.5–5.0 avg) and 50–65% high-risk
patient rates indicate **patient complexity** rather than operational
inefficiency — confirming the value of clinical context alongside LOS metrics.

---

## Power BI Dashboard

**Connected to Snowflake views:**

Claims Overview
<img width="1162" height="657" alt="Screenshot 2026-03-19 164643" src="https://github.com/user-attachments/assets/af5f7924-3980-4f3a-8678-c76da2934362" />

Demographics & Outcomes
<img width="1164" height="659" alt="Screenshot 2026-03-19 164737" src="https://github.com/user-attachments/assets/d4e2c35f-aeb4-45e9-b7d5-f4f5b04d771a" />

Provider Performance
<img width="1165" height="660" alt="Screenshot 2026-03-19 164757" src="https://github.com/user-attachments/assets/ef41fa84-6c3b-4f24-a01f-c1ce9b19cce1" />

Condition Cost Impact
<img width="1161" height="660" alt="Screenshot 2026-03-19 164812" src="https://github.com/user-attachments/assets/59ace4a4-cc33-421d-aa27-27c81e85a39a" />


---

## Real-World Relevance

This pipeline mirrors workflows used across the healthcare industry:

- **Medicare Advantage Plans** — Member risk scoring, actuarial model
  validation, care management prioritization
- **Hospital Systems / ACOs** — LOS outlier detection, value-based care
  program reporting, provider benchmarking
- **Health Analytics Firms** — Gold-layer claims marts powering payer
  population health dashboards (Optum, Cotiviti, Inovalon)
- **Payer-Provider Contracting** — Condition cost attribution used in
  value-based contract benchmarking between payers and hospital systems

---

## Data Source

**CMS DE-SynPUF** — Publicly available, HIPAA-safe synthetic Medicare dataset
structured identically to real CMS claims. No real patient data was used.
