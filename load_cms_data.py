"""
CMS DE-SynPUF Data Loader
Reads raw CMS files, reshapes them, and loads into Snowflake RAW_DB.
"""


import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ── Config ────────────────────────────────────────────────────────────────────

CMS_DIR      = r"C:\Users\rohit\OneDrive - University of St. Thomas\dbt_projects\healthcare_pipeline\data"
SNOWFLAKE_CFG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],     # same as profiles.yml
    "user":      "DBT_USER",
    "password":  os.environ["DBT_SNOWFLAKE_PASSWORD"],
    "role":      "DBT_ROLE",
    "warehouse": "DBT_WH",
    "database":  "RAW_DB",
    "schema":    "PUBLIC",
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_conn():
    return snowflake.connector.connect(**SNOWFLAKE_CFG)

def get_snowflake_columns(conn, table: str):
    """Return set of column names that exist in the Snowflake table."""
    cur = conn.cursor()
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table.upper()}' AND table_schema = 'PUBLIC'")
    return {row[0].upper() for row in cur.fetchall()}

def load_table(conn, df: pd.DataFrame, table: str):
    """Upper-case columns, keep only columns that exist in Snowflake, then write."""
    df.columns = [c.upper() for c in df.columns]
    # Only keep columns that exist in the target Snowflake table
    sf_cols = get_snowflake_columns(conn, table)
    valid_cols = [c for c in df.columns if c in sf_cols]
    df = df[valid_cols]
    success, nchunks, nrows, _ = write_pandas(conn, df, table.upper())
    if success:
        print(f"  ✅ {table}: {nrows:,} rows loaded")
    else:
        print(f"  ❌ {table}: load failed")

def find_file(pattern: str) -> str:
    """Search CMS_DIR and one level of subfolders for a file matching pattern."""
    for entry in os.listdir(CMS_DIR):
        entry_path = os.path.join(CMS_DIR, entry)
        # check top-level files first
        if os.path.isfile(entry_path) and pattern.lower() in entry.lower():
            return entry_path
        # check one level of subfolders
        if os.path.isdir(entry_path):
            for f in os.listdir(entry_path):
                if pattern.lower() in f.lower():
                    return os.path.join(entry_path, f)
    raise FileNotFoundError(f"No file matching '{pattern}' found under {CMS_DIR}")

# ── 1. Beneficiary Summary → beneficiaries table ──────────────────────────────

def load_beneficiaries(conn):
    print("\nLoading beneficiaries...")
    path = find_file("Beneficiary_Summary")
    df   = pd.read_csv(path, dtype=str)

    # Rename CMS columns → our standard names
    df = df.rename(columns={
        "DESYNPUF_ID":         "beneficiary_id",
        "BENE_BIRTH_DT":       "birth_date",
        "BENE_DEATH_DT":       "death_date",
        "BENE_SEX_IDENT_CD":   "gender_code",
        "BENE_RACE_CD":        "race_code",
        "BENE_ESRD_IND":       "esrd_indicator",
        "SP_STATE_CODE":       "state_code",
        "BENE_COUNTY_CD":      "county_code",
        "BENE_HI_CVRAGE_TOT_MONS":  "hi_coverage_months",
        "BENE_SMI_CVRAGE_TOT_MONS": "smi_coverage_months",
        "BENE_HMO_CVRAGE_TOT_MONS": "hmo_coverage_months",
        "PLAN_CVRG_MOS_NUM":   "part_d_coverage_months",
        "SP_ALZHDMTA":         "alzheimer_flag",
        "SP_CHF":              "chf_flag",
        "SP_CHRNKIDN":         "chronic_kidney_flag",
        "SP_CNCR":             "cancer_flag",
        "SP_COPD":             "copd_flag",
        "SP_DEPRESSN":         "depression_flag",
        "SP_DIABETES":         "diabetes_flag",
        "SP_ISCHMCHT":         "ischemic_heart_flag",
        "SP_OSTEOPRS":         "osteoporosis_flag",
        "SP_RA_OA":            "arthritis_flag",
        "SP_STRKETIA":         "stroke_flag",
        "MEDREIMB_IP":         "inpatient_reimbursement",
        "BENRES_IP":           "inpatient_beneficiary_resp",
        "PPPYMT_IP":           "inpatient_payer_payment",
        "MEDREIMB_OP":         "outpatient_reimbursement",
        "BENRES_OP":           "outpatient_beneficiary_resp",
        "PPPYMT_OP":           "outpatient_payer_payment",
        "MEDREIMB_CAR":        "carrier_reimbursement",
        "BENRES_CAR":          "carrier_beneficiary_resp",
        "PPPYMT_CAR":          "carrier_payer_payment",
    })

    # Keep only renamed columns (drop any unmapped ones)
    df = df[[c for c in df.columns if c == c.lower()]]
    load_table(conn, df, "BENEFICIARIES")

# ── 2. Inpatient Claims → inpatient_claims table ──────────────────────────────

def load_inpatient(conn):
    print("\nLoading inpatient claims...")
    path = find_file("Inpatient_Claims")
    df   = pd.read_csv(path, dtype=str)

    df = df.rename(columns={
        "DESYNPUF_ID":          "beneficiary_id",
        "CLM_ID":               "claim_id",
        "SEGMENT":              "segment",
        "CLM_FROM_DT":          "claim_from_date",
        "CLM_THRU_DT":          "claim_thru_date",
        "PRVDR_NUM":            "provider_id",
        "CLM_PMT_AMT":          "claim_payment_amount",
        "NCH_PRMRY_PYR_CLM_PD_AMT": "primary_payer_paid_amount",
        "AT_PHYSN_NPI":         "attending_physician_npi",
        "OP_PHYSN_NPI":         "operating_physician_npi",
        "OT_PHYSN_NPI":         "other_physician_npi",
        "CLM_ADMSN_DT":         "admission_date",
        "ADMTNG_ICD9_DGNS_CD":  "admitting_diagnosis_code",
        "CLM_PASS_THRU_PER_DIEM_AMT": "per_diem_amount",
        "NCH_BENE_IP_DDCTBL_AMT":     "inpatient_deductible_amount",
        "NCH_BENE_PTA_COINSRNC_LBLTY_AM": "coinsurance_amount",
        "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM": "blood_deductible_amount",
        "CLM_UTLZTN_DAY_CNT":   "utilization_day_count",
        "NCH_BENE_DSCHRG_DT":   "discharge_date",
        "CLM_DRG_CD":           "drg_code",
    })

    # Grab diagnosis columns dynamically
    diag_cols = [c for c in df.columns if "ICD9_DGNS" in c.upper()]
    proc_cols = [c for c in df.columns if "ICD9_PRCDR" in c.upper()]

    keep = [
        "beneficiary_id","claim_id","segment","claim_from_date","claim_thru_date",
        "provider_id","claim_payment_amount","primary_payer_paid_amount",
        "attending_physician_npi","operating_physician_npi","other_physician_npi",
        "admission_date","admitting_diagnosis_code","per_diem_amount",
        "inpatient_deductible_amount","coinsurance_amount","blood_deductible_amount",
        "utilization_day_count","discharge_date","drg_code",
    ] + diag_cols + proc_cols

    df = df[[c for c in keep if c in df.columns]]
    load_table(conn, df, "INPATIENT_CLAIMS")

# ── 3. Outpatient Claims → outpatient_claims table ────────────────────────────

def load_outpatient(conn):
    print("\nLoading outpatient claims...")
    path = find_file("Outpatient_Claims")
    df   = pd.read_csv(path, dtype=str)

    df = df.rename(columns={
        "DESYNPUF_ID":          "beneficiary_id",
        "CLM_ID":               "claim_id",
        "SEGMENT":              "segment",
        "CLM_FROM_DT":          "claim_from_date",
        "CLM_THRU_DT":          "claim_thru_date",
        "PRVDR_NUM":            "provider_id",
        "CLM_PMT_AMT":          "claim_payment_amount",
        "NCH_PRMRY_PYR_CLM_PD_AMT": "primary_payer_paid_amount",
        "AT_PHYSN_NPI":         "attending_physician_npi",
        "OP_PHYSN_NPI":         "operating_physician_npi",
        "OT_PHYSN_NPI":         "other_physician_npi",
        "NCH_BENE_BLOOD_DDCTBL_LBLTY_AM": "blood_deductible_amount",
        "NCH_BENE_PTB_DDCTBL_AMT":        "part_b_deductible_amount",
        "NCH_BENE_PTB_COINSRNC_AMT":      "part_b_coinsurance_amount",
    })

    diag_cols = [c for c in df.columns if "ICD9_DGNS" in c.upper()]
    proc_cols = [c for c in df.columns if "ICD9_PRCDR" in c.upper() or "HCPCS" in c.upper()]

    keep = [
        "beneficiary_id","claim_id","segment","claim_from_date","claim_thru_date",
        "provider_id","claim_payment_amount","primary_payer_paid_amount",
        "attending_physician_npi","operating_physician_npi","other_physician_npi",
        "blood_deductible_amount","part_b_deductible_amount","part_b_coinsurance_amount",
    ] + diag_cols + proc_cols

    df = df[[c for c in keep if c in df.columns]]
    load_table(conn, df, "OUTPATIENT_CLAIMS")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Connecting to Snowflake...")
    conn = get_conn()
    print("✅ Connected\n")

    load_beneficiaries(conn)
    load_inpatient(conn)
    load_outpatient(conn)

    conn.close()
    print("\n🎉 All CMS tables loaded into RAW_DB successfully!")
    print("\nRun this in Snowflake to verify:")
    print("  SELECT 'beneficiaries'    AS tbl, COUNT(*) FROM RAW_DB.PUBLIC.BENEFICIARIES   UNION ALL")
    print("  SELECT 'inpatient_claims' AS tbl, COUNT(*) FROM RAW_DB.PUBLIC.INPATIENT_CLAIMS UNION ALL")
    print("  SELECT 'outpatient_claims'AS tbl, COUNT(*) FROM RAW_DB.PUBLIC.OUTPATIENT_CLAIMS;")

if __name__ == "__main__":
    main()