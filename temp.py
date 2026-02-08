import pandas as pd
import numpy as np

# =========================
# GOAL: df_program_evms with ZERO missing values
# - No NaNs in BCWS/BCWP/ACWP/ETC CTD or YTD
# - No NaNs in SPI/CPI (use 0 when denominator is 0)
# - Correctly filter to latest closed status date (<= latest_close_date)
# =========================

df = cobra_merged_df.copy()

# --- types
df["DATE"]  = pd.to_datetime(df["DATE"], errors="coerce")
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0)

for c in ["PROGRAM", "COST-SET"]:
    df[c] = df[c].astype("string").fillna("").str.strip()

# --- enforce canonical COST-SET (extra safety; assumes already normalized)
df["COST-SET"] = df["COST-SET"].astype("string").str.strip().str.upper()

# --- latest close date (status cutoff)
latest_close_date = df["DATE"].max()

# --- fiscal year start (adjust if needed)
fy_start = pd.Timestamp(year=latest_close_date.year, month=1, day=1)

# --- filter to "closed" status periods (CTD/YTD should only use <= close date)
df_status = df[df["DATE"].notna() & (df["DATE"] <= latest_close_date)].copy()

# --- helper: wide pivot that ALWAYS returns all 4 cost sets and fills with 0
def costset_wide(d, idx_cols):
    w = (
        d.pivot_table(index=idx_cols, columns="COST-SET", values="HOURS", aggfunc="sum")
         .reindex(columns=["BCWS","BCWP","ACWP","ETC"])   # force full schema
         .fillna(0)
         .reset_index()
    )
    return w

# --- safe ratios with NO NaNs (0 if denom is 0)
def ratio(num, den):
    num = num.astype(float)
    den = den.astype(float)
    out = np.zeros(len(num), dtype=float)
    m = den != 0
    out[m] = num[m] / den[m]
    return out

# =========================
# Build CTD + YTD then merge (inner to avoid mismatched program sets)
# =========================
ctd = costset_wide(df_status, ["PROGRAM"]).rename(
    columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD","ETC":"ETC_CTD"}
)

ytd = costset_wide(df_status[df_status["DATE"] >= fy_start], ["PROGRAM"]).rename(
    columns={"BCWS":"BCWS_YTD","BCWP":"BCWP_YTD","ACWP":"ACWP_YTD","ETC":"ETC_YTD"}
)

# Use OUTER but fill all numeric to 0 so nothing goes missing
df_program_evms = ctd.merge(ytd, on="PROGRAM", how="outer")

# Fill any missing numeric values to 0 (this eliminates the “Missing value” UI flags)
num_cols = [c for c in df_program_evms.columns if c != "PROGRAM"]
df_program_evms[num_cols] = df_program_evms[num_cols].fillna(0)

# =========================
# Compute SPI/CPI (CTD & YTD) with 0 instead of NaN
# =========================
df_program_evms["SPI_CTD"] = ratio(df_program_evms["BCWP_CTD"], df_program_evms["BCWS_CTD"])
df_program_evms["CPI_CTD"] = ratio(df_program_evms["BCWP_CTD"], df_program_evms["ACWP_CTD"])
df_program_evms["SPI_YTD"] = ratio(df_program_evms["BCWP_YTD"], df_program_evms["BCWS_YTD"])
df_program_evms["CPI_YTD"] = ratio(df_program_evms["BCWP_YTD"], df_program_evms["ACWP_YTD"])

# Final ordering
df_program_evms = df_program_evms[
    ["PROGRAM",
     "BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CTD","SPI_CTD","CPI_CTD",
     "BCWS_YTD","BCWP_YTD","ACWP_YTD","ETC_YTD","SPI_YTD","CPI_YTD"]
].sort_values("PROGRAM").reset_index(drop=True)

# =========================
# Hard validation: assert no missing values anywhere
# =========================
if df_program_evms.isna().any().any():
    bad = df_program_evms.columns[df_program_evms.isna().any()].tolist()
    raise ValueError(f"Still has missing values in: {bad}")

print("Latest Close Date Used:", latest_close_date.date())
print("FY Start Used:", fy_start.date())
display(df_program_evms)