import pandas as pd
import numpy as np

# ============================================================
# CONFIG — YOU EDIT THIS LIST (Accounting Period Close Dates)
# ============================================================
# LSD date = latest close date <= snapshot_date
# Put *all* close dates for the FY here (datetime strings ok).
ACCT_CLOSE_DATES = [
    # "2026-01-04", "2026-02-01", "2026-03-01", ...
]

# ============================================================
# 1) BASIC CLEANUP + NORMALIZATION
# ============================================================
df = cobra_df.copy()

# Ensure DATE is datetime (your column list shows 'DATE' exists)
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

# Snapshot date per source: use max DATE in that file's data
snapshot_by_source = (
    df.groupby("source", as_index=False)
      .agg(snapshot_date=("DATE", "max"))
)

# Normalize COST-SET terms (language drift / naming drift)
def norm_cost_set(s):
    if pd.isna(s):
        return None
    x = str(s).strip().lower()

    # common “drift” patterns you showed + typical Cobra variations
    if x in {"budget", "bac"}:
        return "BAC"
    if x in {"progress", "earned value", "bcwp"}:
        return "BCWP"
    if x in {"bcws", "planned value"}:
        return "BCWS"
    if x in {"acwp", "actual cost"}:
        return "ACWP"
    if x in {"eac"}:
        return "EAC"
    if x in {"etc"}:
        return "ETC"

    # Hours / weekly-ish variants (we do NOT call these LSD automatically)
    if x in {"acwp_hrs", "actual hours", "hours"}:
        return "ACT_HRS"
    if x in {"weekly actuals", "acwp_wkl", "weekly acwp"}:
        return "WEEKLY_ACTUALS"  # keep as a separate bucket (not "LSD" by itself)

    # if unknown, keep raw so we can inspect later
    return f"OTHER::{x}"

df["cost_set_norm"] = df["COST-SET"].apply(norm_cost_set)

# Value column: your sheet has HOURS; keep numeric
VALUE_COL = "HOURS" if "HOURS" in df.columns else None
if VALUE_COL is None:
    raise ValueError("Expected a numeric column like 'HOURS' but did not find it.")
df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors="coerce").fillna(0.0)

# ============================================================
# 2) BUILD ACCOUNTING CLOSE TABLE + LSD WINDOW PER SOURCE
# ============================================================
acct = pd.DataFrame({"close_date": pd.to_datetime(ACCT_CLOSE_DATES, errors="coerce")}).dropna()
acct = acct.sort_values("close_date").reset_index(drop=True)

if acct.empty:
    raise ValueError("ACCT_CLOSE_DATES is empty or not parseable. Paste the accounting close dates first.")

# For each source, find LSD close date = max close_date <= snapshot_date
snapshot_by_source["lsd_close_date"] = snapshot_by_source["snapshot_date"].apply(
    lambda d: acct.loc[acct["close_date"] <= d, "close_date"].max() if pd.notna(d) else pd.NaT
)

# Prior close date (start of LSD window)
def prior_close_date(cur_close):
    if pd.isna(cur_close):
        return pd.NaT
    idx = acct.index[acct["close_date"] == cur_close]
    if len(idx) == 0:
        return pd.NaT
    i = int(idx[0])
    return acct.loc[i-1, "close_date"] if i > 0 else pd.NaT

snapshot_by_source["prior_close_date"] = snapshot_by_source["lsd_close_date"].apply(prior_close_date)

# Join LSD window info back to rows
df = df.merge(snapshot_by_source, on="source", how="left")

# ============================================================
# 3) CTD FACT (Cumulative as-of snapshot)
# ============================================================
# We treat BAC/BCWS/BCWP/ACWP/EAC/ETC as cumulative (CTD-like) cost sets in the extract.
CTD_KEYS = {"BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"}

df_ctd = df[df["cost_set_norm"].isin(CTD_KEYS)].copy()

# Pivot to get numeric columns per (source, SUB_TEAM)
ctd = (
    df_ctd
    .pivot_table(index=["source","SUB_TEAM"], columns="cost_set_norm", values=VALUE_COL, aggfunc="sum", fill_value=0.0)
    .reset_index()
)

# Ensure columns exist
for col in ["BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"]:
    if col not in ctd.columns:
        ctd[col] = 0.0

# CTD metrics
ctd["SPI_CTD"] = np.where(ctd["BCWS"] != 0, ctd["BCWP"] / ctd["BCWS"], np.nan)
ctd["CPI_CTD"] = np.where(ctd["ACWP"] != 0, ctd["BCWP"] / ctd["ACWP"], np.nan)
ctd["BEI_CTD"] = np.where(ctd["BAC"]  != 0, ctd["BCWP"] / ctd["BAC"],  np.nan)
ctd["VAC_CTD"] = ctd["BAC"] - ctd["EAC"]

# ============================================================
# 4) LSD FACT (Incremental between prior close and close)
# ============================================================
# If your sheet is time-phased by DATE, we can compute LSD by restricting rows to that window.
# Window is (prior_close_date, lsd_close_date] for each source.
# If your extract is NOT time-phased, we’ll instead compute LSD as a CTD delta at close dates once we have close-date snapshots.
mask_lsd_window = (
    df["DATE"].notna() &
    df["lsd_close_date"].notna() &
    (df["DATE"] <= df["lsd_close_date"]) &
    (
        df["prior_close_date"].isna() |
        (df["DATE"] > df["prior_close_date"])
    )
)

df_lsd = df[mask_lsd_window & df["cost_set_norm"].isin(CTD_KEYS)].copy()

lsd = (
    df_lsd
    .pivot_table(index=["source","SUB_TEAM"], columns="cost_set_norm", values=VALUE_COL, aggfunc="sum", fill_value=0.0)
    .reset_index()
)

for col in ["BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"]:
    if col not in lsd.columns:
        lsd[col] = 0.0

# LSD metrics (incremental EV on incremental PV/AC, etc.)
lsd["SPI_LSD"] = np.where(lsd["BCWS"] != 0, lsd["BCWP"] / lsd["BCWS"], np.nan)
lsd["CPI_LSD"] = np.where(lsd["ACWP"] != 0, lsd["BCWP"] / lsd["ACWP"], np.nan)

# ============================================================
# 5) BUILD REQUESTED OUTPUT TABLES
# ============================================================

# A) By source CTD & LSD: SPI, CPI, BEI
source_ctd = (
    ctd.groupby("source", as_index=False)
       .agg(BAC=("BAC","sum"), BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"))
)
source_ctd["SPI_CTD"] = np.where(source_ctd["BCWS"] != 0, source_ctd["BCWP"]/source_ctd["BCWS"], np.nan)
source_ctd["CPI_CTD"] = np.where(source_ctd["ACWP"] != 0, source_ctd["BCWP"]/source_ctd["ACWP"], np.nan)
source_ctd["BEI_CTD"] = np.where(source_ctd["BAC"]  != 0, source_ctd["BCWP"]/source_ctd["BAC"],  np.nan)

source_lsd = (
    lsd.groupby("source", as_index=False)
       .agg(BAC=("BAC","sum"), BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"))
)
source_lsd["SPI_LSD"] = np.where(source_lsd["BCWS"] != 0, source_lsd["BCWP"]/source_lsd["BCWS"], np.nan)
source_lsd["CPI_LSD"] = np.where(source_lsd["ACWP"] != 0, source_lsd["BCWP"]/source_lsd["ACWP"], np.nan)
# BEI is usually CTD (BCWP/BAC). LSD BEI is not typically used; keep CTD only.

source_evms_metrics = (
    source_ctd.merge(source_lsd[["source","SPI_LSD","CPI_LSD"]], on="source", how="left")
)

# B) By source + sub team: SPI/CPI CTD & LSD
subteam_metrics = (
    ctd[["source","SUB_TEAM","SPI_CTD","CPI_CTD","BAC","EAC","VAC_CTD"]]
    .merge(lsd[["source","SUB_TEAM","SPI_LSD","CPI_LSD"]], on=["source","SUB_TEAM"], how="left")
)

# C) By source + sub team: BAC, EAC, VAC (already in subteam_metrics)
subteam_cost = subteam_metrics[["source","SUB_TEAM","BAC","EAC","VAC_CTD"]].copy()

# D) Demand Hours, Actual Hours, % Var, Next Mo BCWS Hours, Next Month ETC Hours
# If you have explicit next-month columns later, swap them in here.
hours_tbl = (
    lsd[["source","SUB_TEAM","BCWS","ACT_HRS","ETC"]]
    .rename(columns={
        "BCWS": "Demand_Hours",
        "ACT_HRS": "Actual_Hours",
        "ETC": "Next_Month_ETC_Hours"  # placeholder: ETC bucket in-window
    })
)

hours_tbl["Pct_Var"] = np.where(hours_tbl["Demand_Hours"] != 0,
                                (hours_tbl["Actual_Hours"] - hours_tbl["Demand_Hours"]) / hours_tbl["Demand_Hours"],
                                np.nan)

# Next Month BCWS Hours:
# If your extract has a way to identify "next month" rows (e.g., DATE in next month),
# you can compute it. For now, leave as NaN placeholder.
hours_tbl["Next_Mo_BCWS_Hours"] = np.nan

# ============================================================
# PRINT QUICK VALIDATION
# ============================================================
print("✅ Created tables:")
print(" - source_evms_metrics (SPI/CPI/BEI CTD + SPI/CPI LSD)")
print(" - subteam_metrics (SPI/CPI CTD+LSD + BAC/EAC/VAC)")
print(" - subteam_cost (BAC/EAC/VAC)")
print(" - hours_tbl (Demand/Actual/%Var/NextMo placeholders)")
print("\nLSD dates by source (sample):")
print(snapshot_by_source.head(10))