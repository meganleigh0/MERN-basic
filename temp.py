import pandas as pd
import numpy as np
import re

# -----------------------------
# CONFIG: 2026 accounting close dates
# -----------------------------
ACCT_CLOSE_DATES = pd.to_datetime([
    "2026-01-04","2026-02-01","2026-03-01","2026-04-05","2026-05-03","2026-06-07",
    "2026-07-05","2026-08-02","2026-09-06","2026-10-04","2026-11-01","2026-12-06"
])

def safe_div(n, d):
    n = n.astype(float)
    d = d.astype(float)
    return np.where(d != 0, n / d, np.nan)

def norm_text(x):
    if pd.isna(x): 
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^a-z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cost_set(raw):
    s = norm_text(raw)

    # specific first
    if "acwp" in s and ("hrs" in s or "hour" in s):
        return "ACT_HRS"
    if ("acwp" in s and ("wkl" in s or "week" in s)) or ("weekly actual" in s):
        return "WEEKLY_ACTUALS"

    # core EVMS
    if "acwp" in s or "actual cost" in s:
        return "ACWP"
    if "bcws" in s or "planned value" in s:
        return "BCWS"
    if "bcwp" in s or "earned value" in s or "progress" in s:
        return "BCWP"
    if "bac" in s or "budget" in s:
        return "BAC"
    if "eac" in s:
        return "EAC"
    if "etc" in s:
        return "ETC"

    return None  # ignore unknowns (you already confirmed there are none)

# -----------------------------
# Start
# -----------------------------
df = cobra_df.copy()

# Parse date + numeric value
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["VALUE"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0.0)

# Normalize cost set
df["cost_set_norm"] = df["COST-SET"].apply(norm_cost_set)
df = df[df["cost_set_norm"].notna()].copy()

# ✅ CRITICAL FIX:
# Force currency based on cost set (do NOT trust 'Currency' column for EVMS)
COST_SETS_USD = {"BAC","BCWS","BCWP","ACWP","EAC","ETC"}
COST_SETS_HRS = {"ACT_HRS","WEEKLY_ACTUALS"}

df["currency_norm"] = np.where(df["cost_set_norm"].isin(COST_SETS_USD), "USD",
                        np.where(df["cost_set_norm"].isin(COST_SETS_HRS), "HRS", "UNK"))

# Snapshot date per source
snapshot_by_source = (
    df.groupby("source", as_index=False)
      .agg(snapshot_date=("DATE", "max"))
)

ACCT_CLOSE_DATES = pd.Series(ACCT_CLOSE_DATES).sort_values().reset_index(drop=True)

def last_close(d):
    if pd.isna(d):
        return pd.NaT
    closes = ACCT_CLOSE_DATES[ACCT_CLOSE_DATES <= d]
    return closes.max() if len(closes) else pd.NaT

snapshot_by_source["lsd_close_date"] = snapshot_by_source["snapshot_date"].apply(last_close)

def prior_close(cur):
    if pd.isna(cur):
        return pd.NaT
    idx = np.where(ACCT_CLOSE_DATES.values == np.datetime64(cur))[0]
    if len(idx) == 0:
        return pd.NaT
    i = int(idx[0])
    return ACCT_CLOSE_DATES.iloc[i-1] if i > 0 else pd.NaT

snapshot_by_source["prior_close_date"] = snapshot_by_source["lsd_close_date"].apply(prior_close)

df = df.merge(snapshot_by_source, on="source", how="left")

# -----------------------------
# CTD Pivot (USD + HRS separately)
# -----------------------------
ctd = (
    df.pivot_table(
        index=["source","SUB_TEAM","currency_norm"],
        columns="cost_set_norm",
        values="VALUE",
        aggfunc="sum",
        fill_value=0.0
    )
    .reset_index()
)

for col in ["BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"]:
    if col not in ctd.columns:
        ctd[col] = 0.0

# CTD metrics for USD only
ctd_usd = ctd[ctd["currency_norm"].eq("USD")].copy()
ctd_usd["SPI_CTD"] = safe_div(ctd_usd["BCWP"], ctd_usd["BCWS"])
ctd_usd["CPI_CTD"] = safe_div(ctd_usd["BCWP"], ctd_usd["ACWP"])
ctd_usd["BEI_CTD"] = safe_div(ctd_usd["BCWP"], ctd_usd["BAC"])
ctd_usd["VAC_CTD"] = ctd_usd["BAC"] - ctd_usd["EAC"]

# -----------------------------
# LSD window + Pivot
# -----------------------------
mask_lsd = (
    df["DATE"].notna() &
    df["lsd_close_date"].notna() &
    (df["DATE"] <= df["lsd_close_date"]) &
    (df["prior_close_date"].isna() | (df["DATE"] > df["prior_close_date"]))
)

lsd = (
    df[mask_lsd]
    .pivot_table(
        index=["source","SUB_TEAM","currency_norm"],
        columns="cost_set_norm",
        values="VALUE",
        aggfunc="sum",
        fill_value=0.0
    )
    .reset_index()
)

for col in ["BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"]:
    if col not in lsd.columns:
        lsd[col] = 0.0

lsd_usd = lsd[lsd["currency_norm"].eq("USD")].copy()
lsd_usd["SPI_LSD"] = safe_div(lsd_usd["BCWP"], lsd_usd["BCWS"])
lsd_usd["CPI_LSD"] = safe_div(lsd_usd["BCWP"], lsd_usd["ACWP"])

# -----------------------------
# OUTPUT TABLES
# -----------------------------
source_evms_metrics = (
    ctd_usd.groupby("source", as_index=False)
           .agg(BAC=("BAC","sum"), BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"), EAC=("EAC","sum"))
)
source_evms_metrics["SPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BCWS"])
source_evms_metrics["CPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["ACWP"])
source_evms_metrics["BEI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BAC"])
source_evms_metrics["VAC_CTD"] = source_evms_metrics["BAC"] - source_evms_metrics["EAC"]

source_lsd_metrics = (
    lsd_usd.groupby("source", as_index=False)
           .agg(BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"))
)
source_lsd_metrics["SPI_LSD"] = safe_div(source_lsd_metrics["BCWP"], source_lsd_metrics["BCWS"])
source_lsd_metrics["CPI_LSD"] = safe_div(source_lsd_metrics["BCWP"], source_lsd_metrics["ACWP"])

source_evms_metrics = source_evms_metrics.merge(
    source_lsd_metrics[["source","SPI_LSD","CPI_LSD"]],
    on="source",
    how="left"
)

subteam_metrics = (
    ctd_usd[["source","SUB_TEAM","SPI_CTD","CPI_CTD","BEI_CTD","BAC","EAC","VAC_CTD"]]
    .merge(lsd_usd[["source","SUB_TEAM","SPI_LSD","CPI_LSD"]], on=["source","SUB_TEAM"], how="left")
)

subteam_cost = subteam_metrics[["source","SUB_TEAM","BAC","EAC","VAC_CTD"]].copy()

hours_tbl = (
    lsd[lsd["currency_norm"].eq("HRS")]
    [["source","SUB_TEAM","BCWS","ACT_HRS","ETC"]]
    .rename(columns={"BCWS":"Demand_Hours","ACT_HRS":"Actual_Hours","ETC":"Next_Month_ETC_Hours"})
)
hours_tbl["Pct_Var"] = safe_div((hours_tbl["Actual_Hours"] - hours_tbl["Demand_Hours"]), hours_tbl["Demand_Hours"])
hours_tbl["Next_Mo_BCWS_Hours"] = np.nan  # requires explicit next-month rule

# -----------------------------
# Quick validation: where are zeros coming from now?
# -----------------------------
print("✅ Tables created: source_evms_metrics, subteam_metrics, subteam_cost, hours_tbl")

missing_spi = (subteam_metrics["SPI_CTD"].isna()).sum()
missing_cpi = (subteam_metrics["CPI_CTD"].isna()).sum()
print(f"\nMissing SPI_CTD rows: {missing_spi}")
print(f"Missing CPI_CTD rows: {missing_cpi}")

print("\nTop causes (USD):")
tmp = ctd_usd.copy()
tmp["SPI_missing_reason"] = np.where(tmp["BCWS"]==0, "BCWS=0", "")
tmp["CPI_missing_reason"] = np.where(tmp["ACWP"]==0, "ACWP=0", "")
print(tmp[["SPI_missing_reason","CPI_missing_reason"]].replace("", np.nan).stack().value_counts().head(10))