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

# -----------------------------
# Helpers
# -----------------------------
def safe_div(n, d, default=np.nan):
    n = n.astype(float)
    d = d.astype(float)
    out = np.where(d != 0, n / d, default)
    return out

def norm_text(x):
    if pd.isna(x): 
        return ""
    s = str(x).strip().lower()
    # keep letters/numbers/_ and spaces
    s = re.sub(r"[^a-z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cost_set(raw):
    s = norm_text(raw)

    # Handle common Cobra labels + drift using substring logic
    # Order matters (more specific first)
    if "acwp" in s and ("hrs" in s or "hour" in s):
        return "ACT_HRS"
    if ("acwp" in s and ("wkl" in s or "week" in s)) or ("weekly actual" in s):
        return "WEEKLY_ACTUALS"
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

    return f"OTHER::{s}" if s else "OTHER::(blank)"

def norm_currency(raw):
    s = norm_text(raw).upper()

    # If currency is blank, we keep "UNK" so you can inspect
    if s in {"", "UNK", "UNKNOWN"}:
        return "UNK"

    # Hours indicators
    if any(k in s for k in ["HRS", "HOURS", "HR"]):
        return "HRS"

    # Dollars indicators (common variants)
    if any(k in s for k in ["USD", "DOLLAR", "$", "DOL", "COST", "AMT"]):
        return "USD"

    return s  # pass-through

# -----------------------------
# Start from cobra_df
# -----------------------------
df = cobra_df.copy()

# Required columns check
required = {"source", "SUB_TEAM", "COST-SET", "DATE", "HOURS", "Currency"}
missing = required - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

# Parse date + numeric value
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["VALUE"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0.0)

# Normalize cost set + currency
df["cost_set_norm"] = df["COST-SET"].apply(norm_cost_set)
df["currency_norm"] = df["Currency"].apply(norm_currency)

# Snapshot date per source (max DATE in the file)
snapshot_by_source = (
    df.groupby("source", as_index=False)
      .agg(snapshot_date=("DATE", "max"))
)

# LSD close date per source = last close date <= snapshot date
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
# CTD: Use all rows (cumulative snapshot extract)
# -----------------------------
KEEP_KEYS = {"BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"}
df_ctd = df[df["cost_set_norm"].isin(KEEP_KEYS)].copy()

ctd = (
    df_ctd
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
    if col not in ctd.columns:
        ctd[col] = 0.0

# Compute metrics per currency bucket (USD vs HRS)
ctd["SPI_CTD"] = safe_div(ctd["BCWP"], ctd["BCWS"])
ctd["CPI_CTD"] = safe_div(ctd["BCWP"], ctd["ACWP"])
ctd["BEI_CTD"] = safe_div(ctd["BCWP"], ctd["BAC"])
ctd["VAC_CTD"] = ctd["BAC"] - ctd["EAC"]

# Add flags so missing values are explainable instead of mysterious
ctd["SPI_CTD_reason"] = np.where(ctd["BCWS"] == 0, "BCWS=0", "")
ctd["CPI_CTD_reason"] = np.where(ctd["ACWP"] == 0, "ACWP=0", "")

# -----------------------------
# LSD: Filter rows by accounting window (DATE in (prior_close, lsd_close])
# Only works if DATE is time-phased in the extract; otherwise LSD must be CTD-delta across snapshots.
# -----------------------------
mask_lsd = (
    df["DATE"].notna() &
    df["lsd_close_date"].notna() &
    (df["DATE"] <= df["lsd_close_date"]) &
    (df["prior_close_date"].isna() | (df["DATE"] > df["prior_close_date"]))
)

df_lsd = df[mask_lsd & df["cost_set_norm"].isin(KEEP_KEYS)].copy()

lsd = (
    df_lsd
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

lsd["SPI_LSD"] = safe_div(lsd["BCWP"], lsd["BCWS"])
lsd["CPI_LSD"] = safe_div(lsd["BCWP"], lsd["ACWP"])
lsd["SPI_LSD_reason"] = np.where(lsd["BCWS"] == 0, "BCWS=0", "")
lsd["CPI_LSD_reason"] = np.where(lsd["ACWP"] == 0, "ACWP=0", "")

# -----------------------------
# OUTPUT TABLES (what you asked for)
# -----------------------------
# 1) By source: SPI/CPI/BEI CTD + SPI/CPI LSD (USD only by default)
source_evms_metrics = (
    ctd[ctd["currency_norm"].isin(["USD","UNK"])]
    .groupby("source", as_index=False)
    .agg(BAC=("BAC","sum"), BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"), EAC=("EAC","sum"))
)
source_evms_metrics["SPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BCWS"])
source_evms_metrics["CPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["ACWP"])
source_evms_metrics["BEI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BAC"])
source_evms_metrics["VAC_CTD"] = source_evms_metrics["BAC"] - source_evms_metrics["EAC"]

source_lsd_metrics = (
    lsd[lsd["currency_norm"].isin(["USD","UNK"])]
    .groupby("source", as_index=False)
    .agg(BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"))
)
source_lsd_metrics["SPI_LSD"] = safe_div(source_lsd_metrics["BCWP"], source_lsd_metrics["BCWS"])
source_lsd_metrics["CPI_LSD"] = safe_div(source_lsd_metrics["BCWP"], source_lsd_metrics["ACWP"])

source_evms_metrics = source_evms_metrics.merge(
    source_lsd_metrics[["source","SPI_LSD","CPI_LSD"]],
    on="source",
    how="left"
)

# 2) By source + subteam: SPI/CPI CTD + LSD + BAC/EAC/VAC (USD only)
subteam_metrics = (
    ctd[ctd["currency_norm"].isin(["USD","UNK"])]
    [["source","SUB_TEAM","SPI_CTD","CPI_CTD","BEI_CTD","BAC","EAC","VAC_CTD","SPI_CTD_reason","CPI_CTD_reason"]]
    .merge(
        lsd[lsd["currency_norm"].isin(["USD","UNK"])]
        [["source","SUB_TEAM","SPI_LSD","CPI_LSD","SPI_LSD_reason","CPI_LSD_reason"]],
        on=["source","SUB_TEAM"],
        how="left"
    )
)

# 3) Cost table
subteam_cost = subteam_metrics[["source","SUB_TEAM","BAC","EAC","VAC_CTD"]].copy()

# 4) Hours table (HRS currency)
hours_tbl = (
    lsd[lsd["currency_norm"].eq("HRS")]
    [["source","SUB_TEAM","BCWS","ACT_HRS","ETC"]]
    .rename(columns={"BCWS":"Demand_Hours","ACT_HRS":"Actual_Hours","ETC":"Next_Month_ETC_Hours"})
)
hours_tbl["Pct_Var"] = safe_div((hours_tbl["Actual_Hours"] - hours_tbl["Demand_Hours"]), hours_tbl["Demand_Hours"], default=np.nan)
hours_tbl["Next_Mo_BCWS_Hours"] = np.nan  # needs explicit "next month" logic from your extract

# -----------------------------
# Diagnostics: show what didn't map + why NaNs exist
# -----------------------------
unmapped = (
    df[df["cost_set_norm"].str.startswith("OTHER::", na=False)]
    ["COST-SET"]
    .value_counts()
    .head(25)
)

print("✅ Tables created: source_evms_metrics, subteam_metrics, subteam_cost, hours_tbl")
print("\n--- TOP UNMAPPED COST-SET VALUES (fix mapping if needed) ---")
print(unmapped if len(unmapped) else "None 🎉")

print("\n--- Why SPI/CPI are missing (top reasons) ---")
print(subteam_metrics[["SPI_CTD_reason","CPI_CTD_reason","SPI_LSD_reason","CPI_LSD_reason"]].replace("", np.nan).stack().value_counts().head(10))