import pandas as pd
import numpy as np
import re

# ============================================================
# 0) CONFIG — 2026 Accounting Period Close Dates
# ============================================================
ACCT_CLOSE_DATES = pd.to_datetime([
    "2026-01-04","2026-02-01","2026-03-01","2026-04-05","2026-05-03","2026-06-07",
    "2026-07-05","2026-08-02","2026-09-06","2026-10-04","2026-11-01","2026-12-06"
]).sort_values()

# ============================================================
# 1) Helpers
# ============================================================
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

    # Hours variants
    if "acwp" in s and ("hrs" in s or "hour" in s):
        return "ACT_HRS"
    if ("weekly actual" in s) or ("acwp" in s and ("wkl" in s or "week" in s)):
        return "WEEKLY_ACTUALS"

    # Core EVMS
    if "bcws" in s or "planned value" in s:
        return "BCWS"
    if "bcwp" in s or "earned value" in s or "progress" in s:
        return "BCWP"
    if "acwp" in s or "actual cost" in s:
        return "ACWP"
    if "bac" in s or "budget" in s:
        return "BAC"
    if "eac" in s:
        return "EAC"
    if "etc" in s:
        return "ETC"

    return None

def choose_value_column(df):
    """
    Your exports sometimes store dollars/hours in different numeric columns.
    This picks the best numeric value column by coverage + magnitude on core cost sets.
    """
    candidates = [c for c in df.columns if c.upper() in {"HOURS","AMOUNT","VALUE","DOLLARS","COST","TOTAL"}]
    # also consider any numeric columns besides obvious IDs
    numeric_candidates = []
    for c in df.columns:
        if c in {"DATE"}: 
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            numeric_candidates.append(c)
    candidates = list(dict.fromkeys(candidates + numeric_candidates))  # unique preserve order

    if not candidates:
        raise ValueError("Could not find a numeric value column (e.g., HOURS/AMOUNT/VALUE).")

    core = {"BCWS","BCWP","ACWP","BAC","EAC","ETC"}
    scores = []
    for c in candidates:
        x = pd.to_numeric(df[c], errors="coerce")
        # score based on non-null count on core cost sets and total abs magnitude
        mask = df["cost_set_norm"].isin(core)
        score = (x[mask].notna().sum()) * 10 + (x[mask].abs().sum(skipna=True))
        scores.append((score, c))
    scores.sort(reverse=True)
    return scores[0][1]

def asof_value(group, target_date, value_col):
    """
    group: rows for one (source, SUB_TEAM, cost_set_norm)
    Returns SUM(value_col) at the latest DATE <= target_date.
    If DATE is missing, falls back to SUM across all rows.
    """
    if group["DATE"].notna().any() and pd.notna(target_date):
        g = group[group["DATE"].notna()].sort_values("DATE")
        g = g[g["DATE"] <= target_date]
        if g.empty:
            return 0.0
        last_date = g["DATE"].max()
        return pd.to_numeric(g.loc[g["DATE"] == last_date, value_col], errors="coerce").fillna(0.0).sum()
    else:
        return pd.to_numeric(group[value_col], errors="coerce").fillna(0.0).sum()

def last_close(d):
    if pd.isna(d): 
        return pd.NaT
    closes = ACCT_CLOSE_DATES[ACCT_CLOSE_DATES <= d]
    return closes.max() if len(closes) else pd.NaT

def prior_close(cur):
    if pd.isna(cur): 
        return pd.NaT
    idx = np.where(ACCT_CLOSE_DATES.values == np.datetime64(cur))[0]
    if len(idx) == 0:
        return pd.NaT
    i = int(idx[0])
    return ACCT_CLOSE_DATES[i-1] if i > 0 else pd.NaT

# ============================================================
# 2) Normalize + auto-detect value column
# ============================================================
df = cobra_df.copy()

# Required columns
need = {"source","SUB_TEAM","COST-SET","DATE"}
missing = need - set(df.columns)
if missing:
    raise ValueError(f"Missing columns: {missing}")

df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["cost_set_norm"] = df["COST-SET"].apply(norm_cost_set)
df = df[df["cost_set_norm"].notna()].copy()

value_col = choose_value_column(df)
df[value_col] = pd.to_numeric(df[value_col], errors="coerce").fillna(0.0)

print(f"✅ Using value column: {value_col}")

# ============================================================
# 3) Build snapshot + LSD close dates per source
# ============================================================
snap = df.groupby("source", as_index=False).agg(snapshot_date=("DATE","max"))
snap["lsd_close_date"] = snap["snapshot_date"].apply(last_close)
snap["prior_close_date"] = snap["lsd_close_date"].apply(prior_close)

df = df.merge(snap, on="source", how="left")

# ============================================================
# 4) Compute CTD and LSD using AS-OF logic (works across file types)
# ============================================================
# We'll compute "as-of" values for each (source, SUB_TEAM, cost_set_norm)
keys = ["source","SUB_TEAM","cost_set_norm"]

grouped = df.groupby(keys, as_index=False)

records = []
for (source, sub, cs), g in df.groupby(keys):
    snapshot_date = g["snapshot_date"].iloc[0]
    lsd_close_date = g["lsd_close_date"].iloc[0]
    prior_close_date = g["prior_close_date"].iloc[0]

    ctd_val = asof_value(g, snapshot_date, value_col)
    ctd_close = asof_value(g, lsd_close_date, value_col)
    ctd_prior = asof_value(g, prior_close_date, value_col)
    lsd_val = ctd_close - ctd_prior

    records.append({
        "source": source,
        "SUB_TEAM": sub,
        "cost_set_norm": cs,
        "snapshot_date": snapshot_date,
        "lsd_close_date": lsd_close_date,
        "prior_close_date": prior_close_date,
        "CTD": ctd_val,
        "LSD": lsd_val
    })

fact = pd.DataFrame(records)

# Pivot to wide format
ctd_wide = fact.pivot_table(index=["source","SUB_TEAM"], columns="cost_set_norm", values="CTD", aggfunc="sum", fill_value=0.0).reset_index()
lsd_wide = fact.pivot_table(index=["source","SUB_TEAM"], columns="cost_set_norm", values="LSD", aggfunc="sum", fill_value=0.0).reset_index()

# Ensure columns exist
for col in ["BAC","BCWS","BCWP","ACWP","EAC","ETC","ACT_HRS","WEEKLY_ACTUALS"]:
    if col not in ctd_wide.columns: ctd_wide[col] = 0.0
    if col not in lsd_wide.columns: lsd_wide[col] = 0.0

# ============================================================
# 5) Metrics (CTD + LSD)
# ============================================================
ctd_wide["SPI_CTD"] = safe_div(ctd_wide["BCWP"], ctd_wide["BCWS"])
ctd_wide["CPI_CTD"] = safe_div(ctd_wide["BCWP"], ctd_wide["ACWP"])
ctd_wide["BEI_CTD"] = safe_div(ctd_wide["BCWP"], ctd_wide["BAC"])
ctd_wide["VAC_CTD"] = ctd_wide["BAC"] - ctd_wide["EAC"]

lsd_wide["SPI_LSD"] = safe_div(lsd_wide["BCWP"], lsd_wide["BCWS"])
lsd_wide["CPI_LSD"] = safe_div(lsd_wide["BCWP"], lsd_wide["ACWP"])

# ============================================================
# 6) Output tables requested
# ============================================================
# A) Source-level
source_evms_metrics = (
    ctd_wide.groupby("source", as_index=False)
            .agg(BAC=("BAC","sum"), BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"), EAC=("EAC","sum"))
)
source_evms_metrics["SPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BCWS"])
source_evms_metrics["CPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["ACWP"])
source_evms_metrics["BEI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BAC"])
source_evms_metrics["VAC_CTD"] = source_evms_metrics["BAC"] - source_evms_metrics["EAC"]

source_lsd = (
    lsd_wide.groupby("source", as_index=False)
            .agg(BCWS=("BCWS","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP","sum"))
)
source_lsd["SPI_LSD"] = safe_div(source_lsd["BCWP"], source_lsd["BCWS"])
source_lsd["CPI_LSD"] = safe_div(source_lsd["BCWP"], source_lsd["ACWP"])

source_evms_metrics = source_evms_metrics.merge(source_lsd[["source","SPI_LSD","CPI_LSD"]], on="source", how="left")

# B) Subteam metrics
subteam_metrics = (
    ctd_wide[["source","SUB_TEAM","SPI_CTD","CPI_CTD","BEI_CTD","BAC","EAC","VAC_CTD"]]
    .merge(lsd_wide[["source","SUB_TEAM","SPI_LSD","CPI_LSD"]], on=["source","SUB_TEAM"], how="left")
)

# C) Subteam cost
subteam_cost = subteam_metrics[["source","SUB_TEAM","BAC","EAC","VAC_CTD"]].copy()

# D) Hours / demand table (best-effort from what exists)
# If BCWS in your file is $ not hours, you’ll replace Demand_Hours logic later with the real “demand hours” field.
hours_tbl = (
    lsd_wide[["source","SUB_TEAM","BCWS","ACT_HRS","ETC"]]
    .rename(columns={"BCWS":"Demand_Hours_proxy","ACT_HRS":"Actual_Hours","ETC":"Next_Month_ETC_proxy"})
)
hours_tbl["Pct_Var"] = safe_div((hours_tbl["Actual_Hours"] - hours_tbl["Demand_Hours_proxy"]), hours_tbl["Demand_Hours_proxy"])
hours_tbl["Next_Mo_BCWS_Hours"] = np.nan  # needs explicit next-month rule from your extract

# ============================================================
# 7) Diagnostics — prove what’s truly missing
# ============================================================
diag = (
    ctd_wide.assign(
        missing_BCWS=lambda d: d["BCWS"].eq(0),
        missing_ACWP=lambda d: d["ACWP"].eq(0),
        missing_BCWP=lambda d: d["BCWP"].eq(0),
    )
    .groupby("source", as_index=False)
    .agg(
        rows=("SUB_TEAM","count"),
        pct_BCWS_zero=("missing_BCWS","mean"),
        pct_ACWP_zero=("missing_ACWP","mean"),
        pct_BCWP_zero=("missing_BCWP","mean")
    )
)

print("\n✅ Created: source_evms_metrics, subteam_metrics, subteam_cost, hours_tbl")
print("\n--- Coverage diagnostics (share this if still missing) ---")
print(diag.sort_values(["pct_BCWS_zero","pct_ACWP_zero"], ascending=False).head(20))