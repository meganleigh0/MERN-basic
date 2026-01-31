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
    n = n.astype(float); d = d.astype(float)
    return np.where(d != 0, n / d, np.nan)

def norm_text(x):
    if pd.isna(x): return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^a-z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_cost_set(raw):
    s = norm_text(raw)

    # NOTE: "Budget" is often PV (BCWS) in these extracts
    if "budget" in s:
        return "BUDGET_PV"
    if "progress" in s or "earned value" in s or "bcwp" in s:
        return "BCWP"
    if "planned value" in s or "bcws" in s:
        return "BCWS"

    # Actuals can appear multiple ways
    if ("weekly actual" in s) or ("acwp" in s and ("wkl" in s or "week" in s)):
        return "ACWP_FALLBACK"
    if "acwp" in s or "actual cost" in s:
        return "ACWP"

    # Forecast buckets
    if "eac" in s:
        return "EAC"
    if "etc" in s:
        return "ETC"

    # Hours actuals (keep separate)
    if "acwp" in s and ("hrs" in s or "hour" in s):
        return "ACT_HRS"

    return None

def last_close(d):
    if pd.isna(d): return pd.NaT
    closes = ACCT_CLOSE_DATES[ACCT_CLOSE_DATES <= d]
    return closes.max() if len(closes) else pd.NaT

def prior_close(cur):
    if pd.isna(cur): return pd.NaT
    idx = np.where(ACCT_CLOSE_DATES.values == np.datetime64(cur))[0]
    if len(idx) == 0: return pd.NaT
    i = int(idx[0])
    return ACCT_CLOSE_DATES[i-1] if i > 0 else pd.NaT

def asof_sum(g, target_date, value_col):
    # sum at latest DATE <= target_date; if no DATE, sum all
    if g["DATE"].notna().any() and pd.notna(target_date):
        gg = g[g["DATE"].notna()].sort_values("DATE")
        gg = gg[gg["DATE"] <= target_date]
        if gg.empty:
            return 0.0
        last_d = gg["DATE"].max()
        return gg.loc[gg["DATE"] == last_d, value_col].sum()
    return g[value_col].sum()

# ============================================================
# 2) Build df + robust VALUE (coalesce numeric columns row-wise)
# ============================================================
df = cobra_df.copy()

need = {"source","SUB_TEAM","COST-SET","DATE"}
missing = need - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["cost_set_norm"] = df["COST-SET"].apply(norm_cost_set)
df = df[df["cost_set_norm"].notna()].copy()

# Candidate numeric columns (different file types store values differently)
exclude = {"DATE"}
cands = []
for c in df.columns:
    if c in exclude: 
        continue
    if pd.api.types.is_numeric_dtype(df[c]):
        cands.append(c)
# also include common named numeric columns even if imported as object
for c in ["HOURS","AMOUNT","VALUE","DOLLARS","COST","TOTAL"]:
    if c in df.columns and c not in cands:
        cands.append(c)

if not cands:
    raise ValueError("No numeric columns found to compute VALUE.")

# Coalesce numeric columns row-wise: take the max absolute numeric as the VALUE
# (works when some columns are blank for certain cost sets)
cand_numeric = []
for c in cands:
    cand_numeric.append(pd.to_numeric(df[c], errors="coerce"))
vals = pd.concat(cand_numeric, axis=1)
df["VALUE"] = vals.fillna(0.0).abs().max(axis=1) * np.sign(vals.fillna(0.0).sum(axis=1).replace(0, 1))
df["VALUE"] = df["VALUE"].fillna(0.0)

# ============================================================
# 3) Snapshot + accounting close dates per source
# ============================================================
snap = df.groupby("source", as_index=False).agg(snapshot_date=("DATE","max"))
snap["lsd_close_date"] = snap["snapshot_date"].apply(last_close)
snap["prior_close_date"] = snap["lsd_close_date"].apply(prior_close)
df = df.merge(snap, on="source", how="left")

# ============================================================
# 4) Compute CTD as-of snapshot; LSD via close deltas
# ============================================================
records = []
for (source, sub, cs), g in df.groupby(["source","SUB_TEAM","cost_set_norm"]):
    snapshot_date = g["snapshot_date"].iloc[0]
    close_date = g["lsd_close_date"].iloc[0]
    prior_date = g["prior_close_date"].iloc[0]

    ctd = asof_sum(g, snapshot_date, "VALUE")
    ctd_close = asof_sum(g, close_date, "VALUE")
    ctd_prior = asof_sum(g, prior_date, "VALUE")
    lsd = ctd_close - ctd_prior

    records.append({
        "source": source, "SUB_TEAM": sub, "metric": cs,
        "CTD": ctd, "LSD": lsd
    })

fact = pd.DataFrame(records)

ctd = fact.pivot_table(index=["source","SUB_TEAM"], columns="metric", values="CTD", aggfunc="sum", fill_value=0.0).reset_index()
lsd = fact.pivot_table(index=["source","SUB_TEAM"], columns="metric", values="LSD", aggfunc="sum", fill_value=0.0).reset_index()

# Ensure expected columns exist
for col in ["BCWS","BCWP","ACWP","ACWP_FALLBACK","BUDGET_PV","EAC","ETC","ACT_HRS"]:
    if col not in ctd.columns: ctd[col] = 0.0
    if col not in lsd.columns: lsd[col] = 0.0

# ============================================================
# 5) Reconciliation logic per file type
#    - BCWS = BCWS if present else BUDGET_PV
#    - ACWP = ACWP if present else ACWP_FALLBACK
#    - BAC  = total baseline = SUM(BUDGET_PV across all dates)  (use CTD(BUDGET_PV_total))
# ============================================================
# BAC derivation: total baseline budget = sum of BUDGET_PV over all dates (NOT as-of)
bac_total = (
    df[df["cost_set_norm"].eq("BUDGET_PV")]
    .groupby(["source","SUB_TEAM"], as_index=False)["VALUE"]
    .sum()
    .rename(columns={"VALUE":"BAC"})
)

ctd = ctd.merge(bac_total, on=["source","SUB_TEAM"], how="left")
ctd["BAC"] = ctd["BAC"].fillna(0.0)

ctd["BCWS_eff"] = np.where(ctd["BCWS"] != 0, ctd["BCWS"], ctd["BUDGET_PV"])
ctd["ACWP_eff"] = np.where(ctd["ACWP"] != 0, ctd["ACWP"], ctd["ACWP_FALLBACK"])

lsd["BCWS_eff"] = np.where(lsd["BCWS"] != 0, lsd["BCWS"], lsd["BUDGET_PV"])
lsd["ACWP_eff"] = np.where(lsd["ACWP"] != 0, lsd["ACWP"], lsd["ACWP_FALLBACK"])

# ============================================================
# 6) EVMS metrics
# ============================================================
ctd["SPI_CTD"] = safe_div(ctd["BCWP"], ctd["BCWS_eff"])
ctd["CPI_CTD"] = safe_div(ctd["BCWP"], ctd["ACWP_eff"])
ctd["BEI_CTD"] = safe_div(ctd["BCWP"], ctd["BAC"])
ctd["VAC_CTD"] = ctd["BAC"] - ctd.get("EAC", 0.0)

lsd["SPI_LSD"] = safe_div(lsd["BCWP"], lsd["BCWS_eff"])
lsd["CPI_LSD"] = safe_div(lsd["BCWP"], lsd["ACWP_eff"])

# ============================================================
# 7) Output tables
# ============================================================
source_evms_metrics = (
    ctd.groupby("source", as_index=False)
       .agg(BAC=("BAC","sum"),
            BCWS=("BCWS_eff","sum"),
            BCWP=("BCWP","sum"),
            ACWP=("ACWP_eff","sum"),
            EAC=("EAC","sum"))
)
source_evms_metrics["SPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BCWS"])
source_evms_metrics["CPI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["ACWP"])
source_evms_metrics["BEI_CTD"] = safe_div(source_evms_metrics["BCWP"], source_evms_metrics["BAC"])
source_evms_metrics["VAC_CTD"] = source_evms_metrics["BAC"] - source_evms_metrics["EAC"]

source_lsd = (
    lsd.groupby("source", as_index=False)
       .agg(BCWS=("BCWS_eff","sum"), BCWP=("BCWP","sum"), ACWP=("ACWP_eff","sum"))
)
source_lsd["SPI_LSD"] = safe_div(source_lsd["BCWP"], source_lsd["BCWS"])
source_lsd["CPI_LSD"] = safe_div(source_lsd["BCWP"], source_lsd["ACWP"])

source_evms_metrics = source_evms_metrics.merge(source_lsd[["source","SPI_LSD","CPI_LSD"]], on="source", how="left")

subteam_metrics = (
    ctd[["source","SUB_TEAM","SPI_CTD","CPI_CTD","BEI_CTD","BAC","EAC","VAC_CTD"]]
    .merge(lsd[["source","SUB_TEAM","SPI_LSD","CPI_LSD"]], on=["source","SUB_TEAM"], how="left")
)

subteam_cost = subteam_metrics[["source","SUB_TEAM","BAC","EAC","VAC_CTD"]].copy()

hours_tbl = (
    lsd[["source","SUB_TEAM","BCWS_eff","ACT_HRS","ETC"]]
    .rename(columns={"BCWS_eff":"Demand_proxy","ACT_HRS":"Actual_Hours","ETC":"Next_Month_ETC_proxy"})
)
hours_tbl["Pct_Var"] = safe_div((hours_tbl["Actual_Hours"] - hours_tbl["Demand_proxy"]), hours_tbl["Demand_proxy"])
hours_tbl["Next_Mo_BCWS_Hours"] = np.nan

# ============================================================
# 8) Diagnostics: prove which files use which labels
# ============================================================
label_audit = (
    df.groupby(["source","cost_set_norm"], as_index=False)
      .size()
      .pivot_table(index="source", columns="cost_set_norm", values="size", fill_value=0)
      .reset_index()
)

diag = (
    ctd.assign(
        bcws_zero=lambda d: (d["BCWS_eff"] == 0).astype(float),
        acwp_zero=lambda d: (d["ACWP_eff"] == 0).astype(float),
        bcwp_zero=lambda d: (d["BCWP"] == 0).astype(float)
    )
    .groupby("source", as_index=False)
    .agg(rows=("SUB_TEAM","count"),
         pct_BCWS_zero=("bcws_zero","mean"),
         pct_ACWP_zero=("acwp_zero","mean"),
         pct_BCWP_zero=("bcwp_zero","mean"))
)

print("✅ Created: source_evms_metrics, subteam_metrics, subteam_cost, hours_tbl")
print("\n--- Coverage diagnostics (top 15) ---")
print(diag.sort_values(["pct_BCWS_zero","pct_ACWP_zero"], ascending=False).head(15))
print("\n--- Label audit (counts by source; check Budget/Weekly Actuals usage) ---")
print(label_audit.head(15))