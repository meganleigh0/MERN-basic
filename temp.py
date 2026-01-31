import pandas as pd
import numpy as np
import re

# -----------------------------
# Accounting close dates
# -----------------------------
ACCT_CLOSE_DATES = pd.to_datetime([
    "2026-01-04","2026-02-01","2026-03-01","2026-04-05","2026-05-03","2026-06-07",
    "2026-07-05","2026-08-02","2026-09-06","2026-10-04","2026-11-01","2026-12-06"
]).sort_values()

def safe_div(n, d):
    n = n.astype(float); d = d.astype(float)
    return np.where(d != 0, n / d, np.nan)

def norm_text(x):
    if pd.isna(x): return ""
    s = str(x).strip().lower()
    s = re.sub(r"[^a-z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# -----------------------------
# COST-SET normalization
# -----------------------------
def norm_cost_set(raw):
    s = norm_text(raw)

    # Explicit BAC exists in some exports
    if s == "bac" or " budget at completion" in s:
        return "BAC"

    # Budget line (often time-phased baseline plan)
    if "budget" in s:
        return "BUDGET"

    # Planned/Earned/Actual
    if "bcws" in s or "planned value" in s:
        return "BCWS"
    if "bcwp" in s or "earned value" in s or "progress" in s:
        return "BCWP"
    if ("weekly actual" in s) or ("acwp" in s and ("wkl" in s or "week" in s)):
        return "ACWP_FALLBACK"
    if "acwp" in s or "actual cost" in s:
        return "ACWP"

    # Forecast
    if "eac" in s:
        return "EAC"
    if "etc" in s:
        return "ETC"

    # Hours
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
            return np.nan
        last_d = gg["DATE"].max()
        return pd.to_numeric(gg.loc[gg["DATE"] == last_d, value_col], errors="coerce").fillna(0.0).sum()
    return pd.to_numeric(g[value_col], errors="coerce").fillna(0.0).sum()

# -----------------------------
# Build working df
# -----------------------------
df = cobra_df.copy()

req = {"source","SUB_TEAM","COST-SET","DATE"}
missing = req - set(df.columns)
if missing:
    raise ValueError(f"Missing required columns: {missing}")

df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["metric"] = df["COST-SET"].apply(norm_cost_set)
df = df[df["metric"].notna()].copy()

# Pick value column(s) by NAME (avoid numeric ID columns)
preferred = [c for c in df.columns if re.search(r"(hours|amount|value|dollars|cost)", str(c), re.I)]
preferred = [c for c in preferred if c.upper() not in {"CHG#", "CHG", "C CODE"}]

if not preferred:
    # fallback: HOURS if present
    preferred = [c for c in ["HOURS","AMOUNT","VALUE"] if c in df.columns]
if not preferred:
    raise ValueError("No usable value columns found (expected HOURS/AMOUNT/VALUE-like).")

# Use first preferred column as primary numeric value
value_col = preferred[0]
df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

# Some file types have values in a different column: coalesce across all preferred value columns
vals = pd.concat([pd.to_numeric(df[c], errors="coerce") for c in preferred], axis=1)
df["VALUE"] = vals.bfill(axis=1).iloc[:,0].fillna(0.0)

print(f"✅ Using VALUE from columns (coalesced): {preferred}")

# -----------------------------
# Snapshot / close dates per source
# -----------------------------
snap = df.groupby("source", as_index=False).agg(snapshot_date=("DATE","max"))
snap["lsd_close_date"] = snap["snapshot_date"].apply(last_close)
snap["prior_close_date"] = snap["lsd_close_date"].apply(prior_close)
df = df.merge(snap, on="source", how="left")

# -----------------------------
# Fact table (CTD as-of snapshot; LSD = close delta)
# -----------------------------
records = []
for (source, sub, m), g in df.groupby(["source","SUB_TEAM","metric"]):
    snapshot_date = g["snapshot_date"].iloc[0]
    close_date = g["lsd_close_date"].iloc[0]
    prior_date = g["prior_close_date"].iloc[0]

    ctd = asof_sum(g, snapshot_date, "VALUE")
    ctd_close = asof_sum(g, close_date, "VALUE")
    ctd_prior = asof_sum(g, prior_date, "VALUE")
    lsd = (ctd_close - ctd_prior) if (pd.notna(ctd_close) and pd.notna(ctd_prior)) else np.nan

    records.append({"source":source,"SUB_TEAM":sub,"metric":m,"CTD":ctd,"LSD":lsd})

fact = pd.DataFrame(records)

ctd = fact.pivot_table(index=["source","SUB_TEAM"], columns="metric", values="CTD", aggfunc="sum", fill_value=np.nan).reset_index()
lsd = fact.pivot_table(index=["source","SUB_TEAM"], columns="metric", values="LSD", aggfunc="sum", fill_value=np.nan).reset_index()

# Ensure columns exist (as NaN, not 0)
for col in ["BAC","BUDGET","BCWS","BCWP","ACWP","ACWP_FALLBACK","EAC","ETC","ACT_HRS"]:
    if col not in ctd.columns: ctd[col] = np.nan
    if col not in lsd.columns: lsd[col] = np.nan

# -----------------------------
# BAC logic (robust + transparent)
# -----------------------------
# BAC candidate 1: explicit BAC (as-of snapshot)
bac_explicit = ctd["BAC"]

# BAC candidate 2: if BUDGET is time-phased baseline, BAC = SUM of BUDGET across ALL dates (not as-of)
bac_from_budget = (
    df[df["metric"].eq("BUDGET")]
    .groupby(["source","SUB_TEAM"], as_index=False)["VALUE"]
    .sum()
    .rename(columns={"VALUE":"BAC_from_budget"})
)

ctd = ctd.merge(bac_from_budget, on=["source","SUB_TEAM"], how="left")

# Select BAC with provenance
ctd["BAC_eff"] = np.where(ctd["BAC"].notna() & (ctd["BAC"] != 0), ctd["BAC"],
                  np.where(ctd["BAC_from_budget"].notna() & (ctd["BAC_from_budget"] != 0), ctd["BAC_from_budget"], np.nan))

ctd["BAC_source"] = np.where(ctd["BAC"].notna() & (ctd["BAC"] != 0), "explicit_BAC",
                      np.where(ctd["BAC_from_budget"].notna() & (ctd["BAC_from_budget"] != 0), "sum(BUDGET)", "missing"))

# -----------------------------
# ACWP / BCWS fallbacks (still valid)
# -----------------------------
ctd["BCWS_eff"] = np.where(ctd["BCWS"].notna() & (ctd["BCWS"] != 0), ctd["BCWS"],
                    np.where(ctd["BUDGET"].notna() & (ctd["BUDGET"] != 0), ctd["BUDGET"], np.nan))
ctd["ACWP_eff"] = np.where(ctd["ACWP"].notna() & (ctd["ACWP"] != 0), ctd["ACWP"],
                    np.where(ctd["ACWP_FALLBACK"].notna() & (ctd["ACWP_FALLBACK"] != 0), ctd["ACWP_FALLBACK"], np.nan))

lsd["BCWS_eff"] = np.where(lsd["BCWS"].notna() & (lsd["BCWS"] != 0), lsd["BCWS"],
                    np.where(lsd["BUDGET"].notna() & (lsd["BUDGET"] != 0), lsd["BUDGET"], np.nan))
lsd["ACWP_eff"] = np.where(lsd["ACWP"].notna() & (lsd["ACWP"] != 0), lsd["ACWP"],
                    np.where(lsd["ACWP_FALLBACK"].notna() & (lsd["ACWP_FALLBACK"] != 0), lsd["ACWP_FALLBACK"], np.nan))

# -----------------------------
# EAC logic (transparent)
# -----------------------------
# If EAC missing, we leave NaN by default.
# Optional: compute a formula EAC = BAC_eff / CPI_CTD (ONLY if you want).
ctd["EAC_eff"] = np.where(ctd["EAC"].notna() & (ctd["EAC"] != 0), ctd["EAC"], np.nan)
ctd["EAC_source"] = np.where(ctd["EAC"].notna() & (ctd["EAC"] != 0), "explicit_EAC", "missing")

# -----------------------------
# Metrics
# -----------------------------
ctd["SPI_CTD"] = safe_div(ctd["BCWP"], ctd["BCWS_eff"])
ctd["CPI_CTD"] = safe_div(ctd["BCWP"], ctd["ACWP_eff"])
ctd["BEI_CTD"] = safe_div(ctd["BCWP"], ctd["BAC_eff"])
ctd["VAC_CTD"] = ctd["BAC_eff"] - ctd["EAC_eff"]

lsd["SPI_LSD"] = safe_div(lsd["BCWP"], lsd["BCWS_eff"])
lsd["CPI_LSD"] = safe_div(lsd["BCWP"], lsd["ACWP_eff"])

# -----------------------------
# Output tables
# -----------------------------
source_evms_metrics = (
    ctd.groupby("source", as_index=False)
       .agg(BAC=("BAC_eff","sum"),
            BCWS=("BCWS_eff","sum"),
            BCWP=("BCWP","sum"),
            ACWP=("ACWP_eff","sum"),
            EAC=("EAC_eff","sum"))
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
    ctd[["source","SUB_TEAM","SPI_CTD","CPI_CTD","BEI_CTD","BAC_eff","EAC_eff","VAC_CTD","BAC_source","EAC_source"]]
    .rename(columns={"BAC_eff":"BAC","EAC_eff":"EAC"})
    .merge(lsd[["source","SUB_TEAM","SPI_LSD","CPI_LSD"]], on=["source","SUB_TEAM"], how="left")
)

subteam_cost = subteam_metrics[["source","SUB_TEAM","BAC","EAC","VAC_CTD","BAC_source","EAC_source"]].copy()

hours_tbl = (
    lsd[["source","SUB_TEAM","BCWS_eff","ACT_HRS","ETC"]]
    .rename(columns={"BCWS_eff":"Demand_proxy","ACT_HRS":"Actual_Hours","ETC":"Next_Month_ETC_proxy"})
)
hours_tbl["Pct_Var"] = safe_div((hours_tbl["Actual_Hours"] - hours_tbl["Demand_proxy"]), hours_tbl["Demand_proxy"])
hours_tbl["Next_Mo_BCWS_Hours"] = np.nan

# -----------------------------
# Diagnostics: WHY BAC/EAC are missing
# -----------------------------
diag_bac_eac = (
    subteam_cost.assign(
        BAC_missing=lambda d: d["BAC"].isna(),
        EAC_missing=lambda d: d["EAC"].isna()
    )
    .groupby("source", as_index=False)
    .agg(
        rows=("SUB_TEAM","count"),
        pct_BAC_missing=("BAC_missing","mean"),
        pct_EAC_missing=("EAC_missing","mean")
    )
    .sort_values(["pct_BAC_missing","pct_EAC_missing"], ascending=False)
)

print("✅ Created: source_evms_metrics, subteam_metrics, subteam_cost, hours_tbl")
print("\n--- BAC/EAC missing diagnostics (top 15) ---")
print(diag_bac_eac.head(15))
print("\nTip: filter subteam_cost where BAC_source=='missing' or EAC_source=='missing' to see which file types don't carry those metrics.")