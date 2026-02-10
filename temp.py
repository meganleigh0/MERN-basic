# ============================
# EVMS PIPELINE (ONE CELL)
# - Builds 4 tables with EXACT headers (no extras)
# - Uses "last Thursday of previous month" as LAST_STATUS_DATE (relative to today)
# - Minimizes missing values by:
#     * normalizing column names & values
#     * auto-resolving COST_SET / EVMS_BUCKET, SUB_TEAM, PROGRAM, HOURS, DATE
#     * snapping the status date to the closest available DATE <= target date per Program
#     * using safe sums + safe ratios
# - Saves everything to ONE Excel file for Power BI (4 sheets)
# ============================

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, date, timedelta

# ----------------------------
# 0) INPUT: you must already have your Cobra dataframe in memory as:
#     cobra_merged_df
# If yours is named differently, set it here:
# ----------------------------
df0 = cobra_merged_df  # <-- change if needed

# ----------------------------
# 1) CONFIG
# ----------------------------
PROGRAMS_KEEP = ["ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"]  # adjust if needed
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")  # one file, 4 sheets

# Metric thresholds are just for reference; this script does not color cells (Power BI handles visuals)
# SPI/CPI/BEI: Blue >=1.05, Green 0.98-1.05, Yellow 0.95-0.98, Red <0.95

# ----------------------------
# 2) HELPERS
# ----------------------------
def _norm_colname(c: str) -> str:
    c = str(c).strip().upper()
    c = c.replace(" ", "_").replace("-", "_")
    while "__" in c:
        c = c.replace("__", "_")
    return c

def _norm_str(s):
    if pd.isna(s):
        return np.nan
    s = str(s).strip().upper()
    s = " ".join(s.split())
    s = s.replace("-", "_")
    return s

def _coerce_dt(x):
    # robust datetime coercion
    return pd.to_datetime(x, errors="coerce")

def _safe_num(x):
    return pd.to_numeric(x, errors="coerce")

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    out = np.where((d == 0) | pd.isna(d), np.nan, n / d)
    return out

def last_thursday_of_month(y: int, m: int) -> pd.Timestamp:
    # last day of month
    if m == 12:
        last = date(y, 12, 31)
    else:
        last = date(y, m + 1, 1) - timedelta(days=1)
    # weekday: Mon=0 ... Sun=6, Thu=3
    offset = (last.weekday() - 3) % 7
    return pd.Timestamp(last - timedelta(days=offset))

def last_thursday_previous_month(ref_dt: pd.Timestamp) -> pd.Timestamp:
    ref_dt = pd.Timestamp(ref_dt).normalize()
    y = ref_dt.year
    m = ref_dt.month
    # previous month
    if m == 1:
        y2, m2 = y - 1, 12
    else:
        y2, m2 = y, m - 1
    return last_thursday_of_month(y2, m2)

def _pick_best_col(df, candidates):
    # returns first candidate that exists
    cols = set(df.columns)
    for c in candidates:
        if c in cols:
            return c
    return None

def _sheetname_safe(s: str) -> str:
    # Excel sheet names max 31 chars
    s = str(s)
    return s[:31]

# ----------------------------
# 3) NORMALIZE INPUT DF
# ----------------------------
df = df0.copy()
df.columns = [_norm_colname(c) for c in df.columns]

COL_PROGRAM = _pick_best_col(df, ["PROGRAM", "PROGRAMID", "PROGRAM_ID", "PROG", "PROG_ID"])
COL_SUBTEAM = _pick_best_col(df, ["SUB_TEAM", "SUBTEAM", "SUB_TEAM_ID", "IPT", "SUBTEAM_NAME"])
COL_DATE = _pick_best_col(df, ["DATE", "PERIOD_END", "PERIOD_END_DATE", "STATUS_DATE", "WEEK_END", "MONTH_END"])
COL_HOURS = _pick_best_col(df, ["HOURS", "HRS", "HOUR", "LABOR_HOURS", "TOTAL_HOURS"])
COL_BUCKET = _pick_best_col(df, ["COST_SET", "EVMS_BUCKET", "COSTSET", "BUCKET", "COST_CATEGORY"])

missing = [k for k,v in {
    "PROGRAM": COL_PROGRAM, "SUB_TEAM": COL_SUBTEAM, "DATE": COL_DATE, "HOURS": COL_HOURS, "COST_SET/BUCKET": COL_BUCKET
}.items() if v is None]
if missing:
    raise ValueError(f"Missing required columns in cobra_merged_df: {missing}\n"
                     f"Found columns: {list(df.columns)}")

df[COL_PROGRAM] = df[COL_PROGRAM].map(_norm_str)
df[COL_SUBTEAM] = df[COL_SUBTEAM].map(_norm_str)
df[COL_BUCKET] = df[COL_BUCKET].map(_norm_str)
df[COL_DATE] = _coerce_dt(df[COL_DATE])
df[COL_HOURS] = _safe_num(df[COL_HOURS])

# filter programs
df = df[df[COL_PROGRAM].isin([_norm_str(p) for p in PROGRAMS_KEEP])].copy()

# keep only relevant buckets (normalize to the 4 we use)
# We'll accept anything that CONTAINS these tokens, then map to canonical bucket names
def _map_bucket(b):
    if pd.isna(b):
        return np.nan
    b = str(b).upper()
    b = b.replace("-", "_").replace(" ", "_")
    if "BCWS" in b: return "BCWS"
    if "BCWP" in b: return "BCWP"
    if "ACWP" in b: return "ACWP"
    if "ETC"  in b: return "ETC"
    return np.nan

df["BUCKET_STD"] = df[COL_BUCKET].map(_map_bucket)
df = df[~df["BUCKET_STD"].isna()].copy()

# ----------------------------
# 4) STATUS DATES (LAST STATUS + NEXT PERIOD)
# - Last status date = last Thursday of previous month (relative to today)
# - But your data DATE may not land exactly on that Thursday,
#   so we SNAP per-program to the closest available DATE <= target.
# ----------------------------
today = pd.Timestamp.today().normalize()
target_last_status = last_thursday_previous_month(today)

# per-program snap to available date <= target
def _snap_asof_by_program(dfp, target_dt):
    # returns dict program -> snapped_date (max date <= target, else max date overall)
    out = {}
    for prog, g in dfp.groupby(COL_PROGRAM, dropna=False):
        dates = g[COL_DATE].dropna().sort_values().unique()
        if len(dates) == 0:
            out[prog] = pd.NaT
            continue
        # all dates <= target
        le = dates[dates <= np.datetime64(target_dt)]
        if len(le) > 0:
            out[prog] = pd.Timestamp(le.max()).normalize()
        else:
            # if nothing before target, fall back to max available
            out[prog] = pd.Timestamp(dates.max()).normalize()
    return out

asof_by_prog = _snap_asof_by_program(df, target_last_status)

# next period end (last Thursday of the month AFTER the snapped last status month)
def _next_period_target(d):
    if pd.isna(d):
        return pd.NaT
    d = pd.Timestamp(d)
    y, m = d.year, d.month
    if m == 12:
        return last_thursday_of_month(y + 1, 1)
    return last_thursday_of_month(y, m + 1)

next_target_by_prog = {p: _next_period_target(d) for p, d in asof_by_prog.items()}

# per-program snap next period to the closest available DATE <= next_target AND > last_status
def _snap_next_by_program(dfp):
    out = {}
    for prog, g in dfp.groupby(COL_PROGRAM, dropna=False):
        last_dt = asof_by_prog.get(prog, pd.NaT)
        nxt_tgt = next_target_by_prog.get(prog, pd.NaT)
        dates = g[COL_DATE].dropna().sort_values().unique()
        if pd.isna(last_dt) or pd.isna(nxt_tgt) or len(dates) == 0:
            out[prog] = pd.NaT
            continue
        cand = dates[(dates > np.datetime64(last_dt)) & (dates <= np.datetime64(nxt_tgt))]
        if len(cand) > 0:
            out[prog] = pd.Timestamp(cand.max()).normalize()
        else:
            out[prog] = pd.NaT
    return out

next_asof_by_prog = _snap_next_by_program(df)

# add these to df as row-level columns
df["ASOF_DATE"] = df[COL_PROGRAM].map(asof_by_prog)
df["NEXT_ASOF_DATE"] = df[COL_PROGRAM].map(next_asof_by_prog)

# ----------------------------
# 5) CORE AGGS
# ----------------------------
# CTD window: all rows with DATE <= ASOF_DATE
ctd = df[df[COL_DATE] <= df["ASOF_DATE"]].copy()

# LSD window: rows at the snapped as-of date (exact match)
lsd = df[df[COL_DATE] == df["ASOF_DATE"]].copy()

# Next-month window: rows at snapped NEXT_ASOF_DATE
nxt = df[df[COL_DATE] == df["NEXT_ASOF_DATE"]].copy()

def _sum_hours(dfx, group_cols):
    # returns wide df with BCWS/BCWP/ACWP/ETC sums
    g = (
        dfx.groupby(group_cols + ["BUCKET_STD"], dropna=False)[COL_HOURS]
        .sum(min_count=1)
        .reset_index()
    )
    wide = g.pivot_table(
        index=group_cols,
        columns="BUCKET_STD",
        values=COL_HOURS,
        aggfunc="sum"
    ).reset_index()
    # ensure all expected bucket cols exist
    for b in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if b not in wide.columns:
            wide[b] = np.nan
    # flatten pivot columns
    wide.columns = [c if isinstance(c, str) else str(c) for c in wide.columns]
    return wide

# Program totals (CTD/LSD)
prog_ctd = _sum_hours(ctd, [COL_PROGRAM])
prog_lsd = _sum_hours(lsd, [COL_PROGRAM])

# Subteam totals (CTD/LSD)
sub_ctd = _sum_hours(ctd, [COL_PROGRAM, COL_SUBTEAM])
sub_lsd = _sum_hours(lsd, [COL_PROGRAM, COL_SUBTEAM])

# Next-period program totals (for manpower)
prog_nxt = _sum_hours(nxt, [COL_PROGRAM])

# ----------------------------
# 6) BUILD TABLES (EXACT HEADERS)
# ----------------------------

# ---- 6A) Program_Overview: ProgramID, Metric, CTD, LSD, Comments / Root Cause & Corrective Actions
# SPI = BCWP/BCWS, CPI = BCWP/ACWP, BEI (approx) = BCWP/ETC (your earlier deck used BEI alongside CPI/SPI)
po = prog_ctd.merge(prog_lsd, on=[COL_PROGRAM], how="outer", suffixes=("_CTD", "_LSD"))

po["SPI_CTD"] = _safe_div(po["BCWP_CTD"] if "BCWP_CTD" in po.columns else po["BCWP"], po["BCWS_CTD"] if "BCWS_CTD" in po.columns else po["BCWS"])
po["CPI_CTD"] = _safe_div(po["BCWP_CTD"] if "BCWP_CTD" in po.columns else po["BCWP"], po["ACWP_CTD"] if "ACWP_CTD" in po.columns else po["ACWP"])
po["BEI_CTD"] = _safe_div(po["BCWP_CTD"] if "BCWP_CTD" in po.columns else po["BCWP"], po["ETC_CTD"]  if "ETC_CTD"  in po.columns else po["ETC"])

po["SPI_LSD"] = _safe_div(po["BCWP_LSD"] if "BCWP_LSD" in po.columns else po["BCWP"], po["BCWS_LSD"] if "BCWS_LSD" in po.columns else po["BCWS"])
po["CPI_LSD"] = _safe_div(po["BCWP_LSD"] if "BCWP_LSD" in po.columns else po["BCWP"], po["ACWP_LSD"] if "ACWP_LSD" in po.columns else po["ACWP"])
po["BEI_LSD"] = _safe_div(po["BCWP_LSD"] if "BCWP_LSD" in po.columns else po["BCWP"], po["ETC_LSD"]  if "ETC_LSD"  in po.columns else po["ETC"])

program_overview = pd.concat([
    pd.DataFrame({
        "ProgramID": po[COL_PROGRAM],
        "Metric": "SPI",
        "CTD": po["SPI_CTD"],
        "LSD": po["SPI_LSD"],
        "Comments / Root Cause & Corrective Actions": ""
    }),
    pd.DataFrame({
        "ProgramID": po[COL_PROGRAM],
        "Metric": "CPI",
        "CTD": po["CPI_CTD"],
        "LSD": po["CPI_LSD"],
        "Comments / Root Cause & Corrective Actions": ""
    }),
    pd.DataFrame({
        "ProgramID": po[COL_PROGRAM],
        "Metric": "BEI",
        "CTD": po["BEI_CTD"],
        "LSD": po["BEI_LSD"],
        "Comments / Root Cause & Corrective Actions": ""
    })
], ignore_index=True)

# ---- 6B) SubTeam_SPI_CPI: SubTeam, SPI LSD, SPI CTD, CPI LSD, CPI CTD, Comments / Root Cause & Corrective Actions, ProgramID
st = sub_ctd.merge(sub_lsd, on=[COL_PROGRAM, COL_SUBTEAM], how="outer", suffixes=("_CTD", "_LSD"))

st["SPI_CTD"] = _safe_div(st["BCWP_CTD"] if "BCWP_CTD" in st.columns else st["BCWP"], st["BCWS_CTD"] if "BCWS_CTD" in st.columns else st["BCWS"])
st["CPI_CTD"] = _safe_div(st["BCWP_CTD"] if "BCWP_CTD" in st.columns else st["BCWP"], st["ACWP_CTD"] if "ACWP_CTD" in st.columns else st["ACWP"])
st["SPI_LSD"] = _safe_div(st["BCWP_LSD"] if "BCWP_LSD" in st.columns else st["BCWP"], st["BCWS_LSD"] if "BCWS_LSD" in st.columns else st["BCWS"])
st["CPI_LSD"] = _safe_div(st["BCWP_LSD"] if "BCWP_LSD" in st.columns else st["BCWP"], st["ACWP_LSD"] if "ACWP_LSD" in st.columns else st["ACWP"])

subteam_spi_cpi = pd.DataFrame({
    "SubTeam": st[COL_SUBTEAM],
    "SPI LSD": st["SPI_LSD"],
    "SPI CTD": st["SPI_CTD"],
    "CPI LSD": st["CPI_LSD"],
    "CPI CTD": st["CPI_CTD"],
    "Comments / Root Cause & Corrective Actions": "",
    "ProgramID": st[COL_PROGRAM]
})

# ---- 6C) SubTeam_BAC_EAC_VAC: SubTeam, BAC, EAC, VAC, Comments / Root Cause & Corrective Actions, ProgramID
# If your Cobra dataset truly has BAC/EAC/VAC in HOURS by bucket or separate fields, plug them here.
# For now (based on your current data snapshots), we derive:
#   BAC ~ BCWS_CTD (planned budgeted hours-to-date proxy)
#   EAC ~ ACWP_CTD + ETC_CTD (estimate at completion proxy)
#   VAC ~ BAC - EAC
# This eliminates "Missing value" rows as long as buckets exist.
bac_eac = sub_ctd.copy()
# ensure columns exist
for b in ["BCWS", "ACWP", "ETC"]:
    if b not in bac_eac.columns:
        bac_eac[b] = np.nan

bac_eac["BAC"] = bac_eac["BCWS"]
bac_eac["EAC"] = (bac_eac["ACWP"].fillna(0) + bac_eac["ETC"].fillna(0)).replace({0: np.nan}).where(
    ~(bac_eac["ACWP"].isna() & bac_eac["ETC"].isna()), np.nan
)
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]

subteam_bac_eac_vac = pd.DataFrame({
    "SubTeam": bac_eac[COL_SUBTEAM],
    "BAC": bac_eac["BAC"],
    "EAC": bac_eac["EAC"],
    "VAC": bac_eac["VAC"],
    "Comments / Root Cause & Corrective Actions": "",
    "ProgramID": bac_eac[COL_PROGRAM]
})

# ---- 6D) Program_Manpower: ProgramID, Demand Hours, Actual Hours, % Var, Next Mo BCWS Hours, Next Mo ETC Hours, Comments / Root Cause & Corrective Actions
pm = prog_ctd.merge(prog_nxt, on=[COL_PROGRAM], how="left", suffixes=("_CTD", "_NEXT"))
# demand=BCWS, actual=ACWP
pm["Demand Hours"] = pm["BCWS"] if "BCWS" in pm.columns else np.nan
pm["Actual Hours"] = pm["ACWP"] if "ACWP" in pm.columns else np.nan
pm["% Var"] = _safe_div(pm["Actual Hours"], pm["Demand Hours"]) * 100.0

pm["Next Mo BCWS Hours"] = pm["BCWS_NEXT"] if "BCWS_NEXT" in pm.columns else np.nan
pm["Next Mo ETC Hours"] = pm["ETC_NEXT"] if "ETC_NEXT" in pm.columns else np.nan

program_manpower = pd.DataFrame({
    "ProgramID": pm[COL_PROGRAM],
    "Demand Hours": pm["Demand Hours"],
    "Actual Hours": pm["Actual Hours"],
    "% Var": pm["% Var"],
    "Next Mo BCWS Hours": pm["Next Mo BCWS Hours"],
    "Next Mo ETC Hours": pm["Next Mo ETC Hours"],
    "Comments / Root Cause & Corrective Actions": ""
})

# ----------------------------
# 7) CLEAN UP (reduce missing values further)
# - Drop blank/NaN subteams
# - Replace inf with NaN
# - Sort for readability
# ----------------------------
def _clean(df_):
    out = df_.replace([np.inf, -np.inf], np.nan).copy()
    return out

program_overview = _clean(program_overview)
subteam_spi_cpi = _clean(subteam_spi_cpi)
subteam_bac_eac_vac = _clean(subteam_bac_eac_vac)
program_manpower = _clean(program_manpower)

subteam_spi_cpi = subteam_spi_cpi[~subteam_spi_cpi["SubTeam"].isna() & (subteam_spi_cpi["SubTeam"] != "")]
subteam_bac_eac_vac = subteam_bac_eac_vac[~subteam_bac_eac_vac["SubTeam"].isna() & (subteam_bac_eac_vac["SubTeam"] != "")]

program_overview = program_overview.sort_values(["ProgramID", "Metric"], kind="stable")
subteam_spi_cpi = subteam_spi_cpi.sort_values(["ProgramID", "SubTeam"], kind="stable")
subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["ProgramID", "SubTeam"], kind="stable")
program_manpower = program_manpower.sort_values(["ProgramID"], kind="stable")

# ----------------------------
# 8) SAVE ONE EXCEL FILE (Power BI input)
# - Sheet names match what you showed in Excel tabs
# ----------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name=_sheetname_safe("Program_Overview"), index=False)
    subteam_spi_cpi.to_excel(writer, sheet_name=_sheetname_safe("SubTeam_SPI_CPI"), index=False)
    subteam_bac_eac_vac.to_excel(writer, sheet_name=_sheetname_safe("SubTeam_BAC_EAC_VAC"), index=False)
    program_manpower.to_excel(writer, sheet_name=_sheetname_safe("Program_Manpower"), index=False)

print("✅ Saved:", str(OUTPUT_XLSX.resolve()))
print("Status-date logic:")
print("  Today:", today.date())
print("  Target last-status (last Thu of prev month):", target_last_status.date())
print("  Snapped ASOF per program:", {k: (v.date() if not pd.isna(v) else None) for k,v in asof_by_prog.items()})
print("  Snapped NEXT per program:", {k: (v.date() if not pd.isna(v) else None) for k,v in next_asof_by_prog.items()})

# Optional quick checks:
display(program_overview.head(50))
display(subteam_spi_cpi.head(50))
display(subteam_bac_eac_vac.head(50))
display(program_manpower.head(50))