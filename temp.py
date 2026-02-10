# =========================
# EVMS PIPELINE (ONE CELL)
# - Uses existing dataframe: cobra_merged_df (or cobra_merged / cobra_df fallback)
# - Computes status period end = last Thursday of PRIOR month (relative to today or AS_OF_DATE)
# - Builds 4 PowerBI-ready tables with EXACT headers (no extras)
# - Saves EVERYTHING to one Excel file
# =========================

import pandas as pd
import numpy as np
from datetime import datetime, date

# -------------------------
# 0) INPUTS YOU MAY EDIT
# -------------------------
AS_OF_DATE = None  # e.g. "2026-02-10" (None -> uses today's date)
PROGRAM_FILTER = None  # e.g. ["ABRAMS_22","OLYMPUS","STRYKER_BULG","XM30"] or None
OUTPUT_XLSX = "EVMS_PowerBI_Export.xlsx"

# -------------------------
# 1) GET THE RAW DF
# -------------------------
df0 = None
for _name in ["cobra_merged_df", "cobra_merged", "cobra_df", "df", "cobra"]:
    if _name in globals() and isinstance(globals()[_name], pd.DataFrame):
        df0 = globals()[_name].copy()
        break
if df0 is None:
    raise ValueError("Could not find a dataframe. Expected one of: cobra_merged_df, cobra_merged, cobra_df, df, cobra")

# -------------------------
# 2) HELPERS
# -------------------------
def _as_dt(x):
    return pd.to_datetime(x, errors="coerce")

def safe_div(num, den):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    out = np.where((den == 0) | pd.isna(den), np.nan, num / den)
    return pd.Series(out)

def normalize_cols(d):
    d = d.copy()
    # Normalize column names
    d.columns = [str(c).strip().upper().replace(" ", "_") for c in d.columns]

    # Allow common variants
    rename_map = {}
    if "COST-SET" in d.columns and "COST_SET" not in d.columns:
        rename_map["COST-SET"] = "COST_SET"
    if "SUBTEAM" in d.columns and "SUB_TEAM" not in d.columns:
        rename_map["SUBTEAM"] = "SUB_TEAM"
    if "PROGRAMID" in d.columns and "PROGRAM" not in d.columns:
        rename_map["PROGRAMID"] = "PROGRAM"
    if rename_map:
        d = d.rename(columns=rename_map)

    required = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required if c not in d.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(d.columns)}")

    # Normalize values
    d["PROGRAM"] = d["PROGRAM"].astype(str).str.strip().str.upper()
    d["SUB_TEAM"] = d["SUB_TEAM"].astype(str).str.strip().str.upper()

    d["COST_SET"] = (
        d["COST_SET"].astype(str)
        .str.strip()
        .str.upper()
        .str.replace(r"\s+", "", regex=True)
        .str.replace("-", "", regex=False)
    )

    # Canonical EVMS buckets (exactly what we use)
    cost_map = {
        "BCWS": "BCWS",
        "BCWP": "BCWP",
        "ACWP": "ACWP",
        "ETC":  "ETC",
    }
    d["EVMS_BUCKET"] = d["COST_SET"].map(cost_map)
    d["DATE"] = _as_dt(d["DATE"])
    d["HOURS"] = pd.to_numeric(d["HOURS"], errors="coerce")

    # Keep only usable rows for this pipeline
    d = d.dropna(subset=["PROGRAM", "SUB_TEAM", "DATE", "HOURS", "EVMS_BUCKET"]).copy()
    return d

def last_thursday_of_month(any_day):
    """Return last Thursday (weekday=3) for the month containing any_day."""
    any_day = pd.Timestamp(any_day).normalize()
    last_day = (any_day + pd.offsets.MonthEnd(0)).normalize()
    # Thursday=3
    offset = (last_day.weekday() - 3) % 7
    return (last_day - pd.Timedelta(days=offset)).normalize()

def status_period_end(as_of):
    """
    LAST STATUS DATE rule:
    - last Thursday of the PRIOR month relative to as_of
    Examples:
      as_of=Feb 10 -> last Thu of Jan
      as_of=Jan 28 -> last Thu of Dec
    """
    as_of = pd.Timestamp(as_of).normalize()
    prior_month_day = (as_of - pd.offsets.MonthBegin(1))  # any day in prior month
    return last_thursday_of_month(prior_month_day)

def build_period_calendar(min_date, max_date):
    """
    Build monthly period_end calendar = last Thursday of each month, spanning min_date..max_date.
    """
    min_date = pd.Timestamp(min_date).normalize()
    max_date = pd.Timestamp(max_date).normalize()
    start = (min_date - pd.offsets.MonthBegin(1)).normalize()
    end = (max_date + pd.offsets.MonthEnd(2)).normalize()

    months = pd.date_range(start=start, end=end, freq="MS")
    ends = pd.Series([last_thursday_of_month(m) for m in months], dtype="datetime64[ns]").drop_duplicates().sort_values()
    return ends.reset_index(drop=True)

def assign_period_end(dates, period_ends):
    """
    For each DATE, assign the first period_end >= DATE (like "belongs to that status month").
    Uses searchsorted safely.
    """
    pe = pd.to_datetime(period_ends).values.astype("datetime64[ns]")
    dt = pd.to_datetime(dates).values.astype("datetime64[ns]")
    idx = np.searchsorted(pe, dt, side="left")
    out = np.where(idx < len(pe), pe[idx], np.datetime64("NaT"))
    return pd.to_datetime(out)

# -------------------------
# 3) NORMALIZE + FILTER
# -------------------------
df = normalize_cols(df0)

if PROGRAM_FILTER is not None:
    wanted = [str(x).strip().upper() for x in PROGRAM_FILTER]
    df = df[df["PROGRAM"].isin(wanted)].copy()

as_of = pd.Timestamp(AS_OF_DATE).normalize() if AS_OF_DATE else pd.Timestamp.today().normalize()
status_end = status_period_end(as_of)  # last Thu of PRIOR month (always in the past)
next_end = last_thursday_of_month(as_of)  # last Thu of CURRENT month (may be future)

# Build period calendar from data range (but ensure it covers status_end/next_end too)
min_dt = min(df["DATE"].min(), status_end, next_end)
max_dt = max(df["DATE"].max(), status_end, next_end)
period_ends = build_period_calendar(min_dt, max_dt)

df["PERIOD_END"] = assign_period_end(df["DATE"], period_ends)

# Keep only rows up through the status period for CTD/LSD computations
df_hist = df[df["PERIOD_END"].notna() & (df["PERIOD_END"] <= status_end)].copy()

# -------------------------
# 4) PERIOD PIVOT (PROGRAM+SUBTEAM+PERIOD_END) -> BCWS/BCWP/ACWP/ETC columns
# -------------------------
g = (
    df_hist
    .groupby(["PROGRAM", "SUB_TEAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
    .sum()
    .reset_index()
)

pivot = (
    g.pivot_table(index=["PROGRAM", "SUB_TEAM", "PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum", fill_value=0.0)
    .reset_index()
)

for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
    if c not in pivot.columns:
        pivot[c] = 0.0

pivot = pivot.sort_values(["PROGRAM", "SUB_TEAM", "PERIOD_END"]).reset_index(drop=True)

# Cumulative-to-date for BCWS/BCWP/ACWP (ETC is treated as "current period" value)
for c in ["BCWS", "BCWP", "ACWP"]:
    pivot[f"{c}_CTD"] = pivot.groupby(["PROGRAM", "SUB_TEAM"], dropna=False)[c].cumsum()

pivot["ETC_CUR"] = pivot["ETC"]  # current period ETC (not cumulative)

# Extract LSD = status period values; CTD = cumulative at status
status_rows = pivot[pivot["PERIOD_END"] == status_end].copy()

# If a subteam has no row exactly at status_end, create a 0 row so PowerBI has stable schema
all_keys = pivot[["PROGRAM","SUB_TEAM"]].drop_duplicates()
status_rows = all_keys.merge(status_rows, on=["PROGRAM","SUB_TEAM"], how="left")
for c in ["BCWS","BCWP","ACWP","ETC","BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CUR"]:
    if c in status_rows.columns:
        status_rows[c] = pd.to_numeric(status_rows[c], errors="coerce").fillna(0.0)
status_rows["PERIOD_END"] = status_end

# Previous period CTD (for BEI_LSD denominator)
prev_end = period_ends[period_ends < status_end].max() if (period_ends < status_end).any() else pd.NaT
prev_rows = pivot[pivot["PERIOD_END"] == prev_end][["PROGRAM","SUB_TEAM","BCWS_CTD","BCWP_CTD"]].copy() if pd.notna(prev_end) else pd.DataFrame(columns=["PROGRAM","SUB_TEAM","BCWS_CTD","BCWP_CTD"])
prev_rows = all_keys.merge(prev_rows, on=["PROGRAM","SUB_TEAM"], how="left").fillna(0.0)

# -------------------------
# 5) SUBTEAM SPI/CPI (LSD + CTD)
# -------------------------
sub = status_rows.merge(prev_rows, on=["PROGRAM","SUB_TEAM"], suffixes=("","_PREV"))

sub["SPI_LSD"] = safe_div(sub["BCWP"], sub["BCWS"])
sub["SPI_CTD"] = safe_div(sub["BCWP_CTD"], sub["BCWS_CTD"])
sub["CPI_LSD"] = safe_div(sub["BCWP"], sub["ACWP"])
sub["CPI_CTD"] = safe_div(sub["BCWP_CTD"], sub["ACWP_CTD"])

# -------------------------
# 6) PROGRAM-LEVEL METRICS (SPI/CPI/BEI) for overview table
#    - Program BAC = BCWS_CTD at the LATEST period available in df_hist (per program)
# -------------------------
# Build program-period totals
prog_period = (
    pivot.groupby(["PROGRAM","PERIOD_END"], dropna=False)[["BCWS","BCWP","ACWP","BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CUR"]]
    .sum()
    .reset_index()
    .sort_values(["PROGRAM","PERIOD_END"])
)

# Program BAC = latest BCWS_CTD available in history for that program
prog_bac = (
    prog_period.sort_values(["PROGRAM","PERIOD_END"])
    .groupby("PROGRAM", dropna=False)["BCWS_CTD"]
    .last()
    .rename("BAC_TOTAL")
    .reset_index()
)

prog_status = prog_period[prog_period["PERIOD_END"] == status_end].copy()
prog_status = prog_bac.merge(prog_status, on="PROGRAM", how="left").fillna(0.0)

# Previous program BCWS_CTD (for BEI_LSD denom)
if pd.notna(prev_end):
    prog_prev = prog_period[prog_period["PERIOD_END"] == prev_end][["PROGRAM","BCWS_CTD","BCWP_CTD"]].copy()
else:
    prog_prev = pd.DataFrame({"PROGRAM": prog_status["PROGRAM"], "BCWS_CTD": 0.0, "BCWP_CTD": 0.0})
prog_prev = prog_status[["PROGRAM"]].merge(prog_prev, on="PROGRAM", how="left").fillna(0.0).rename(columns={"BCWS_CTD":"BCWS_CTD_PREV","BCWP_CTD":"BCWP_CTD_PREV"})
prog_status = prog_status.merge(prog_prev, on="PROGRAM", how="left")

# Compute SPI/CPI and BEI
prog_status["SPI_LSD"] = safe_div(prog_status["BCWP"], prog_status["BCWS"])
prog_status["SPI_CTD"] = safe_div(prog_status["BCWP_CTD"], prog_status["BCWS_CTD"])
prog_status["CPI_LSD"] = safe_div(prog_status["BCWP"], prog_status["ACWP"])
prog_status["CPI_CTD"] = safe_div(prog_status["BCWP_CTD"], prog_status["ACWP_CTD"])

# BEI definition used here:
# - BEI_CTD = BCWP_CTD / BAC_TOTAL
# - BEI_LSD = BCWP_LSD / (BAC_TOTAL - BCWS_CTD_PREV)  (progress this period vs remaining baseline)
prog_status["BEI_CTD"] = safe_div(prog_status["BCWP_CTD"], prog_status["BAC_TOTAL"])
prog_status["BEI_LSD"] = safe_div(prog_status["BCWP"], (prog_status["BAC_TOTAL"] - prog_status["BCWS_CTD_PREV"]).clip(lower=0))

# -------------------------
# 7) SUBTEAM BAC/EAC/VAC (hours)
#    - BAC_HRS = BCWS_CTD (through status)
#    - EAC_HRS = ACWP_CTD + ETC_CUR (current ETC at status)
#    - VAC_HRS = BAC_HRS - EAC_HRS
# -------------------------
sub_bac = sub.copy()
sub_bac["BAC_HRS"] = sub_bac["BCWS_CTD"]
sub_bac["EAC_HRS"] = sub_bac["ACWP_CTD"] + sub_bac["ETC_CUR"]
sub_bac["VAC_HRS"] = sub_bac["BAC_HRS"] - sub_bac["EAC_HRS"]

# -------------------------
# 8) PROGRAM MANPOWER (hours) + NEXT MONTH (period_end == next_end)
#    - Demand Hours = BCWS_CTD
#    - Actual Hours = ACWP_CTD
#    - % Var = Actual / Demand
#    - Next Mo BCWS/ETC = totals in period_end == next_end (may be missing if future)
# -------------------------
prog_man = prog_status[["PROGRAM","BCWS_CTD","ACWP_CTD"]].copy()
prog_man = prog_man.rename(columns={"BCWS_CTD":"DEMAND_HRS_CTD","ACWP_CTD":"ACTUAL_HRS_CTD"})
prog_man["PCT_VAR"] = safe_div(prog_man["ACTUAL_HRS_CTD"], prog_man["DEMAND_HRS_CTD"])

# Next period totals (if available in data)
next_period = (
    df[df["PERIOD_END"] == next_end]
    .groupby(["PROGRAM","EVMS_BUCKET"], dropna=False)["HOURS"]
    .sum()
    .unstack("EVMS_BUCKET")
)
for c in ["BCWS","ETC"]:
    if c not in next_period.columns:
        next_period[c] = np.nan
next_period = next_period.reset_index().rename(columns={"BCWS":"NEXT_PERIOD_BCWS_HRS","ETC":"NEXT_PERIOD_ETC_HRS"})

prog_man = prog_man.merge(next_period[["PROGRAM","NEXT_PERIOD_BCWS_HRS","NEXT_PERIOD_ETC_HRS"]], on="PROGRAM", how="left")

# -------------------------
# 9) BUILD EXACT OUTPUT TABLES (EXACT HEADERS ONLY)
# -------------------------

# (A) Program overview table
# EXACT HEADERS: ProgramID | Metric | CTD | LSD | Comments / Root Cause & Corrective Actions
program_overview_rows = []
for _, r in prog_status.sort_values("PROGRAM").iterrows():
    program_overview_rows += [
        {"ProgramID": r["PROGRAM"], "Metric": "SPI", "CTD": r["SPI_CTD"], "LSD": r["SPI_LSD"], "Comments / Root Cause & Corrective Actions": ""},
        {"ProgramID": r["PROGRAM"], "Metric": "CPI", "CTD": r["CPI_CTD"], "LSD": r["CPI_LSD"], "Comments / Root Cause & Corrective Actions": ""},
        {"ProgramID": r["PROGRAM"], "Metric": "BEI", "CTD": r["BEI_CTD"], "LSD": r["BEI_LSD"], "Comments / Root Cause & Corrective Actions": ""},
    ]
program_overview = pd.DataFrame(program_overview_rows, columns=[
    "ProgramID","Metric","CTD","LSD","Comments / Root Cause & Corrective Actions"
])

# (B) Subteam SPI/CPI table
# EXACT HEADERS: SubTeam | SPI LSD | SPI CTD | CPI LSD | CPI CTD | Comments / Root Cause & Corrective Actions | ProgramID
subteam_spi_cpi = pd.DataFrame({
    "SubTeam": sub["SUB_TEAM"],
    "SPI LSD": sub["SPI_LSD"],
    "SPI CTD": sub["SPI_CTD"],
    "CPI LSD": sub["CPI_LSD"],
    "CPI CTD": sub["CPI_CTD"],
    "Comments / Root Cause & Corrective Actions": "",
    "ProgramID": sub["PROGRAM"],
})[[
    "SubTeam","SPI LSD","SPI CTD","CPI LSD","CPI CTD","Comments / Root Cause & Corrective Actions","ProgramID"
]].sort_values(["ProgramID","SubTeam"])

# (C) Subteam BAC/EAC/VAC table
# EXACT HEADERS: SubTeam | BAC | EAC | VAC | Comments / Root Cause & Corrective Actions | ProgramID
subteam_bac_eac_vac = pd.DataFrame({
    "SubTeam": sub_bac["SUB_TEAM"],
    "BAC": sub_bac["BAC_HRS"],
    "EAC": sub_bac["EAC_HRS"],
    "VAC": sub_bac["VAC_HRS"],
    "Comments / Root Cause & Corrective Actions": "",
    "ProgramID": sub_bac["PROGRAM"],
})[[
    "SubTeam","BAC","EAC","VAC","Comments / Root Cause & Corrective Actions","ProgramID"
]].sort_values(["ProgramID","SubTeam"])

# (D) Program manpower table
# EXACT HEADERS: ProgramID | Demand Hours | Actual Hours | % Var | Next Mo BCWS Hours | Next Mo ETC Hours | Comments / Root Cause & Corrective Actions
program_manpower = pd.DataFrame({
    "ProgramID": prog_man["PROGRAM"],
    "Demand Hours": prog_man["DEMAND_HRS_CTD"],
    "Actual Hours": prog_man["ACTUAL_HRS_CTD"],
    "% Var": prog_man["PCT_VAR"] * 100.0,
    "Next Mo BCWS Hours": prog_man["NEXT_PERIOD_BCWS_HRS"],
    "Next Mo ETC Hours": prog_man["NEXT_PERIOD_ETC_HRS"],
    "Comments / Root Cause & Corrective Actions": "",
})[[
    "ProgramID","Demand Hours","Actual Hours","% Var","Next Mo BCWS Hours","Next Mo ETC Hours","Comments / Root Cause & Corrective Actions"
]].sort_values("ProgramID")

# -------------------------
# 10) SAVE ONE EXCEL FILE (PowerBI friendly)
# -------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print("✅ Export complete:", OUTPUT_XLSX)
print("As-of date:", as_of.date())
print("Last status period end (last Thu of prior month):", status_end.date())
print("Next period end (last Thu of current month):", next_end.date())
print("Programs exported:", sorted(program_overview["ProgramID"].unique().tolist()))