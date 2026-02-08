# FULL PIPELINE (single cell) — builds 4 PowerBI-ready tables + debug tabs + writes one Excel
# Assumptions:
#   - You already have a dataframe loaded with Cobra rows (ex: cobra_merged_df)
#   - Required columns (case-insensitive): PROGRAM, SUB_TEAM, COST-SET (or COST_SET), DATE, HOURS
#
# Output:
#   - t1_program_evms
#   - t2_prog_subteam_spi_cpi
#   - t3_prog_subteam_bac_eac_vac
#   - t4_program_demand_actual_next
#   - Excel file: EVMS_Metrics_PowerBI.xlsx (all tables + debug)

import pandas as pd
import numpy as np

# ----------------------------
# 0) INPUT: set your raw df here
# ----------------------------
# If your df is named differently, change this line:
df_raw = cobra_merged_df.copy()

# Only keep these 4 programs for now (edit as needed)
PROGRAMS = ["ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"]

# Placeholder LSD_END: 2 weeks prior to "today"
TODAY = pd.Timestamp.today().normalize()
LSD_END = (TODAY - pd.Timedelta(days=14)).normalize()
LSD_START = LSD_END - pd.Timedelta(days=13)  # 14-day window inclusive
NEXT_START = LSD_END + pd.Timedelta(days=1)
NEXT_END = LSD_END + pd.Timedelta(days=28)   # ~4-week "next month" window inclusive

# CTD start: Fiscal year start (placeholder: Jan 1 of LSD_END year)
FY_START = pd.Timestamp(year=LSD_END.year, month=1, day=1)

print(f"Using placeholder LSD_END: {LSD_END.date()}")
print(f"LSD window: {LSD_START.date()} to {LSD_END.date()}")
print(f"Next window: {NEXT_START.date()} to {NEXT_END.date()}")
print(f"FY start (CTD): {FY_START.date()}")


# ----------------------------
# 1) Helpers
# ----------------------------
def _norm_str(s: pd.Series) -> pd.Series:
    s = s.astype(str)
    s = s.replace({"None": "", "nan": "", "NaN": ""})
    return s.str.strip().str.upper()

def _find_col(df: pd.DataFrame, candidates):
    cols_up = {c.upper(): c for c in df.columns}
    for cand in candidates:
        if cand.upper() in cols_up:
            return cols_up[cand.upper()]
    raise KeyError(f"Missing required column. Tried: {candidates}. Found: {list(df.columns)}")

def _prep_base(df: pd.DataFrame) -> pd.DataFrame:
    c_program = _find_col(df, ["PROGRAM"])
    c_subteam = _find_col(df, ["SUB_TEAM", "SUBTEAM", "SUB TEAM"])
    c_costset = _find_col(df, ["COST-SET", "COST_SET", "COSTSET", "COST SET"])
    c_date    = _find_col(df, ["DATE"])
    c_hours   = _find_col(df, ["HOURS", "HRS", "HOURS_HRS"])

    d = df[[c_program, c_subteam, c_costset, c_date, c_hours]].copy()
    d.columns = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]

    d["PROGRAM"]  = _norm_str(d["PROGRAM"])
    d["SUB_TEAM"] = _norm_str(d["SUB_TEAM"]).replace({"": "UNSPECIFIED"})
    d["COST_SET"] = _norm_str(d["COST_SET"]).replace({"": "UNSPECIFIED"})

    # robust datetime + numeric
    d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
    d["HOURS"] = pd.to_numeric(d["HOURS"], errors="coerce")

    return d

def _sum_window(d: pd.DataFrame, cost_set: str, start: pd.Timestamp, end: pd.Timestamp, by):
    m = (d["COST_SET"] == cost_set) & d["DATE"].between(start, end, inclusive="both")
    out = d.loc[m].groupby(by, as_index=False)["HOURS"].sum()
    out.rename(columns={"HOURS": f"{cost_set}_SUM"}, inplace=True)
    return out

def _sum_ctd(d: pd.DataFrame, cost_set: str, end: pd.Timestamp, by, start: pd.Timestamp=None):
    m = (d["COST_SET"] == cost_set) & (d["DATE"] <= end)
    if start is not None:
        m &= (d["DATE"] >= start)
    out = d.loc[m].groupby(by, as_index=False)["HOURS"].sum()
    out.rename(columns={"HOURS": f"{cost_set}_CTD"}, inplace=True)
    return out

def _asof_latest_value(d: pd.DataFrame, cost_set: str, end: pd.Timestamp, by):
    """
    For point-in-time series like BAC/EAC that can repeat by date,
    take the LATEST DATE <= end per group and sum HOURS on that date.
    """
    x = d[(d["COST_SET"] == cost_set) & (d["DATE"] <= end)].copy()
    if x.empty:
        return pd.DataFrame(columns=by + [f"{cost_set}_ASOF"])
    x.sort_values(["DATE"], inplace=True)
    # find latest date per group
    latest = x.groupby(by)["DATE"].transform("max")
    x = x.loc[x["DATE"].eq(latest)]
    out = x.groupby(by, as_index=False)["HOURS"].sum()
    out.rename(columns={"HOURS": f"{cost_set}_ASOF"}, inplace=True)
    return out

def _safe_ratio(num, den):
    num = num.astype(float)
    den = den.astype(float)
    return np.where(den == 0, 0.0, num / den)

def _left_merge(base, add, on):
    if add is None or add.empty:
        return base
    return base.merge(add, how="left", on=on)

def _fill_zero(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    return df


# ----------------------------
# 2) Clean + filter + debug “bad rows”
# ----------------------------
d0 = _prep_base(df_raw)

# Filter to your 4 programs
d0 = d0[d0["PROGRAM"].isin([p.upper() for p in PROGRAMS])].copy()

# Identify rows that will break time-phased calcs
mask_bad = d0["DATE"].isna() | d0["HOURS"].isna()
bad_rows = d0.loc[mask_bad].copy()

# IMPORTANT: reason mask must be based on bad_rows (fixes your length mismatch error)
bad_rows["BAD_REASON"] = "HOURS_NAN"
bad_rows.loc[bad_rows["DATE"].isna(), "BAD_REASON"] = "DATE_NAT"

# Drop bad rows for all computations
d = d0.loc[~mask_bad].copy()

# Optional: drop obvious header/total noise if present
# (keep "TOTAL" subteams if you want; comment out if needed)
# d = d[d["SUB_TEAM"].ne("TOTAL")].copy()

# Base dimensions
base_program = pd.DataFrame({"PROGRAM": sorted(d["PROGRAM"].unique())})
base_prog_sub = (
    d[["PROGRAM", "SUB_TEAM"]]
    .drop_duplicates()
    .sort_values(["PROGRAM", "SUB_TEAM"])
    .reset_index(drop=True)
)

# ----------------------------
# 3) Table 1 — PROGRAM level (CTD + LSD + SPI/CPI)
# ----------------------------
# CTD (fiscal-year-to-date) for BCWS/BCWP/ACWP
bcws_ctd_p = _sum_ctd(d, "BCWS", LSD_END, ["PROGRAM"], start=FY_START)
bcwp_ctd_p = _sum_ctd(d, "BCWP", LSD_END, ["PROGRAM"], start=FY_START)
acwp_ctd_p = _sum_ctd(d, "ACWP", LSD_END, ["PROGRAM"], start=FY_START)

# LSD window sums for BCWS/BCWP/ACWP
bcws_lsd_p = _sum_window(d, "BCWS", LSD_START, LSD_END, ["PROGRAM"]).rename(columns={"BCWS_SUM": "BCWS_LSD"})
bcwp_lsd_p = _sum_window(d, "BCWP", LSD_START, LSD_END, ["PROGRAM"]).rename(columns={"BCWP_SUM": "BCWP_LSD"})
acwp_lsd_p = _sum_window(d, "ACWP", LSD_START, LSD_END, ["PROGRAM"]).rename(columns={"ACWP_SUM": "ACWP_LSD"})

t1 = base_program.copy()
t1 = _left_merge(t1, bcws_ctd_p, ["PROGRAM"])
t1 = _left_merge(t1, bcwp_ctd_p, ["PROGRAM"])
t1 = _left_merge(t1, acwp_ctd_p, ["PROGRAM"])
t1 = _left_merge(t1, bcws_lsd_p, ["PROGRAM"])
t1 = _left_merge(t1, bcwp_lsd_p, ["PROGRAM"])
t1 = _left_merge(t1, acwp_lsd_p, ["PROGRAM"])

t1 = _fill_zero(t1, ["BCWS_CTD", "BCWP_CTD", "ACWP_CTD", "BCWS_LSD", "BCWP_LSD", "ACWP_LSD"])

t1["SPI_CTD"] = _safe_ratio(t1["BCWP_CTD"], t1["BCWS_CTD"])
t1["CPI_CTD"] = _safe_ratio(t1["BCWP_CTD"], t1["ACWP_CTD"])
t1["SPI_LSD"] = _safe_ratio(t1["BCWP_LSD"], t1["BCWS_LSD"])
t1["CPI_LSD"] = _safe_ratio(t1["BCWP_LSD"], t1["ACWP_LSD"])

# Flags (debug-friendly)
t1["no_BCWS_CTD"] = t1["BCWS_CTD"].eq(0)
t1["no_BCWP_CTD"] = t1["BCWP_CTD"].eq(0)
t1["no_ACWP_CTD"] = t1["ACWP_CTD"].eq(0)
t1["no_BCWS_LSD"] = t1["BCWS_LSD"].eq(0)
t1["no_BCWP_LSD"] = t1["BCWP_LSD"].eq(0)
t1["no_ACWP_LSD"] = t1["ACWP_LSD"].eq(0)

t1_program_evms = t1.copy()


# ----------------------------
# 4) Table 2 — PROGRAM + SUB_TEAM (SPI/CPI CTD + LSD)
# ----------------------------
bcws_ctd_ps = _sum_ctd(d, "BCWS", LSD_END, ["PROGRAM", "SUB_TEAM"], start=FY_START)
bcwp_ctd_ps = _sum_ctd(d, "BCWP", LSD_END, ["PROGRAM", "SUB_TEAM"], start=FY_START)
acwp_ctd_ps = _sum_ctd(d, "ACWP", LSD_END, ["PROGRAM", "SUB_TEAM"], start=FY_START)

bcws_lsd_ps = _sum_window(d, "BCWS", LSD_START, LSD_END, ["PROGRAM", "SUB_TEAM"]).rename(columns={"BCWS_SUM": "BCWS_LSD"})
bcwp_lsd_ps = _sum_window(d, "BCWP", LSD_START, LSD_END, ["PROGRAM", "SUB_TEAM"]).rename(columns={"BCWP_SUM": "BCWP_LSD"})
acwp_lsd_ps = _sum_window(d, "ACWP", LSD_START, LSD_END, ["PROGRAM", "SUB_TEAM"]).rename(columns={"ACWP_SUM": "ACWP_LSD"})

t2 = base_prog_sub.copy()
t2 = _left_merge(t2, bcws_ctd_ps, ["PROGRAM", "SUB_TEAM"])
t2 = _left_merge(t2, bcwp_ctd_ps, ["PROGRAM", "SUB_TEAM"])
t2 = _left_merge(t2, acwp_ctd_ps, ["PROGRAM", "SUB_TEAM"])
t2 = _left_merge(t2, bcws_lsd_ps, ["PROGRAM", "SUB_TEAM"])
t2 = _left_merge(t2, bcwp_lsd_ps, ["PROGRAM", "SUB_TEAM"])
t2 = _left_merge(t2, acwp_lsd_ps, ["PROGRAM", "SUB_TEAM"])

t2 = _fill_zero(t2, ["BCWS_CTD", "BCWP_CTD", "ACWP_CTD", "BCWS_LSD", "BCWP_LSD", "ACWP_LSD"])

t2["SPI_CTD"] = _safe_ratio(t2["BCWP_CTD"], t2["BCWS_CTD"])
t2["CPI_CTD"] = _safe_ratio(t2["BCWP_CTD"], t2["ACWP_CTD"])
t2["SPI_LSD"] = _safe_ratio(t2["BCWP_LSD"], t2["BCWS_LSD"])
t2["CPI_LSD"] = _safe_ratio(t2["BCWP_LSD"], t2["ACWP_LSD"])

t2["no_BCWS_LSD"] = t2["BCWS_LSD"].eq(0)
t2["no_ACWP_LSD"] = t2["ACWP_LSD"].eq(0)

t2_prog_subteam_spi_cpi = t2.copy()


# ----------------------------
# 5) Table 3 — PROGRAM + SUB_TEAM (BAC, EAC, VAC)
# ----------------------------
# Try common cost set labels. If your file uses different ones, add them here.
# (We’ll try BAC/EAC first; if missing, you’ll just get zeros + debug)
bac_ps = _asof_latest_value(d, "BAC", LSD_END, ["PROGRAM", "SUB_TEAM"])
eac_ps = _asof_latest_value(d, "EAC", LSD_END, ["PROGRAM", "SUB_TEAM"])

t3 = base_prog_sub.copy()
t3 = _left_merge(t3, bac_ps, ["PROGRAM", "SUB_TEAM"])
t3 = _left_merge(t3, eac_ps, ["PROGRAM", "SUB_TEAM"])

# Rename to requested names
if "BAC_ASOF" in t3.columns: t3.rename(columns={"BAC_ASOF": "BAC_HRS"}, inplace=True)
else: t3["BAC_HRS"] = 0.0
if "EAC_ASOF" in t3.columns: t3.rename(columns={"EAC_ASOF": "EAC_HRS"}, inplace=True)
else: t3["EAC_HRS"] = 0.0

t3 = _fill_zero(t3, ["BAC_HRS", "EAC_HRS"])
t3["VAC_HRS"] = t3["BAC_HRS"] - t3["EAC_HRS"]

# Optional: ETC as-of LSD for debugging only (NOT required for your dashboard, but helps trace gaps)
etc_asof = _asof_latest_value(d, "ETC", LSD_END, ["PROGRAM", "SUB_TEAM"])
t3 = _left_merge(t3, etc_asof, ["PROGRAM", "SUB_TEAM"])
if "ETC_ASOF" in t3.columns:
    t3.rename(columns={"ETC_ASOF": "ETC_ASOF_LSD"}, inplace=True)
else:
    t3["ETC_ASOF_LSD"] = 0.0
t3 = _fill_zero(t3, ["ETC_ASOF_LSD"])

t3["no_BAC"] = t3["BAC_HRS"].eq(0)
t3["no_EAC"] = t3["EAC_HRS"].eq(0)

t3_prog_subteam_bac_eac_vac = t3.copy()


# ----------------------------
# 6) Table 4 — PROGRAM (Demand/Actual/%Var + NextMo BCWS + NextMo ETC)
# ----------------------------
# Demand Hours LSD = BCWS_LSD ; Actual Hours LSD = ACWP_LSD
t4 = t1_program_evms[["PROGRAM", "BCWS_LSD", "ACWP_LSD"]].copy()
t4.rename(columns={"BCWS_LSD": "Demand_Hours_LSD", "ACWP_LSD": "Actual_Hours_LSD"}, inplace=True)
t4["PctVar_LSD"] = np.where(
    t4["Demand_Hours_LSD"].eq(0),
    0.0,
    (t4["Actual_Hours_LSD"] - t4["Demand_Hours_LSD"]) / t4["Demand_Hours_LSD"]
)

next_bcws = _sum_window(d, "BCWS", NEXT_START, NEXT_END, ["PROGRAM"]).rename(columns={"BCWS_SUM": "NextMo_BCWS_Hours"})
next_etc  = _sum_window(d, "ETC",  NEXT_START, NEXT_END, ["PROGRAM"]).rename(columns={"ETC_SUM": "NextMo_ETC_Hours"})

t4 = _left_merge(t4, next_bcws, ["PROGRAM"])
t4 = _left_merge(t4, next_etc,  ["PROGRAM"])
t4 = _fill_zero(t4, ["NextMo_BCWS_Hours", "NextMo_ETC_Hours"])

t4_program_demand_actual_next = t4.copy()


# ----------------------------
# 7) DEBUG TABLES (to trace “missing”)
# ----------------------------
# Counts by PROGRAM/SUB_TEAM/COST_SET within LSD window (helps prove data exists or not)
dbg_counts_lsd = (
    d[d["DATE"].between(LSD_START, LSD_END, inclusive="both")]
    .groupby(["PROGRAM", "SUB_TEAM", "COST_SET"], as_index=False)
    .agg(rows=("HOURS", "size"), hours=("HOURS", "sum"), min_date=("DATE","min"), max_date=("DATE","max"))
    .sort_values(["PROGRAM","SUB_TEAM","COST_SET"])
)

# Where are we getting zeros for key LSD components?
dbg_missing_lsd = t2_prog_subteam_spi_cpi.loc[
    (t2_prog_subteam_spi_cpi["BCWS_LSD"].eq(0)) | (t2_prog_subteam_spi_cpi["ACWP_LSD"].eq(0)),
    ["PROGRAM","SUB_TEAM","BCWS_LSD","ACWP_LSD","BCWP_LSD","SPI_LSD","CPI_LSD","no_BCWS_LSD","no_ACWP_LSD"]
].copy()

print("\n--- DEBUG SUMMARY (counts) ---")
print("Bad rows dropped (DATE/HOURS missing):", len(bad_rows))
print("Program/SubTeam rows with no LSD Demand (BCWS_LSD==0):", int(t2_prog_subteam_spi_cpi["BCWS_LSD"].eq(0).sum()))
print("Program/SubTeam rows with no LSD Actual (ACWP_LSD==0):", int(t2_prog_subteam_spi_cpi["ACWP_LSD"].eq(0).sum()))
print("Program/SubTeam rows with BAC missing (BAC_HRS==0):", int(t3_prog_subteam_bac_eac_vac["BAC_HRS"].eq(0).sum()))
print("Program/SubTeam rows with EAC missing (EAC_HRS==0):", int(t3_prog_subteam_bac_eac_vac["EAC_HRS"].eq(0).sum()))


# ----------------------------
# 8) WRITE ONE EXCEL FILE (PowerBI-friendly)
# ----------------------------
out_path = "EVMS_Metrics_PowerBI.xlsx"
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    # Required 4 tables
    t1_program_evms.sort_values(["PROGRAM"]).to_excel(writer, sheet_name="T1_Program_EVM", index=False)
    t2_prog_subteam_spi_cpi.sort_values(["PROGRAM","SUB_TEAM"]).to_excel(writer, sheet_name="T2_Prog_SubTeam_SPI_CPI", index=False)
    t3_prog_subteam_bac_eac_vac.sort_values(["PROGRAM","SUB_TEAM"]).to_excel(writer, sheet_name="T3_Prog_SubTeam_BAC_EAC_VAC", index=False)
    t4_program_demand_actual_next.sort_values(["PROGRAM"]).to_excel(writer, sheet_name="T4_Program_Demand_Actual_Next", index=False)

    # Debug tabs
    bad_rows.to_excel(writer, sheet_name="DEBUG_BadRows", index=False)
    dbg_counts_lsd.to_excel(writer, sheet_name="DEBUG_LSD_Counts", index=False)
    dbg_missing_lsd.to_excel(writer, sheet_name="DEBUG_LSD_ZeroRows", index=False)

print(f"\n✅ Wrote: {out_path}")
print("Tables created: t1_program_evms, t2_prog_subteam_spi_cpi, t3_prog_subteam_bac_eac_vac, t4_program_demand_actual_next")