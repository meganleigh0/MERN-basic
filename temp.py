import pandas as pd
import numpy as np

# =========================
# CONFIG (edit if you want)
# =========================
PROGRAMS_KEEP = ["ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"]  # comment out if you want all programs

# Placeholder Last Status Date (LSD): 2 weeks prior to "today" (based on your machine clock)
TODAY = pd.Timestamp.today().normalize()
LSD_END   = (TODAY - pd.Timedelta(days=14)).normalize()
LSD_START = (LSD_END - pd.Timedelta(days=13)).normalize()          # 14-day LSD window
NEXT_START = (LSD_END + pd.Timedelta(days=1)).normalize()
NEXT_END   = (NEXT_START + pd.Timedelta(days=27)).normalize()      # 28-day "next month" window

# Defaults when denominator is 0 (prevents missing values)
DEFAULT_SPI_WHEN_BCWS_ZERO = 1.0   # If no scheduled work, treat SPI as neutral (1.0)
DEFAULT_CPI_WHEN_ACWP_ZERO = 1.0   # If no actuals, treat CPI as neutral (1.0)

OUT_XLSX = "EVMS_Metrics_PowerBI.xlsx"

print(f"Using placeholder LSD_END: {LSD_END.date()}")
print(f"LSD window: {LSD_START.date()} to {LSD_END.date()}")
print(f"Next window: {NEXT_START.date()} to {NEXT_END.date()}")

# =========================
# 0) BASIC CLEANUP
# =========================
df = cobra_merged_df.copy()

# Required columns check (add/remove as needed)
required_cols = ["PROGRAM", "SUB_TEAM", "COST-SET", "DATE", "HOURS"]
missing_required = [c for c in required_cols if c not in df.columns]
if missing_required:
    raise ValueError(f"cobra_merged_df is missing required columns: {missing_required}")

# Normalize text keys
def norm_str(s):
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", " ", regex=True)
         .str.upper()
    )

df["PROGRAM"] = norm_str(df["PROGRAM"])
df["SUB_TEAM"] = df["SUB_TEAM"].fillna("UNASSIGNED")
df["SUB_TEAM"] = norm_str(df["SUB_TEAM"])

# Parse DATE
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
if df["DATE"].isna().any():
    bad = df[df["DATE"].isna()].head(20)
    raise ValueError(f"Found non-parseable DATE values. Sample rows:\n{bad}")

# Ensure HOURS numeric
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0.0)

# Keep only your 4 programs if desired
if PROGRAMS_KEEP:
    df = df[df["PROGRAM"].isin([p.upper() for p in PROGRAMS_KEEP])].copy()

# =========================
# 1) COST-SET NORMALIZATION (NO UNKNOWNS)
# =========================
raw_cs = norm_str(df["COST-SET"].fillna(""))

# A robust mapping that collapses Cobra variants into the handful we need.
# If you see new variants, add them here.
cost_set_map = {
    # BCWS (Demand / Budgeted work scheduled)
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    # BCWP (Earned value)
    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    # ACWP (Actuals)
    "ACWP": "ACWP",
    "ACWP_HRS": "ACWP",

    # ETC
    "ETC": "ETC",
    "EAC": "EAC",        # keep separate if present
    "ACTUALS": "ACWP",   # sometimes "Actuals" shows up
}

df["COST_SET_NORM"] = raw_cs.map(cost_set_map)

unknown_mask = df["COST_SET_NORM"].isna()
if unknown_mask.any():
    unknown_vals = raw_cs[unknown_mask].value_counts().head(50)
    raise ValueError(
        "Found COST-SET values that are not mapped (would create unknowns). "
        "Add them to cost_set_map.\n\nTop unknown COST-SET values:\n"
        f"{unknown_vals.to_string()}"
    )

# =========================
# 2) HELPERS
# =========================
def safe_div(numer, denom, default_when_zero=0.0):
    numer = np.asarray(numer, dtype="float64")
    denom = np.asarray(denom, dtype="float64")
    out = np.empty_like(numer, dtype="float64")
    zero = denom == 0
    out[~zero] = numer[~zero] / denom[~zero]
    out[zero] = default_when_zero
    return out

def sum_hours(d, costset_norm, start=None, end=None, group_cols=("PROGRAM",), asof_end=False):
    """
    If asof_end=True: sums HOURS for costset_norm where DATE <= end (ignores start)
    Else: sums HOURS where start <= DATE <= end (inclusive) if start/end provided.
    """
    m = (d["COST_SET_NORM"] == costset_norm)
    if asof_end:
        if end is None:
            raise ValueError("asof_end=True requires end=")
        m &= (d["DATE"] <= end)
    else:
        if start is not None:
            m &= (d["DATE"] >= start)
        if end is not None:
            m &= (d["DATE"] <= end)

    g = (
        d.loc[m, list(group_cols) + ["HOURS"]]
         .groupby(list(group_cols), dropna=False, as_index=False)["HOURS"]
         .sum()
         .rename(columns={"HOURS": f"{costset_norm}_HRS"})
    )
    return g

def merge_metric(base, metric_df, on_cols):
    out = base.merge(metric_df, on=list(on_cols), how="left")
    return out

# Fiscal Year start based on placeholder LSD year (you can change to match calendar rules)
FY_START = pd.Timestamp(year=LSD_END.year, month=1, day=1)

# =========================
# 3) BASE KEYS (ENSURES FULL POPULATION)
# =========================
program_keys = (
    df[["PROGRAM"]]
    .drop_duplicates()
    .sort_values(["PROGRAM"])
    .reset_index(drop=True)
)

prog_sub_keys = (
    df[["PROGRAM", "SUB_TEAM"]]
    .drop_duplicates()
    .sort_values(["PROGRAM", "SUB_TEAM"])
    .reset_index(drop=True)
)

# =========================
# 4) TABLE 1: PROGRAM SPI/CPI (CTD + LSD)
# =========================
t1 = program_keys.copy()

# CTD (as-of LSD_END)
bcws_ctd = sum_hours(df, "BCWS", end=LSD_END, group_cols=("PROGRAM",), asof_end=True).rename(columns={"BCWS_HRS":"BCWS_CTD"})
bcwp_ctd = sum_hours(df, "BCWP", end=LSD_END, group_cols=("PROGRAM",), asof_end=True).rename(columns={"BCWP_HRS":"BCWP_CTD"})
acwp_ctd = sum_hours(df, "ACWP", end=LSD_END, group_cols=("PROGRAM",), asof_end=True).rename(columns={"ACWP_HRS":"ACWP_CTD"})

# LSD (within LSD window)
bcws_lsd = sum_hours(df, "BCWS", start=LSD_START, end=LSD_END, group_cols=("PROGRAM",), asof_end=False).rename(columns={"BCWS_HRS":"BCWS_LSD"})
bcwp_lsd = sum_hours(df, "BCWP", start=LSD_START, end=LSD_END, group_cols=("PROGRAM",), asof_end=False).rename(columns={"BCWP_HRS":"BCWP_LSD"})
acwp_lsd = sum_hours(df, "ACWP", start=LSD_START, end=LSD_END, group_cols=("PROGRAM",), asof_end=False).rename(columns={"ACWP_HRS":"ACWP_LSD"})

for mdf in [bcws_ctd, bcwp_ctd, acwp_ctd, bcws_lsd, bcwp_lsd, acwp_lsd]:
    t1 = merge_metric(t1, mdf, on_cols=("PROGRAM",))

# Fill metric blanks with 0 (keeps keys fully populated)
for c in ["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]:
    t1[c] = t1[c].fillna(0.0)

t1["SPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["BCWS_CTD"], default_when_zero=DEFAULT_SPI_WHEN_BCWS_ZERO)
t1["CPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["ACWP_CTD"], default_when_zero=DEFAULT_CPI_WHEN_ACWP_ZERO)
t1["SPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["BCWS_LSD"], default_when_zero=DEFAULT_SPI_WHEN_BCWS_ZERO)
t1["CPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["ACWP_LSD"], default_when_zero=DEFAULT_CPI_WHEN_ACWP_ZERO)

t1 = t1[
    ["PROGRAM",
     "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
     "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"]
].copy()

# =========================
# 5) TABLE 2: PROGRAM + SUB_TEAM SPI/CPI (CTD + LSD)
# =========================
t2 = prog_sub_keys.copy()

bcws_ctd_ps = sum_hours(df, "BCWS", end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=True).rename(columns={"BCWS_HRS":"BCWS_CTD"})
bcwp_ctd_ps = sum_hours(df, "BCWP", end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=True).rename(columns={"BCWP_HRS":"BCWP_CTD"})
acwp_ctd_ps = sum_hours(df, "ACWP", end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=True).rename(columns={"ACWP_HRS":"ACWP_CTD"})

bcws_lsd_ps = sum_hours(df, "BCWS", start=LSD_START, end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=False).rename(columns={"BCWS_HRS":"BCWS_LSD"})
bcwp_lsd_ps = sum_hours(df, "BCWP", start=LSD_START, end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=False).rename(columns={"BCWP_HRS":"BCWP_LSD"})
acwp_lsd_ps = sum_hours(df, "ACWP", start=LSD_START, end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=False).rename(columns={"ACWP_HRS":"ACWP_LSD"})

for mdf in [bcws_ctd_ps, bcwp_ctd_ps, acwp_ctd_ps, bcws_lsd_ps, bcwp_lsd_ps, acwp_lsd_ps]:
    t2 = merge_metric(t2, mdf, on_cols=("PROGRAM","SUB_TEAM"))

for c in ["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]:
    t2[c] = t2[c].fillna(0.0)

t2["SPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["BCWS_CTD"], default_when_zero=DEFAULT_SPI_WHEN_BCWS_ZERO)
t2["CPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["ACWP_CTD"], default_when_zero=DEFAULT_CPI_WHEN_ACWP_ZERO)
t2["SPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["BCWS_LSD"], default_when_zero=DEFAULT_SPI_WHEN_BCWS_ZERO)
t2["CPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["ACWP_LSD"], default_when_zero=DEFAULT_CPI_WHEN_ACWP_ZERO)

t2 = t2[
    ["PROGRAM","SUB_TEAM",
     "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
     "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"]
].copy()

# =========================
# 6) TABLE 3: BAC / EAC / VAC (PROGRAM + SUB_TEAM)
#     BAC = total BCWS across all dates (life-to-go included)
#     EAC = ACWP_CTD + ETC_ASOF_LSD (ETC as-of LSD_END)
#     VAC = BAC - EAC
# =========================
t3 = prog_sub_keys.copy()

# BAC = total BCWS over entire timeline (no date filter)
bac = sum_hours(df, "BCWS", group_cols=("PROGRAM","SUB_TEAM"), asof_end=False).rename(columns={"BCWS_HRS":"BAC_HRS"})

# ETC as-of LSD_END (use last-known ETC up to LSD_END)
etc_asof = sum_hours(df, "ETC", end=LSD_END, group_cols=("PROGRAM","SUB_TEAM"), asof_end=True).rename(columns={"ETC_HRS":"ETC_ASOF_LSD"})

# ACWP_CTD already computed above for program/subteam
acwp_ctd_only = acwp_ctd_ps.copy()

for mdf in [bac, acwp_ctd_only, etc_asof]:
    t3 = merge_metric(t3, mdf, on_cols=("PROGRAM","SUB_TEAM"))

t3["BAC_HRS"] = t3["BAC_HRS"].fillna(0.0)
t3["ACWP_CTD"] = t3["ACWP_CTD"].fillna(0.0)
t3["ETC_ASOF_LSD"] = t3["ETC_ASOF_LSD"].fillna(0.0)

t3["EAC_HRS"] = t3["ACWP_CTD"] + t3["ETC_ASOF_LSD"]
t3["VAC_HRS"] = t3["BAC_HRS"] - t3["EAC_HRS"]

t3 = t3[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]].copy()

# =========================
# 7) TABLE 4: PROGRAM Demand/Actual/%Var (LSD) + NextMo BCWS + NextMo ETC
# =========================
t4 = program_keys.copy()

demand_lsd = bcws_lsd.rename(columns={"BCWS_LSD":"Demand_Hours_LSD"})
actual_lsd = acwp_lsd.rename(columns={"ACWP_LSD":"Actual_Hours_LSD"})

next_bcws = sum_hours(df, "BCWS", start=NEXT_START, end=NEXT_END, group_cols=("PROGRAM",), asof_end=False).rename(columns={"BCWS_HRS":"NextMo_BCWS_Hours"})
next_etc  = sum_hours(df, "ETC",  start=NEXT_START, end=NEXT_END, group_cols=("PROGRAM",), asof_end=False).rename(columns={"ETC_HRS":"NextMo_ETC_Hours"})

for mdf in [demand_lsd, actual_lsd, next_bcws, next_etc]:
    t4 = merge_metric(t4, mdf, on_cols=("PROGRAM",))

for c in ["Demand_Hours_LSD","Actual_Hours_LSD","NextMo_BCWS_Hours","NextMo_ETC_Hours"]:
    t4[c] = t4[c].fillna(0.0)

# %Var = (Actual - Demand) / Demand ; if Demand=0 => 0 by default
t4["PctVar_LSD"] = safe_div(
    (t4["Actual_Hours_LSD"] - t4["Demand_Hours_LSD"]),
    t4["Demand_Hours_LSD"],
    default_when_zero=0.0
)

t4 = t4[["PROGRAM","Demand_Hours_LSD","Actual_Hours_LSD","PctVar_LSD","NextMo_BCWS_Hours","NextMo_ETC_Hours"]].copy()

# =========================
# 8) DEBUG: TRACE WHAT'S "MISSING" (WHY IT HAPPENS)
#    (You can keep these tabs for transparency in PowerBI)
# =========================
# Rows where LSD demand or actual is 0 (often indicates missing costset rows in that window)
dbg_ps = t2.copy()
dbg_ps["FLAG_NO_LSD_DEMAND"] = (dbg_ps["BCWS_LSD"] == 0).astype(int)
dbg_ps["FLAG_NO_LSD_ACTUAL"] = (dbg_ps["ACWP_LSD"] == 0).astype(int)

# Rows where ETC needed for EAC is 0 (indicates no ETC rows up to LSD_END for that group)
dbg_eac = t3.copy()
dbg_eac["FLAG_NO_ETC_ASOF_LSD"] = (dbg_eac["EAC_HRS"] == dbg_eac["ACWP_CTD"]).astype(int)

debug_missing_groups = (
    dbg_ps.merge(dbg_eac[["PROGRAM","SUB_TEAM","FLAG_NO_ETC_ASOF_LSD"]], on=["PROGRAM","SUB_TEAM"], how="left")
          .fillna({"FLAG_NO_ETC_ASOF_LSD":0})
)
debug_missing_groups = debug_missing_groups[
    (debug_missing_groups["FLAG_NO_LSD_DEMAND"] == 1) |
    (debug_missing_groups["FLAG_NO_LSD_ACTUAL"] == 1) |
    (debug_missing_groups["FLAG_NO_ETC_ASOF_LSD"] == 1)
].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# Coverage summary by costset (date min/max)
coverage = (
    df.groupby("COST_SET_NORM")["DATE"]
      .agg(["min","max","count"])
      .reset_index()
      .sort_values("COST_SET_NORM")
)

print("\n--- DEBUG SUMMARY (counts) ---")
print("Program/SubTeam rows with no LSD Demand (BCWS_LSD==0):", int((dbg_ps["BCWS_LSD"] == 0).sum()))
print("Program/SubTeam rows with no LSD Actual (ACWP_LSD==0):", int((dbg_ps["ACWP_LSD"] == 0).sum()))
print("Program/SubTeam rows with ETC missing-asof-LSD (ETC_ASOF_LSD==0):", int((t3["EAC_HRS"] == t3["ACWP_CTD"]).sum()))
print("\nCost-set coverage:\n", coverage.to_string(index=False))

# =========================
# 9) FINAL SANITY: NO NaNs IN OUTPUT TABLES
# =========================
for name, tab in [("t1",t1),("t2",t2),("t3",t3),("t4",t4)]:
    if tab.isna().any().any():
        nan_cols = tab.columns[tab.isna().any()].tolist()
        raise ValueError(f"{name} still has NaNs in columns: {nan_cols}")

# =========================
# 10) WRITE SINGLE EXCEL FOR POWERBI
# =========================
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    t1.to_excel(writer, index=False, sheet_name="01_Program_SPI_CPI")
    t2.to_excel(writer, index=False, sheet_name="02_Prog_SubTeam_SPI_CPI")
    t3.to_excel(writer, index=False, sheet_name="03_BAC_EAC_VAC")
    t4.to_excel(writer, index=False, sheet_name="04_Demand_Actual_NextMo")

    # Optional debug tabs (remove if you don't want them)
    coverage.to_excel(writer, index=False, sheet_name="DEBUG_CostsetCoverage")
    debug_missing_groups.to_excel(writer, index=False, sheet_name="DEBUG_FlaggedGroups")

print(f"\n✅ Wrote: {OUT_XLSX}")
print("Tables created: t1, t2, t3, t4 (plus debug tabs).")
display(t1)
display(t2.head(50))
display(t3.head(50))
display(t4)