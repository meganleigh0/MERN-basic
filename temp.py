import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# =========================
# CONFIG
# =========================
PROGRAMS_KEEP = {"ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"}  # adjust if needed
OUT_XLSX = "EVMS_Metrics_PowerBI.xlsx"

# Placeholder LSD end = 2 weeks prior to today (local)
today = pd.Timestamp.today().normalize()
LSD_END = today - pd.Timedelta(days=14)      # placeholder
LSD_START = LSD_END - pd.Timedelta(days=13)  # 14-day window inclusive
NEXT_START = LSD_END + pd.Timedelta(days=1)
NEXT_END = LSD_END + pd.Timedelta(days=28)   # ~4 weeks "next period"

# Fiscal-year start (calendar FY). If GDLS FY differs, change this.
FY_START = pd.Timestamp(year=LSD_END.year, month=1, day=1)

print(f"Using placeholder LSD_END: {LSD_END.date()}")
print(f"LSD window: {LSD_START.date()} to {LSD_END.date()}")
print(f"Next window: {NEXT_START.date()} to {NEXT_END.date()}")
print(f"FY start (CTD): {FY_START.date()}")

# =========================
# HELPERS
# =========================
def _norm_str(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.strip()
         .str.replace(r"\s+", "_", regex=True)
         .str.replace(r"_+", "_", regex=True)
         .str.upper()
    )

def safe_div(numer, denom):
    numer = np.asarray(numer, dtype="float64")
    denom = np.asarray(denom, dtype="float64")
    out = np.zeros_like(numer, dtype="float64")
    mask = denom != 0
    out[mask] = numer[mask] / denom[mask]
    out[~np.isfinite(out)] = 0.0
    return out

def sum_hours(df, mask, group_cols):
    if df.empty:
        return pd.DataFrame(columns=group_cols + ["HOURS_SUM"])
    tmp = df.loc[mask, group_cols + ["HOURS"]].copy()
    if tmp.empty:
        return pd.DataFrame(columns=group_cols + ["HOURS_SUM"])
    return tmp.groupby(group_cols, as_index=False)["HOURS"].sum().rename(columns={"HOURS": "HOURS_SUM"})

def make_complete_grid(df, group_cols):
    # Ensures every PROGRAM (and SUB_TEAM if included) appears even if some cost-sets are missing in a window
    vals = [sorted(df[c].dropna().unique().tolist()) for c in group_cols]
    if any(len(v) == 0 for v in vals):
        return pd.DataFrame(columns=group_cols)
    mi = pd.MultiIndex.from_product(vals, names=group_cols)
    return mi.to_frame(index=False)

# =========================
# 1) CLEAN INPUT
# =========================
df = cobra_merged_df.copy()

# Standardize expected columns (edit here if your raw headers differ)
rename_map = {}
for c in df.columns:
    cu = c.strip().upper()
    if cu in ["COST-SET", "COST_SET", "COSTSET"]:
        rename_map[c] = "COST_SET"
    elif cu in ["SUB TEAM", "SUB_TEAM", "SUBTEAM"]:
        rename_map[c] = "SUB_TEAM"
    elif cu in ["PROGRAM", "PROG"]:
        rename_map[c] = "PROGRAM"
    elif cu in ["DATE", "PERIOD", "STATUS_DATE"]:
        rename_map[c] = "DATE"
    elif cu in ["HOURS", "HRS"]:
        rename_map[c] = "HOURS"
df = df.rename(columns=rename_map)

required = {"PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"}
missing_cols = required - set(df.columns)
if missing_cols:
    raise ValueError(f"cobra_merged_df is missing required columns: {missing_cols}. "
                     f"Found: {list(df.columns)}")

# Normalize key dimensions
df["PROGRAM"] = _norm_str(df["PROGRAM"])
df["SUB_TEAM"] = _norm_str(df["SUB_TEAM"]).replace({"NAN": np.nan, "NONE": np.nan})
df["COST_SET"] = _norm_str(df["COST_SET"])

# Keep only the 4 programs for now
df = df[df["PROGRAM"].isin(PROGRAMS_KEEP)].copy()

# Parse DATE safely
df["DATE_RAW"] = df["DATE"]
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

# Coerce HOURS numeric
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

# Identify truly bad rows for timephased calcs (DATE or HOURS missing)
bad_rows = df[df["DATE"].isna() | df["HOURS"].isna()].copy()
bad_rows["BAD_REASON"] = np.where(df["DATE"].isna(), "DATE_NaT", "HOURS_NaN")

# Drop bad rows for all time-window computations
df = df.dropna(subset=["DATE", "HOURS"]).copy()

# If SUB_TEAM missing, keep but label so you don't lose dollars/hours
df["SUB_TEAM"] = df["SUB_TEAM"].fillna("UNKNOWN")

print("\n--- CLEANING SUMMARY ---")
print(f"Rows kept for calc: {len(df):,}")
print(f"Rows dropped (bad DATE or HOURS): {len(bad_rows):,}")
if len(bad_rows):
    print("Top bad PROGRAM/SUB_TEAM:")
    print(bad_rows.groupby(["PROGRAM","SUB_TEAM"]).size().sort_values(ascending=False).head(10))

# Optional: sanity check cost sets present
print("\nCost sets present (top 20):")
print(df["COST_SET"].value_counts().head(20))

# =========================
# 2) DEFINE COST SETS WE CARE ABOUT
# =========================
# Adjust these if your export uses different labels
CS_BCWS = "BCWS"
CS_BCWP = "BCWP"
CS_ACWP = "ACWP"
CS_ETC  = "ETC"

# Filter to only cost sets needed for requested outputs
df = df[df["COST_SET"].isin([CS_BCWS, CS_BCWP, CS_ACWP, CS_ETC])].copy()

# =========================
# 3) WINDOW MASKS
# =========================
m_ctd = (df["DATE"] >= FY_START) & (df["DATE"] <= LSD_END)
m_lsd = (df["DATE"] >= LSD_START) & (df["DATE"] <= LSD_END)
m_next = (df["DATE"] >= NEXT_START) & (df["DATE"] <= NEXT_END)
m_future = df["DATE"] > LSD_END  # for ETC remaining

# =========================
# 4) TABLE 1: PROGRAM summary (CTD/LSD SPI/CPI + base hours)
# =========================
g_prog = ["PROGRAM"]
grid_prog = make_complete_grid(df, g_prog)

def prog_costsum(cost_set, mask):
    return sum_hours(df, (df["COST_SET"] == cost_set) & mask, g_prog).rename(columns={"HOURS_SUM": f"{cost_set}_SUM"})

# CTD
p_bcws_ctd = prog_costsum(CS_BCWS, m_ctd).rename(columns={f"{CS_BCWS}_SUM": "BCWS_CTD"})
p_bcwp_ctd = prog_costsum(CS_BCWP, m_ctd).rename(columns={f"{CS_BCWP}_SUM": "BCWP_CTD"})
p_acwp_ctd = prog_costsum(CS_ACWP, m_ctd).rename(columns={f"{CS_ACWP}_SUM": "ACWP_CTD"})

# LSD
p_bcws_lsd = prog_costsum(CS_BCWS, m_lsd).rename(columns={f"{CS_BCWS}_SUM": "BCWS_LSD"})
p_bcwp_lsd = prog_costsum(CS_BCWP, m_lsd).rename(columns={f"{CS_BCWP}_SUM": "BCWP_LSD"})
p_acwp_lsd = prog_costsum(CS_ACWP, m_lsd).rename(columns={f"{CS_ACWP}_SUM": "ACWP_LSD"})

t1 = grid_prog.merge(p_bcws_ctd, on=g_prog, how="left") \
              .merge(p_bcwp_ctd, on=g_prog, how="left") \
              .merge(p_acwp_ctd, on=g_prog, how="left") \
              .merge(p_bcws_lsd, on=g_prog, how="left") \
              .merge(p_bcwp_lsd, on=g_prog, how="left") \
              .merge(p_acwp_lsd, on=g_prog, how="left")

for c in ["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]:
    t1[c] = t1[c].fillna(0.0)

t1["SPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["BCWS_CTD"])
t1["CPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["ACWP_CTD"])
t1["SPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["BCWS_LSD"])
t1["CPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["ACWP_LSD"])

# =========================
# 5) TABLE 2: PROGRAM + SUB_TEAM (SPI/CPI CTD & LSD + base hours)
# =========================
g_ps = ["PROGRAM", "SUB_TEAM"]
grid_ps = make_complete_grid(df, g_ps)

def ps_costsum(cost_set, mask, outname):
    return sum_hours(df, (df["COST_SET"] == cost_set) & mask, g_ps).rename(columns={"HOURS_SUM": outname})

t2 = grid_ps \
    .merge(ps_costsum(CS_BCWS, m_ctd, "BCWS_CTD"), on=g_ps, how="left") \
    .merge(ps_costsum(CS_BCWP, m_ctd, "BCWP_CTD"), on=g_ps, how="left") \
    .merge(ps_costsum(CS_ACWP, m_ctd, "ACWP_CTD"), on=g_ps, how="left") \
    .merge(ps_costsum(CS_BCWS, m_lsd, "BCWS_LSD"), on=g_ps, how="left") \
    .merge(ps_costsum(CS_BCWP, m_lsd, "BCWP_LSD"), on=g_ps, how="left") \
    .merge(ps_costsum(CS_ACWP, m_lsd, "ACWP_LSD"), on=g_ps, how="left")

for c in ["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]:
    t2[c] = t2[c].fillna(0.0)

t2["SPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["BCWS_CTD"])
t2["CPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["ACWP_CTD"])
t2["SPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["BCWS_LSD"])
t2["CPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["ACWP_LSD"])

# =========================
# 6) TABLE 3: PROGRAM + SUB_TEAM (BAC/EAC/VAC)
#    - BAC = total BCWS across all dates
#    - ETC_remaining = sum ETC after LSD_END
#    - EAC = ACWP_CTD + ETC_remaining
#    - VAC = BAC - EAC
# =========================
bac = sum_hours(df, df["COST_SET"].eq(CS_BCWS), g_ps).rename(columns={"HOURS_SUM": "BAC_HRS"})
etc_remaining = sum_hours(df, df["COST_SET"].eq(CS_ETC) & m_future, g_ps).rename(columns={"HOURS_SUM": "ETC_REMAINING"})
acwp_ctd_ps = sum_hours(df, df["COST_SET"].eq(CS_ACWP) & m_ctd, g_ps).rename(columns={"HOURS_SUM": "ACWP_CTD"})

t3 = grid_ps.merge(bac, on=g_ps, how="left") \
            .merge(acwp_ctd_ps, on=g_ps, how="left") \
            .merge(etc_remaining, on=g_ps, how="left")

t3[["BAC_HRS","ACWP_CTD","ETC_REMAINING"]] = t3[["BAC_HRS","ACWP_CTD","ETC_REMAINING"]].fillna(0.0)
t3["EAC_HRS"] = t3["ACWP_CTD"] + t3["ETC_REMAINING"]
t3["VAC_HRS"] = t3["BAC_HRS"] - t3["EAC_HRS"]

# Debug flag: no ETC remaining (could be legit for closed work)
t3["no_ETC_remaining"] = t3["ETC_REMAINING"].eq(0)

# =========================
# 7) TABLE 4: PROGRAM Demand/Actual/%Var + Next Mo BCWS/ETC
# =========================
p_demand_lsd = sum_hours(df, df["COST_SET"].eq(CS_BCWS) & m_lsd, g_prog).rename(columns={"HOURS_SUM": "Demand_Hours_LSD"})
p_actual_lsd = sum_hours(df, df["COST_SET"].eq(CS_ACWP) & m_lsd, g_prog).rename(columns={"HOURS_SUM": "Actual_Hours_LSD"})
p_next_bcws = sum_hours(df, df["COST_SET"].eq(CS_BCWS) & m_next, g_prog).rename(columns={"HOURS_SUM": "NextMo_BCWS_Hours"})
p_next_etc  = sum_hours(df, df["COST_SET"].eq(CS_ETC)  & m_next, g_prog).rename(columns={"HOURS_SUM": "NextMo_ETC_Hours"})

t4 = grid_prog.merge(p_demand_lsd, on=g_prog, how="left") \
              .merge(p_actual_lsd, on=g_prog, how="left") \
              .merge(p_next_bcws, on=g_prog, how="left") \
              .merge(p_next_etc,  on=g_prog, how="left")

for c in ["Demand_Hours_LSD","Actual_Hours_LSD","NextMo_BCWS_Hours","NextMo_ETC_Hours"]:
    t4[c] = t4[c].fillna(0.0)

t4["PctVar_LSD"] = np.where(
    t4["Demand_Hours_LSD"].to_numpy() == 0,
    0.0,
    (t4["Actual_Hours_LSD"] - t4["Demand_Hours_LSD"]) / t4["Demand_Hours_LSD"]
)

# =========================
# 8) COVERAGE / DEBUG TABLES (so we can trace remaining gaps)
# =========================
# Which PROGRAM/SUB_TEAM combos have 0 demand or 0 actual in LSD?
dbg_ps = t2[["PROGRAM","SUB_TEAM","BCWS_LSD","ACWP_LSD","BCWS_CTD","ACWP_CTD"]].copy()
dbg_ps["no_LSD_Demand"] = dbg_ps["BCWS_LSD"].eq(0)
dbg_ps["no_LSD_Actual"] = dbg_ps["ACWP_LSD"].eq(0)

# Which combos have no future ETC remaining (affects EAC/VAC)
dbg_eac = t3[["PROGRAM","SUB_TEAM","BAC_HRS","ACWP_CTD","ETC_REMAINING","EAC_HRS","VAC_HRS","no_ETC_remaining"]].copy()

print("\n--- DEBUG SUMMARY (counts) ---")
print(f"Program/SubTeam rows with no LSD Demand (BCWS_LSD==0): {int(dbg_ps['no_LSD_Demand'].sum())}")
print(f"Program/SubTeam rows with no LSD Actual (ACWP_LSD==0): {int(dbg_ps['no_LSD_Actual'].sum())}")
print(f"Program/SubTeam rows with no ETC remaining (ETC_REMAINING==0): {int(dbg_eac['no_ETC_remaining'].sum())}")

# =========================
# 9) FINAL FORMATTING / SORT
# =========================
t1 = t1.sort_values(["PROGRAM"]).reset_index(drop=True)
t2 = t2.sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
t3 = t3.sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
t4 = t4.sort_values(["PROGRAM"]).reset_index(drop=True)

# =========================
# 10) WRITE EXCEL (Power BI friendly)
# =========================
with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
    t1.to_excel(writer, sheet_name="T1_Program_EVM", index=False)
    t2.to_excel(writer, sheet_name="T2_Prog_SubTeam_SPI_CPI", index=False)
    t3.to_excel(writer, sheet_name="T3_Prog_SubTeam_BAC_EAC_VAC", index=False)
    t4.to_excel(writer, sheet_name="T4_Program_Demand_Actual_Next", index=False)

    # Debug tabs
    bad_rows.to_excel(writer, sheet_name="DEBUG_BadRows_Dropped", index=False)
    dbg_ps.to_excel(writer, sheet_name="DEBUG_Prog_SubTeam_Coverage", index=False)
    dbg_eac.to_excel(writer, sheet_name="DEBUG_EAC_ETC_Remaining", index=False)

print(f"\n✅ Wrote: {OUT_XLSX}")
print("Sheets: T1, T2, T3, T4 + DEBUG_*")