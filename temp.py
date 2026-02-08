import pandas as pd
import numpy as np

# =========================
# EVMS METRICS PIPELINE (1 cell)
# Assumes cobra_merged_df exists with columns:
# PROGRAM, SUB_TEAM, COST-SET, PLUG, DATE, HOURS
# =========================

# ---------- helpers ----------
def _norm_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.upper()

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce").fillna(0.0)
    d = pd.to_numeric(d, errors="coerce").fillna(0.0)
    out = np.zeros(len(n), dtype=float)
    m = d.to_numpy() != 0
    out[m] = (n.to_numpy()[m] / d.to_numpy()[m])
    return out

def _sum_hours(df: pd.DataFrame, costset: str, start: pd.Timestamp, end: pd.Timestamp, by_cols: list[str]) -> pd.DataFrame:
    x = df[(df["DATE"] >= start) & (df["DATE"] <= end) & (df["COST_SET"] == costset)]
    if x.empty:
        return pd.DataFrame(columns=by_cols + [costset])
    return (x.groupby(by_cols, as_index=False)["HOURS"].sum().rename(columns={"HOURS": costset}))

def _latest_hours(df: pd.DataFrame, costset: str, end: pd.Timestamp, by_cols: list[str]) -> pd.DataFrame:
    # Use LAST value (by DATE) up to end (ETC/EAC are often point-in-time “as of” values)
    x = df[(df["DATE"] <= end) & (df["COST_SET"] == costset)].sort_values("DATE")
    if x.empty:
        return pd.DataFrame(columns=by_cols + [costset])
    return (x.groupby(by_cols, as_index=False).tail(1)[by_cols + ["HOURS"]]
              .rename(columns={"HOURS": costset}))

def _ensure_keys(base: pd.DataFrame, metrics: pd.DataFrame, on: list[str]) -> pd.DataFrame:
    out = base.merge(metrics, on=on, how="left")
    return out

# ---------- 0) prep / hard rules ----------
d = cobra_merged_df.copy()

# Standardize column names we rely on
# (If your source already uses exact names, this is harmless)
d.columns = [c.strip().upper().replace(" ", "_") for c in d.columns]

required = {"PROGRAM", "SUB_TEAM", "COST-SET", "PLUG", "DATE", "HOURS"}
missing_cols = required - set(d.columns)
if missing_cols:
    raise ValueError(f"cobra_merged_df is missing required columns: {sorted(missing_cols)}")

d = d.rename(columns={"COST-SET": "COST_SET"})

# Parse / normalize
d["PROGRAM"] = _norm_str(d["PROGRAM"])
d["SUB_TEAM"] = _norm_str(d["SUB_TEAM"])
d["COST_SET"] = _norm_str(d["COST_SET"])
d["PLUG"] = _norm_str(d["PLUG"])
d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
d["HOURS"] = pd.to_numeric(d["HOURS"], errors="coerce").fillna(0.0)

# Filter to HOUR plug rows (your screenshots show PLUG == HOURS)
d = d[d["PLUG"].eq("HOURS")].copy()

# Collapse cost-set variants (NO unknowns / no NaNs introduced — any unmapped keeps original)
cost_set_map = {
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    "ACWP": "ACWP",
    "ACWP_HRS": "ACWP",
    "ACTUALS": "ACWP",

    "ETC": "ETC",
    "EAC": "ETC",  # if EAC appears as a cost-set, treat it as ETC-style forecast input
}
d["COST_SET"] = d["COST_SET"].map(cost_set_map).fillna(d["COST_SET"])

# Hard stop if DATE has nulls after parsing (these cause “missing metrics” silently)
bad_dates = d["DATE"].isna().sum()
if bad_dates:
    raise ValueError(f"{bad_dates} rows have invalid DATE after parsing. Fix DATE before computing EVMS.")

# ---------- 1) define periods (placeholder LSD = 2 weeks prior to today) ----------
TODAY = pd.Timestamp.today().normalize()
LSD = TODAY - pd.Timedelta(days=14)                      # placeholder
LSD_START = LSD - pd.Timedelta(days=13)                  # 2-week LSD window (14 days inclusive)
CTD_START = pd.Timestamp.min                             # contract-to-date (all history up to LSD)

NEXT_START = LSD + pd.Timedelta(days=1)                  # next period window for “NextMo”
NEXT_END = LSD + pd.Timedelta(days=28)                   # ~4 weeks; adjust if you want calendar month boundaries

print("Using placeholder LSD:", LSD.date())
print("LSD window:", LSD_START.date(), "to", LSD.date())
print("Next window:", NEXT_START.date(), "to", NEXT_END.date())

# ---------- 2) keys (force “complete tables” via base keyframes) ----------
program_keys = (d[["PROGRAM"]].drop_duplicates().sort_values("PROGRAM").reset_index(drop=True))
prog_team_keys = (d[["PROGRAM", "SUB_TEAM"]].drop_duplicates()
                  .sort_values(["PROGRAM", "SUB_TEAM"]).reset_index(drop=True))

# ---------- 3) compute CTD + LSD components ----------
# NOTE: For SPI/CPI we need BCWS, BCWP, ACWP. We do NOT need ETC columns for Tables 1–2.
for_window = [
    ("CTD", CTD_START, LSD),
    ("LSD", LSD_START, LSD),
    ("NEXT", NEXT_START, NEXT_END),
]

# Program-level sums
prog_parts = {}
for tag, start, end in for_window:
    bcws = _sum_hours(d, "BCWS", start, end, ["PROGRAM"]).rename(columns={"BCWS": f"BCWS_{tag}"})
    bcwp = _sum_hours(d, "BCWP", start, end, ["PROGRAM"]).rename(columns={"BCWP": f"BCWP_{tag}"})
    acwp = _sum_hours(d, "ACWP", start, end, ["PROGRAM"]).rename(columns={"ACWP": f"ACWP_{tag}"})
    prog_parts[tag] = (bcws.merge(bcwp, on=["PROGRAM"], how="outer").merge(acwp, on=["PROGRAM"], how="outer"))

# Program+SubTeam sums
pt_parts = {}
for tag, start, end in for_window:
    bcws = _sum_hours(d, "BCWS", start, end, ["PROGRAM", "SUB_TEAM"]).rename(columns={"BCWS": f"BCWS_{tag}"})
    bcwp = _sum_hours(d, "BCWP", start, end, ["PROGRAM", "SUB_TEAM"]).rename(columns={"BCWP": f"BCWP_{tag}"})
    acwp = _sum_hours(d, "ACWP", start, end, ["PROGRAM", "SUB_TEAM"]).rename(columns={"ACWP": f"ACWP_{tag}"})
    pt_parts[tag] = (bcws.merge(bcwp, on=["PROGRAM","SUB_TEAM"], how="outer")
                         .merge(acwp, on=["PROGRAM","SUB_TEAM"], how="outer"))

# ---------- 4) Table 1: PROGRAM — SPI/CPI for CTD and LSD ----------
t1 = program_keys.copy()
t1 = _ensure_keys(t1, prog_parts["CTD"], ["PROGRAM"])
t1 = _ensure_keys(t1, prog_parts["LSD"], ["PROGRAM"])

# Fill missing component sums with 0 (means “no data in window”), but SPI/CPI will be safe-divided.
for c in ["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]:
    if c not in t1.columns: t1[c] = 0.0
t1[["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]] = t1[["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]].fillna(0.0)

t1["SPI_CTD"] = _safe_div(t1["BCWP_CTD"], t1["BCWS_CTD"])
t1["CPI_CTD"] = _safe_div(t1["BCWP_CTD"], t1["ACWP_CTD"])
t1["SPI_LSD"] = _safe_div(t1["BCWP_LSD"], t1["BCWS_LSD"])
t1["CPI_LSD"] = _safe_div(t1["BCWP_LSD"], t1["ACWP_LSD"])

# Keep only what you asked for (+ keep underlying sums if you want to sanity check)
t1 = t1[["PROGRAM","SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD","BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]]

# ---------- 5) Table 2: PROGRAM + SUB_TEAM — SPI/CPI for CTD and LSD ----------
t2 = prog_team_keys.copy()
t2 = _ensure_keys(t2, pt_parts["CTD"], ["PROGRAM","SUB_TEAM"])
t2 = _ensure_keys(t2, pt_parts["LSD"], ["PROGRAM","SUB_TEAM"])

for c in ["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]:
    if c not in t2.columns: t2[c] = 0.0
t2[["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]] = t2[["BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]].fillna(0.0)

t2["SPI_CTD"] = _safe_div(t2["BCWP_CTD"], t2["BCWS_CTD"])
t2["CPI_CTD"] = _safe_div(t2["BCWP_CTD"], t2["ACWP_CTD"])
t2["SPI_LSD"] = _safe_div(t2["BCWP_LSD"], t2["BCWS_LSD"])
t2["CPI_LSD"] = _safe_div(t2["BCWP_LSD"], t2["ACWP_LSD"])

t2 = t2[["PROGRAM","SUB_TEAM","SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD","BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD"]]

# ---------- 6) Table 3: PROGRAM + SUB_TEAM — BAC/EAC/VAC (hours) ----------
# BAC as total budget hours = BCWS_CTD
# EAC = ACWP_CTD + ETC_asof_LSD (use latest ETC <= LSD per PROGRAM+SUB_TEAM, fallback 0)
etc_asof = _latest_hours(d, "ETC", LSD, ["PROGRAM","SUB_TEAM"]).rename(columns={"ETC": "ETC_ASOF_LSD"})
t3 = prog_team_keys.copy()
t3 = _ensure_keys(t3, pt_parts["CTD"][["PROGRAM","SUB_TEAM","BCWS_CTD","ACWP_CTD"]], ["PROGRAM","SUB_TEAM"])
t3 = _ensure_keys(t3, etc_asof, ["PROGRAM","SUB_TEAM"])

for c in ["BCWS_CTD","ACWP_CTD","ETC_ASOF_LSD"]:
    if c not in t3.columns: t3[c] = 0.0
t3[["BCWS_CTD","ACWP_CTD","ETC_ASOF_LSD"]] = t3[["BCWS_CTD","ACWP_CTD","ETC_ASOF_LSD"]].fillna(0.0)

t3["BAC_HRS"] = t3["BCWS_CTD"]
t3["EAC_HRS"] = t3["ACWP_CTD"] + t3["ETC_ASOF_LSD"]
t3["VAC_HRS"] = t3["BAC_HRS"] - t3["EAC_HRS"]

t3 = t3[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS","BCWS_CTD","ACWP_CTD","ETC_ASOF_LSD"]]

# ---------- 7) Table 4: PROGRAM — Demand/Actual/%Var for LSD + NextMo BCWS/ETC ----------
# Demand Hours LSD = BCWS_LSD
# Actual Hours LSD = ACWP_LSD
# %Var LSD = (Actual - Demand) / Demand
# NextMo BCWS Hours = sum BCWS in NEXT window
# NextMo ETC Hours  = sum ETC in NEXT window (if ETC is point-in-time and not periodic, you may prefer latest instead of sum)
t4 = program_keys.copy()
t4 = _ensure_keys(t4, prog_parts["LSD"][["PROGRAM","BCWS_LSD","ACWP_LSD"]], ["PROGRAM"])
t4 = _ensure_keys(t4, prog_parts["NEXT"][["PROGRAM","BCWS_NEXT"]], ["PROGRAM"])
t4_next_etc = _sum_hours(d, "ETC", NEXT_START, NEXT_END, ["PROGRAM"]).rename(columns={"ETC":"ETC_NEXT"})
t4 = _ensure_keys(t4, t4_next_etc, ["PROGRAM"])

for c in ["BCWS_LSD","ACWP_LSD","BCWS_NEXT","ETC_NEXT"]:
    if c not in t4.columns: t4[c] = 0.0
t4[["BCWS_LSD","ACWP_LSD","BCWS_NEXT","ETC_NEXT"]] = t4[["BCWS_LSD","ACWP_LSD","BCWS_NEXT","ETC_NEXT"]].fillna(0.0)

t4 = t4.rename(columns={
    "BCWS_LSD": "Demand_Hours_LSD",
    "ACWP_LSD": "Actual_Hours_LSD",
    "BCWS_NEXT": "NextMo_BCWS_Hours",
    "ETC_NEXT": "NextMo_ETC_Hours",
})
t4["PctVar_LSD"] = np.where(
    t4["Demand_Hours_LSD"].to_numpy() == 0,
    0.0,
    (t4["Actual_Hours_LSD"] - t4["Demand_Hours_LSD"]) / t4["Demand_Hours_LSD"]
)
t4 = t4[["PROGRAM","Demand_Hours_LSD","Actual_Hours_LSD","PctVar_LSD","NextMo_BCWS_Hours","NextMo_ETC_Hours"]]

# ---------- 8) Debug: trace “why missing / zero-looking rows happen” ----------
# This shows keys where an index is 0 because inputs are 0 (not because of NaN).
# You can quickly see if it’s “no BCWS” vs “no ACWP” etc.
dbg_prog = t1[["PROGRAM","BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD"]].copy()
dbg_prog["FLAG_no_BCWS_CTD"] = dbg_prog["BCWS_CTD"].eq(0)
dbg_prog["FLAG_no_BCWP_CTD"] = dbg_prog["BCWP_CTD"].eq(0)
dbg_prog["FLAG_no_ACWP_CTD"] = dbg_prog["ACWP_CTD"].eq(0)
dbg_prog["FLAG_no_BCWS_LSD"] = dbg_prog["BCWS_LSD"].eq(0)
dbg_prog["FLAG_no_BCWP_LSD"] = dbg_prog["BCWP_LSD"].eq(0)
dbg_prog["FLAG_no_ACWP_LSD"] = dbg_prog["ACWP_LSD"].eq(0)

dbg_team = t2[["PROGRAM","SUB_TEAM","BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD"]].copy()
dbg_team["FLAG_no_BCWS_CTD"] = dbg_team["BCWS_CTD"].eq(0)
dbg_team["FLAG_no_BCWP_CTD"] = dbg_team["BCWP_CTD"].eq(0)
dbg_team["FLAG_no_ACWP_CTD"] = dbg_team["ACWP_CTD"].eq(0)
dbg_team["FLAG_no_BCWS_LSD"] = dbg_team["BCWS_LSD"].eq(0)
dbg_team["FLAG_no_BCWP_LSD"] = dbg_team["BCWP_LSD"].eq(0)
dbg_team["FLAG_no_ACWP_LSD"] = dbg_team["ACWP_LSD"].eq(0)

# If ETC is “missing” for BAC/EAC/VAC, show it explicitly:
dbg_eac = t3[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS","ETC_ASOF_LSD"]].copy()
dbg_eac["FLAG_no_ETC_asof_LSD"] = dbg_eac["ETC_ASOF_LSD"].eq(0)

print("\n--- DEBUG SUMMARY (counts) ---")
print("Programs with no LSD Demand (BCWS_LSD==0):", int(dbg_prog["FLAG_no_BCWS_LSD"].sum()))
print("Programs with no LSD Actual (ACWP_LSD==0):", int(dbg_prog["FLAG_no_ACWP_LSD"].sum()))
print("Program/SubTeam rows with no LSD Demand (BCWS_LSD==0):", int(dbg_team["FLAG_no_BCWS_LSD"].sum()))
print("Program/SubTeam rows with no LSD Actual (ACWP_LSD==0):", int(dbg_team["FLAG_no_ACWP_LSD"].sum()))
print("Program/SubTeam rows with ETC missing-asof-LSD (ETC_ASOF_LSD==0):", int(dbg_eac["FLAG_no_ETC_asof_LSD"].sum()))

# Optional: inspect the *raw* contributing rows for any “weird” key
# Example usage after this cell runs:
# d[(d["PROGRAM"]=="XM30") & (d["SUB_TEAM"]=="PM") & (d["DATE"].between(LSD_START, LSD))].sort_values(["DATE","COST_SET"]).tail(50)

# ---------- 9) Final: guarantee NO NaN in outputs ----------
t1 = t1.replace([np.inf, -np.inf], 0).fillna(0)
t2 = t2.replace([np.inf, -np.inf], 0).fillna(0)
t3 = t3.replace([np.inf, -np.inf], 0).fillna(0)
t4 = t4.replace([np.inf, -np.inf], 0).fillna(0)

# ---------- 10) Save to ONE Excel file (PowerBI ingest) ----------
out_path = "EVMS_Metrics_PowerBI.xlsx"
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    t1.to_excel(writer, sheet_name="T1_Program_SPI_CPI", index=False)
    t2.to_excel(writer, sheet_name="T2_Prog_SubTeam_SPI_CPI", index=False)
    t3.to_excel(writer, sheet_name="T3_Prog_SubTeam_BAC_EAC_VAC", index=False)
    t4.to_excel(writer, sheet_name="T4_Program_Demand_Actual_Next", index=False)
    dbg_prog.to_excel(writer, sheet_name="DEBUG_Program", index=False)
    dbg_team.to_excel(writer, sheet_name="DEBUG_Prog_SubTeam", index=False)
    dbg_eac.to_excel(writer, sheet_name="DEBUG_EAC_ETC", index=False)

print("\n✅ Wrote:", out_path)
print("Tables:", "t1, t2, t3, t4 (plus debug tabs)")
display(t1)
display(t2.head(50))
display(t3.head(50))
display(t4)