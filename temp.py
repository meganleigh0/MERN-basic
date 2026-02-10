# =========================
# EVMS -> PowerBI Excel (ONE CELL)
# - Hardcodes 4 programs
# - Uses "as-of" = last Thursday of previous month (relative to TODAY)
# - LSD values use "latest available DATE <= as-of" per Program/SubTeam to minimize missing
# - Outputs EXACT headers + sheet names for PowerBI
# =========================

import os, re, math
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# 0) SETTINGS (edit if needed)
# -------------------------
PROGRAMS_KEEP = ["ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"]

# If you want to simulate a "today" date (for testing), set to a date string like "2026-02-10"
TODAY_OVERRIDE = None  # e.g. "2026-02-10"

# Output Excel
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

# If you want to force reading input files instead of using an in-memory df, set to True
FORCE_READ_FILES = False

# If reading files, put your file paths here (csv/xlsx). Otherwise it will try to auto-discover.
INPUT_FILES = []  # e.g. ["./data/Cobra_Janji.xlsx", "./data/Cobra_Abrams.xlsx", ...]

# -------------------------
# 1) DATE HELPERS
# -------------------------
def _to_date(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date()
    if isinstance(x, date):
        return x
    return pd.to_datetime(x).date()

def last_thursday_of_month(year: int, month: int) -> date:
    # last day of month
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    # Thursday = 3 (Mon=0..Sun=6)
    offset = (last.weekday() - 3) % 7
    return last - timedelta(days=offset)

def last_thursday_prev_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 1:
        y, m = y - 1, 12
    else:
        m = m - 1
    return last_thursday_of_month(y, m)

def add_month(d: date, months: int = 1) -> date:
    # simple month add without external deps
    y, m = d.year, d.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    # clamp day
    day = min(d.day, (date(y, m % 12 + 1, 1) - timedelta(days=1)).day if m != 12 else 31)
    return date(y, m, day)

# -------------------------
# 2) LOAD / BUILD BASE DF
# -------------------------
def normalize_key(s):
    if pd.isna(s):
        return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    s = re.sub(r"__+", "_", s)
    return s

def normalize_cost_set(s):
    if pd.isna(s):
        return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    # common aliases -> canonical
    aliases = {
        "BCWS": "BCWS",
        "BCWP": "BCWP",
        "ACWP": "ACWP",
        "ETC":  "ETC",
        "EAC":  "EAC",
        "BAC":  "BAC",
        "VAC":  "VAC",
    }
    return aliases.get(s, s)

def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # normalize column names
    colmap = {c: str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in df.columns}
    df.rename(columns=colmap, inplace=True)

    # allow common variants
    # program
    if "PROGRAM" not in df.columns:
        for c in ["PROGRAMID", "PROG", "PROJECT", "IPT_PROGRAM"]:
            if c in df.columns:
                df.rename(columns={c: "PROGRAM"}, inplace=True)
                break

    # subteam
    if "SUB_TEAM" not in df.columns:
        for c in ["SUBTEAM", "SUB_TEAM_NAME", "IPT", "IPT_NAME", "CONTROL_ACCOUNT", "CA", "SUBTEAM_NAME"]:
            if c in df.columns:
                df.rename(columns={c: "SUB_TEAM"}, inplace=True)
                break

    # date
    if "DATE" not in df.columns:
        for c in ["PERIOD_END", "PERIODEND", "STATUS_DATE", "AS_OF_DATE"]:
            if c in df.columns:
                df.rename(columns={c: "DATE"}, inplace=True)
                break

    # cost set
    if "COST_SET" not in df.columns:
        for c in ["COSTSET", "COST-SET", "COST_SET_NAME", "COST_CATEGORY"]:
            if c in df.columns:
                df.rename(columns={c: "COST_SET"}, inplace=True)
                break

    # hours/value
    if "HOURS" not in df.columns:
        for c in ["VALUE", "AMOUNT", "HRS", "HOURS_WORKED", "TOTAL_HOURS"]:
            if c in df.columns:
                df.rename(columns={c: "HOURS"}, inplace=True)
                break

    required = ["PROGRAM", "SUB_TEAM", "DATE", "COST_SET", "HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Found: {list(df.columns)}")

    # normalize key fields
    df["PROGRAM"]  = df["PROGRAM"].map(normalize_key)
    df["SUB_TEAM"] = df["SUB_TEAM"].map(normalize_key)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)

    # parse dates
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date

    # numeric hours
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    # drop unusable
    df = df.dropna(subset=["PROGRAM", "SUB_TEAM", "DATE", "COST_SET", "HOURS"])
    return df

def load_inputs() -> pd.DataFrame:
    # prefer existing in-memory df if present
    if not FORCE_READ_FILES:
        for name in ["cobra_merged_df", "cobra_df", "df", "raw_df"]:
            if name in globals() and isinstance(globals()[name], pd.DataFrame) and len(globals()[name]) > 0:
                return coerce_columns(globals()[name])

    # else read files
    files = list(INPUT_FILES)
    if not files:
        # auto-discover in cwd (you can narrow this if you want)
        candidates = []
        for pat in ["*.csv", "*.xlsx", "*.xls"]:
            candidates += list(Path(".").glob(pat))
        # prefer files with "cobra" in name
        candidates = sorted(candidates, key=lambda p: (("cobra" not in p.name.lower()), p.name.lower()))
        files = [str(p) for p in candidates[:20]]  # cap
        if not files:
            raise FileNotFoundError("No input files found and no in-memory dataframe found.")

    frames = []
    for fp in files:
        p = Path(fp)
        if not p.exists():
            continue
        if p.suffix.lower() == ".csv":
            tmp = pd.read_csv(p)
            frames.append(tmp)
        elif p.suffix.lower() in [".xlsx", ".xls"]:
            # read all sheets and concat (robust)
            xls = pd.ExcelFile(p)
            for sh in xls.sheet_names:
                tmp = pd.read_excel(p, sheet_name=sh)
                frames.append(tmp)

    if not frames:
        raise FileNotFoundError("No readable input data found from INPUT_FILES / auto-discovery.")
    return coerce_columns(pd.concat(frames, ignore_index=True))

base = load_inputs()

# keep only programs
base = base[base["PROGRAM"].isin([normalize_key(p) for p in PROGRAMS_KEEP])].copy()

# -------------------------
# 3) AS-OF + NEXT PERIOD LOGIC
# -------------------------
_today = _to_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = last_thursday_prev_month(_today)
# "next month" period end = last Thursday of the month AFTER AS_OF_DATE's month
month_after = add_month(AS_OF_DATE, 1)
NEXT_PERIOD_END = last_thursday_of_month(month_after.year, month_after.month)

# For CTD calculations, use year = as-of year
YEAR_FILTER = AS_OF_DATE.year
YEAR_START = date(YEAR_FILTER, 1, 1)
YEAR_END   = date(YEAR_FILTER, 12, 31)

print("As-of logic")
print("TODAY:", _today)
print("AS_OF_DATE (last Thu of prev month):", AS_OF_DATE)
print("NEXT_PERIOD_END (last Thu of next month):", NEXT_PERIOD_END)
print("YEAR_FILTER:", YEAR_FILTER)

# -------------------------
# 4) MINIMIZE MISSING LSD: pick latest DATE <= AS_OF_DATE per (PROGRAM, SUB_TEAM)
# -------------------------
base_in_scope = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()
base_to_asof = base_in_scope[base_in_scope["DATE"] <= AS_OF_DATE].copy()

# If some programs have no rows in that year<=asof, fall back to "DATE <= AS_OF_DATE" without year restriction for LSD
fallback_to_asof = base[base["DATE"] <= AS_OF_DATE].copy()

def latest_date_per_group(df, keys):
    if df.empty:
        return pd.DataFrame(columns=keys + ["LSD_DATE"])
    return (
        df.groupby(keys, as_index=False)["DATE"]
          .max()
          .rename(columns={"DATE": "LSD_DATE"})
    )

lsd_prog_sub = latest_date_per_group(base_to_asof, ["PROGRAM", "SUB_TEAM"])
# fallback fill for missing (program, subteam) pairs
if len(lsd_prog_sub) == 0:
    lsd_prog_sub = latest_date_per_group(fallback_to_asof, ["PROGRAM", "SUB_TEAM"])
else:
    missing_pairs = (
        base[["PROGRAM", "SUB_TEAM"]].drop_duplicates()
        .merge(lsd_prog_sub[["PROGRAM", "SUB_TEAM"]], on=["PROGRAM","SUB_TEAM"], how="left", indicator=True)
    )
    missing_pairs = missing_pairs[missing_pairs["_merge"] == "left_only"][["PROGRAM","SUB_TEAM"]]
    if len(missing_pairs) > 0:
        lsd_fallback = (
            fallback_to_asof.merge(missing_pairs, on=["PROGRAM","SUB_TEAM"], how="inner")
            .groupby(["PROGRAM","SUB_TEAM"], as_index=False)["DATE"].max()
            .rename(columns={"DATE":"LSD_DATE"})
        )
        lsd_prog_sub = pd.concat([lsd_prog_sub, lsd_fallback], ignore_index=True)

# program-level LSD date (latest <= as-of)
lsd_prog = latest_date_per_group(base_to_asof, ["PROGRAM"])
if len(lsd_prog) == 0:
    lsd_prog = latest_date_per_group(fallback_to_asof, ["PROGRAM"])

# -------------------------
# 5) AGGREGATIONS (CTD + LSD) for needed cost sets
# -------------------------
NEEDED_COSTSETS = ["BCWS", "BCWP", "ACWP", "ETC"]  # drive SPI/CPI + manpower + EAC

base_to_asof_needed = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].copy()

# CTD by Program/SubTeam/CostSet: sum HOURS for DATE <= AS_OF_DATE (year-filtered)
ctd_sub = (
    base_to_asof_needed
    .groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["HOURS"]
    .sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# CTD by Program/CostSet
ctd_prog = (
    base_to_asof_needed
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"]
    .sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# LSD by Program/SubTeam/CostSet: sum HOURS at each group's LSD_DATE
base_lsd = base.merge(lsd_prog_sub, on=["PROGRAM","SUB_TEAM"], how="inner")
base_lsd = base_lsd[(base_lsd["DATE"] == base_lsd["LSD_DATE"]) & (base_lsd["COST_SET"].isin(NEEDED_COSTSETS))].copy()

lsd_sub = (
    base_lsd
    .groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["HOURS"]
    .sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

# LSD by Program/CostSet
base_lsd_prog = base.merge(lsd_prog, on=["PROGRAM"], how="inner")
base_lsd_prog = base_lsd_prog[(base_lsd_prog["DATE"] == base_lsd_prog["LSD_DATE"]) & (base_lsd_prog["COST_SET"].isin(NEEDED_COSTSETS))].copy()

lsd_prog_agg = (
    base_lsd_prog
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"]
    .sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

def pivot_costsets(df, idx_cols, val_col):
    if df.empty:
        # create an empty pivot with expected cols
        out = df[idx_cols].drop_duplicates().copy()
        for cs in NEEDED_COSTSETS:
            out[cs] = np.nan
        return out
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values=val_col, aggfunc="sum")
    pv = pv.reset_index()
    for cs in NEEDED_COSTSETS:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

ctd_sub_p = pivot_costsets(ctd_sub, ["PROGRAM","SUB_TEAM"], "CTD_HRS")
lsd_sub_p = pivot_costsets(lsd_sub, ["PROGRAM","SUB_TEAM"], "LSD_HRS")
ctd_prog_p = pivot_costsets(ctd_prog, ["PROGRAM"], "CTD_HRS")
lsd_prog_p = pivot_costsets(lsd_prog_agg, ["PROGRAM"], "LSD_HRS")

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

# -------------------------
# 6) BUILD TABLES (EXACT HEADERS)
# -------------------------

# 6A) Program Overview (Metric / CTD / LSD)
prog = ctd_prog_p.merge(lsd_prog_p, on=["PROGRAM"], how="outer", suffixes=("_CTD","_LSD"))
prog.rename(columns={"PROGRAM":"ProgramID"}, inplace=True)

# compute indices (CTD and LSD)
prog_spi_ctd = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog_cpi_ctd = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog_spi_lsd = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog_cpi_lsd = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

# BEI: use SPI (matches your current outputs where BEI == SPI)
prog_bei_ctd = prog_spi_ctd
prog_bei_lsd = prog_spi_lsd

program_overview = pd.concat([
    pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric": "SPI", "CTD": prog_spi_ctd, "LSD": prog_spi_lsd}),
    pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric": "CPI", "CTD": prog_cpi_ctd, "LSD": prog_cpi_lsd}),
    pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric": "BEI", "CTD": prog_bei_ctd, "LSD": prog_bei_lsd}),
], ignore_index=True)

# EXACT headers + order
program_overview = program_overview[["ProgramID","Metric","CTD","LSD"]].copy()
program_overview["Comments / Root Cause & Corrective Actions"] = ""  # EXACT column name
program_overview = program_overview[["ProgramID","Metric","CTD","LSD","Comments / Root Cause & Corrective Actions"]]

# 6B) SubTeam SPI/CPI table
sub = ctd_sub_p.merge(lsd_sub_p, on=["PROGRAM","SUB_TEAM"], how="outer", suffixes=("_CTD","_LSD"))
sub.rename(columns={"PROGRAM":"ProgramID","SUB_TEAM":"SubTeam"}, inplace=True)

sub_spi_ctd = safe_div(sub["BCWP_CTD"], sub["BCWS_CTD"])
sub_cpi_ctd = safe_div(sub["BCWP_CTD"], sub["ACWP_CTD"])
sub_spi_lsd = safe_div(sub["BCWP_LSD"], sub["BCWS_LSD"])
sub_cpi_lsd = safe_div(sub["BCWP_LSD"], sub["ACWP_LSD"])

subteam_spi_cpi = pd.DataFrame({
    "SubTeam": sub["SubTeam"],
    "SPI LSD": sub_spi_lsd,
    "SPI CTD": sub_spi_ctd,
    "CPI LSD": sub_cpi_lsd,
    "CPI CTD": sub_cpi_ctd,
    "Cause & Corrective Actions": "",     # EXACT column name
    "ProgramID": sub["ProgramID"],
})

# 6C) SubTeam BAC/EAC/VAC table (reduce missing)
# BAC = total BCWS for the full YEAR_FILTER (Jan1..Dec31)
bcws_year = base_in_scope[base_in_scope["COST_SET"] == "BCWS"].groupby(["PROGRAM","SUB_TEAM"], as_index=False)["HOURS"].sum()
bcws_year.rename(columns={"HOURS":"BAC"}, inplace=True)

# EAC = ACWP_CTD + ETC_CTD (as-of)
acwp_ctd = ctd_sub_p[["PROGRAM","SUB_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_sub_p[["PROGRAM","SUB_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","SUB_TEAM"], how="outer")
eac["EAC"] = pd.to_numeric(eac["ACWP_CTD"], errors="coerce").fillna(0) + pd.to_numeric(eac["ETC_CTD"], errors="coerce").fillna(0)

bac_eac = bcws_year.merge(eac[["PROGRAM","SUB_TEAM","EAC"]], on=["PROGRAM","SUB_TEAM"], how="outer")
bac_eac["BAC"] = pd.to_numeric(bac_eac["BAC"], errors="coerce")
bac_eac["EAC"] = pd.to_numeric(bac_eac["EAC"], errors="coerce")
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]

subteam_bac_eac_vac = bac_eac.copy()
subteam_bac_eac_vac.rename(columns={"PROGRAM":"ProgramID","SUB_TEAM":"SubTeam"}, inplace=True)
subteam_bac_eac_vac["Cause & Corrective Actions"] = ""
subteam_bac_eac_vac = subteam_bac_eac_vac[["SubTeam","BAC","EAC","VAC","Cause & Corrective Actions","ProgramID"]]

# 6D) Program Manpower (Hours)
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual/Demand
# Next Mo BCWS Hours / Next Mo ETC Hours = sum HOURS between (AS_OF_DATE, NEXT_PERIOD_END]
next_window = base[(base["DATE"] > AS_OF_DATE) & (base["DATE"] <= NEXT_PERIOD_END) & (base["COST_SET"].isin(["BCWS","ETC"]))].copy()
next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index=["PROGRAM"], columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
for cs in ["BCWS","ETC"]:
    if cs not in next_prog.columns:
        next_prog[cs] = np.nan

man = ctd_prog_p.copy()  # has BCWS, ACWP, etc as CTD in year scope
man.rename(columns={"PROGRAM":"ProgramID","BCWS":"Demand Hours","ACWP":"Actual Hours"}, inplace=True)
man["Demand Hours"] = pd.to_numeric(man["Demand Hours"], errors="coerce")
man["Actual Hours"] = pd.to_numeric(man["Actual Hours"], errors="coerce")
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0  # percent

next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"}, inplace=True)
program_manpower = man.merge(next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]], on="ProgramID", how="left")

program_manpower["Cause & Corrective Actions"] = ""
program_manpower = program_manpower[[
    "ProgramID",
    "Demand Hours",
    "Actual Hours",
    "% Var",
    "Next Mo BCWS Hours",
    "Next Mo ETC Hours",
    "Cause & Corrective Actions"
]]

# -------------------------
# 7) FINAL CLEANUP: enforce exact headers, drop extras, reduce missing where possible
# -------------------------
# If CPI/SPI LSD missing because BCWP_LSD/BCWS_LSD etc missing, they will be NaN.
# But we already minimized LSD missing by using latest available date <= as-of per group.
# Still, if a group lacks BCWP/BCWS/ACWP entirely, those will remain NaN (real missing).

# Sort for readability
program_overview = program_overview.sort_values(["ProgramID","Metric"]).reset_index(drop=True)
subteam_spi_cpi = subteam_spi_cpi.sort_values(["ProgramID","SubTeam"]).reset_index(drop=True)
subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["ProgramID","SubTeam"]).reset_index(drop=True)
program_manpower = program_manpower.sort_values(["ProgramID"]).reset_index(drop=True)

# -------------------------
# 8) WRITE ONE EXCEL FILE (PowerBI)
# -------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")
print("\nQuick missingness check:")
print("Program_Overview missing CTD:", program_overview["CTD"].isna().mean(), " missing LSD:", program_overview["LSD"].isna().mean())
print("SubTeam_SPI_CPI missing SPI LSD:", subteam_spi_cpi["SPI LSD"].isna().mean(), " CPI LSD:", subteam_spi_cpi["CPI LSD"].isna().mean())
print("SubTeam_BAC_EAC_VAC missing BAC:", subteam_bac_eac_vac["BAC"].isna().mean(), " EAC:", subteam_bac_eac_vac["EAC"].isna().mean(), " VAC:", subteam_bac_eac_vac["VAC"].isna().mean())
print("Program_Manpower missing Demand:", program_manpower["Demand Hours"].isna().mean(), " Actual:", program_manpower["Actual Hours"].isna().mean())

display(program_overview.head(15))
display(subteam_spi_cpi.head(15))
display(subteam_bac_eac_vac.head(15))
display(program_manpower)