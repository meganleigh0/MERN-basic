# =========================
# EVMS -> PowerBI Excel (ONE CELL, FIXED)
# - Hardcodes 4 programs
# - As-of = last Thursday of previous month (relative to TODAY)
# - LSD FIX: compute LSD *per COST_SET* using the latest available DATE <= as-of for that (Program, SubTeam, CostSet)
#            (this removes the huge “Missing value” problem caused by picking one LSD_DATE that doesn’t have all cost sets)
# - Program Overview: SPI + CPI ONLY (NO BEI)
# - Preserves user-entered comments if the Excel already exists (so people can type comments, refresh PBI)
# - Writes ONE Excel with 4 sheets, EXACT headers
# =========================

import os, re
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS
# -------------------------
PROGRAMS_KEEP = ["ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"]
TODAY_OVERRIDE = None  # e.g. "2026-02-10" for testing
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

FORCE_READ_FILES = False
INPUT_FILES = []  # optional explicit list of csv/xlsx

# -------------------------
# HELPERS
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

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

def _to_date(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date()
    if isinstance(x, date):
        return x
    return pd.to_datetime(x).date()

def last_thursday_of_month(year: int, month: int) -> date:
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last.weekday() - 3) % 7  # Thu=3
    return last - timedelta(days=offset)

def last_thursday_prev_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 1:
        y, m = y - 1, 12
    else:
        m -= 1
    return last_thursday_of_month(y, m)

def add_month(d: date, months: int = 1) -> date:
    y, m = d.year, d.month + months
    while m > 12:
        y += 1
        m -= 12
    while m < 1:
        y -= 1
        m += 12
    # clamp day
    if m == 12:
        last_day = 31
    else:
        last_day = (date(y, m + 1, 1) - timedelta(days=1)).day
    return date(y, m, min(d.day, last_day))

def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    colmap = {c: str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in df.columns}
    df.rename(columns=colmap, inplace=True)

    # PROGRAM
    if "PROGRAM" not in df.columns:
        for c in ["PROGRAMID", "PROG", "PROJECT", "IPT_PROGRAM"]:
            if c in df.columns:
                df.rename(columns={c: "PROGRAM"}, inplace=True)
                break

    # SUBTEAM
    if "SUB_TEAM" not in df.columns:
        for c in ["SUBTEAM", "SUB_TEAM_NAME", "IPT", "IPT_NAME", "CONTROL_ACCOUNT", "CA", "SUBTEAM_NAME"]:
            if c in df.columns:
                df.rename(columns={c: "SUB_TEAM"}, inplace=True)
                break

    # DATE
    if "DATE" not in df.columns:
        for c in ["PERIOD_END", "PERIODEND", "STATUS_DATE", "AS_OF_DATE"]:
            if c in df.columns:
                df.rename(columns={c: "DATE"}, inplace=True)
                break

    # COST_SET
    if "COST_SET" not in df.columns:
        for c in ["COSTSET", "COST-SET", "COST_SET_NAME", "COST_CATEGORY"]:
            if c in df.columns:
                df.rename(columns={c: "COST_SET"}, inplace=True)
                break

    # HOURS
    if "HOURS" not in df.columns:
        for c in ["VALUE", "AMOUNT", "HRS", "HOURS_WORKED", "TOTAL_HOURS"]:
            if c in df.columns:
                df.rename(columns={c: "HOURS"}, inplace=True)
                break

    required = ["PROGRAM", "SUB_TEAM", "DATE", "COST_SET", "HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Found: {list(df.columns)}")

    df["PROGRAM"]  = df["PROGRAM"].map(normalize_key)
    df["SUB_TEAM"] = df["SUB_TEAM"].map(normalize_key)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","SUB_TEAM","DATE","COST_SET","HOURS"])
    return df

def load_inputs() -> pd.DataFrame:
    if not FORCE_READ_FILES:
        for name in ["cobra_merged_df", "cobra_df", "df", "raw_df"]:
            if name in globals() and isinstance(globals()[name], pd.DataFrame) and len(globals()[name]) > 0:
                return coerce_columns(globals()[name])

    files = list(INPUT_FILES)
    if not files:
        candidates = []
        for pat in ["*.csv", "*.xlsx", "*.xls"]:
            candidates += list(Path(".").glob(pat))
        candidates = sorted(candidates, key=lambda p: (("cobra" not in p.name.lower()), p.name.lower()))
        files = [str(p) for p in candidates[:30]]
        if not files:
            raise FileNotFoundError("No input files found and no in-memory dataframe (cobra_merged_df/df/...) found.")

    frames = []
    for fp in files:
        p = Path(fp)
        if not p.exists():
            continue
        if p.suffix.lower() == ".csv":
            frames.append(pd.read_csv(p))
        elif p.suffix.lower() in [".xlsx",".xls"]:
            xls = pd.ExcelFile(p)
            for sh in xls.sheet_names:
                frames.append(pd.read_excel(p, sheet_name=sh))
    if not frames:
        raise FileNotFoundError("No readable input data found from INPUT_FILES / auto-discovery.")
    return coerce_columns(pd.concat(frames, ignore_index=True))

# -------------------------
# LOAD + FILTER PROGRAMS
# -------------------------
base = load_inputs()
base = base[base["PROGRAM"].isin([normalize_key(p) for p in PROGRAMS_KEEP])].copy()

# -------------------------
# AS-OF / NEXT PERIOD
# -------------------------
_today = _to_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = last_thursday_prev_month(_today)
month_after = add_month(AS_OF_DATE, 1)
NEXT_PERIOD_END = last_thursday_of_month(month_after.year, month_after.month)

YEAR_FILTER = AS_OF_DATE.year
YEAR_START = date(YEAR_FILTER, 1, 1)
YEAR_END   = date(YEAR_FILTER, 12, 31)

print("As-of logic")
print("TODAY:", _today)
print("AS_OF_DATE (last Thu of prev month):", AS_OF_DATE)
print("NEXT_PERIOD_END (last Thu of next month):", NEXT_PERIOD_END)
print("YEAR_FILTER:", YEAR_FILTER)

# -------------------------
# CORE FILTERS
# -------------------------
NEEDED_COSTSETS = ["BCWS", "BCWP", "ACWP", "ETC"]

# Use YEAR_FILTER for CTD + BAC logic
base_year = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()
base_to_asof = base_year[base_year["DATE"] <= AS_OF_DATE].copy()

# -------------------------
# CTD (sum up to AS_OF_DATE)
# -------------------------
ctd_sub = (
    base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)]
    .groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

ctd_prog = (
    base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# -------------------------
# LSD FIX (per COST_SET): latest DATE <= AS_OF_DATE for (PROGRAM, SUBTEAM, COST_SET) and (PROGRAM, COST_SET)
# -------------------------
# Subteam LSD rows
tmp_sub = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].copy()
tmp_sub = tmp_sub.sort_values(["PROGRAM","SUB_TEAM","COST_SET","DATE"])
# pick latest DATE per key
sub_last_date = (
    tmp_sub.groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE":"LSD_DATE"})
)
# join back to get hours at that LSD_DATE (sum if multiple rows)
lsd_sub = (
    tmp_sub.merge(sub_last_date, on=["PROGRAM","SUB_TEAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

# Program LSD rows
tmp_prog = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].copy()
tmp_prog = tmp_prog.sort_values(["PROGRAM","COST_SET","DATE"])
prog_last_date = (
    tmp_prog.groupby(["PROGRAM","COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE":"LSD_DATE"})
)
lsd_prog = (
    tmp_prog.merge(prog_last_date, on=["PROGRAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

def pivot_costsets(df, idx_cols, val_col):
    if df.empty:
        out = df[idx_cols].drop_duplicates().copy()
        for cs in NEEDED_COSTSETS:
            out[cs] = np.nan
        return out
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values=val_col, aggfunc="sum").reset_index()
    for cs in NEEDED_COSTSETS:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

ctd_sub_p  = pivot_costsets(ctd_sub,  ["PROGRAM","SUB_TEAM"], "CTD_HRS")
lsd_sub_p  = pivot_costsets(lsd_sub,  ["PROGRAM","SUB_TEAM"], "LSD_HRS")
ctd_prog_p = pivot_costsets(ctd_prog, ["PROGRAM"], "CTD_HRS")
lsd_prog_p = pivot_costsets(lsd_prog, ["PROGRAM"], "LSD_HRS")

# -------------------------
# PROGRAM OVERVIEW (SPI + CPI only; NO BEI)
# EXACT headers:
# ProgramID | Metric | CTD | LSD | Comments / Root Cause & Corrective Actions
# -------------------------
prog = ctd_prog_p.merge(lsd_prog_p, on=["PROGRAM"], how="outer", suffixes=("_CTD","_LSD"))
prog.rename(columns={"PROGRAM":"ProgramID"}, inplace=True)

prog_spi_ctd = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog_cpi_ctd = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog_spi_lsd = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog_cpi_lsd = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

program_overview = pd.concat([
    pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric": "SPI", "CTD": prog_spi_ctd, "LSD": prog_spi_lsd}),
    pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric": "CPI", "CTD": prog_cpi_ctd, "LSD": prog_cpi_lsd}),
], ignore_index=True)

program_overview["Comments / Root Cause & Corrective Actions"] = ""
program_overview = program_overview[["ProgramID","Metric","CTD","LSD","Comments / Root Cause & Corrective Actions"]]
program_overview = program_overview.sort_values(["ProgramID","Metric"]).reset_index(drop=True)

# -------------------------
# SUBTEAM SPI/CPI
# EXACT headers:
# SubTeam | SPI LSD | SPI CTD | CPI LSD | CPI CTD | Cause & Corrective Actions | ProgramID
# -------------------------
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
    "Cause & Corrective Actions": "",
    "ProgramID": sub["ProgramID"],
}).sort_values(["ProgramID","SubTeam"]).reset_index(drop=True)

# -------------------------
# SUBTEAM BAC/EAC/VAC
# EXACT headers:
# SubTeam | BAC | EAC | VAC | Cause & Corrective Actions | ProgramID
# -------------------------
# BAC = YEAR total BCWS
bcws_year = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM","SUB_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"BAC"})
)

# EAC = ACWP_CTD + ETC_CTD
acwp_ctd = ctd_sub_p[["PROGRAM","SUB_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_sub_p[["PROGRAM","SUB_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","SUB_TEAM"], how="outer")
eac["EAC"] = pd.to_numeric(eac["ACWP_CTD"], errors="coerce").fillna(0) + pd.to_numeric(eac["ETC_CTD"], errors="coerce").fillna(0)

bac_eac = bcws_year.merge(eac[["PROGRAM","SUB_TEAM","EAC"]], on=["PROGRAM","SUB_TEAM"], how="outer")
bac_eac["BAC"] = pd.to_numeric(bac_eac["BAC"], errors="coerce")
bac_eac["EAC"] = pd.to_numeric(bac_eac["EAC"], errors="coerce")
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]

subteam_bac_eac_vac = bac_eac.rename(columns={"PROGRAM":"ProgramID","SUB_TEAM":"SubTeam"}).copy()
subteam_bac_eac_vac["Cause & Corrective Actions"] = ""
subteam_bac_eac_vac = subteam_bac_eac_vac[["SubTeam","BAC","EAC","VAC","Cause & Corrective Actions","ProgramID"]]
subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["ProgramID","SubTeam"]).reset_index(drop=True)

# -------------------------
# PROGRAM MANPOWER (Hours)
# EXACT headers:
# ProgramID | Demand Hours | Actual Hours | % Var | Next Mo BCWS Hours | Next Mo ETC Hours | Cause & Corrective Actions
# -------------------------
man = ctd_prog_p.rename(columns={"PROGRAM":"ProgramID","BCWS":"Demand Hours","ACWP":"Actual Hours"}).copy()
man["Demand Hours"] = pd.to_numeric(man["Demand Hours"], errors="coerce")
man["Actual Hours"] = pd.to_numeric(man["Actual Hours"], errors="coerce")
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0

next_window = base[(base["DATE"] > AS_OF_DATE) & (base["DATE"] <= NEXT_PERIOD_END) & (base["COST_SET"].isin(["BCWS","ETC"]))].copy()
next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index=["PROGRAM"], columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
for cs in ["BCWS","ETC"]:
    if cs not in next_prog.columns:
        next_prog[cs] = np.nan

next_prog = next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"})
program_manpower = man.merge(next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]], on="ProgramID", how="left")
program_manpower["Cause & Corrective Actions"] = ""
program_manpower = program_manpower[[
    "ProgramID","Demand Hours","Actual Hours","% Var","Next Mo BCWS Hours","Next Mo ETC Hours","Cause & Corrective Actions"
]].sort_values(["ProgramID"]).reset_index(drop=True)

# -------------------------
# PRESERVE EXISTING COMMENTS (if file exists)
# -------------------------
def preserve_comments(existing_path: Path, sheet: str, df_new: pd.DataFrame, key_cols, comment_col):
    if (not existing_path.exists()) or (comment_col not in df_new.columns):
        return df_new
    try:
        old = pd.read_excel(existing_path, sheet_name=sheet)
    except Exception:
        return df_new
    if old is None or len(old) == 0:
        return df_new
    # Only preserve if old has the expected columns
    if not all(k in old.columns for k in key_cols) or comment_col not in old.columns:
        return df_new
    old = old[key_cols + [comment_col]].copy()
    # merge old comments onto new; prefer old non-empty
    out = df_new.merge(old, on=key_cols, how="left", suffixes=("", "_old"))
    if f"{comment_col}_old" in out.columns:
        oldvals = out[f"{comment_col}_old"]
        newvals = out[comment_col]
        keep_old = oldvals.notna() & (oldvals.astype(str).str.strip() != "")
        out.loc[keep_old, comment_col] = oldvals.loc[keep_old]
        out = out.drop(columns=[f"{comment_col}_old"])
    return out

program_overview = preserve_comments(
    OUTPUT_XLSX, "Program_Overview", program_overview,
    key_cols=["ProgramID","Metric"],
    comment_col="Comments / Root Cause & Corrective Actions"
)
subteam_spi_cpi = preserve_comments(
    OUTPUT_XLSX, "SubTeam_SPI_CPI", subteam_spi_cpi,
    key_cols=["ProgramID","SubTeam"],
    comment_col="Cause & Corrective Actions"
)
subteam_bac_eac_vac = preserve_comments(
    OUTPUT_XLSX, "SubTeam_BAC_EAC_VAC", subteam_bac_eac_vac,
    key_cols=["ProgramID","SubTeam"],
    comment_col="Cause & Corrective Actions"
)
program_manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower", program_manpower,
    key_cols=["ProgramID"],
    comment_col="Cause & Corrective Actions"
)

# -------------------------
# WRITE ONE EXCEL (PowerBI)
# -------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

print("\nQuick missingness check:")
print("Program_Overview missing CTD:", float(program_overview["CTD"].isna().mean()),
      " missing LSD:", float(program_overview["LSD"].isna().mean()))
print("SubTeam_SPI_CPI missing SPI LSD:", float(subteam_spi_cpi["SPI LSD"].isna().mean()),
      " CPI LSD:", float(subteam_spi_cpi["CPI LSD"].isna().mean()))
print("SubTeam_BAC_EAC_VAC missing BAC:", float(subteam_bac_eac_vac["BAC"].isna().mean()),
      " EAC:", float(subteam_bac_eac_vac["EAC"].isna().mean()),
      " VAC:", float(subteam_bac_eac_vac["VAC"].isna().mean()))
print("Program_Manpower missing Demand:", float(program_manpower["Demand Hours"].isna().mean()),
      " Actual:", float(program_manpower["Actual Hours"].isna().mean()))

display(program_overview.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_manpower)