# ============================================================
# EVMS -> PowerBI Excel (ONE CELL, FULL PIPELINE, FIXED)
#
# Writes ONE Excel: EVMS_PowerBI_Input.xlsx
# Sheet order (Program_Overview is FIRST):
#   1) Program_Overview
#   2) ProductTeam_SPI_CPI
#   3) ProductTeam_BAC_EAC_VAC
#   4) Program_Manpower
#
# Includes ALL requested color-spec columns:
# - SPI/CPI cell colors (CTD/LSD) using PPT thresholds (with rounding bands)
# - VAC coloring using VAC/BAC thresholds
# - % Var coloring using Program Manpower thresholds
# Preserves existing comment fields if file already exists.
# ============================================================

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

# Fix for SPI inflation when BCWS effectively represents half the span vs BCWP/ACWP
BCWS_SCALE_FACTOR = 2.0

FORCE_READ_FILES = False
INPUT_FILES = []  # optional explicit list of csv/xlsx; if empty -> auto-discover or in-memory df

# -------------------------
# GDLS COLOR PALETTE (from PPT)
# -------------------------
CLR_DARK_BLUE  = "#1F497D"  # RGB 031,073,125 (header color; optional use)
CLR_LIGHT_BLUE = "#8EB4E3"  # RGB 142,180,227
CLR_GREEN      = "#339966"  # RGB 051,153,102
CLR_YELLOW     = "#FFFF99"  # RGB 255,255,153
CLR_RED        = "#C0504D"  # RGB 192,080,077

# -------------------------
# THRESHOLDS (match PPT "rounding" bands shown)
# -------------------------
def color_spi_cpi_bei(x):
    # Blue: x >= 1.055
    # Green: 0.975 <= x < 1.055
    # Yellow: 0.945 <= x < 0.975
    # Red: x < 0.945
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

def color_program_manpower(pct):
    # Red:   pct >= 109.5
    # Yellow:105.5 <= pct < 109.5
    # Green: 89.5  <= pct < 105.5
    # Yellow:85.5  <= pct < 89.5
    # Red:   pct < 85.5
    pct = pd.to_numeric(pct, errors="coerce")
    if pd.isna(pct): return None
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

def color_vac_over_bac(x):
    # VAC/BAC thresholds (PPT rounding bands)
    # Blue:   x >= +0.055
    # Green:  -0.025 <= x < +0.055
    # Yellow: -0.055 <= x < -0.025
    # Red:    x < -0.055
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

# -------------------------
# HELPERS
# -------------------------
def normalize_key(s):
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_cost_set(s):
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", "", s).replace("-", "").replace("_", "")
    aliases = {
        "BCWS":"BCWS","BCWP":"BCWP","ACWP":"ACWP","ETC":"ETC",
        "EAC":"EAC","BAC":"BAC","VAC":"VAC"
    }
    return aliases.get(s, s)

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

def _to_date(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    if isinstance(x, date): return x
    return pd.to_datetime(x, errors="coerce").date()

def last_thursday_of_month(year: int, month: int) -> date:
    last = date(year, 12, 31) if month == 12 else (date(year, month + 1, 1) - timedelta(days=1))
    offset = (last.weekday() - 3) % 7  # Thu=3
    return last - timedelta(days=offset)

def last_thursday_prev_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 1: y, m = y - 1, 12
    else: m -= 1
    return last_thursday_of_month(y, m)

def add_month(d: date, months: int = 1) -> date:
    y, m = d.year, d.month + months
    while m > 12:
        y += 1; m -= 12
    while m < 1:
        y -= 1; m += 12
    last_day = 31 if m == 12 else (date(y, m + 1, 1) - timedelta(days=1)).day
    return date(y, m, min(d.day, last_day))

def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    colmap = {c: str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in df.columns}
    df.rename(columns=colmap, inplace=True)

    # program
    if "PROGRAM" not in df.columns:
        for c in ["PROGRAMID", "PROG", "PROJECT", "IPT_PROGRAM"]:
            if c in df.columns:
                df.rename(columns={c: "PROGRAM"}, inplace=True)
                break

    # subteam/product team
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

    df["PROGRAM"]  = df["PROGRAM"].map(normalize_key)
    df["SUB_TEAM"] = df["SUB_TEAM"].map(normalize_key)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"]     = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"]    = pd.to_numeric(df["HOURS"], errors="coerce")

    return df.dropna(subset=["PROGRAM", "SUB_TEAM", "DATE", "COST_SET", "HOURS"])

def load_inputs() -> pd.DataFrame:
    # prefer in-memory df if present (to match your notebook flow)
    if not FORCE_READ_FILES:
        for name in ["cobra_merged_df", "cobra_df", "df", "raw_df"]:
            if name in globals() and isinstance(globals()[name], pd.DataFrame) and len(globals()[name]) > 0:
                return coerce_columns(globals()[name])

    files = list(INPUT_FILES)
    if not files:
        candidates = []
        for pat in ["*.csv", "*.xlsx", "*.xls"]:
            candidates += list(Path(".").glob(pat))
        candidates = sorted(candidates, key=lambda p: ("cobra" not in p.name.lower(), p.name.lower()))
        files = [str(p) for p in candidates[:30]]

    if not files:
        raise FileNotFoundError("No input files found and no in-memory dataframe found (cobra_merged_df/df/...).")

    frames = []
    for fp in files:
        p = Path(fp)
        if not p.exists():
            continue
        if p.suffix.lower() == ".csv":
            frames.append(pd.read_csv(p))
        elif p.suffix.lower() in [".xlsx", ".xls"]:
            xls = pd.ExcelFile(p)
            for sh in xls.sheet_names:
                frames.append(pd.read_excel(p, sheet_name=sh))

    if not frames:
        raise FileNotFoundError("No readable input data found from INPUT_FILES / auto-discovery.")

    return coerce_columns(pd.concat(frames, ignore_index=True))

def preserve_comments(existing_path: Path, sheet: str, df_new: pd.DataFrame, key_cols, comment_col: str) -> pd.DataFrame:
    if (not existing_path.exists()) or (comment_col not in df_new.columns):
        return df_new
    try:
        old = pd.read_excel(existing_path, sheet_name=sheet)
    except Exception:
        return df_new
    if old is None or len(old) == 0:
        return df_new
    if (comment_col not in old.columns) or (not all(k in old.columns for k in key_cols)):
        return df_new

    old = old[key_cols + [comment_col]].copy().dropna(subset=key_cols)
    old = old.rename(columns={comment_col: f"{comment_col}_old"})
    out = df_new.merge(old, on=key_cols, how="left")
    oldcol = f"{comment_col}_old"
    if oldcol in out.columns:
        oldvals = out[oldcol]
        mask = oldvals.notna() & (oldvals.astype(str).str.strip() != "")
        out.loc[mask, comment_col] = oldvals.loc[mask]
        out = out.drop(columns=[oldcol])
    return out

def pivot_costsets(df: pd.DataFrame, idx_cols, val_col, needed_costsets):
    if df.empty:
        out = df[idx_cols].drop_duplicates().copy()
        for cs in needed_costsets:
            out[cs] = np.nan
        return out
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values=val_col, aggfunc="sum").reset_index()
    for cs in needed_costsets:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

# -------------------------
# LOAD + FILTER PROGRAMS
# -------------------------
base = load_inputs()
base = base[base["PROGRAM"].isin([normalize_key(p) for p in PROGRAMS_KEEP])].copy()

# -------------------------
# AS-OF / NEXT PERIOD WINDOW
# -------------------------
today = _to_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = last_thursday_prev_month(today)
month_after = add_month(AS_OF_DATE, 1)
NEXT_PERIOD_END = last_thursday_of_month(month_after.year, month_after.month)

YEAR_FILTER = AS_OF_DATE.year
YEAR_START  = date(YEAR_FILTER, 1, 1)
YEAR_END    = date(YEAR_FILTER, 12, 31)

print("As-of logic")
print("TODAY:", today)
print("AS_OF_DATE (last Thu prev month):", AS_OF_DATE)
print("NEXT_PERIOD_END (last Thu next month):", NEXT_PERIOD_END)
print("YEAR_FILTER:", YEAR_FILTER)
print("BCWS_SCALE_FACTOR:", BCWS_SCALE_FACTOR)

# -------------------------
# CORE FILTERS
# -------------------------
NEEDED_COSTSETS = ["BCWS", "BCWP", "ACWP", "ETC"]
base_to_asof = base[base["DATE"] <= AS_OF_DATE].copy()
base_year    = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()

# -------------------------
# CTD: SUM UP TO AS_OF_DATE
# -------------------------
ctd_sub = (
    base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)]
    .groupby(["PROGRAM", "SUB_TEAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "CTD_HRS"})
)
ctd_prog = (
    base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)]
    .groupby(["PROGRAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "CTD_HRS"})
)

# -------------------------
# LSD FIX: LATEST DATE <= AS_OF_DATE PER KEY
# -------------------------
tmp_sub = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].sort_values(
    ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE"]
).copy()
sub_last_date = (
    tmp_sub.groupby(["PROGRAM", "SUB_TEAM", "COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE": "LSD_DATE"})
)
lsd_sub = (
    tmp_sub.merge(sub_last_date, on=["PROGRAM", "SUB_TEAM", "COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM", "SUB_TEAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "LSD_HRS"})
)

tmp_prog = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].sort_values(
    ["PROGRAM", "COST_SET", "DATE"]
).copy()
prog_last_date = (
    tmp_prog.groupby(["PROGRAM", "COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE": "LSD_DATE"})
)
lsd_prog = (
    tmp_prog.merge(prog_last_date, on=["PROGRAM", "COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "LSD_HRS"})
)

# -------------------------
# PIVOT COSTSETS TO WIDE
# -------------------------
ctd_sub_p  = pivot_costsets(ctd_sub,  ["PROGRAM", "SUB_TEAM"], "CTD_HRS", NEEDED_COSTSETS)
lsd_sub_p  = pivot_costsets(lsd_sub,  ["PROGRAM", "SUB_TEAM"], "LSD_HRS", NEEDED_COSTSETS)
ctd_prog_p = pivot_costsets(ctd_prog, ["PROGRAM"],            "CTD_HRS", NEEDED_COSTSETS)
lsd_prog_p = pivot_costsets(lsd_prog, ["PROGRAM"],            "LSD_HRS", NEEDED_COSTSETS)

# -------------------------
# BCWS SCALE FIX (SPI inflation fix)
# -------------------------
for dfp in [ctd_sub_p, lsd_sub_p, ctd_prog_p, lsd_prog_p]:
    dfp["BCWS"] = pd.to_numeric(dfp["BCWS"], errors="coerce") * float(BCWS_SCALE_FACTOR)

# ============================================================
# SHEET 1: PROGRAM_Overview (Power BI friendly)
# One row per Program, SPI/CPI as columns + color fields
# ============================================================
prog = ctd_prog_p.merge(lsd_prog_p, on=["PROGRAM"], how="outer", suffixes=("_CTD", "_LSD")).copy()
prog.rename(columns={"PROGRAM": "ProgramID"}, inplace=True)

# SPI = BCWP / BCWS, CPI = BCWP / ACWP
prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

prog["SPI_CTD_Color"] = prog["SPI_CTD"].map(color_spi_cpi_bei)
prog["SPI_LSD_Color"] = prog["SPI_LSD"].map(color_spi_cpi_bei)
prog["CPI_CTD_Color"] = prog["CPI_CTD"].map(color_spi_cpi_bei)
prog["CPI_LSD_Color"] = prog["CPI_LSD"].map(color_spi_cpi_bei)

comment_prog = "Comments / Root Cause & Corrective Actions"
program_overview = prog[
    ["ProgramID",
     "SPI_CTD", "SPI_LSD", "SPI_CTD_Color", "SPI_LSD_Color",
     "CPI_CTD", "CPI_LSD", "CPI_CTD_Color", "CPI_LSD_Color"]
].copy()
program_overview[comment_prog] = ""
program_overview = program_overview.sort_values(["ProgramID"]).reset_index(drop=True)

# Preserve existing comments
program_overview = preserve_comments(
    OUTPUT_XLSX, "Program_Overview", program_overview,
    key_cols=["ProgramID"], comment_col=comment_prog
)

# ============================================================
# SHEET 2: ProductTeam_SPI_CPI (formerly SubTeam)
# One row per (ProgramID, Product Team) with SPI/CPI CTD/LSD + color fields
# ============================================================
sub = ctd_sub_p.merge(lsd_sub_p, on=["PROGRAM", "SUB_TEAM"], how="outer", suffixes=("_CTD", "_LSD")).copy()
sub.rename(columns={"PROGRAM": "ProgramID", "SUB_TEAM": "Product Team"}, inplace=True)

sub["SPI_CTD"] = safe_div(sub["BCWP_CTD"], sub["BCWS_CTD"])
sub["SPI_LSD"] = safe_div(sub["BCWP_LSD"], sub["BCWS_LSD"])
sub["CPI_CTD"] = safe_div(sub["BCWP_CTD"], sub["ACWP_CTD"])
sub["CPI_LSD"] = safe_div(sub["BCWP_LSD"], sub["ACWP_LSD"])

sub["SPI_CTD_Color"] = sub["SPI_CTD"].map(color_spi_cpi_bei)
sub["SPI_LSD_Color"] = sub["SPI_LSD"].map(color_spi_cpi_bei)
sub["CPI_CTD_Color"] = sub["CPI_CTD"].map(color_spi_cpi_bei)
sub["CPI_LSD_Color"] = sub["CPI_LSD"].map(color_spi_cpi_bei)

comment_pt = "Cause & Corrective Actions"
productteam_spi_cpi = sub[
    ["ProgramID", "Product Team",
     "SPI_LSD", "SPI_CTD", "CPI_LSD", "CPI_CTD",
     "SPI_LSD_Color", "SPI_CTD_Color", "CPI_LSD_Color", "CPI_CTD_Color"]
].copy()
productteam_spi_cpi[comment_pt] = ""
productteam_spi_cpi = productteam_spi_cpi.sort_values(["ProgramID", "Product Team"]).reset_index(drop=True)

productteam_spi_cpi = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_SPI_CPI", productteam_spi_cpi,
    key_cols=["ProgramID", "Product Team"], comment_col=comment_pt
)

# ============================================================
# SHEET 3: ProductTeam_BAC_EAC_VAC (VAC coloring included)
# BAC = YEAR total BCWS (scaled)
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# VAC_BAC = VAC / BAC, VAC_Color uses VAC/BAC thresholds
# ============================================================
bcws_year = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM", "SUB_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "BAC"})
)
bcws_year["BAC"] = pd.to_numeric(bcws_year["BAC"], errors="coerce") * float(BCWS_SCALE_FACTOR)

acwp_ctd = ctd_sub_p[["PROGRAM", "SUB_TEAM", "ACWP"]].rename(columns={"ACWP": "ACWP_CTD"})
etc_ctd  = ctd_sub_p[["PROGRAM", "SUB_TEAM", "ETC"]].rename(columns={"ETC": "ETC_CTD"})
eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM", "SUB_TEAM"], how="outer")
eac["ACWP_CTD"] = pd.to_numeric(eac["ACWP_CTD"], errors="coerce").fillna(0.0)
eac["ETC_CTD"]  = pd.to_numeric(eac["ETC_CTD"],  errors="coerce").fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bcws_year.merge(eac[["PROGRAM", "SUB_TEAM", "EAC"]], on=["PROGRAM", "SUB_TEAM"], how="outer")
bac_eac["BAC"] = pd.to_numeric(bac_eac["BAC"], errors="coerce")
bac_eac["EAC"] = pd.to_numeric(bac_eac["EAC"], errors="coerce")
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]

bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

productteam_bac_eac_vac = bac_eac.rename(columns={"PROGRAM": "ProgramID", "SUB_TEAM": "Product Team"}).copy()
productteam_bac_eac_vac[comment_pt] = ""
productteam_bac_eac_vac = productteam_bac_eac_vac[
    ["ProgramID", "Product Team", "BAC", "EAC", "VAC", "VAC_BAC", "VAC_Color", comment_pt]
].sort_values(["ProgramID", "Product Team"]).reset_index(drop=True)

productteam_bac_eac_vac = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", productteam_bac_eac_vac,
    key_cols=["ProgramID", "Product Team"], comment_col=comment_pt
)

# ============================================================
# SHEET 4: Program_Manpower (% Var coloring included)
# Demand Hours = BCWS_CTD (scaled)
# Actual Hours = ACWP_CTD
# % Var = (Actual/Demand)*100
# Next Mo BCWS Hours & Next Mo ETC Hours from window (AS_OF_DATE, NEXT_PERIOD_END]
# ============================================================
man = ctd_prog_p.rename(columns={"PROGRAM": "ProgramID", "BCWS": "Demand Hours", "ACWP": "Actual Hours"}).copy()
man["Demand Hours"] = pd.to_numeric(man["Demand Hours"], errors="coerce")
man["Actual Hours"] = pd.to_numeric(man["Actual Hours"], errors="coerce")
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_program_manpower)

next_window = base[
    (base["DATE"] > AS_OF_DATE) &
    (base["DATE"] <= NEXT_PERIOD_END) &
    (base["COST_SET"].isin(["BCWS", "ETC"]))
].copy()

next_prog = (
    next_window.groupby(["PROGRAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog["BCWS"] = pd.to_numeric(next_prog["BCWS"], errors="coerce") * float(BCWS_SCALE_FACTOR)

next_prog = next_prog.rename(columns={
    "PROGRAM": "ProgramID",
    "BCWS": "Next Mo BCWS Hours",
    "ETC": "Next Mo ETC Hours"
})

program_manpower = man.merge(
    next_prog[["ProgramID", "Next Mo BCWS Hours", "Next Mo ETC Hours"]],
    on="ProgramID", how="left"
)
program_manpower[comment_pt] = ""
program_manpower = program_manpower[
    ["ProgramID", "Demand Hours", "Actual Hours", "% Var", "% Var Color",
     "Next Mo BCWS Hours", "Next Mo ETC Hours", comment_pt]
].sort_values(["ProgramID"]).reset_index(drop=True)

program_manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower", program_manpower,
    key_cols=["ProgramID"], comment_col=comment_pt
)

# ============================================================
# WRITE ONE EXCEL (Program_Overview FIRST)
# ============================================================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)                # 1st
    productteam_spi_cpi.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)         # 2nd
    productteam_bac_eac_vac.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False) # 3rd
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)               # 4th

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

# ============================================================
# QUICK DIAGNOSTICS (missingness)
# ============================================================
def miss_rate(s): 
    return float(pd.to_numeric(s, errors="coerce").isna().mean())

print("\nQuick missingness check:")
print("Program_Overview SPI_CTD missing:", miss_rate(program_overview["SPI_CTD"]))
print("Program_Overview CPI_CTD missing:", miss_rate(program_overview["CPI_CTD"]))
print("ProductTeam_SPI_CPI SPI_LSD missing:", miss_rate(productteam_spi_cpi["SPI_LSD"]))
print("ProductTeam_BAC_EAC_VAC VAC missing:", miss_rate(productteam_bac_eac_vac["VAC"]))
print("Program_Manpower % Var missing:", miss_rate(program_manpower["% Var"]))

display(program_overview.head(20))
display(productteam_spi_cpi.head(20))
display(productteam_bac_eac_vac.head(20))
display(program_manpower.head(20))