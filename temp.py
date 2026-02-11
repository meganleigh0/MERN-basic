# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# MUST START FROM: cobra_merged_df (your cleaned LONG dataset)
# No BCWS scaling / no assumptions. SPI/CPI computed directly.
#
# Output sheets (NAMES LOCKED):
#   Program_Overview
#   ProductTeam_SPI_CPI
#   ProductTeam_BAC_EAC_VAC
#   Program_Manpower
# ============================================================

import re
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS (edit if needed)
# -------------------------
PROGRAMS_KEEP = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]  # must match your slicer labels
TODAY_OVERRIDE = None  # e.g. "2026-02-10" (leave None for today)
ASOF_OVERRIDE = None   # e.g. "2026-01-29" (leave None to auto compute last Thu prev month)
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

# -------------------------
# PPT COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"  # 142,180,227
CLR_GREEN      = "#339966"  # 051,153,102
CLR_YELLOW     = "#FFFF99"  # 255,255,153
CLR_RED        = "#C0504D"  # 192,080,077

def _to_num(x):
    return pd.to_numeric(x, errors="coerce")

# SPI/CPI thresholds (your PPT rounded bands)
def color_spi_cpi(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

# VAC/BAC thresholds (PPT)
def color_vac_over_bac(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

# Manpower %Var thresholds (PPT)
def color_manpower_pct(pct):
    pct = _to_num(pct)
    if pd.isna(pct): return None
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

def safe_div(a, b):
    a = _to_num(a)
    b = _to_num(b)
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

# -------------------------
# DATE HELPERS
# -------------------------
def as_date(x):
    if x is None: return None
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    return pd.to_datetime(x, errors="coerce").date()

def last_thursday_of_month(year, month):
    last = date(year, 12, 31) if month == 12 else (date(year, month + 1, 1) - timedelta(days=1))
    offset = (last.weekday() - 3) % 7  # Thu=3
    return last - timedelta(days=offset)

def last_thursday_prev_month(d):
    y, m = d.year, d.month
    if m == 1: y, m = y - 1, 12
    else: m -= 1
    return last_thursday_of_month(y, m)

def add_month(d, months=1):
    y, m = d.year, d.month + months
    while m > 12:
        y += 1; m -= 12
    while m < 1:
        y -= 1; m += 12
    last_day = 31 if m == 12 else (date(y, m + 1, 1) - timedelta(days=1)).day
    return date(y, m, min(d.day, last_day))

# -------------------------
# COLUMN NORMALIZATION (NO assumptions on names, but STRICT on meaning)
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip().upper().replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    # keep A-Z0-9 only (KUW stays KUW even if "K U W")
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s if s else None

def normalize_cost_set(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    # YOU said you already fixed cost-sets, so we do NOT remap here.
    # We just normalize to consistent tokens: BCWS/BCWP/ACWP/ETC/BAC/EAC/VAC etc.
    return s

def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # detect if this is already an OUTPUT/WIDE table (your error screenshot)
    wide_markers = {"BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD"}
    if len(wide_markers.intersection(set(df.columns))) >= 2 and "DATE" not in df.columns and "COST_SET" not in df.columns:
        raise ValueError(
            "cobra_merged_df looks like a WIDE/OUTPUT table (has *_CTD columns) not the raw LONG cobra dataset.\n"
            "Fix: set cobra_merged_df to your cleaned long data with Program/ProductTeam/Date/Cost_Set/Hours.\n"
            f"Columns found: {list(df.columns)}"
        )

    # map required columns from synonyms
    colmap = {}

    # PROGRAM
    for c in ["PROGRAM","PROGRAMID","PROG","PROJECT","IPT_PROGRAM","PROGRAM_NAME"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break

    # PRODUCT TEAM
    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME","SUB_TEAM_NAME","CA","CONTROL_ACCOUNT"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break

    # DATE
    for c in ["DATE","PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
        if c in df.columns: colmap[c] = "DATE"; break

    # COST SET
    for c in ["COST_SET","COSTSET","COST_SET_NAME","COST_CATEGORY","COSTSETNAME","COST_SET_TYPE","COST-SET"]:
        if c in df.columns: colmap[c] = "COST_SET"; break

    # HOURS
    for c in ["HOURS","HRS","VALUE","AMOUNT","HOURS_WORKED","TOTAL_HOURS"]:
        if c in df.columns: colmap[c] = "HOURS"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing required columns {missing} in cobra_merged_df.\n"
            f"Columns found: {list(df.columns)}"
        )

    df["PROGRAM"] = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

# -------------------------
# COMMENTS PRESERVATION
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
    if (comment_col not in old.columns) or (not all(k in old.columns for k in key_cols)):
        return df_new

    old = old[key_cols + [comment_col]].copy().dropna(subset=key_cols)
    old = old.rename(columns={comment_col: f"{comment_col}_old"})
    out = df_new.merge(old, on=key_cols, how="left")
    oldcol = f"{comment_col}_old"
    if oldcol in out.columns:
        mask = out[oldcol].notna() & (out[oldcol].astype(str).str.strip() != "")
        out.loc[mask, comment_col] = out.loc[mask, oldcol]
        out = out.drop(columns=[oldcol])
    return out

def pivot_costsets(df, idx_cols, val_col, needed):
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values=val_col, aggfunc="sum").reset_index()
    for cs in needed:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

# ============================================================
# START: cobra_merged_df ONLY (as requested)
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_to_long(cobra_merged_df)

# Filter programs
keep_norm = [normalize_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()

# Dates
today = as_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = as_date(ASOF_OVERRIDE) if ASOF_OVERRIDE else last_thursday_prev_month(today)
month_after = add_month(AS_OF_DATE, 1)
NEXT_PERIOD_END = last_thursday_of_month(month_after.year, month_after.month)

YEAR_FILTER = AS_OF_DATE.year
YEAR_START = date(YEAR_FILTER, 1, 1)
YEAR_END   = date(YEAR_FILTER, 12, 31)

print("TODAY:", today)
print("AS_OF_DATE:", AS_OF_DATE)
print("NEXT_PERIOD_END:", NEXT_PERIOD_END)
print("YEAR_FILTER:", YEAR_FILTER)

# Cost sets needed (STRICT EVMS math)
NEEDED = ["BCWS","BCWP","ACWP","ETC"]

# Keep only what we need for SPI/CPI + ETC/EAC/VAC logic
base_evms = base[base["COST_SET"].isin(NEEDED)].copy()

# Windowed subsets
to_asof = base_evms[base_evms["DATE"] <= AS_OF_DATE].copy()
base_year = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()

# -------------------------
# CTD sums
# -------------------------
ctd_pt = (
    to_asof.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)
ctd_prog = (
    to_asof.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# -------------------------
# LSD FIX: latest DATE <= AS_OF_DATE per key
# -------------------------
tmp_pt = to_asof.sort_values(["PROGRAM","PRODUCT_TEAM","COST_SET","DATE"]).copy()
pt_last = (
    tmp_pt.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE":"LSD_DATE"})
)
lsd_pt = (
    tmp_pt.merge(pt_last, on=["PROGRAM","PRODUCT_TEAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

tmp_prog = to_asof.sort_values(["PROGRAM","COST_SET","DATE"]).copy()
prog_last = (
    tmp_prog.groupby(["PROGRAM","COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE":"LSD_DATE"})
)
lsd_prog = (
    tmp_prog.merge(prog_last, on=["PROGRAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

# -------------------------
# Pivot wide
# -------------------------
ctd_pt_w   = pivot_costsets(ctd_pt,   ["PROGRAM","PRODUCT_TEAM"], "CTD_HRS", NEEDED)
lsd_pt_w   = pivot_costsets(lsd_pt,   ["PROGRAM","PRODUCT_TEAM"], "LSD_HRS", NEEDED)
ctd_prog_w = pivot_costsets(ctd_prog, ["PROGRAM"],              "CTD_HRS", NEEDED)
lsd_prog_w = pivot_costsets(lsd_prog, ["PROGRAM"],              "LSD_HRS", NEEDED)

# ============================================================
# PROGRAM OVERVIEW (LONG)
# ProgramID | Metric | CTD | LSD | CTD_Color | LSD_Color | Comments...
# ============================================================
prog = ctd_prog_w.merge(lsd_prog_w, on=["PROGRAM"], how="outer", suffixes=("_CTD","_LSD")).copy()
prog.rename(columns={"PROGRAM":"ProgramID"}, inplace=True)

prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

rows = []
for metric in ["SPI","CPI"]:
    if metric == "SPI":
        ctd = prog["SPI_CTD"]; lsd = prog["SPI_LSD"]
    else:
        ctd = prog["CPI_CTD"]; lsd = prog["CPI_LSD"]
    rows.append(pd.DataFrame({
        "ProgramID": prog["ProgramID"],
        "Metric": metric,
        "CTD": ctd,
        "LSD": lsd,
        "CTD_Color": ctd.map(color_spi_cpi),
        "LSD_Color": lsd.map(color_spi_cpi),
    }))

Program_Overview = pd.concat(rows, ignore_index=True)
comment_overview = "Comments / Root Cause & Corrective Actions"
Program_Overview[comment_overview] = ""
Program_Overview = Program_Overview.sort_values(["ProgramID","Metric"]).reset_index(drop=True)
Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID","Metric"], comment_overview)

# ============================================================
# PRODUCT TEAM SPI/CPI
# ============================================================
pt = ctd_pt_w.merge(lsd_pt_w, on=["PROGRAM","PRODUCT_TEAM"], how="outer", suffixes=("_CTD","_LSD")).copy()
pt.rename(columns={"PROGRAM":"ProgramID", "PRODUCT_TEAM":"Product Team"}, inplace=True)

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

ProductTeam_SPI_CPI = pt[["ProgramID","Product Team","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]].copy()
ProductTeam_SPI_CPI["SPI_LSD_Color"] = ProductTeam_SPI_CPI["SPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = ProductTeam_SPI_CPI["SPI_CTD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = ProductTeam_SPI_CPI["CPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = ProductTeam_SPI_CPI["CPI_CTD"].map(color_spi_cpi)

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt)

# ============================================================
# PRODUCT TEAM BAC/EAC/VAC
# BAC = YEAR total BCWS
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# Color based on VAC/BAC
# ============================================================
bcws_year = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"BAC"})
)

acwp_ctd = ctd_pt_w[["PROGRAM","PRODUCT_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_pt_w[["PROGRAM","PRODUCT_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","PRODUCT_TEAM"], how="outer")
eac["ACWP_CTD"] = _to_num(eac["ACWP_CTD"]).fillna(0.0)
eac["ETC_CTD"]  = _to_num(eac["ETC_CTD"]).fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bcws_year.merge(eac[["PROGRAM","PRODUCT_TEAM","EAC"]], on=["PROGRAM","PRODUCT_TEAM"], how="outer")
bac_eac["BAC"] = _to_num(bac_eac["BAC"])
bac_eac["EAC"] = _to_num(bac_eac["EAC"])
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}).copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[
    ["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color",comment_pt]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC,
    ["ProgramID","Product Team"], comment_pt
)

# ============================================================
# PROGRAM MANPOWER
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual / Demand * 100
# Add color column for % Var
# Next Mo BCWS/ETC from (AS_OF_DATE, NEXT_PERIOD_END]
# ============================================================
man = ctd_prog_w.rename(columns={"PROGRAM":"ProgramID", "BCWS":"Demand Hours", "ACWP":"Actual Hours"}).copy()
man["Demand Hours"] = _to_num(man["Demand Hours"])
man["Actual Hours"] = _to_num(man["Actual Hours"])
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

next_window = base_evms[
    (base_evms["DATE"] > AS_OF_DATE) &
    (base_evms["DATE"] <= NEXT_PERIOD_END) &
    (base_evms["COST_SET"].isin(["BCWS","ETC"]))
].copy()

next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog = next_prog.rename(columns={
    "PROGRAM":"ProgramID",
    "BCWS":"Next Mo BCWS Hours",
    "ETC":"Next Mo ETC Hours"
})

Program_Manpower = man.merge(
    next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]],
    on="ProgramID", how="left"
)
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next Mo BCWS Hours","Next Mo ETC Hours",comment_pt]
].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ============================================================
# HARD VERIFICATION OUTPUTS (SPI sanity)
# ============================================================
print("\n--- SPI/CPI sanity checks (NO scaling applied) ---")
print("Program-level SPI_CTD median by program:")
print(ProductTeam_SPI_CPI.groupby("ProgramID")["SPI_CTD"].median(numeric_only=True))
print("\nProgram-level SPI_LSD median by program:")
print(ProductTeam_SPI_CPI.groupby("ProgramID")["SPI_LSD"].median(numeric_only=True))

print("\nKUW check (ABRAMS 22):")
kuw = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["ProgramID"]=="ABRAMS 22") & (ProductTeam_SPI_CPI["Product Team"]=="KUW")]
print(kuw if len(kuw) else "KUW not present in ProductTeam_SPI_CPI output (check base data for KUW rows in BCWS/BCWP/ACWP/ETC).")

# ============================================================
# WRITE EXCEL (sheet order matters)
# ============================================================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

# ============================================================
# POWER BI: make Program_Overview look like your 2-row SPI/CPI card
# ============================================================
print("""
Power BI formatting for the overview card:
1) Use table visual (or matrix).
2) Add fields in this order:
   - Metric
   - CTD
   - LSD
   - Comments / Root Cause & Corrective Actions
3) Add slicer for ProgramID (select one program).
4) Conditional formatting:
   - For CTD column: Background color -> Format by Field value -> Based on CTD_Color
   - For LSD column: Background color -> Format by Field value -> Based on LSD_Color
""")
