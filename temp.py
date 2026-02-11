# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# START FROM: cobra_merged_df (your cleaned LONG dataset)
#
# FIXES:
#   - LSD is computed using a single status-period WINDOW
#       (prev_status_end, AS_OF_DATE] for ALL cost sets
#     NOT "latest date per cost set"
#   - Program_Overview is WIDE: SPI/CPI columns (no Metric col)
#   - Sheet names unchanged
# ============================================================

import re
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS
# -------------------------
PROGRAMS_KEEP = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

# Make up a status period (your request):
# Default: AS_OF_DATE = 2 weeks prior to today (you can override)
TODAY_OVERRIDE = None       # e.g. "2026-02-10"
ASOF_OVERRIDE  = None       # e.g. "2026-01-30"
STATUS_CADENCE_DAYS = 7     # set to 14 if your LSD is biweekly in Cobra

# -------------------------
# PPT COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"  # 142,180,227
CLR_GREEN      = "#339966"  # 051,153,102
CLR_YELLOW     = "#FFFF99"  # 255,255,153
CLR_RED        = "#C0504D"  # 192,080,077

def _to_num(x): return pd.to_numeric(x, errors="coerce")

def color_spi_cpi(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

def color_vac_over_bac(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

def color_manpower_pct(pct):
    pct = _to_num(pct)
    if pd.isna(pct): return None
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

def safe_div(a, b):
    a = _to_num(a); b = _to_num(b)
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

# -------------------------
# Helpers
# -------------------------
def as_date(x):
    if x is None: return None
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    return pd.to_datetime(x, errors="coerce").date()

def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip().upper().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", s).strip()

def normalize_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s if s else None

def normalize_cost_set(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s).replace("-", "").replace("_", "")
    return s

def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # map required columns from synonyms
    colmap = {}
    for c in ["PROGRAM","PROGRAMID","PROG","PROJECT","PROGRAM_NAME"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break
    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME","CA","CONTROL_ACCOUNT"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break
    for c in ["DATE","PERIOD_END","STATUS_DATE","AS_OF_DATE","PERIODEND"]:
        if c in df.columns: colmap[c] = "DATE"; break
    for c in ["COST_SET","COSTSET","COST_SET_NAME","COST_CATEGORY","COST-SET"]:
        if c in df.columns: colmap[c] = "COST_SET"; break
    for c in ["HOURS","HRS","VALUE","AMOUNT","TOTAL_HOURS"]:
        if c in df.columns: colmap[c] = "HOURS"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"cobra_merged_df missing columns {missing}. Found: {list(df.columns)}")

    df["PROGRAM"] = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

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

def pivot_costsets(df, idx_cols, needed):
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values="HOURS", aggfunc="sum").reset_index()
    for cs in needed:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

# ============================================================
# RUN
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty.")

base = coerce_to_long(cobra_merged_df)

# keep programs
keep_norm = [normalize_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()

# choose AS_OF_DATE (make one up if not provided)
today = as_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = as_date(ASOF_OVERRIDE) if ASOF_OVERRIDE else (today - timedelta(days=14))
PREV_STATUS_END = AS_OF_DATE - timedelta(days=STATUS_CADENCE_DAYS)

# next-month window end (for manpower "next mo" fields)
NEXT_PERIOD_END = AS_OF_DATE + timedelta(days=STATUS_CADENCE_DAYS)

print("TODAY:", today)
print("AS_OF_DATE (status end):", AS_OF_DATE)
print("PREV_STATUS_END:", PREV_STATUS_END)
print("LSD window:", f"({PREV_STATUS_END} , {AS_OF_DATE}]")
print("NEXT_PERIOD_END (for next window):", NEXT_PERIOD_END)

NEEDED = ["BCWS","BCWP","ACWP","ETC"]
base_evms = base[base["COST_SET"].isin(NEEDED)].copy()

# CTD: <= AS_OF_DATE
ctd = base_evms[base_evms["DATE"] <= AS_OF_DATE].copy()

# LSD: (PREV_STATUS_END, AS_OF_DATE]
lsd = base_evms[(base_evms["DATE"] > PREV_STATUS_END) & (base_evms["DATE"] <= AS_OF_DATE)].copy()

# YEAR (for BAC)
YEAR_FILTER = AS_OF_DATE.year
YEAR_START = date(YEAR_FILTER, 1, 1)
YEAR_END   = date(YEAR_FILTER, 12, 31)
base_year = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()

# -------------------------
# PROGRAM pivots (CTD/LSD)
# -------------------------
ctd_prog_w = pivot_costsets(ctd.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum(), ["PROGRAM"], NEEDED)
lsd_prog_w = pivot_costsets(lsd.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum(), ["PROGRAM"], NEEDED)

ctd_prog_w = ctd_prog_w.rename(columns={"PROGRAM":"ProgramID"})
lsd_prog_w = lsd_prog_w.rename(columns={"PROGRAM":"ProgramID"})

# -------------------------
# PRODUCT TEAM pivots (CTD/LSD)
# -------------------------
ctd_pt_w = pivot_costsets(ctd.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum(), ["PROGRAM","PRODUCT_TEAM"], NEEDED)
lsd_pt_w = pivot_costsets(lsd.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum(), ["PROGRAM","PRODUCT_TEAM"], NEEDED)

ctd_pt_w = ctd_pt_w.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
lsd_pt_w = lsd_pt_w.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})

# ============================================================
# PROGRAM OVERVIEW (WIDE)
# ============================================================
prog = ctd_prog_w.merge(lsd_prog_w, on="ProgramID", how="outer", suffixes=("_CTD","_LSD"))

prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

prog["SPI_CTD_Color"] = pd.Series(prog["SPI_CTD"]).map(color_spi_cpi)
prog["SPI_LSD_Color"] = pd.Series(prog["SPI_LSD"]).map(color_spi_cpi)
prog["CPI_CTD_Color"] = pd.Series(prog["CPI_CTD"]).map(color_spi_cpi)
prog["CPI_LSD_Color"] = pd.Series(prog["CPI_LSD"]).map(color_spi_cpi)

comment_overview = "Comments / Root Cause & Corrective Actions"
prog[comment_overview] = ""

Program_Overview = prog[
    ["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     comment_overview]
].sort_values("ProgramID").reset_index(drop=True)

Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID"], comment_overview)

# ============================================================
# PRODUCT TEAM SPI/CPI
# ============================================================
pt = ctd_pt_w.merge(lsd_pt_w, on=["ProgramID","Product Team"], how="outer", suffixes=("_CTD","_LSD"))

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
# ============================================================
bcws_year = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team","HOURS":"BAC"})
)

# CTD ACWP/ETC from ctd_pt_w
acwp_ctd = ctd_pt_w[["ProgramID","Product Team","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_pt_w[["ProgramID","Product Team","ETC"]].rename(columns={"ETC":"ETC_CTD"})

eac = acwp_ctd.merge(etc_ctd, on=["ProgramID","Product Team"], how="outer")
eac["ACWP_CTD"] = _to_num(eac["ACWP_CTD"]).fillna(0.0)
eac["ETC_CTD"]  = _to_num(eac["ETC_CTD"]).fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bcws_year.merge(eac[["ProgramID","Product Team","EAC"]], on=["ProgramID","Product Team"], how="outer")
bac_eac["BAC"] = _to_num(bac_eac["BAC"])
bac_eac["EAC"] = _to_num(bac_eac["EAC"])
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac.copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[
    ["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color",comment_pt]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

ProductTeam_BAC_EAC_VAC = preserve_comments(OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC, ["ProgramID","Product Team"], comment_pt)

# ============================================================
# PROGRAM MANPOWER
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual / Demand * 100
# Next window BCWS/ETC = (AS_OF_DATE, NEXT_PERIOD_END]
# ============================================================
man = ctd_prog_w.rename(columns={"BCWS":"Demand Hours", "ACWP":"Actual Hours"}).copy()
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
# SANITY CHECKS (helps you validate "two weeks of BCWS" issue)
# ============================================================
print("\n--- LSD window sums sanity (program-level) ---")
chk = lsd.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
print(chk.pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum"))

print("\n--- Program Overview preview ---")
display(Program_Overview)

# ============================================================
# WRITE EXCEL (sheet order locked)
# ============================================================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")