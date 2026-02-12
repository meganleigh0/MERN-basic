# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL) — FIXED, CONSISTENT 4-WEEK LSD
# INPUT MUST START FROM: cobra_merged_df (LONG format)
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
# SETTINGS
# -------------------------
TODAY_OVERRIDE = None     # e.g. "2026-02-12"
LSD_WEEKS = 4             # FIXED WINDOW (4 weeks)
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

# -------------------------
# COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"
CLR_GREEN      = "#339966"
CLR_YELLOW     = "#FFFF99"
CLR_RED        = "#C0504D"

def _to_num(x):
    return pd.to_numeric(x, errors="coerce")

def safe_div(a, b):
    a = _to_num(a)
    b = _to_num(b)
    out = np.where((b == 0) | pd.isna(b), np.nan, a / b)
    return out

# SPI/CPI thresholds (your PPT bands)
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

# -------------------------
# LIGHT NORMALIZATION ONLY (do NOT remap cost sets)
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)          # KUW / K U W -> KUW
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s if s else None

def normalize_cost_set(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    # expected already mapped to BCWS/BCWP/ACWP/ETC (or close variants)
    # normalize common variants safely without "re-mapping" business logic:
    if s in ["BCWS", "BCWP", "ACWP", "ETC"]:
        return s
    # allow "COSTSET" already clean but with extra text (rare):
    return s

def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # detect and block if already wide output
    wide_markers = {"SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD"}
    if len(wide_markers.intersection(set(df.columns))) >= 2 and "COST_SET" not in df.columns:
        raise ValueError(
            "cobra_merged_df looks like a WIDE output table, not the LONG cobra dataset.\n"
            "Need LONG rows with Program/ProductTeam/Date/Cost_Set/Value."
        )

    # column synonyms
    colmap = {}

    for c in ["PROGRAM","PROG","PROJECT","PROGRAM_NAME","IPT_PROGRAM"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break

    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME","CONTROL_ACCOUNT","CA"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break

    for c in ["DATE","PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
        if c in df.columns: colmap[c] = "DATE"; break

    for c in ["COST_SET","COSTSET","COST_SET_NAME","COSTSETNAME","COST_CATEGORY","COST_SET_TYPE"]:
        if c in df.columns: colmap[c] = "COST_SET"; break

    # value column can be HOURS or VAL etc.
    for c in ["HOURS","HRS","VAL","VALUE","AMOUNT","TOTAL_HOURS"]:
        if c in df.columns: colmap[c] = "VAL"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","VAL"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Found: {list(df.columns)}")

    df["PROGRAM"] = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["VAL"]  = pd.to_numeric(df["VAL"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","VAL"])
    return df

# -------------------------
# COMMENT PRESERVATION
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

# -------------------------
# START
# -------------------------
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty.")

base = coerce_to_long(cobra_merged_df)

# keep only EVMS cost sets we need (no remapping, just select)
EVMS_SETS = ["BCWS","BCWP","ACWP","ETC"]
base = base[base["COST_SET"].isin(EVMS_SETS)].copy()

today = pd.to_datetime(TODAY_OVERRIDE).date() if TODAY_OVERRIDE else date.today()

# LSD_END = latest date in data <= today
base_le_today = base[base["DATE"] <= today]
if base_le_today.empty:
    raise ValueError("No rows in cobra_merged_df have DATE <= today (or TODAY_OVERRIDE). Check your dates.")
LSD_END = base_le_today["DATE"].max()

# Fixed 4-week window (inclusive)
LSD_START = LSD_END - timedelta(days=(LSD_WEEKS * 7 - 1))
PREV_DATE = LSD_START  # consistent, not “special calendar”
AS_OF_DATE = LSD_END   # CTD cutoff = LSD_END

# Next month window (28 days after LSD_END)
NEXT_END = LSD_END + timedelta(days=28)

print("TODAY:", today)
print("LSD_START:", LSD_START)
print("LSD_END:", LSD_END)
print("AS_OF_DATE:", AS_OF_DATE)
print("NEXT_END:", NEXT_END)
print("Rows in EVMS (BCWS/BCWP/ACWP/ETC):", len(base))

# skeletons to eliminate missing rows
programs = sorted(base["PROGRAM"].dropna().unique().tolist())
pt_keys  = base[["PROGRAM","PRODUCT_TEAM"]].drop_duplicates()

# -------------------------
# Aggregations
# -------------------------
def agg_costsets(df, group_cols):
    g = (df.groupby(group_cols + ["COST_SET"], as_index=False)["VAL"].sum())
    pv = g.pivot_table(index=group_cols, columns="COST_SET", values="VAL", aggfunc="sum").reset_index()
    for cs in EVMS_SETS:
        if cs not in pv.columns:
            pv[cs] = 0.0
    pv[EVMS_SETS] = pv[EVMS_SETS].fillna(0.0)
    return pv

# CTD: everything <= LSD_END
ctd_df = base[base["DATE"] <= LSD_END]
ctd_prog = agg_costsets(ctd_df, ["PROGRAM"])
ctd_pt   = agg_costsets(ctd_df, ["PROGRAM","PRODUCT_TEAM"])

# LSD window: LSD_START..LSD_END inclusive
lsd_df = base[(base["DATE"] >= LSD_START) & (base["DATE"] <= LSD_END)]
lsd_prog = agg_costsets(lsd_df, ["PROGRAM"])
lsd_pt   = agg_costsets(lsd_df, ["PROGRAM","PRODUCT_TEAM"])

# Next 4 weeks after LSD_END for manpower projection
next_df = base[(base["DATE"] > LSD_END) & (base["DATE"] <= NEXT_END) & (base["COST_SET"].isin(["BCWS","ETC"]))]
next_prog = agg_costsets(next_df, ["PROGRAM"])[["PROGRAM","BCWS","ETC"]].rename(
    columns={"BCWS":"Next Mo BCWS Hours", "ETC":"Next Mo ETC Hours"}
)

# -------------------------
# PROGRAM OVERVIEW (WIDE like your screenshot)
# -------------------------
prog_skel = pd.DataFrame({"PROGRAM": programs})
prog = (prog_skel
        .merge(lsd_prog, on="PROGRAM", how="left", suffixes=("","_LSD"))
        .merge(ctd_prog, on="PROGRAM", how="left", suffixes=("_LSD","_CTD"))
       )

# after merges, ensure expected columns exist
for cs in EVMS_SETS:
    if f"{cs}_LSD" not in prog.columns: prog[f"{cs}_LSD"] = 0.0
    if f"{cs}_CTD" not in prog.columns: prog[f"{cs}_CTD"] = 0.0

prog[[f"{cs}_LSD" for cs in EVMS_SETS] + [f"{cs}_CTD" for cs in EVMS_SETS]] = \
    prog[[f"{cs}_LSD" for cs in EVMS_SETS] + [f"{cs}_CTD" for cs in EVMS_SETS]].fillna(0.0)

prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])
prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])

# user asked: no NaN -> replace NaN ratios with 0
for c in ["SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD"]:
    prog[c] = pd.to_numeric(prog[c], errors="coerce").fillna(0.0)

Program_Overview = pd.DataFrame({
    "ProgramID": prog["PROGRAM"],
    "SPI_LSD": prog["SPI_LSD"],
    "SPI_CTD": prog["SPI_CTD"],
    "CPI_LSD": prog["CPI_LSD"],
    "CPI_CTD": prog["CPI_CTD"],
})

Program_Overview["LSD_START"] = LSD_START
Program_Overview["LSD_END"]   = LSD_END
Program_Overview["AS_OF_DATE"] = AS_OF_DATE
Program_Overview["PREV_DATE"] = PREV_DATE

Program_Overview["SPI_LSD_Color"] = Program_Overview["SPI_LSD"].map(color_spi_cpi)
Program_Overview["SPI_CTD_Color"] = Program_Overview["SPI_CTD"].map(color_spi_cpi)
Program_Overview["CPI_LSD_Color"] = Program_Overview["CPI_LSD"].map(color_spi_cpi)
Program_Overview["CPI_CTD_Color"] = Program_Overview["CPI_CTD"].map(color_spi_cpi)

comment_overview = "Comments / Root Cause & Corrective Actions"
Program_Overview[comment_overview] = ""
Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID"], comment_overview)

# -------------------------
# PRODUCT TEAM SPI/CPI
# -------------------------
pt_skel = pt_keys.copy()
pt = (pt_skel
      .merge(lsd_pt, on=["PROGRAM","PRODUCT_TEAM"], how="left", suffixes=("","_LSD"))
      .merge(ctd_pt, on=["PROGRAM","PRODUCT_TEAM"], how="left", suffixes=("_LSD","_CTD"))
     )

for cs in EVMS_SETS:
    if f"{cs}_LSD" not in pt.columns: pt[f"{cs}_LSD"] = 0.0
    if f"{cs}_CTD" not in pt.columns: pt[f"{cs}_CTD"] = 0.0

pt[[f"{cs}_LSD" for cs in EVMS_SETS] + [f"{cs}_CTD" for cs in EVMS_SETS]] = \
    pt[[f"{cs}_LSD" for cs in EVMS_SETS] + [f"{cs}_CTD" for cs in EVMS_SETS]].fillna(0.0)

pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])
pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])

for c in ["SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD"]:
    pt[c] = pd.to_numeric(pt[c], errors="coerce").fillna(0.0)

ProductTeam_SPI_CPI = pt.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})[
    ["ProgramID","Product Team","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]
].copy()

ProductTeam_SPI_CPI["SPI_LSD_Color"] = ProductTeam_SPI_CPI["SPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = ProductTeam_SPI_CPI["SPI_CTD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = ProductTeam_SPI_CPI["CPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = ProductTeam_SPI_CPI["CPI_CTD"].map(color_spi_cpi)

ProductTeam_SPI_CPI["LSD_START"] = LSD_START
ProductTeam_SPI_CPI["LSD_END"]   = LSD_END
ProductTeam_SPI_CPI["AS_OF_DATE"] = AS_OF_DATE
ProductTeam_SPI_CPI["PREV_DATE"] = PREV_DATE

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI,
    ["ProgramID","Product Team"], comment_pt
)

# -------------------------
# PRODUCT TEAM BAC/EAC/VAC (robust, KUW won’t be missing)
# BAC = CTD BCWS (through LSD_END)  [consistent with your “hours” world]
# EAC = CTD ACWP + CTD ETC
# VAC = BAC - EAC
# -------------------------
bac_eac = pt.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}).copy()

bac_eac["BAC"] = bac_eac["BCWS_CTD"]
bac_eac["EAC"] = bac_eac["ACWP_CTD"] + bac_eac["ETC_CTD"]
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])

bac_eac["BAC"] = _to_num(bac_eac["BAC"]).fillna(0.0)
bac_eac["EAC"] = _to_num(bac_eac["EAC"]).fillna(0.0)
bac_eac["VAC"] = _to_num(bac_eac["VAC"]).fillna(0.0)
bac_eac["VAC_BAC"] = _to_num(bac_eac["VAC_BAC"]).fillna(0.0)

bac_eac["VAC_Color"] = bac_eac["VAC_BAC"].map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac[
    ["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color"]
].copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC,
    ["ProgramID","Product Team"], comment_pt
)

# -------------------------
# PROGRAM MANPOWER (guarantee Next Mo columns exist)
# Demand Hours = CTD BCWS
# Actual Hours = CTD ACWP
# % Var = Actual / Demand * 100
# -------------------------
man = prog_skel.merge(ctd_prog, on="PROGRAM", how="left").copy()
for cs in EVMS_SETS:
    if cs not in man.columns: man[cs] = 0.0
man[EVMS_SETS] = man[EVMS_SETS].fillna(0.0)

man = man.rename(columns={"PROGRAM":"ProgramID"})
man["Demand Hours"] = _to_num(man["BCWS"]).fillna(0.0)
man["Actual Hours"] = _to_num(man["ACWP"]).fillna(0.0)

man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var"] = _to_num(man["% Var"]).fillna(0.0)
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

# attach next 4 weeks
Program_Manpower = man.merge(
    next_prog.rename(columns={"PROGRAM":"ProgramID"}),
    on="ProgramID", how="left"
)

# force columns to exist + no NaN
if "Next Mo BCWS Hours" not in Program_Manpower.columns:
    Program_Manpower["Next Mo BCWS Hours"] = 0.0
if "Next Mo ETC Hours" not in Program_Manpower.columns:
    Program_Manpower["Next Mo ETC Hours"] = 0.0

Program_Manpower["Next Mo BCWS Hours"] = _to_num(Program_Manpower["Next Mo BCWS Hours"]).fillna(0.0)
Program_Manpower["Next Mo ETC Hours"]  = _to_num(Program_Manpower["Next Mo ETC Hours"]).fillna(0.0)

Program_Manpower["AS_OF_DATE"] = AS_OF_DATE
Program_Manpower["LSD_START"] = LSD_START
Program_Manpower["LSD_END"] = LSD_END

Program_Manpower[comment_pt] = ""
Program_Manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower", Program_Manpower,
    ["ProgramID"], comment_pt
)

Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color",
     "Next Mo BCWS Hours","Next Mo ETC Hours","AS_OF_DATE","LSD_START","LSD_END",comment_pt]
].copy()

# -------------------------
# WRITE EXCEL
# -------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

print("""
Power BI formatting (OVERVIEW wide):
- Visual: Table
- Fields: ProgramID, SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD
- Conditional formatting -> Background color -> Format by: Field value
    SPI_LSD uses SPI_LSD_Color
    SPI_CTD uses SPI_CTD_Color
    CPI_LSD uses CPI_LSD_Color
    CPI_CTD uses CPI_CTD_Color
""")