# ============================================
# EVMS PowerBI Input Builder (Single Pipeline)
# Fixes LSD SPI/CPI by using per-group "Last Common Date" (LCD)
# Standard rolling LSD window (default 4 weeks)
# Outputs 4 Excel sheets for Power BI
# ============================================

from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path

# ----------------------------
# CONFIG
# ----------------------------
TODAY = pd.Timestamp.today().normalize()

LSD_WEEKS = 4
LSD_DAYS = LSD_WEEKS * 7

# Input options:
#  - If you already have cobra_merged_df in memory, set INPUT_MODE="dataframe" and assign it.
#  - Otherwise set INPUT_MODE="file" and point to a CSV/TSV/Excel extract that contains the EVMS long table.

INPUT_MODE = "file"   # "file" or "dataframe"

# If file:
INPUT_PATH = Path(r"C:\Users\YOURUSER\Desktop\evms_powerbi\cobra_evms_long.tsv")  # change
INPUT_FILETYPE = "tsv"  # "tsv", "csv", or "xlsx"

# If excel:
INPUT_SHEET = "EVMS_LONG"  # only used when INPUT_FILETYPE == "xlsx"

# Output
OUTPUT_XLSX = Path(r"C:\Users\YOURUSER\Desktop\evms_powerbi\EVMS_PowerBI_Input.xlsx")

# Columns expected in the LONG input:
# PROGRAM, PRODUCT_TEAM, COST_SET, DATE, VAL
COL_PROGRAM = "PROGRAM"
COL_TEAM    = "PRODUCT_TEAM"
COL_COSTSET = "COST_SET"
COL_DATE    = "DATE"
COL_VAL     = "VAL"

# -----------------------------------
# OPTIONAL: if your source uses different labels
# and you already did mapping earlier, keep this empty.
# -----------------------------------
COSTSET_NORMALIZE_MAP = {
    # examples (only needed if your file isn't already mapped):
    # "BCWS HRS": "BCWS",
    # "BCWP HRS": "BCWP",
    # "ACWP HRS": "ACWP",
    # "ETC HRS":  "ETC",
}

KEEP_COSTSETS = {"BCWS","BCWP","ACWP","ETC"}  # what we care about

# ----------------------------
# HELPERS
# ----------------------------
def _norm_str(x: str) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    out = np.where((d == 0) | pd.isna(d) | pd.isna(n), np.nan, n / d)
    return out

def color_rule_spi_cpi(x: float) -> str | None:
    # same thresholds you used previously
    if pd.isna(x):
        return None
    if x >= 1.05:
        return "#8EB4E3"  # blue
    if x >= 0.98:
        return "#339966"  # green
    if x >= 0.95:
        return "#FFFF99"  # yellow
    return "#C0504D"      # red

def color_rule_vac_pct(x: float) -> str | None:
    # You can tune these if you want; leaving sensible defaults:
    # >=0 green, between 0 and -0.05 yellow, < -0.05 red
    if pd.isna(x):
        return None
    if x >= 0:
        return "#339966"
    if x >= -0.05:
        return "#FFFF99"
    return "#C0504D"

def _coerce_date(s):
    return pd.to_datetime(s, errors="coerce").dt.date

# ----------------------------
# STEP 1) LOAD LONG EVMS TABLE
# ----------------------------
if INPUT_MODE == "dataframe":
    # Expect you already defined cobra_merged_df upstream
    base = cobra_merged_df.copy()
else:
    if INPUT_FILETYPE == "tsv":
        base = pd.read_csv(INPUT_PATH, sep="\t")
    elif INPUT_FILETYPE == "csv":
        base = pd.read_csv(INPUT_PATH)
    elif INPUT_FILETYPE == "xlsx":
        base = pd.read_excel(INPUT_PATH, sheet_name=INPUT_SHEET)
    else:
        raise ValueError("INPUT_FILETYPE must be one of: tsv, csv, xlsx")

# Standardize columns
base = base.rename(columns={
    COL_PROGRAM: "PROGRAM",
    COL_TEAM: "PRODUCT_TEAM",
    COL_COSTSET: "COST_SET",
    COL_DATE: "DATE",
    COL_VAL: "VAL"
})

for c in ["PROGRAM","PRODUCT_TEAM","COST_SET"]:
    base[c] = base[c].map(_norm_str)

# Date + numeric
base["DATE"] = pd.to_datetime(base["DATE"], errors="coerce").dt.normalize()
base["VAL"]  = pd.to_numeric(base["VAL"], errors="coerce")

# Minimal cost set normalize (won't redo your mapping unless needed)
if COSTSET_NORMALIZE_MAP:
    base["COST_SET"] = base["COST_SET"].replace(COSTSET_NORMALIZE_MAP)

# Keep only relevant rows
base = base[base["COST_SET"].isin(KEEP_COSTSETS)].copy()
base = base.dropna(subset=["DATE","PROGRAM","PRODUCT_TEAM","COST_SET"])

print("TODAY:", TODAY.date())
print("Rows in base:", len(base))
print("Programs:", base["PROGRAM"].nunique(), "| Product Teams:", base["PRODUCT_TEAM"].nunique())

# ----------------------------
# STEP 2) FIX LSD END DATE (PER GROUP): "LAST COMMON POSTED DATE"
# ----------------------------
need_for_spi_cpi = ["BCWS","BCWP","ACWP"]  # must exist for LSD to be meaningful

mx = (base[base["COST_SET"].isin(need_for_spi_cpi)]
      .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"])["DATE"]
      .max()
      .unstack("COST_SET")
      .reset_index())

# Last Common Date (LCD) where all three exist (earliest of the max dates)
mx["LCD_END"] = mx[need_for_spi_cpi].min(axis=1)

# If a team is missing one costset entirely, LCD_END becomes NaT; handle later
mx["LSD_END"] = mx["LCD_END"]

# Standard rolling LSD window
mx["LSD_START"] = mx["LSD_END"] - pd.to_timedelta(LSD_DAYS - 1, unit="D")

# We also define CTD cutoff as LCD_END so CTD doesn't run past earned/actual
mx["CTD_END"] = mx["LCD_END"]

# Merge back
base2 = base.merge(mx[["PROGRAM","PRODUCT_TEAM","LSD_START","LSD_END","CTD_END"]],
                   on=["PROGRAM","PRODUCT_TEAM"], how="left")

# Filter windows
lsd_df = base2[(base2["DATE"] >= base2["LSD_START"]) & (base2["DATE"] <= base2["LSD_END"])].copy()
ctd_df = base2[base2["DATE"] <= base2["CTD_END"]].copy()

print("LSD window days:", LSD_DAYS)
print("Rows in LSD df:", len(lsd_df), "| Rows in CTD df:", len(ctd_df))

# ----------------------------
# STEP 3) AGGREGATION FUNCTION
# ----------------------------
def agg_costsets(df: pd.DataFrame, group_cols: list[str], suffix: str) -> pd.DataFrame:
    g = (df.groupby(group_cols + ["COST_SET"])["VAL"]
           .sum()
           .unstack("COST_SET")
           .reset_index())
    for cs in KEEP_COSTSETS:
        if cs not in g.columns:
            g[cs] = 0.0

    out = g[group_cols].copy()
    out[f"BCWS_{suffix}"] = g["BCWS"]
    out[f"BCWP_{suffix}"] = g["BCWP"]
    out[f"ACWP_{suffix}"] = g["ACWP"]
    out[f"ETC_{suffix}"]  = g["ETC"]
    return out

# ----------------------------
# STEP 4) PRODUCT TEAM SPI/CPI (LSD + CTD)
# ----------------------------
pt_lsd = agg_costsets(lsd_df, ["PROGRAM","PRODUCT_TEAM"], "LSD")
pt_ctd = agg_costsets(ctd_df, ["PROGRAM","PRODUCT_TEAM"], "CTD")

pt = pt_lsd.merge(pt_ctd, on=["PROGRAM","PRODUCT_TEAM"], how="outer")

# Data availability flags (avoid fake zeros)
pt["DATA_OK_LSD"] = ((pt["BCWS_LSD"] > 0) & (pt["BCWP_LSD"] > 0)).astype(int)
pt["DATA_OK_CTD"] = ((pt["BCWS_CTD"] > 0) & (pt["BCWP_CTD"] > 0)).astype(int)

pt["SPI_LSD"] = np.where(pt["DATA_OK_LSD"]==1, _safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"]), np.nan)
pt["CPI_LSD"] = np.where(pt["DATA_OK_LSD"]==1, _safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"]), np.nan)

pt["SPI_CTD"] = np.where(pt["DATA_OK_CTD"]==1, _safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"]), np.nan)
pt["CPI_CTD"] = np.where(pt["DATA_OK_CTD"]==1, _safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"]), np.nan)

# Bring in LSD/CTD dates per team for auditing
pt_dates = mx[["PROGRAM","PRODUCT_TEAM","LSD_START","LSD_END","CTD_END"]].copy()
pt = pt.merge(pt_dates, on=["PROGRAM","PRODUCT_TEAM"], how="left")

# Colors for PBI
pt["SPI_LSD_Color"] = pt["SPI_LSD"].map(color_rule_spi_cpi)
pt["SPI_CTD_Color"] = pt["SPI_CTD"].map(color_rule_spi_cpi)
pt["CPI_LSD_Color"] = pt["CPI_LSD"].map(color_rule_spi_cpi)
pt["CPI_CTD_Color"] = pt["CPI_CTD"].map(color_rule_spi_cpi)

# Output shaping
ProductTeam_SPI_CPI = pt.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
ProductTeam_SPI_CPI["Cause & Corrective Actions"] = ""

ProductTeam_SPI_CPI = ProductTeam_SPI_CPI[[
    "ProgramID","Product Team",
    "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
    "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
    "LSD_START","LSD_END","CTD_END",
    "DATA_OK_LSD","DATA_OK_CTD",
    "Cause & Corrective Actions"
]].sort_values(["ProgramID","Product Team"])

# ----------------------------
# STEP 5) PROGRAM OVERVIEW (aggregate across teams)
# ----------------------------
prog_lsd = agg_costsets(lsd_df, ["PROGRAM"], "LSD")
prog_ctd = agg_costsets(ctd_df, ["PROGRAM"], "CTD")
prog = prog_lsd.merge(prog_ctd, on=["PROGRAM"], how="outer")

prog["DATA_OK_LSD"] = ((prog["BCWS_LSD"] > 0) & (prog["BCWP_LSD"] > 0)).astype(int)
prog["DATA_OK_CTD"] = ((prog["BCWS_CTD"] > 0) & (prog["BCWP_CTD"] > 0)).astype(int)

prog["SPI_LSD"] = np.where(prog["DATA_OK_LSD"]==1, _safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"]), np.nan)
prog["CPI_LSD"] = np.where(prog["DATA_OK_LSD"]==1, _safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"]), np.nan)
prog["SPI_CTD"] = np.where(prog["DATA_OK_CTD"]==1, _safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"]), np.nan)
prog["CPI_CTD"] = np.where(prog["DATA_OK_CTD"]==1, _safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"]), np.nan)

# Program LCD dates: take min across teams for that program (conservative)
prog_dates = (mx.groupby("PROGRAM")[["LSD_START","LSD_END","CTD_END"]]
                .min()
                .reset_index())
prog = prog.merge(prog_dates, on="PROGRAM", how="left")

prog["SPI_LSD_Color"] = prog["SPI_LSD"].map(color_rule_spi_cpi)
prog["SPI_CTD_Color"] = prog["SPI_CTD"].map(color_rule_spi_cpi)
prog["CPI_LSD_Color"] = prog["CPI_LSD"].map(color_rule_spi_cpi)
prog["CPI_CTD_Color"] = prog["CPI_CTD"].map(color_rule_spi_cpi)

Program_Overview = prog.rename(columns={"PROGRAM":"ProgramID"})
Program_Overview["Cause & Corrective Actions"] = ""

Program_Overview = Program_Overview[[
    "ProgramID",
    "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
    "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
    "LSD_START","LSD_END","CTD_END",
    "DATA_OK_LSD","DATA_OK_CTD",
    "Cause & Corrective Actions"
]].sort_values("ProgramID")

# ----------------------------
# STEP 6) BAC / EAC / VAC BY PRODUCT TEAM
# Assumption: BAC = total BCWS (CTD to LCD_END), EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# ----------------------------
pt_bac = pt.copy()
pt_bac["BAC"] = pt_bac["BCWS_CTD"]
pt_bac["EAC"] = pt_bac["ACWP_CTD"] + pt_bac["ETC_CTD"]
pt_bac["VAC"] = pt_bac["BAC"] - pt_bac["EAC"]
pt_bac["VAC_PCT"] = _safe_div(pt_bac["VAC"], pt_bac["BAC"])

pt_bac["VAC_Color"] = pt_bac["VAC_PCT"].map(color_rule_vac_pct)

ProductTeam_BAC_EAC_VAC = pt_bac.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
ProductTeam_BAC_EAC_VAC["Corrective Actions"] = ""

ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[[
    "ProgramID","Product Team",
    "BAC","EAC","VAC","VAC_PCT","VAC_Color",
    "CTD_END",
    "Corrective Actions"
]].sort_values(["ProgramID","Product Team"])

# ----------------------------
# STEP 7) PROGRAM MANPOWER
# Demand/Actual Hours = BCWS/ACWP in LSD window
# Next Month BCWS/ETC = sum in next 28 days after LSD_END (standard, consistent)
# ----------------------------
# LSD window sums already available in prog_lsd
pm = prog_lsd.rename(columns={"PROGRAM":"ProgramID"})
pm["Demand Hours"] = pm["BCWS_LSD"]
pm["Actual Hours"] = pm["ACWP_LSD"]
pm["%Var"] = _safe_div(pm["Actual Hours"] - pm["Demand Hours"], pm["Demand Hours"])
pm["%Var_Col"] = pm["%Var"].map(color_rule_vac_pct)

# Build next-window frame
# Next period is (LSD_END+1) to (LSD_END + 28)
prog_lsd_end = prog_dates.rename(columns={"PROGRAM":"ProgramID"})
prog_lsd_end["NEXT_START"] = prog_lsd_end["LSD_END"] + pd.to_timedelta(1, unit="D")
prog_lsd_end["NEXT_END"]   = prog_lsd_end["LSD_END"] + pd.to_timedelta(LSD_DAYS, unit="D")

base_prog = base.merge(prog_lsd_end[["ProgramID","NEXT_START","NEXT_END"]], left_on="PROGRAM", right_on="ProgramID", how="left")
next_df = base_prog[(base_prog["DATE"] >= base_prog["NEXT_START"]) & (base_prog["DATE"] <= base_prog["NEXT_END"])].copy()

next_prog = (next_df.groupby(["ProgramID","COST_SET"])["VAL"].sum().unstack("COST_SET").reset_index())
for cs in KEEP_COSTSETS:
    if cs not in next_prog.columns:
        next_prog[cs] = 0.0

next_prog = next_prog.rename(columns={"BCWS":"Next Mo BCWS Hrs", "ETC":"Next Mo ETC Hrs"})
next_prog = next_prog[["ProgramID","Next Mo BCWS Hrs","Next Mo ETC Hrs"]]

Program_Manpower = pm.merge(next_prog, on="ProgramID", how="left")
Program_Manpower = Program_Manpower.merge(prog_dates.rename(columns={"PROGRAM":"ProgramID"}), on="ProgramID", how="left")

Program_Manpower = Program_Manpower[[
    "ProgramID",
    "Demand Hours","Actual Hours","%Var","%Var_Col",
    "Next Mo BCWS Hrs","Next Mo ETC Hrs",
    "LSD_START","LSD_END","CTD_END"
]].sort_values("ProgramID")

# ----------------------------
# STEP 8) WRITE EXCEL
# ----------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print("Saved:", str(OUTPUT_XLSX))

# ----------------------------
# STEP 9) QUICK SANITY CHECKS (prints)
# ----------------------------
# Find any teams where SPI_LSD is NaN due to missing cost sets in LSD window
bad_lsd = ProductTeam_SPI_CPI[ProductTeam_SPI_CPI["DATA_OK_LSD"] == 0][["ProgramID","Product Team","LSD_START","LSD_END","DATA_OK_LSD"]]
if len(bad_lsd):
    print("\nTeams missing LSD data (BCWS/BCWP not both present in window) - should be rare after LCD fix:")
    print(bad_lsd.head(25).to_string(index=False))

# If you want to debug a specific team quickly:
# p, t = "ABRAMS STS 2022", "KUW"
# tmp = lsd_df[(lsd_df["PROGRAM"]==p) & (lsd_df["PRODUCT_TEAM"]==t)]
# print("\nLSD sums:", p, t)
# print(tmp.groupby("COST_SET")["VAL"].sum())