import os
import numpy as np
import pandas as pd
from pathlib import Path

# =========================
# EXPECTED INPUT DATAFRAMES (already in your notebook):
#   program_overview
#   subteam_spi_cpi
#   subteam_bac_eac_vac
#   program_manpower
# =========================

# --------- Output locations (edit if you want) ----------
OUTPUT_DIR = Path.cwd()  # current notebook folder
OUTPUT_XLSX = OUTPUT_DIR / "EVMS_PowerBI_Input.xlsx"

TSV_DIR = OUTPUT_DIR / "tsv_exports"
TSV_DIR.mkdir(parents=True, exist_ok=True)

# --------- Color palette (from your GDLS threshold key) ----------
HEX_DARK_BLUE = "#1F497D"  # RGB 31,73,125 (rarely used in thresholds; kept for completeness)
HEX_LIGHT_BLUE = "#8EB4E3" # RGB 142,180,227
HEX_GREEN = "#339966"      # RGB 51,153,102
HEX_YELLOW = "#FFFF99"     # RGB 255,255,153
HEX_RED = "#C0504D"        # RGB 192,80,77

# --------- Helpers ----------
def _to_num(s):
    """Coerce to numeric, preserve NaN."""
    return pd.to_numeric(s, errors="coerce")

def _clean_str(s):
    return s.astype(str).str.strip()

def spi_cpi_color(x):
    """
    SPI/CPI thresholds (same for LSD/CTD):
      x >= 1.05  -> Light Blue
      x >= 0.98  -> Green
      x >= 0.95  -> Yellow
      else       -> Red
    """
    if pd.isna(x):
        return np.nan
    try:
        x = float(x)
    except Exception:
        return np.nan
    if x >= 1.05:
        return HEX_LIGHT_BLUE
    if x >= 0.98:
        return HEX_GREEN
    if x >= 0.95:
        return HEX_YELLOW
    return HEX_RED

def vac_bac_color(r):
    """
    VAC/BAC thresholds (ratio, not percent):
      r >= +0.05                    -> Light Blue
      +0.05 > r >= -0.02            -> Green
      -0.02 > r >= -0.05            -> Yellow
      r < -0.05                     -> Red
    """
    if pd.isna(r):
        return np.nan
    try:
        r = float(r)
    except Exception:
        return np.nan
    if r >= 0.05:
        return HEX_LIGHT_BLUE
    if r >= -0.02:
        return HEX_GREEN
    if r >= -0.05:
        return HEX_YELLOW
    return HEX_RED

def manpower_color(p):
    """
    Program manpower thresholds are on % (e.g., 110%, 105%, 90%, 85%).
    Your Program_Manpower table shows '% Var' like 94.67, 111.71, etc.
    We'll accept:
      - 0.9467 (ratio) OR 94.67 (percent)
    Convert to percent points internally.
      >=110% -> Red
      110> x >=105 -> Yellow
      105> x >=90  -> Green
      90>  x >=85  -> Yellow
      <85% -> Red
    """
    if pd.isna(p):
        return np.nan
    try:
        p = float(p)
    except Exception:
        return np.nan

    # Normalize: if it's a ratio (0-2), turn into percent
    if p <= 2.0:
        p = p * 100.0

    if p >= 110.0:
        return HEX_RED
    if p >= 105.0:
        return HEX_YELLOW
    if p >= 90.0:
        return HEX_GREEN
    if p >= 85.0:
        return HEX_YELLOW
    return HEX_RED

# =========================
# 0) Validate expected dataframes exist
# =========================
missing = [name for name in ["program_overview","subteam_spi_cpi","subteam_bac_eac_vac","program_manpower"] if name not in globals()]
if missing:
    raise NameError(f"Missing dataframe(s) in memory: {missing}. Make sure you ran the cell that creates them first.")

# Work on copies (avoid mutating originals unexpectedly)
program_overview = program_overview.copy()
subteam_spi_cpi = subteam_spi_cpi.copy()
subteam_bac_eac_vac = subteam_bac_eac_vac.copy()
program_manpower = program_manpower.copy()

# =========================
# 1) Program_Overview (NO BEI)
# Expected cols: ProgramID, Metric, CTD, LSD, (optional) Comments/Root Cause...
# =========================
# Normalize key string columns
if "ProgramID" in program_overview.columns:
    program_overview["ProgramID"] = _clean_str(program_overview["ProgramID"])
if "Metric" in program_overview.columns:
    program_overview["Metric"] = _clean_str(program_overview["Metric"]).str.upper()

# Coerce numeric
for c in ["CTD", "LSD"]:
    if c in program_overview.columns:
        program_overview[c] = _to_num(program_overview[c])

# Drop BEI rows if they exist
if "Metric" in program_overview.columns:
    program_overview = program_overview[program_overview["Metric"].isin(["SPI","CPI"])].copy()

# Add color columns for CTD/LSD (SPI/CPI use same thresholds)
if "CTD" in program_overview.columns:
    program_overview["Color_CTD"] = program_overview["CTD"].map(spi_cpi_color)
if "LSD" in program_overview.columns:
    program_overview["Color_LSD"] = program_overview["LSD"].map(spi_cpi_color)

# =========================
# 2) SubTeam_SPI_CPI
# Expected cols: ProgramID, SubTeam, SPI LSD, SPI CTD, CPI LSD, CPI CTD, (optional) Cause/Comments...
# =========================
for c in ["ProgramID","SubTeam"]:
    if c in subteam_spi_cpi.columns:
        subteam_spi_cpi[c] = _clean_str(subteam_spi_cpi[c])

for c in ["SPI LSD","SPI CTD","CPI LSD","CPI CTD"]:
    if c in subteam_spi_cpi.columns:
        subteam_spi_cpi[c] = _to_num(subteam_spi_cpi[c])

# Add color columns
if "SPI LSD" in subteam_spi_cpi.columns:
    subteam_spi_cpi["Color_SPI_LSD"] = subteam_spi_cpi["SPI LSD"].map(spi_cpi_color)
if "SPI CTD" in subteam_spi_cpi.columns:
    subteam_spi_cpi["Color_SPI_CTD"] = subteam_spi_cpi["SPI CTD"].map(spi_cpi_color)
if "CPI LSD" in subteam_spi_cpi.columns:
    subteam_spi_cpi["Color_CPI_LSD"] = subteam_spi_cpi["CPI LSD"].map(spi_cpi_color)
if "CPI CTD" in subteam_spi_cpi.columns:
    subteam_spi_cpi["Color_CPI_CTD"] = subteam_spi_cpi["CPI CTD"].map(spi_cpi_color)

# =========================
# 3) SubTeam_BAC_EAC_VAC
# Expected cols: ProgramID, SubTeam, BAC, EAC, VAC, (optional) Cause/Comments...
# Add VAC/BAC + Color_VAC_BAC
# =========================
for c in ["ProgramID","SubTeam"]:
    if c in subteam_bac_eac_vac.columns:
        subteam_bac_eac_vac[c] = _clean_str(subteam_bac_eac_vac[c])

for c in ["BAC","EAC","VAC"]:
    if c in subteam_bac_eac_vac.columns:
        subteam_bac_eac_vac[c] = _to_num(subteam_bac_eac_vac[c])

# Compute VAC/BAC safely
if "VAC" in subteam_bac_eac_vac.columns and "BAC" in subteam_bac_eac_vac.columns:
    denom = subteam_bac_eac_vac["BAC"].replace({0: np.nan})
    subteam_bac_eac_vac["VAC/BAC"] = subteam_bac_eac_vac["VAC"] / denom
    subteam_bac_eac_vac["Color_VAC_BAC"] = subteam_bac_eac_vac["VAC/BAC"].map(vac_bac_color)

# =========================
# 4) Program_Manpower
# Expected cols: ProgramID, Demand Hours, Actual Hours, % Var, Next Mo BCWS Hours, Next Mo ETC Hours, (optional) Cause/Comments...
# Add PctVar + Color_PctVar
# =========================
if "ProgramID" in program_manpower.columns:
    program_manpower["ProgramID"] = _clean_str(program_manpower["ProgramID"])

for c in ["Demand Hours","Actual Hours","% Var","Next Mo BCWS Hours","Next Mo ETC Hours"]:
    if c in program_manpower.columns:
        program_manpower[c] = _to_num(program_manpower[c])

# If % Var is missing but demand/actual exist, compute it as (Actual/Demand)*100
if "% Var" not in program_manpower.columns and {"Demand Hours","Actual Hours"}.issubset(program_manpower.columns):
    denom = program_manpower["Demand Hours"].replace({0: np.nan})
    program_manpower["% Var"] = (program_manpower["Actual Hours"] / denom) * 100.0

# Normalized helper + color
if "% Var" in program_manpower.columns:
    program_manpower["PctVar"] = program_manpower["% Var"]
    program_manpower["Color_PctVar"] = program_manpower["PctVar"].map(manpower_color)

# =========================
# 5) Quick missingness check (key numeric cols)
# =========================
def miss(df, cols):
    out = {}
    for c in cols:
        if c in df.columns:
            out[c] = float(df[c].isna().mean())
    return out

print("Quick missingness check:")
print("Program_Overview:", miss(program_overview, ["CTD","LSD"]))
print("SubTeam_SPI_CPI:", miss(subteam_spi_cpi, ["SPI LSD","SPI CTD","CPI LSD","CPI CTD"]))
print("SubTeam_BAC_EAC_VAC:", miss(subteam_bac_eac_vac, ["BAC","EAC","VAC","VAC/BAC"]))
print("Program_Manpower:", miss(program_manpower, ["Demand Hours","Actual Hours","% Var","Next Mo BCWS Hours","Next Mo ETC Hours"]))

# =========================
# 6) Export Excel + TSV (tab-separated)
# =========================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

program_overview.to_csv(TSV_DIR / "Program_Overview_withColors.tsv", sep="\t", index=False)
subteam_spi_cpi.to_csv(TSV_DIR / "SubTeam_SPI_CPI_withColors.tsv", sep="\t", index=False)
subteam_bac_eac_vac.to_csv(TSV_DIR / "SubTeam_BAC_EAC_VAC_withColors.tsv", sep="\t", index=False)
program_manpower.to_csv(TSV_DIR / "Program_Manpower_withColors.tsv", sep="\t", index=False)

print(f"\nSaved Excel: {OUTPUT_XLSX.resolve()}")
print(f"Saved TSVs:  {TSV_DIR.resolve()}")

# Preview
display(program_overview.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_manpower.head(20))