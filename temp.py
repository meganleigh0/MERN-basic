import numpy as np
import pandas as pd
from pathlib import Path

# ---------- CONFIG ----------
OUTPUT_XLSX = Path(r"EVMS_PowerBI_Input.xlsx")  # change if needed

# GDLS palette (from your guideline)
BLUE   = "#8EB4E3"
GREEN  = "#339966"
YELLOW = "#FFFF99"
RED    = "#C0504D"

# ---------- COLOR FUNCTIONS ----------
def color_spi_cpi(x):
    """SPI/CPI/BEI style thresholds (we'll use for SPI & CPI only)."""
    if pd.isna(x):
        return None
    try:
        x = float(x)
    except Exception:
        return None
    if x >= 1.05: return BLUE
    if x >= 0.98: return GREEN
    if x >= 0.95: return YELLOW
    return RED

def color_vac_bac(r):
    """VAC/BAC thresholds."""
    if pd.isna(r):
        return None
    try:
        r = float(r)
    except Exception:
        return None
    if r > 0.05: return BLUE
    if r >= -0.02: return GREEN
    if r >= -0.05: return YELLOW
    return RED

def color_manpower_pct(x):
    """Program manpower thresholds where x is a percent like 94.7 for 94.7%."""
    if pd.isna(x):
        return None
    try:
        x = float(x)
    except Exception:
        return None
    if x >= 110: return RED
    if x >= 105: return YELLOW
    if x >= 90:  return GREEN
    if x >= 85:  return YELLOW
    return RED

def require_cols(df, cols, name):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{name} is missing columns: {missing}\nFound: {list(df.columns)}")
    return df

# ---------- VALIDATE INPUT DFS EXIST ----------
needed = ["df_program_overview", "df_subteam_spi_cpi", "df_subteam_bac_eac_vac", "df_program_manpower"]
for n in needed:
    if n not in globals():
        raise NameError(f"{n} not found. Make sure your pipeline creates it BEFORE this cell.")

# ---------- PROGRAM_OVERVIEW (no BEI) ----------
require_cols(df_program_overview, ["ProgramID", "Metric", "CTD", "LSD"], "df_program_overview")

df_program_overview["CTD"] = pd.to_numeric(df_program_overview["CTD"], errors="coerce")
df_program_overview["LSD"] = pd.to_numeric(df_program_overview["LSD"], errors="coerce")

# Only color SPI/CPI rows (everything else blank)
mask_spi_cpi = df_program_overview["Metric"].astype(str).str.upper().isin(["SPI","CPI"])
df_program_overview["Color_CTD"] = np.where(mask_spi_cpi, df_program_overview["CTD"].map(color_spi_cpi), None)
df_program_overview["Color_LSD"] = np.where(mask_spi_cpi, df_program_overview["LSD"].map(color_spi_cpi), None)

# ---------- SUBTEAM_SPI_CPI ----------
require_cols(df_subteam_spi_cpi,
             ["ProgramID", "SubTeam", "SPI LSD", "SPI CTD", "CPI LSD", "CPI CTD"],
             "df_subteam_spi_cpi")

for c in ["SPI LSD","SPI CTD","CPI LSD","CPI CTD"]:
    df_subteam_spi_cpi[c] = pd.to_numeric(df_subteam_spi_cpi[c], errors="coerce")

df_subteam_spi_cpi["Color_SPI_LSD"] = df_subteam_spi_cpi["SPI LSD"].map(color_spi_cpi)
df_subteam_spi_cpi["Color_SPI_CTD"] = df_subteam_spi_cpi["SPI CTD"].map(color_spi_cpi)
df_subteam_spi_cpi["Color_CPI_LSD"] = df_subteam_spi_cpi["CPI LSD"].map(color_spi_cpi)
df_subteam_spi_cpi["Color_CPI_CTD"] = df_subteam_spi_cpi["CPI CTD"].map(color_spi_cpi)

# ---------- SUBTEAM_BAC_EAC_VAC ----------
require_cols(df_subteam_bac_eac_vac, ["ProgramID", "SubTeam", "BAC", "EAC", "VAC"], "df_subteam_bac_eac_vac")

for c in ["BAC","EAC","VAC"]:
    df_subteam_bac_eac_vac[c] = pd.to_numeric(df_subteam_bac_eac_vac[c], errors="coerce")

# helper + color
df_subteam_bac_eac_vac["VAC_BAC"] = np.where(
    (df_subteam_bac_eac_vac["BAC"].notna()) & (df_subteam_bac_eac_vac["BAC"] != 0),
    df_subteam_bac_eac_vac["VAC"] / df_subteam_bac_eac_vac["BAC"],
    np.nan
)
df_subteam_bac_eac_vac["Color_VAC_BAC"] = df_subteam_bac_eac_vac["VAC_BAC"].map(color_vac_bac)

# ---------- PROGRAM_MANPOWER ----------
require_cols(df_program_manpower,
             ["ProgramID", "Demand Hours", "Actual Hours", "% Var", "Next Mo BCWS Hours", "Next Mo ETC Hours"],
             "df_program_manpower")

# % Var should be a percent number like 94.67 (not 0.9467). If yours is 0-1, uncomment the conversion line below.
df_program_manpower["% Var"] = pd.to_numeric(df_program_manpower["% Var"], errors="coerce")
# df_program_manpower["% Var"] = df_program_manpower["% Var"] * 100  # uncomment if % Var is stored 0-1

df_program_manpower["Color_%Var"] = df_program_manpower["% Var"].map(color_manpower_pct)

# ---------- WRITE ONE EXCEL (Power BI-friendly) ----------
# Keep tables consistent and relationship-friendly (ProgramID always present)
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    df_program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    df_subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    df_subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    df_program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"Saved: {OUTPUT_XLSX.resolve()}")
print("Color columns created:",
      "\n- Program_Overview: Color_CTD, Color_LSD",
      "\n- SubTeam_SPI_CPI: Color_SPI_LSD, Color_SPI_CTD, Color_CPI_LSD, Color_CPI_CTD",
      "\n- SubTeam_BAC_EAC_VAC: VAC_BAC, Color_VAC_BAC",
      "\n- Program_Manpower: Color_%Var")
    