import numpy as np
import pandas as pd

# --- Color palette (from your guideline) ---
BLUE  = "#8EB4E3"
GREEN = "#339966"
YELL  = "#FFFF99"
RED   = "#C0504D"

def _isna(x):
    return x is None or (isinstance(x, float) and np.isnan(x))

def color_spi_cpi(x):
    """SPI/CPI thresholds: Blue>=1.05, Green>=0.98, Yellow>=0.95, else Red"""
    if _isna(x): 
        return None
    try:
        x = float(x)
    except:
        return None
    if x >= 1.05: return BLUE
    if x >= 0.98: return GREEN
    if x >= 0.95: return YELL
    return RED

def color_manpower_pct(x):
    """
    Program manpower thresholds:
      Red >=110%
      Yellow 105-110
      Green 90-105
      Yellow 85-90
      Red <85
    Works whether x is 94.67 (percent) or 0.9467 (ratio).
    """
    if _isna(x): 
        return None
    try:
        x = float(x)
    except:
        return None

    # Normalize: if looks like a ratio (<= 2.5), convert to percent
    x_pct = x * 100 if x <= 2.5 else x

    if x_pct >= 110: return RED
    if x_pct >= 105: return YELL
    if x_pct >= 90:  return GREEN
    if x_pct >= 85:  return YELL
    return RED

def safe_divide(a, b):
    if _isna(a) or _isna(b):
        return np.nan
    try:
        a = float(a); b = float(b)
    except:
        return np.nan
    if b == 0:
        return np.nan
    return a / b

def color_vac_bac(vac_bac):
    """
    VAC/BAC thresholds (ratio):
      Blue >= +0.05
      Green +0.05 > x >= -0.02
      Yellow -0.02 > x >= -0.05
      Red < -0.05
    """
    if _isna(vac_bac): 
        return None
    try:
        x = float(vac_bac)
    except:
        return None

    if x >= 0.05:  return BLUE
    if x >= -0.02: return GREEN
    if x >= -0.05: return YELL
    return RED

def enforce_exact_columns(df, cols):
    """Keep EXACT columns in EXACT order; create missing ones as blank."""
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = None
    out = out[cols]
    return out

# ==============================
# 1) Program_Overview (no BEI)
# Required exact headers (from your screenshot)
po_cols = ["ProgramID", "Metric", "CTD", "LSD", "Comments / Root Cause & Corrective Actions"]

df_program_overview = enforce_exact_columns(df_program_overview, po_cols)

# Optional: ensure numeric
df_program_overview["CTD"] = pd.to_numeric(df_program_overview["CTD"], errors="coerce")
df_program_overview["LSD"] = pd.to_numeric(df_program_overview["LSD"], errors="coerce")

# Add color attributes (these will be used in Power BI conditional formatting)
df_program_overview["CTD Color"] = df_program_overview["CTD"].map(color_spi_cpi)
df_program_overview["LSD Color"] = df_program_overview["LSD"].map(color_spi_cpi)

# ==============================
# 2) SubTeam_SPI_CPI
st_cols = ["SubTeam", "SPI LSD", "SPI CTD", "CPI LSD", "CPI CTD", "Cause & Corrective Actions", "ProgramID"]
df_subteam_spi_cpi = enforce_exact_columns(df_subteam_spi_cpi, st_cols)

for c in ["SPI LSD", "SPI CTD", "CPI LSD", "CPI CTD"]:
    df_subteam_spi_cpi[c] = pd.to_numeric(df_subteam_spi_cpi[c], errors="coerce")

df_subteam_spi_cpi["SPI LSD Color"] = df_subteam_spi_cpi["SPI LSD"].map(color_spi_cpi)
df_subteam_spi_cpi["SPI CTD Color"] = df_subteam_spi_cpi["SPI CTD"].map(color_spi_cpi)
df_subteam_spi_cpi["CPI LSD Color"] = df_subteam_spi_cpi["CPI LSD"].map(color_spi_cpi)
df_subteam_spi_cpi["CPI CTD Color"] = df_subteam_spi_cpi["CPI CTD"].map(color_spi_cpi)

# ==============================
# 3) SubTeam_BAC_EAC_VAC  (add VAC/BAC)
sev_cols = ["SubTeam", "BAC", "EAC", "VAC", "Cause & Corrective Actions", "ProgramID"]
df_subteam_bac_eac_vac = enforce_exact_columns(df_subteam_bac_eac_vac, sev_cols)

for c in ["BAC", "EAC", "VAC"]:
    df_subteam_bac_eac_vac[c] = pd.to_numeric(df_subteam_bac_eac_vac[c], errors="coerce")

df_subteam_bac_eac_vac["VAC/BAC"] = [
    safe_divide(v, b) for v, b in zip(df_subteam_bac_eac_vac["VAC"], df_subteam_bac_eac_vac["BAC"])
]
df_subteam_bac_eac_vac["VAC/BAC Color"] = df_subteam_bac_eac_vac["VAC/BAC"].map(color_vac_bac)

# ==============================
# 4) Program_Manpower
pm_cols = ["ProgramID", "Demand Hours", "Actual Hours", "% Var", "Next Mo BCWS Hours", "Next Mo ETC Hours", "Cause & Corrective Actions"]
df_program_manpower = enforce_exact_columns(df_program_manpower, pm_cols)

for c in ["Demand Hours", "Actual Hours", "% Var", "Next Mo BCWS Hours", "Next Mo ETC Hours"]:
    df_program_manpower[c] = pd.to_numeric(df_program_manpower[c], errors="coerce")

df_program_manpower["% Var Color"] = df_program_manpower["% Var"].map(color_manpower_pct)

# ==============================
# Save to one Excel file (4 tabs)
out_path = "EVMS_PowerBI_Input.xlsx"  # change if you want
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    df_program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    df_subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    df_subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    df_program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"Saved: {out_path}")

# Quick missingness check
def miss(df, cols):
    return {c: float(df[c].isna().mean()) for c in cols}

print("Missingness (key numeric cols):")
print("Program_Overview:", miss(df_program_overview, ["CTD","LSD"]))
print("SubTeam_SPI_CPI:", miss(df_subteam_spi_cpi, ["SPI LSD","SPI CTD","CPI LSD","CPI CTD"]))
print("SubTeam_BAC_EAC_VAC:", miss(df_subteam_bac_eac_vac, ["BAC","EAC","VAC","VAC/BAC"]))
print("Program_Manpower:", miss(df_program_manpower, ["Demand Hours","Actual Hours","% Var"]))