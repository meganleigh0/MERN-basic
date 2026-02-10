import pandas as pd
import numpy as np
from pathlib import Path

# =============================
# CONFIG (EDIT THESE 2 LINES)
# =============================
INPUT_DIR = Path(".")  # folder where your TSVs live (ex: Path(r"C:\Users\...\cobra evms metrics"))
OUTPUT_XLSX = INPUT_DIR / "EVMS_PowerBI_Input.xlsx"

# Expected TSV filenames (edit if yours are named differently)
TSV_PROGRAM_OVERVIEW   = INPUT_DIR / "Program_Overview.tsv"
TSV_SUBTEAM_SPI_CPI    = INPUT_DIR / "SubTeam_SPI_CPI.tsv"
TSV_SUBTEAM_BAC_EAC_VAC= INPUT_DIR / "SubTeam_BAC_EAC_VAC.tsv"
TSV_PROGRAM_MANPOWER   = INPUT_DIR / "Program_Manpower.tsv"

# If you don't have exactly these names, we'll try to find them by "contains" keywords.
def find_tsv_fallback(preferred_path: Path, contains: str):
    if preferred_path.exists():
        return preferred_path
    hits = sorted(INPUT_DIR.glob("*.tsv"))
    hits = [p for p in hits if contains.lower() in p.name.lower()]
    if len(hits) == 1:
        return hits[0]
    if len(hits) > 1:
        # pick shortest name match
        hits = sorted(hits, key=lambda p: len(p.name))
        return hits[0]
    raise FileNotFoundError(f"Could not find TSV for '{contains}'. Looked for {preferred_path.name} and any *.tsv containing '{contains}' in {INPUT_DIR.resolve()}")

TSV_PROGRAM_OVERVIEW    = find_tsv_fallback(TSV_PROGRAM_OVERVIEW, "overview")
TSV_SUBTEAM_SPI_CPI     = find_tsv_fallback(TSV_SUBTEAM_SPI_CPI, "spi")
TSV_SUBTEAM_BAC_EAC_VAC = find_tsv_fallback(TSV_SUBTEAM_BAC_EAC_VAC, "bac")
TSV_PROGRAM_MANPOWER    = find_tsv_fallback(TSV_PROGRAM_MANPOWER, "manpower")

print("Using TSVs:")
print(" -", TSV_PROGRAM_OVERVIEW)
print(" -", TSV_SUBTEAM_SPI_CPI)
print(" -", TSV_SUBTEAM_BAC_EAC_VAC)
print(" -", TSV_PROGRAM_MANPOWER)

# =============================
# COLORS (from your guideline)
# =============================
BLUE   = "#8EB4E3"  # light blue
GREEN  = "#339966"
YELLOW = "#FFFF99"
RED    = "#C0504D"

# =============================
# HELPERS
# =============================
def read_tsv(path: Path) -> pd.DataFrame:
    # TSV = tab-separated values
    df = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
    # Turn empty strings into NA
    df = df.replace({"": np.nan})
    return df

def clean_colnames(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]
    return df

def normalize_program_id(s):
    if pd.isna(s): return s
    return str(s).strip()

def normalize_metric(s):
    if pd.isna(s): return s
    return str(s).strip().upper()

def to_num(series):
    return pd.to_numeric(series, errors="coerce")

def color_spi_cpi(x):
    # SPI/CPI thresholds
    if pd.isna(x): return None
    try: x = float(x)
    except: return None
    if x >= 1.05: return BLUE
    if x >= 0.98: return GREEN
    if x >= 0.95: return YELLOW
    return RED

def color_vac_bac(r):
    # VAC/BAC thresholds
    if pd.isna(r): return None
    try: r = float(r)
    except: return None
    if r > 0.05: return BLUE
    if r >= -0.02: return GREEN
    if r >= -0.05: return YELLOW
    return RED

def color_manpower_pct(x):
    # Program manpower thresholds; x is percent like 94.7 = 94.7%
    if pd.isna(x): return None
    try: x = float(x)
    except: return None
    if x >= 110: return RED
    if x >= 105: return YELLOW
    if x >= 90:  return GREEN
    if x >= 85:  return YELLOW
    return RED

def ensure_cols(df, required, name):
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing columns: {missing}\nColumns found: {list(df.columns)}")
    return df

# =============================
# 1) LOAD TSVs
# =============================
program_overview    = clean_colnames(read_tsv(TSV_PROGRAM_OVERVIEW))
subteam_spi_cpi     = clean_colnames(read_tsv(TSV_SUBTEAM_SPI_CPI))
subteam_bac_eac_vac = clean_colnames(read_tsv(TSV_SUBTEAM_BAC_EAC_VAC))
program_manpower    = clean_colnames(read_tsv(TSV_PROGRAM_MANPOWER))

# =============================
# 2) STANDARDIZE COLUMN NAMES (so they match your Power BI model)
#    (We keep your comment columns but normalize spelling)
# =============================

# ---- Program_Overview expected: ProgramID, Metric, CTD, LSD, Comments / Root Cause & Corrective Actions
po_rename = {
    "Comments": "Comments / Root Cause & Corrective Actions",
    "Comments / Root Cause & Corrective Action": "Comments / Root Cause & Corrective Actions",
    "Comments / Root Cause & Corrective Actions": "Comments / Root Cause & Corrective Actions",
}
program_overview = program_overview.rename(columns={k:v for k,v in po_rename.items() if k in program_overview.columns})

# ---- SubTeam_SPI_CPI expected: ProgramID, SubTeam, SPI LSD, SPI CTD, CPI LSD, CPI CTD, Cause & Corrective Actions
st_rename = {
    "Cause & Corrective Action": "Cause & Corrective Actions",
    "Cause & Corrective Actions": "Cause & Corrective Actions",
}
subteam_spi_cpi = subteam_spi_cpi.rename(columns={k:v for k,v in st_rename.items() if k in subteam_spi_cpi.columns})

# ---- SubTeam_BAC_EAC_VAC expected: ProgramID, SubTeam, BAC, EAC, VAC, Cause & Corrective Actions
subteam_bac_eac_vac = subteam_bac_eac_vac.rename(columns={k:v for k,v in st_rename.items() if k in subteam_bac_eac_vac.columns})

# ---- Program_Manpower expected:
# ProgramID, Demand Hours, Actual Hours, % Var, Next Mo BCWS Hours, Next Mo ETC Hours, Cause & Corrective Actions
pm_rename = {
    "Next Mo BCWS": "Next Mo BCWS Hours",
    "Next Mo BCWS Hrs": "Next Mo BCWS Hours",
    "Next Mo ETC": "Next Mo ETC Hours",
    "Next Mo ETC Hrs": "Next Mo ETC Hours",
    "Cause & Corrective Action": "Cause & Corrective Actions",
    "Cause & Corrective Actions": "Cause & Corrective Actions",
}
program_manpower = program_manpower.rename(columns={k:v for k,v in pm_rename.items() if k in program_manpower.columns})

# =============================
# 3) ENFORCE REQUIRED COLUMNS
# =============================
# Program Overview: NO BEI (per your request)
ensure_cols(program_overview, ["ProgramID","Metric","CTD","LSD"], "Program_Overview")
if "BEI" in program_overview["Metric"].astype(str).str.upper().unique():
    # drop BEI rows if they exist
    program_overview = program_overview[~program_overview["Metric"].astype(str).str.upper().eq("BEI")].copy()

# Comment column exists or create it
if "Comments / Root Cause & Corrective Actions" not in program_overview.columns:
    program_overview["Comments / Root Cause & Corrective Actions"] = np.nan

ensure_cols(subteam_spi_cpi, ["ProgramID","SubTeam","SPI LSD","SPI CTD","CPI LSD","CPI CTD"], "SubTeam_SPI_CPI")
if "Cause & Corrective Actions" not in subteam_spi_cpi.columns:
    subteam_spi_cpi["Cause & Corrective Actions"] = np.nan

ensure_cols(subteam_bac_eac_vac, ["ProgramID","SubTeam","BAC","EAC","VAC"], "SubTeam_BAC_EAC_VAC")
if "Cause & Corrective Actions" not in subteam_bac_eac_vac.columns:
    subteam_bac_eac_vac["Cause & Corrective Actions"] = np.nan

ensure_cols(program_manpower, ["ProgramID","Demand Hours","Actual Hours","% Var","Next Mo BCWS Hours","Next Mo ETC Hours"], "Program_Manpower")
if "Cause & Corrective Actions" not in program_manpower.columns:
    program_manpower["Cause & Corrective Actions"] = np.nan

# Normalize keys
for df in [program_overview, subteam_spi_cpi, subteam_bac_eac_vac, program_manpower]:
    df["ProgramID"] = df["ProgramID"].map(normalize_program_id)

program_overview["Metric"] = program_overview["Metric"].map(normalize_metric)

# =============================
# 4) NUMERIC COERCION + COLOR ATTRIBUTE COLUMNS (WHAT POWER BI WILL USE)
# =============================

# ---- Program_Overview colors (CTD + LSD) for SPI/CPI only
program_overview["CTD"] = to_num(program_overview["CTD"])
program_overview["LSD"] = to_num(program_overview["LSD"])
mask_spi_cpi = program_overview["Metric"].isin(["SPI","CPI"])

program_overview["Color_CTD"] = np.where(mask_spi_cpi, program_overview["CTD"].map(color_spi_cpi), None)
program_overview["Color_LSD"] = np.where(mask_spi_cpi, program_overview["LSD"].map(color_spi_cpi), None)

# ---- SubTeam_SPI_CPI colors (SPI LSD/CTD, CPI LSD/CTD)
for c in ["SPI LSD","SPI CTD","CPI LSD","CPI CTD"]:
    subteam_spi_cpi[c] = to_num(subteam_spi_cpi[c])

subteam_spi_cpi["Color_SPI_LSD"] = subteam_spi_cpi["SPI LSD"].map(color_spi_cpi)
subteam_spi_cpi["Color_SPI_CTD"] = subteam_spi_cpi["SPI CTD"].map(color_spi_cpi)
subteam_spi_cpi["Color_CPI_LSD"] = subteam_spi_cpi["CPI LSD"].map(color_spi_cpi)
subteam_spi_cpi["Color_CPI_CTD"] = subteam_spi_cpi["CPI CTD"].map(color_spi_cpi)

# ---- SubTeam_BAC_EAC_VAC VAC/BAC ratio + color
for c in ["BAC","EAC","VAC"]:
    subteam_bac_eac_vac[c] = to_num(subteam_bac_eac_vac[c])

subteam_bac_eac_vac["VAC_BAC"] = np.where(
    (subteam_bac_eac_vac["BAC"].notna()) & (subteam_bac_eac_vac["BAC"] != 0),
    subteam_bac_eac_vac["VAC"] / subteam_bac_eac_vac["BAC"],
    np.nan
)
subteam_bac_eac_vac["Color_VAC_BAC"] = subteam_bac_eac_vac["VAC_BAC"].map(color_vac_bac)

# ---- Program_Manpower %Var color
program_manpower["% Var"] = to_num(program_manpower["% Var"])

# If your % Var is 0-1 instead of 0-100, auto-fix
# (Example: 0.9467 -> 94.67)
if program_manpower["% Var"].dropna().between(0, 2).mean() > 0.8:
    program_manpower["% Var"] = program_manpower["% Var"] * 100

program_manpower["Color_%Var"] = program_manpower["% Var"].map(color_manpower_pct)

# =============================
# 5) QUICK MISSINGNESS CHECK (for the core metric columns)
# =============================
def miss(df, cols): 
    return {c: float(df[c].isna().mean()) for c in cols}

print("\nMissingness (key numeric cols):")
print("Program_Overview:", miss(program_overview, ["CTD","LSD"]))
print("SubTeam_SPI_CPI:", miss(subteam_spi_cpi, ["SPI LSD","SPI CTD","CPI LSD","CPI CTD"]))
print("SubTeam_BAC_EAC_VAC:", miss(subteam_bac_eac_vac, ["BAC","EAC","VAC","VAC_BAC"]))
print("Program_Manpower:", miss(program_manpower, ["Demand Hours","Actual Hours","% Var"]))

# =============================
# 6) EXPORT (Excel for Power BI + optional TSVs)
# =============================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    subteam_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    subteam_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

# Optional: export TSVs w/ colors for debugging / re-import
(program_overview).to_csv(INPUT_DIR / "Program_Overview_withColors.tsv", sep="\t", index=False)
(subteam_spi_cpi).to_csv(INPUT_DIR / "SubTeam_SPI_CPI_withColors.tsv", sep="\t", index=False)
(subteam_bac_eac_vac).to_csv(INPUT_DIR / "SubTeam_BAC_EAC_VAC_withColors.tsv", sep="\t", index=False)
(program_manpower).to_csv(INPUT_DIR / "Program_Manpower_withColors.tsv", sep="\t", index=False)

print(f"\nSaved Excel: {OUTPUT_XLSX.resolve()}")
print("Saved TSVs: *_withColors.tsv")

# Preview
display(program_overview.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_manpower.head(20))