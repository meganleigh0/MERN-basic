"""
SOL FIXED: Clean + Safe Excel Loading
This block:
- Loads each Excel sheet explicitly
- Avoids ExcelFile engine conflicts
- Confirms exactly what you have
"""

from pathlib import Path
import pandas as pd

DATA_DIR = Path(r"./CONTRACTS/data")

RESULTS_XLSX  = DATA_DIR / "BSCA-65AC2-2544 (Executed) Results.xlsx"
GUIDANCE_XLSX = DATA_DIR / "BSCA-65AC2-2544 (Executed) Guidance.xlsx"
REF_XLSX      = DATA_DIR / "GDMS FAR_DFARS Database 03-12-2025.xlsx"

# ----------------------------
# Load ClauseBot outputs
# ----------------------------
results = pd.read_excel(RESULTS_XLSX, sheet_name="Sheet1")

guidance_raw = pd.read_excel(
    GUIDANCE_XLSX,
    sheet_name="Guidance.Raw"
)

guidance_nodupes = pd.read_excel(
    GUIDANCE_XLSX,
    sheet_name="Guidance.Sort.NoDupes"
)

# ----------------------------
# Load FAR / DFARS reference data
# ----------------------------
far_database = pd.read_excel(
    REF_XLSX,
    sheet_name="FAR_DATABASE"
)

dfars_database = pd.read_excel(
    REF_XLSX,
    sheet_name="DFARS_DATABASE"
)

effective_thresholds = pd.read_excel(
    REF_XLSX,
    sheet_name="Effective Date- Thresholds"
)

# ----------------------------
# Sanity check prints
# ----------------------------
print("\n=== DATA LOADED SUCCESSFULLY ===\n")

print("ClauseBot Results:", results.shape)
display(results.head())

print("\nClauseBot Guidance (No Dupes):", guidance_nodupes.shape)
display(guidance_nodupes.head())

print("\nFAR Database:", far_database.shape)
display(far_database.head())

print("\nDFARS Database:", dfars_database.shape)
display(dfars_database.head())

print("\nEffective Date Thresholds:", effective_thresholds.shape)
display(effective_thresholds.head())