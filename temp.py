from pathlib import Path
import pandas as pd

DATA_DIR = Path(r"data")
REF_XLSX = DATA_DIR / "GDMS FAR_DFARS Database 03-12-2025.xlsx"

# Load reference tables using EXACT sheet names
far_database = pd.read_excel(
    REF_XLSX,
    sheet_name="FAR_DATABASE"
)

dfars_database = pd.read_excel(
    REF_XLSX,
    sheet_name="DFARS DATABASE"   # <-- space matters
)

effective_thresholds = pd.read_excel(
    REF_XLSX,
    sheet_name="Effective Date- Thresholds"
)

# Sanity check
print("FAR Database shape:", far_database.shape)
print("DFARS Database shape:", dfars_database.shape)
print("Effective Thresholds shape:", effective_thresholds.shape)

display(far_database.head())
display(dfars_database.head())
display(effective_thresholds.head())