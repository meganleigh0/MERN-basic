
from pathlib import Path
import pandas as pd

DATA_DIR = Path(r"./CONTRACTS/data")
REF_XLSX = DATA_DIR / "GDMS FAR_DFARS Database 03-12-2025.xlsx"

# 1) Show the EXACT sheet names in the file
xls = pd.ExcelFile(REF_XLSX)
print("Sheets found in REF_XLSX:")
for s in xls.sheet_names:
    print("-", repr(s))