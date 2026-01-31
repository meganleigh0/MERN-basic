import pandas as pd
from pathlib import Path

# ---- CONFIG ----
DATA_DIR = Path("data")
FILE_PREFIX = "cobra"          # case-insensitive
SHEET_KEYWORDS = ["tbl", "weekly", "extract"]

# ---- STORAGE ----
frames = []
skipped_files = []

# ---- PROCESS FILES ----
for file_path in DATA_DIR.glob("*.xlsx"):
    if not file_path.name.lower().startswith(FILE_PREFIX.lower()):
        continue

    try:
        xls = pd.ExcelFile(file_path)

        # Find first matching sheet
        sheet_name = next(
            (
                s for s in xls.sheet_names
                if any(k in s.lower() for k in SHEET_KEYWORDS)
            ),
            None
        )

        if sheet_name is None:
            skipped_files.append((file_path.name, "No matching sheet"))
            continue

        df = pd.read_excel(xls, sheet_name=sheet_name)

        # ---- PROVENANCE ----
        df["source"] = file_path.name
        df["source_sheet"] = sheet_name

        frames.append(df)

        print(f"✅ Loaded {file_path.name} → {sheet_name}")

    except Exception as e:
        skipped_files.append((file_path.name, str(e)))

# ---- FINAL COMBINED DATAFRAME ----
cobra_df = (
    pd.concat(frames, ignore_index=True)
    if frames else pd.DataFrame()
)

# ---- SUMMARY ----
print("\n--- LOAD SUMMARY ---")
print(f"Files loaded: {len(frames)}")
print(f"Files skipped: {len(skipped_files)}")

if skipped_files:
    print("\nSkipped files:")
    for f, reason in skipped_files:
        print(f" - {f}: {reason}")
