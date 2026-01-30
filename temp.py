import pandas as pd
from pathlib import Path

# ---- CONFIG ----
DATA_DIR = Path("data")  # change if needed
FILE_PREFIX = "cobra"    # case-insensitive
SHEET_KEYWORDS = ["tbl", "weekly", "extract"]

# ---- STORAGE ----
loaded_frames = []
skipped_files = []

# ---- LOOP THROUGH FILES ----
for file_path in DATA_DIR.glob("*.xlsx"):
    if not file_path.name.lower().startswith(FILE_PREFIX):
        continue

    try:
        # Load Excel file metadata only (fast)
        xls = pd.ExcelFile(file_path)

        # Find matching sheet name
        matching_sheets = [
            sheet for sheet in xls.sheet_names
            if any(k in sheet.lower() for k in SHEET_KEYWORDS)
        ]

        if not matching_sheets:
            skipped_files.append((file_path.name, "No matching sheet"))
            continue

        # If multiple matches, take the first (or change logic if needed)
        sheet_name = matching_sheets[0]

        # Read the sheet
        df = pd.read_excel(
            xls,
            sheet_name=sheet_name
        )

        # Add provenance columns
        df["source_file"] = file_path.name
        df["source_sheet"] = sheet_name

        loaded_frames.append(df)

        print(f"✅ Loaded: {file_path.name} → '{sheet_name}'")

    except Exception as e:
        skipped_files.append((file_path.name, str(e)))

# ---- COMBINE ALL DATA ----
if loaded_frames:
    cobra_df = pd.concat(loaded_frames, ignore_index=True)
else:
    cobra_df = pd.DataFrame()

# ---- SUMMARY ----
print("\n--- LOAD SUMMARY ---")
print(f"Files loaded: {len(loaded_frames)}")
print(f"Files skipped: {len(skipped_files)}")

if skipped_files:
    print("\nSkipped files:")
    for f, reason in skipped_files:
        print(f" - {f}: {reason}")