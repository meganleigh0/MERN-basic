import numpy as np
import pandas as pd

out_path = DATA_DIR / "cobra_evms_tables.xlsx"

# Put all your tables here
tables = {
    "fact_long": cobra_fact,
    "program_metrics": program_metrics,
    "subteam_metrics": subteam_metrics,
    "subteam_cost": subteam_cost,
    "hours_metrics": hours_metrics,
    "label_audit": label_audit,
    "coverage_audit": coverage_audit,
}

def _sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Flatten multiindex columns if any
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in tup if x is not None]) for tup in df.columns]

    # Ensure string column names + Excel-safe length
    df.columns = [str(c)[:255] for c in df.columns]

    # Replace inf/-inf which Excel cannot write
    df = df.replace([np.inf, -np.inf], np.nan)

    # Convert timezone-aware datetimes (Excel can't handle tz-aware)
    for c in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[c]):
            df[c] = df[c].dt.tz_convert(None)

    return df

# Prefer xlsxwriter if installed; otherwise openpyxl
engine = "xlsxwriter"
try:
    import xlsxwriter  # noqa
except Exception:
    engine = "openpyxl"

errors = []

with pd.ExcelWriter(out_path, engine=engine) as writer:
    for sheet, df in tables.items():
        try:
            safe_sheet = str(sheet)[:31]  # Excel sheet name limit
            safe_df = _sanitize_for_excel(df)
            safe_df.to_excel(writer, sheet_name=safe_sheet, index=False)
        except Exception as e:
            errors.append((sheet, str(e)))

# Report results
print(f"✅ Wrote Excel workbook: {out_path}")
if errors:
    print("\n⚠️ Some sheets failed to write:")
    for s, msg in errors:
        print(f" - {s}: {msg}")

    # OPTIONAL: write the error log as its own sheet in a second file
    err_path = DATA_DIR / "cobra_evms_tables_WRITE_ERRORS.xlsx"
    pd.DataFrame(errors, columns=["sheet","error"]).to_excel(err_path, index=False)
    print(f"\n✅ Error log written: {err_path}")