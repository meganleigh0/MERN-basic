# ============================
# EVMS COBRA MVP PIPELINE
# COSTSET-DRIVEN, DELTA-BASED
# SINGLE CELL, DEBUG-FIRST
# ============================

import pandas as pd
import numpy as np
from pathlib import Path

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 200)

DATA_DIR = Path("data")

FILES = {
    "Abrams": "Cobra-Abrams STS 2022.xlsx",
    "Stryker": "Cobra-Stryker Bulgaria 150.xlsx",
    "XM30": "Cobra-XM30.xlsx",
}

SHEET_KEYWORDS = ["CAP", "Weekly", "Extract"]

COSTSET_MAP = {
    "BCWS": ["Budget"],
    "BCWP": ["Progress"],
    "ACWP": ["ACWP_HRS"],
    "ETC": ["ETC"],
    "EAC": ["EAC"]
}

# ----------------------------
# 1. Load files
# ----------------------------
frames = []

print("\n================ LOADING FILES ================")
for label, fname in FILES.items():
    path = DATA_DIR / fname
    xls = pd.ExcelFile(path)

    sheet = next(s for s in xls.sheet_names if any(k in s for k in SHEET_KEYWORDS))
    df = pd.read_excel(xls, sheet)

    df.columns = df.columns.str.upper().str.strip()

    required = {"DATE", "COSTSET", "HOURS"}
    missing = required - set(df.columns)

    if missing:
        print(f"{fname} ❌ missing {missing}")
        continue

    df["DATE"] = pd.to_datetime(df["DATE"])
    df["SOURCE"] = fname

    frames.append(df[["SOURCE", "DATE", "COSTSET", "HOURS"]])

cobra = pd.concat(frames, ignore_index=True)

print(f"\nLoaded rows: {len(cobra):,}")
print(cobra.groupby("SOURCE")["DATE"].agg(["min", "max"]))

# ----------------------------
# 2. Normalize COSTSET → METRIC
# ----------------------------
def map_metric(costset):
    for m, vals in COSTSET_MAP.items():
        if costset in vals:
            return m
    return "OTHER"

cobra["METRIC"] = cobra["COSTSET"].map(map_metric)

print("\n================ METRIC COVERAGE ================")
print(cobra.groupby(["SOURCE", "METRIC"]).agg(
    rows=("HOURS", "count"),
    sum_hours=("HOURS", "sum")
))

# ----------------------------
# 3. Snapshot logic
# ----------------------------
def get_closes(df):
    closes = sorted(df["DATE"].unique())
    curr = closes[-1]
    prev = closes[-2] if len(closes) > 1 else closes[-1]
    return prev, curr

# ----------------------------
# 4. Cumulative totals
# ----------------------------
def cumulative_at(df, metric, date):
    return df[
        (df["METRIC"] == metric) &
        (df["DATE"] <= date)
    ]["HOURS"].sum()

# ----------------------------
# 5. Program metrics (CORRECT)
# ----------------------------
rows = []

print("\n================ PROGRAM METRICS ================")

for src in cobra["SOURCE"].unique():
    df = cobra[cobra["SOURCE"] == src]
    prev_close, curr_close = get_closes(df)

    print(f"\nSOURCE: {src}")
    print(f"Prev close: {prev_close}")
    print(f"Curr close: {curr_close}")

    vals = {}
    for m in ["BCWS", "BCWP", "ACWP", "ETC", "EAC"]:
        vals[f"{m}_CTD"] = cumulative_at(df, m, curr_close)
        vals[f"{m}_PREV"] = cumulative_at(df, m, prev_close)
        vals[f"{m}_LSD"] = vals[f"{m}_CTD"] - vals[f"{m}_PREV"]

        print(f"{m}: CTD={vals[f'{m}_CTD']:.2f} | LSD={vals[f'{m}_LSD']:.2f}")

    # Derived metrics
    vals["SPI_CTD"] = vals["BCWP_CTD"] / vals["BCWS_CTD"] if vals["BCWS_CTD"] else np.nan
    vals["CPI_CTD"] = vals["BCWP_CTD"] / vals["ACWP_CTD"] if vals["ACWP_CTD"] else np.nan
    vals["SPI_LSD"] = vals["BCWP_LSD"] / vals["BCWS_LSD"] if vals["BCWS_LSD"] else np.nan
    vals["CPI_LSD"] = vals["BCWP_LSD"] / vals["ACWP_LSD"] if vals["ACWP_LSD"] else np.nan

    # Hours & variance
    vals["Demand_Hours"] = vals["BCWS_LSD"]
    vals["Actual_Hours"] = vals["ACWP_LSD"]
    vals["Pct_Var"] = (
        (vals["Actual_Hours"] - vals["Demand_Hours"]) / vals["Demand_Hours"]
        if vals["Demand_Hours"] else np.nan
    )

    rows.append({
        "SOURCE": src,
        "SNAPSHOT_DATE": curr_close,
        **vals
    })

program_metrics = pd.DataFrame(rows)

print("\n================ FINAL PROGRAM METRICS ================")
display(program_metrics)

print("""
NOTES:
- All LSD values are DELTAS of cumulative totals (correct)
- No window sums are used
- BEI is intentionally excluded
- Any zero here is now a REAL zero, not a bug
""")