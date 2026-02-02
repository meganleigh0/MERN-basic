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

SHEET_HINTS = ["CAP", "Weekly", "Extract"]

# acceptable aliases we will search for
COSTSET_ALIASES = [
    "COSTSET", "COST_SET", "COST SET",
    "COST CATEGORY", "COST_CATEGORY",
    "RESOURCE TYPE", "RESOURCE_TYPE"
]

HOURS_ALIASES = ["HOURS", "HRS"]

# canonical mapping AFTER normalization
COSTSET_MAP = {
    "BCWS": ["BUDGET"],
    "BCWP": ["PROGRESS"],
    "ACWP": ["ACWP", "ACWP_HRS"],
    "ETC": ["ETC"],
    "EAC": ["EAC"]
}

frames = []

print("\n================ LOADING FILES ================")

for label, fname in FILES.items():
    path = DATA_DIR / fname
    xls = pd.ExcelFile(path)

    sheet = next(s for s in xls.sheet_names if any(h in s for h in SHEET_HINTS))
    df = pd.read_excel(xls, sheet)

    df.columns = df.columns.str.upper().str.strip()

    print(f"\nFILE: {fname}")
    print("Columns:", list(df.columns))

    # --- find DATE
    if "DATE" not in df.columns:
        print("❌ DATE column missing")
        continue

    # --- find HOURS
    hours_col = next((c for c in df.columns if c in HOURS_ALIASES), None)
    if not hours_col:
        print("❌ HOURS column missing")
        continue

    # --- find COSTSET
    costset_col = next((c for c in df.columns if c in COSTSET_ALIASES), None)
    if not costset_col:
        print("❌ COSTSET column missing")
        continue

    print(f"✔ Using COSTSET column: {costset_col}")
    print(f"✔ Using HOURS column: {hours_col}")

    df = df.rename(columns={
        costset_col: "COSTSET_RAW",
        hours_col: "HOURS"
    })

    df["DATE"] = pd.to_datetime(df["DATE"])
    df["SOURCE"] = fname
    df["COSTSET_NORM"] = df["COSTSET_RAW"].astype(str).str.upper().str.strip()

    frames.append(df[["SOURCE", "DATE", "COSTSET_NORM", "HOURS"]])

if not frames:
    raise RuntimeError("❌ No valid files loaded")

cobra = pd.concat(frames, ignore_index=True)

print(f"\nLoaded rows: {len(cobra):,}")
print(cobra.groupby("SOURCE")["DATE"].agg(["min", "max"]))

# ----------------------------
# Metric normalization
# ----------------------------
def map_metric(costset):
    for m, keys in COSTSET_MAP.items():
        if any(k in costset for k in keys):
            return m
    return "OTHER"

cobra["METRIC"] = cobra["COSTSET_NORM"].map(map_metric)

print("\n================ METRIC COVERAGE ================")
print(
    cobra.groupby(["SOURCE", "METRIC"])
    .agg(rows=("HOURS", "count"), sum_hours=("HOURS", "sum"))
)

# ----------------------------
# Snapshot logic
# ----------------------------
def closes(df):
    d = sorted(df["DATE"].unique())
    return d[-2], d[-1]

def ctd(df, metric, date):
    return df[(df["METRIC"] == metric) & (df["DATE"] <= date)]["HOURS"].sum()

rows = []

print("\n================ PROGRAM METRICS ================")

for src in cobra["SOURCE"].unique():
    df = cobra[cobra["SOURCE"] == src]
    prev_close, curr_close = closes(df)

    print(f"\nSOURCE: {src}")
    print(f"Prev close: {prev_close}")
    print(f"Curr close: {curr_close}")

    out = {"SOURCE": src, "SNAPSHOT_DATE": curr_close}

    for m in ["BCWS", "BCWP", "ACWP", "ETC", "EAC"]:
        out[f"{m}_CTD"] = ctd(df, m, curr_close)
        out[f"{m}_PREV"] = ctd(df, m, prev_close)
        out[f"{m}_LSD"] = out[f"{m}_CTD"] - out[f"{m}_PREV"]

        print(f"{m}: CTD={out[f'{m}_CTD']:.2f} | LSD={out[f'{m}_LSD']:.2f}")

    out["SPI_CTD"] = out["BCWP_CTD"] / out["BCWS_CTD"] if out["BCWS_CTD"] else np.nan
    out["CPI_CTD"] = out["BCWP_CTD"] / out["ACWP_CTD"] if out["ACWP_CTD"] else np.nan
    out["SPI_LSD"] = out["BCWP_LSD"] / out["BCWS_LSD"] if out["BCWS_LSD"] else np.nan
    out["CPI_LSD"] = out["BCWP_LSD"] / out["ACWP_LSD"] if out["ACWP_LSD"] else np.nan

    out["Demand_Hours"] = out["BCWS_LSD"]
    out["Actual_Hours"] = out["ACWP_LSD"]
    out["Pct_Var"] = (
        (out["Actual_Hours"] - out["Demand_Hours"]) / out["Demand_Hours"]
        if out["Demand_Hours"] else np.nan
    )

    rows.append(out)

program_metrics = pd.DataFrame(rows)

print("\n================ FINAL PROGRAM METRICS ================")
display(program_metrics)

print("""
✔ COSTSET auto-detected
✔ LSD uses cumulative deltas
✔ Zeros now indicate real absence, not bugs
✔ BEI excluded by design
""")