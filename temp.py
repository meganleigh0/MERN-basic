import pandas as pd
import numpy as np
import re
from pathlib import Path

# -----------------------------
# Helper: program name from file
# -----------------------------
def program_from_filename(fname: str) -> str:
    # examples: "Cobra-ARV.xlsx" -> "ARV"
    base = Path(fname).stem
    base = re.sub(r"(?i)^cobra[-_\s]*", "", base)  # remove leading Cobra / Cobra-
    return base.strip()

# ---------------------------------------------------------
# Helper: find the "value" column that contains the numbers
# ---------------------------------------------------------
def detect_value_col(df: pd.DataFrame) -> str:
    # prefer obvious names first
    preferred = [
        "VALUE", "Value", "AMOUNT", "Amount", "TOTAL", "Total",
        "CTD", "CUM", "CUMULATIVE", "Cumulative"
    ]
    for c in preferred:
        if c in df.columns:
            return c

    # otherwise pick the numeric column with the most non-nulls
    numeric_cols = []
    for c in df.columns:
        if c in ("source_file", "source_sheet"):
            continue
        if pd.api.types.is_numeric_dtype(df[c]):
            numeric_cols.append(c)

    if numeric_cols:
        return max(numeric_cols, key=lambda c: df[c].notna().sum())

    # last resort: try to coerce columns to numeric and choose best
    best_col = None
    best_score = -1
    for c in df.columns:
        if c in ("source_file", "source_sheet"):
            continue
        coerced = pd.to_numeric(df[c], errors="coerce")
        score = coerced.notna().sum()
        if score > best_score:
            best_score = score
            best_col = c

    if best_col is None:
        raise ValueError("Could not detect a numeric value column in this file.")
    return best_col

# -----------------------------------------
# Helper: normalize COST-SET labels a little
# -----------------------------------------
def normalize_cost_set(s: str) -> str:
    if pd.isna(s):
        return s
    t = str(s).strip()
    t = t.replace(" ", "_").replace("-", "_")
    t = re.sub(r"__+", "_", t)
    return t.upper()

# -----------------------------------------
# Helper: safe divide
# -----------------------------------------
def safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    return np.where(d == 0, np.nan, n / d)

# =========================================================
# 1) Combine all program extracts into one normalized long df
# =========================================================
# Assumes you already have `loaded_frames` from earlier code
# Each df should have source_file/source_sheet columns

long_frames = []
for df in loaded_frames:
    df = df.copy()

    if "source_file" not in df.columns:
        raise ValueError("Expected `source_file` in each loaded frame. Re-run your loader that adds provenance columns.")

    df["program_name"] = df["source_file"].apply(program_from_filename)

    # normalize COST-SET column name variations
    cost_col = None
    for c in df.columns:
        if c.strip().lower().replace("-", "_") in ("cost_set", "costset"):
            cost_col = c
            break
    if cost_col is None:
        raise ValueError(f"Could not find COST-SET column in {df['source_file'].iloc[0]}")

    df["COST_SET_NORM"] = df[cost_col].apply(normalize_cost_set)

    # detect numeric value column
    val_col = detect_value_col(df)
    df["VALUE_NUM"] = pd.to_numeric(df[val_col], errors="coerce")

    # keep likely IDs
    # (these exist in your screenshots; if some are missing in a file, that's ok)
    keep_cols = ["program_name", "source_file", "COST_SET_NORM", "VALUE_NUM"]
    for c in ["SUB_TEAM", "Control_Acct", "CHG#", "RESP_DEPT", "BE_DEPT", "PLUG"]:
        if c in df.columns:
            keep_cols.append(c)

    # also keep any period/date column if present (for "last status" / next month)
    # try common names:
    period_candidates = [c for c in df.columns if str(c).strip().lower() in
                         ("period", "fiscal_period", "month", "status_date", "as_of", "asof", "date")]
    if period_candidates:
        keep_cols.append(period_candidates[0])
        df = df.rename(columns={period_candidates[0]: "PERIOD_RAW"})
    else:
        df["PERIOD_RAW"] = np.nan
        keep_cols.append("PERIOD_RAW")

    long_frames.append(df[keep_cols])

cobra_long = pd.concat(long_frames, ignore_index=True)

# =========================================================
# 2) Pivot COST_SET_NORM into columns (money & hours)
# =========================================================
id_cols = ["program_name"]
if "SUB_TEAM" in cobra_long.columns:
    id_cols.append("SUB_TEAM")

# include sub-dim columns so you can drill deeper later if you want
for c in ["Control_Acct", "CHG#", "RESP_DEPT", "BE_DEPT", "PLUG", "PERIOD_RAW"]:
    if c in cobra_long.columns and c not in id_cols:
        id_cols.append(c)

cobra_wide = (
    cobra_long
    .pivot_table(index=id_cols, columns="COST_SET_NORM", values="VALUE_NUM", aggfunc="sum")
    .reset_index()
)

# Flatten pivoted columns (pandas MultiIndex -> plain)
cobra_wide.columns = [c if not isinstance(c, tuple) else c[-1] for c in cobra_wide.columns]

# =========================================================
# 3) Identify key EVMS cost sets (case-insensitive patterns)
# =========================================================
def find_col(regex_list, cols):
    for rgx in regex_list:
        pattern = re.compile(rgx, flags=re.IGNORECASE)
        matches = [c for c in cols if pattern.fullmatch(str(c)) or pattern.search(str(c))]
        if matches:
            # prefer exact short names if present
            for exact in ["ACWP", "BCWP", "BCWS", "BAC", "EAC", "ETC"]:
                if exact in matches:
                    return exact
            return matches[0]
    return None

cols = list(cobra_wide.columns)

ACWP_col = find_col([r"\bACWP\b"], cols)
BCWP_col = find_col([r"\bBCWP\b"], cols)
BCWS_col = find_col([r"\bBCWS\b"], cols)
BAC_col  = find_col([r"\bBAC\b"], cols)
EAC_col  = find_col([r"\bEAC\b"], cols)
ETC_col  = find_col([r"\bETC\b"], cols)

# Hours variants (often ACWP_HRS, BCWS_HRS, etc.)
ACWP_H_col = find_col([r"ACWP.*HRS", r"ACWP.*HOUR", r"\bACWP_HRS\b"], cols)
BCWS_H_col = find_col([r"BCWS.*HRS", r"BCWS.*HOUR", r"\bBCWS_HRS\b"], cols)
BCWP_H_col = find_col([r"BCWP.*HRS", r"BCWP.*HOUR", r"\bBCWP_HRS\b"], cols)
ETC_H_col  = find_col([r"ETC.*HRS",  r"ETC.*HOUR",  r"\bETC_HRS\b"], cols)

# =========================================================
# 4) Build table 1: EVMS Overall metrics (by program)
# =========================================================
grp_prog = cobra_wide.groupby("program_name", dropna=False)

overall = grp_prog.agg(
    ACWP=(ACWP_col, "sum") if ACWP_col else ("program_name", "size"),
    BCWP=(BCWP_col, "sum") if BCWP_col else ("program_name", "size"),
    BCWS=(BCWS_col, "sum") if BCWS_col else ("program_name", "size"),
    BAC =(BAC_col,  "sum") if BAC_col  else ("program_name", "size"),
    EAC =(EAC_col,  "sum") if EAC_col  else ("program_name", "size"),
    ETC =(ETC_col,  "sum") if ETC_col  else ("program_name", "size"),
).reset_index()

# If any key cols missing, the above placeholder "size" will be wrong — fix to NaN
for k in ["ACWP","BCWP","BCWS","BAC","EAC","ETC"]:
    if k not in overall.columns or overall[k].dtype == "int64":
        overall[k] = np.nan

overall["CPI_CTD"] = safe_div(overall["BCWP"], overall["ACWP"])
overall["SPI_CTD"] = safe_div(overall["BCWP"], overall["BCWS"])
overall["VAC"] = overall["BAC"] - overall["EAC"]

# "Last status to date" (best-effort):
# If PERIOD_RAW exists with meaningful values, take the max per program. Otherwise blank.
if "PERIOD_RAW" in cobra_wide.columns:
    last_status = (
        cobra_wide.dropna(subset=["PERIOD_RAW"])
        .groupby("program_name")["PERIOD_RAW"]
        .max()
        .reset_index()
        .rename(columns={"PERIOD_RAW": "LAST_STATUS_TO_DATE"})
    )
    overall = overall.merge(last_status, on="program_name", how="left")
else:
    overall["LAST_STATUS_TO_DATE"] = np.nan

# =========================================================
# 5) Build table 2: EVMS Subteam metrics (SPI/CPI by subteam)
# =========================================================
if "SUB_TEAM" in cobra_wide.columns:
    subteam = (
        cobra_wide
        .groupby(["program_name", "SUB_TEAM"], dropna=False)
        .agg(
            ACWP=(ACWP_col, "sum") if ACWP_col else ("program_name", "size"),
            BCWP=(BCWP_col, "sum") if BCWP_col else ("program_name", "size"),
            BCWS=(BCWS_col, "sum") if BCWS_col else ("program_name", "size"),
            BAC =(BAC_col,  "sum") if BAC_col  else ("program_name", "size"),
            EAC =(EAC_col,  "sum") if EAC_col  else ("program_name", "size"),
            ETC =(ETC_col,  "sum") if ETC_col  else ("program_name", "size"),
        )
        .reset_index()
    )

    for k in ["ACWP","BCWP","BCWS","BAC","EAC","ETC"]:
        if k not in subteam.columns or subteam[k].dtype == "int64":
            subteam[k] = np.nan

    subteam["CPI_CTD"] = safe_div(subteam["BCWP"], subteam["ACWP"])
    subteam["SPI_CTD"] = safe_div(subteam["BCWP"], subteam["BCWS"])
    subteam["VAC"] = subteam["BAC"] - subteam["EAC"]
else:
    subteam = pd.DataFrame(columns=["program_name","SUB_TEAM","ACWP","BCWP","BCWS","BAC","EAC","ETC","CPI_CTD","SPI_CTD","VAC"])

# =========================================================
# 6) Build table 3: EVMS Subteam labor/manpower (BAC/EAC/VAC in hours if present)
# =========================================================
# Prefer hours columns if available; otherwise fall back to dollars.
if "SUB_TEAM" in cobra_wide.columns:
    use_BAC = BAC_col
    use_EAC = EAC_col
    # If hours BAC/EAC exist, use them (common in Cobra extracts, sometimes named BAC_HRS/EAC_HRS)
    BAC_H_col = find_col([r"BAC.*HRS", r"BAC.*HOUR", r"\bBAC_HRS\b"], cols)
    EAC_H_col = find_col([r"EAC.*HRS", r"EAC.*HOUR", r"\bEAC_HRS\b"], cols)

    if BAC_H_col: use_BAC = BAC_H_col
    if EAC_H_col: use_EAC = EAC_H_col

    subteam_labor = (
        cobra_wide
        .groupby(["program_name", "SUB_TEAM"], dropna=False)
        .agg(
            BAC=(use_BAC, "sum") if use_BAC else ("program_name", "size"),
            EAC=(use_EAC, "sum") if use_EAC else ("program_name", "size"),
        )
        .reset_index()
    )

    if subteam_labor["BAC"].dtype == "int64": subteam_labor["BAC"] = np.nan
    if subteam_labor["EAC"].dtype == "int64": subteam_labor["EAC"] = np.nan

    subteam_labor["VAC"] = subteam_labor["BAC"] - subteam_labor["EAC"]
else:
    subteam_labor = pd.DataFrame(columns=["program_name","SUB_TEAM","BAC","EAC","VAC"])

# =========================================================
# 7) Build table 4: Program manpower (hours-based)
# =========================================================
# Required columns:
# - demand hours
# - actual hours
# - % variance
# - next month's BCWS hours
# - next month's ETC hours
#
# Best-effort detection:
# - demand_hours: BCWS_HRS if present else BCWS (if your data is already hours)
# - actual_hours: ACWP_HRS if present else ACWP (if your data is already hours)
# - next month: look for any columns containing "NEXT" and BCWS/ETC and HRS
next_bcws_col = find_col([r"NEXT.*BCWS.*HRS", r"BCWS.*NEXT.*HRS", r"BCWS.*NM.*HRS"], cols)
next_etc_col  = find_col([r"NEXT.*ETC.*HRS",  r"ETC.*NEXT.*HRS",  r"ETC.*NM.*HRS"], cols)

# choose demand/actual hour fields
demand_col = BCWS_H_col or BCWS_col
actual_col = ACWP_H_col or ACWP_col

program_manpower = (
    cobra_wide
    .groupby("program_name", dropna=False)
    .agg(
        demand_hours=(demand_col, "sum") if demand_col else ("program_name", "size"),
        actual_hours=(actual_col, "sum") if actual_col else ("program_name", "size"),
        next_month_bcws_hours=(next_bcws_col, "sum") if next_bcws_col else ("program_name", "size"),
        next_month_etc_hours=(next_etc_col, "sum") if next_etc_col else ("program_name", "size"),
    )
    .reset_index()
)

# Fix placeholders to NaN if they were "size"
for c in ["demand_hours","actual_hours","next_month_bcws_hours","next_month_etc_hours"]:
    if program_manpower[c].dtype == "int64":
        program_manpower[c] = np.nan

program_manpower["pct_variance"] = safe_div(
    program_manpower["actual_hours"] - program_manpower["demand_hours"],
    program_manpower["demand_hours"]
)

# =========================================================
# 8) EVMS Detailed metrics table (more granular rollup)
# =========================================================
# You asked for "detail metrics broken down by subteam" and also "use COST-SET"
# This table keeps the COST-SET pivoted totals by subteam (and can be extended later).
detail_cols = ["program_name"]
if "SUB_TEAM" in cobra_wide.columns:
    detail_cols.append("SUB_TEAM")
for c in ["Control_Acct", "CHG#", "RESP_DEPT", "BE_DEPT"]:
    if c in cobra_wide.columns:
        detail_cols.append(c)

# Keep only key EVMS columns if they exist
evms_keep = [c for c in [ACWP_col, BCWP_col, BCWS_col, BAC_col, EAC_col, ETC_col] if c]
evms_keep = list(dict.fromkeys(evms_keep))  # unique, preserve order

evms_detail = cobra_wide[detail_cols + evms_keep].copy()

# Add SPI/CPI at this detail level too (useful for drill-through)
if BCWP_col and ACWP_col:
    evms_detail["CPI_CTD"] = safe_div(evms_detail[BCWP_col], evms_detail[ACWP_col])
else:
    evms_detail["CPI_CTD"] = np.nan

if BCWP_col and BCWS_col:
    evms_detail["SPI_CTD"] = safe_div(evms_detail[BCWP_col], evms_detail[BCWS_col])
else:
    evms_detail["SPI_CTD"] = np.nan

# =========================================================
# 9) Export all four tables to one Excel file
# =========================================================
out_path = Path("outputs")
out_path.mkdir(exist_ok=True)
excel_file = out_path / "COBRA_EVMS_Metrics_AllPrograms.xlsx"

with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
    overall.to_excel(writer, sheet_name="EVMS_Overall", index=False)
    subteam.to_excel(writer, sheet_name="EVMS_Subteam", index=False)
    evms_detail.to_excel(writer, sheet_name="EVMS_Detail", index=False)
    subteam_labor.to_excel(writer, sheet_name="Subteam_Labor", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"✅ Wrote Excel: {excel_file.resolve()}")
print("\nDetected columns used:")
print(f"  ACWP: {ACWP_col} | BCWP: {BCWP_col} | BCWS: {BCWS_col} | BAC: {BAC_col} | EAC: {EAC_col} | ETC: {ETC_col}")
print(f"  Hours ACWP: {ACWP_H_col} | Hours BCWS: {BCWS_H_col} | Next month BCWS hrs: {next_bcws_col} | Next month ETC hrs: {next_etc_col}")