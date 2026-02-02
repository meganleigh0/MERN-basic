# ============================================================
# EVMS COBRA pipeline (single-cell, debug-heavy)
# - Program metrics computed by SUM across all subteams (no SUB_TEAM=='ALL' dependency)
# - ACWP/BCWP treated as "snapshot-at-close" (sum at DATE==close); LSD=delta of snapshots
# - BCWS/ETC treated as timephased (CTD=sum<=close, LSD=window sum)
# - BEI intentionally excluded (requires Open Plan activity file)
# ============================================================

import os
import re
from pathlib import Path
import numpy as np
import pandas as pd

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 220)

# ---------------------------
# 0) CONFIG (edit this block)
# ---------------------------
FILES = [
    r"Cobra-Abrams STS 2022.xlsx",
    r"Cobra-Stryker Bulgaria 150.xlsx",
    r"Cobra-XM30.xlsx",
]

# If you want to point at a directory, set BASE_DIR. If blank, uses current working directory.
BASE_DIR = ""  # e.g. r"C:\Users\you\Downloads"

# Candidate sheets (we will try these first, then fall back to scanning all sheets)
PREFERRED_SHEETS = [
    "CAP_Extract", "CAP Extract",
    "tbl_Weekly Extract", "tbl_Weekly_Extract",
    "Weekly Extract", "Weekly_Extract",
    "Report"
]

# How many sheets to scan if preferred not found
MAX_SHEETS_TO_SCAN = 50

# ---------------------------
# 1) Helpers
# ---------------------------
def _norm_col(s: str) -> str:
    """Upper, strip, collapse to alnum+underscore."""
    s = str(s).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def _pick_col(cols_norm, wanted_norm_names):
    """Return first matching col from cols_norm based on list of candidates."""
    for w in wanted_norm_names:
        if w in cols_norm:
            return cols_norm[w]
    return None

def _standardize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Detect & rename DATE / COSTSET / HOURS / SUB_TEAM robustly.
    Returns df with canonical columns:
      DATE, COSTSET_RAW, HOURS, SUB_TEAM
    """
    original_cols = list(df.columns)
    cols_norm = {_norm_col(c): c for c in original_cols}

    date_col = _pick_col(cols_norm, ["DATE", "PERIOD", "STATUS_DATE", "WEEK_END", "WEEKENDING", "WEEK_ENDING"])
    costset_col = _pick_col(cols_norm, ["COSTSET", "COST_SET", "COST_SET_NAME", "COST", "COSTSETID", "COST_SET_ID", "COST_SET_TYPE", "COST_SET_CATEGORY", "COST_SET_", "COST_SET__"])
    # handle "COST-SET" -> normalizes to "COST_SET"
    if costset_col is None:
        # try any normalized col that contains COST and SET
        for k, v in cols_norm.items():
            if "COST" in k and "SET" in k:
                costset_col = v
                break

    hours_col = _pick_col(cols_norm, ["HOURS", "HRS", "HOUR", "LABOR_HOURS", "TOTAL_HOURS"])
    subteam_col = _pick_col(cols_norm, ["SUB_TEAM", "SUBTEAM", "SUB_TEAM_ID", "SUBTEAM_ID", "SUBTEAMID", "SUBTEAM_CODE", "SUBTEAMCODE"])

    missing = []
    if date_col is None:   missing.append("DATE")
    if costset_col is None: missing.append("COSTSET")
    if hours_col is None:  missing.append("HOURS")
    if subteam_col is None:
        # allow missing subteam, but we’ll create one
        subteam_col = None

    meta = {
        "date_col": date_col,
        "costset_col": costset_col,
        "hours_col": hours_col,
        "subteam_col": subteam_col,
        "missing_required": missing,
        "cols_norm_keys": list(cols_norm.keys())[:50]
    }

    if missing:
        return df, meta

    out = df[[date_col, costset_col, hours_col] + ([subteam_col] if subteam_col else [])].copy()
    out = out.rename(columns={
        date_col: "DATE",
        costset_col: "COSTSET_RAW",
        hours_col: "HOURS",
    })
    if subteam_col:
        out = out.rename(columns={subteam_col: "SUB_TEAM"})
    else:
        out["SUB_TEAM"] = "UNKNOWN"

    # coerce
    out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")
    out["HOURS"] = pd.to_numeric(out["HOURS"], errors="coerce")

    # drop unusable
    out = out.dropna(subset=["DATE", "HOURS"])
    out["SUB_TEAM"] = out["SUB_TEAM"].astype(str).str.strip()

    return out, meta

def _map_costset(costset_raw: str) -> str:
    """
    Map raw cost set strings to canonical:
      BCWS (budget), BCWP (earned/progress), ACWP (actual), ETC, OTHER
    Supports STS-like values: Budget, Progress, ACWP_HRS
    """
    if costset_raw is None:
        return "OTHER"
    s = str(costset_raw).strip().upper()

    # common direct hits
    if "BCWS" in s or s in {"BUDGET", "BUD", "BASELINE", "PLAN"}:
        return "BCWS"
    if "BCWP" in s or s in {"PROGRESS", "EARNED", "EARNED_VALUE"}:
        return "BCWP"
    if "ACWP" in s or "ACTUAL" in s or s in {"ACWP_HRS", "ACWP_HR", "ACWPHRS"}:
        return "ACWP"
    if "ETC" in s or "ESTIMATE_TO_COMPLETE" in s:
        return "ETC"

    # sometimes ACWP appears as "ACWP" exactly
    if s == "ACWP": return "ACWP"
    if s == "BCWP": return "BCWP"
    if s == "BCWS": return "BCWS"
    if s == "ETC":  return "ETC"

    return "OTHER"

def _safe_div(n, d):
    if d is None or pd.isna(d) or d == 0:
        return np.nan
    return n / d

def _month_window_after(dt: pd.Timestamp):
    """Return (start, end) for NEXT MONTH after dt (calendar month)."""
    if pd.isna(dt):
        return (pd.NaT, pd.NaT)
    # next month start
    nm = (dt + pd.offsets.MonthBegin(1)).normalize()
    # next month end
    ne = (nm + pd.offsets.MonthEnd(1)).normalize()
    return nm, ne

# Metric semantics (based on your screenshots):
# - ACWP/BCWP: appear as snapshot-at-close (rows exist AT close date; window rows often 0)
# - BCWS/ETC: timephased across dates
SNAPSHOT_METRICS = {"ACWP", "BCWP"}
TIMEPHASE_METRICS = {"BCWS", "ETC"}

def _ctd(scope_df: pd.DataFrame, metric: str, close: pd.Timestamp) -> float:
    """Compute CTD based on metric semantics."""
    if pd.isna(close):
        return np.nan
    sdf = scope_df[scope_df["COSTSET_NORM"] == metric]
    if sdf.empty:
        return np.nan

    if metric in SNAPSHOT_METRICS:
        # Prefer exact close snapshot (this matches your exports)
        exact = sdf[sdf["DATE"] == close]
        if not exact.empty:
            return float(exact["HOURS"].sum())
        # fallback (some exports may be timephased)
        return float(sdf.loc[sdf["DATE"] <= close, "HOURS"].sum())

    # timephased
    return float(sdf.loc[sdf["DATE"] <= close, "HOURS"].sum())

def _lsd(scope_df: pd.DataFrame, metric: str, prev_close: pd.Timestamp, curr_close: pd.Timestamp) -> float:
    """Compute LSD based on metric semantics."""
    if pd.isna(curr_close) or pd.isna(prev_close):
        return np.nan
    sdf = scope_df[scope_df["COSTSET_NORM"] == metric]
    if sdf.empty:
        return np.nan

    if metric in SNAPSHOT_METRICS:
        # LSD = delta between close snapshots
        c_prev = _ctd(scope_df, metric, prev_close)
        c_curr = _ctd(scope_df, metric, curr_close)
        if pd.isna(c_prev) or pd.isna(c_curr):
            return np.nan
        return float(c_curr - c_prev)

    # timephased: window sum in (prev_close, curr_close]
    w = sdf[(sdf["DATE"] > prev_close) & (sdf["DATE"] <= curr_close)]
    return float(w["HOURS"].sum()) if not w.empty else 0.0

def _sum_in_window(scope_df: pd.DataFrame, metric: str, start: pd.Timestamp, end: pd.Timestamp) -> float:
    if pd.isna(start) or pd.isna(end):
        return np.nan
    sdf = scope_df[scope_df["COSTSET_NORM"] == metric]
    if sdf.empty:
        return np.nan
    w = sdf[(sdf["DATE"] >= start) & (sdf["DATE"] <= end)]
    return float(w["HOURS"].sum()) if not w.empty else 0.0

def _choose_close_dates(source_df: pd.DataFrame) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Determine curr_close and prev_close driven by ACWP/BCWP dates (status cadence).
    Falls back to BCWS if needed.
    """
    driver = source_df[source_df["COSTSET_NORM"].isin(["ACWP","BCWP"])].copy()
    if driver.empty:
        driver = source_df[source_df["COSTSET_NORM"].isin(["BCWS"])].copy()
    if driver.empty:
        return (pd.NaT, pd.NaT)

    dates = pd.Index(driver["DATE"].dropna().unique())
    if len(dates) == 0:
        return (pd.NaT, pd.NaT)
    dates = dates.sort_values()

    curr = dates[-1]
    prev = dates[-2] if len(dates) >= 2 else pd.NaT
    return (pd.Timestamp(curr), pd.Timestamp(prev))

# ---------------------------
# 2) Load files
# ---------------------------
print("\n" + "="*90)
print("1) LOADING SELECTED FILES")
print("="*90)

base = Path(BASE_DIR) if BASE_DIR else Path.cwd()
load_log = []
frames = []

def _try_read_sheet(path: Path, sheet_name: str):
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, engine="openpyxl")
        return df, None
    except Exception as e:
        return None, str(e)

for f in FILES:
    path = Path(f)
    if not path.is_absolute():
        path = base / path

    if not path.exists():
        print(f"❌ FILE NOT FOUND: {path}")
        load_log.append({"file": str(path), "status": "MISSING", "sheet": None, "rows": 0, "notes": "not found"})
        continue

    xls = pd.ExcelFile(path, engine="openpyxl")
    sheet_candidates = [s for s in PREFERRED_SHEETS if s in xls.sheet_names]
    if not sheet_candidates:
        sheet_candidates = xls.sheet_names[:MAX_SHEETS_TO_SCAN]

    chosen = None
    chosen_meta = None
    chosen_sheet = None
    chosen_notes = None

    for sh in sheet_candidates:
        raw, err = _try_read_sheet(path, sh)
        if raw is None:
            continue

        std, meta = _standardize_columns(raw)
        if meta["missing_required"]:
            continue

        # success
        chosen = std
        chosen_meta = meta
        chosen_sheet = sh
        chosen_notes = "OK"
        break

    if chosen is None:
        # print a quick hint by loading first sheet and showing cols
        try:
            raw0 = pd.read_excel(path, sheet_name=xls.sheet_names[0], engine="openpyxl", nrows=5)
            cols0 = list(raw0.columns)
        except Exception:
            cols0 = []
        print(f"❌ {path.name} | could not find a sheet with DATE+COSTSET+HOURS")
        print(f"   Sheets tried: {len(sheet_candidates)} | First-sheet cols (sample): {cols0[:30]}")
        load_log.append({"file": str(path), "status": "MISSING REQUIRED COLS", "sheet": None, "rows": 0, "notes": "no valid sheet"})
        continue

    chosen["SOURCE"] = path.name
    chosen["SOURCE_SHEET"] = chosen_sheet
    chosen["COSTSET_NORM"] = chosen["COSTSET_RAW"].map(_map_costset)

    # debug mapping
    sample_costsets = sorted(chosen["COSTSET_RAW"].dropna().astype(str).str.strip().unique().tolist())[:8]
    print(f"\n✅ {path.name} | sheet={chosen_sheet} | rows={len(chosen):,}")
    print(f"   Costset raw sample: {sample_costsets}")
    print("   Costset mapped counts:")
    print(chosen["COSTSET_NORM"].value_counts(dropna=False).to_string())

    frames.append(chosen[["SOURCE","SOURCE_SHEET","SUB_TEAM","DATE","COSTSET_RAW","COSTSET_NORM","HOURS"]])
    load_log.append({"file": str(path), "status": "OK", "sheet": chosen_sheet, "rows": len(chosen), "notes": chosen_notes})

if not frames:
    raise RuntimeError("No valid files loaded. Fix file paths / sheet names / required columns and re-run.")

cobra_fact = pd.concat(frames, ignore_index=True)
cobra_fact["SUB_TEAM"] = cobra_fact["SUB_TEAM"].replace({"": "UNKNOWN"}).fillna("UNKNOWN")

print("\n--- LOAD LOG ---")
load_log_df = pd.DataFrame(load_log)
print(load_log_df[["file","status","sheet","rows","notes"]].to_string(index=False))

print(f"\nLoaded cobra_fact rows: {len(cobra_fact):,}")
print("Sources loaded:", sorted(cobra_fact["SOURCE"].unique().tolist()))
print("Date range:", cobra_fact["DATE"].min(), "to", cobra_fact["DATE"].max())

# ---------------------------
# 3) Coverage audit
# ---------------------------
print("\n" + "="*90)
print("2) COVERAGE AUDIT (source x metric)")
print("="*90)

coverage = (
    cobra_fact
    .groupby(["SOURCE","COSTSET_NORM"])
    .agg(
        rows=("HOURS","size"),
        sum_hours=("HOURS","sum"),
        min_date=("DATE","min"),
        max_date=("DATE","max"),
        n_subteams=("SUB_TEAM", lambda s: s.nunique())
    )
    .reset_index()
    .sort_values(["SOURCE","COSTSET_NORM"])
)

print(coverage.to_string(index=False))

# ---------------------------
# 4) Snapshot / close dates
# ---------------------------
print("\n" + "="*90)
print("3) SNAPSHOT / CLOSE DATES (driven by ACWP/BCWP)")
print("="*90)

snap_rows = []
for src in sorted(cobra_fact["SOURCE"].unique()):
    sdf = cobra_fact[cobra_fact["SOURCE"] == src]
    curr_close, prev_close = _choose_close_dates(sdf)
    snap_rows.append({
        "SOURCE": src,
        "snapshot_date": curr_close,
        "curr_close": curr_close,
        "prev_close": prev_close,
        "n_driver_dates": sdf.loc[sdf["COSTSET_NORM"].isin(["ACWP","BCWP"]), "DATE"].nunique()
    })

snapshots_df = pd.DataFrame(snap_rows)
print(snapshots_df.to_string(index=False))

# ---------------------------
# 5) Compute metrics (program + subteam)
# ---------------------------
print("\n" + "="*90)
print("4) COMPUTE PROGRAM METRICS (program-level = sum across subteams)")
print("="*90)

def compute_one_scope(scope_df: pd.DataFrame, src: str, subteam_label: str, curr_close: pd.Timestamp, prev_close: pd.Timestamp):
    # Program totals are simply scope_df = all rows for that source (no 'ALL' filter required).
    bcws_ctd = _ctd(scope_df, "BCWS", curr_close)
    bcwp_ctd = _ctd(scope_df, "BCWP", curr_close)
    acwp_ctd = _ctd(scope_df, "ACWP", curr_close)

    bcws_lsd = _lsd(scope_df, "BCWS", prev_close, curr_close)
    bcwp_lsd = _lsd(scope_df, "BCWP", prev_close, curr_close)
    acwp_lsd = _lsd(scope_df, "ACWP", prev_close, curr_close)

    spi_ctd = _safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = _safe_div(bcwp_ctd, acwp_ctd)

    spi_lsd = _safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = _safe_div(bcwp_lsd, acwp_lsd)

    # BAC: total budget at completion (sum of all BCWS across all dates)
    bac = float(scope_df.loc[scope_df["COSTSET_NORM"]=="BCWS","HOURS"].sum()) if (scope_df["COSTSET_NORM"]=="BCWS").any() else np.nan

    # ETC totals: remaining ETC after curr_close (timephased)
    etc_total = float(scope_df.loc[(scope_df["COSTSET_NORM"]=="ETC") & (scope_df["DATE"] > curr_close), "HOURS"].sum()) if (scope_df["COSTSET_NORM"]=="ETC").any() and not pd.isna(curr_close) else np.nan

    # EAC (simple): ACWP_CTD + ETC_TOTAL
    eac = (acwp_ctd + etc_total) if (not pd.isna(acwp_ctd) and not pd.isna(etc_total)) else np.nan

    # Next month windows
    nm_start, nm_end = _month_window_after(curr_close)
    next_mo_etc = _sum_in_window(scope_df, "ETC", nm_start, nm_end)
    next_mo_bcws = _sum_in_window(scope_df, "BCWS", nm_start, nm_end)

    # Demand vs Actual (for your variance fields):
    # Demand_Hours := BCWP_LSD (earned this period)
    # Actual_Hours := ACWP_LSD (actuals this period)
    demand = bcwp_lsd
    actual = acwp_lsd
    pct_var = _safe_div((actual - demand), demand)

    # diagnostics
    diag = {
        "ACWP_rows_at_curr_close": int(scope_df[(scope_df["COSTSET_NORM"]=="ACWP") & (scope_df["DATE"]==curr_close)].shape[0]) if not pd.isna(curr_close) else 0,
        "BCWP_rows_at_curr_close": int(scope_df[(scope_df["COSTSET_NORM"]=="BCWP") & (scope_df["DATE"]==curr_close)].shape[0]) if not pd.isna(curr_close) else 0,
        "ACWP_rows_in_window": int(scope_df[(scope_df["COSTSET_NORM"]=="ACWP") & (scope_df["DATE"]>prev_close) & (scope_df["DATE"]<=curr_close)].shape[0]) if (not pd.isna(prev_close) and not pd.isna(curr_close)) else 0,
        "BCWP_rows_in_window": int(scope_df[(scope_df["COSTSET_NORM"]=="BCWP") & (scope_df["DATE"]>prev_close) & (scope_df["DATE"]<=curr_close)].shape[0]) if (not pd.isna(prev_close) and not pd.isna(curr_close)) else 0,
        "BCWS_rows_in_window": int(scope_df[(scope_df["COSTSET_NORM"]=="BCWS") & (scope_df["DATE"]>prev_close) & (scope_df["DATE"]<=curr_close)].shape[0]) if (not pd.isna(prev_close) and not pd.isna(curr_close)) else 0,
    }

    return {
        "SOURCE": src,
        "SUB_TEAM": subteam_label,
        "SNAPSHOT_DATE": curr_close,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,

        "BCWS_CTD": bcws_ctd,
        "BCWP_CTD": bcwp_ctd,
        "ACWP_CTD": acwp_ctd,

        "BCWS_LSD": bcws_lsd,
        "BCWP_LSD": bcwp_lsd,
        "ACWP_LSD": acwp_lsd,

        "SPI_CTD": spi_ctd,
        "CPI_CTD": cpi_ctd,
        "SPI_LSD": spi_lsd,
        "CPI_LSD": cpi_lsd,

        "BAC": bac,
        "ETC_TOTAL": etc_total,
        "EAC": eac,

        "Next_Mo_BCWS_Hours": next_mo_bcws,
        "Next_Mo_ETC_Hours": next_mo_etc,

        "Demand_Hours": demand,
        "Actual_Hours": actual,
        "Pct_Var": pct_var,

        # BEI intentionally excluded

        **diag
    }

# Program-level
program_rows = []
for row in snapshots_df.itertuples(index=False):
    src = row.SOURCE
    sdf = cobra_fact[cobra_fact["SOURCE"] == src]
    program_rows.append(compute_one_scope(sdf, src, "PROGRAM_TOTAL", row.curr_close, row.prev_close))

program_metrics = pd.DataFrame(program_rows)

# Debug slices (show why your old program table went NaN)
print("\n--- DEBUG: PROGRAM-LEVEL SLICE CHECK (this should NOT be empty) ---")
for row in snapshots_df.itertuples(index=False):
    src = row.SOURCE
    sdf = cobra_fact[cobra_fact["SOURCE"] == src]
    print(f"\nSOURCE={src}")
    print(f"  prev_close={row.prev_close}  curr_close={row.curr_close}")
    for m in ["ACWP","BCWP","BCWS","ETC"]:
        ctd = _ctd(sdf, m, row.curr_close)
        lsd = _lsd(sdf, m, row.prev_close, row.curr_close) if not pd.isna(row.prev_close) else np.nan
        n_exact = sdf[(sdf["COSTSET_NORM"]==m) & (sdf["DATE"]==row.curr_close)].shape[0] if not pd.isna(row.curr_close) else 0
        print(f"  {m}: CTD@curr={ctd:.4f}  LSD={lsd if pd.notna(lsd) else np.nan}  rows_at_curr_close={n_exact}")

print("\nPROGRAM METRICS (preview):")
print(program_metrics.sort_values("SOURCE").to_string(index=False))

# Subteam-level
print("\n" + "="*90)
print("5) COMPUTE SUBTEAM METRICS")
print("="*90)

subteam_rows = []
for row in snapshots_df.itertuples(index=False):
    src = row.SOURCE
    sdf_all = cobra_fact[cobra_fact["SOURCE"] == src]
    subteams = sorted(sdf_all["SUB_TEAM"].unique().tolist())

    for st in subteams:
        sdf = sdf_all[sdf_all["SUB_TEAM"] == st]
        subteam_rows.append(compute_one_scope(sdf, src, st, row.curr_close, row.prev_close))

subteam_metrics = pd.DataFrame(subteam_rows)

print("\nSUBTEAM METRICS (first 30 rows):")
print(subteam_metrics.sort_values(["SOURCE","SUB_TEAM"]).head(30).to_string(index=False))

# ---------------------------
# 6) Missing / NaN diagnostics
# ---------------------------
print("\n" + "="*90)
print("6) MISSING / NaN DIAGNOSTICS")
print("="*90)

def nan_pct_by_source(df: pd.DataFrame, label: str):
    cols = [c for c in df.columns if c not in {"SOURCE","SUB_TEAM","SNAPSHOT_DATE","CURR_CLOSE","PREV_CLOSE"}]
    out = (
        df.groupby("SOURCE")[cols]
        .apply(lambda g: g.isna().mean())
        .reset_index()
    )
    out = out.rename(columns={c: f"pct_nan_{c}" for c in cols})
    print(f"\n{label} %NaN by source:")
    print(out.to_string(index=False))
    return out

nan_program = nan_pct_by_source(program_metrics, "Program-level")
nan_subteam = nan_pct_by_source(subteam_metrics, "Subteam-level")

print("\nIMPORTANT INTERPRETATION NOTES:")
print(" - Program-level rows should now be populated (no SUB_TEAM=='ALL' dependency).")
print(" - ACWP/BCWP are computed as snapshot sums at DATE==close (matches your exports).")
print(" - BCWS_LSD is computed as a WINDOW SUM (prev_close, curr_close] because BCWS is timephased.")
print(" - BEI is intentionally excluded (requires Open Plan activity file).")

print("\nObjects in memory: cobra_fact, coverage, snapshots_df, program_metrics, subteam_metrics, nan_program, nan_subteam")