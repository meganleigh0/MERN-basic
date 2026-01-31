# EVMS COBRA pipeline (COST-SET driven, selected files only, no Excel output)
# - Only uses: DATE + COSTSET + HOURS (+ SUB_TEAM if present)
# - Robust to different COSTSET wording (Budget/Progress/Weekly Actuals/etc.)
# - Computes program + subteam: BCWS/BCWP/ACWP (CTD + LSD), SPI/CPI (CTD + LSD), BEI (CTD + LSD)
# - Computes BAC/EAC/VAC (program + subteam)
# - Computes hours metrics: Demand_Hours, Actual_Hours, %Var, Next_Mo_BCWS_Hours, Next_Mo_ETC_Hours
#
# Outputs (in memory):
#   cobra_fact, program_metrics, subteam_metrics, subteam_cost, hours_metrics,
#   coverage_audit, value_from_audit, missing_summary, pipeline_issues

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = Path("data")

# Pick EXACT files you want to test (edit these names to match your folder)
SELECT_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
    "Cobra-XM30.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
]

# Which sheets to consider (we score & pick best per file)
SHEET_KEYWORDS = ["tbl", "weekly", "extract", "cap", "report", "evms"]

# 2026 accounting period close dates (replace if you have the authoritative list)
# IMPORTANT: this list is only used to pick CURR_CLOSE/PREV_CLOSE/NEXT_CLOSE.
# If none match <= snapshot date, we fall back to month-end.
ACCOUNTING_CLOSE_DATES_2026 = pd.to_datetime([
    "2026-01-04",
    "2026-02-01",
    "2026-03-01",
    "2026-03-29",
    "2026-04-05",
    "2026-05-03",
    "2026-05-31",
    "2026-06-28",
    "2026-07-05",
    "2026-08-02",
    "2026-08-30",
    "2026-09-27",
    "2026-10-04",
    "2026-11-01",
    "2026-11-29",
    "2026-12-27",
], errors="coerce").dropna().sort_values()

# -----------------------------
# HELPERS: normalize columns + costset values
# -----------------------------
def _clean_col(c: str) -> str:
    c = str(c).strip()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^A-Za-z0-9_]+", "", c)
    return c.upper()

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_clean_col(c) for c in out.columns]
    # common aliases -> canonical
    rename_map = {}
    for c in out.columns:
        if c in ["COST-SET", "COSTSET", "COST_SET"]:
            rename_map[c] = "COSTSET"
        if c in ["SUBTEAM", "SUB_TEAM", "SUBTEAM_NAME"]:
            rename_map[c] = "SUB_TEAM"
    if rename_map:
        out = out.rename(columns=rename_map)
    return out

def _ensure_required_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    issues = []
    df = _normalize_columns(df)

    # DATE
    if "DATE" not in df.columns:
        # try common date cols
        for cand in ["PERIOD", "STATUS_DATE", "AS_OF_DATE"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "DATE"})
                break
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    else:
        issues.append("Missing DATE column")

    # COSTSET
    if "COSTSET" not in df.columns:
        issues.append("Missing COSTSET column")

    # HOURS (the value column)
    if "HOURS" not in df.columns:
        # try other common numeric columns
        for cand in ["AMOUNT", "VALUE", "HRS", "HOUR"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "HOURS"})
                break
    if "HOURS" in df.columns:
        df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")
    else:
        issues.append("Missing HOURS column")

    # optional: SUB_TEAM (if missing, we still compute program-level)
    if "SUB_TEAM" not in df.columns:
        df["SUB_TEAM"] = "PROGRAM"

    return df, issues

def _norm_costset_val(x) -> str:
    s = "" if pd.isna(x) else str(x)
    s = s.strip().upper()
    s = re.sub(r"[\s\-_/]+", " ", s)
    s = re.sub(r"[^A-Z0-9 ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# Map COSTSET values -> canonical metric buckets
# We only look at COSTSET values (NOT other columns like Currency/Plug)
COSTSET_RULES = [
    ("ACWP", re.compile(r"\bACWP\b|\bACTUAL\b|\bWEEKLY ACTUALS?\b|\bACWP HRS\b|\bACWP_HRS\b|\bACWP WKL\b|\bACWP_WKL\b")),
    ("BCWS", re.compile(r"\bBCWS\b|\bBUDGET\b|\bPLANNED\b|\bPLAN\b|\bBUDGET PV\b|\bPV\b")),
    ("BCWP", re.compile(r"\bBCWP\b|\bEARNED\b|\bPROGRESS\b|\bPERFORM\b")),
    ("ETC",  re.compile(r"\bETC\b|\bESTIMATE TO COMPLETE\b|\bREMAINING\b|\bTO GO\b")),
    ("EAC",  re.compile(r"\bEAC\b|\bESTIMATE AT COMPLETE\b")),
    # optional buckets you might care about later:
    ("OTHER", re.compile(r".*")),
]

def _assign_metric_bucket(costset_series: pd.Series) -> pd.Series:
    norm = costset_series.map(_norm_costset_val)
    out = []
    for v in norm:
        b = "OTHER"
        for name, rx in COSTSET_RULES:
            if rx.search(v):
                b = name
                break
        out.append(b)
    return pd.Series(out, index=costset_series.index, dtype="object")

# -----------------------------
# SHEET PICKER (robust)
# -----------------------------
def _best_sheet(path: Path) -> tuple[str, pd.DataFrame, list[str]]:
    issues = []
    xl = pd.ExcelFile(path)
    best = None
    best_score = -1
    best_cols = []

    # score each sheet by:
    # - keyword hits in sheet name
    # - presence of DATE + COSTSET + HOURS
    for sh in xl.sheet_names:
        score = 0
        name_l = sh.lower()
        score += sum(1 for k in SHEET_KEYWORDS if k in name_l)

        try:
            hdr = pd.read_excel(path, sheet_name=sh, nrows=0)
            hdr = _normalize_columns(hdr)
            cols = set(hdr.columns)
            needed = {"DATE", "COSTSET", "HOURS"}
            # allow aliases: if COSTSET/HOURS not directly present, still let it compete
            # (we will normalize later), but favor direct matches
            score += int("DATE" in cols) + int("COSTSET" in cols) + int("HOURS" in cols)
            # small bonus for having SUB_TEAM
            score += 1 if "SUB_TEAM" in cols else 0
            # small penalty for very wide or very narrow sheets
            score -= abs(len(cols) - 15) / 50.0
        except Exception:
            continue

        if score > best_score:
            best = sh
            best_score = score
            best_cols = list(cols) if 'cols' in locals() else []

    if best is None:
        # fallback first sheet
        best = xl.sheet_names[0]
        issues.append("Could not score sheets; used first sheet.")

    df = pd.read_excel(path, sheet_name=best)
    df, req_issues = _ensure_required_cols(df)
    issues.extend(req_issues)

    return best, df, issues

# -----------------------------
# ACCOUNTING CLOSE UTILITIES
# -----------------------------
def _close_pair_for_snapshot(snapshot_date: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    """
    Returns (curr_close, prev_close, next_close).
    Uses ACCOUNTING_CLOSE_DATES_2026 if possible; else falls back to month-ends around snapshot.
    """
    if pd.isna(snapshot_date):
        return (pd.NaT, pd.NaT, pd.NaT)

    closes = ACCOUNTING_CLOSE_DATES_2026.copy()
    closes = closes.sort_values()

    le = closes[closes <= snapshot_date]
    ge = closes[closes > snapshot_date]

    if len(le) >= 2:
        curr_close, prev_close = le[-1], le[-2]
    elif len(le) == 1:
        curr_close, prev_close = le[-1], le[-1]
    else:
        # fallback month-ends
        curr_close = snapshot_date.to_period("M").to_timestamp("M")
        prev_close = (snapshot_date.to_period("M") - 1).to_timestamp("M")

    if len(ge) >= 1:
        next_close = ge[0]
    else:
        # fallback: next month end
        next_close = (snapshot_date.to_period("M") + 1).to_timestamp("M")

    return curr_close, prev_close, next_close

# -----------------------------
# CORE AGG: sum by metric bucket over windows
# -----------------------------
def _sum_metric(df: pd.DataFrame, bucket: str, start_exclusive: pd.Timestamp | None, end_inclusive: pd.Timestamp | None) -> float:
    x = df
    x = x[x["METRIC"] == bucket]
    if start_exclusive is not None and not pd.isna(start_exclusive):
        x = x[x["DATE"] > start_exclusive]
    if end_inclusive is not None and not pd.isna(end_inclusive):
        x = x[x["DATE"] <= end_inclusive]
    return float(x["HOURS"].sum(skipna=True))

def _safe_div(num: float, den: float) -> float:
    if den is None or den == 0 or np.isnan(den):
        return np.nan
    return num / den

def _compute_scope_metrics(scope_df: pd.DataFrame, snapshot_date: pd.Timestamp) -> dict:
    """
    Compute all metrics for one scope (program or subteam).
    Only uses DATE + METRIC (from COSTSET) + HOURS.
    """
    curr_close, prev_close, next_close = _close_pair_for_snapshot(snapshot_date)

    # CTD (cumulative up to close)
    bcws_ctd = _sum_metric(scope_df, "BCWS", None, curr_close)
    bcwp_ctd = _sum_metric(scope_df, "BCWP", None, curr_close)
    acwp_ctd = _sum_metric(scope_df, "ACWP", None, curr_close)

    # LSD (this accounting period)
    bcws_lsd = _sum_metric(scope_df, "BCWS", prev_close, curr_close)
    bcwp_lsd = _sum_metric(scope_df, "BCWP", prev_close, curr_close)
    acwp_lsd = _sum_metric(scope_df, "ACWP", prev_close, curr_close)

    # Next month planned/remaining (if present as timephased)
    next_bcws = _sum_metric(scope_df, "BCWS", curr_close, next_close)
    next_etc  = _sum_metric(scope_df, "ETC",  curr_close, next_close)

    # BAC: interpret as total planned baseline across full file (sum of BCWS across all dates)
    # (this avoids missing BAC for files where "Budget" is the only baseline signal)
    bac_total = float(scope_df.loc[scope_df["METRIC"] == "BCWS", "HOURS"].sum(skipna=True))

    # EAC / ETC totals: some exports provide them as a single "status" line; some as timephased.
    # Use TOTAL across file as robust default (works for both: single line or timephased).
    eac_total = float(scope_df.loc[scope_df["METRIC"] == "EAC", "HOURS"].sum(skipna=True))
    etc_total = float(scope_df.loc[scope_df["METRIC"] == "ETC", "HOURS"].sum(skipna=True))

    # If EAC is not present but ETC is, approximate EAC ~= ACWP_CTD + ETC_total (common EVMS relationship).
    if (eac_total == 0 or np.isnan(eac_total)) and (etc_total and etc_total > 0):
        eac_total = acwp_ctd + etc_total

    vac = bac_total - eac_total if (bac_total and eac_total) else np.nan

    spi_ctd = _safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = _safe_div(bcwp_ctd, acwp_ctd)
    spi_lsd = _safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = _safe_div(bcwp_lsd, acwp_lsd)

    # BEI: if you don’t have discrete milestones/BCWP events, a practical proxy is EV-based efficiency.
    # Using BCWP/BCWS aligns to schedule performance; if you later add milestones, swap this.
    bei_ctd = spi_ctd
    bei_lsd = spi_lsd

    # Demand/Actual hours for the current period
    demand_hours = bcws_lsd
    actual_hours = acwp_lsd
    pct_var = _safe_div((actual_hours - demand_hours), demand_hours)

    return {
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,
        "NEXT_CLOSE": next_close,

        "BCWS_CTD": bcws_ctd,
        "BCWP_CTD": bcwp_ctd,
        "ACWP_CTD": acwp_ctd,

        "BCWS_LSD": bcws_lsd,
        "BCWP_LSD": bcwp_lsd,
        "ACWP_LSD": acwp_lsd,

        "SPI_CTD": spi_ctd,
        "CPI_CTD": cpi_ctd,
        "BEI_CTD": bei_ctd,

        "SPI_LSD": spi_lsd,
        "CPI_LSD": cpi_lsd,
        "BEI_LSD": bei_lsd,

        "BAC": bac_total,
        "EAC": eac_total,
        "VAC": vac,

        "Demand_Hours": demand_hours,
        "Actual_Hours": actual_hours,
        "Pct_Var": pct_var,
        "Next_Mo_BCWS_Hours": next_bcws,
        "Next_Mo_ETC_Hours": next_etc,
    }

# -----------------------------
# LOAD SELECTED FILES
# -----------------------------
pipeline_issues = []
frames = []
coverage_rows = []

for fn in SELECT_FILES:
    p = DATA_DIR / fn
    if not p.exists():
        pipeline_issues.append(f"Missing file: {fn}")
        continue

    sheet_name, df, issues = _best_sheet(p)
    if issues:
        pipeline_issues.extend([f"{fn} | {x}" for x in issues])

    # Keep only the columns we actually need + provenance
    keep_cols = [c for c in ["DATE", "COSTSET", "HOURS", "SUB_TEAM"] if c in df.columns]
    df = df[keep_cols].copy()

    df["SOURCE"] = p.name
    df["SOURCE_SHEET"] = sheet_name

    # metric bucket from COSTSET values
    df["COSTSET_NORM"] = df["COSTSET"].map(_norm_costset_val) if "COSTSET" in df.columns else ""
    df["METRIC"] = _assign_metric_bucket(df["COSTSET"])

    # coverage diagnostics (do NOT filter anything out here)
    snap = df["DATE"].max() if "DATE" in df.columns else pd.NaT
    coverage_rows.append({
        "SOURCE": p.name,
        "SHEET": sheet_name,
        "rows": len(df),
        "min_DATE": df["DATE"].min() if "DATE" in df.columns else pd.NaT,
        "max_DATE": snap,
        "has_DATE": "DATE" in df.columns,
        "has_COSTSET": "COSTSET" in df.columns,
        "has_HOURS": "HOURS" in df.columns,
        "has_SUB_TEAM": "SUB_TEAM" in df.columns,
        "metric_counts": df["METRIC"].value_counts(dropna=False).to_dict(),
    })

    frames.append(df)

cobra_fact = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# basic clean: drop rows missing required trio (DATE/COSTSET/HOURS)
if not cobra_fact.empty:
    cobra_fact = cobra_fact.dropna(subset=["DATE", "COSTSET", "HOURS"]).copy()

# -----------------------------
# AUDITS (what values exist, where)
# -----------------------------
coverage_audit = pd.DataFrame(coverage_rows)

# How much of each metric bucket is present per source?
value_from_audit = (cobra_fact
    .groupby(["SOURCE", "METRIC"], as_index=False)
    .agg(rows=("HOURS", "size"),
         nonnull_hours=("HOURS", lambda s: int(s.notna().sum())),
         sum_hours=("HOURS", "sum"),
         min_date=("DATE", "min"),
         max_date=("DATE", "max"))
    .sort_values(["SOURCE", "METRIC"])
)

# -----------------------------
# METRICS: program + subteam
# -----------------------------
program_rows = []
subteam_rows = []
subteam_cost_rows = []
hours_rows = []

if not cobra_fact.empty:
    for src, src_df in cobra_fact.groupby("SOURCE"):
        snapshot = src_df["DATE"].max()

        # program (whole file)
        m = _compute_scope_metrics(src_df, snapshot)
        m["SOURCE"] = src
        program_rows.append(m)

        # subteams (if any)
        for st, st_df in src_df.groupby("SUB_TEAM"):
            mm = _compute_scope_metrics(st_df, snapshot)
            mm["SOURCE"] = src
            mm["SUB_TEAM"] = st
            subteam_rows.append(mm)

            # cost-only table (BAC/EAC/VAC)
            subteam_cost_rows.append({
                "SOURCE": src,
                "SUB_TEAM": st,
                "SNAPSHOT_DATE": snapshot,
                "BAC": mm["BAC"],
                "EAC": mm["EAC"],
                "VAC": mm["VAC"],
            })

            # hours-metrics table
            hours_rows.append({
                "SOURCE": src,
                "SUB_TEAM": st,
                "SNAPSHOT_DATE": snapshot,
                "CURR_CLOSE": mm["CURR_CLOSE"],
                "PREV_CLOSE": mm["PREV_CLOSE"],
                "Demand_Hours": mm["Demand_Hours"],
                "Actual_Hours": mm["Actual_Hours"],
                "Pct_Var": mm["Pct_Var"],
                "Next_Mo_BCWS_Hours": mm["Next_Mo_BCWS_Hours"],
                "Next_Mo_ETC_Hours": mm["Next_Mo_ETC_Hours"],
            })

program_metrics = pd.DataFrame(program_rows)
subteam_metrics = pd.DataFrame(subteam_rows)
subteam_cost = pd.DataFrame(subteam_cost_rows)
hours_metrics = pd.DataFrame(hours_rows)

# -----------------------------
# MISSING SUMMARY (why things look "missing")
# -----------------------------
def _pct_nan(s: pd.Series) -> float:
    if len(s) == 0:
        return np.nan
    return float(s.isna().mean())

if not subteam_metrics.empty:
    missing_summary = (subteam_metrics
        .groupby("SOURCE", as_index=False)
        .agg(
            subteams=("SUB_TEAM", "nunique"),
            pct_BCWS_CTD_missing=("BCWS_CTD", _pct_nan),
            pct_BCWP_CTD_missing=("BCWP_CTD", _pct_nan),
            pct_ACWP_CTD_missing=("ACWP_CTD", _pct_nan),
            pct_BAC_missing=("BAC", _pct_nan),
            pct_EAC_missing=("EAC", _pct_nan),
            pct_SPI_CTD_missing=("SPI_CTD", _pct_nan),
            pct_CPI_CTD_missing=("CPI_CTD", _pct_nan),
        )
        .sort_values("SOURCE")
    )
else:
    missing_summary = pd.DataFrame()

# -----------------------------
# PREVIEW PRINTS
# -----------------------------
print("✅ Loaded rows:", len(cobra_fact))
print("✅ Sources:", cobra_fact["SOURCE"].nunique() if not cobra_fact.empty else 0)
print("\n--- Pipeline issues (if any) ---")
for x in pipeline_issues[:50]:
    print(" -", x)
if len(pipeline_issues) > 50:
    print(f" ... ({len(pipeline_issues)-50} more)")

print("\n--- METRIC/UNIT COVERAGE (per source, per metric bucket) ---")
display(value_from_audit)

print("\n--- PROGRAM METRICS (preview) ---")
display(program_metrics)

print("\n--- SUBTEAM METRICS (preview) ---")
display(subteam_metrics.head(50))

print("\n--- MISSING SUMMARY (by source) ---")
display(missing_summary)

print("\n✅ Outputs in memory:",
      "cobra_fact, program_metrics, subteam_metrics, subteam_cost, hours_metrics, "
      "coverage_audit, value_from_audit, missing_summary, pipeline_issues")
    