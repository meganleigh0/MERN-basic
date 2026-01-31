# EVMS COBRA pipeline (robust, COST-SET driven, selected files only, no Excel output)
import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = Path("data")

# Pick EXACT file names you want to test (edit as needed)
TARGET_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
    "Cobra-XM30.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
    # add one more Stryker if you want:
    # "Cobra-Stryker C4ISR -F0162.xlsx",
]

SHEET_KEYWORDS = ["tbl", "weekly", "extract", "cap_extract", "cap"]  # wide net

# IMPORTANT:
# Use accounting close dates. Your screenshots show 2026-11-01 and 2026-12-06 being used.
# Put your true close dates here (edit this list to match your calendar).
ACCOUNTING_CLOSE_DATES = pd.to_datetime(sorted([
    # --- 2026 close dates (PLACEHOLDER; replace with your official closes) ---
    # Example values you already saw in output:
    "2026-11-01",
    "2026-12-06",
    # add the rest...
]))

# -----------------------------
# HELPERS
# -----------------------------
def _clean_colname(c: str) -> str:
    c = str(c).strip()
    c = re.sub(r"\s+", " ", c)
    c = c.replace("\u00A0", " ")
    return c

def _norm_costset_value(x) -> str:
    """Normalize COST-SET cell values so label matching works across files."""
    s = str(x) if x is not None else ""
    s = s.strip().upper()
    s = s.replace("\u00A0", " ")
    s = re.sub(r"\s+", " ", s)
    # remove punctuation except underscores
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # normalize common variants
    # ACWP variants
    s = s.replace("ACWP HRS", "ACWP_HRS")
    s = s.replace("ACWP HOURS", "ACWP_HRS")
    s = s.replace("ACWP WKLY", "ACWP_WKL")
    s = s.replace("WEEKLY ACTUALS", "ACWP_WKL")
    # Budget variants
    if s in {"BUDGET", "BUDGET PV", "BUDGET_PV", "PV"}:
        s = "BUDGET"
    # Progress variants often mean earned (BCWP)
    if s in {"PROGRESS", "EARNED", "EARNED VALUE", "PERFORM", "PERFORMANCE"}:
        s = "BCWP"
    # ETC variants
    if s in {"ETC", "ESTIMATE TO COMPLETE", "ESTIMATED TO COMPLETE", "TO GO", "REMAINING"}:
        s = "ETC"
    # EAC variants
    if s in {"EAC", "ESTIMATE AT COMPLETION", "ESTIMATED AT COMPLETION"}:
        s = "EAC"

    return s

def _pick_best_sheet(xl: pd.ExcelFile) -> str:
    """Pick the best sheet by keywords; fallback to first sheet."""
    scores = []
    for sh in xl.sheet_names:
        sh_l = sh.lower()
        score = sum(1 for k in SHEET_KEYWORDS if k in sh_l)
        scores.append((score, sh))
    scores.sort(reverse=True)
    return scores[0][1] if scores else xl.sheet_names[0]

def _ensure_required_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_clean_colname(c) for c in df.columns]

    # Harmonize common column name differences
    rename_map = {}
    for c in df.columns:
        cl = c.lower()
        if cl in {"cost-set", "cost set", "costset", "cost_set"}:
            rename_map[c] = "COSTSET"
        elif cl in {"date", "as of date", "period date", "time", "status date"}:
            rename_map[c] = "DATE"
        elif cl in {"hours", "hrs", "value", "amount"}:
            # In your exports, HOURS is the numeric payload even if units are “currency” sometimes.
            rename_map[c] = "HOURS"
        elif cl in {"sub_team", "subteam", "sub team", "sub-team"}:
            rename_map[c] = "SUB_TEAM"
        elif cl in {"plug", "unit"}:
            # keep but not required
            rename_map[c] = "UNIT"

    if rename_map:
        df = df.rename(columns=rename_map)

    # If SUB_TEAM missing, set to PROGRAM for rollups
    if "SUB_TEAM" not in df.columns:
        df["SUB_TEAM"] = "PROGRAM"

    # Parse date & numeric
    if "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    if "HOURS" in df.columns:
        df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    # Normalize costset values
    if "COSTSET" in df.columns:
        df["COSTSET_NORM"] = df["COSTSET"].map(_norm_costset_value)
    else:
        df["COSTSET_NORM"] = np.nan

    return df

def _close_pair_for_snapshot(snapshot_date: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp]:
    """
    Returns (curr_close, prev_close) based on ACCOUNTING_CLOSE_DATES.
    If close dates list is incomplete for the snapshot year, fallback to month-ends.
    """
    if pd.isna(snapshot_date):
        return (pd.NaT, pd.NaT)

    closes = ACCOUNTING_CLOSE_DATES.dropna().sort_values()
    closes = closes[closes <= snapshot_date]

    if len(closes) >= 2:
        return closes.iloc[-1], closes.iloc[-2]
    if len(closes) == 1:
        return closes.iloc[-1], closes.iloc[-1]

    # fallback: month-ends from snapshot
    me = snapshot_date.to_period("M").to_timestamp("M")
    prev_me = (snapshot_date.to_period("M") - 1).to_timestamp("M")
    return me, prev_me

def _sum_by_window(df: pd.DataFrame, costset_norm: str, start_exclusive, end_inclusive) -> float:
    d = df
    if pd.isna(end_inclusive):
        return np.nan
    m = (d["COSTSET_NORM"] == costset_norm) & (d["DATE"].notna()) & (d["HOURS"].notna())
    m &= (d["DATE"] <= end_inclusive)
    if pd.notna(start_exclusive):
        m &= (d["DATE"] > start_exclusive)
    return float(d.loc[m, "HOURS"].sum())

def _month_sum(df: pd.DataFrame, costset_norm: str, period: pd.Period) -> float:
    if period is None or pd.isna(period):
        return np.nan
    d = df
    m = (d["COSTSET_NORM"] == costset_norm) & d["DATE"].notna() & d["HOURS"].notna()
    m &= (d["DATE"].dt.to_period("M") == period)
    return float(d.loc[m, "HOURS"].sum())

def _compute_one_scope(scope_df: pd.DataFrame, snapshot_date: pd.Timestamp) -> dict:
    """
    Compute all metrics for one (source) or (source, subteam) slice.
    Only uses COSTSET_NORM + DATE + HOURS.
    """
    curr_close, prev_close = _close_pair_for_snapshot(snapshot_date)

    # CTD cumulative to curr_close
    bcws_ctd = _sum_by_window(scope_df, "BCWS", None, curr_close)
    bcwp_ctd = _sum_by_window(scope_df, "BCWP", None, curr_close)
    acwp_ctd = _sum_by_window(scope_df, "ACWP", None, curr_close)

    # LSD window (period between prev_close and curr_close)
    bcws_lsd = _sum_by_window(scope_df, "BCWS", prev_close, curr_close)
    bcwp_lsd = _sum_by_window(scope_df, "BCWP", prev_close, curr_close)
    acwp_lsd = _sum_by_window(scope_df, "ACWP", prev_close, curr_close)

    # SPI/CPI
    spi_ctd = (bcwp_ctd / bcws_ctd) if bcws_ctd and bcws_ctd != 0 else np.nan
    cpi_ctd = (bcwp_ctd / acwp_ctd) if acwp_ctd and acwp_ctd != 0 else np.nan
    spi_lsd = (bcwp_lsd / bcws_lsd) if bcws_lsd and bcws_lsd != 0 else np.nan
    cpi_lsd = (bcwp_lsd / acwp_lsd) if acwp_lsd and acwp_lsd != 0 else np.nan

    # BEI (treat as incremental earned-vs-planned execution index in the LSD window)
    bei_ctd = spi_ctd
    bei_lsd = spi_lsd

    # BAC/EAC/ETC/VAC:
    # BAC from full BUDGET total (timephased) across ALL dates in extract
    bac = float(scope_df.loc[(scope_df["COSTSET_NORM"] == "BUDGET") & scope_df["HOURS"].notna(), "HOURS"].sum())

    # EAC/ETC totals across all dates (works whether they are timephased or not)
    eac = float(scope_df.loc[(scope_df["COSTSET_NORM"] == "EAC") & scope_df["HOURS"].notna(), "HOURS"].sum())
    etc = float(scope_df.loc[(scope_df["COSTSET_NORM"] == "ETC") & scope_df["HOURS"].notna(), "HOURS"].sum())

    # If EAC not present but ETC present, approximate EAC = ACWP_CTD + ETC_total (common hours forecast pattern)
    eac_eff = eac if eac and eac != 0 else (acwp_ctd + etc if (pd.notna(acwp_ctd) and pd.notna(etc)) else np.nan)
    vac = (bac - eac_eff) if (pd.notna(bac) and pd.notna(eac_eff)) else np.nan

    # Hours metrics (status month = month of curr_close)
    status_period = curr_close.to_period("M") if pd.notna(curr_close) else (snapshot_date.to_period("M") if pd.notna(snapshot_date) else None)
    next_period = (status_period + 1) if status_period is not None else None

    demand_hours = _month_sum(scope_df, "BCWS", status_period)
    actual_hours = _month_sum(scope_df, "ACWP", status_period)
    pct_var = (actual_hours / demand_hours - 1.0) if (demand_hours and demand_hours != 0) else np.nan

    next_mo_bcws_hours = _month_sum(scope_df, "BCWS", next_period)
    next_mo_etc_hours = _month_sum(scope_df, "ETC", next_period)

    return dict(
        SNAPSHOT_DATE=snapshot_date,
        CURR_CLOSE=curr_close,
        PREV_CLOSE=prev_close,
        BCWS_CTD=bcws_ctd, BCWP_CTD=bcwp_ctd, ACWP_CTD=acwp_ctd,
        BCWS_LSD=bcws_lsd, BCWP_LSD=bcwp_lsd, ACWP_LSD=acwp_lsd,
        SPI_CTD=spi_ctd, CPI_CTD=cpi_ctd, BEI_CTD=bei_ctd,
        SPI_LSD=spi_lsd, CPI_LSD=cpi_lsd, BEI_LSD=bei_lsd,
        BAC=bac, EAC=eac_eff, ETC=etc, VAC=vac,
        Demand_Hours=demand_hours, Actual_Hours=actual_hours, Pct_Var=pct_var,
        Next_Mo_BCWS_Hours=next_mo_bcws_hours, Next_Mo_ETC_Hours=next_mo_etc_hours,
    )

# -----------------------------
# LOAD SELECTED FILES
# -----------------------------
frames = []
file_log = []

for fn in TARGET_FILES:
    p = DATA_DIR / fn
    if not p.exists():
        file_log.append({"source": fn, "status": "missing file on disk", "sheet": None, "rows": 0})
        continue

    try:
        xl = pd.ExcelFile(p)
        sh = _pick_best_sheet(xl)
        df = pd.read_excel(xl, sheet_name=sh)
        df = _ensure_required_cols(df)
        df["SOURCE"] = p.name
        df["SOURCE_SHEET"] = sh
        frames.append(df)
        file_log.append({"source": p.name, "status": "loaded", "sheet": sh, "rows": len(df)})
    except Exception as e:
        file_log.append({"source": p.name, "status": f"error: {e}", "sheet": None, "rows": 0})

cobra_fact = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

file_log = pd.DataFrame(file_log)

# -----------------------------
# COVERAGE / AUDITS (THIS IS WHERE “MISSING” USUALLY COMES FROM)
# -----------------------------
# Cost-set presence per source
coverage = (
    cobra_fact.dropna(subset=["COSTSET_NORM"])
    .groupby(["SOURCE", "COSTSET_NORM"], as_index=False)
    .agg(rows=("HOURS", "size"), nonnull=("HOURS", lambda s: int(s.notna().sum())), total=("HOURS", "sum"))
)

# Quick “do we have the labels we need” audit
NEEDED_LABELS = ["BCWS", "BCWP", "ACWP", "BUDGET", "EAC", "ETC"]
label_audit = (
    cobra_fact.groupby(["SOURCE", "COSTSET_NORM"], as_index=False)
    .size()
    .pivot_table(index="SOURCE", columns="COSTSET_NORM", values="size", fill_value=0)
    .reset_index()
)
for lab in NEEDED_LABELS:
    if lab not in label_audit.columns:
        label_audit[lab] = 0

# A second audit: do we actually have HOURS values for those labels?
value_from_audit = (
    cobra_fact.groupby(["SOURCE", "COSTSET_NORM"], as_index=False)
    .agg(hours_nonnull=("HOURS", lambda s: int(s.notna().sum())), hours_sum=("HOURS", "sum"))
    .pivot_table(index="SOURCE", columns="COSTSET_NORM", values="hours_nonnull", fill_value=0)
    .reset_index()
)
for lab in NEEDED_LABELS:
    if lab not in value_from_audit.columns:
        value_from_audit[lab] = 0

# -----------------------------
# PROGRAM METRICS (by SOURCE)
# -----------------------------
program_rows = []
subteam_rows = []

if not cobra_fact.empty:
    # snapshot date = latest DATE in each file (as-of snapshot)
    snapshot_by_source = cobra_fact.groupby("SOURCE")["DATE"].max().reset_index().rename(columns={"DATE": "SNAPSHOT_DATE"})

    for _, r in snapshot_by_source.iterrows():
        src = r["SOURCE"]
        snap = r["SNAPSHOT_DATE"]
        src_df = cobra_fact[cobra_fact["SOURCE"] == src].copy()

        # program (whole file)
        m = _compute_one_scope(src_df, snap)
        m["SOURCE"] = src
        program_rows.append(m)

        # by subteam
        for st, st_df in src_df.groupby("SUB_TEAM"):
            mm = _compute_one_scope(st_df, snap)
            mm["SOURCE"] = src
            mm["SUB_TEAM"] = st
            subteam_rows.append(mm)

program_metrics = pd.DataFrame(program_rows)
subteam_metrics = pd.DataFrame(subteam_rows)

# -----------------------------
# MISSING SUMMARY (what’s still “missing” and why)
# -----------------------------
def _pct_missing(s: pd.Series) -> float:
    if len(s) == 0:
        return np.nan
    return float(s.isna().mean())

if not subteam_metrics.empty:
    missing_summary = (
        subteam_metrics.groupby("SOURCE", as_index=False)
        .agg(
            subteams=("SUB_TEAM", "nunique"),
            pct_BCWS_CTD_missing=("BCWS_CTD", _pct_missing),
            pct_BCWP_CTD_missing=("BCWP_CTD", _pct_missing),
            pct_ACWP_CTD_missing=("ACWP_CTD", _pct_missing),
            pct_BAC_missing=("BAC", _pct_missing),
            pct_EAC_missing=("EAC", _pct_missing),
        )
        .sort_values(["pct_BCWS_CTD_missing","pct_ACWP_CTD_missing","pct_BAC_missing"], ascending=False)
    )
else:
    missing_summary = pd.DataFrame()

# -----------------------------
# IMPORTANT: WHY YOU WERE SEEING “MISSING” BEFORE
# -----------------------------
# If these are high, it means the file either:
#   (a) doesn’t have that COST-SET label at all, or
#   (b) has the label but HOURS is blank/zero, or
#   (c) DATE parsing failed (so nothing falls into CTD/LSD windows)
#
# These audits show which one it is.
#
# Outputs kept in memory:
#   cobra_fact, file_log, coverage, label_audit, value_from_audit,
#   program_metrics, subteam_metrics, missing_summary

print("Loaded files:")
display(file_log)

print("\nLabel audit (counts per SOURCE x COSTSET_NORM):")
display(label_audit[["SOURCE"] + [c for c in NEEDED_LABELS if c in label_audit.columns]])

print("\nValue audit (non-null HOURS per SOURCE x COSTSET_NORM):")
display(value_from_audit[["SOURCE"] + [c for c in NEEDED_LABELS if c in value_from_audit.columns]])

print("\nProgram metrics (preview):")
display(program_metrics.head(10))

print("\nSubteam metrics (preview):")
display(subteam_metrics.head(20))

print("\nMissing summary (by source):")
display(missing_summary)