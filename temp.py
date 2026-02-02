import pandas as pd
import numpy as np
import re
from pathlib import Path

# -----------------------------
# CONFIG
# -----------------------------
DATA_DIR = Path("data")

SELECT_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
    "Cobra-XM30.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
]

SHEET_KEYWORDS = ["tbl", "weekly", "extract", "cap", "evms", "capa", "report"]

# -----------------------------
# COLUMN NORMALIZATION
# -----------------------------
def _clean_col(c: str) -> str:
    c = str(c).strip()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^A-Za-z0-9_]+", "", c)
    return c.upper()

def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [_clean_col(c) for c in out.columns]
    # aliases -> canonical
    rename = {}
    for c in out.columns:
        if c in ["COST_SET", "COST-SET", "COSTSET"]:
            rename[c] = "COSTSET"
        if c in ["SUBTEAM", "SUB_TEAM", "SUBTEAM_NAME"]:
            rename[c] = "SUB_TEAM"
        if c in ["HRS", "HOUR"]:
            rename[c] = "HOURS"
        if c in ["AS_OF_DATE", "STATUS_DATE", "PERIOD"]:
            # only rename if DATE missing
            if "DATE" not in out.columns:
                rename[c] = "DATE"
    if rename:
        out = out.rename(columns=rename)
    return out

def _ensure_required_cols(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    issues = []
    df = _normalize_columns(df)

    if "DATE" not in df.columns:
        issues.append("Missing DATE column")
    else:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    if "COSTSET" not in df.columns:
        issues.append("Missing COSTSET column")

    if "HOURS" not in df.columns:
        # try a couple common alternates
        for cand in ["AMOUNT", "VALUE"]:
            if cand in df.columns:
                df = df.rename(columns={cand: "HOURS"})
                break
    if "HOURS" not in df.columns:
        issues.append("Missing HOURS column")
    else:
        df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    if "SUB_TEAM" not in df.columns:
        df["SUB_TEAM"] = "PROGRAM"

    return df, issues

# -----------------------------
# COSTSET -> METRIC BUCKET (ONLY COSTSET VALUES)
# -----------------------------
def _norm_costset_val(x) -> str:
    s = "" if pd.isna(x) else str(x)
    s = s.strip().upper()
    s = re.sub(r"[\s\-_/]+", " ", s)
    s = re.sub(r"[^A-Z0-9 ]+", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

COSTSET_RULES = [
    ("ACWP", re.compile(r"\bACWP\b|\bACTUAL\b|\bWEEKLY ACTUALS?\b")),
    ("BCWS", re.compile(r"\bBCWS\b|\bBUDGET\b|\bPLANNED\b|\bPLAN\b|\bPV\b")),
    ("BCWP", re.compile(r"\bBCWP\b|\bEARNED\b|\bPROGRESS\b|\bPERFORM\b")),
    ("ETC",  re.compile(r"\bETC\b|\bREMAINING\b|\bTO GO\b|\bESTIMATE TO COMPLETE\b")),
    ("EAC",  re.compile(r"\bEAC\b|\bESTIMATE AT COMPLETE\b")),
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
# SHEET PICKER
# -----------------------------
def _best_sheet(path: Path) -> tuple[str, pd.DataFrame, list[str]]:
    issues = []
    xl = pd.ExcelFile(path)

    best = None
    best_score = -1

    for sh in xl.sheet_names:
        score = 0
        name_l = sh.lower()
        score += sum(1 for k in SHEET_KEYWORDS if k in name_l)
        try:
            hdr = pd.read_excel(path, sheet_name=sh, nrows=0)
            hdr = _normalize_columns(hdr)
            cols = set(hdr.columns)
            score += int("DATE" in cols) + int("COSTSET" in cols) + int("HOURS" in cols)
            score += 1 if "SUB_TEAM" in cols else 0
        except Exception:
            continue

        if score > best_score:
            best = sh
            best_score = score

    if best is None:
        best = xl.sheet_names[0]
        issues.append("Could not score sheets; used first sheet.")

    df = pd.read_excel(path, sheet_name=best)
    df, req_issues = _ensure_required_cols(df)
    issues.extend(req_issues)
    return best, df, issues

# -----------------------------
# CLOSE CALENDAR (INFERRED FROM DATA)
# -----------------------------
def _infer_month_closes(dates: pd.Series) -> pd.DatetimeIndex:
    """
    Per source: for each month in the file, take the max DATE in that month as the close.
    Works across all years and matches the export’s own calendar.
    """
    d = pd.to_datetime(dates, errors="coerce").dropna()
    if d.empty:
        return pd.DatetimeIndex([])
    tmp = pd.DataFrame({"DATE": d})
    tmp["PERIOD"] = tmp["DATE"].dt.to_period("M")
    closes = tmp.groupby("PERIOD")["DATE"].max().sort_values()
    return pd.DatetimeIndex(closes.values)

def _close_triplet(snapshot_date: pd.Timestamp, closes: pd.DatetimeIndex) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    """
    Returns (curr_close, prev_close, next_close) using inferred closes.
    Fallback: month-end boundaries if closes missing.
    """
    if pd.isna(snapshot_date):
        return (pd.NaT, pd.NaT, pd.NaT)

    closes = pd.DatetimeIndex(pd.to_datetime(closes, errors="coerce")).dropna().sort_values()

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
        next_close = (curr_close.to_period("M") + 1).to_timestamp("M")

    return curr_close, prev_close, next_close

# -----------------------------
# CORE SUMS / METRICS
# -----------------------------
def _sum_metric(df: pd.DataFrame, bucket: str, start_exclusive, end_inclusive) -> float:
    x = df[df["METRIC"] == bucket]
    if start_exclusive is not None and not pd.isna(start_exclusive):
        x = x[x["DATE"] > start_exclusive]
    if end_inclusive is not None and not pd.isna(end_inclusive):
        x = x[x["DATE"] <= end_inclusive]
    return float(x["HOURS"].sum(skipna=True))

def _safe_div(num: float, den: float) -> float:
    if den is None or den == 0 or np.isnan(den):
        return np.nan
    return num / den

def _compute_scope_metrics(scope_df: pd.DataFrame, snapshot_date: pd.Timestamp, closes: pd.DatetimeIndex) -> dict:
    """
    Scope = program or (program, subteam).
    Uses only: DATE, METRIC (from COSTSET), HOURS.
    NO BEI computed here (requires OpenPlan activity).
    """
    curr_close, prev_close, next_close = _close_triplet(snapshot_date, closes)

    bcws_ctd = _sum_metric(scope_df, "BCWS", None, curr_close)
    bcwp_ctd = _sum_metric(scope_df, "BCWP", None, curr_close)
    acwp_ctd = _sum_metric(scope_df, "ACWP", None, curr_close)

    bcws_lsd = _sum_metric(scope_df, "BCWS", prev_close, curr_close)
    bcwp_lsd = _sum_metric(scope_df, "BCWP", prev_close, curr_close)
    acwp_lsd = _sum_metric(scope_df, "ACWP", prev_close, curr_close)

    # Next-month window based on inferred close dates
    next_bcws = _sum_metric(scope_df, "BCWS", curr_close, next_close)
    next_etc  = _sum_metric(scope_df, "ETC",  curr_close, next_close)

    # BAC = total baseline across file (robust for "Budget/Planned" costsets)
    bac_total = float(scope_df.loc[scope_df["METRIC"] == "BCWS", "HOURS"].sum(skipna=True))

    # EAC/ETC totals (robust if provided as single line or timephased)
    eac_total = float(scope_df.loc[scope_df["METRIC"] == "EAC", "HOURS"].sum(skipna=True))
    etc_total = float(scope_df.loc[scope_df["METRIC"] == "ETC", "HOURS"].sum(skipna=True))
    if (eac_total == 0 or np.isnan(eac_total)) and (etc_total and etc_total > 0):
        eac_total = acwp_ctd + etc_total

    vac = bac_total - eac_total if (bac_total and eac_total) else np.nan

    spi_ctd = _safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = _safe_div(bcwp_ctd, acwp_ctd)

    spi_lsd = _safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = _safe_div(bcwp_lsd, acwp_lsd)

    # Hours overview (period)
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

        "SPI_LSD": spi_lsd,
        "CPI_LSD": cpi_lsd,

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

for fn in SELECT_FILES:
    p = DATA_DIR / fn
    if not p.exists():
        pipeline_issues.append(f"Missing file: {fn}")
        continue

    sheet_name, df, issues = _best_sheet(p)
    if issues:
        pipeline_issues.extend([f"{fn} | {x}" for x in issues])

    # keep only required + provenance
    df = df[[c for c in ["DATE", "COSTSET", "HOURS", "SUB_TEAM"] if c in df.columns]].copy()
    df["SOURCE"] = p.name
    df["SOURCE_SHEET"] = sheet_name

    df["COSTSET_NORM"] = df["COSTSET"].map(_norm_costset_val)
    df["METRIC"] = _assign_metric_bucket(df["COSTSET"])

    frames.append(df)

cobra_fact = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# strict required trio
if not cobra_fact.empty:
    cobra_fact = cobra_fact.dropna(subset=["DATE", "COSTSET", "HOURS"]).copy()

# -----------------------------
# AUDIT: what is present per source/metric
# -----------------------------
value_from_audit = (cobra_fact
    .groupby(["SOURCE", "METRIC"], as_index=False)
    .agg(
        rows=("HOURS", "size"),
        sum_hours=("HOURS", "sum"),
        min_date=("DATE", "min"),
        max_date=("DATE", "max"),
        unique_costsets=("COSTSET_NORM", lambda s: s.nunique()),
    )
    .sort_values(["SOURCE", "METRIC"])
)

# -----------------------------
# METRICS: program + subteam (NO BEI)
# -----------------------------
program_rows = []
subteam_rows = []
subteam_cost_rows = []
hours_rows = []

if not cobra_fact.empty:
    for src, src_df in cobra_fact.groupby("SOURCE"):
        snapshot = src_df["DATE"].max()
        closes = _infer_month_closes(src_df["DATE"])

        # program
        m = _compute_scope_metrics(src_df, snapshot, closes)
        m["SOURCE"] = src
        program_rows.append(m)

        # subteams
        for st, st_df in src_df.groupby("SUB_TEAM"):
            mm = _compute_scope_metrics(st_df, snapshot, closes)
            mm["SOURCE"] = src
            mm["SUB_TEAM"] = st
            subteam_rows.append(mm)

            subteam_cost_rows.append({
                "SOURCE": src,
                "SUB_TEAM": st,
                "SNAPSHOT_DATE": snapshot,
                "BAC": mm["BAC"],
                "EAC": mm["EAC"],
                "VAC": mm["VAC"],
            })

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
# MISSING SUMMARY (zeros & nans are different!)
# -----------------------------
def _pct_zero(s: pd.Series) -> float:
    if len(s) == 0:
        return np.nan
    return float((s.fillna(0) == 0).mean())

missing_summary = pd.DataFrame()
if not program_metrics.empty:
    missing_summary = (program_metrics
        .groupby("SOURCE", as_index=False)
        .agg(
            pct_BCWS_CTD_zero=("BCWS_CTD", _pct_zero),
            pct_BCWP_CTD_zero=("BCWP_CTD", _pct_zero),
            pct_ACWP_CTD_zero=("ACWP_CTD", _pct_zero),
            pct_NextMo_BCWS_zero=("Next_Mo_BCWS_Hours", _pct_zero),
            pct_NextMo_ETC_zero=("Next_Mo_ETC_Hours", _pct_zero),
        )
        .sort_values("SOURCE")
    )

# -----------------------------
# PREVIEW
# -----------------------------
print("✅ Loaded rows:", len(cobra_fact))
print("✅ Sources:", cobra_fact["SOURCE"].nunique() if not cobra_fact.empty else 0)

print("\n--- Pipeline issues (if any) ---")
for x in pipeline_issues[:50]:
    print(" -", x)
if len(pipeline_issues) > 50:
    print(f" ... ({len(pipeline_issues)-50} more)")

print("\n--- COSTSET-driven coverage (per source/metric) ---")
display(value_from_audit)

print("\n--- PROGRAM METRICS (NO BEI) ---")
display(program_metrics)

print("\n--- HOURS METRICS ---")
display(hours_metrics)

print("\n--- ZERO SUMMARY (by source) ---")
display(missing_summary)

print("\n✅ Outputs in memory:",
      "cobra_fact, program_metrics, subteam_metrics, subteam_cost, hours_metrics, "
      "value_from_audit, missing_summary, pipeline_issues")