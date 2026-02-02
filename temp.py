# EVMS COBRA pipeline (COST-SET driven, selected files only, NO Excel output)
# - Robustly finds COSTSET even if the column is named "COST-SET" (or similar)
# - Uses ONLY: DATE + COSTSET + HOURS (+ SUB_TEAM when present)
# - Computes CTD + LSD metrics from COSTSET mappings (no currency/unit logic)
# - Close dates are driven by ACTUAL/PROGRESS (ACWP/BCWP) dates (NOT BCWS future dates)
# - BEI is EXCLUDED (requires Open Plan activity file; not loaded here)

import re
import numpy as np
import pandas as pd
from pathlib import Path

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 200)

# =========================
# 0) CONFIG
# =========================
DATA_DIR = Path("data")

SELECTED_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
    "Cobra-XM30.xlsx",
    # Add/replace as needed:
    # "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
]

# Preference order for sheets to read (we’ll pick the first matching)
SHEET_KEYWORDS = [
    "tbl_weekly", "weekly", "cap_extract", "cap extract", "extract", "report"
]

# =========================
# 1) HELPERS: column detection & normalization
# =========================
def _norm_colname(c: str) -> str:
    """
    Normalize column names so that e.g. 'COST-SET' -> 'COST_SET'
    and 'Sub Team' -> 'SUB_TEAM'
    """
    c = str(c).strip().upper()
    c = re.sub(r"[^A-Z0-9]+", "_", c)   # non-alnum -> underscore
    c = re.sub(r"_+", "_", c).strip("_")
    return c

def _find_col(df: pd.DataFrame, candidates):
    """
    Return first matching column name in df (after normalization), else None.
    candidates: list of normalized col names to match exactly.
    """
    norm_map = {_norm_colname(c): c for c in df.columns}
    for cand in candidates:
        if cand in norm_map:
            return norm_map[cand]
    return None

def _pick_sheet(xls: pd.ExcelFile) -> str:
    sheets = list(xls.sheet_names)
    norm_sheets = {s: _norm_colname(s) for s in sheets}
    for kw in SHEET_KEYWORDS:
        nkw = _norm_colname(kw)
        for s, ns in norm_sheets.items():
            if nkw in ns:
                return s
    return sheets[0]  # fallback

def _coerce_date(s: pd.Series) -> pd.Series:
    dt = pd.to_datetime(s, errors="coerce")
    return dt

def _coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

# =========================
# 2) COSTSET mapping (what actually matters)
# =========================
def _norm_costset(v) -> str:
    if pd.isna(v):
        return np.nan
    s = str(v).strip().upper()
    s = re.sub(r"\s+", "_", s)
    s = s.replace("-", "_")
    return s

# Map common COBRA cost-set labels to EVMS metrics
# (Tweak here if your exports use different naming)
COSTSET_MAP = {
    # Planned
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    # Earned
    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    # Actual
    "ACWP": "ACWP",
    "ACWP_HRS": "ACWP",
    "ACWP_HOURS": "ACWP",
    "ACTUAL": "ACWP",
    "ACTUALS": "ACWP",

    # Forecast / remaining
    "ETC": "ETC",
    "ESTIMATE_TO_COMPLETE": "ETC",

    # At-completion (may or may not exist in your extracts)
    "BAC": "BAC",
    "EAC": "EAC",
}

def map_costset(raw_costset: pd.Series) -> pd.Series:
    c = raw_costset.map(_norm_costset)
    return c.map(lambda x: COSTSET_MAP.get(x, "OTHER") if pd.notna(x) else np.nan)

# =========================
# 3) Determine whether a metric series looks cumulative or incremental
# =========================
def is_cumulative(series_by_date: pd.Series) -> bool:
    """
    series_by_date: index=DATE sorted, values=aggregate HOURS for that metric by DATE
    Heuristic:
      - If mostly non-decreasing, treat as cumulative
      - else treat as incremental
    """
    if series_by_date is None or len(series_by_date) < 3:
        return True  # default
    v = series_by_date.values
    diffs = np.diff(v)
    nondec_ratio = np.mean(diffs >= -1e-9)  # allow tiny numeric wiggle
    return nondec_ratio >= 0.80

def value_at_or_before(series_by_date: pd.Series, dt: pd.Timestamp) -> float:
    if series_by_date is None or len(series_by_date) == 0 or pd.isna(dt):
        return np.nan
    # series_by_date index must be sorted datetime
    eligible = series_by_date.loc[series_by_date.index <= dt]
    if len(eligible) == 0:
        return np.nan
    return float(eligible.iloc[-1])

def sum_through(series_by_date: pd.Series, dt: pd.Timestamp) -> float:
    if series_by_date is None or len(series_by_date) == 0 or pd.isna(dt):
        return np.nan
    eligible = series_by_date.loc[series_by_date.index <= dt]
    if len(eligible) == 0:
        return 0.0
    return float(eligible.sum())

def sum_in_window(series_by_date: pd.Series, prev_dt: pd.Timestamp, curr_dt: pd.Timestamp) -> float:
    if series_by_date is None or len(series_by_date) == 0 or pd.isna(curr_dt) or pd.isna(prev_dt):
        return np.nan
    eligible = series_by_date.loc[(series_by_date.index > prev_dt) & (series_by_date.index <= curr_dt)]
    if len(eligible) == 0:
        return 0.0
    return float(eligible.sum())

# =========================
# 4) Load selected files into a long "fact" table
# =========================
print("="*90)
print("1) LOADING SELECTED FILES")
print("="*90)

frames = []
load_log = []

for fname in SELECTED_FILES:
    fpath = DATA_DIR / fname
    if not fpath.exists():
        load_log.append({"file": fname, "status": "MISSING FILE", "sheet": None, "rows": 0, "notes": str(fpath)})
        continue

    try:
        xls = pd.ExcelFile(fpath)
        sheet = _pick_sheet(xls)
        df = pd.read_excel(fpath, sheet_name=sheet, engine="openpyxl")
        orig_cols = list(df.columns)

        # Detect key columns robustly
        date_col   = _find_col(df, ["DATE", "AS_OF_DATE", "PERIOD", "PERIOD_END", "STATUS_DATE"])
        hours_col  = _find_col(df, ["HOURS", "HRS", "VALUE", "AMOUNT"])
        cost_col   = _find_col(df, ["COSTSET", "COST_SET", "COST_SET_", "COST", "COST_SET_NAME", "COST_SET_TYPE", "COSTSET_NAME", "COST_SET_ID", "COST_SET_CODE", "COST_SET_DESC", "COST_SET_DESCRIPTION",
                                    "COSTSETTYPE", "COST_SET_TYPE_", "COST_SET_TYPE__", "COST_SET_TYPE___",
                                    "COST_SET", "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    "COST_SET", "COST_SET",
                                    # IMPORTANT: catch "COST-SET" after normalization:
                                    "COST_SET"  # normalization covers COST-SET -> COST_SET
                                   ])

        # Fallback: if cost_col still None, do a normalized search for COST_SET specifically
        if cost_col is None:
            norm_map = {_norm_colname(c): c for c in df.columns}
            if "COST_SET" in norm_map:
                cost_col = norm_map["COST_SET"]

        subteam_col = _find_col(df, ["SUB_TEAM", "SUBTEAM", "ORG", "CAM", "WBS", "CONTROL_ACCOUNT", "RESP_DEPT", "BE_DEPT"])

        missing = [k for k, v in {"DATE": date_col, "HOURS": hours_col, "COSTSET": cost_col}.items() if v is None]
        if missing:
            load_log.append({"file": fname, "status": "MISSING REQUIRED COLS", "sheet": sheet, "rows": 0, "notes": f"Missing: {missing}. Columns: {orig_cols}"})
            print(f"\n❌ {fname} | sheet={sheet} | Missing required cols: {missing}")
            print(f"   Columns seen (normalized -> original):")
            for c in orig_cols[:80]:
                print(f"     {_norm_colname(c):<25} -> {c}")
            continue

        out = pd.DataFrame({
            "SOURCE": fname,
            "SOURCE_SHEET": sheet,
            "DATE": _coerce_date(df[date_col]),
            "COSTSET_RAW": df[cost_col],
            "HOURS": _coerce_num(df[hours_col]),
        })

        if subteam_col is not None:
            out["SUB_TEAM"] = df[subteam_col].astype(str).str.strip()
        else:
            out["SUB_TEAM"] = "ALL"

        out["COSTSET_NORM"] = map_costset(out["COSTSET_RAW"])

        # Drop bad rows
        out = out.dropna(subset=["DATE", "COSTSET_NORM", "HOURS"])
        out = out[out["COSTSET_NORM"].isin(["BCWS","BCWP","ACWP","ETC","BAC","EAC","OTHER"])]
        frames.append(out)

        load_log.append({"file": fname, "status": "OK", "sheet": sheet, "rows": len(out), "notes": ""})

        print(f"\n✅ {fname} | sheet={sheet} | rows={len(out):,}")
        print("   Costset raw sample:", list(pd.Series(out["COSTSET_RAW"].unique()).head(12)))
        print("   Costset mapped counts:\n", out["COSTSET_NORM"].value_counts().head(10))

    except Exception as e:
        load_log.append({"file": fname, "status": "ERROR", "sheet": None, "rows": 0, "notes": repr(e)})
        print(f"\n❌ {fname} | ERROR: {repr(e)}")

load_log_df = pd.DataFrame(load_log)
print("\n--- LOAD LOG ---")
print(load_log_df)

if not frames:
    raise RuntimeError("No valid files loaded. Fix required columns or filenames, then rerun.")

cobra_fact = pd.concat(frames, ignore_index=True)
print("\nLoaded cobra_fact rows:", f"{len(cobra_fact):,}")
print("Sources loaded:", cobra_fact["SOURCE"].nunique(), list(cobra_fact["SOURCE"].unique()))
print("Date range:", cobra_fact["DATE"].min(), "to", cobra_fact["DATE"].max())

# =========================
# 5) Coverage audit (by source + metric)
# =========================
print("\n" + "="*90)
print("2) COVERAGE AUDIT (source x metric)")
print("="*90)

cov = (
    cobra_fact.groupby(["SOURCE", "COSTSET_NORM"])
    .agg(
        rows=("HOURS", "size"),
        sum_hours=("HOURS", "sum"),
        min_date=("DATE", "min"),
        max_date=("DATE", "max"),
        n_subteams=("SUB_TEAM", "nunique"),
    )
    .reset_index()
    .sort_values(["SOURCE", "COSTSET_NORM"])
)
print(cov)

# =========================
# 6) Build date-level series per (source, subteam, metric)
# =========================
# This is the core simplification: everything becomes a time series in HOURS by COSTSET.
grp = (
    cobra_fact
    .groupby(["SOURCE", "SUB_TEAM", "COSTSET_NORM", "DATE"], as_index=False)["HOURS"]
    .sum()
)

# Helper to get a series for a slice
def get_series(source: str, sub_team: str, metric: str) -> pd.Series:
    sdf = grp[(grp["SOURCE"] == source) & (grp["SUB_TEAM"] == sub_team) & (grp["COSTSET_NORM"] == metric)].copy()
    if sdf.empty:
        return pd.Series(dtype=float)
    sdf = sdf.sort_values("DATE")
    s = pd.Series(sdf["HOURS"].values, index=pd.to_datetime(sdf["DATE"]))
    s = s.sort_index()
    return s

# =========================
# 7) Close date logic (FIXES your zero problem)
# =========================
def pick_close_dates_for_source(source: str) -> dict:
    """
    Close dates should be driven by ACTUAL/PROGRESS dates, not planned (BCWS) future dates.
    We pick:
      curr_close = max date among ACWP or BCWP in the file (program-level)
      prev_close = second max distinct date among ACWP or BCWP
    """
    sdf = cobra_fact[cobra_fact["SOURCE"] == source]
    actual_prog = sdf[sdf["COSTSET_NORM"].isin(["ACWP","BCWP"])]["DATE"].dropna().drop_duplicates().sort_values()
    if len(actual_prog) == 0:
        # fallback: use any dates
        any_dates = sdf["DATE"].dropna().drop_duplicates().sort_values()
        curr = any_dates.max() if len(any_dates) else pd.NaT
        prev = any_dates.iloc[-2] if len(any_dates) >= 2 else curr
        return {"snapshot_date": curr, "curr_close": curr, "prev_close": prev}
    curr = actual_prog.max()
    prev = actual_prog.iloc[-2] if len(actual_prog) >= 2 else curr
    return {"snapshot_date": curr, "curr_close": curr, "prev_close": prev}

snapshots = []
for src in cobra_fact["SOURCE"].unique():
    d = pick_close_dates_for_source(src)
    snapshots.append({"SOURCE": src, **d})

snapshots_df = pd.DataFrame(snapshots)
print("\n" + "="*90)
print("3) SNAPSHOT / CLOSE DATES (driven by ACWP/BCWP)")
print("="*90)
print(snapshots_df)

# =========================
# 8) Compute program + subteam metrics
# =========================
def compute_metrics_for_slice(source: str, sub_team: str, closes: dict, verbose=False) -> dict:
    snap = closes["snapshot_date"]
    curr_close = closes["curr_close"]
    prev_close = closes["prev_close"]

    # Get series
    s_bcws = get_series(source, sub_team, "BCWS")
    s_bcwp = get_series(source, sub_team, "BCWP")
    s_acwp = get_series(source, sub_team, "ACWP")
    s_etc  = get_series(source, sub_team, "ETC")
    s_bac  = get_series(source, sub_team, "BAC")
    s_eac  = get_series(source, sub_team, "EAC")

    # Decide cum vs incr (ETC is treated as incremental/time-phased by default)
    bcws_cum = is_cumulative(s_bcws) if len(s_bcws) else True
    bcwp_cum = is_cumulative(s_bcwp) if len(s_bcwp) else True
    acwp_cum = is_cumulative(s_acwp) if len(s_acwp) else True

    etc_cum = False  # force incremental interpretation for ETC time-phasing

    # CTD values
    def ctd(s, close, cum_flag):
        return value_at_or_before(s, close) if cum_flag else sum_through(s, close)

    bcws_ctd = ctd(s_bcws, curr_close, bcws_cum)
    bcwp_ctd = ctd(s_bcwp, curr_close, bcwp_cum)
    acwp_ctd = ctd(s_acwp, curr_close, acwp_cum)

    # LSD (delta between closes)
    def lsd(s, prev_dt, curr_dt, cum_flag):
        if cum_flag:
            v_curr = value_at_or_before(s, curr_dt)
            v_prev = value_at_or_before(s, prev_dt)
            if pd.isna(v_curr) or pd.isna(v_prev):
                return np.nan
            return float(v_curr - v_prev)
        else:
            return sum_in_window(s, prev_dt, curr_dt)

    bcws_lsd = lsd(s_bcws, prev_close, curr_close, bcws_cum)
    bcwp_lsd = lsd(s_bcwp, prev_close, curr_close, bcwp_cum)
    acwp_lsd = lsd(s_acwp, prev_close, curr_close, acwp_cum)

    # BAC / EAC
    bac = np.nan
    if len(s_bac):
        bac = float(value_at_or_before(s_bac, s_bac.index.max()))
    elif len(s_bcws):
        # If no explicit BAC costset, approximate BAC as last planned cumulative value
        bac = float(value_at_or_before(s_bcws, s_bcws.index.max())) if bcws_cum else float(s_bcws.sum())

    # ETC remaining (sum of ETC after close)
    etc_total = np.nan
    next_mo_etc = np.nan
    if len(s_etc):
        etc_total = float(s_etc.loc[s_etc.index > curr_close].sum()) if not etc_cum else float(max(0.0, value_at_or_before(s_etc, s_etc.index.max()) - value_at_or_before(s_etc, curr_close)))

        # Next calendar month bucket
        next_month_start = (pd.Timestamp(curr_close) + pd.offsets.MonthBegin(1)).normalize()
        next_month_end   = (pd.Timestamp(curr_close) + pd.offsets.MonthEnd(1)).normalize()
        next_mo_etc = float(s_etc.loc[(s_etc.index >= next_month_start) & (s_etc.index <= next_month_end)].sum())

    # If explicit EAC present use it, else compute EAC ≈ ACWP_CTD + ETC_TOTAL (when ETC exists)
    eac = np.nan
    if len(s_eac):
        eac = float(value_at_or_before(s_eac, s_eac.index.max()))
    elif pd.notna(acwp_ctd) and pd.notna(etc_total):
        eac = float(acwp_ctd + etc_total)

    # SPI/CPI
    spi_ctd = bcwp_ctd / bcws_ctd if (pd.notna(bcwp_ctd) and pd.notna(bcws_ctd) and bcws_ctd != 0) else np.nan
    cpi_ctd = bcwp_ctd / acwp_ctd if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan

    spi_lsd = bcwp_lsd / bcws_lsd if (pd.notna(bcwp_lsd) and pd.notna(bcws_lsd) and bcws_lsd != 0) else np.nan
    cpi_lsd = bcwp_lsd / acwp_lsd if (pd.notna(bcwp_lsd) and pd.notna(acwp_lsd) and acwp_lsd != 0) else np.nan

    # Demand/Actual/PctVar for the period (LSD)
    demand_hours = bcws_lsd
    actual_hours = acwp_lsd
    pct_var = (actual_hours - demand_hours) / demand_hours if (pd.notna(actual_hours) and pd.notna(demand_hours) and demand_hours != 0) else np.nan

    # Next month planned BCWS (simple delta to next month end) + next month ETC
    next_mo_bcws = np.nan
    if len(s_bcws):
        next_month_end = (pd.Timestamp(curr_close) + pd.offsets.MonthEnd(1)).normalize()
        if bcws_cum:
            v_next = value_at_or_before(s_bcws, next_month_end)
            v_curr = value_at_or_before(s_bcws, curr_close)
            next_mo_bcws = (v_next - v_curr) if (pd.notna(v_next) and pd.notna(v_curr)) else np.nan
        else:
            # incremental: sum next month dates
            next_month_start = (pd.Timestamp(curr_close) + pd.offsets.MonthBegin(1)).normalize()
            next_mo_bcws = float(s_bcws.loc[(s_bcws.index >= next_month_start) & (s_bcws.index <= next_month_end)].sum())

    if verbose:
        print("\n--- DEBUG SLICE ---")
        print("SOURCE:", source, "| SUB_TEAM:", sub_team)
        print("prev_close:", prev_close, "| curr_close:", curr_close)
        print("cum flags -> BCWS:", bcws_cum, "BCWP:", bcwp_cum, "ACWP:", acwp_cum, "ETC(incr forced)")
        print("CTD -> BCWS:", bcws_ctd, "BCWP:", bcwp_ctd, "ACWP:", acwp_ctd)
        print("LSD -> BCWS:", bcws_lsd, "BCWP:", bcwp_lsd, "ACWP:", acwp_lsd)
        print("ETC_TOTAL:", etc_total, "Next_Mo_ETC:", next_mo_etc, "Next_Mo_BCWS:", next_mo_bcws)

    return {
        "SOURCE": source,
        "SUB_TEAM": sub_team,
        "SNAPSHOT_DATE": snap,
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
        "EAC": eac,
        "ETC_TOTAL": etc_total,
        "Next_Mo_BCWS_Hours": next_mo_bcws,
        "Next_Mo_ETC_Hours": next_mo_etc,

        "Demand_Hours": demand_hours,
        "Actual_Hours": actual_hours,
        "Pct_Var": pct_var,
    }

print("\n" + "="*90)
print("4) COMPUTE PROGRAM METRICS (program-level = SUB_TEAM='ALL')")
print("="*90)

program_rows = []
for src in cobra_fact["SOURCE"].unique():
    closes = snapshots_df[snapshots_df["SOURCE"] == src].iloc[0].to_dict()
    program_rows.append(compute_metrics_for_slice(src, "ALL", closes, verbose=True))

program_metrics = pd.DataFrame(program_rows)

print("\nPROGRAM METRICS (preview):")
print(program_metrics)

print("\n" + "="*90)
print("5) COMPUTE SUBTEAM METRICS")
print("="*90)

subteam_rows = []
for src in cobra_fact["SOURCE"].unique():
    closes = snapshots_df[snapshots_df["SOURCE"] == src].iloc[0].to_dict()
    subteams = sorted(cobra_fact.loc[cobra_fact["SOURCE"] == src, "SUB_TEAM"].dropna().unique().tolist())
    # If a file doesn't really have subteam structure, you'll just get ["ALL"] here.
    for st in subteams:
        subteam_rows.append(compute_metrics_for_slice(src, st, closes, verbose=False))

subteam_metrics = pd.DataFrame(subteam_rows)

print("\nSUBTEAM METRICS (first 25 rows):")
print(subteam_metrics.head(25))

# =========================
# 9) Missing/NaN diagnostics (WHY you’re seeing “missing data”)
# =========================
print("\n" + "="*90)
print("6) MISSING/NaN DIAGNOSTICS")
print("="*90)

def pct_nan(df, col):
    return float(df[col].isna().mean()) if col in df.columns else np.nan

diag_cols = [
    "BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD",
    "SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD","BAC","EAC","ETC_TOTAL",
    "Demand_Hours","Actual_Hours","Pct_Var","Next_Mo_BCWS_Hours","Next_Mo_ETC_Hours"
]

ms = []
for src in program_metrics["SOURCE"].unique():
    sdf = program_metrics[program_metrics["SOURCE"] == src]
    row = {"SOURCE": src}
    for c in diag_cols:
        row[f"pct_nan_{c}"] = pct_nan(sdf, c)
    ms.append(row)

missing_summary = pd.DataFrame(ms)
print("Program-level %NaN by source:")
print(missing_summary)

print("\nIMPORTANT INTERPRETATION:")
print("- If SPI/CPI are NaN, it almost always means a denominator is missing/0 at close (BCWS_CTD or ACWP_CTD).")
print("- If LSD values are NaN, it means we could not get BOTH close-point values (prev and curr) for that metric.")
print("- This version FIXES the most common cause of bogus zeros: using BCWS future max-date as curr_close.")
print("- BEI is intentionally excluded (you said Open Plan activity file is required).")

# =========================
# 10) Objects produced (for you to inspect in Data Wrangler)
# =========================
print("\n" + "="*90)
print("DONE")
print("="*90)
print("Objects in memory:")
print(" - cobra_fact (long fact table)")
print(" - cov (coverage audit)")
print(" - snapshots_df (close dates per source)")
print(" - program_metrics (overview metrics per source)")
print(" - subteam_metrics (metrics per source+subteam)")