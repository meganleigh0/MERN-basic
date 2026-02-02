# ============================
# EVMS COBRA PIPELINE (robust)
# - Auto-find files + sheets
# - COSTSET normalization (COST-SET, COSTSET, etc.)
# - Close dates driven by ACWP/BCWP (NOT BCWS)
# - Program-level aggregates across all subteams (no SUB_TEAM='ALL' dependency)
# - CTD/LSD computed using auto-detected metric behavior (cumulative vs incremental)
# - No BEI (requires Open Plan)
# ============================

import os
from pathlib import Path
import pandas as pd
import numpy as np

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 200)

# ---------------------------
# 0) CONFIG
# ---------------------------
BASE_DIR = r"C:\Users\GRIFFIN12\Desktop\evms powerbi\cobra evms metrics"   # <-- CHANGE IF NEEDED

# Keyword picks (adjust if your filenames differ)
PICK_KEYWORDS = {
    "Abrams":  ["abrams", "sts"],
    "Bulgaria":["bulgaria", "stryker"],
    "XM30":    ["xm30"],
}

# If you want to hardcode exact paths instead, set FILES explicitly and skip discovery
FILES = None  # set to list of full paths to bypass auto-discovery

# Sheet preference (if multiple sheets match, prefer these names)
SHEET_PREFER = ["CAP_Extract", "tbl_Weekly Extract", "Weekly Extract", "Report", "CAP Extract"]

# Required "core" columns (we'll normalize names)
REQ_CORE = {"DATE", "HOURS"}   # COSTSET + SUB_TEAM we handle flexibly


# ---------------------------
# 1) HELPERS: text + cols
# ---------------------------
def _norm_col(c: str) -> str:
    return str(c).strip().replace("\n", " ").replace("\t", " ").upper()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    # Normalize known variants for costset
    # many extracts have "COST-SET"
    if "COST-SET" in df.columns and "COSTSET" not in df.columns:
        df.rename(columns={"COST-SET": "COSTSET"}, inplace=True)
    if "COST SET" in df.columns and "COSTSET" not in df.columns:
        df.rename(columns={"COST SET": "COSTSET"}, inplace=True)
    # Some extracts may use COST_SET or COSTSET_NAME etc.
    if "COST_SET" in df.columns and "COSTSET" not in df.columns:
        df.rename(columns={"COST_SET": "COSTSET"}, inplace=True)
    return df

def find_best_sheet(xls: pd.ExcelFile) -> tuple[str, str]:
    """
    Choose a sheet that contains DATE, HOURS, and some costset-like column.
    Returns: (sheet_name, note)
    """
    best = None
    best_note = None

    # Try preferred names first if they exist
    for pref in SHEET_PREFER:
        if pref in xls.sheet_names:
            df0 = pd.read_excel(xls, sheet_name=pref, nrows=5)
            df0 = normalize_columns(df0)
            cols = set(df0.columns)
            if "DATE" in cols and "HOURS" in cols and ("COSTSET" in cols or "COST-SET" in cols or "COST SET" in cols or "COST_SET" in cols):
                return pref, "preferred sheet matched"

    # Otherwise scan all sheets
    for sh in xls.sheet_names:
        try:
            df0 = pd.read_excel(xls, sheet_name=sh, nrows=5)
            df0 = normalize_columns(df0)
            cols = set(df0.columns)

            ok_core = REQ_CORE.issubset(cols)
            ok_cost = ("COSTSET" in cols)
            if ok_core and ok_cost:
                score = 3
            elif ok_core and ("COST-SET" in cols or "COST SET" in cols or "COST_SET" in cols):
                score = 2
            elif ok_core:
                score = 1
            else:
                score = 0

            if score > 0:
                if best is None or score > best[0]:
                    best = (score, sh)
                    best_note = f"scanned match score={score}"
        except Exception:
            continue

    if best:
        return best[1], best_note
    return None, "no sheet had required columns"


def map_costset(raw: str) -> str:
    """
    Map raw costset to normalized EVMS buckets used in pipeline.
    You can adjust this mapping to match your exports exactly.
    """
    if pd.isna(raw):
        return "OTHER"
    s = str(raw).strip().upper()

    # Common variants seen in your screenshots:
    # - 'ACWP_HRS' (Abrams file)
    # - 'Budget' (BCWS)
    # - 'Progress' (BCWP)
    # - 'BCWS','BCWP','ACWP','ETC' (others)
    if s in ["BUDGET", "BCWS"]:
        return "BCWS"
    if s in ["PROGRESS", "BCWP"]:
        return "BCWP"
    if s in ["ACWP", "ACWP_HRS", "ACWP HRS", "ACTUAL", "ACTUALS"]:
        return "ACWP"
    if s in ["ETC", "E.T.C.", "EST TO COMPLETE", "ESTIMATE TO COMPLETE"]:
        return "ETC"
    if s in ["EAC", "ESTIMATE AT COMPLETE"]:
        return "EAC"

    return "OTHER"


def safe_to_datetime(s):
    return pd.to_datetime(s, errors="coerce")


# ---------------------------
# 2) FILE DISCOVERY
# ---------------------------
print("\n" + "="*90)
print("1) DISCOVER / SELECT FILES")
print("="*90)

if FILES is None:
    base = Path(BASE_DIR)
    if not base.exists():
        raise RuntimeError(f"BASE_DIR does not exist: {base}")

    candidates = list(base.rglob("*.xlsx"))
    print(f"BASE_DIR: {base}")
    print(f"Found {len(candidates)} .xlsx files under BASE_DIR")
    for p in candidates[:30]:
        print(" ", p)

    def pick_by_keywords(keywords):
        hits = []
        for p in candidates:
            name = p.name.lower()
            if all(k.lower() in name for k in keywords):
                hits.append(p)
        # prefer shortest (least suffix noise)
        hits = sorted(hits, key=lambda x: (len(x.name), x.name))
        return hits[0] if hits else None

    selected = {}
    for label, kws in PICK_KEYWORDS.items():
        p = pick_by_keywords(kws)
        selected[label] = p

    print("\nSelected files:")
    FILES = []
    for label, p in selected.items():
        if p is None:
            print(f" ❌ {label}: NOT FOUND (keywords={PICK_KEYWORDS[label]})")
        else:
            print(f" ✅ {label}: {p}")
            FILES.append(str(p))

    if len(FILES) < 1:
        raise RuntimeError("No Cobra files found. Check BASE_DIR and filenames.")
else:
    print("Using hardcoded FILES:")
    for f in FILES:
        print(" ", f)

# Existence check
missing = [f for f in FILES if not Path(f).exists()]
if missing:
    print("\n❌ FILE NOT FOUND:")
    for f in missing:
        print(" ", f)
    raise RuntimeError("Fix file paths or BASE_DIR. Rerun.")


# ---------------------------
# 3) LOAD + STANDARDIZE FACT TABLE
# ---------------------------
print("\n" + "="*90)
print("2) LOADING SELECTED FILES + AUTO-SHEET SELECTION")
print("="*90)

frames = []
load_log = []

for f in FILES:
    fpath = Path(f)
    try:
        xls = pd.ExcelFile(fpath)
    except Exception as e:
        load_log.append({"file": str(fpath), "status": "FAIL", "sheet": None, "rows": 0, "notes": f"ExcelFile error: {e}"})
        print(f" ❌ {fpath.name}: cannot open ({e})")
        continue

    sheet, note = find_best_sheet(xls)
    if sheet is None:
        load_log.append({"file": str(fpath), "status": "FAIL", "sheet": None, "rows": 0, "notes": f"no valid sheet: {note}"})
        print(f" ❌ {fpath.name}: no valid sheet ({note})")
        continue

    try:
        df = pd.read_excel(xls, sheet_name=sheet)
        df = normalize_columns(df)

        # required columns
        cols = set(df.columns)
        missing_cols = []
        if "DATE" not in cols: missing_cols.append("DATE")
        if "HOURS" not in cols: missing_cols.append("HOURS")
        if "COSTSET" not in cols: missing_cols.append("COSTSET (or COST-SET)")
        if missing_cols:
            load_log.append({"file": str(fpath), "status": "FAIL", "sheet": sheet, "rows": 0, "notes": f"missing {missing_cols}, found={sorted(list(cols))[:25]}..."})
            print(f" ❌ {fpath.name}: missing {missing_cols} | sheet={sheet}")
            print("    Columns sample:", list(df.columns)[:25])
            continue

        # SUB_TEAM might not exist; create fallback
        if "SUB_TEAM" not in cols:
            df["SUB_TEAM"] = "UNKNOWN"

        out = df[["SUB_TEAM", "DATE", "COSTSET", "HOURS"]].copy()
        out["SOURCE"] = fpath.name
        out["DATE"] = safe_to_datetime(out["DATE"])
        out["HOURS"] = pd.to_numeric(out["HOURS"], errors="coerce")

        # Map costset -> COSTSET_NORM
        out["COSTSET_RAW"] = out["COSTSET"].astype(str)
        out["COSTSET_NORM"] = out["COSTSET"].apply(map_costset)

        # Minimal cleaning
        out = out.dropna(subset=["DATE", "HOURS"])
        out["SUB_TEAM"] = out["SUB_TEAM"].fillna("UNKNOWN").astype(str)

        frames.append(out[["SOURCE","SUB_TEAM","DATE","COSTSET_RAW","COSTSET_NORM","HOURS"]])
        load_log.append({"file": str(fpath), "status": "OK", "sheet": sheet, "rows": len(out), "notes": note})

        # Print costset sample + mapping counts
        sample_raw = list(pd.Series(out["COSTSET_RAW"].unique()).head(8))
        mapped_counts = out["COSTSET_NORM"].value_counts()
        print(f" ✅ {fpath.name} | sheet={sheet} | rows={len(out):,}")
        print("    Costset raw sample:", sample_raw)
        print("    Costset mapped counts:")
        print(mapped_counts.to_string())

    except Exception as e:
        load_log.append({"file": str(fpath), "status": "FAIL", "sheet": sheet, "rows": 0, "notes": f"read/transform error: {e}"})
        print(f" ❌ {fpath.name}: read/transform error ({e})")
        continue

load_log_df = pd.DataFrame(load_log)
print("\n--- LOAD LOG ---")
print(load_log_df.to_string(index=False))

if not frames:
    raise RuntimeError("No valid files loaded. Check load log for missing columns/sheets.")

cobra_fact = pd.concat(frames, ignore_index=True)

print(f"\nLoaded cobra_fact rows: {len(cobra_fact):,}")
print("Sources loaded:", cobra_fact["SOURCE"].nunique(), sorted(cobra_fact["SOURCE"].unique()))
print("Date range:", cobra_fact["DATE"].min(), "to", cobra_fact["DATE"].max())


# ---------------------------
# 4) COVERAGE AUDIT
# ---------------------------
print("\n" + "="*90)
print("3) COVERAGE AUDIT (source x metric)")
print("="*90)

coverage = (
    cobra_fact
    .groupby(["SOURCE","COSTSET_NORM"])
    .agg(rows=("HOURS","size"),
         sum_hours=("HOURS","sum"),
         min_date=("DATE","min"),
         max_date=("DATE","max"),
         n_subteams=("SUB_TEAM","nunique"))
    .reset_index()
    .sort_values(["SOURCE","COSTSET_NORM"])
)

print(coverage.to_string(index=False))

# Show OTHER rows %
pct_other = (
    cobra_fact.assign(is_other=cobra_fact["COSTSET_NORM"].eq("OTHER"))
    .groupby("SOURCE")["is_other"].mean()
    .reset_index()
    .rename(columns={"is_other":"pct_other_rows"})
)
print("\nPct OTHER rows by source:")
print(pct_other.to_string(index=False))


# ---------------------------
# 5) DETECT CLOSE DATES (drive from ACWP/BCWP dates)
# ---------------------------
print("\n" + "="*90)
print("4) SNAPSHOT / CLOSE DATES (DRIVEN BY ACWP/BCWP, NOT BCWS)")
print("="*90)

def close_dates_for_source(src_df: pd.DataFrame):
    """
    Choose curr_close and prev_close based on ACWP dates if possible, else BCWP dates, else any.
    """
    def uniq_sorted(metric):
        d = src_df.loc[src_df["COSTSET_NORM"].eq(metric), "DATE"].dropna().unique()
        d = pd.to_datetime(pd.Series(d)).sort_values().tolist()
        return d

    acwp_dates = uniq_sorted("ACWP")
    bcwp_dates = uniq_sorted("BCWP")

    base = acwp_dates if len(acwp_dates) >= 1 else bcwp_dates
    if len(base) >= 2:
        curr_close = base[-1]
        prev_close = base[-2]
    elif len(base) == 1:
        curr_close = base[-1]
        prev_close = pd.NaT
    else:
        # fallback: any date
        any_dates = pd.to_datetime(src_df["DATE"].dropna().unique())
        any_dates = sorted(any_dates)
        curr_close = any_dates[-1] if any_dates else pd.NaT
        prev_close = any_dates[-2] if len(any_dates) >= 2 else pd.NaT

    return curr_close, prev_close, len(acwp_dates), len(bcwp_dates)

snap_rows = []
for src in cobra_fact["SOURCE"].unique():
    sdf = cobra_fact[cobra_fact["SOURCE"].eq(src)]
    curr_close, prev_close, n_acwp_dates, n_bcwp_dates = close_dates_for_source(sdf)
    snap_rows.append({
        "SOURCE": src,
        "curr_close": curr_close,
        "prev_close": prev_close,
        "n_unique_ACWP_dates": n_acwp_dates,
        "n_unique_BCWP_dates": n_bcwp_dates
    })

snapshots_df = pd.DataFrame(snap_rows).sort_values("SOURCE")
print(snapshots_df.to_string(index=False))


# ---------------------------
# 6) AUTO-DETECT METRIC BEHAVIOR: cumulative vs incremental
# ---------------------------
print("\n" + "="*90)
print("5) METRIC BEHAVIOR DETECTION (cumulative-by-date vs incremental-by-date)")
print("="*90)

def detect_behavior(src_df: pd.DataFrame, metric: str):
    """
    Determine whether metric behaves like:
      - cumulative-by-date: series mostly nondecreasing; CTD(date) = value at date
      - incremental-by-date: CTD(date) = sum up to date
    """
    mdf = src_df[src_df["COSTSET_NORM"].eq(metric)]
    if mdf.empty:
        return None, None

    series = (
        mdf.groupby("DATE")["HOURS"].sum()
        .sort_index()
    )

    if len(series) <= 2:
        # too little data: treat as cumulative-by-date (value at date)
        return "cumulative", series

    diffs = series.diff().dropna()
    frac_nonneg = float((diffs >= -1e-6).mean())
    last_is_max = float(series.iloc[-1] >= series.max() - 1e-6)

    # Heuristic:
    # if it's very monotonic and last is max -> cumulative
    if frac_nonneg >= 0.95 and last_is_max >= 0.999:
        return "cumulative", series
    return "incremental", series

behavior_rows = []
behavior_cache = {}  # (src, metric) -> dict(mode, series)
for src in cobra_fact["SOURCE"].unique():
    sdf = cobra_fact[cobra_fact["SOURCE"].eq(src)]
    for metric in ["BCWS","BCWP","ACWP","ETC","EAC"]:
        mode, series = detect_behavior(sdf, metric)
        if mode is None:
            continue
        behavior_cache[(src, metric)] = {"mode": mode, "series": series}
        behavior_rows.append({
            "SOURCE": src,
            "METRIC": metric,
            "mode": mode,
            "n_dates": len(series),
            "min_date": series.index.min(),
            "max_date": series.index.max(),
            "last_value": float(series.iloc[-1]),
            "sum_all": float(series.sum())
        })

behavior_df = pd.DataFrame(behavior_rows).sort_values(["SOURCE","METRIC"])
print(behavior_df.to_string(index=False))


# ---------------------------
# 7) METRIC COMPUTATION HELPERS (CTD + LSD)
# ---------------------------
def ctd_at(src: str, metric: str, date: pd.Timestamp):
    """
    CTD at a given close date.
    If metric mode is cumulative: CTD = value at date (or last <= date).
    If incremental: CTD = sum of values <= date.
    """
    if pd.isna(date):
        return np.nan
    key = (src, metric)
    if key not in behavior_cache:
        return np.nan

    mode = behavior_cache[key]["mode"]
    series = behavior_cache[key]["series"]  # date-indexed sums

    # Choose last known point <= date for cumulative
    if mode == "cumulative":
        # If exact date exists, use it; else use last date <= date
        series_le = series[series.index <= date]
        if series_le.empty:
            return np.nan
        return float(series_le.iloc[-1])

    # incremental
    return float(series[series.index <= date].sum())


def lsd_delta(src: str, metric: str, prev_close: pd.Timestamp, curr_close: pd.Timestamp):
    """
    LSD as delta(CTD) between prev and curr.
    Works for both cumulative and incremental behaviors.
    """
    if pd.isna(curr_close) or pd.isna(prev_close):
        return np.nan
    prev_v = ctd_at(src, metric, prev_close)
    curr_v = ctd_at(src, metric, curr_close)
    if np.isnan(prev_v) or np.isnan(curr_v):
        return np.nan
    return float(curr_v - prev_v)


def bac_for_src(src_df: pd.DataFrame, src: str):
    """
    BAC = total BCWS (all dates) for a source (timephased baseline).
    """
    key = (src, "BCWS")
    if key not in behavior_cache:
        return np.nan
    series = behavior_cache[key]["series"]
    return float(series.sum())


def next_month_sum(src_df: pd.DataFrame, src: str, metric: str, curr_close: pd.Timestamp):
    """
    Sum metric in the next calendar month after curr_close.
    """
    if pd.isna(curr_close):
        return np.nan
    mdf = src_df[(src_df["COSTSET_NORM"].eq(metric))].copy()
    if mdf.empty:
        return np.nan

    # next month window [first_day_next_month, first_day_month_after_next)
    first_next = (pd.Timestamp(curr_close.year, curr_close.month, 1) + pd.offsets.MonthBegin(1))
    first_after = first_next + pd.offsets.MonthBegin(1)

    mdf = mdf[(mdf["DATE"] >= first_next) & (mdf["DATE"] < first_after)]
    if mdf.empty:
        return 0.0
    return float(mdf["HOURS"].sum())


# ---------------------------
# 8) COMPUTE PROGRAM METRICS (aggregate across ALL subteams)
# ---------------------------
print("\n" + "="*90)
print("6) COMPUTE PROGRAM METRICS (AGG ALL SUBTEAMS)")
print("="*90)

program_rows = []
for row in snapshots_df.itertuples(index=False):
    src = row.SOURCE
    curr_close = pd.Timestamp(row.curr_close) if not pd.isna(row.curr_close) else pd.NaT
    prev_close = pd.Timestamp(row.prev_close) if not pd.isna(row.prev_close) else pd.NaT

    sdf = cobra_fact[cobra_fact["SOURCE"].eq(src)]

    bcws_ctd = ctd_at(src, "BCWS", curr_close)
    bcwp_ctd = ctd_at(src, "BCWP", curr_close)
    acwp_ctd = ctd_at(src, "ACWP", curr_close)

    bcws_lsd = lsd_delta(src, "BCWS", prev_close, curr_close)
    bcwp_lsd = lsd_delta(src, "BCWP", prev_close, curr_close)
    acwp_lsd = lsd_delta(src, "ACWP", prev_close, curr_close)

    bac = bac_for_src(sdf, src)

    # ETC totals and next month ETC
    etc_total = float(sdf.loc[sdf["COSTSET_NORM"].eq("ETC"), "HOURS"].sum()) if not sdf.loc[sdf["COSTSET_NORM"].eq("ETC")].empty else np.nan
    next_mo_etc = next_month_sum(sdf, src, "ETC", curr_close)
    next_mo_bcws = next_month_sum(sdf, src, "BCWS", curr_close)

    # Indices
    spi_ctd = (bcwp_ctd / bcws_ctd) if (bcws_ctd not in [0, np.nan] and not np.isnan(bcws_ctd) and not np.isnan(bcwp_ctd) and bcws_ctd != 0) else np.nan
    cpi_ctd = (bcwp_ctd / acwp_ctd) if (acwp_ctd not in [0, np.nan] and not np.isnan(acwp_ctd) and not np.isnan(bcwp_ctd) and acwp_ctd != 0) else np.nan

    spi_lsd = (bcwp_lsd / bcws_lsd) if (bcws_lsd not in [0, np.nan] and not np.isnan(bcws_lsd) and not np.isnan(bcwp_lsd) and bcws_lsd != 0) else np.nan
    cpi_lsd = (bcwp_lsd / acwp_lsd) if (acwp_lsd not in [0, np.nan] and not np.isnan(acwp_lsd) and not np.isnan(bcwp_lsd) and acwp_lsd != 0) else np.nan

    program_rows.append({
        "SOURCE": src,
        "SUB_TEAM": "ALL",
        "SNAPSHOT_DATE": curr_close,     # Using curr_close as snapshot date
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
        "Next_Mo_BCWS_Hours": next_mo_bcws,
        "Next_Mo_ETC_Hours": next_mo_etc,

        # Open Plan-driven / external inputs: intentionally excluded
        "Demand_Hours": np.nan,
        "Actual_Hours": np.nan,
        "Pct_Var": np.nan,
        "BEI_LSD": np.nan,   # EXCLUDED (needs Open Plan)
    })

program_metrics = pd.DataFrame(program_rows)

print("\nPROGRAM METRICS PREVIEW:")
print(program_metrics.head(25).to_string(index=False))

# NaN diagnostics at program level
nan_program = program_metrics.isna().mean().sort_values(ascending=False).reset_index()
nan_program.columns = ["col", "pct_nan"]
print("\nProgram-level %NaN by column (top 20):")
print(nan_program.head(20).to_string(index=False))


# ---------------------------
# 9) COMPUTE SUBTEAM METRICS
# ---------------------------
print("\n" + "="*90)
print("7) COMPUTE SUBTEAM METRICS (per source x subteam)")
print("="*90)

subteam_rows = []
for row in snapshots_df.itertuples(index=False):
    src = row.SOURCE
    curr_close = pd.Timestamp(row.curr_close) if not pd.isna(row.curr_close) else pd.NaT
    prev_close = pd.Timestamp(row.prev_close) if not pd.isna(row.prev_close) else pd.NaT

    sdf_all = cobra_fact[cobra_fact["SOURCE"].eq(src)]

    # pre-calc BAC at program level (same for all subteams)
    bac = bac_for_src(sdf_all, src)

    for st, sdf in sdf_all.groupby("SUB_TEAM"):
        # For subteam CTD, we need to recompute behavior series filtered to subteam.
        # We'll compute directly on filtered data with same cumulative/incremental modes learned at program level.

        def series_for(metric):
            mdf = sdf[sdf["COSTSET_NORM"].eq(metric)]
            if mdf.empty:
                return None
            return mdf.groupby("DATE")["HOURS"].sum().sort_index()

        def ctd_at_sub(metric, date):
            if pd.isna(date):
                return np.nan
            key = (src, metric)
            if key not in behavior_cache:
                return np.nan
            mode = behavior_cache[key]["mode"]
            series = series_for(metric)
            if series is None or series.empty:
                return np.nan
            if mode == "cumulative":
                le = series[series.index <= date]
                if le.empty:
                    return np.nan
                return float(le.iloc[-1])
            return float(series[series.index <= date].sum())

        def lsd_sub(metric, prev_d, curr_d):
            if pd.isna(prev_d) or pd.isna(curr_d):
                return np.nan
            a = ctd_at_sub(metric, prev_d)
            b = ctd_at_sub(metric, curr_d)
            if np.isnan(a) or np.isnan(b):
                return np.nan
            return float(b - a)

        bcws_ctd = ctd_at_sub("BCWS", curr_close)
        bcwp_ctd = ctd_at_sub("BCWP", curr_close)
        acwp_ctd = ctd_at_sub("ACWP", curr_close)

        bcws_lsd = lsd_sub("BCWS", prev_close, curr_close)
        bcwp_lsd = lsd_sub("BCWP", prev_close, curr_close)
        acwp_lsd = lsd_sub("ACWP", prev_close, curr_close)

        spi_ctd = (bcwp_ctd / bcws_ctd) if (not np.isnan(bcwp_ctd) and not np.isnan(bcws_ctd) and bcws_ctd != 0) else np.nan
        cpi_ctd = (bcwp_ctd / acwp_ctd) if (not np.isnan(bcwp_ctd) and not np.isnan(acwp_ctd) and acwp_ctd != 0) else np.nan
        spi_lsd = (bcwp_lsd / bcws_lsd) if (not np.isnan(bcwp_lsd) and not np.isnan(bcws_lsd) and bcws_lsd != 0) else np.nan
        cpi_lsd = (bcwp_lsd / acwp_lsd) if (not np.isnan(bcwp_lsd) and not np.isnan(acwp_lsd) and acwp_lsd != 0) else np.nan

        etc_total = float(sdf.loc[sdf["COSTSET_NORM"].eq("ETC"), "HOURS"].sum()) if not sdf.loc[sdf["COSTSET_NORM"].eq("ETC")].empty else np.nan
        next_mo_etc = next_month_sum(sdf, src, "ETC", curr_close)
        next_mo_bcws = next_month_sum(sdf, src, "BCWS", curr_close)

        subteam_rows.append({
            "SOURCE": src,
            "SUB_TEAM": st,
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
            "Next_Mo_BCWS_Hours": next_mo_bcws,
            "Next_Mo_ETC_Hours": next_mo_etc,

            "Demand_Hours": np.nan,
            "Actual_Hours": np.nan,
            "Pct_Var": np.nan,
            "BEI_LSD": np.nan,
        })

subteam_metrics = pd.DataFrame(subteam_rows)

print("\nSUBTEAM METRICS PREVIEW (first 25 rows):")
print(subteam_metrics.head(25).to_string(index=False))

# NaN diagnostics at subteam level
nan_subteam = subteam_metrics.isna().mean().sort_values(ascending=False).reset_index()
nan_subteam.columns = ["col", "pct_nan"]
print("\nSubteam-level %NaN by column (top 20):")
print(nan_subteam.head(20).to_string(index=False))


# ---------------------------
# 10) MISSING/NaN ROOT-CAUSE PRINTS
# ---------------------------
print("\n" + "="*90)
print("8) ROOT-CAUSE DIAGNOSTICS FOR NaNs")
print("="*90)

def explain_nan_row(src):
    sdf = cobra_fact[cobra_fact["SOURCE"].eq(src)]
    curr_close, prev_close, _, _ = close_dates_for_source(sdf)

    print(f"\n--- SOURCE: {src} ---")
    print("curr_close:", curr_close, "prev_close:", prev_close)

    for metric in ["ACWP","BCWP","BCWS","ETC"]:
        key = (src, metric)
        if key not in behavior_cache:
            print(f" {metric}: MISSING ENTIRELY")
            continue
        mode = behavior_cache[key]["mode"]
        series = behavior_cache[key]["series"]
        print(f" {metric}: mode={mode}, n_dates={len(series)}, min={series.index.min()}, max={series.index.max()}, last={series.iloc[-1]:.4f}, sum_all={series.sum():.4f}")

        # show the last few dates and values
        tail = series.tail(5)
        print("   last 5 date sums:")
        for d, v in tail.items():
            print(f"     {pd.Timestamp(d).date()} -> {v:.4f}")

        # CTD at close + prev
        c_curr = ctd_at(src, metric, curr_close)
        c_prev = ctd_at(src, metric, prev_close) if not pd.isna(prev_close) else np.nan
        print(f"   CTD(curr)={c_curr} | CTD(prev)={c_prev} | LSD(delta)={lsd_delta(src, metric, prev_close, curr_close)}")

for src in cobra_fact["SOURCE"].unique():
    explain_nan_row(src)


print("\n" + "="*90)
print("DONE")
print("="*90)
print("Objects in memory:")
print(" - cobra_fact (fact table)")
print(" - coverage (coverage audit)")
print(" - snapshots_df (curr/prev close per source)")
print(" - behavior_df (metric behavior detection)")
print(" - program_metrics (program-level overview)")
print(" - subteam_metrics (subteam-level metrics)")
print("\nNOTES:")
print(" - BEI_LSD is intentionally excluded (needs Open Plan activity file).")
print(" - Demand_Hours/Actual_Hours/Pct_Var are left NaN (not in Cobra exports unless you load those sources).")
