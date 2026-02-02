import pandas as pd
import numpy as np
import re
from pathlib import Path

# =============================================================================
# CONFIG — EDIT ONLY THIS SECTION
# =============================================================================
DATA_DIR = Path("data")  # change if needed

SELECT_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
    "Cobra-XM30.xlsx",
    # "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",  # include if it has the right columns
]

# If you KNOW the sheet names, you can hardcode here. Otherwise we auto-pick.
PREFERRED_SHEET_HINTS = [
    "CAP", "EXTRACT", "WEEKLY", "TBL", "REPORT"
]

# =============================================================================
# HELPERS
# =============================================================================
def _norm_col(s: str) -> str:
    """Normalize a column name: uppercase + remove all non-alphanumerics."""
    return re.sub(r"[^A-Z0-9]", "", str(s).upper().strip())

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    return df

def pick_sheet(xls: pd.ExcelFile) -> str:
    """Pick the best sheet based on hints + presence of required columns."""
    candidates = xls.sheet_names

    # Score by hint match first
    def hint_score(name: str) -> int:
        u = name.upper()
        score = 0
        for h in PREFERRED_SHEET_HINTS:
            if h in u:
                score += 1
        return score

    # Try in descending hint score; validate required columns
    ordered = sorted(candidates, key=lambda s: hint_score(s), reverse=True)

    for s in ordered:
        try:
            tmp = pd.read_excel(xls, sheet_name=s, nrows=10)
            tmp = normalize_columns(tmp)
            req = {"DATE", "HOURS"}
            # COSTSET can be COSTSET or COSTSET-like after normalization
            has_costset = "COSTSET" in tmp.columns or "COSTSET" in [_norm_col(c) for c in tmp.columns]
            if req.issubset(set(tmp.columns)) and ("COSTSET" in tmp.columns):
                return s
            # Some exports might call COSTSET something else; we’ll handle via scan below,
            # but we still want a sheet that at least has DATE+HOURS and a costset-like column
            if req.issubset(set(tmp.columns)):
                # allow for later rename scan
                return s
        except Exception:
            pass

    # fallback
    return candidates[0]

def find_costset_col(cols_norm):
    """
    After normalization, we expect COSTSET.
    But if something weird happens, detect the closest variant.
    """
    if "COSTSET" in cols_norm:
        return "COSTSET"
    # Try common variants (already normalized)
    for c in cols_norm:
        if c.endswith("COSTSET") or c.startswith("COSTSET"):
            return c
    return None

def coerce_date(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")

def coerce_num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def map_metric_from_costset(costset: str) -> str:
    """
    Map COSTSET text to a metric bucket.
    ONLY use COSTSET (unit columns ignored).
    """
    if costset is None or (isinstance(costset, float) and np.isnan(costset)):
        return "OTHER"
    s = str(costset).upper().strip()

    # Normalize separators for matching
    s_clean = re.sub(r"[^A-Z0-9]+", " ", s).strip()

    # Strong matches first
    if re.search(r"\bBCWS\b", s_clean) or "BUDGET" in s_clean:
        return "BCWS"
    if re.search(r"\bBCWP\b", s_clean) or "PROGRESS" in s_clean or "EARNED" in s_clean:
        return "BCWP"
    if re.search(r"\bACWP\b", s_clean):
        return "ACWP"
    if re.search(r"\bETC\b", s_clean):
        return "ETC"
    if re.search(r"\bBAC\b", s_clean):
        return "BAC"
    if re.search(r"\bEAC\b", s_clean):
        return "EAC"

    return "OTHER"

def choose_snapshot_date(df: pd.DataFrame) -> pd.Timestamp:
    """
    Use max DATE as snapshot_date (export date proxy).
    If DATE is missing, returns NaT.
    """
    mx = df["DATE"].max()
    return mx

def close_pair_for_snapshot(snapshot_date: pd.Timestamp, all_dates: pd.Series):
    """
    Choose current close = latest date <= snapshot_date
    prev close = previous distinct date < current close
    """
    closes = pd.Series(pd.to_datetime(all_dates.dropna().unique()))
    closes = closes.sort_values()

    closes = closes[closes <= snapshot_date]
    if len(closes) == 0:
        return (pd.NaT, pd.NaT)

    curr = closes.iloc[-1]
    prev = closes.iloc[-2] if len(closes) >= 2 else closes.iloc[-1]
    return (curr, prev)

def sum_hours_up_to(df, metric, up_to_date):
    """CTD sum: sum HOURS where METRIC == metric and DATE <= up_to_date"""
    if pd.isna(up_to_date):
        return np.nan
    m = df.loc[(df["METRIC"] == metric) & (df["DATE"] <= up_to_date), "HOURS"].sum()
    return float(m)

def sum_hours_in_window(df, metric, start_date_exclusive, end_date_inclusive):
    """Window sum for ETC next-month etc."""
    if pd.isna(end_date_inclusive):
        return np.nan
    if pd.isna(start_date_exclusive):
        start_date_exclusive = pd.Timestamp.min
    m = df.loc[
        (df["METRIC"] == metric) &
        (df["DATE"] > start_date_exclusive) &
        (df["DATE"] <= end_date_inclusive),
        "HOURS"
    ].sum()
    return float(m)

def safe_div(a, b):
    if b in [0, 0.0] or pd.isna(b):
        return np.nan
    return a / b

# =============================================================================
# 1) LOAD SELECTED FILES → LONG FACT TABLE (SOURCE, SUB_TEAM, DATE, COSTSET, HOURS, METRIC)
# =============================================================================
print("\n================= 1) LOADING SELECTED FILES =================")
frames = []
load_log = []

for fname in SELECT_FILES:
    fpath = DATA_DIR / fname
    if not fpath.exists():
        load_log.append({"file": fname, "status": "MISSING FILE", "sheet": None, "rows": 0, "notes": str(fpath)})
        continue

    try:
        xls = pd.ExcelFile(fpath)
        sheet = pick_sheet(xls)

        raw = pd.read_excel(xls, sheet_name=sheet)
        raw = normalize_columns(raw)

        # Find COSTSET col
        cost_col = find_costset_col(raw.columns.tolist())
        if cost_col is None:
            load_log.append({"file": fname, "status": "MISSING COSTSET", "sheet": sheet, "rows": len(raw), "notes": f"cols={list(raw.columns)[:15]}..."})
            continue

        # Required cols
        if "DATE" not in raw.columns or "HOURS" not in raw.columns:
            load_log.append({"file": fname, "status": "MISSING DATE/HOURS", "sheet": sheet, "rows": len(raw), "notes": f"cols={list(raw.columns)[:15]}..."})
            continue

        # SUB_TEAM optional
        sub_col = "SUBTEAM" if "SUBTEAM" in raw.columns else ("SUB_TEAM" if "SUB_TEAM" in raw.columns else None)
        if sub_col is None and "SUBTEAM" in raw.columns:
            sub_col = "SUBTEAM"

        df = pd.DataFrame({
            "SOURCE": fname,
            "SOURCE_SHEET": sheet,
            "DATE": coerce_date(raw["DATE"]),
            "COSTSET_RAW": raw[cost_col].astype(str),
            "HOURS": coerce_num(raw["HOURS"]),
            "SUB_TEAM": raw[sub_col] if sub_col is not None else np.nan
        })

        df["COSTSET_RAW"] = df["COSTSET_RAW"].replace({"nan": np.nan, "None": np.nan})
        df = df.dropna(subset=["DATE"])  # must have date
        df["HOURS"] = df["HOURS"].fillna(0.0)  # missing hours treated as 0
        df["METRIC"] = df["COSTSET_RAW"].map(map_metric_from_costset)

        frames.append(df)
        load_log.append({"file": fname, "status": "OK", "sheet": sheet, "rows": len(df), "notes": f"cost_col={cost_col}, sub_col={sub_col}"})

    except Exception as e:
        load_log.append({"file": fname, "status": "ERROR", "sheet": None, "rows": 0, "notes": repr(e)})

load_log_df = pd.DataFrame(load_log)
print("\nLoad log:")
print(load_log_df.to_string(index=False))

if not frames:
    raise RuntimeError("❌ No valid files loaded. Check load log above.")

cobra_fact = pd.concat(frames, ignore_index=True)
print(f"\nLoaded cobra_fact rows: {len(cobra_fact):,}")
print("Sources loaded:", cobra_fact["SOURCE"].nunique())
print("Date range:", cobra_fact["DATE"].min(), "→", cobra_fact["DATE"].max())

# =============================================================================
# 2) COVERAGE AUDITS (what METRICs exist? how much OTHER?)
# =============================================================================
print("\n================= 2) COVERAGE AUDITS =================")

cov = (cobra_fact
       .groupby(["SOURCE", "METRIC"])
       .agg(rows=("HOURS", "size"),
            sum_hours=("HOURS", "sum"),
            min_date=("DATE", "min"),
            max_date=("DATE", "max"))
       .reset_index()
       .sort_values(["SOURCE", "METRIC"]))

print("\nMetric coverage by source (rows/sum/min/max):")
print(cov.to_string(index=False))

pct_other = (cobra_fact.assign(IS_OTHER=cobra_fact["METRIC"].eq("OTHER"))
             .groupby("SOURCE")["IS_OTHER"]
             .mean()
             .reset_index(name="pct_OTHER_rows"))
print("\nPct OTHER rows by source (should be near 0; if high, mapping rules need update):")
print(pct_other.to_string(index=False))

top_other = (cobra_fact.loc[cobra_fact["METRIC"].eq("OTHER")]
             .groupby(["SOURCE", "COSTSET_RAW"])
             .size()
             .reset_index(name="count")
             .sort_values(["SOURCE", "count"], ascending=[True, False])
             .groupby("SOURCE")
             .head(10))
print("\nTop unmapped COSTSET values (per source) — if these should be BCWS/BCWP/ACWP/ETC/BAC/EAC, tell me and we’ll add rules:")
if len(top_other) == 0:
    print("(none)")
else:
    print(top_other.to_string(index=False))

# =============================================================================
# 3) PROGRAM METRICS (CTD + LSD as delta-of-cumulative totals)
# =============================================================================
print("\n================= 3) PROGRAM METRICS (CTD + LSD deltas) =================")

program_rows = []
snapshots = []

for src, sdf in cobra_fact.groupby("SOURCE"):
    sdf = sdf.copy()
    snapshot_date = choose_snapshot_date(sdf)
    curr_close, prev_close = close_pair_for_snapshot(snapshot_date, sdf["DATE"])
    next_close = sdf["DATE"].dropna().max()

    snapshots.append({
        "SOURCE": src,
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,
        "NEXT_CLOSE": next_close
    })

    # CTD (cumulative through curr close)
    bcws_ctd = sum_hours_up_to(sdf, "BCWS", curr_close)
    bcwp_ctd = sum_hours_up_to(sdf, "BCWP", curr_close)
    acwp_ctd = sum_hours_up_to(sdf, "ACWP", curr_close)
    etc_total = sum_hours_up_to(sdf, "ETC", curr_close)

    # LSD using DELTA OF CUMULATIVE (correct for weekly exports)
    bcws_prev = sum_hours_up_to(sdf, "BCWS", prev_close)
    bcwp_prev = sum_hours_up_to(sdf, "BCWP", prev_close)
    acwp_prev = sum_hours_up_to(sdf, "ACWP", prev_close)

    bcws_lsd = bcws_ctd - bcws_prev
    bcwp_lsd = bcwp_ctd - bcwp_prev
    acwp_lsd = acwp_ctd - acwp_prev

    # BAC/EAC/VAC — if present as costsets
    bac = sum_hours_up_to(sdf, "BAC", curr_close)
    eac = sum_hours_up_to(sdf, "EAC", curr_close)
    vac = bac - eac if (not pd.isna(bac) and not pd.isna(eac)) else np.nan

    # SPI/CPI
    spi_ctd = safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = safe_div(bcwp_ctd, acwp_ctd)

    spi_lsd = safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = safe_div(bcwp_lsd, acwp_lsd)

    # ETC next month — based on DATE window (curr_close, curr_close + 1 month]
    next_month_end = (pd.Timestamp(curr_close) + pd.offsets.MonthEnd(1))
    etc_next_mo = sum_hours_in_window(sdf, "ETC", curr_close, next_month_end)

    program_rows.append({
        "SOURCE": src,
        "SNAPSHOT_DATE": snapshot_date,
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
        "VAC": vac,

        "ETC_TOTAL": etc_total,
        "ETC_NEXT_MONTH": etc_next_mo
    })

snapshots_df = pd.DataFrame(snapshots)
program_metrics = pd.DataFrame(program_rows)

print("\nSnapshot/close dates per source:")
print(snapshots_df.to_string(index=False))

print("\nProgram metrics preview:")
print(program_metrics.to_string(index=False))

# =============================================================================
# 4) SUBTEAM METRICS (same logic, but grouped by SUB_TEAM)
# =============================================================================
print("\n================= 4) SUBTEAM METRICS =================")

sub_rows = []
for (src, sub), sdf in cobra_fact.groupby(["SOURCE", "SUB_TEAM"], dropna=False):
    if pd.isna(sub):
        continue

    snapshot_date = snapshots_df.loc[snapshots_df["SOURCE"] == src, "SNAPSHOT_DATE"].iloc[0]
    curr_close = snapshots_df.loc[snapshots_df["SOURCE"] == src, "CURR_CLOSE"].iloc[0]
    prev_close = snapshots_df.loc[snapshots_df["SOURCE"] == src, "PREV_CLOSE"].iloc[0]

    bcws_ctd = sum_hours_up_to(sdf, "BCWS", curr_close)
    bcwp_ctd = sum_hours_up_to(sdf, "BCWP", curr_close)
    acwp_ctd = sum_hours_up_to(sdf, "ACWP", curr_close)

    bcws_prev = sum_hours_up_to(sdf, "BCWS", prev_close)
    bcwp_prev = sum_hours_up_to(sdf, "BCWP", prev_close)
    acwp_prev = sum_hours_up_to(sdf, "ACWP", prev_close)

    bcws_lsd = bcws_ctd - bcws_prev
    bcwp_lsd = bcwp_ctd - bcwp_prev
    acwp_lsd = acwp_ctd - acwp_prev

    spi_ctd = safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = safe_div(bcwp_ctd, acwp_ctd)

    spi_lsd = safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = safe_div(bcwp_lsd, acwp_lsd)

    bac = sum_hours_up_to(sdf, "BAC", curr_close)
    eac = sum_hours_up_to(sdf, "EAC", curr_close)

    sub_rows.append({
        "SOURCE": src,
        "SUB_TEAM": sub,
        "SNAPSHOT_DATE": snapshot_date,
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
        "EAC": eac
    })

subteam_metrics = pd.DataFrame(sub_rows)
print("\nSubteam metrics (first 25 rows):")
print(subteam_metrics.head(25).to_string(index=False))

# =============================================================================
# 5) ZERO/NULL DIAGNOSTICS — WHY ARE VALUES ZERO?
# =============================================================================
print("\n================= 5) ZERO DIAGNOSTICS =================")

def diag_source(src):
    sdf = cobra_fact[cobra_fact["SOURCE"] == src].copy()
    curr_close = snapshots_df.loc[snapshots_df["SOURCE"] == src, "CURR_CLOSE"].iloc[0]
    prev_close = snapshots_df.loc[snapshots_df["SOURCE"] == src, "PREV_CLOSE"].iloc[0]

    print("\n" + "-"*80)
    print(f"SOURCE: {src}")
    print(f"prev_close={prev_close}  curr_close={curr_close}")
    print("Metric totals (counts/sums):")
    tot = sdf.groupby("METRIC").agg(count=("HOURS","size"), sum=("HOURS","sum"), min_date=("DATE","min"), max_date=("DATE","max")).reset_index()
    print(tot.to_string(index=False))

    # For each metric, show if CTD changes between prev and curr
    for metric in ["BCWS","BCWP","ACWP","ETC","BAC","EAC"]:
        ctd_curr = sum_hours_up_to(sdf, metric, curr_close)
        ctd_prev = sum_hours_up_to(sdf, metric, prev_close)
        delta = ctd_curr - ctd_prev
        print(f"\n{metric}: CTD_prev={ctd_prev:.4f}  CTD_curr={ctd_curr:.4f}  DELTA(LSD)={delta:.4f}")

        # Show if any rows exist on/near the closes
        near = sdf[(sdf["METRIC"]==metric) & (sdf["DATE"]>=prev_close - pd.Timedelta(days=45)) & (sdf["DATE"]<=curr_close + pd.Timedelta(days=5))]
        if len(near)==0:
            print(f"  -> No rows within ~45d of close dates for {metric}")
        else:
            print(f"  -> Rows near closes for {metric}: {len(near)}  (date min={near['DATE'].min()}, max={near['DATE'].max()})")
            print(near.sort_values("DATE")[["DATE","COSTSET_RAW","HOURS"]].tail(8).to_string(index=False))

# Run diag for each loaded source
for src in cobra_fact["SOURCE"].unique():
    diag_source(src)

print("\n================= DONE =================")
print("Objects created in memory:")
print(" - cobra_fact (long fact)")
print(" - program_metrics (program overview)")
print(" - subteam_metrics (subteam overview)")
print(" - cov (coverage table)")
print(" - snapshots_df (close dates)")