import pandas as pd
import numpy as np
import re
from pathlib import Path

# =============================================================================
# CONFIG
# =============================================================================
DATA_DIR = Path("data")
SELECT_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
    "Cobra-XM30.xlsx",
    # "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
]
PREFERRED_SHEET_HINTS = ["CAP", "EXTRACT", "WEEKLY", "TBL", "REPORT"]

# =============================================================================
# HELPERS
# =============================================================================
def _norm_col(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(s).upper().strip())

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    return df

def pick_sheet(xls: pd.ExcelFile) -> str:
    def hint_score(name: str) -> int:
        u = name.upper()
        return sum(1 for h in PREFERRED_SHEET_HINTS if h in u)

    ordered = sorted(xls.sheet_names, key=lambda s: hint_score(s), reverse=True)

    # Pick first sheet that has DATE + HOURS and some COSTSET-like column
    for s in ordered:
        try:
            tmp = pd.read_excel(xls, sheet_name=s, nrows=25)
            tmp = normalize_columns(tmp)
            if "DATE" in tmp.columns and "HOURS" in tmp.columns:
                # COSTSET might be COSTSET, COSTSETX, COSTSET_ etc after normalization
                for c in tmp.columns:
                    if c == "COSTSET" or "COSTSET" in c:
                        return s
                # allow if DATE/HOURS exists; we'll fail loudly later if no COSTSET
                return s
        except Exception:
            pass
    return xls.sheet_names[0]

def find_costset_col(cols_norm):
    if "COSTSET" in cols_norm:
        return "COSTSET"
    # handle COSTSET variants
    for c in cols_norm:
        if "COSTSET" in c:
            return c
    return None

def coerce_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce")

def coerce_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def map_metric_from_costset(costset: str) -> str:
    if costset is None or (isinstance(costset, float) and np.isnan(costset)):
        return "OTHER"
    s = str(costset).upper().strip()
    s_clean = re.sub(r"[^A-Z0-9]+", " ", s).strip()

    if re.search(r"\bBCWS\b", s_clean) or "BUDGET" in s_clean:
        return "BCWS"
    if re.search(r"\bBCWP\b", s_clean) or "PROGRESS" in s_clean or "EARNED" in s_clean:
        return "BCWP"
    if re.search(r"\bACWP\b", s_clean) or "ACWPHRS" in s_clean or "ACWP HRS" in s_clean:
        return "ACWP"
    if re.search(r"\bETC\b", s_clean):
        return "ETC"
    if re.search(r"\bBAC\b", s_clean):
        return "BAC"
    if re.search(r"\bEAC\b", s_clean):
        return "EAC"
    return "OTHER"

def safe_div(a, b):
    if b is None or pd.isna(b) or b == 0:
        return np.nan
    return a / b

def ctd_sum(df, metric, up_to):
    if pd.isna(up_to):
        return np.nan
    return float(df.loc[(df["METRIC"] == metric) & (df["DATE"] <= up_to), "HOURS"].sum())

def window_sum(df, metric, start_excl, end_incl):
    if pd.isna(end_incl):
        return np.nan
    if pd.isna(start_excl):
        start_excl = pd.Timestamp.min
    return float(df.loc[(df["METRIC"] == metric) & (df["DATE"] > start_excl) & (df["DATE"] <= end_incl), "HOURS"].sum())

def posting_dates(df, metric):
    """Dates where metric daily total > 0 (i.e., the metric actually changes)."""
    d = (df.loc[df["METRIC"] == metric]
           .groupby("DATE")["HOURS"].sum()
           .sort_index())
    return d[d > 0].index.to_list()

def choose_closes_by_posting(df):
    """
    Choose close dates based on posting cadence.
    Priority: ACWP then BCWP then BCWS.
    If only 1 posting date exists for chosen metric, prev=curr (LSD becomes 0/NaN by design).
    """
    acwp_dates = posting_dates(df, "ACWP")
    bcwp_dates = posting_dates(df, "BCWP")
    bcws_dates = posting_dates(df, "BCWS")

    # Prefer metrics that represent status posting
    if len(acwp_dates) >= 2:
        base = "ACWP"; dates = acwp_dates
    elif len(bcwp_dates) >= 2:
        base = "BCWP"; dates = bcwp_dates
    elif len(acwp_dates) == 1:
        base = "ACWP"; dates = acwp_dates
    elif len(bcwp_dates) == 1:
        base = "BCWP"; dates = bcwp_dates
    else:
        base = "BCWS"; dates = bcws_dates

    if len(dates) == 0:
        return (pd.NaT, pd.NaT, base)

    curr = dates[-1]
    prev = dates[-2] if len(dates) >= 2 else dates[-1]
    return (curr, prev, base)

# =============================================================================
# 1) LOAD FILES → cobra_fact
# =============================================================================
print("\n================= 1) LOADING FILES =================")
frames = []
log = []

for fname in SELECT_FILES:
    fpath = DATA_DIR / fname
    if not fpath.exists():
        log.append({"file": fname, "status": "MISSING FILE", "sheet": None, "rows": 0, "notes": str(fpath)})
        continue

    try:
        xls = pd.ExcelFile(fpath)
        sheet = pick_sheet(xls)
        raw = pd.read_excel(xls, sheet_name=sheet)
        raw = normalize_columns(raw)

        cost_col = find_costset_col(list(raw.columns))
        if cost_col is None:
            log.append({"file": fname, "status": "MISSING COSTSET", "sheet": sheet, "rows": len(raw), "notes": f"cols={list(raw.columns)[:20]}"})
            continue
        if "DATE" not in raw.columns or "HOURS" not in raw.columns:
            log.append({"file": fname, "status": "MISSING DATE/HOURS", "sheet": sheet, "rows": len(raw), "notes": f"cols={list(raw.columns)[:20]}"})
            continue

        sub_col = None
        if "SUBTEAM" in raw.columns: sub_col = "SUBTEAM"
        if "SUB_TEAM" in raw.columns: sub_col = "SUB_TEAM"

        df = pd.DataFrame({
            "SOURCE": fname,
            "SOURCE_SHEET": sheet,
            "DATE": coerce_date(raw["DATE"]),
            "COSTSET_RAW": raw[cost_col].astype(str),
            "HOURS": coerce_num(raw["HOURS"]),
            "SUB_TEAM": raw[sub_col] if sub_col else np.nan
        })

        df["COSTSET_RAW"] = df["COSTSET_RAW"].replace({"nan": np.nan, "None": np.nan})
        df = df.dropna(subset=["DATE"])
        df["HOURS"] = df["HOURS"].fillna(0.0)
        df["METRIC"] = df["COSTSET_RAW"].map(map_metric_from_costset)

        frames.append(df)
        log.append({"file": fname, "status": "OK", "sheet": sheet, "rows": len(df), "notes": f"cost_col={cost_col}, sub_col={sub_col}"})

    except Exception as e:
        log.append({"file": fname, "status": "ERROR", "sheet": None, "rows": 0, "notes": repr(e)})

log_df = pd.DataFrame(log)
print("\nLoad log:")
print(log_df.to_string(index=False))

if not frames:
    raise RuntimeError("❌ No valid files loaded.")

cobra_fact = pd.concat(frames, ignore_index=True)
print(f"\nLoaded rows: {len(cobra_fact):,}")
print("Sources:", cobra_fact["SOURCE"].unique())
print("Date range:", cobra_fact["DATE"].min(), "→", cobra_fact["DATE"].max())

# =============================================================================
# 2) COVERAGE / UNMAPPED COSTSETS
# =============================================================================
print("\n================= 2) COVERAGE =================")
cov = (cobra_fact.groupby(["SOURCE","METRIC"])
       .agg(rows=("HOURS","size"), sum_hours=("HOURS","sum"), min_date=("DATE","min"), max_date=("DATE","max"))
       .reset_index()
       .sort_values(["SOURCE","METRIC"]))
print("\nMetric coverage:")
print(cov.to_string(index=False))

pct_other = (cobra_fact.assign(is_other=cobra_fact["METRIC"].eq("OTHER"))
             .groupby("SOURCE")["is_other"].mean()
             .reset_index(name="pct_OTHER_rows"))
print("\nPct OTHER rows:")
print(pct_other.to_string(index=False))

top_other = (cobra_fact.loc[cobra_fact["METRIC"].eq("OTHER")]
             .groupby(["SOURCE","COSTSET_RAW"]).size().reset_index(name="count")
             .sort_values(["SOURCE","count"], ascending=[True, False])
             .groupby("SOURCE").head(15))
print("\nTop OTHER costsets (if these should map, we add rules):")
print("(none)" if len(top_other)==0 else top_other.to_string(index=False))

# =============================================================================
# 3) PROGRAM METRICS — CLOSE DATES FROM POSTING CADENCE
# =============================================================================
print("\n================= 3) PROGRAM METRICS =================")
program_rows = []
snap_rows = []

for src, sdf in cobra_fact.groupby("SOURCE"):
    sdf = sdf.copy()

    curr_close, prev_close, base_metric = choose_closes_by_posting(sdf)

    # snapshot_date is just "what we're treating as current"
    snapshot_date = curr_close

    snap_rows.append({
        "SOURCE": src,
        "BASE_METRIC_FOR_CLOSE": base_metric,
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,
        "ACWP_posting_dates": len(posting_dates(sdf, "ACWP")),
        "BCWP_posting_dates": len(posting_dates(sdf, "BCWP")),
        "BCWS_posting_dates": len(posting_dates(sdf, "BCWS")),
    })

    bcws_ctd = ctd_sum(sdf, "BCWS", curr_close)
    bcwp_ctd = ctd_sum(sdf, "BCWP", curr_close)
    acwp_ctd = ctd_sum(sdf, "ACWP", curr_close)

    bcws_prev = ctd_sum(sdf, "BCWS", prev_close)
    bcwp_prev = ctd_sum(sdf, "BCWP", prev_close)
    acwp_prev = ctd_sum(sdf, "ACWP", prev_close)

    bcws_lsd = bcws_ctd - bcws_prev
    bcwp_lsd = bcwp_ctd - bcwp_prev
    acwp_lsd = acwp_ctd - acwp_prev

    spi_ctd = safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = safe_div(bcwp_ctd, acwp_ctd)
    spi_lsd = safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = safe_div(bcwp_lsd, acwp_lsd)

    bac = ctd_sum(sdf, "BAC", curr_close)
    eac = ctd_sum(sdf, "EAC", curr_close)
    vac = bac - eac if (not pd.isna(bac) and not pd.isna(eac)) else np.nan

    etc_total = ctd_sum(sdf, "ETC", curr_close)

    # Next month ETC = (curr_close, month-end(curr_close)+1]
    next_month_end = pd.Timestamp(curr_close) + pd.offsets.MonthEnd(1) if not pd.isna(curr_close) else pd.NaT
    etc_next_month = window_sum(sdf, "ETC", curr_close, next_month_end) if not pd.isna(curr_close) else np.nan

    program_rows.append({
        "SOURCE": src,
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,
        "CLOSE_BASE_METRIC": base_metric,

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
        "ETC_NEXT_MONTH": etc_next_month
    })

snapshots_df = pd.DataFrame(snap_rows)
program_metrics = pd.DataFrame(program_rows)

print("\nClose-date selection diagnostics (THIS is the key):")
print(snapshots_df.to_string(index=False))

print("\nProgram metrics:")
print(program_metrics.to_string(index=False))

# =============================================================================
# 4) SUBTEAM METRICS — USE SAME PROGRAM CLOSE DATES
# =============================================================================
print("\n================= 4) SUBTEAM METRICS =================")
sub_rows = []

for (src, sub), sdf in cobra_fact.groupby(["SOURCE","SUB_TEAM"], dropna=False):
    if pd.isna(sub):
        continue
    meta = snapshots_df.loc[snapshots_df["SOURCE"] == src].iloc[0]
    curr_close = meta["CURR_CLOSE"]
    prev_close = meta["PREV_CLOSE"]

    bcws_ctd = ctd_sum(sdf, "BCWS", curr_close)
    bcwp_ctd = ctd_sum(sdf, "BCWP", curr_close)
    acwp_ctd = ctd_sum(sdf, "ACWP", curr_close)

    bcws_prev = ctd_sum(sdf, "BCWS", prev_close)
    bcwp_prev = ctd_sum(sdf, "BCWP", prev_close)
    acwp_prev = ctd_sum(sdf, "ACWP", prev_close)

    bcws_lsd = bcws_ctd - bcws_prev
    bcwp_lsd = bcwp_ctd - bcwp_prev
    acwp_lsd = acwp_ctd - acwp_prev

    spi_ctd = safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = safe_div(bcwp_ctd, acwp_ctd)
    spi_lsd = safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = safe_div(bcwp_lsd, acwp_lsd)

    bac = ctd_sum(sdf, "BAC", curr_close)
    eac = ctd_sum(sdf, "EAC", curr_close)

    sub_rows.append({
        "SOURCE": src,
        "SUB_TEAM": sub,
        "SNAPSHOT_DATE": meta["SNAPSHOT_DATE"],
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
# 5) WHY LSD IS ZERO? PRINT PER-SOURCE POSTING DATES + LAST CHANGE DAYS
# =============================================================================
print("\n================= 5) LSD ZERO DEBUG =================")
for src, sdf in cobra_fact.groupby("SOURCE"):
    meta = snapshots_df.loc[snapshots_df["SOURCE"] == src].iloc[0]
    print("\n" + "-"*90)
    print(f"SOURCE: {src}")
    print(f"Close base metric: {meta['BASE_METRIC_FOR_CLOSE']}")
    print(f"prev_close={meta['PREV_CLOSE']}  curr_close={meta['CURR_CLOSE']}")
    for m in ["ACWP","BCWP","BCWS","ETC"]:
        dates = posting_dates(sdf, m)
        print(f"{m} posting dates (count={len(dates)}): last 5 -> {dates[-5:] if len(dates)>0 else dates}")

    # show whether the metric actually changes between closes
    for m in ["BCWS","BCWP","ACWP"]:
        c_prev = ctd_sum(sdf, m, meta["PREV_CLOSE"])
        c_curr = ctd_sum(sdf, m, meta["CURR_CLOSE"])
        print(f"{m} CTD_prev={c_prev:.4f}  CTD_curr={c_curr:.4f}  LSD(delta)={c_curr-c_prev:.4f}")

print("\n================= DONE =================")
print("Objects in memory: cobra_fact, program_metrics, subteam_metrics, snapshots_df, cov")
