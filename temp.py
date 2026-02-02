# ============================
# EVMS COBRA PIPELINE (LOCAL PROJECT ROOT)
# - Uses local VS Code project folder (no full path required)
# - Auto-detects project root by finding ./data and/or ./EVMS_Output
# - Loads Cobra extracts, normalizes COSTSET, computes program + subteam metrics
# ============================

import os
from pathlib import Path
import pandas as pd
import numpy as np

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 200)

# ---------------------------
# 0) PROJECT ROOT AUTO-DETECT
# ---------------------------
def find_project_root(start: Path, max_up: int = 6) -> Path:
    start = start.resolve()
    candidates = [start] + list(start.parents)[:max_up]
    for p in candidates:
        if (p / "data").exists() or (p / "EVMS_Output").exists():
            return p
    return start

CWD = Path.cwd()
PROJECT_ROOT = find_project_root(CWD)

DATA_DIR = PROJECT_ROOT / "data"
OUT_DIR = PROJECT_ROOT / "EVMS_Output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

print("\n" + "="*90)
print("PROJECT CONTEXT")
print("="*90)
print(f"cwd         : {CWD}")
print(f"project_root: {PROJECT_ROOT}")
print(f"data_dir    : {DATA_DIR} (exists={DATA_DIR.exists()})")
print(f"out_dir     : {OUT_DIR} (exists={OUT_DIR.exists()})")

# ---------------------------
# 1) FILE DISCOVERY (LOCAL)
# ---------------------------
# Keyword picks (adjust if needed)
PICK_KEYWORDS = {
    "Abrams":  ["abrams", "sts"],
    "Bulgaria":["bulgaria", "stryker"],
    "XM30":    ["xm30"],
}

# preferred sheet names
SHEET_PREFER = ["CAP_Extract", "tbl_Weekly Extract", "Weekly Extract", "CAP Extract", "Report"]

REQ_CORE = {"DATE", "HOURS"}  # COSTSET normalized separately

def _norm_col(c: str) -> str:
    return str(c).strip().replace("\n", " ").replace("\t", " ").upper()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_norm_col(c) for c in df.columns]
    # COSTSET variants
    if "COST-SET" in df.columns and "COSTSET" not in df.columns:
        df.rename(columns={"COST-SET":"COSTSET"}, inplace=True)
    if "COST SET" in df.columns and "COSTSET" not in df.columns:
        df.rename(columns={"COST SET":"COSTSET"}, inplace=True)
    if "COST_SET" in df.columns and "COSTSET" not in df.columns:
        df.rename(columns={"COST_SET":"COSTSET"}, inplace=True)
    return df

def safe_to_datetime(s):
    return pd.to_datetime(s, errors="coerce")

def find_best_sheet(xls: pd.ExcelFile):
    # Try preferred sheets first
    for pref in SHEET_PREFER:
        if pref in xls.sheet_names:
            df0 = normalize_columns(pd.read_excel(xls, sheet_name=pref, nrows=5))
            cols = set(df0.columns)
            if ("DATE" in cols) and ("HOURS" in cols) and ("COSTSET" in cols):
                return pref, "preferred sheet matched"

    # Else scan all sheets
    best = None
    best_note = None
    for sh in xls.sheet_names:
        try:
            df0 = normalize_columns(pd.read_excel(xls, sheet_name=sh, nrows=5))
            cols = set(df0.columns)

            ok_core = REQ_CORE.issubset(cols)
            ok_cost = ("COSTSET" in cols)

            score = 2 if (ok_core and ok_cost) else (1 if ok_core else 0)
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
    if pd.isna(raw):
        return "OTHER"
    s = str(raw).strip().upper()
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

def list_xlsx_candidates():
    # Prefer ./data, else scan project root (excluding EVMS_Output)
    if DATA_DIR.exists():
        candidates = list(DATA_DIR.rglob("*.xlsx"))
        scope = str(DATA_DIR)
    else:
        candidates = [p for p in PROJECT_ROOT.rglob("*.xlsx") if "EVMS_Output" not in str(p)]
        scope = str(PROJECT_ROOT)
    # remove temp/hidden
    candidates = [p for p in candidates if "~$" not in p.name and ".ipynb_checkpoints" not in str(p)]
    return candidates, scope

candidates, scope = list_xlsx_candidates()

print("\n" + "="*90)
print("1) DISCOVER / SELECT FILES")
print("="*90)
print(f"Search scope: {scope}")
print(f"Found {len(candidates)} .xlsx files")
for p in candidates[:25]:
    print(" ", p.relative_to(PROJECT_ROOT))

def pick_by_keywords(keywords):
    hits = []
    for p in candidates:
        name = p.name.lower()
        if all(k.lower() in name for k in keywords):
            hits.append(p)
    hits = sorted(hits, key=lambda x: (len(x.name), x.name))
    return hits[0] if hits else None

selected = {}
for label, kws in PICK_KEYWORDS.items():
    selected[label] = pick_by_keywords(kws)

FILES = []
print("\nSelected files:")
for label, p in selected.items():
    if p is None:
        print(f" ❌ {label}: NOT FOUND (keywords={PICK_KEYWORDS[label]})")
    else:
        print(f" ✅ {label}: {p.relative_to(PROJECT_ROOT)}")
        FILES.append(p)

# If keyword picking fails, fall back to "use all xlsx in scope"
if not FILES and candidates:
    print("\n⚠️ No keyword matches found. Falling back to ALL .xlsx files in scope.")
    FILES = candidates

if not FILES:
    raise RuntimeError("No .xlsx files found in your project scope. Put your Cobra files under ./data/ and re-run.")

# ---------------------------
# 2) LOAD + STANDARDIZE FACT TABLE
# ---------------------------
print("\n" + "="*90)
print("2) LOADING FILES + AUTO-SHEET SELECTION")
print("="*90)

frames = []
load_log = []

for p in FILES:
    try:
        xls = pd.ExcelFile(p)
    except Exception as e:
        load_log.append({"file": str(p), "status":"FAIL", "sheet":None, "rows":0, "notes":f"Excel open error: {e}"})
        print(f" ❌ {p.name}: cannot open ({e})")
        continue

    sheet, note = find_best_sheet(xls)
    if sheet is None:
        load_log.append({"file": str(p), "status":"FAIL", "sheet":None, "rows":0, "notes":note})
        print(f" ❌ {p.name}: no valid sheet ({note})")
        continue

    df = normalize_columns(pd.read_excel(xls, sheet_name=sheet))

    cols = set(df.columns)
    miss = []
    if "DATE" not in cols: miss.append("DATE")
    if "HOURS" not in cols: miss.append("HOURS")
    if "COSTSET" not in cols: miss.append("COSTSET/COST-SET/COST SET")
    if miss:
        load_log.append({"file": str(p), "status":"FAIL", "sheet":sheet, "rows":0, "notes":f"missing {miss}"})
        print(f" ❌ {p.name}: missing {miss} | sheet={sheet}")
        continue

    if "SUB_TEAM" not in cols:
        df["SUB_TEAM"] = "UNKNOWN"

    out = df[["SUB_TEAM","DATE","COSTSET","HOURS"]].copy()
    out["SOURCE"] = p.name
    out["DATE"] = safe_to_datetime(out["DATE"])
    out["HOURS"] = pd.to_numeric(out["HOURS"], errors="coerce")
    out["COSTSET_RAW"] = out["COSTSET"].astype(str)
    out["COSTSET_NORM"] = out["COSTSET"].apply(map_costset)
    out["SUB_TEAM"] = out["SUB_TEAM"].fillna("UNKNOWN").astype(str)

    out = out.dropna(subset=["DATE","HOURS"])
    frames.append(out[["SOURCE","SUB_TEAM","DATE","COSTSET_RAW","COSTSET_NORM","HOURS"]])

    load_log.append({"file": str(p), "status":"OK", "sheet":sheet, "rows":len(out), "notes":note})

    print(f" ✅ {p.name} | sheet={sheet} | rows={len(out):,}")
    print("    costset raw sample:", list(pd.Series(out["COSTSET_RAW"].unique()).head(8)))
    print("    mapped counts:\n", out["COSTSET_NORM"].value_counts().to_string())

load_log_df = pd.DataFrame(load_log)
print("\n--- LOAD LOG ---")
print(load_log_df.to_string(index=False))

if not frames:
    raise RuntimeError("No valid files loaded. See LOAD LOG above for why.")

cobra_fact = pd.concat(frames, ignore_index=True)
print(f"\nLoaded cobra_fact rows: {len(cobra_fact):,}")
print("Sources:", sorted(cobra_fact["SOURCE"].unique()))
print("Date range:", cobra_fact["DATE"].min(), "to", cobra_fact["DATE"].max())

# ---------------------------
# 3) COVERAGE AUDIT
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

# ---------------------------
# 4) CLOSE DATES (driven by ACWP/BCWP)
# ---------------------------
print("\n" + "="*90)
print("4) SNAPSHOT / CLOSE DATES (DRIVEN BY ACWP/BCWP)")
print("="*90)

def close_dates_for_source(src_df: pd.DataFrame):
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
        any_dates = pd.to_datetime(src_df["DATE"].dropna().unique())
        any_dates = sorted(any_dates)
        curr_close = any_dates[-1] if any_dates else pd.NaT
        prev_close = any_dates[-2] if len(any_dates) >= 2 else pd.NaT

    return curr_close, prev_close

snap_rows = []
for src in cobra_fact["SOURCE"].unique():
    sdf = cobra_fact[cobra_fact["SOURCE"].eq(src)]
    curr_close, prev_close = close_dates_for_source(sdf)
    snap_rows.append({"SOURCE": src, "curr_close": curr_close, "prev_close": prev_close})

snapshots_df = pd.DataFrame(snap_rows).sort_values("SOURCE")
print(snapshots_df.to_string(index=False))

# ---------------------------
# 5) DETECT METRIC BEHAVIOR (cumulative vs incremental)
# ---------------------------
print("\n" + "="*90)
print("5) METRIC BEHAVIOR DETECTION")
print("="*90)

behavior_cache = {}
behavior_rows = []

def detect_behavior(src_df: pd.DataFrame, metric: str):
    mdf = src_df[src_df["COSTSET_NORM"].eq(metric)]
    if mdf.empty:
        return None, None
    series = mdf.groupby("DATE")["HOURS"].sum().sort_index()
    if len(series) <= 2:
        return "cumulative", series
    diffs = series.diff().dropna()
    frac_nonneg = float((diffs >= -1e-6).mean())
    last_is_max = float(series.iloc[-1] >= series.max() - 1e-6)
    if frac_nonneg >= 0.95 and last_is_max >= 0.999:
        return "cumulative", series
    return "incremental", series

for src in cobra_fact["SOURCE"].unique():
    sdf = cobra_fact[cobra_fact["SOURCE"].eq(src)]
    for metric in ["BCWS","BCWP","ACWP","ETC","EAC"]:
        mode, series = detect_behavior(sdf, metric)
        if mode is None:
            continue
        behavior_cache[(src, metric)] = {"mode": mode, "series": series}
        behavior_rows.append({
            "SOURCE": src, "METRIC": metric, "mode": mode,
            "n_dates": len(series),
            "min_date": series.index.min(), "max_date": series.index.max(),
            "last_value": float(series.iloc[-1]), "sum_all": float(series.sum())
        })

behavior_df = pd.DataFrame(behavior_rows).sort_values(["SOURCE","METRIC"])
print(behavior_df.to_string(index=False))

def ctd_at(src: str, metric: str, date: pd.Timestamp):
    if pd.isna(date): return np.nan
    key = (src, metric)
    if key not in behavior_cache: return np.nan
    mode = behavior_cache[key]["mode"]
    series = behavior_cache[key]["series"]
    if mode == "cumulative":
        le = series[series.index <= date]
        return float(le.iloc[-1]) if not le.empty else np.nan
    return float(series[series.index <= date].sum())

def lsd_delta(src: str, metric: str, prev_close: pd.Timestamp, curr_close: pd.Timestamp):
    if pd.isna(prev_close) or pd.isna(curr_close): return np.nan
    a = ctd_at(src, metric, prev_close)
    b = ctd_at(src, metric, curr_close)
    if np.isnan(a) or np.isnan(b): return np.nan
    return float(b - a)

def bac_for_src(src: str):
    key = (src, "BCWS")
    if key not in behavior_cache: return np.nan
    return float(behavior_cache[key]["series"].sum())

# ---------------------------
# 6) PROGRAM METRICS (agg all subteams)
# ---------------------------
print("\n" + "="*90)
print("6) PROGRAM METRICS")
print("="*90)

program_rows = []
for r in snapshots_df.itertuples(index=False):
    src = r.SOURCE
    curr_close = pd.Timestamp(r.curr_close) if not pd.isna(r.curr_close) else pd.NaT
    prev_close = pd.Timestamp(r.prev_close) if not pd.isna(r.prev_close) else pd.NaT

    bcws_ctd = ctd_at(src, "BCWS", curr_close)
    bcwp_ctd = ctd_at(src, "BCWP", curr_close)
    acwp_ctd = ctd_at(src, "ACWP", curr_close)

    bcws_lsd = lsd_delta(src, "BCWS", prev_close, curr_close)
    bcwp_lsd = lsd_delta(src, "BCWP", prev_close, curr_close)
    acwp_lsd = lsd_delta(src, "ACWP", prev_close, curr_close)

    spi_ctd = (bcwp_ctd / bcws_ctd) if (not np.isnan(bcwp_ctd) and not np.isnan(bcws_ctd) and bcws_ctd != 0) else np.nan
    cpi_ctd = (bcwp_ctd / acwp_ctd) if (not np.isnan(bcwp_ctd) and not np.isnan(acwp_ctd) and acwp_ctd != 0) else np.nan
    spi_lsd = (bcwp_lsd / bcws_lsd) if (not np.isnan(bcwp_lsd) and not np.isnan(bcws_lsd) and bcws_lsd != 0) else np.nan
    cpi_lsd = (bcwp_lsd / acwp_lsd) if (not np.isnan(bcwp_lsd) and not np.isnan(acwp_lsd) and acwp_lsd != 0) else np.nan

    program_rows.append({
        "SOURCE": src, "SUB_TEAM": "ALL",
        "SNAPSHOT_DATE": curr_close, "CURR_CLOSE": curr_close, "PREV_CLOSE": prev_close,
        "BCWS_CTD": bcws_ctd, "BCWP_CTD": bcwp_ctd, "ACWP_CTD": acwp_ctd,
        "BCWS_LSD": bcws_lsd, "BCWP_LSD": bcwp_lsd, "ACWP_LSD": acwp_lsd,
        "SPI_CTD": spi_ctd, "CPI_CTD": cpi_ctd, "SPI_LSD": spi_lsd, "CPI_LSD": cpi_lsd,
        "BAC": bac_for_src(src),
        "BEI_LSD": np.nan,  # intentionally excluded (needs Open Plan)
    })

program_metrics = pd.DataFrame(program_rows)
print(program_metrics.to_string(index=False))

# ---------------------------
# 7) SUBTEAM METRICS
# ---------------------------
print("\n" + "="*90)
print("7) SUBTEAM METRICS (first 25)")
print("="*90)

subteam_rows = []
for r in snapshots_df.itertuples(index=False):
    src = r.SOURCE
    curr_close = pd.Timestamp(r.curr_close) if not pd.isna(r.curr_close) else pd.NaT
    prev_close = pd.Timestamp(r.prev_close) if not pd.isna(r.prev_close) else pd.NaT
    sdf_all = cobra_fact[cobra_fact["SOURCE"].eq(src)]

    # behavior mode uses program-level detection, but we compute sums per subteam slice
    def sub_series(sdf, metric):
        mdf = sdf[sdf["COSTSET_NORM"].eq(metric)]
        if mdf.empty:
            return None
        return mdf.groupby("DATE")["HOURS"].sum().sort_index()

    def sub_ctd(sdf, metric, date):
        if pd.isna(date): return np.nan
        key = (src, metric)
        if key not in behavior_cache: return np.nan
        mode = behavior_cache[key]["mode"]
        series = sub_series(sdf, metric)
        if series is None or series.empty: return np.nan
        if mode == "cumulative":
            le = series[series.index <= date]
            return float(le.iloc[-1]) if not le.empty else np.nan
        return float(series[series.index <= date].sum())

    def sub_lsd(sdf, metric, prev_d, curr_d):
        if pd.isna(prev_d) or pd.isna(curr_d): return np.nan
        a = sub_ctd(sdf, metric, prev_d)
        b = sub_ctd(sdf, metric, curr_d)
        if np.isnan(a) or np.isnan(b): return np.nan
        return float(b - a)

    for st, sdf in sdf_all.groupby("SUB_TEAM"):
        bcws_ctd = sub_ctd(sdf, "BCWS", curr_close)
        bcwp_ctd = sub_ctd(sdf, "BCWP", curr_close)
        acwp_ctd = sub_ctd(sdf, "ACWP", curr_close)

        bcws_lsd = sub_lsd(sdf, "BCWS", prev_close, curr_close)
        bcwp_lsd = sub_lsd(sdf, "BCWP", prev_close, curr_close)
        acwp_lsd = sub_lsd(sdf, "ACWP", prev_close, curr_close)

        spi_ctd = (bcwp_ctd / bcws_ctd) if (not np.isnan(bcwp_ctd) and not np.isnan(bcws_ctd) and bcws_ctd != 0) else np.nan
        cpi_ctd = (bcwp_ctd / acwp_ctd) if (not np.isnan(bcwp_ctd) and not np.isnan(acwp_ctd) and acwp_ctd != 0) else np.nan
        spi_lsd = (bcwp_lsd / bcws_lsd) if (not np.isnan(bcwp_lsd) and not np.isnan(bcws_lsd) and bcws_lsd != 0) else np.nan
        cpi_lsd = (bcwp_lsd / acwp_lsd) if (not np.isnan(bcwp_lsd) and not np.isnan(acwp_lsd) and acwp_lsd != 0) else np.nan

        subteam_rows.append({
            "SOURCE": src, "SUB_TEAM": st,
            "SNAPSHOT_DATE": curr_close, "CURR_CLOSE": curr_close, "PREV_CLOSE": prev_close,
            "BCWS_CTD": bcws_ctd, "BCWP_CTD": bcwp_ctd, "ACWP_CTD": acwp_ctd,
            "BCWS_LSD": bcws_lsd, "BCWP_LSD": bcwp_lsd, "ACWP_LSD": acwp_lsd,
            "SPI_CTD": spi_ctd, "CPI_CTD": cpi_ctd, "SPI_LSD": spi_lsd, "CPI_LSD": cpi_lsd,
            "BAC": bac_for_src(src),
            "BEI_LSD": np.nan,
        })

subteam_metrics = pd.DataFrame(subteam_rows)
print(subteam_metrics.head(25).to_string(index=False))

# ---------------------------
# 8) EXPORT (optional but helpful for Power BI)
# ---------------------------
out_path = OUT_DIR / "evms_metrics_output.xlsx"
with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
    cobra_fact.to_excel(writer, sheet_name="cobra_fact", index=False)
    coverage.to_excel(writer, sheet_name="coverage", index=False)
    snapshots_df.to_excel(writer, sheet_name="snapshots", index=False)
    behavior_df.to_excel(writer, sheet_name="behavior", index=False)
    program_metrics.to_excel(writer, sheet_name="program_metrics", index=False)
    subteam_metrics.to_excel(writer, sheet_name="subteam_metrics", index=False)

print("\n" + "="*90)
print("DONE")
print("="*90)
print(f"Wrote: {out_path}")
print("Objects: cobra_fact, coverage, snapshots_df, behavior_df, program_metrics, subteam_metrics")