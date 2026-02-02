# EVMS COBRA DIAGNOSTIC (single-cell)
# - Loads ONLY selected files (you control exact filenames)
# - Uses ONLY DATE + COSTSET + HOURS (no currency/unit columns)
# - Normalizes costset -> METRIC (BCWS/BCWP/ACWP/ETC/EAC)
# - Builds cobra_fact + coverage + per-source "why LSD is 0" diagnostics
# - Computes program_metrics with "value-at-close / delta-between-closes" logic
# - Prints lots of detail so you can paste output back and we can retool

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 220)

# =========================
# CONFIG — EDIT THESE
# =========================
DATA_DIR = Path("data")  # <-- your folder from the screenshot

# Put EXACT filenames here (must exist under DATA_DIR)
SELECT_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
    "Cobra-XM30.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
    # add one more Stryker/Abrams if you want:
    # "Cobra-Stryker C4ISR -F0162.xlsx",
]

# Accounting closes (EDIT to your correct year/close dates)
# NOTE: The previous pipeline error was from using `.iloc` on DatetimeIndex.
# We'll store closes as a sorted list of Timestamps and index via [-1],[-2].
ACCOUNTING_CLOSES = [
    "2026-01-26",
    "2026-02-23",
    "2026-03-30",
    "2026-04-27",
    "2026-05-25",
    "2026-06-29",
    "2026-07-27",
    "2026-08-24",
    "2026-09-28",
    "2026-10-26",
    "2026-11-23",
    "2026-12-31",
]
CLOSES = sorted(pd.to_datetime(ACCOUNTING_CLOSES))

# Sheet picking keywords (we'll score sheets by these)
SHEET_KEYWORDS = [
    "weekly", "extract", "cap", "tbl", "evms", "cobra", "cost", "set", "hours"
]

# Required columns (we will try to find synonyms)
REQ_ANY = [
    ["date"],                 # must find a DATE-like column
    ["costset", "cost set"],  # must find a COSTSET-like column
    ["hours"],                # must find HOURS-like column
]

# =========================
# UTILS
# =========================
def _clean_colname(c: str) -> str:
    c = str(c).strip()
    c = re.sub(r"[\s\-\/]+", "_", c)
    c = re.sub(r"[^A-Za-z0-9_]", "", c)
    return c.upper()

def _norm_text(x) -> str:
    return re.sub(r"[\s\-\_]+", "", str(x).upper()).strip()

def _coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")

def _safe_div(a, b):
    try:
        if b is None or pd.isna(b) or float(b) == 0.0:
            return np.nan
        return float(a) / float(b)
    except Exception:
        return np.nan

def _pick_best_sheet(path: Path):
    xl = pd.ExcelFile(path)
    best = None
    best_score = -1e9
    best_cols = None

    for sh in xl.sheet_names:
        try:
            hdr = pd.read_excel(path, sheet_name=sh, nrows=0)
            cols_raw = list(hdr.columns)
            cols = [_clean_colname(c) for c in cols_raw]
            cols_norm = [_norm_text(c) for c in cols_raw]

            score = 0.0

            # keyword score
            sh_l = sh.lower()
            for kw in SHEET_KEYWORDS:
                if kw in sh_l:
                    score += 2.0

            # column match score
            def has_any(group):
                group_norm = [_norm_text(g) for g in group]
                return any(any(g in cn for cn in cols_norm) for g in group_norm)

            ok = True
            for group in REQ_ANY:
                if has_any(group):
                    score += 5.0
                else:
                    ok = False
                    score -= 10.0

            # small bonus for "reasonable" width
            score += min(len(cols), 40) / 100.0

            if ok and score > best_score:
                best_score = score
                best = sh
                best_cols = cols_raw
        except Exception:
            continue

    if best is None:
        # fall back to first sheet
        best = xl.sheet_names[0]
    return best

def _find_col(df_cols, candidates):
    # candidates: list of synonyms
    # returns exact df column name
    cols = list(df_cols)
    cols_norm = {_norm_text(c): c for c in cols}
    cand_norm = [_norm_text(x) for x in candidates]

    # direct contains match in normalized form
    for cnorm, c in cols_norm.items():
        for pat in cand_norm:
            if pat in cnorm:
                return c
    return None

def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_clean_colname(c) for c in df.columns]
    return df

# =========================
# COSTSET -> METRIC MAPPING
# (ONLY uses COSTSET text)
# =========================
def map_costset_to_metric(costset: str) -> str:
    c = _norm_text(costset)

    # ACWP (actuals) often include ACTUAL / ACWP / ACWP HRS / A(H)CP etc
    if re.search(r"(ACWP|ACTUAL|ACT|ACWPHR|ACWPHRS|ACWPHOURS|ACWP_HR|ACWP_HRS|ACPHR|ACPHRS|ACPH|ACHR|ACHRS)", c):
        return "ACWP"

    # BCWS (planned value / budget / scheduled)
    if re.search(r"(BCWS|BUDGET|PLAN|PLANNED|SCHEDULE|SCHED|PV)", c):
        return "BCWS"

    # BCWP (earned value / earned / progress / performed / EV)
    if re.search(r"(BCWP|EARNED|EARN|EVD|EV|PROGRESS|PERFORM|PERFORMED)", c):
        return "BCWP"

    # ETC (estimate to complete / remaining / to-go)
    if re.search(r"(ETC|ESTIMATETOCOMPLETE|REMAIN|REMAINING|TOGO|TO_GO|TOG0)", c):
        return "ETC"

    # EAC (estimate at completion)
    if re.search(r"(EAC|ESTIMATEATCOMPLETE|ATCOMPLETE|AT_COMPLETE)", c):
        return "EAC"

    return "OTHER"

# =========================
# CLOSE DATE HELPERS
# =========================
def close_triplet(snapshot_date: pd.Timestamp, closes_list):
    # closes_list is a sorted python list of timestamps
    closes = [d for d in closes_list if d <= snapshot_date]
    if len(closes) >= 2:
        curr_close, prev_close = closes[-1], closes[-2]
    elif len(closes) == 1:
        curr_close = prev_close = closes[-1]
    else:
        # if snapshot before first close, use first close as both
        curr_close = prev_close = closes_list[0]

    # next close after curr
    next_candidates = [d for d in closes_list if d > curr_close]
    next_close = next_candidates[0] if len(next_candidates) else curr_close
    return curr_close, prev_close, next_close

def metric_cum_series(scope_df: pd.DataFrame, metric: str) -> pd.Series:
    s = (scope_df[scope_df["METRIC"] == metric]
         .groupby("DATE")["HOURS"].sum()
         .sort_index())
    return s.cumsum()

def value_at_close(cum_s: pd.Series, close_date: pd.Timestamp) -> float:
    if cum_s is None or cum_s.empty or pd.isna(close_date):
        return 0.0
    idx = cum_s.index[cum_s.index <= close_date]
    if len(idx) == 0:
        return 0.0
    return float(cum_s.loc[idx[-1]])

def delta_between_closes(cum_s: pd.Series, prev_close: pd.Timestamp, curr_close: pd.Timestamp) -> float:
    return value_at_close(cum_s, curr_close) - value_at_close(cum_s, prev_close)

# =========================
# LOAD FILES (selected only)
# =========================
print("\n============================")
print("1) Loading selected files...")
print("============================")

frames = []
load_log = []

for fn in SELECT_FILES:
    path = DATA_DIR / fn
    if not path.exists():
        load_log.append({"file": fn, "status": "MISSING FILE", "sheet": None, "rows": 0, "notes": str(path)})
        continue

    try:
        sh = _pick_best_sheet(path)
        df = pd.read_excel(path, sheet_name=sh)
        df = _normalize_df(df)

        date_col = _find_col(df.columns, ["DATE"])
        cost_col = _find_col(df.columns, ["COSTSET", "COST SET", "COST_SET"])
        hrs_col  = _find_col(df.columns, ["HOURS", "HRS", "HR", "LABOR_HOURS", "LABOR HOURS"])

        if date_col is None or cost_col is None or hrs_col is None:
            load_log.append({
                "file": fn, "status": "MISSING REQUIRED COLS",
                "sheet": sh, "rows": len(df),
                "notes": f"Found cols: date={date_col}, costset={cost_col}, hours={hrs_col}. All cols={list(df.columns)[:25]}"
            })
            continue

        out = df[[date_col, cost_col, hrs_col]].copy()
        out.columns = ["DATE", "COSTSET", "HOURS"]

        out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")
        out["HOURS"] = _coerce_numeric(out["HOURS"])
        out = out.dropna(subset=["DATE", "COSTSET", "HOURS"])

        out["SOURCE"] = fn
        out["SOURCE_SHEET"] = sh

        # subteam optional (if present)
        sub_col = _find_col(df.columns, ["SUB_TEAM", "SUBTEAM", "WBS", "WBS_ID", "CONTROL_ACCOUNT", "CA", "ORG", "CAM"])
        if sub_col is not None:
            out["SUB_TEAM"] = df.loc[out.index, sub_col].astype(str).str.strip()
        else:
            out["SUB_TEAM"] = "PROGRAM"

        out["METRIC"] = out["COSTSET"].astype(str).apply(map_costset_to_metric)
        out["COSTSET_NORM"] = out["COSTSET"].astype(str).apply(_norm_text)

        frames.append(out)
        load_log.append({"file": fn, "status": "OK", "sheet": sh, "rows": len(out), "notes": ""})
    except Exception as e:
        load_log.append({"file": fn, "status": "ERROR", "sheet": None, "rows": 0, "notes": repr(e)})

load_log_df = pd.DataFrame(load_log)
print("\nLoad log:")
print(load_log_df)

if len(frames) == 0:
    raise RuntimeError("No files loaded successfully. Fix file paths / filenames first.")

cobra_fact = pd.concat(frames, ignore_index=True)
print("\nLoaded cobra_fact rows:", len(cobra_fact))
print("Sources loaded:", cobra_fact["SOURCE"].nunique())
print("Date range:", cobra_fact["DATE"].min(), "to", cobra_fact["DATE"].max())

# =========================
# COVERAGE / MAPPING AUDITS
# =========================
print("\n============================")
print("2) Coverage & mapping audits")
print("============================")

# show top unmapped costsets
unmapped = (cobra_fact[cobra_fact["METRIC"] == "OTHER"]
            .groupby(["SOURCE","COSTSET_NORM"])["HOURS"].agg(["count","sum"])
            .sort_values(["count","sum"], ascending=False)
            .reset_index()
            .head(25))
print("\nTop unmapped (OTHER) costsets (top 25):")
print(unmapped)

# metric coverage by source
coverage = (cobra_fact
            .groupby(["SOURCE","METRIC"])
            .agg(rows=("HOURS","size"), sum_hours=("HOURS","sum"), min_date=("DATE","min"), max_date=("DATE","max"))
            .reset_index()
            .sort_values(["SOURCE","METRIC"]))
print("\nMetric coverage by source (rows/sum/min/max):")
print(coverage)

# for each source, % of rows that are OTHER
other_rate = (cobra_fact.assign(is_other=(cobra_fact["METRIC"]=="OTHER"))
              .groupby("SOURCE")["is_other"].mean()
              .reset_index(name="pct_OTHER_rows")
              .sort_values("pct_OTHER_rows", ascending=False))
print("\nPct OTHER rows by source:")
print(other_rate)

# =========================
# SNAPSHOT DATE PER SOURCE (max DATE)
# =========================
print("\n============================")
print("3) Snapshot/close dates")
print("============================")

snapshots = (cobra_fact.groupby("SOURCE")["DATE"].max()
             .reset_index(name="SNAPSHOT_DATE")
             .sort_values("SOURCE"))
snapshots["CURR_CLOSE"] = snapshots["SNAPSHOT_DATE"].apply(lambda d: close_triplet(d, CLOSES)[0])
snapshots["PREV_CLOSE"] = snapshots["SNAPSHOT_DATE"].apply(lambda d: close_triplet(d, CLOSES)[1])
snapshots["NEXT_CLOSE"] = snapshots["SNAPSHOT_DATE"].apply(lambda d: close_triplet(d, CLOSES)[2])
print("\nSnapshot and close triplets:")
print(snapshots)

# =========================
# WHY LSD IS ZERO DIAGNOSTIC (per source, program-level)
# =========================
print("\n============================")
print("4) LSD zero diagnostic (program-level)")
print("============================")

def _window_row_count(scope_df, metric, prev_close, curr_close):
    t = scope_df[scope_df["METRIC"] == metric]
    in_window = t[(t["DATE"] > prev_close) & (t["DATE"] <= curr_close)]
    return len(in_window), float(in_window["HOURS"].sum()) if len(in_window) else 0.0

diag_rows = []
program_rows = []

for _, row in snapshots.iterrows():
    src = row["SOURCE"]
    snap = row["SNAPSHOT_DATE"]
    curr_close, prev_close, next_close = row["CURR_CLOSE"], row["PREV_CLOSE"], row["NEXT_CLOSE"]

    src_df = cobra_fact[cobra_fact["SOURCE"] == src].copy()

    # Program scope only
    scope = src_df.copy()

    # cumulative series
    bcws_cum = metric_cum_series(scope, "BCWS")
    bcwp_cum = metric_cum_series(scope, "BCWP")
    acwp_cum = metric_cum_series(scope, "ACWP")
    etc_cum  = metric_cum_series(scope, "ETC")
    eac_cum  = metric_cum_series(scope, "EAC")

    # CTD @ curr close
    bcws_ctd = value_at_close(bcws_cum, curr_close)
    bcwp_ctd = value_at_close(bcwp_cum, curr_close)
    acwp_ctd = value_at_close(acwp_cum, curr_close)

    # LSD delta (correct method)
    bcws_lsd = delta_between_closes(bcws_cum, prev_close, curr_close)
    bcwp_lsd = delta_between_closes(bcwp_cum, prev_close, curr_close)
    acwp_lsd = delta_between_closes(acwp_cum, prev_close, curr_close)

    # "wrong method" window-sum just to compare
    bcws_win_n, bcws_win_sum = _window_row_count(scope, "BCWS", prev_close, curr_close)
    bcwp_win_n, bcwp_win_sum = _window_row_count(scope, "BCWP", prev_close, curr_close)
    acwp_win_n, acwp_win_sum = _window_row_count(scope, "ACWP", prev_close, curr_close)

    # BAC/EAC totals (end of series)
    bac_total = float(bcws_cum.iloc[-1]) if not bcws_cum.empty else 0.0
    eac_total = float(eac_cum.iloc[-1]) if not eac_cum.empty else 0.0
    etc_total = float(etc_cum.iloc[-1]) if not etc_cum.empty else 0.0
    if (eac_total == 0.0) and (etc_total > 0.0):
        eac_total = acwp_ctd + etc_total

    spi_ctd = _safe_div(bcwp_ctd, bcws_ctd)
    cpi_ctd = _safe_div(bcwp_ctd, acwp_ctd)

    spi_lsd = _safe_div(bcwp_lsd, bcws_lsd)
    cpi_lsd = _safe_div(bcwp_lsd, acwp_lsd)

    demand_hours = bcws_lsd
    actual_hours = acwp_lsd
    pct_var = _safe_div((actual_hours - demand_hours), demand_hours)

    program_rows.append({
        "SOURCE": src,
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
        "BAC": bac_total,
        "EAC": eac_total,
        "ETC_TOTAL": etc_total,
        "Demand_Hours": demand_hours,
        "Actual_Hours": actual_hours,
        "Pct_Var": pct_var,
    })

    diag_rows.append({
        "SOURCE": src,
        "SNAPSHOT_DATE": snap,
        "PREV_CLOSE": prev_close,
        "CURR_CLOSE": curr_close,
        "BCWS_CTD": bcws_ctd,
        "BCWP_CTD": bcwp_ctd,
        "ACWP_CTD": acwp_ctd,
        "BCWS_LSD(delta)": bcws_lsd,
        "BCWP_LSD(delta)": bcwp_lsd,
        "ACWP_LSD(delta)": acwp_lsd,
        "BCWS_window_rows": bcws_win_n,
        "BCWS_window_sum": bcws_win_sum,
        "BCWP_window_rows": bcwp_win_n,
        "BCWP_window_sum": bcwp_win_sum,
        "ACWP_window_rows": acwp_win_n,
        "ACWP_window_sum": acwp_win_sum,
        "LIKELY_CAUSE_IF_WINDOW_ZERO": (
            "No rows inside close window (export is cumulative / sparse dates)"
            if (bcwp_win_n == 0 or acwp_win_n == 0) else ""
        )
    })

program_metrics = pd.DataFrame(program_rows)
diag = pd.DataFrame(diag_rows)

print("\n--- LSD Diagnostic (compare delta vs window-sum) ---")
print(diag)

# show if any LSD came out 0 while CTD > 0
z = program_metrics.copy()
z["bcwp_lsd_zero"] = (z["BCWP_LSD"].fillna(0) == 0)
z["acwp_lsd_zero"] = (z["ACWP_LSD"].fillna(0) == 0)
z["bcwp_ctd_pos"] = z["BCWP_CTD"] > 0
z["acwp_ctd_pos"] = z["ACWP_CTD"] > 0

problem = z[(z["bcwp_ctd_pos"] & z["bcwp_lsd_zero"]) | (z["acwp_ctd_pos"] & z["acwp_lsd_zero"])][
    ["SOURCE","SNAPSHOT_DATE","PREV_CLOSE","CURR_CLOSE","BCWP_CTD","ACWP_CTD","BCWP_LSD","ACWP_LSD","Demand_Hours","Actual_Hours","Pct_Var"]
]
print("\n--- Sources where CTD>0 but LSD==0 (this is your 'zeros' problem) ---")
print(problem if len(problem) else "None 🎉")

# =========================
# DEEP DIVE: show last 20 rows for key metrics around closes (per source)
# =========================
print("\n============================")
print("5) Deep-dive per source: last rows for ACWP/BCWP/BCWS and window presence")
print("============================")

def _print_metric_tail(src, metric, prev_close, curr_close, n=20):
    sdf = cobra_fact[(cobra_fact["SOURCE"]==src) & (cobra_fact["METRIC"]==metric)].copy()
    sdf = sdf.sort_values("DATE")
    tail = sdf[["DATE","COSTSET","HOURS"]].tail(n)
    print(f"\n[{src}] {metric} last {n} rows:")
    print(tail.to_string(index=False))

    in_window = sdf[(sdf["DATE"]>prev_close) & (sdf["DATE"]<=curr_close)]
    print(f"Rows INSIDE window (prev_close, curr_close] = ({prev_close.date()}, {curr_close.date()}]: {len(in_window)} ; sum={in_window['HOURS'].sum() if len(in_window) else 0.0}")
    if len(in_window):
        print(in_window[["DATE","COSTSET","HOURS"]].to_string(index=False))

for _, row in snapshots.iterrows():
    src = row["SOURCE"]
    prev_close = row["PREV_CLOSE"]
    curr_close = row["CURR_CLOSE"]

    print("\n--------------------------------------------")
    print("SOURCE:", src)
    print("prev_close:", prev_close, "curr_close:", curr_close)
    print("--------------------------------------------")

    # Basic counts by metric
    counts = (cobra_fact[cobra_fact["SOURCE"]==src]
              .groupby("METRIC")["HOURS"].agg(["count","sum"])
              .reset_index()
              .sort_values("count", ascending=False))
    print("\nMetric counts/sums:")
    print(counts.to_string(index=False))

    # tails
    for metric in ["BCWS","BCWP","ACWP","ETC","EAC","OTHER"]:
        _print_metric_tail(src, metric, prev_close, curr_close, n=15)

# =========================
# MISSING SUMMARY (what is actually missing vs just zero?)
# =========================
print("\n============================")
print("6) Missing vs zero summary")
print("============================")

def _pct_zero(s):
    s = pd.to_numeric(s, errors="coerce")
    s = s.fillna(np.nan)
    return float((s == 0).mean()) if len(s.dropna()) else np.nan

missing_summary = (program_metrics
                   .assign(
                       pct_BCWS_LSD_zero=lambda d: d["BCWS_LSD"].apply(lambda x: 1.0 if (pd.notna(x) and x==0) else 0.0),
                       pct_BCWP_LSD_zero=lambda d: d["BCWP_LSD"].apply(lambda x: 1.0 if (pd.notna(x) and x==0) else 0.0),
                       pct_ACWP_LSD_zero=lambda d: d["ACWP_LSD"].apply(lambda x: 1.0 if (pd.notna(x) and x==0) else 0.0),
                   )[["SOURCE","BCWS_CTD","BCWP_CTD","ACWP_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD","Demand_Hours","Actual_Hours","Pct_Var","BAC","EAC","ETC_TOTAL"]])

print("\nProgram metrics preview:")
print(program_metrics)

print("\nNote: any 0.0 in LSD/Hours/Pct_Var usually means either:")
print(" - No rows exist inside your close window (window-sum method would fail), OR")
print(" - Cumulative series did not change between closes (true zero), OR")
print(" - Costset mapping missed ACWP/BCWP/BCWS and they became OTHER.")

print("\nOutputs created in memory:")
print(" - cobra_fact (long fact table)")
print(" - coverage (metric coverage by source)")
print(" - snapshots (snapshot and close triplets)")
print(" - diag (delta vs window-sum comparison)")
print(" - program_metrics (overview table we want to be correct)")
print(" - missing_summary (quick review)")

# Keep these variables alive for your next cells
cobra_fact, coverage, snapshots, diag, program_metrics, missing_summary