import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# ============================================================
# CONFIG
# ============================================================
DATA_DIR = Path("data")
FILE_PREFIX = "cobra"  # case-insensitive
SHEET_KEYWORDS = ["tbl", "weekly", "extract", "cap"]  # choose first matching sheet

# Accounting closes (include both 2025 (from your screenshot) and 2026 (calendar image))
# NOTE: update/add years as needed.
ACCOUNTING_CLOSINGS = {
    (2025, 1): 26, (2025, 2): 23, (2025, 3): 30, (2025, 4): 27, (2025, 5): 25, (2025, 6): 29,
    (2025, 7): 27, (2025, 8): 24, (2025, 9): 28, (2025,10): 26, (2025,11): 23, (2025,12): 31,
    (2026, 1): 4,  (2026, 2): 1,  (2026, 3): 1,  (2026, 4): 5,  (2026, 5): 3,  (2026, 6): 7,
    (2026, 7): 5,  (2026, 8): 2,  (2026, 9): 6,  (2026,10): 4,  (2026,11): 1,  (2026,12): 6,
}

# ============================================================
# Helpers
# ============================================================
def _clean_colname(c: str) -> str:
    s = str(c).strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "", s)
    return s.upper()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_clean_colname(c) for c in df.columns]
    return df

def safe_div(n, d):
    n = n.astype(float)
    d = d.astype(float)
    return np.where(d != 0, n / d, np.nan)

def build_close_dates_from_dict(closing_dict: dict) -> pd.DatetimeIndex:
    dates = []
    for (y, m), day in closing_dict.items():
        try:
            dates.append(pd.Timestamp(datetime(y, m, day)))
        except Exception:
            pass
    return pd.DatetimeIndex(sorted(set(dates)))

ACCT_CLOSE_DATES = build_close_dates_from_dict(ACCOUNTING_CLOSINGS)

def get_status_dates_from_data(max_date: pd.Timestamp):
    """
    Returns (curr_close, prev_close). If no close <= max_date, falls back to month-end logic.
    """
    if pd.isna(max_date):
        return (pd.NaT, pd.NaT)
    closes = ACCT_CLOSE_DATES[ACCT_CLOSE_DATES <= max_date]
    if len(closes) >= 2:
        return (closes[-1], closes[-2])
    if len(closes) == 1:
        return (closes[-1], closes[-1])
    # fallback: month-end
    me = pd.Timestamp(max_date).to_period("M").to_timestamp("M")
    prev_me = (me - pd.offsets.MonthEnd(1))
    return (me, prev_me)

def best_sheet(path: Path):
    """
    Pick the best sheet based on keywords, otherwise first sheet.
    """
    xls = pd.ExcelFile(path)
    sheets = xls.sheet_names
    scored = []
    for sh in sheets:
        s = sh.lower()
        score = sum(1 for k in SHEET_KEYWORDS if k in s)
        scored.append((score, sh))
    scored.sort(reverse=True, key=lambda t: t[0])
    return scored[0][1] if scored else None

def pick_value_column(df: pd.DataFrame) -> str:
    """
    Choose the numeric value column deterministically.
    Prefer HOURS, then AMOUNT/COST/DOLLARS/VALUE variants.
    """
    cols = list(df.columns)
    candidates = []
    prefer_order = ["HOURS", "AMOUNT", "COST", "DOLLARS", "VALUE", "TOTAL"]
    for p in prefer_order:
        if p in cols:
            candidates.append(p)
    # add any numeric columns that look like value fields
    for c in cols:
        if c in candidates:
            continue
        if re.search(r"(HOUR|AMOUNT|COST|DOLLAR|VALUE|TOTAL)", c, re.I):
            candidates.append(c)
    # fallback: first numeric column
    if not candidates:
        num_cols = [c for c in cols if pd.api.types.is_numeric_dtype(df[c])]
        if num_cols:
            return num_cols[0]
        # try coercion for all cols
        for c in cols:
            try:
                pd.to_numeric(df[c].head(50), errors="raise")
                return c
            except Exception:
                continue
        raise ValueError("No numeric value column found.")
    return candidates[0]

# ============================================================
# COST SET / METRIC NORMALIZATION
# ============================================================
# We categorize into:
# FLOW metrics (sum monthly then cumsum): BCWS, BCWP, ACWP
# STOCK metrics (last monthly then ffill): BAC, EAC, ETC
# HOURS flow (sum monthly then cumsum + next month incremental): BCWS_HRS, ACWP_HRS, ETC_HRS

FLOW_METRICS = {"BCWS", "BCWP", "ACWP", "BCWS_HRS", "ACWP_HRS", "ETC_HRS"}
STOCK_METRICS = {"BAC", "EAC", "ETC"}  # dollar ETC is often stock-like; if yours is flow you can move it to FLOW

def norm_cost_set(raw: str) -> str | None:
    s = str(raw).strip().upper()
    s = re.sub(r"[^A-Z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Hours variants
    if re.search(r"\bBCWS\b.*\b(HRS|HOURS)\b|\bBCWS_HRS\b|\bPLAN(HR|HRS|HOURS)\b", s):
        return "BCWS_HRS"
    if re.search(r"\bACWP\b.*\b(HRS|HOURS)\b|\bACWP_HRS\b|\bACHP_HRS\b|\bACTUAL(HR|HRS|HOURS)\b", s):
        return "ACWP_HRS"
    if re.search(r"\bETC\b.*\b(HRS|HOURS)\b|\bETC_HRS\b", s):
        return "ETC_HRS"

    # Explicit BAC/EAC/ETC
    if re.search(r"\bBAC\b|BUDGET AT COMPLETION", s):
        return "BAC"
    if re.search(r"\bEAC\b|ESTIMATE AT COMPLETION", s):
        return "EAC"
    if re.search(r"\bETC\b|ESTIMATE TO COMPLETE|REMAINING|TO GO", s):
        return "ETC"

    # Planned/Earned/Actual (dollars)
    # BCWS synonyms: planned value, plan, schedule, budget (in some extracts "Budget" is PV not BAC)
    if re.search(r"\bBCWS\b|PLANNED VALUE|\bPLAN\b|\bSCHEDULE\b", s):
        return "BCWS"
    if s == "BUDGET":  # ambiguous; treat as PV (BCWS) in these extracts
        return "BCWS"

    # BCWP synonyms: earned value, progress, perform
    if re.search(r"\bBCWP\b|EARNED|PROGRESS|PERFORM", s):
        return "BCWP"

    # ACWP synonyms: actual cost, weekly actuals, ACWP_WKL
    if re.search(r"\bACWP\b|ACTUAL COST|WEEKLY ACTUALS|ACWP_WKL|ACWP WKL", s):
        return "ACWP"

    return None

# ============================================================
# SERIES BUILDERS (this is the key fix vs the broken pipeline)
# ============================================================
def build_metric_series(group_df: pd.DataFrame, date_col: str, metric: str, value_col: str) -> pd.Series:
    """
    Returns a MONTH-END indexed series.
    For FLOW metrics: monthly sum then cumsum (CTD series)
    For STOCK metrics: monthly last then ffill (point-in-time)
    """
    g = group_df[group_df["METRIC"] == metric].copy()
    if g.empty:
        return pd.Series(dtype=float)

    g[date_col] = pd.to_datetime(g[date_col], errors="coerce")
    g = g.dropna(subset=[date_col])
    if g.empty:
        return pd.Series(dtype=float)

    g["PERIOD_ME"] = g[date_col].dt.to_period("M").dt.to_timestamp("M")
    g[value_col] = pd.to_numeric(g[value_col], errors="coerce").fillna(0.0)

    if metric in FLOW_METRICS:
        s = g.groupby("PERIOD_ME")[value_col].sum().sort_index()
        return s.cumsum()
    else:
        # STOCK
        s = g.sort_values(date_col).groupby("PERIOD_ME")[value_col].last().sort_index()
        return s.ffill()

def value_at(series: pd.Series, when: pd.Timestamp) -> float:
    if series is None or series.empty or pd.isna(when):
        return np.nan
    when_me = pd.Timestamp(when).to_period("M").to_timestamp("M")
    s = series[series.index <= when_me]
    return float(s.iloc[-1]) if len(s) else np.nan

def monthly_increment_at(group_df: pd.DataFrame, date_col: str, metric: str, value_col: str, when: pd.Timestamp) -> float:
    """
    For NEXT MONTH metrics we want the monthly incremental, not cumulative.
    """
    g = group_df[group_df["METRIC"] == metric].copy()
    if g.empty or pd.isna(when):
        return np.nan
    g[date_col] = pd.to_datetime(g[date_col], errors="coerce")
    g = g.dropna(subset=[date_col])
    if g.empty:
        return np.nan
    g["PERIOD_ME"] = g[date_col].dt.to_period("M").dt.to_timestamp("M")
    g[value_col] = pd.to_numeric(g[value_col], errors="coerce").fillna(0.0)
    s = g.groupby("PERIOD_ME")[value_col].sum().sort_index()  # incremental
    when_me = pd.Timestamp(when).to_period("M").to_timestamp("M")
    return float(s.get(when_me, np.nan))

# ============================================================
# LOAD ALL COBRA FILES -> LONG FACT TABLE
# ============================================================
loaded = []
pipeline_issues = []

for path in DATA_DIR.glob("*.xlsx"):
    if not path.name.lower().startswith(FILE_PREFIX.lower()):
        continue

    try:
        sh = best_sheet(path)
        df = pd.read_excel(path, sheet_name=sh)
        df = normalize_columns(df)

        # column harmonization
        # Accept COST-SET variants
        if "COST-SET" in df.columns and "COSTSET" not in df.columns:
            df = df.rename(columns={"COST-SET": "COSTSET"})
        if "COST_SET" in df.columns and "COSTSET" not in df.columns:
            df = df.rename(columns={"COST_SET": "COSTSET"})
        if "SUB_TEAM" not in df.columns:
            # try common alternatives
            for alt in ["SUBTEAM", "SUB_TEAM_", "RESP_DEPT", "RESPDEPT", "BE_DEPT", "BEDEPT"]:
                if alt in df.columns:
                    df = df.rename(columns={alt: "SUB_TEAM"})
                    break

        if "DATE" not in df.columns or "COSTSET" not in df.columns:
            pipeline_issues.append((path.name, sh, "Missing DATE and/or COSTSET column"))
            continue

        # choose value column (deterministic)
        val_col = pick_value_column(df)
        df[val_col] = pd.to_numeric(df[val_col], errors="coerce")

        # normalize metric
        df["METRIC"] = df["COSTSET"].apply(norm_cost_set)

        # filter to rows we care about
        df = df[df["METRIC"].notna()].copy()
        if df.empty:
            pipeline_issues.append((path.name, sh, "No recognized COSTSET values after mapping"))
            continue

        # ensure subteam exists
        if "SUB_TEAM" not in df.columns:
            df["SUB_TEAM"] = "PROGRAM"

        df["SOURCE"] = path.name
        df["SOURCE_SHEET"] = sh
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["DATE"])

        # build long fact
        fact = df[["SOURCE", "SOURCE_SHEET", "SUB_TEAM", "DATE", "METRIC", val_col]].copy()
        fact = fact.rename(columns={val_col: "VALUE"})
        fact["VALUE"] = pd.to_numeric(fact["VALUE"], errors="coerce").fillna(0.0)

        loaded.append(fact)

        print(f"✅ Loaded {path.name} → {sh} | value_col={val_col} | rows={len(fact):,}")

    except Exception as e:
        pipeline_issues.append((path.name, None, f"Load error: {e}"))

cobra_fact = pd.concat(loaded, ignore_index=True) if loaded else pd.DataFrame(
    columns=["SOURCE","SOURCE_SHEET","SUB_TEAM","DATE","METRIC","VALUE"]
)

# ============================================================
# BUILD METRICS TABLES
# ============================================================
if cobra_fact.empty:
    raise ValueError("cobra_fact is empty. Check pipeline_issues for load/mapping problems.")

# Snapshot dates per source
snap = cobra_fact.groupby("SOURCE", as_index=False).agg(SNAPSHOT_DATE=("DATE","max"))
snap["CURR_CLOSE"], snap["PREV_CLOSE"] = zip(*snap["SNAPSHOT_DATE"].apply(get_status_dates_from_data))

cobra_fact = cobra_fact.merge(snap, on="SOURCE", how="left")

# Audits
label_audit = (
    cobra_fact.groupby(["SOURCE","METRIC"], as_index=False).size()
    .pivot_table(index="SOURCE", columns="METRIC", values="size", fill_value=0)
    .reset_index()
)

# Compute per SOURCE + SUB_TEAM
rows = []
for (src, st), g in cobra_fact.groupby(["SOURCE","SUB_TEAM"]):
    snapshot_date = g["SNAPSHOT_DATE"].iloc[0]
    curr_close = g["CURR_CLOSE"].iloc[0]
    prev_close = g["PREV_CLOSE"].iloc[0]

    # Build CTD series for flow metrics (cumsum) and stock metrics (ffill)
    series = {}
    for m in ["BCWS","BCWP","ACWP","BAC","EAC","ETC","BCWS_HRS","ACWP_HRS","ETC_HRS"]:
        series[m] = build_metric_series(g, "DATE", m, "VALUE")

    # CTD values at snapshot
    bcws_ctd = value_at(series["BCWS"], snapshot_date)
    bcwp_ctd = value_at(series["BCWP"], snapshot_date)
    acwp_ctd = value_at(series["ACWP"], snapshot_date)

    # LSD values via close deltas on CTD series
    bcws_lsd = value_at(series["BCWS"], curr_close) - value_at(series["BCWS"], prev_close)
    bcwp_lsd = value_at(series["BCWP"], curr_close) - value_at(series["BCWP"], prev_close)
    acwp_lsd = value_at(series["ACWP"], curr_close) - value_at(series["ACWP"], prev_close)

    # BAC (prefer explicit BAC stock; else derive as max cumulative BCWS across entire baseline)
    bac_explicit = value_at(series["BAC"], snapshot_date)
    bac_method = "explicit_BAC" if pd.notna(bac_explicit) and bac_explicit != 0 else None

    if bac_method is None:
        # Derive BAC as max of cumulative BCWS (works when BCWS is baseline PV CTD)
        bac_derived = float(series["BCWS"].max()) if (series["BCWS"] is not None and not series["BCWS"].empty) else np.nan
        bac = bac_derived
        bac_method = "derived_max_cum_BCWS" if pd.notna(bac_derived) and bac_derived != 0 else "missing"
    else:
        bac = bac_explicit

    # EAC (prefer explicit EAC; else EAC = ACWP + ETC if ETC exists; else EAC = BAC / CPI if possible)
    eac_explicit = value_at(series["EAC"], snapshot_date)
    if pd.notna(eac_explicit) and eac_explicit != 0:
        eac = eac_explicit
        eac_method = "explicit_EAC"
    else:
        etc_stock = value_at(series["ETC"], snapshot_date)
        if pd.notna(etc_stock) and etc_stock != 0 and pd.notna(acwp_ctd):
            eac = acwp_ctd + etc_stock
            eac_method = "derived_ACWP_plus_ETC"
        else:
            cpi_ctd = (bcwp_ctd / acwp_ctd) if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan
            if pd.notna(bac) and bac != 0 and pd.notna(cpi_ctd) and cpi_ctd != 0:
                eac = bac / cpi_ctd
                eac_method = "derived_BAC_div_CPI"
            else:
                eac = np.nan
                eac_method = "missing"

    vac_ctd = bac - eac if (pd.notna(bac) and pd.notna(eac)) else np.nan

    spi_ctd = (bcwp_ctd / bcws_ctd) if (pd.notna(bcwp_ctd) and pd.notna(bcws_ctd) and bcws_ctd != 0) else np.nan
    cpi_ctd = (bcwp_ctd / acwp_ctd) if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan
    spi_lsd = (bcwp_lsd / bcws_lsd) if (pd.notna(bcwp_lsd) and pd.notna(bcws_lsd) and bcws_lsd != 0) else np.nan
    cpi_lsd = (bcwp_lsd / acwp_lsd) if (pd.notna(bcwp_lsd) and pd.notna(acwp_lsd) and acwp_lsd != 0) else np.nan

    bei_ctd = (bcwp_ctd / bac) if (pd.notna(bcwp_ctd) and pd.notna(bac) and bac != 0) else np.nan

    # Hours metrics (monthly incremental for next month + CTD from cumsum)
    # Demand Hours = BCWS_HRS LSD (current close period increment)
    demand_hrs = value_at(series["BCWS_HRS"], curr_close) - value_at(series["BCWS_HRS"], prev_close) if (not series["BCWS_HRS"].empty) else np.nan
    actual_hrs = value_at(series["ACWP_HRS"], curr_close) - value_at(series["ACWP_HRS"], prev_close) if (not series["ACWP_HRS"].empty) else np.nan

    pct_var = ((actual_hrs - demand_hrs) / demand_hrs) if (pd.notna(actual_hrs) and pd.notna(demand_hrs) and demand_hrs != 0) else np.nan

    next_month = (pd.Timestamp(curr_close).to_period("M") + 1).to_timestamp("M") if pd.notna(curr_close) else pd.NaT
    next_mo_bcws_hrs = monthly_increment_at(g, "DATE", "BCWS_HRS", "VALUE", next_month)
    next_mo_etc_hrs = monthly_increment_at(g, "DATE", "ETC_HRS", "VALUE", next_month)

    rows.append({
        "SOURCE": src,
        "SUB_TEAM": st,
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,

        "BCWS_CTD": bcws_ctd,
        "BCWP_CTD": bcwp_ctd,
        "ACWP_CTD": acwp_ctd,
        "SPI_CTD": spi_ctd,
        "CPI_CTD": cpi_ctd,
        "BEI_CTD": bei_ctd,

        "BCWS_LSD": bcws_lsd,
        "BCWP_LSD": bcwp_lsd,
        "ACWP_LSD": acwp_lsd,
        "SPI_LSD": spi_lsd,
        "CPI_LSD": cpi_lsd,

        "BAC": bac,
        "BAC_METHOD": bac_method,
        "EAC": eac,
        "EAC_METHOD": eac_method,
        "VAC_CTD": vac_ctd,

        "Demand_Hours": demand_hrs,
        "Actual_Hours": actual_hrs,
        "Pct_Var": pct_var,
        "Next_Mo_BCWS_Hours": next_mo_bcws_hrs,
        "Next_Mo_ETC_Hours": next_mo_etc_hrs
    })

subteam_metrics = pd.DataFrame(rows)

# Program-level rollup (SOURCE only)
program_metrics = (
    subteam_metrics.groupby("SOURCE", as_index=False)
    .agg(
        SNAPSHOT_DATE=("SNAPSHOT_DATE","max"),
        CURR_CLOSE=("CURR_CLOSE","max"),
        PREV_CLOSE=("PREV_CLOSE","max"),
        BCWS_CTD=("BCWS_CTD","sum"),
        BCWP_CTD=("BCWP_CTD","sum"),
        ACWP_CTD=("ACWP_CTD","sum"),
        BAC=("BAC","sum"),
        EAC=("EAC","sum"),
        VAC_CTD=("VAC_CTD","sum"),
        BCWS_LSD=("BCWS_LSD","sum"),
        BCWP_LSD=("BCWP_LSD","sum"),
        ACWP_LSD=("ACWP_LSD","sum"),
        Demand_Hours=("Demand_Hours","sum"),
        Actual_Hours=("Actual_Hours","sum"),
        Next_Mo_BCWS_Hours=("Next_Mo_BCWS_Hours","sum"),
        Next_Mo_ETC_Hours=("Next_Mo_ETC_Hours","sum"),
    )
)

program_metrics["SPI_CTD"] = safe_div(program_metrics["BCWP_CTD"], program_metrics["BCWS_CTD"])
program_metrics["CPI_CTD"] = safe_div(program_metrics["BCWP_CTD"], program_metrics["ACWP_CTD"])
program_metrics["BEI_CTD"] = safe_div(program_metrics["BCWP_CTD"], program_metrics["BAC"])
program_metrics["SPI_LSD"] = safe_div(program_metrics["BCWP_LSD"], program_metrics["BCWS_LSD"])
program_metrics["CPI_LSD"] = safe_div(program_metrics["BCWP_LSD"], program_metrics["ACWP_LSD"])
program_metrics["Pct_Var"] = safe_div(program_metrics["Actual_Hours"] - program_metrics["Demand_Hours"], program_metrics["Demand_Hours"])

# Cost-only table
subteam_cost = subteam_metrics[["SOURCE","SUB_TEAM","BAC","BAC_METHOD","EAC","EAC_METHOD","VAC_CTD"]].copy()

# Hours-only table
hours_metrics = subteam_metrics[[
    "SOURCE","SUB_TEAM","SNAPSHOT_DATE","CURR_CLOSE",
    "Demand_Hours","Actual_Hours","Pct_Var","Next_Mo_BCWS_Hours","Next_Mo_ETC_Hours"
]].copy()

# Coverage audit: where are things missing and why
coverage_audit = (
    subteam_metrics.assign(
        BCWS_missing=lambda d: d["BCWS_CTD"].isna() | (d["BCWS_CTD"]==0),
        ACWP_missing=lambda d: d["ACWP_CTD"].isna() | (d["ACWP_CTD"]==0),
        BAC_missing=lambda d: d["BAC"].isna() | (d["BAC"]==0),
        EAC_missing=lambda d: d["EAC"].isna() | (d["EAC"]==0),
    )
    .groupby("SOURCE", as_index=False)
    .agg(
        rows=("SUB_TEAM","count"),
        pct_BCWS_missing=("BCWS_missing","mean"),
        pct_ACWP_missing=("ACWP_missing","mean"),
        pct_BAC_missing=("BAC_missing","mean"),
        pct_EAC_missing=("EAC_missing","mean"),
    )
    .sort_values(["pct_BCWS_missing","pct_ACWP_missing","pct_BAC_missing","pct_EAC_missing"], ascending=False)
)

print("\n✅ Pipeline outputs created:")
print(" - cobra_fact (long fact)")
print(" - program_metrics (per program/source)")
print(" - subteam_metrics (per program/source + subteam)")
print(" - subteam_cost (BAC/EAC/VAC)")
print(" - hours_metrics (Demand/Actual/%Var/Next month)")
print(" - label_audit, coverage_audit, pipeline_issues")

print("\n--- Top coverage issues (sources with most missing) ---")
print(coverage_audit.head(15))

if pipeline_issues:
    print("\n--- Load/mapping issues ---")
    for it in pipeline_issues[:25]:
        print(" -", it)

# ============================================================
# OPTIONAL: Write all tables to ONE Excel for Power BI
# ============================================================
# out_path = DATA_DIR / "cobra_evms_tables.xlsx"
# with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
#     cobra_fact.to_excel(writer, sheet_name="fact_long", index=False)
#     program_metrics.to_excel(writer, sheet_name="program_metrics", index=False)
#     subteam_metrics.to_excel(writer, sheet_name="subteam_metrics", index=False)
#     subteam_cost.to_excel(writer, sheet_name="subteam_cost", index=False)
#     hours_metrics.to_excel(writer, sheet_name="hours_metrics", index=False)
#     label_audit.to_excel(writer, sheet_name="label_audit", index=False)
#     coverage_audit.to_excel(writer, sheet_name="coverage_audit", index=False)
# print(f"\n✅ Wrote: {out_path}")
