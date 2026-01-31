import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# =========================
# 1) PICK EXACT FILES HERE
# =========================
DATA_DIR = Path("data")
SELECT_FILES = [
    "Cobra-Abrams STS 2022.xlsx",
    "Cobra-John G Weekly CAP OLY 12.07.2025.xlsx",
    "Cobra-XM30.xlsx",
    "Cobra-Stryker Bulgaria 150.xlsx",
]

# =========================
# 2) ACCOUNTING CLOSE DATES
# =========================
ACCOUNTING_CLOSINGS = {
    (2025, 1): 26, (2025, 2): 23, (2025, 3): 30, (2025, 4): 27, (2025, 5): 25, (2025, 6): 29,
    (2025, 7): 27, (2025, 8): 24, (2025, 9): 28, (2025,10): 26, (2025,11): 23, (2025,12): 31,
    (2026, 1): 4,  (2026, 2): 1,  (2026, 3): 1,  (2026, 4): 5,  (2026, 5): 3,  (2026, 6): 7,
    (2026, 7): 5,  (2026, 8): 2,  (2026, 9): 6,  (2026,10): 4,  (2026,11): 1,  (2026,12): 6,
}
def _close_dates(d):
    out=[]
    for (y,m),day in d.items():
        try: out.append(pd.Timestamp(datetime(y,m,day)))
        except: pass
    return pd.DatetimeIndex(sorted(set(out)))
ACCT_CLOSE_DATES = _close_dates(ACCOUNTING_CLOSINGS)

def get_status_dates(max_date):
    if pd.isna(max_date): return (pd.NaT, pd.NaT)
    closes = ACCT_CLOSE_DATES[ACCT_CLOSE_DATES <= max_date]
    if len(closes) >= 2: return (closes[-1], closes[-2])
    if len(closes) == 1: return (closes[-1], closes[-1])
    me = pd.Timestamp(max_date).to_period("M").to_timestamp("M")
    prev_me = me - pd.offsets.MonthEnd(1)
    return (me, prev_me)

# =========================
# 3) HELPERS
# =========================
SHEET_KEYWORDS = ["tbl", "weekly", "extract", "cap"]

def _clean_col(c):
    s = str(c).strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_]", "", s)
    return s.upper()

def normalize_cols(df):
    df=df.copy()
    df.columns=[_clean_col(c) for c in df.columns]
    return df

def best_sheet(path: Path):
    xls = pd.ExcelFile(path)
    scored=[]
    for sh in xls.sheet_names:
        s=sh.lower()
        score=sum(1 for k in SHEET_KEYWORDS if k in s)
        scored.append((score, sh))
    scored.sort(reverse=True, key=lambda t:t[0])
    return scored[0][1] if scored else xls.sheet_names[0]

def norm_costset(raw):
    s = str(raw).strip().upper()
    s = re.sub(r"[^A-Z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # HOURS flavors explicitly labeled
    if re.search(r"\bBCWS\b.*\b(HRS|HOURS)\b|\bBCWS_HRS\b", s): return ("BCWS", "HRS")
    if re.search(r"\bACWP\b.*\b(HRS|HOURS)\b|\bACWP_HRS\b|\bACHP_HRS\b", s): return ("ACWP", "HRS")
    if re.search(r"\bETC\b.*\b(HRS|HOURS)\b|\bETC_HRS\b", s): return ("ETC", "HRS")

    # Explicit totals USD
    if re.search(r"\bBAC\b|BUDGET AT COMPLETION", s): return ("BAC", "USD")
    if re.search(r"\bEAC\b|ESTIMATE AT COMPLETION", s): return ("EAC", "USD")
    if re.search(r"\bETC\b|ESTIMATE TO COMPLETE|REMAINING|TO GO", s): return ("ETC", "USD")

    # Flows USD (Cobra exports vary: Budget/Progress/Weekly Actuals etc.)
    if re.search(r"\bBCWS\b|PLANNED VALUE|\bPLAN\b|\bSCHEDULE\b", s): return ("BCWS", "USD")
    if s == "BUDGET": return ("BCWS", "USD")
    if re.search(r"\bBCWP\b|EARNED|PROGRESS|PERFORM", s): return ("BCWP", "USD")
    if re.search(r"\bACWP\b|ACTUAL COST|WEEKLY ACTUALS|ACWP_WKL", s): return ("ACWP", "USD")

    return (None, None)

FLOW_USD = {"BCWS","BCWP","ACWP"}  # monthly flow -> CTD is cumulative sum

def _to_num_series(s):
    # handles strings like "1,234.56" too
    return pd.to_numeric(
        s.astype(str).str.replace(",", "", regex=False).str.replace("$","", regex=False),
        errors="coerce"
    )

def pick_numeric_columns(df):
    # identify numeric-ish columns besides keys
    key_like = {"DATE","PERIOD","MONTH","COSTSET","COST_SET","COSTSET","SUB_TEAM","SOURCE","SOURCE_SHEET"}
    cols = [c for c in df.columns if c not in key_like]

    # common names first
    hrs_like = [c for c in cols if re.search(r"\bHOUR\b|\bHRS\b", c)]
    usd_like = [c for c in cols if re.search(r"\bCURR\b|\bCURRENCY\b|\bAMOUNT\b|\bUSD\b|\bCOST\b|\bDOLL", c)]

    # keep stable ordering / unique
    def uniq(xs):
        out=[]
        for x in xs:
            if x not in out: out.append(x)
        return out

    hrs_like = uniq(hrs_like)
    usd_like = uniq([c for c in usd_like if c not in hrs_like])

    # as a final fallback, anything that looks numeric in sample
    fallback=[]
    for c in cols:
        if c in hrs_like or c in usd_like: 
            continue
        sample = _to_num_series(df[c].head(200))
        if sample.notna().mean() > 0.6:
            fallback.append(c)

    return hrs_like, usd_like, fallback

def build_monthly_series(g, metric, unit):
    """
    Build monthly series for a group (source/subteam) using VALUE column only.
    - USD flow metrics (BCWS/BCWP/ACWP): monthly sum -> cumsum
    - USD totals (BAC/EAC/ETC): monthly last -> ffill
    - HRS metrics: monthly sum -> cumsum for BCWS/ACWP, and monthly last->ffill for ETC (hours) (works either way)
    """
    gg = g[(g["METRIC"]==metric) & (g["UNIT"]==unit)].copy()
    if gg.empty:
        return pd.Series(dtype=float)

    gg["DATE"] = pd.to_datetime(gg["DATE"], errors="coerce")
    gg = gg.dropna(subset=["DATE"])
    if gg.empty:
        return pd.Series(dtype=float)

    gg["PERIOD_ME"] = gg["DATE"].dt.to_period("M").dt.to_timestamp("M")
    v = pd.to_numeric(gg["VALUE"], errors="coerce")

    # IMPORTANT: if it's all NaN, return empty (do NOT turn into zeros)
    if v.notna().sum() == 0:
        return pd.Series(dtype=float)

    gg["VAL"] = v

    if unit=="USD" and metric in FLOW_USD:
        s = gg.groupby("PERIOD_ME")["VAL"].sum().sort_index()
        return s.cumsum()
    else:
        # totals / ETC / hours etc: last known value
        s = gg.sort_values("DATE").groupby("PERIOD_ME")["VAL"].last().sort_index()
        return s.ffill()

def value_at(s, when):
    if s is None or s.empty or pd.isna(when): 
        return np.nan
    when_me = pd.Timestamp(when).to_period("M").to_timestamp("M")
    ss = s[s.index <= when_me]
    return float(ss.iloc[-1]) if len(ss) else np.nan

def monthly_inc(g, metric, unit, when):
    gg = g[(g["METRIC"]==metric) & (g["UNIT"]==unit)].copy()
    if gg.empty or pd.isna(when): 
        return np.nan
    gg["DATE"] = pd.to_datetime(gg["DATE"], errors="coerce")
    gg = gg.dropna(subset=["DATE"])
    if gg.empty: 
        return np.nan
    gg["PERIOD_ME"] = gg["DATE"].dt.to_period("M").dt.to_timestamp("M")
    v = pd.to_numeric(gg["VALUE"], errors="coerce")
    if v.notna().sum() == 0:
        return np.nan
    inc = gg.assign(VAL=v).groupby("PERIOD_ME")["VAL"].sum().sort_index()
    when_me = pd.Timestamp(when).to_period("M").to_timestamp("M")
    return float(inc.get(when_me, np.nan))

# =========================
# 4) LOAD SELECTED FILES -> FACT
# =========================
facts=[]
load_log=[]
issues=[]

for fname in SELECT_FILES:
    p = DATA_DIR / fname
    if not p.exists():
        issues.append((fname, "FILE_NOT_FOUND"))
        continue

    try:
        sh = best_sheet(p)
        raw = pd.read_excel(p, sheet_name=sh)
        df = normalize_cols(raw)

        # Standardize COSTSET col
        if "COSTSET" not in df.columns:
            for alt in ["COST-SET","COST_SET","COST SET"]:
                a = _clean_col(alt)
                if a in df.columns:
                    df = df.rename(columns={a:"COSTSET"})
                    break

        # Standardize DATE col
        if "DATE" not in df.columns:
            for alt in ["PERIOD","MONTH","STATUS_DATE","AS_OF_DATE"]:
                a = _clean_col(alt)
                if a in df.columns:
                    df = df.rename(columns={a:"DATE"})
                    break

        # Standardize SUB_TEAM col
        if "SUB_TEAM" not in df.columns:
            for alt in ["SUBTEAM","RESP_DEPT","RESPDEPT","BE_DEPT","BEDEPT"]:
                if alt in df.columns:
                    df = df.rename(columns={alt:"SUB_TEAM"})
                    break
        if "SUB_TEAM" not in df.columns:
            df["SUB_TEAM"] = "PROGRAM"

        if "DATE" not in df.columns or "COSTSET" not in df.columns:
            issues.append((fname, f"MISSING COLS: DATE={ 'DATE' in df.columns }, COSTSET={ 'COSTSET' in df.columns }"))
            continue

        # Determine best numeric columns
        hrs_like, usd_like, fallback = pick_numeric_columns(df)

        # If there is no obvious USD column, we will still work by using HOURS as fallback for USD.
        # We'll compute VALUE per-row based on (METRIC, UNIT) and column availability.

        mapped = df["COSTSET"].apply(norm_costset)
        df["METRIC"] = mapped.apply(lambda x: x[0])
        df["UNIT"]   = mapped.apply(lambda x: x[1])

        df = df[df["METRIC"].notna()].copy()
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["DATE"])

        # Build candidate numeric series for fast row-wise selection
        # (we parse into numeric once per candidate col)
        num_cols = {}
        for c in hrs_like + usd_like + fallback:
            try:
                num_cols[c] = _to_num_series(df[c])
            except Exception:
                pass

        # Choose VALUE:
        # - If UNIT == HRS: prefer HOURS-like columns; then fallback to any numeric
        # - If UNIT == USD: prefer USD-like; then fallback to HOURS-like; then any numeric
        def choose_value(row_idx, unit):
            if unit == "HRS":
                prefs = hrs_like + usd_like + fallback
            else:
                prefs = usd_like + hrs_like + fallback
            for c in prefs:
                if c in num_cols:
                    v = num_cols[c].iat[row_idx]
                    if pd.notna(v):
                        return v, c
            return np.nan, None

        chosen_vals=[]
        chosen_from=[]
        for i in range(len(df)):
            v, c = choose_value(i, df["UNIT"].iat[i])
            chosen_vals.append(v)
            chosen_from.append(c)

        df["VALUE"] = chosen_vals
        df["VALUE_FROM"] = chosen_from

        df["SOURCE"] = fname
        df["SOURCE_SHEET"] = sh

        fact = df[["SOURCE","SOURCE_SHEET","SUB_TEAM","DATE","COSTSET","METRIC","UNIT","VALUE","VALUE_FROM"]].copy()
        facts.append(fact)

        load_log.append({
            "SOURCE": fname,
            "SHEET": sh,
            "ROWS": len(fact),
            "HRS_CANDIDATES": ", ".join(hrs_like[:3]),
            "USD_CANDIDATES": ", ".join(usd_like[:3]),
            "FALLBACK_NUM": ", ".join(fallback[:3]),
        })

    except Exception as e:
        issues.append((fname, f"LOAD_ERROR: {e}"))

cobra_fact = pd.concat(facts, ignore_index=True) if facts else pd.DataFrame()
load_log = pd.DataFrame(load_log)

print("=== LOAD LOG ===")
display(load_log)

if issues:
    print("\n=== LOAD ISSUES ===")
    for x in issues:
        print(" -", x)

if cobra_fact.empty:
    raise ValueError("cobra_fact is empty after loading selected files.")

# =========================
# 5) STATUS DATES PER SOURCE
# =========================
snap = cobra_fact.groupby("SOURCE", as_index=False).agg(SNAPSHOT_DATE=("DATE","max"))
snap["CURR_CLOSE"], snap["PREV_CLOSE"] = zip(*snap["SNAPSHOT_DATE"].apply(get_status_dates))
cobra_fact = cobra_fact.merge(snap, on="SOURCE", how="left")

# =========================
# 6) COVERAGE AUDITS (THIS IS WHAT WE USE TO FIX)
# =========================
coverage = (
    cobra_fact.groupby(["SOURCE","METRIC","UNIT"], as_index=False)
    .agg(
        rows=("VALUE","size"),
        nonnull=("VALUE", lambda s: int(pd.notna(s).sum())),
        picked_from=("VALUE_FROM", lambda s: ", ".join(pd.Series(s.dropna().unique()).head(3).tolist()))
    )
)
coverage["pct_missing"] = 1 - coverage["nonnull"]/coverage["rows"]
print("\n=== METRIC/UNIT COVERAGE (per source) ===")
display(coverage.sort_values(["SOURCE","METRIC","UNIT"]))

value_from_audit = (
    cobra_fact.groupby(["SOURCE","VALUE_FROM"], as_index=False)
    .agg(rows=("VALUE","size"), nonnull=("VALUE", lambda s: int(pd.notna(s).sum())))
    .sort_values(["SOURCE","rows"], ascending=[True, False])
)
print("\n=== VALUE_FROM AUDIT (what columns are actually used) ===")
display(value_from_audit)

# =========================
# 7) METRICS TABLES
# =========================
rows=[]
for (src, st), g in cobra_fact.groupby(["SOURCE","SUB_TEAM"]):
    snapshot_date = g["SNAPSHOT_DATE"].iloc[0]
    curr_close = g["CURR_CLOSE"].iloc[0]
    prev_close = g["PREV_CLOSE"].iloc[0]

    # USD CTD (cumulative where needed)
    s_bcws = build_monthly_series(g, "BCWS","USD")
    s_bcwp = build_monthly_series(g, "BCWP","USD")
    s_acwp = build_monthly_series(g, "ACWP","USD")

    bcws_ctd = value_at(s_bcws, snapshot_date)
    bcwp_ctd = value_at(s_bcwp, snapshot_date)
    acwp_ctd = value_at(s_acwp, snapshot_date)

    bcws_lsd = value_at(s_bcws, curr_close) - value_at(s_bcws, prev_close)
    bcwp_lsd = value_at(s_bcwp, curr_close) - value_at(s_bcwp, prev_close)
    acwp_lsd = value_at(s_acwp, curr_close) - value_at(s_acwp, prev_close)

    # BAC/EAC/ETC (USD)
    s_bac = build_monthly_series(g, "BAC","USD")
    bac_exp = value_at(s_bac, snapshot_date)
    if pd.notna(bac_exp) and bac_exp != 0:
        bac, bac_method = bac_exp, "explicit_BAC"
    else:
        bac = float(s_bcws.max()) if not s_bcws.empty else np.nan
        bac_method = "derived_max_cum_BCWS" if pd.notna(bac) else "missing"

    s_eac = build_monthly_series(g, "EAC","USD")
    s_etc = build_monthly_series(g, "ETC","USD")
    eac_exp = value_at(s_eac, snapshot_date)

    if pd.notna(eac_exp) and eac_exp != 0:
        eac, eac_method = eac_exp, "explicit_EAC"
    else:
        etc_usd = value_at(s_etc, snapshot_date)
        if pd.notna(etc_usd) and pd.notna(acwp_ctd):
            eac, eac_method = acwp_ctd + etc_usd, "derived_ACWP_plus_ETC"
        else:
            cpi_tmp = (bcwp_ctd / acwp_ctd) if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan
            if pd.notna(bac) and pd.notna(cpi_tmp) and cpi_tmp != 0:
                eac, eac_method = bac / cpi_tmp, "derived_BAC_div_CPI"
            else:
                eac, eac_method = np.nan, "missing"

    vac = bac - eac if (pd.notna(bac) and pd.notna(eac)) else np.nan

    spi_ctd = (bcwp_ctd / bcws_ctd) if (pd.notna(bcwp_ctd) and pd.notna(bcws_ctd) and bcws_ctd != 0) else np.nan
    cpi_ctd = (bcwp_ctd / acwp_ctd) if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan
    spi_lsd = (bcwp_lsd / bcws_lsd) if (pd.notna(bcwp_lsd) and pd.notna(bcws_lsd) and bcws_lsd != 0) else np.nan
    cpi_lsd = (bcwp_lsd / acwp_lsd) if (pd.notna(bcwp_lsd) and pd.notna(acwp_lsd) and acwp_lsd != 0) else np.nan
    bei_ctd = (bcwp_ctd / bac) if (pd.notna(bcwp_ctd) and pd.notna(bac) and bac != 0) else np.nan

    # HOURS demand/actual (use HRS unit rows)
    s_bcws_h = build_monthly_series(g, "BCWS","HRS")
    s_acwp_h = build_monthly_series(g, "ACWP","HRS")

    demand_hrs = (value_at(s_bcws_h, curr_close) - value_at(s_bcws_h, prev_close)) if not s_bcws_h.empty else np.nan
    actual_hrs = (value_at(s_acwp_h, curr_close) - value_at(s_acwp_h, prev_close)) if not s_acwp_h.empty else np.nan
    pct_var = ((actual_hrs - demand_hrs)/demand_hrs) if (pd.notna(actual_hrs) and pd.notna(demand_hrs) and demand_hrs != 0) else np.nan

    next_month = (pd.Timestamp(curr_close).to_period("M")+1).to_timestamp("M") if pd.notna(curr_close) else pd.NaT
    next_bcws_hrs = monthly_inc(g, "BCWS","HRS", next_month)
    next_etc_hrs  = monthly_inc(g, "ETC","HRS", next_month)

    rows.append({
        "SOURCE": src,
        "SUB_TEAM": st,
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,
        "BCWS_CTD": bcws_ctd, "BCWP_CTD": bcwp_ctd, "ACWP_CTD": acwp_ctd,
        "BCWS_LSD": bcws_lsd, "BCWP_LSD": bcwp_lsd, "ACWP_LSD": acwp_lsd,
        "SPI_CTD": spi_ctd, "CPI_CTD": cpi_ctd, "BEI_CTD": bei_ctd,
        "SPI_LSD": spi_lsd, "CPI_LSD": cpi_lsd,
        "BAC": bac, "BAC_METHOD": bac_method,
        "EAC": eac, "EAC_METHOD": eac_method,
        "VAC": vac,
        "Demand_Hours": demand_hrs,
        "Actual_Hours": actual_hrs,
        "Pct_Var": pct_var,
        "Next_Mo_BCWS_Hours": next_bcws_hrs,
        "Next_Mo_ETC_Hours": next_etc_hrs
    })

subteam_metrics = pd.DataFrame(rows)

# Program rollup = sum numerators/denominators then recompute ratios
program_metrics = (
    subteam_metrics.groupby("SOURCE", as_index=False)
    .agg(
        SNAPSHOT_DATE=("SNAPSHOT_DATE","max"),
        CURR_CLOSE=("CURR_CLOSE","max"),
        PREV_CLOSE=("PREV_CLOSE","max"),
        BCWS_CTD=("BCWS_CTD","sum"),
        BCWP_CTD=("BCWP_CTD","sum"),
        ACWP_CTD=("ACWP_CTD","sum"),
        BCWS_LSD=("BCWS_LSD","sum"),
        BCWP_LSD=("BCWP_LSD","sum"),
        ACWP_LSD=("ACWP_LSD","sum"),
        BAC=("BAC","sum"),
        EAC=("EAC","sum"),
        VAC=("VAC","sum"),
        Demand_Hours=("Demand_Hours","sum"),
        Actual_Hours=("Actual_Hours","sum"),
        Next_Mo_BCWS_Hours=("Next_Mo_BCWS_Hours","sum"),
        Next_Mo_ETC_Hours=("Next_Mo_ETC_Hours","sum"),
    )
)
program_metrics["SPI_CTD"] = program_metrics["BCWP_CTD"] / program_metrics["BCWS_CTD"].replace(0,np.nan)
program_metrics["CPI_CTD"] = program_metrics["BCWP_CTD"] / program_metrics["ACWP_CTD"].replace(0,np.nan)
program_metrics["BEI_CTD"] = program_metrics["BCWP_CTD"] / program_metrics["BAC"].replace(0,np.nan)
program_metrics["SPI_LSD"] = program_metrics["BCWP_LSD"] / program_metrics["BCWS_LSD"].replace(0,np.nan)
program_metrics["CPI_LSD"] = program_metrics["BCWP_LSD"] / program_metrics["ACWP_LSD"].replace(0,np.nan)
program_metrics["Pct_Var"] = (program_metrics["Actual_Hours"]-program_metrics["Demand_Hours"]) / program_metrics["Demand_Hours"].replace(0,np.nan)

missing_summary = (
    subteam_metrics.groupby("SOURCE", as_index=False)
    .agg(
        subteams=("SUB_TEAM","nunique"),
        pct_BCWS_CTD_missing=("BCWS_CTD", lambda s: float(s.isna().mean())),
        pct_BCWP_CTD_missing=("BCWP_CTD", lambda s: float(s.isna().mean())),
        pct_ACWP_CTD_missing=("ACWP_CTD", lambda s: float(s.isna().mean())),
        pct_BAC_missing=("BAC", lambda s: float(s.isna().mean())),
        pct_EAC_missing=("EAC", lambda s: float(s.isna().mean())),
    )
    .sort_values("pct_BCWS_CTD_missing", ascending=False)
)

print("\n=== PROGRAM METRICS (preview) ===")
display(program_metrics)

print("\n=== SUBTEAM METRICS (preview) ===")
display(subteam_metrics.head(40))

print("\n=== MISSING SUMMARY (by source) ===")
display(missing_summary)

print("\n✅ Outputs in memory: cobra_fact, program_metrics, subteam_metrics, coverage, value_from_audit, missing_summary")