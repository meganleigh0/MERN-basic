import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime

# =========================
# CONFIG
# =========================
DATA_DIR = Path("data")
FILE_PREFIX = "cobra"
SHEET_KEYWORDS = ["tbl", "weekly", "extract", "cap"]

ACCOUNTING_CLOSINGS = {
    (2025, 1): 26, (2025, 2): 23, (2025, 3): 30, (2025, 4): 27, (2025, 5): 25, (2025, 6): 29,
    (2025, 7): 27, (2025, 8): 24, (2025, 9): 28, (2025,10): 26, (2025,11): 23, (2025,12): 31,
    (2026, 1): 4,  (2026, 2): 1,  (2026, 3): 1,  (2026, 4): 5,  (2026, 5): 3,  (2026, 6): 7,
    (2026, 7): 5,  (2026, 8): 2,  (2026, 9): 6,  (2026,10): 4,  (2026,11): 1,  (2026,12): 6,
}

def build_close_dates(d):
    out=[]
    for (y,m),day in d.items():
        try: out.append(pd.Timestamp(datetime(y,m,day)))
        except: pass
    return pd.DatetimeIndex(sorted(set(out)))

ACCT_CLOSE_DATES = build_close_dates(ACCOUNTING_CLOSINGS)

def get_status_dates(max_date):
    if pd.isna(max_date): return (pd.NaT, pd.NaT)
    closes = ACCT_CLOSE_DATES[ACCT_CLOSE_DATES <= max_date]
    if len(closes) >= 2: return (closes[-1], closes[-2])
    if len(closes) == 1: return (closes[-1], closes[-1])
    me = pd.Timestamp(max_date).to_period("M").to_timestamp("M")
    prev_me = me - pd.offsets.MonthEnd(1)
    return (me, prev_me)

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

# -------------------------
# COSTSET -> METRIC + UNIT
# -------------------------
def norm_costset(raw):
    s = str(raw).strip().upper()
    s = re.sub(r"[^A-Z0-9_ ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # hours flavors
    if re.search(r"\bBCWS\b.*\b(HRS|HOURS)\b|\bBCWS_HRS\b", s): return ("BCWS", "HRS")
    if re.search(r"\bACWP\b.*\b(HRS|HOURS)\b|\bACWP_HRS\b|\bACHP_HRS\b", s): return ("ACWP", "HRS")
    if re.search(r"\bETC\b.*\b(HRS|HOURS)\b|\bETC_HRS\b", s): return ("ETC", "HRS")

    # explicit totals
    if re.search(r"\bBAC\b|BUDGET AT COMPLETION", s): return ("BAC", "USD")
    if re.search(r"\bEAC\b|ESTIMATE AT COMPLETION", s): return ("EAC", "USD")
    if re.search(r"\bETC\b|ESTIMATE TO COMPLETE|REMAINING|TO GO", s): return ("ETC", "USD")

    # planned / earned / actual dollars
    if re.search(r"\bBCWS\b|PLANNED VALUE|\bPLAN\b|\bSCHEDULE\b", s): return ("BCWS", "USD")
    if s == "BUDGET": return ("BCWS", "USD")  # in your extracts "Budget" behaves like PV/BCWS, not BAC
    if re.search(r"\bBCWP\b|EARNED|PROGRESS|PERFORM", s): return ("BCWP", "USD")
    if re.search(r"\bACWP\b|ACTUAL COST|WEEKLY ACTUALS|ACWP_WKL", s): return ("ACWP", "USD")

    return (None, None)

# -------------------------
# Build series correctly
# -------------------------
FLOW = {"BCWS","BCWP","ACWP"}   # dollars flow metrics in your extracts
STOCK = {"BAC","EAC","ETC"}     # point-in-time (we will treat USD ETC as stock; hours ETC is handled via unit)

def build_series(df, metric, unit):
    g = df[(df["METRIC"]==metric) & (df["UNIT"]==unit)].copy()
    if g.empty:
        return pd.Series(dtype=float)

    g["DATE"] = pd.to_datetime(g["DATE"], errors="coerce")
    g = g.dropna(subset=["DATE"])
    if g.empty:
        return pd.Series(dtype=float)

    g["PERIOD_ME"] = g["DATE"].dt.to_period("M").dt.to_timestamp("M")

    if unit == "HRS":
        val = pd.to_numeric(g["VALUE_HRS"], errors="coerce")
    else:
        val = pd.to_numeric(g["VALUE_USD"], errors="coerce")

    g["VAL"] = val.fillna(0.0)

    if (metric in FLOW) and (unit == "USD"):
        s = g.groupby("PERIOD_ME")["VAL"].sum().sort_index()
        return s.cumsum()
    else:
        # STOCK or hours: last+ffill (better behaved for ETC_HRS / EAC etc across exports)
        s = g.sort_values("DATE").groupby("PERIOD_ME")["VAL"].last().sort_index()
        return s.ffill()

def value_at(s, when):
    if s is None or s.empty or pd.isna(when): return np.nan
    when_me = pd.Timestamp(when).to_period("M").to_timestamp("M")
    ss = s[s.index <= when_me]
    return float(ss.iloc[-1]) if len(ss) else np.nan

def monthly_inc(df, metric, unit, when):
    g = df[(df["METRIC"]==metric) & (df["UNIT"]==unit)].copy()
    if g.empty or pd.isna(when): return np.nan
    g["DATE"] = pd.to_datetime(g["DATE"], errors="coerce")
    g = g.dropna(subset=["DATE"])
    if g.empty: return np.nan
    g["PERIOD_ME"] = g["DATE"].dt.to_period("M").dt.to_timestamp("M")
    val = pd.to_numeric(g["VALUE_HRS"] if unit=="HRS" else g["VALUE_USD"], errors="coerce").fillna(0.0)
    g["VAL"]=val
    inc = g.groupby("PERIOD_ME")["VAL"].sum().sort_index()
    when_me = pd.Timestamp(when).to_period("M").to_timestamp("M")
    return float(inc.get(when_me, np.nan))

# =========================
# LOAD FILES -> FACT TABLE WITH USD + HRS
# =========================
loaded=[]
pipeline_issues=[]

for p in DATA_DIR.glob("*.xlsx"):
    if not p.name.lower().startswith(FILE_PREFIX.lower()):
        continue
    try:
        sh = best_sheet(p)
        df = pd.read_excel(p, sheet_name=sh)
        df = normalize_cols(df)

        # harmonize expected cols
        for old,new in [("COST-SET","COSTSET"), ("COST_SET","COSTSET"), ("COSTSET","COSTSET")]:
            if old in df.columns and "COSTSET" not in df.columns:
                df = df.rename(columns={old:new})

        if "SUB_TEAM" not in df.columns:
            for alt in ["SUBTEAM","RESP_DEPT","RESPDEPT","BE_DEPT","BEDEPT"]:
                if alt in df.columns:
                    df = df.rename(columns={alt:"SUB_TEAM"})
                    break
        if "SUB_TEAM" not in df.columns:
            df["SUB_TEAM"] = "PROGRAM"

        if "DATE" not in df.columns or "COSTSET" not in df.columns:
            pipeline_issues.append((p.name, sh, "Missing DATE and/or COSTSET"))
            continue

        # Identify candidate USD and HRS columns
        # (Your screenshot shows HOURS and Currency; this handles lots of variants.)
        hrs_candidates = [c for c in df.columns if re.fullmatch(r"HOURS", c) or re.search(r"\bHRS\b|\bHOURS\b", c)]
        usd_candidates = [c for c in df.columns if re.search(r"CURRENCY|AMOUNT|COST|DOLLAR|USD|VALUE", c)]

        # pick best
        HRS_COL = "HOURS" if "HOURS" in df.columns else (hrs_candidates[0] if hrs_candidates else None)
        USD_COL = None
        # avoid picking HOURS again as USD
        usd_candidates = [c for c in usd_candidates if c != HRS_COL]
        if "CURRENCY" in df.columns: USD_COL = "CURRENCY"
        elif "AMOUNT" in df.columns: USD_COL = "AMOUNT"
        elif usd_candidates: USD_COL = usd_candidates[0]

        df["VALUE_HRS"] = pd.to_numeric(df[HRS_COL], errors="coerce") if HRS_COL else np.nan
        df["VALUE_USD"] = pd.to_numeric(df[USD_COL], errors="coerce") if USD_COL else np.nan

        # map metric + unit
        mapped = df["COSTSET"].apply(norm_costset)
        df["METRIC"] = mapped.apply(lambda x: x[0])
        df["UNIT"]   = mapped.apply(lambda x: x[1])

        df = df[df["METRIC"].notna()].copy()
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
        df = df.dropna(subset=["DATE"])

        df["SOURCE"] = p.name
        df["SOURCE_SHEET"] = sh

        fact = df[["SOURCE","SOURCE_SHEET","SUB_TEAM","DATE","COSTSET","METRIC","UNIT","VALUE_USD","VALUE_HRS"]].copy()
        loaded.append(fact)

        print(f"✅ Loaded {p.name} → {sh} | rows={len(fact):,} | USD_COL={USD_COL} | HRS_COL={HRS_COL}")

    except Exception as e:
        pipeline_issues.append((p.name, None, f"Load error: {e}"))

cobra_fact = pd.concat(loaded, ignore_index=True) if loaded else pd.DataFrame()

if cobra_fact.empty:
    raise ValueError("cobra_fact is empty; check pipeline_issues")

# =========================
# SNAPSHOT + CLOSE DATES
# =========================
snap = cobra_fact.groupby("SOURCE", as_index=False).agg(SNAPSHOT_DATE=("DATE","max"))
snap["CURR_CLOSE"], snap["PREV_CLOSE"] = zip(*snap["SNAPSHOT_DATE"].apply(get_status_dates))
cobra_fact = cobra_fact.merge(snap, on="SOURCE", how="left")

# =========================
# AUDITS
# =========================
label_audit = (
    cobra_fact.groupby(["SOURCE","METRIC","UNIT"], as_index=False).size()
    .pivot_table(index="SOURCE", columns=["METRIC","UNIT"], values="size", fill_value=0)
)
label_audit.columns = [f"{a}_{b}" for a,b in label_audit.columns]
label_audit = label_audit.reset_index()

# =========================
# COMPUTE SUBTEAM METRICS
# =========================
out=[]
for (src, st), g in cobra_fact.groupby(["SOURCE","SUB_TEAM"]):
    snapshot_date = g["SNAPSHOT_DATE"].iloc[0]
    curr_close = g["CURR_CLOSE"].iloc[0]
    prev_close = g["PREV_CLOSE"].iloc[0]

    # build USD cumulative series
    s_bcws = build_series(g, "BCWS","USD")
    s_bcwp = build_series(g, "BCWP","USD")
    s_acwp = build_series(g, "ACWP","USD")

    # CTD
    bcws_ctd = value_at(s_bcws, snapshot_date)
    bcwp_ctd = value_at(s_bcwp, snapshot_date)
    acwp_ctd = value_at(s_acwp, snapshot_date)

    # LSD via close delta on cumulative
    bcws_lsd = value_at(s_bcws, curr_close) - value_at(s_bcws, prev_close)
    bcwp_lsd = value_at(s_bcwp, curr_close) - value_at(s_bcwp, prev_close)
    acwp_lsd = value_at(s_acwp, curr_close) - value_at(s_acwp, prev_close)

    # BAC (USD): explicit BAC stock if present; else derive as max cumulative BCWS
    s_bac = build_series(g, "BAC","USD")
    bac_exp = value_at(s_bac, snapshot_date)
    if pd.notna(bac_exp) and bac_exp != 0:
        bac = bac_exp
        bac_method = "explicit_BAC"
    else:
        bac = float(s_bcws.max()) if not s_bcws.empty else np.nan
        bac_method = "derived_max_cum_BCWS" if pd.notna(bac) and bac != 0 else "missing"

    # EAC (USD): explicit EAC else ACWP+ETC(USD) else BAC/CPI
    s_eac = build_series(g, "EAC","USD")
    s_etc = build_series(g, "ETC","USD")
    eac_exp = value_at(s_eac, snapshot_date)
    if pd.notna(eac_exp) and eac_exp != 0:
        eac = eac_exp
        eac_method = "explicit_EAC"
    else:
        etc_usd = value_at(s_etc, snapshot_date)
        if pd.notna(etc_usd) and etc_usd != 0 and pd.notna(acwp_ctd):
            eac = acwp_ctd + etc_usd
            eac_method = "derived_ACWP_plus_ETC"
        else:
            cpi = (bcwp_ctd / acwp_ctd) if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan
            if pd.notna(bac) and bac != 0 and pd.notna(cpi) and cpi != 0:
                eac = bac / cpi
                eac_method = "derived_BAC_div_CPI"
            else:
                eac = np.nan
                eac_method = "missing"

    vac = bac - eac if (pd.notna(bac) and pd.notna(eac)) else np.nan

    spi_ctd = (bcwp_ctd / bcws_ctd) if (pd.notna(bcwp_ctd) and pd.notna(bcws_ctd) and bcws_ctd != 0) else np.nan
    cpi_ctd = (bcwp_ctd / acwp_ctd) if (pd.notna(bcwp_ctd) and pd.notna(acwp_ctd) and acwp_ctd != 0) else np.nan
    spi_lsd = (bcwp_lsd / bcws_lsd) if (pd.notna(bcwp_lsd) and pd.notna(bcws_lsd) and bcws_lsd != 0) else np.nan
    cpi_lsd = (bcwp_lsd / acwp_lsd) if (pd.notna(bcwp_lsd) and pd.notna(acwp_lsd) and acwp_lsd != 0) else np.nan
    bei_ctd = (bcwp_ctd / bac) if (pd.notna(bcwp_ctd) and pd.notna(bac) and bac != 0) else np.nan

    # HOURS: demand/actual via BCWS(HRS), ACWP(HRS) close deltas
    s_bcws_h = build_series(g, "BCWS","HRS")
    s_acwp_h = build_series(g, "ACWP","HRS")
    demand_hrs = value_at(s_bcws_h, curr_close) - value_at(s_bcws_h, prev_close) if not s_bcws_h.empty else np.nan
    actual_hrs = value_at(s_acwp_h, curr_close) - value_at(s_acwp_h, prev_close) if not s_acwp_h.empty else np.nan
    pct_var = ((actual_hrs - demand_hrs)/demand_hrs) if (pd.notna(actual_hrs) and pd.notna(demand_hrs) and demand_hrs != 0) else np.nan

    # Next month incremental hours (BCWS/ETC hours)
    next_month = (pd.Timestamp(curr_close).to_period("M")+1).to_timestamp("M") if pd.notna(curr_close) else pd.NaT
    next_bcws_hrs = monthly_inc(g, "BCWS","HRS", next_month)
    next_etc_hrs  = monthly_inc(g, "ETC","HRS", next_month)

    out.append({
        "SOURCE": src,
        "SUB_TEAM": st,
        "SNAPSHOT_DATE": snapshot_date,
        "CURR_CLOSE": curr_close,
        "PREV_CLOSE": prev_close,

        "BCWS_CTD": bcws_ctd, "BCWP_CTD": bcwp_ctd, "ACWP_CTD": acwp_ctd,
        "SPI_CTD": spi_ctd, "CPI_CTD": cpi_ctd, "BEI_CTD": bei_ctd,

        "BCWS_LSD": bcws_lsd, "BCWP_LSD": bcwp_lsd, "ACWP_LSD": acwp_lsd,
        "SPI_LSD": spi_lsd, "CPI_LSD": cpi_lsd,

        "BAC": bac, "BAC_METHOD": bac_method,
        "EAC": eac, "EAC_METHOD": eac_method,
        "VAC_CTD": vac,

        "Demand_Hours": demand_hrs,
        "Actual_Hours": actual_hrs,
        "Pct_Var": pct_var,
        "Next_Mo_BCWS_Hours": next_bcws_hrs,
        "Next_Mo_ETC_Hours": next_etc_hrs
    })

subteam_metrics = pd.DataFrame(out)

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
program_metrics["SPI_CTD"] = program_metrics["BCWP_CTD"] / program_metrics["BCWS_CTD"].replace(0,np.nan)
program_metrics["CPI_CTD"] = program_metrics["BCWP_CTD"] / program_metrics["ACWP_CTD"].replace(0,np.nan)
program_metrics["BEI_CTD"] = program_metrics["BCWP_CTD"] / program_metrics["BAC"].replace(0,np.nan)
program_metrics["SPI_LSD"] = program_metrics["BCWP_LSD"] / program_metrics["BCWS_LSD"].replace(0,np.nan)
program_metrics["CPI_LSD"] = program_metrics["BCWP_LSD"] / program_metrics["ACWP_LSD"].replace(0,np.nan)
program_metrics["Pct_Var"] = (program_metrics["Actual_Hours"]-program_metrics["Demand_Hours"]) / program_metrics["Demand_Hours"].replace(0,np.nan)

subteam_cost = subteam_metrics[["SOURCE","SUB_TEAM","BAC","BAC_METHOD","EAC","EAC_METHOD","VAC_CTD"]].copy()
hours_metrics = subteam_metrics[["SOURCE","SUB_TEAM","SNAPSHOT_DATE","CURR_CLOSE","Demand_Hours","Actual_Hours","Pct_Var","Next_Mo_BCWS_Hours","Next_Mo_ETC_Hours"]].copy()

coverage_audit = (
    subteam_metrics.assign(
        BCWS_missing=lambda d: d["BCWS_CTD"].isna() | (d["BCWS_CTD"]==0),
        ACWP_missing=lambda d: d["ACWP_CTD"].isna() | (d["ACWP_CTD"]==0),
        BAC_missing=lambda d: d["BAC"].isna() | (d["BAC"]==0),
        EAC_missing=lambda d: d["EAC"].isna() | (d["EAC"]==0),
        HRS_missing=lambda d: d["Demand_Hours"].isna() | (d["Demand_Hours"]==0)
    )
    .groupby("SOURCE", as_index=False)
    .agg(
        rows=("SUB_TEAM","count"),
        pct_BCWS_missing=("BCWS_missing","mean"),
        pct_ACWP_missing=("ACWP_missing","mean"),
        pct_BAC_missing=("BAC_missing","mean"),
        pct_EAC_missing=("EAC_missing","mean"),
        pct_HRS_missing=("HRS_missing","mean"),
    )
    .sort_values(["pct_BCWS_missing","pct_ACWP_missing","pct_HRS_missing","pct_BAC_missing","pct_EAC_missing"], ascending=False)
)

print("\n✅ Built tables: cobra_fact, program_metrics, subteam_metrics, subteam_cost, hours_metrics, label_audit, coverage_audit")
print("\n--- Top coverage issues ---")
print(coverage_audit.head(15))

# =========================
# EXPORTS FOR POWER BI
# =========================
# 1) fact table: parquet (no Excel row limit)
fact_path = DATA_DIR / "cobra_fact.parquet"
try:
    cobra_fact.to_parquet(fact_path, index=False)
    print(f"\n✅ Wrote fact table: {fact_path}")
except Exception as e:
    # fallback: CSV
    fact_csv = DATA_DIR / "cobra_fact.csv"
    cobra_fact.to_csv(fact_csv, index=False)
    print(f"\n⚠️ Parquet failed ({e}); wrote CSV instead: {fact_csv}")

# 2) summaries: Excel
def _sanitize(df):
    df = df.replace([np.inf,-np.inf], np.nan)
    for c in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[c]):
            df[c] = df[c].dt.tz_convert(None)
    return df

out_xlsx = DATA_DIR / "cobra_evms_tables.xlsx"
engine = "xlsxwriter"
try:
    import xlsxwriter  # noqa
except Exception:
    engine = "openpyxl"

with pd.ExcelWriter(out_xlsx, engine=engine) as w:
    _sanitize(program_metrics).to_excel(w, sheet_name="program_metrics", index=False)
    _sanitize(subteam_metrics).to_excel(w, sheet_name="subteam_metrics", index=False)
    _sanitize(subteam_cost).to_excel(w, sheet_name="subteam_cost", index=False)
    _sanitize(hours_metrics).to_excel(w, sheet_name="hours_metrics", index=False)
    _sanitize(label_audit).to_excel(w, sheet_name="label_audit", index=False)
    _sanitize(coverage_audit).to_excel(w, sheet_name="coverage_audit", index=False)
print(f"✅ Wrote summaries: {out_xlsx}")

if pipeline_issues:
    print("\n--- pipeline_issues (first 25) ---")
    for x in pipeline_issues[:25]:
        print(" -", x)