# ============================================================
# EVMS -> Power BI Export Pipeline (History + Latest Status Tables)
#   - Reads Cobra exports
#   - Builds star-schema-friendly tables for Power BI
#   - Adds "Latest status" tables so Cards don't need DAX
# ============================================================

import os, re
import numpy as np
import pandas as pd
from pathlib import Path

# -----------------------------
# CONFIG — UPDATE THESE
# -----------------------------
PROGRAM_CONFIG = [
    {"Program": "Abrams_STS_2022",       "CobraPath": r"data/Cobra-Abrams STS 2022.xlsx"},
    {"Program": "Abrams_STS",            "CobraPath": r"data/Cobra-Abrams STS.xlsx"},
    {"Program": "ARV",                   "CobraPath": r"data/Cobra-ARV.xlsx"},
    {"Program": "ARV30",                 "CobraPath": r"data/Cobra-ARV30.xlsx"},
    {"Program": "Stryker_Bulgaria_150",  "CobraPath": r"data/Cobra-Stryker Bulgaria 150.xlsx"},
    {"Program": "XM30",                  "CobraPath": r"data/Cobra-XM30.xlsx"},
]

# If your Cobra export has a known sheet, set it. Otherwise leave None (auto-detect best sheet).
COBRA_SHEET_NAME = None  # e.g., "tbl_Weekly Extract"

# Output folder (local)
OUT_ROOT = Path("outputs")
MODEL_DIR = OUT_ROOT / "model"
ACTIONS_DIR = OUT_ROOT / "actions"
LOG_DIR = OUT_ROOT / "logs"
for p in [MODEL_DIR, ACTIONS_DIR, LOG_DIR]:
    p.mkdir(parents=True, exist_ok=True)

# Parquet optional (Power BI supports Parquet)
USE_PARQUET = True

# -----------------------------
# Helpers
# -----------------------------
def norm(s: str) -> str:
    s = str(s).strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s

def pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {norm(c): c for c in df.columns}
    for cand in candidates:
        k = norm(cand)
        if k in cols:
            return cols[k]
    for cand in candidates:
        k = norm(cand)
        for kn, orig in cols.items():
            if k in kn:
                return orig
    return None

def to_float(s):
    return pd.to_numeric(s, errors="coerce")

def normalize_costset(v) -> str:
    s = "" if pd.isna(v) else str(v)
    s = s.upper().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^A-Z0-9 ]+", "", s)
    return s

def infer_metric(costset_norm: str) -> str | None:
    s = costset_norm.replace(" ", "")
    # core EV
    if "BCWS" in s: return "BCWS"
    if "BCWP" in s: return "BCWP"
    if "ACWP" in s: return "ACWP"
    if "ETC"  in s: return "ETC"
    if re.fullmatch(r".*BAC.*", s): return "BAC"
    if re.fullmatch(r".*EAC.*", s): return "EAC"
    # manpower
    if "DEMAND" in s and "HOUR" in s: return "DEMAND_HOURS"
    if ("ACTUAL" in s and "HOUR" in s) or s in {"ACTUALHOURS","ACTHOURS"}: return "ACTUAL_HOURS"
    if "NEXT" in s and "BCWS" in s and "HOUR" in s: return "NEXT_MO_BCWS_HOURS"
    if "NEXT" in s and "ETC"  in s and "HOUR" in s: return "NEXT_MO_ETC_HOURS"
    return None

def safe_date(x):
    # keep as Timestamp; normalize time for consistent joins
    d = pd.to_datetime(x, errors="coerce")
    if pd.isna(d): return pd.NaT
    return pd.Timestamp(d).normalize()

def make_period_key(period_end: pd.Series) -> pd.Series:
    # integer yyyymmdd for stable joins
    d = pd.to_datetime(period_end, errors="coerce")
    return (d.dt.year * 10000 + d.dt.month * 100 + d.dt.day).astype("Int64")

def write_table(df: pd.DataFrame, path_no_ext: Path):
    # Writes parquet (preferred) and CSV mirror (helpful for troubleshooting / if parquet issues)
    if USE_PARQUET:
        try:
            df.to_parquet(str(path_no_ext.with_suffix(".parquet")), index=False)
        except Exception as e:
            print(f"[WARN] Parquet failed for {path_no_ext.name}: {e}. Writing CSV only.")
    df.to_csv(str(path_no_ext.with_suffix(".csv")), index=False)

# -----------------------------
# Load Cobra and pivot wide per period/subteam
# -----------------------------
def load_cobra_long(xlsx_path: str, preferred_sheet: str | None) -> pd.DataFrame:
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Missing file: {xlsx_path}")

    sheets = pd.read_excel(xlsx_path, sheet_name=None)
    if not sheets:
        raise ValueError("No sheets found.")

    if preferred_sheet and preferred_sheet in sheets:
        df = sheets[preferred_sheet].copy()
    else:
        best_name, best_score = None, -1
        for name, sdf in sheets.items():
            if sdf is None or sdf.empty:
                continue
            cols = [norm(c) for c in sdf.columns]
            score = 0
            if any(("COST" in c and "SET" in c) for c in cols): score += 3
            if any(("HOUR" in c or "HRS" in c) for c in cols): score += 3
            if any(("SUB" in c and "TEAM" in c) for c in cols): score += 2
            if any(("DATE" in c or "PERIOD" in c or "MONTH" in c) for c in cols): score += 1
            if score > best_score:
                best_score = score
                best_name = name
        df = sheets[best_name].copy()

    cost_col = pick_col(df, ["COSTSET","COST-SET","COST SET"])
    hrs_col  = pick_col(df, ["HOURS","HRS","HOUR","VALUE","AMOUNT"])
    sub_col  = pick_col(df, ["SUB_TEAM","SUBTEAM","SUB TEAM","TEAM","SUBTEAM NAME"])
    date_col = pick_col(df, ["DATE","STATUS DATE","PERIOD END","PERIOD","MONTH","MONTHENDDATE"])

    if cost_col is None or hrs_col is None:
        raise ValueError(f"Could not find COSTSET/HOURS columns in {xlsx_path}")

    out = pd.DataFrame({
        "COSTSET_RAW": df[cost_col],
        "HOURS": to_float(df[hrs_col]).fillna(0.0),
        "Subteam": (df[sub_col].astype(str).str.strip() if sub_col else "ALL"),
        "PeriodEndDate": (pd.to_datetime(df[date_col], errors="coerce") if date_col else pd.NaT),
    })
    out["Subteam"] = out["Subteam"].replace({"": "UNSPECIFIED", "nan": "UNSPECIFIED"}).fillna("UNSPECIFIED")
    out["PeriodEndDate"] = out["PeriodEndDate"].map(safe_date)

    out["COSTSET_NORM"] = out["COSTSET_RAW"].map(normalize_costset)
    out["Metric"] = out["COSTSET_NORM"].map(infer_metric)
    out = out[pd.notna(out["Metric"])].copy()

    return out

def build_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    has_dates = long_df["PeriodEndDate"].notna().any()
    df = long_df.copy()

    if has_dates:
        g = df.groupby(["PeriodEndDate","Subteam","Metric"], as_index=False)["HOURS"].sum()
        wide = (
            g.pivot_table(index=["PeriodEndDate","Subteam"], columns="Metric", values="HOURS", aggfunc="sum")
             .fillna(0.0)
             .reset_index()
             .sort_values(["Subteam","PeriodEndDate"])
             .reset_index(drop=True)
        )
    else:
        g = df.groupby(["Subteam","Metric"], as_index=False)["HOURS"].sum()
        wide = (
            g.pivot_table(index=["Subteam"], columns="Metric", values="HOURS", aggfunc="sum")
             .fillna(0.0)
             .reset_index()
             .sort_values(["Subteam"])
             .reset_index(drop=True)
        )
        wide["PeriodEndDate"] = pd.NaT

    # ensure all expected columns exist
    for c in ["BCWS","BCWP","ACWP","ETC","BAC","EAC","DEMAND_HOURS","ACTUAL_HOURS","NEXT_MO_BCWS_HOURS","NEXT_MO_ETC_HOURS"]:
        if c not in wide.columns:
            wide[c] = 0.0

    return wide

# -----------------------------
# Compute metrics at Subteam + Period level, plus Program rollups
# -----------------------------
def compute_metrics_fact(wide: pd.DataFrame) -> pd.DataFrame:
    w = wide.copy().sort_values(["Subteam","PeriodEndDate"])

    # cumulative by subteam (CTD)
    w["BCWS_CUM"] = w.groupby("Subteam")["BCWS"].cumsum()
    w["BCWP_CUM"] = w.groupby("Subteam")["BCWP"].cumsum()
    w["ACWP_CUM"] = w.groupby("Subteam")["ACWP"].cumsum()

    w["SPI_LSD"] = np.where(w["BCWS"]>0, w["BCWP"]/w["BCWS"], np.nan)
    w["CPI_LSD"] = np.where(w["ACWP"]>0, w["BCWP"]/w["ACWP"], np.nan)

    w["SPI_CTD"] = np.where(w["BCWS_CUM"]>0, w["BCWP_CUM"]/w["BCWS_CUM"], np.nan)
    w["CPI_CTD"] = np.where(w["ACWP_CUM"]>0, w["BCWP_CUM"]/w["ACWP_CUM"], np.nan)

    # labor calc (BAC/EAC/VAC)
    bac_calc = np.where(w["BAC"]>0, w["BAC"], w["BCWS_CUM"])  # proxy if BAC missing
    cpi_for_eac = np.where(w["ACWP_CUM"]>0, w["BCWP_CUM"]/w["ACWP_CUM"], np.nan)
    eac_calc = np.where(
        w["EAC"]>0, w["EAC"],
        np.where(
            w["ETC"]>0, w["ACWP_CUM"] + w["ETC"],
            np.where((bac_calc>0) & np.isfinite(cpi_for_eac) & (cpi_for_eac>0),
                     w["ACWP_CUM"] + (bac_calc - w["BCWP_CUM"]) / cpi_for_eac,
                     np.nan)
        )
    )
    w["BAC_CALC"] = bac_calc
    w["EAC_CALC"] = eac_calc
    w["VAC"] = w["BAC_CALC"] - w["EAC_CALC"]
    w["VAC_RATIO"] = np.where(w["BAC_CALC"]>0, w["VAC"]/w["BAC_CALC"], np.nan)

    # manpower
    w["ManpowerPctVar"] = np.where(w["DEMAND_HOURS"]>0, w["ACTUAL_HOURS"]/w["DEMAND_HOURS"], np.nan)

    return w

def compute_program_monthly(fact: pd.DataFrame) -> pd.DataFrame:
    # aggregate by Program + PeriodEndDate (after Program is added)
    g = fact.groupby(["Program","PeriodEndDate"], as_index=False)[
        ["BCWS","BCWP","ACWP","ETC","BAC","EAC","DEMAND_HOURS","ACTUAL_HOURS","NEXT_MO_BCWS_HOURS","NEXT_MO_ETC_HOURS"]
    ].sum()

    g = g.sort_values(["Program","PeriodEndDate"]).reset_index(drop=True)

    g["BCWS_CUM"] = g.groupby("Program")["BCWS"].cumsum()
    g["BCWP_CUM"] = g.groupby("Program")["BCWP"].cumsum()
    g["ACWP_CUM"] = g.groupby("Program")["ACWP"].cumsum()

    g["SPI_LSD"] = np.where(g["BCWS"]>0, g["BCWP"]/g["BCWS"], np.nan)
    g["CPI_LSD"] = np.where(g["ACWP"]>0, g["BCWP"]/g["ACWP"], np.nan)
    g["SPI_CTD"] = np.where(g["BCWS_CUM"]>0, g["BCWP_CUM"]/g["BCWS_CUM"], np.nan)
    g["CPI_CTD"] = np.where(g["ACWP_CUM"]>0, g["BCWP_CUM"]/g["ACWP_CUM"], np.nan)

    bac_calc = np.where(g["BAC"]>0, g["BAC"], g["BCWS_CUM"])
    cpi_for_eac = np.where(g["ACWP_CUM"]>0, g["BCWP_CUM"]/g["ACWP_CUM"], np.nan)
    eac_calc = np.where(
        g["EAC"]>0, g["EAC"],
        np.where(
            g["ETC"]>0, g["ACWP_CUM"] + g["ETC"],
            np.where((bac_calc>0) & np.isfinite(cpi_for_eac) & (cpi_for_eac>0),
                     g["ACWP_CUM"] + (bac_calc - g["BCWP_CUM"]) / cpi_for_eac,
                     np.nan)
        )
    )
    g["BAC_CALC"] = bac_calc
    g["EAC_CALC"] = eac_calc
    g["VAC"] = g["BAC_CALC"] - g["EAC_CALC"]
    g["VAC_RATIO"] = np.where(g["BAC_CALC"]>0, g["VAC"]/g["BAC_CALC"], np.nan)

    g["ManpowerPctVar"] = np.where(g["DEMAND_HOURS"]>0, g["ACTUAL_HOURS"]/g["DEMAND_HOURS"], np.nan)

    return g

# -----------------------------
# Build all programs -> one model
# -----------------------------
all_fact = []
run_log = []

for cfg in PROGRAM_CONFIG:
    program = cfg["Program"]
    path = cfg["CobraPath"]
    print(f"Processing {program}...")
    try:
        long_df = load_cobra_long(path, COBRA_SHEET_NAME)
        wide = build_wide(long_df)
        fact = compute_metrics_fact(wide)
        fact.insert(0, "Program", program)

        # Keys for Power BI joins
        fact["PeriodKey"] = make_period_key(fact["PeriodEndDate"])
        fact["ActionJoinKey"] = fact["Program"].astype(str) + "|" + fact["Subteam"].astype(str) + "|" + fact["PeriodKey"].astype(str)

        all_fact.append(fact)

        run_log.append({"Program": program, "Status": "OK", "Rows": len(fact), "MinDate": fact["PeriodEndDate"].min(), "MaxDate": fact["PeriodEndDate"].max()})
    except Exception as e:
        run_log.append({"Program": program, "Status": f"ERROR: {e}", "Rows": 0, "MinDate": None, "MaxDate": None})
        print(f"  ERROR: {e}")

fact_all = pd.concat(all_fact, ignore_index=True) if all_fact else pd.DataFrame()

# -----------------------------
# Build ProgramMonthly (rollup) and latest tables
# -----------------------------
if fact_all.empty:
    raise RuntimeError("No data produced. Check Cobra paths / sheet detection.")

program_monthly = compute_program_monthly(fact_all)

program_monthly["PeriodKey"] = make_period_key(program_monthly["PeriodEndDate"])
program_monthly["ActionJoinKey"] = program_monthly["Program"].astype(str) + "|ALL|" + program_monthly["PeriodKey"].astype(str)

# Latest per Program (NO DAX needed in Power BI cards)
idx_prog_latest = program_monthly.groupby("Program")["PeriodEndDate"].idxmax()
program_latest = program_monthly.loc[idx_prog_latest].copy().reset_index(drop=True)
program_latest = program_latest.rename(columns={"PeriodEndDate": "LastStatusDate"})

# Latest per Program+Subteam
idx_sub_latest = fact_all.groupby(["Program","Subteam"])["PeriodEndDate"].idxmax()
subteam_latest = fact_all.loc[idx_sub_latest].copy().reset_index(drop=True)
subteam_latest = subteam_latest.rename(columns={"PeriodEndDate": "LastStatusDate"})

# Add simple "IsLatest" flags to history (optional, helpful for filtering without DAX)
fact_all["IsLatestSubteam"] = False
fact_all.loc[idx_sub_latest, "IsLatestSubteam"] = True

program_monthly["IsLatestProgram"] = False
program_monthly.loc[idx_prog_latest, "IsLatestProgram"] = True

# -----------------------------
# Dimensions (star schema)
# -----------------------------
dim_program = pd.DataFrame({"Program": sorted(fact_all["Program"].unique())})

dim_subteam = (
    fact_all[["Program","Subteam"]]
    .drop_duplicates()
    .sort_values(["Program","Subteam"])
    .reset_index(drop=True)
)

dim_date = (
    pd.DataFrame({"PeriodEndDate": sorted(pd.to_datetime(fact_all["PeriodEndDate"].dropna().unique()))})
)
dim_date["PeriodEndDate"] = dim_date["PeriodEndDate"].map(safe_date)
dim_date["PeriodKey"] = make_period_key(dim_date["PeriodEndDate"])
dim_date["Year"] = pd.to_datetime(dim_date["PeriodEndDate"]).dt.year
dim_date["MonthNum"] = pd.to_datetime(dim_date["PeriodEndDate"]).dt.month
dim_date["YearMonth"] = pd.to_datetime(dim_date["PeriodEndDate"]).dt.to_period("M").astype(str)
dim_date["DateLabel"] = pd.to_datetime(dim_date["PeriodEndDate"]).dt.strftime("%Y-%m-%d")

# -----------------------------
# Corrective Actions template (optional starter)
# -----------------------------
actions_path = ACTIONS_DIR / "CorrectiveActions.xlsx"
if not actions_path.exists():
    corrective_actions_template = pd.DataFrame(columns=[
        "ActionID", "Program", "Subteam", "PeriodKey", "StatusDate",
        "Owner", "CorrectiveAction", "RootCause",
        "Status", "DueDate", "EvidenceLink", "LastUpdated",
        "ActionJoinKey"
    ])
    corrective_actions_template.to_excel(actions_path, index=False)

# -----------------------------
# Write outputs (Parquet + CSV mirror)
# -----------------------------
write_table(fact_all,           MODEL_DIR / "EVMS_FactMonthly")
write_table(program_monthly,    MODEL_DIR / "EVMS_ProgramMonthly")
write_table(program_latest,     MODEL_DIR / "EVMS_ProgramLatest")
write_table(subteam_latest,     MODEL_DIR / "EVMS_SubteamLatest")
write_table(dim_date.rename(columns={"PeriodEndDate":"Date"}), MODEL_DIR / "EVMS_DimDate")
write_table(dim_program,        MODEL_DIR / "EVMS_DimProgram")
write_table(dim_subteam,        MODEL_DIR / "EVMS_DimSubteam")

pd.DataFrame(run_log).to_csv(LOG_DIR / "run_log.csv", index=False)

print("\nDONE. Power BI tables written to:")
print(f"  {MODEL_DIR.resolve()}")
print("\nKey tables for cards (no DAX):")
print("  EVMS_ProgramLatest (Program-level latest status)")
print("  EVMS_SubteamLatest (Subteam-level latest status)")
print("\nCorrective Actions log template:")
print(f"  {actions_path.resolve()}")
