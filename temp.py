# ============================================
# EVMS PowerBI Input Builder (AUTO-RUN VERSION)
# - Uses existing dataframe if present (like your earlier pipeline)
# - Otherwise auto-finds latest export file
# - Calculates SPI/CPI correctly using per-team Last Common Date (LCD)
# - Standard 4-week LSD window
# - Outputs 4 sheets to Excel
# ============================================

from __future__ import annotations
import pandas as pd
import numpy as np
from pathlib import Path
import glob
import os

# ----------------------------
# CONFIG YOU MAY EDIT
# ----------------------------
TODAY = pd.Timestamp.today().normalize()

LSD_WEEKS = 4
LSD_DAYS = LSD_WEEKS * 7

# Where to auto-search if no dataframe exists (add/remove folders)
SEARCH_DIRS = [
    Path.cwd(),
    Path.home() / "Desktop",
    Path.home() / "Downloads",
    Path(r"C:\Users") / os.getlogin() / "Desktop",
]

# If you know your folder, add it here for fastest success
# SEARCH_DIRS.append(Path(r"C:\Users\GRIFFIM2\Desktop\evms_powerbi\cobra evms metrics"))

OUTPUT_XLSX = Path(r"C:\Users") / os.getlogin() / "Desktop" / "EVMS_PowerBI_Input.xlsx"

# Expected long columns (rename here if your df uses different names)
COLMAP = {
    "PROGRAM": "PROGRAM",
    "Program": "PROGRAM",
    "ProgramID": "PROGRAM",
    "PRODUCT_TEAM": "PRODUCT_TEAM",
    "Product Team": "PRODUCT_TEAM",
    "SUB_TEAM": "PRODUCT_TEAM",
    "COST_SET": "COST_SET",
    "Cost Set": "COST_SET",
    "DATE": "DATE",
    "Date": "DATE",
    "VAL": "VAL",
    "Value": "VAL",
    "HOURS": "VAL",
}

KEEP_COSTSETS = {"BCWS","BCWP","ACWP","ETC"}

# Only needed if your file isn’t already mapped to BCWS/BCWP/ACWP/ETC
COSTSET_NORMALIZE_MAP = {
    # examples:
    # "BCWS HRS": "BCWS",
    # "BCWP HRS": "BCWP",
    # "ACWP HRS": "ACWP",
    # "ETC HRS":  "ETC",
}

# ----------------------------
# HELPERS
# ----------------------------
def _norm_str(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip()

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    return np.where((d == 0) | pd.isna(d) | pd.isna(n), np.nan, n / d)

def color_rule_spi_cpi(x: float) -> str | None:
    if pd.isna(x): return None
    if x >= 1.05:  return "#8EB4E3"  # blue
    if x >= 0.98:  return "#339966"  # green
    if x >= 0.95:  return "#FFFF99"  # yellow
    return "#C0504D"                 # red

def color_rule_vac_pct(x: float) -> str | None:
    if pd.isna(x): return None
    if x >= 0:     return "#339966"
    if x >= -0.05: return "#FFFF99"
    return "#C0504D"

def _find_latest_export(search_dirs: list[Path]) -> Path | None:
    patterns = ["*.tsv", "*.csv", "*.xlsx"]
    candidates = []
    for d in search_dirs:
        try:
            if not d.exists():
                continue
            for pat in patterns:
                candidates.extend([Path(p) for p in glob.glob(str(d / pat))])
        except Exception:
            pass
    if not candidates:
        return None
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)

def _standardize_long(df: pd.DataFrame) -> pd.DataFrame:
    # rename columns via COLMAP where possible
    rename = {}
    for c in df.columns:
        if c in COLMAP:
            rename[c] = COLMAP[c]
    df = df.rename(columns=rename).copy()

    required = {"PROGRAM","PRODUCT_TEAM","COST_SET","DATE","VAL"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}. "
                         f"Have columns: {list(df.columns)[:25]}...")

    df["PROGRAM"] = df["PROGRAM"].map(_norm_str)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(_norm_str)
    df["COST_SET"] = df["COST_SET"].map(_norm_str)

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.normalize()
    df["VAL"] = pd.to_numeric(df["VAL"], errors="coerce")

    if COSTSET_NORMALIZE_MAP:
        df["COST_SET"] = df["COST_SET"].replace(COSTSET_NORMALIZE_MAP)

    df = df[df["COST_SET"].isin(KEEP_COSTSETS)].copy()
    df = df.dropna(subset=["DATE","PROGRAM","PRODUCT_TEAM","COST_SET"])
    return df

# ----------------------------
# STEP 1) GET BASE DATA (DF FIRST, FILE SECOND)
# ----------------------------
base = None
for varname in ["cobra_merged_df", "cobra_merged", "base_evms", "base_evm", "evms_long"]:
    if varname in globals() and isinstance(globals()[varname], pd.DataFrame):
        base = globals()[varname].copy()
        print(f"Using existing dataframe: {varname} (rows={len(base)})")
        break

if base is None:
    latest = _find_latest_export(SEARCH_DIRS)
    if latest is None:
        raise FileNotFoundError(
            "I couldn't find any .tsv/.csv/.xlsx export in SEARCH_DIRS.\n"
            f"SEARCH_DIRS checked:\n- " + "\n- ".join(str(d) for d in SEARCH_DIRS) + "\n\n"
            "Fix: either (1) add your exact folder to SEARCH_DIRS, or (2) make sure cobra_merged_df exists."
        )
    print(f"Loading latest file found: {latest}")
    if latest.suffix.lower() == ".tsv":
        base = pd.read_csv(latest, sep="\t")
    elif latest.suffix.lower() == ".csv":
        base = pd.read_csv(latest)
    elif latest.suffix.lower() == ".xlsx":
        base = pd.read_excel(latest)  # if you need a specific sheet, change here
    else:
        raise ValueError(f"Unsupported file type: {latest.suffix}")

# Standardize long format
base = _standardize_long(base)

print("TODAY:", TODAY.date())
print("Rows in base:", len(base))
print("Programs:", base["PROGRAM"].nunique(), "| Product Teams:", base["PRODUCT_TEAM"].nunique())

# ----------------------------
# STEP 2) LCD FIX: per Program+Team last common posted date for BCWS/BCWP/ACWP
# ----------------------------
need_for_spi_cpi = ["BCWS","BCWP","ACWP"]

mx = (base[base["COST_SET"].isin(need_for_spi_cpi)]
      .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"])["DATE"].max()
      .unstack("COST_SET")
      .reset_index())

mx["LCD_END"] = mx[need_for_spi_cpi].min(axis=1)
mx["LSD_END"] = mx["LCD_END"]
mx["LSD_START"] = mx["LSD_END"] - pd.to_timedelta(LSD_DAYS - 1, unit="D")
mx["CTD_END"] = mx["LCD_END"]

base2 = base.merge(mx[["PROGRAM","PRODUCT_TEAM","LSD_START","LSD_END","CTD_END"]],
                   on=["PROGRAM","PRODUCT_TEAM"], how="left")

lsd_df = base2[(base2["DATE"] >= base2["LSD_START"]) & (base2["DATE"] <= base2["LSD_END"])].copy()
ctd_df = base2[base2["DATE"] <= base2["CTD_END"]].copy()

print("LSD window days:", LSD_DAYS)
print("Rows in LSD df:", len(lsd_df), "| Rows in CTD df:", len(ctd_df))

# ----------------------------
# STEP 3) Aggregation helper
# ----------------------------
def agg_costsets(df: pd.DataFrame, group_cols: list[str], suffix: str) -> pd.DataFrame:
    g = (df.groupby(group_cols + ["COST_SET"])["VAL"].sum().unstack("COST_SET").reset_index())
    for cs in KEEP_COSTSETS:
        if cs not in g.columns:
            g[cs] = 0.0
    out = g[group_cols].copy()
    out[f"BCWS_{suffix}"] = g["BCWS"]
    out[f"BCWP_{suffix}"] = g["BCWP"]
    out[f"ACWP_{suffix}"] = g["ACWP"]
    out[f"ETC_{suffix}"]  = g["ETC"]
    return out

# ----------------------------
# STEP 4) Product Team SPI/CPI
# ----------------------------
pt_lsd = agg_costsets(lsd_df, ["PROGRAM","PRODUCT_TEAM"], "LSD")
pt_ctd = agg_costsets(ctd_df, ["PROGRAM","PRODUCT_TEAM"], "CTD")
pt = pt_lsd.merge(pt_ctd, on=["PROGRAM","PRODUCT_TEAM"], how="outer")

pt["DATA_OK_LSD"] = ((pt["BCWS_LSD"] > 0) & (pt["BCWP_LSD"] > 0) & (pt["ACWP_LSD"] > 0)).astype(int)
pt["DATA_OK_CTD"] = ((pt["BCWS_CTD"] > 0) & (pt["BCWP_CTD"] > 0) & (pt["ACWP_CTD"] > 0)).astype(int)

pt["SPI_LSD"] = np.where(pt["DATA_OK_LSD"]==1, _safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"]), np.nan)
pt["CPI_LSD"] = np.where(pt["DATA_OK_LSD"]==1, _safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"]), np.nan)
pt["SPI_CTD"] = np.where(pt["DATA_OK_CTD"]==1, _safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"]), np.nan)
pt["CPI_CTD"] = np.where(pt["DATA_OK_CTD"]==1, _safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"]), np.nan)

pt_dates = mx[["PROGRAM","PRODUCT_TEAM","LSD_START","LSD_END","CTD_END"]].copy()
pt = pt.merge(pt_dates, on=["PROGRAM","PRODUCT_TEAM"], how="left")

pt["SPI_LSD_Color"] = pt["SPI_LSD"].map(color_rule_spi_cpi)
pt["SPI_CTD_Color"] = pt["SPI_CTD"].map(color_rule_spi_cpi)
pt["CPI_LSD_Color"] = pt["CPI_LSD"].map(color_rule_spi_cpi)
pt["CPI_CTD_Color"] = pt["CPI_CTD"].map(color_rule_spi_cpi)

ProductTeam_SPI_CPI = pt.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
ProductTeam_SPI_CPI["Cause & Corrective Actions"] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI[[
    "ProgramID","Product Team",
    "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
    "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
    "LSD_START","LSD_END","CTD_END",
    "DATA_OK_LSD","DATA_OK_CTD",
    "Cause & Corrective Actions"
]].sort_values(["ProgramID","Product Team"])

# ----------------------------
# STEP 5) Program Overview
# ----------------------------
prog_lsd = agg_costsets(lsd_df, ["PROGRAM"], "LSD")
prog_ctd = agg_costsets(ctd_df, ["PROGRAM"], "CTD")
prog = prog_lsd.merge(prog_ctd, on=["PROGRAM"], how="outer")

prog["DATA_OK_LSD"] = ((prog["BCWS_LSD"] > 0) & (prog["BCWP_LSD"] > 0) & (prog["ACWP_LSD"] > 0)).astype(int)
prog["DATA_OK_CTD"] = ((prog["BCWS_CTD"] > 0) & (prog["BCWP_CTD"] > 0) & (prog["ACWP_CTD"] > 0)).astype(int)

prog["SPI_LSD"] = np.where(prog["DATA_OK_LSD"]==1, _safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"]), np.nan)
prog["CPI_LSD"] = np.where(prog["DATA_OK_LSD"]==1, _safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"]), np.nan)
prog["SPI_CTD"] = np.where(prog["DATA_OK_CTD"]==1, _safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"]), np.nan)
prog["CPI_CTD"] = np.where(prog["DATA_OK_CTD"]==1, _safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"]), np.nan)

prog_dates = (mx.groupby("PROGRAM")[["LSD_START","LSD_END","CTD_END"]].min().reset_index())
prog = prog.merge(prog_dates, on="PROGRAM", how="left")

prog["SPI_LSD_Color"] = prog["SPI_LSD"].map(color_rule_spi_cpi)
prog["SPI_CTD_Color"] = prog["SPI_CTD"].map(color_rule_spi_cpi)
prog["CPI_LSD_Color"] = prog["CPI_LSD"].map(color_rule_spi_cpi)
prog["CPI_CTD_Color"] = prog["CPI_CTD"].map(color_rule_spi_cpi)

Program_Overview = prog.rename(columns={"PROGRAM":"ProgramID"})
Program_Overview["Cause & Corrective Actions"] = ""
Program_Overview = Program_Overview[[
    "ProgramID",
    "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
    "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
    "LSD_START","LSD_END","CTD_END",
    "DATA_OK_LSD","DATA_OK_CTD",
    "Cause & Corrective Actions"
]].sort_values("ProgramID")

# ----------------------------
# STEP 6) BAC/EAC/VAC by Product Team
# ----------------------------
pt_bac = pt.copy()
pt_bac["BAC"] = pt_bac["BCWS_CTD"]
pt_bac["EAC"] = pt_bac["ACWP_CTD"] + pt_bac["ETC_CTD"]
pt_bac["VAC"] = pt_bac["BAC"] - pt_bac["EAC"]
pt_bac["VAC_PCT"] = _safe_div(pt_bac["VAC"], pt_bac["BAC"])
pt_bac["VAC_Color"] = pt_bac["VAC_PCT"].map(color_rule_vac_pct)

ProductTeam_BAC_EAC_VAC = pt_bac.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
ProductTeam_BAC_EAC_VAC["Corrective Actions"] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[[
    "ProgramID","Product Team","BAC","EAC","VAC","VAC_PCT","VAC_Color","CTD_END","Corrective Actions"
]].sort_values(["ProgramID","Product Team"])

# ----------------------------
# STEP 7) Program Manpower (Demand/Actual + Next 4 weeks BCWS/ETC)
# ----------------------------
pm = prog_lsd.rename(columns={"PROGRAM":"ProgramID"})
pm["Demand Hours"] = pm["BCWS_LSD"]
pm["Actual Hours"] = pm["ACWP_LSD"]
pm["%Var"] = _safe_div(pm["Actual Hours"] - pm["Demand Hours"], pm["Demand Hours"])
pm["%Var_Col"] = pm["%Var"].map(color_rule_vac_pct)

prog_dates2 = prog_dates.rename(columns={"PROGRAM":"ProgramID"})
prog_dates2["NEXT_START"] = prog_dates2["LSD_END"] + pd.to_timedelta(1, unit="D")
prog_dates2["NEXT_END"]   = prog_dates2["LSD_END"] + pd.to_timedelta(LSD_DAYS, unit="D")

base_prog = base.merge(prog_dates2[["ProgramID","NEXT_START","NEXT_END"]], left_on="PROGRAM", right_on="ProgramID", how="left")
next_df = base_prog[(base_prog["DATE"] >= base_prog["NEXT_START"]) & (base_prog["DATE"] <= base_prog["NEXT_END"])].copy()

next_prog = (next_df.groupby(["ProgramID","COST_SET"])["VAL"].sum().unstack("COST_SET").reset_index())
for cs in KEEP_COSTSETS:
    if cs not in next_prog.columns:
        next_prog[cs] = 0.0

next_prog = next_prog.rename(columns={"BCWS":"Next Mo BCWS Hrs", "ETC":"Next Mo ETC Hrs"})
next_prog = next_prog[["ProgramID","Next Mo BCWS Hrs","Next Mo ETC Hrs"]]

Program_Manpower = pm.merge(next_prog, on="ProgramID", how="left").merge(
    prog_dates.rename(columns={"PROGRAM":"ProgramID"}), on="ProgramID", how="left"
)
Program_Manpower = Program_Manpower[[
    "ProgramID","Demand Hours","Actual Hours","%Var","%Var_Col",
    "Next Mo BCWS Hrs","Next Mo ETC Hrs","LSD_START","LSD_END","CTD_END"
]].sort_values("ProgramID")

# ----------------------------
# STEP 8) WRITE EXCEL
# ----------------------------
OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print("Saved:", OUTPUT_XLSX)

# ----------------------------
# STEP 9) QUICK “WHY IS IT NaN/0?” CHECK
# ----------------------------
bad = ProductTeam_SPI_CPI[ProductTeam_SPI_CPI["DATA_OK_LSD"] == 0][["ProgramID","Product Team","LSD_START","LSD_END"]]
if len(bad):
    print("\nTeams still missing LSD inputs in-window (BCWS/BCWP/ACWP not all present):")
    print(bad.head(30).to_string(index=False))