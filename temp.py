# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# MUST START FROM: cobra_merged_df (your cleaned LONG dataset)
#
# FIX for your crash:
#   The error "3 columns passed, passed data had 14 columns" was caused by
#   incorrect unpacking of groupby keys when detecting cumulative vs incremental
#   series. This version fixes that logic and hardens the rollup.
#
# Output sheets (NAMES LOCKED):
#   Program_Overview
#   ProductTeam_SPI_CPI
#   ProductTeam_BAC_EAC_VAC
#   Program_Manpower
# ============================================================

import re
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS
# -------------------------
PROGRAMS_KEEP   = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]
TODAY_OVERRIDE  = None  # e.g. "2026-02-11"
OUTPUT_XLSX     = Path("EVMS_PowerBI_Input.xlsx")

# -------------------------
# PPT COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"  # 142,180,227
CLR_GREEN      = "#339966"  # 051,153,102
CLR_YELLOW     = "#FFFF99"  # 255,255,153
CLR_RED        = "#C0504D"  # 192,080,077

def _to_num(x):
    return pd.to_numeric(x, errors="coerce")

def safe_div(a, b):
    a = _to_num(a)
    b = _to_num(b)
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

def color_spi_cpi(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

def color_vac_over_bac(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

def color_manpower_pct(pct):
    pct = _to_num(pct)
    if pd.isna(pct): return None
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

# -------------------------
# NORMALIZATION (no remapping)
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip().upper().replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", s).strip()

def normalize_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)  # KUW stays KUW
    return s if s else None

def normalize_cost_set(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    return s

def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    colmap = {}
    for c in ["PROGRAM","PROGRAMID","PROG","PROJECT","PROGRAM_NAME"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break
    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME","CONTROL_ACCOUNT","CA"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break
    for c in ["DATE","PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
        if c in df.columns: colmap[c] = "DATE"; break
    for c in ["COST_SET","COSTSET","COST_SET_NAME","COST_CATEGORY","COSTSETNAME","COST_SET_TYPE","COST-SET"]:
        if c in df.columns: colmap[c] = "COST_SET"; break
    for c in ["HOURS","HRS","VALUE","AMOUNT","TOTAL","TOTAL_HOURS"]:
        if c in df.columns: colmap[c] = "HOURS"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"cobra_merged_df missing required columns: {missing}\nFound: {list(df.columns)}")

    df["PROGRAM"]      = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"]     = df["COST_SET"].map(normalize_cost_set)
    df["DATE"]         = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"]        = pd.to_numeric(df["HOURS"], errors="coerce")
    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

# -------------------------
# COMMENTS PRESERVATION
# -------------------------
def preserve_comments(existing_path: Path, sheet: str, df_new: pd.DataFrame, key_cols, comment_col):
    if (not existing_path.exists()) or (comment_col not in df_new.columns):
        return df_new
    try:
        old = pd.read_excel(existing_path, sheet_name=sheet)
    except Exception:
        return df_new
    if old is None or len(old) == 0:
        return df_new
    if (comment_col not in old.columns) or (not all(k in old.columns for k in key_cols)):
        return df_new

    old = old[key_cols + [comment_col]].copy().dropna(subset=key_cols)
    old = old.rename(columns={comment_col: f"{comment_col}_old"})
    out = df_new.merge(old, on=key_cols, how="left")
    oldcol = f"{comment_col}_old"
    if oldcol in out.columns:
        mask = out[oldcol].notna() & (out[oldcol].astype(str).str.strip() != "")
        out.loc[mask, comment_col] = out.loc[mask, oldcol]
        out = out.drop(columns=[oldcol])
    return out

# -------------------------
# EVMS ROLLUP (cumulative vs incremental SAFE)
# -------------------------
NEEDED = ["BCWS","BCWP","ACWP","ETC"]

def is_cumulative_series(vals: pd.Series) -> bool:
    vals = pd.to_numeric(vals, errors="coerce").dropna()
    if len(vals) < 3:
        return False
    diffs = vals.diff().dropna()
    nondec_frac = (diffs >= -1e-9).mean()
    level = float(vals.iloc[-1])
    step_med = float(np.nanmedian(np.abs(diffs))) if len(diffs) else 0.0
    level_big_vs_step = (step_med == 0.0 and level != 0.0) or (step_med > 0 and level / step_med >= 5)
    return (nondec_frac >= 0.90) and level_big_vs_step

def build_evms_rollup(df_long: pd.DataFrame, group_cols):
    df = df_long[df_long["COST_SET"].isin(NEEDED)].copy()

    # LSD per group (across cost sets)
    lsd_dates = df.groupby(group_cols, as_index=False)["DATE"].max().rename(columns={"DATE":"LSD_DATE"})

    # prev status date per group
    dates = df[group_cols + ["DATE"]].drop_duplicates().sort_values(group_cols + ["DATE"])
    dates["PREV_DATE"] = dates.groupby(group_cols)["DATE"].shift(1)
    prev_map = dates.merge(lsd_dates, on=group_cols, how="right")
    prev_map = prev_map[prev_map["DATE"] == prev_map["LSD_DATE"]][group_cols + ["LSD_DATE","PREV_DATE"]]

    # aggregate by date/cost_set
    by_date = (
        df.groupby(group_cols + ["DATE","COST_SET"], as_index=False)["HOURS"].sum()
          .rename(columns={"HOURS":"VAL"})
          .sort_values(group_cols + ["COST_SET","DATE"])
    )

    # >>> FIXED: robust key splitting (no wrong unpacking) <<<
    flags = []
    for group_key, sub in by_date.groupby(group_cols + ["COST_SET"], sort=False):
        # group_key is a tuple of length len(group_cols)+1 (or a scalar if len==1)
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        gvals = list(group_key[:-1])
        cs = group_key[-1]
        flags.append(gvals + [cs, is_cumulative_series(sub["VAL"])])

    flags_df = pd.DataFrame(flags, columns=[*group_cols, "COST_SET", "IS_CUMULATIVE"])
    by_date = by_date.merge(flags_df, on=group_cols + ["COST_SET"], how="left")
    by_date["IS_CUMULATIVE"] = by_date["IS_CUMULATIVE"].fillna(False)

    # build cumulative if incremental
    by_date["CUM"] = np.where(
        by_date["IS_CUMULATIVE"],
        by_date["VAL"],
        by_date.groupby(group_cols + ["COST_SET"])["VAL"].cumsum()
    )

    # attach LSD/PREV
    by_date = by_date.merge(prev_map, on=group_cols, how="left")

    # CTD = CUM at LSD_DATE
    at_lsd = by_date[by_date["DATE"] == by_date["LSD_DATE"]].copy()
    ctd = at_lsd.pivot_table(index=group_cols, columns="COST_SET", values="CUM", aggfunc="first").reset_index()
    ctd.columns = [*group_cols] + [f"{c}_CTD" for c in ctd.columns[len(group_cols):]]

    # LSD period value:
    #   cumulative: CUM(LSD) - CUM(PREV)
    #   incremental: VAL at LSD_DATE
    prev_rows = by_date[by_date["DATE"] == by_date["PREV_DATE"]][group_cols + ["COST_SET","CUM"]].rename(columns={"CUM":"CUM_PREV"})
    at_lsd = at_lsd.merge(prev_rows, on=group_cols + ["COST_SET"], how="left")

    at_lsd["PERIOD"] = np.where(
        at_lsd["IS_CUMULATIVE"],
        at_lsd["CUM"] - at_lsd["CUM_PREV"],
        at_lsd["VAL"]
    )

    lsd = at_lsd.pivot_table(index=group_cols, columns="COST_SET", values="PERIOD", aggfunc="first").reset_index()
    lsd.columns = [*group_cols] + [f"{c}_LSD" for c in lsd.columns[len(group_cols):]]

    out = prev_map.merge(ctd, on=group_cols, how="left").merge(lsd, on=group_cols, how="left")

    # ensure all needed columns exist
    for cs in NEEDED:
        for suf in ["CTD","LSD"]:
            col = f"{cs}_{suf}"
            if col not in out.columns:
                out[col] = np.nan
    return out

# ============================================================
# RUN
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty.")

base = coerce_to_long(cobra_merged_df)

# filter programs
keep_norm = [normalize_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()

today = pd.to_datetime(TODAY_OVERRIDE).date() if TODAY_OVERRIDE else date.today()
GLOBAL_LSD = base.loc[base["DATE"] <= today, "DATE"].max()
if pd.isna(GLOBAL_LSD):
    GLOBAL_LSD = base["DATE"].max()

print("TODAY:", today)
print("GLOBAL_LSD (max date in data <= today):", GLOBAL_LSD)

# -------------------------
# Program overview (WIDE)
# -------------------------
prog_roll = build_evms_rollup(base, group_cols=["PROGRAM"]).rename(columns={"PROGRAM":"ProgramID"})

prog_roll["SPI_CTD"] = safe_div(prog_roll["BCWP_CTD"], prog_roll["BCWS_CTD"])
prog_roll["CPI_CTD"] = safe_div(prog_roll["BCWP_CTD"], prog_roll["ACWP_CTD"])
prog_roll["SPI_LSD"] = safe_div(prog_roll["BCWP_LSD"], prog_roll["BCWS_LSD"])
prog_roll["CPI_LSD"] = safe_div(prog_roll["BCWP_LSD"], prog_roll["ACWP_LSD"])

Program_Overview = prog_roll[[
    "ProgramID","LSD_DATE","PREV_DATE",
    "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"
]].copy()

Program_Overview["SPI_LSD_Color"] = Program_Overview["SPI_LSD"].map(color_spi_cpi)
Program_Overview["SPI_CTD_Color"] = Program_Overview["SPI_CTD"].map(color_spi_cpi)
Program_Overview["CPI_LSD_Color"] = Program_Overview["CPI_LSD"].map(color_spi_cpi)
Program_Overview["CPI_CTD_Color"] = Program_Overview["CPI_CTD"].map(color_spi_cpi)

comment_overview = "Comments / Root Cause & Corrective Actions"
Program_Overview[comment_overview] = ""
Program_Overview = Program_Overview.sort_values(["ProgramID"]).reset_index(drop=True)
Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID"], comment_overview)

# -------------------------
# Product Team SPI/CPI
# -------------------------
pt_roll = build_evms_rollup(base, group_cols=["PROGRAM","PRODUCT_TEAM"]).rename(
    columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}
)

pt_roll["SPI_CTD"] = safe_div(pt_roll["BCWP_CTD"], pt_roll["BCWS_CTD"])
pt_roll["CPI_CTD"] = safe_div(pt_roll["BCWP_CTD"], pt_roll["ACWP_CTD"])
pt_roll["SPI_LSD"] = safe_div(pt_roll["BCWP_LSD"], pt_roll["BCWS_LSD"])
pt_roll["CPI_LSD"] = safe_div(pt_roll["BCWP_LSD"], pt_roll["ACWP_LSD"])

ProductTeam_SPI_CPI = pt_roll[[
    "ProgramID","Product Team","LSD_DATE","PREV_DATE",
    "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"
]].copy()

ProductTeam_SPI_CPI["SPI_LSD_Color"] = ProductTeam_SPI_CPI["SPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = ProductTeam_SPI_CPI["SPI_CTD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = ProductTeam_SPI_CPI["CPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = ProductTeam_SPI_CPI["CPI_CTD"].map(color_spi_cpi)

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt)

# -------------------------
# Product Team BAC/EAC/VAC
# BAC = BCWS_CTD (at LSD)
# -------------------------
bac_eac = pt_roll[["ProgramID","Product Team","BCWS_CTD","ACWP_CTD","ETC_CTD"]].copy()
bac_eac["BAC"] = _to_num(bac_eac["BCWS_CTD"])
bac_eac["ACWP_CTD"] = _to_num(bac_eac["ACWP_CTD"]).fillna(0.0)
bac_eac["ETC_CTD"]  = _to_num(bac_eac["ETC_CTD"]).fillna(0.0)
bac_eac["EAC"] = bac_eac["ACWP_CTD"] + bac_eac["ETC_CTD"]
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = bac_eac["VAC_BAC"].map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac[["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color"]].copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_BAC_EAC_VAC = preserve_comments(OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC, ["ProgramID","Product Team"], comment_pt)

# -------------------------
# Program Manpower
# Demand = BCWS_CTD, Actual = ACWP_CTD
# -------------------------
Program_Manpower = prog_roll[["ProgramID","LSD_DATE","PREV_DATE","BCWS_CTD","ACWP_CTD"]].copy()
Program_Manpower = Program_Manpower.rename(columns={"BCWS_CTD":"Demand Hours","ACWP_CTD":"Actual Hours"})
Program_Manpower["Demand Hours"] = _to_num(Program_Manpower["Demand Hours"])
Program_Manpower["Actual Hours"] = _to_num(Program_Manpower["Actual Hours"])
Program_Manpower["% Var"] = safe_div(Program_Manpower["Actual Hours"], Program_Manpower["Demand Hours"]) * 100.0
Program_Manpower["% Var Color"] = Program_Manpower["% Var"].map(color_manpower_pct)

# next period (data-driven)
prog_dates = base[["PROGRAM","DATE"]].drop_duplicates().sort_values(["PROGRAM","DATE"])
lsd_map = prog_roll.rename(columns={"ProgramID":"PROGRAM"})[["PROGRAM","LSD_DATE"]].copy()
next_dates = prog_dates.merge(lsd_map, on="PROGRAM", how="inner")
next_dates = next_dates[next_dates["DATE"] > next_dates["LSD_DATE"]]
next_dates = next_dates.groupby("PROGRAM", as_index=False)["DATE"].min().rename(columns={"DATE":"NEXT_LSD_DATE"})

mw = base[base["COST_SET"].isin(["BCWS","ETC"])].copy()
mw = mw.merge(lsd_map, on="PROGRAM", how="left").merge(next_dates, on="PROGRAM", how="left")
mw_in = mw[mw["NEXT_LSD_DATE"].notna()].copy()
mw_in = mw_in[(mw_in["DATE"] > mw_in["LSD_DATE"]) & (mw_in["DATE"] <= mw_in["NEXT_LSD_DATE"])]

next_prog = (
    mw_in.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
        .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
        .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog = next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"})
Program_Manpower = Program_Manpower.merge(next_prog, on="ProgramID", how="left")

Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[[
    "ProgramID","LSD_DATE","PREV_DATE",
    "Demand Hours","Actual Hours","% Var","% Var Color",
    "Next Mo BCWS Hours","Next Mo ETC Hours",
    comment_pt
]].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# -------------------------
# KUW check (ABRAMS 22)
# -------------------------
print("\nKUW check (ABRAMS 22):")
kuw = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["ProgramID"]=="ABRAMS 22") & (ProductTeam_SPI_CPI["Product Team"]=="KUW")]
print(kuw if len(kuw) else "KUW not present in output. Confirm cobra_merged_df has KUW rows for BCWS/BCWP/ACWP/ETC on LSD_DATE.")

# -------------------------
# WRITE EXCEL
# -------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

print("""
Power BI formatting for the OVERVIEW table (WIDE like your screenshot):
- Visual: Table
- Fields: ProgramID, SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD
- Conditional formatting (Background color -> Field value):
    SPI_LSD uses SPI_LSD_Color
    SPI_CTD uses SPI_CTD_Color
    CPI_LSD uses CPI_LSD_Color
    CPI_CTD uses CPI_CTD_Color
""")