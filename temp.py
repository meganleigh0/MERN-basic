# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# STARTS FROM: cobra_merged_df (your cleaned LONG dataset)
#
# KEY FIXES vs prior versions:
# 1) NO program filtering (keeps ALL ProgramID values in the data)
# 2) LSD is a *STATUS PERIOD* window, not "last date only"
#    - LSD period = (PREV_DATE, LSD_DATE] per Program (and per Product Team)
# 3) Program_Overview is WIDE (separate SPI/CPI columns, like your screenshot)
# 4) Keeps sheet names EXACTLY:
#    - Program_Overview
#    - ProductTeam_SPI_CPI
#    - ProductTeam_BAC_EAC_VAC
#    - Program_Manpower
# 5) Adds color columns for each metric column (Field value formatting in Power BI)
# 6) Does NOT redo cost-set remapping (assumes COST_SET already standardized)
# ============================================================

import re
from pathlib import Path
from datetime import date, datetime
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS
# -------------------------
TODAY_OVERRIDE = None          # e.g. "2026-02-12" (None = today)
ASOF_OVERRIDE  = None          # e.g. "2026-02-08" (None = auto = max DATE <= today)
OUTPUT_XLSX    = Path("EVMS_PowerBI_Input.xlsx")

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

# SPI/CPI thresholds (rounded per your PPT bands)
def color_spi_cpi(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

# VAC/BAC thresholds (PPT)
def color_vac_over_bac(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

# Manpower %Var thresholds (PPT)
def color_manpower_pct(pct):
    pct = _to_num(pct)
    if pd.isna(pct): return None
    # percent is already *100
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

# -------------------------
# LIGHT NORMALIZATION ONLY (no remapping)
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def normalize_cost_set(x):
    # IMPORTANT: do NOT remap; just make consistent tokens
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    return s

def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # REQUIRED columns (your "cleaned long" should already have these meanings)
    # We'll accept a few synonyms but we are NOT guessing business meaning beyond this.
    colmap = {}

    for c in ["PROGRAM", "PROGRAMID"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break
    for c in ["PRODUCT_TEAM", "PRODUCTTEAM", "SUB_TEAM", "SUBTEAM"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break
    for c in ["DATE", "STATUS_DATE", "PERIOD_END", "PERIODEND"]:
        if c in df.columns: colmap[c] = "DATE"; break
    for c in ["COST_SET", "COSTSET"]:
        if c in df.columns: colmap[c] = "COST_SET"; break
    for c in ["HOURS", "HRS", "VALUE", "AMOUNT"]:
        if c in df.columns: colmap[c] = "HOURS"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM", "PRODUCT_TEAM", "DATE", "COST_SET", "HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"cobra_merged_df is missing required columns: {missing}\nFound: {list(df.columns)}")

    df["PROGRAM"]      = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"]     = df["COST_SET"].map(normalize_cost_set)
    df["DATE"]         = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"]        = pd.to_numeric(df["HOURS"], errors="coerce")

    # Drop only rows that cannot be used in any EVMS math
    df = df.dropna(subset=["PROGRAM", "PRODUCT_TEAM", "DATE", "COST_SET", "HOURS"]).copy()

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
# STATUS DATES (IMPORTANT FIX)
# LSD is a STATUS PERIOD: (PREV_DATE, LSD_DATE]
# computed per PROGRAM (not global)
# -------------------------
def compute_status_dates(base: pd.DataFrame, asof_date: date):
    # Per-program LSD_DATE = max DATE <= asof_date
    # Per-program PREV_DATE = previous available DATE < LSD_DATE
    dates_by_prog = (
        base.loc[base["DATE"] <= asof_date, ["PROGRAM", "DATE"]]
        .drop_duplicates()
        .sort_values(["PROGRAM", "DATE"])
    )

    lsd = dates_by_prog.groupby("PROGRAM", as_index=False)["DATE"].max().rename(columns={"DATE": "LSD_DATE"})

    prev = (
        dates_by_prog.merge(lsd, on="PROGRAM", how="left")
        .loc[lambda d: d["DATE"] < d["LSD_DATE"]]
        .groupby("PROGRAM", as_index=False)["DATE"].max()
        .rename(columns={"DATE": "PREV_DATE"})
    )

    out = lsd.merge(prev, on="PROGRAM", how="left")
    out["AS_OF_DATE"] = asof_date
    return out

# -------------------------
# HELPERS: window sums
# -------------------------
EVMS_COSTSETS = {"BCWS", "BCWP", "ACWP", "ETC"}

def window_sum(df, keys, start_exclusive_col, end_inclusive_col, value_col="HOURS"):
    """
    Sum HOURS by keys + COST_SET within (start_exclusive, end_inclusive]
    start_exclusive_col/end_inclusive_col are columns in df (dates)
    """
    d = df.copy()
    # keep rows where DATE <= end AND (DATE > start OR start is null)
    d = d[d["DATE"] <= d[end_inclusive_col]].copy()
    if start_exclusive_col is not None:
        d = d[(d[start_exclusive_col].isna()) | (d["DATE"] > d[start_exclusive_col])].copy()
    return (
        d.groupby(keys + ["COST_SET"], as_index=False)[value_col].sum()
        .rename(columns={value_col: "HRS"})
    )

def pivot_costsets(df_long, idx_cols):
    pv = df_long.pivot_table(index=idx_cols, columns="COST_SET", values="HRS", aggfunc="sum").reset_index()
    for cs in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

# ============================================================
# START: cobra_merged_df ONLY
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_to_long(cobra_merged_df)

# keep only EVMS cost sets used in these tabs (no remapping, but strict membership)
base_evms = base[base["COST_SET"].isin(EVMS_COSTSETS)].copy()

today = pd.to_datetime(TODAY_OVERRIDE).date() if TODAY_OVERRIDE else date.today()

# AS_OF_DATE: if not forced, use max DATE in data <= today
if ASOF_OVERRIDE:
    AS_OF_DATE = pd.to_datetime(ASOF_OVERRIDE).date()
else:
    max_in_data = base_evms.loc[base_evms["DATE"] <= today, "DATE"]
    if max_in_data.empty:
        raise ValueError("No rows in cobra_merged_df have DATE <= today (or TODAY_OVERRIDE). Cannot set AS_OF_DATE.")
    AS_OF_DATE = max_in_data.max()

print("TODAY:", today)
print("AS_OF_DATE:", AS_OF_DATE)

# Per-program LSD_DATE and PREV_DATE (status period boundary)
status = compute_status_dates(base_evms, AS_OF_DATE)
print("Programs found:", status["PROGRAM"].nunique())

# Attach LSD/PREV dates to each row (by PROGRAM)
ev = base_evms.merge(status[["PROGRAM", "LSD_DATE", "PREV_DATE", "AS_OF_DATE"]], on="PROGRAM", how="left")

# -------------------------
# CTD sums (<= LSD_DATE per program)
# -------------------------
ev_ctd = ev.copy()
ev_ctd = ev_ctd[ev_ctd["DATE"] <= ev_ctd["LSD_DATE"]].copy()

ctd_prog_long = (
    ev_ctd.groupby(["PROGRAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "HRS"})
)
ctd_pt_long = (
    ev_ctd.groupby(["PROGRAM", "PRODUCT_TEAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "HRS"})
)

# -------------------------
# LSD (status period) sums: (PREV_DATE, LSD_DATE]
# -------------------------
ev_lsd = ev.copy()
ev_lsd = ev_lsd[(ev_lsd["DATE"] <= ev_lsd["LSD_DATE"]) & ((ev_lsd["PREV_DATE"].isna()) | (ev_lsd["DATE"] > ev_lsd["PREV_DATE"]))].copy()

lsd_prog_long = (
    ev_lsd.groupby(["PROGRAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "HRS"})
)
lsd_pt_long = (
    ev_lsd.groupby(["PROGRAM", "PRODUCT_TEAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "HRS"})
)

# Pivot wide
ctd_prog = pivot_costsets(ctd_prog_long, ["PROGRAM"])
lsd_prog = pivot_costsets(lsd_prog_long, ["PROGRAM"])
ctd_pt   = pivot_costsets(ctd_pt_long,   ["PROGRAM", "PRODUCT_TEAM"])
lsd_pt   = pivot_costsets(lsd_pt_long,   ["PROGRAM", "PRODUCT_TEAM"])

# ============================================================
# Program_Overview (WIDE like your screenshot)
# ============================================================
prog = (
    status.rename(columns={"PROGRAM": "ProgramID"})
    .merge(ctd_prog.rename(columns={"PROGRAM": "ProgramID"}), on="ProgramID", how="left", suffixes=("", "_CTD"))
    .merge(lsd_prog.rename(columns={"PROGRAM": "ProgramID"}), on="ProgramID", how="left", suffixes=("_CTD", "_LSD"))
)

# After merges, columns are BCWS_CTD, BCWP_CTD, etc and BCWS_LSD, ...
# Compute metrics (no scaling)
prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

Program_Overview = prog[[
    "ProgramID", "SPI_LSD", "SPI_CTD", "CPI_LSD", "CPI_CTD", "LSD_DATE", "PREV_DATE", "AS_OF_DATE"
]].copy()

# Color columns for each metric column (Power BI "Field value")
Program_Overview["SPI_LSD_Color"] = Program_Overview["SPI_LSD"].map(color_spi_cpi)
Program_Overview["SPI_CTD_Color"] = Program_Overview["SPI_CTD"].map(color_spi_cpi)
Program_Overview["CPI_LSD_Color"] = Program_Overview["CPI_LSD"].map(color_spi_cpi)
Program_Overview["CPI_CTD_Color"] = Program_Overview["CPI_CTD"].map(color_spi_cpi)

comment_overview = "Comments / Root Cause & Corrective Actions"
Program_Overview[comment_overview] = ""
Program_Overview = Program_Overview.sort_values(["ProgramID"]).reset_index(drop=True)
Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID"], comment_overview)

# ============================================================
# ProductTeam_SPI_CPI
# ============================================================
pt = (
    status.merge(ctd_pt, on="PROGRAM", how="left")
          .merge(lsd_pt, on=["PROGRAM", "PRODUCT_TEAM"], how="left", suffixes=("_CTD", "_LSD"))
)

pt = pt.rename(columns={"PROGRAM": "ProgramID", "PRODUCT_TEAM": "Product Team"})
pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

ProductTeam_SPI_CPI = pt[[
    "ProgramID", "Product Team",
    "SPI_LSD", "SPI_CTD", "CPI_LSD", "CPI_CTD",
    "LSD_DATE", "PREV_DATE", "AS_OF_DATE"
]].copy()

ProductTeam_SPI_CPI["SPI_LSD_Color"] = ProductTeam_SPI_CPI["SPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = ProductTeam_SPI_CPI["SPI_CTD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = ProductTeam_SPI_CPI["CPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = ProductTeam_SPI_CPI["CPI_CTD"].map(color_spi_cpi)

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID", "Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID", "Product Team"], comment_pt)

# ============================================================
# ProductTeam_BAC_EAC_VAC
# IMPORTANT FIX (so VAC/BAC isn't insane):
# - BAC = TOTAL BCWS across ALL dates available in the dataset (not just the year)
# - EAC = ACWP_CTD + ETC_CTD (as of LSD_DATE)
# ============================================================
bac_all = (
    base_evms[base_evms["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM", "PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "BAC"})
)

acwp_ctd = (
    ctd_pt_long[ctd_pt_long["COST_SET"] == "ACWP"][["PROGRAM", "PRODUCT_TEAM", "HRS"]]
    .rename(columns={"HRS": "ACWP_CTD"})
)
etc_ctd = (
    ctd_pt_long[ctd_pt_long["COST_SET"] == "ETC"][["PROGRAM", "PRODUCT_TEAM", "HRS"]]
    .rename(columns={"HRS": "ETC_CTD"})
)

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM", "PRODUCT_TEAM"], how="outer")
eac["ACWP_CTD"] = _to_num(eac["ACWP_CTD"]).fillna(0.0)
eac["ETC_CTD"]  = _to_num(eac["ETC_CTD"]).fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bac_all.merge(eac[["PROGRAM", "PRODUCT_TEAM", "EAC"]], on=["PROGRAM", "PRODUCT_TEAM"], how="outer")
bac_eac["BAC"] = _to_num(bac_eac["BAC"])
bac_eac["EAC"] = _to_num(bac_eac["EAC"])
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac.rename(columns={"PROGRAM": "ProgramID", "PRODUCT_TEAM": "Product Team"}).copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[[
    "ProgramID", "Product Team", "BAC", "EAC", "VAC", "VAC_BAC", "VAC_Color", comment_pt
]].sort_values(["ProgramID", "Product Team"]).reset_index(drop=True)

ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC,
    ["ProgramID", "Product Team"], comment_pt
)

# ============================================================
# Program_Manpower
# Demand Hours = BCWS_CTD (as of LSD_DATE per program)
# Actual Hours = ACWP_CTD (as of LSD_DATE per program)
# % Var = Actual / Demand * 100
# Next Mo BCWS/ETC = next status-period window (LSD_DATE, NEXT_DATE]
# ============================================================
man = (
    status.merge(ctd_prog, on="PROGRAM", how="left")
          .rename(columns={"PROGRAM": "ProgramID"})
)

man["Demand Hours"] = _to_num(man["BCWS"])
man["Actual Hours"] = _to_num(man["ACWP"])
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

# next status date per program (first available DATE > LSD_DATE)
dates_after = (
    base_evms[["PROGRAM", "DATE"]].drop_duplicates()
    .merge(status[["PROGRAM", "LSD_DATE"]], on="PROGRAM", how="left")
)
dates_after = dates_after[dates_after["DATE"] > dates_after["LSD_DATE"]].copy()

next_date = (
    dates_after.groupby("PROGRAM", as_index=False)["DATE"].min()
    .rename(columns={"DATE": "NEXT_DATE"})
)

# Compute next-window sums for BCWS + ETC
ev_next = base_evms.merge(status[["PROGRAM", "LSD_DATE"]], on="PROGRAM", how="left").merge(next_date, on="PROGRAM", how="left")
ev_next = ev_next[ev_next["NEXT_DATE"].notna()].copy()
ev_next = ev_next[(ev_next["DATE"] > ev_next["LSD_DATE"]) & (ev_next["DATE"] <= ev_next["NEXT_DATE"]) & (ev_next["COST_SET"].isin(["BCWS", "ETC"]))].copy()

next_prog = (
    ev_next.groupby(["PROGRAM", "COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS": "HRS"})
)
next_prog = next_prog.pivot_table(index="PROGRAM", columns="COST_SET", values="HRS", aggfunc="sum").reset_index()
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan
next_prog = next_prog.rename(columns={"PROGRAM": "ProgramID", "BCWS": "Next Mo BCWS Hours", "ETC": "Next Mo ETC Hours"})

Program_Manpower = man.merge(next_prog, on="ProgramID", how="left")
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[[
    "ProgramID",
    "Demand Hours", "Actual Hours", "% Var", "% Var Color",
    "Next Mo BCWS Hours", "Next Mo ETC Hours",
    "LSD_DATE", "PREV_DATE", "AS_OF_DATE",
    comment_pt
]].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ============================================================
# QUICK DIAGNOSTICS (helps you validate the "SPI_LSD starts with 2" issue)
# ============================================================
print("\n--- Diagnostics ---")
print("Programs with missing PREV_DATE (meaning only one status date exists <= AS_OF_DATE):")
print(status[status["PREV_DATE"].isna()][["PROGRAM", "LSD_DATE", "PREV_DATE"]].head(20))

print("\nExample Program_Overview rows with SPI_LSD > 1.5 (likely period mismatch if this is unexpected):")
tmp_bad = Program_Overview.loc[Program_Overview["SPI_LSD"] > 1.5, ["ProgramID", "SPI_LSD", "LSD_DATE", "PREV_DATE"]]
print(tmp_bad.head(20) if len(tmp_bad) else "None")

# ============================================================
# WRITE EXCEL (sheet order matters)
# ============================================================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

# ============================================================
# POWER BI: make Program_Overview look like your wide OVERVIEW table
# ============================================================
print("""
Power BI formatting for the OVERVIEW table (WIDE like your screenshot):
1) Visual: Table
2) Fields (in this order):
   - ProgramID
   - SPI_LSD
   - SPI_CTD
   - CPI_LSD
   - CPI_CTD
3) Conditional formatting (Background color -> Format by: Field value):
   - SPI_LSD uses SPI_LSD_Color
   - SPI_CTD uses SPI_CTD_Color
   - CPI_LSD uses CPI_LSD_Color
   - CPI_CTD uses CPI_CTD_Color
4) Set numeric formatting to 2 decimals.
5) Turn off totals/subtotals for that visual.

Notes:
- LSD_DATE and PREV_DATE are included in the dataset so you can confirm the status-period window used.
- SPI_LSD is computed from (BCWP in the last status period) / (BCWS in the last status period).
  If someone expects "two weeks of BCWP must match two weeks of BCWS", THIS is exactly that window.
""")