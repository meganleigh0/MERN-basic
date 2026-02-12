# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# STARTS FROM: cobra_merged_df (your cleaned LONG dataset)
#
# Output sheets (NAMES LOCKED):
#   Program_Overview                (WIDE: ProgramID, SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD + color cols)
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
# SETTINGS (edit if needed)
# -------------------------
PROGRAMS_KEEP = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]   # must match slicer labels
TODAY_OVERRIDE = None                                            # e.g. "2026-02-12"
USE_FAKE_STATUS_PERIOD = False                                   # True = pretend as-of is (today - 14 days)
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

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

# SPI/CPI thresholds (rounded bands from PPT)
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
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

# -------------------------
# NORMALIZATION
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def norm_program(x):
    if pd.isna(x): return None
    s = str(x).strip().upper().replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def norm_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s or None

def norm_cost_set_raw(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    return s or None

# Explicit mapping (edit this if your tokens differ)
# The goal is: map whatever your Cobra export uses -> one of: BCWS, BCWP, ACWP, ETC
COST_SET_MAP = {
    # BCWS variants
    "BCWS": "BCWS",
    "BUDGET": "BCWS",
    "BUDGETHRS": "BCWS",
    "BCWSHRS": "BCWS",
    "BCWS_HRS": "BCWS",

    # BCWP variants
    "BCWP": "BCWP",
    "PROGRESS": "BCWP",
    "EARNED": "BCWP",
    "EARNEDVALUE": "BCWP",
    "BCWPHRS": "BCWP",
    "BCWP_HRS": "BCWP",

    # ACWP variants
    "ACWP": "ACWP",
    "ACTUAL": "ACWP",
    "ACTUALHRS": "ACWP",
    "ACWPHRS": "ACWP",
    "ACWPHOURS": "ACWP",
    "ACWP_HRS": "ACWP",

    # ETC variants
    "ETC": "ETC",
    "ESTIMATETOCOMPLETE": "ETC",
    "ETC_HRS": "ETC",
    "ETCHRS": "ETC",
}

def coerce_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # detect if user accidentally passed a WIDE output table
    wide_markers = {"BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD"}
    if len(wide_markers.intersection(set(df.columns))) >= 2 and "DATE" not in df.columns and "COST_SET" not in df.columns:
        raise ValueError(
            "cobra_merged_df looks like a WIDE/OUTPUT table (has *_CTD columns). "
            "You must pass the LONG dataset with Program/Product Team/Date/Cost_Set/Hours."
        )

    # map required columns from common synonyms (NO guessing beyond names)
    rename = {}
    # PROGRAM
    for c in ["PROGRAM","PROGRAMID","PROG","PROJECT","PROGRAM_NAME"]:
        if c in df.columns: rename[c] = "PROGRAM"; break
    # PRODUCT TEAM (you want it to be Product Team in outputs; internal key = PRODUCT_TEAM)
    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME","CONTROL_ACCOUNT","CA"]:
        if c in df.columns: rename[c] = "PRODUCT_TEAM"; break
    # DATE
    for c in ["DATE","PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
        if c in df.columns: rename[c] = "DATE"; break
    # COST SET
    for c in ["COST_SET","COSTSET","COST_SET_NAME","COST_CATEGORY","COSTSETNAME","COST_SET_TYPE","COST-SET"]:
        if c in df.columns: rename[c] = "COST_SET"; break
    # HOURS
    for c in ["HOURS","HRS","VALUE","AMOUNT","TOTAL_HOURS","HOURS_WORKED"]:
        if c in df.columns: rename[c] = "HOURS"; break

    df = df.rename(columns=rename)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Found columns: {list(df.columns)}")

    # Parse date robustly (handles strings, timestamps, and excel serial numbers)
    # If DATE is numeric and looks like excel serial, to_datetime will handle it with origin="1899-12-30"
    if pd.api.types.is_numeric_dtype(df["DATE"]):
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce", unit="D", origin="1899-12-30")
    else:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")

    df["PROGRAM"] = df["PROGRAM"].map(norm_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(norm_product_team)
    df["COST_SET_RAW"] = df["COST_SET"].map(norm_cost_set_raw)

    # Standardize cost set using mapping (keeps unmapped for diagnostics)
    df["COST_SET_STD"] = df["COST_SET_RAW"].map(COST_SET_MAP)

    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    # Keep rows even if some fields are bad; only drop rows that cannot ever be used
    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","HOURS"])

    # normalize DATE to date (not datetime) for grouping
    df["DATE"] = df["DATE"].dt.date

    return df

def pivot_costsets(df, idx_cols, val_col, needed):
    pv = df.pivot_table(index=idx_cols, columns="COST_SET_STD", values=val_col, aggfunc="sum").reset_index()
    for cs in needed:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

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

# ============================================================
# START: cobra_merged_df ONLY
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_long(cobra_merged_df)

# Diagnostics (THIS is usually where the bug reveals itself)
today = pd.to_datetime(TODAY_OVERRIDE).date() if TODAY_OVERRIDE else date.today()
print("TODAY:", today)
print("Rows after coercion:", len(base))
print("Programs found:", sorted(base["PROGRAM"].dropna().unique())[:20], "...")
print("Unmapped COST_SET examples (first 30):",
      base.loc[base["COST_SET_STD"].isna(), "COST_SET_RAW"].dropna().unique()[:30])

# Filter programs
keep_norm = [norm_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()
print("Rows after PROGRAMS_KEEP filter:", len(base))

# Choose AS_OF_DATE
# Default = GLOBAL_LSD = max date in data <= today
dates_le_today = base.loc[base["DATE"] <= today, "DATE"]
if len(dates_le_today) == 0:
    raise ValueError("No rows have DATE <= today. Your DATE parsing is likely wrong (or your data is future-dated).")

GLOBAL_LSD = max(dates_le_today)

if USE_FAKE_STATUS_PERIOD:
    AS_OF_DATE = today - timedelta(days=14)
    # clamp to available data
    AS_OF_DATE = min(AS_OF_DATE, GLOBAL_LSD)
else:
    AS_OF_DATE = GLOBAL_LSD

print("GLOBAL_LSD (max DATE in data <= today):", GLOBAL_LSD)
print("AS_OF_DATE (used for CTD and LSD):", AS_OF_DATE)

NEEDED = ["BCWS","BCWP","ACWP","ETC"]

# Keep ONLY mapped cost sets for EVMS calcs, but do NOT drop the rest globally
evms = base[base["COST_SET_STD"].isin(NEEDED)].copy()
print("Rows with mapped EVMS cost sets (BCWS/BCWP/ACWP/ETC):", len(evms))
if len(evms) == 0:
    raise ValueError(
        "After mapping, there are 0 EVMS rows. Your COST_SET tokens are not mapping to BCWS/BCWP/ACWP/ETC.\n"
        "Fix: edit COST_SET_MAP at the top to match your cobra_merged_df COST_SET values."
    )

# -------------------------
# CTD (<= AS_OF_DATE)
# -------------------------
to_asof = evms[evms["DATE"] <= AS_OF_DATE].copy()

# Build a COMPLETE key set so we don't “lose” teams/programs during merges
keys_prog = pd.DataFrame({"PROGRAM": sorted(to_asof["PROGRAM"].unique())})
keys_pt = to_asof[["PROGRAM","PRODUCT_TEAM"]].drop_duplicates()

ctd_pt = (to_asof.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET_STD"], as_index=False)["HOURS"].sum()
          .rename(columns={"HOURS":"CTD_HRS"}))
ctd_prog = (to_asof.groupby(["PROGRAM","COST_SET_STD"], as_index=False)["HOURS"].sum()
            .rename(columns={"HOURS":"CTD_HRS"}))

# -------------------------
# LSD (last date <= AS_OF_DATE) per PROGRAM/TEAM/COST_SET
# -------------------------
tmp_pt = to_asof.sort_values(["PROGRAM","PRODUCT_TEAM","COST_SET_STD","DATE"])
pt_last = (tmp_pt.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET_STD"], as_index=False)["DATE"].max()
           .rename(columns={"DATE":"LSD_DATE"}))
lsd_pt = (tmp_pt.merge(pt_last, on=["PROGRAM","PRODUCT_TEAM","COST_SET_STD"], how="inner")
          .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
          .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET_STD"], as_index=False)["HOURS"].sum()
          .rename(columns={"HOURS":"LSD_HRS"}))

tmp_prog = to_asof.sort_values(["PROGRAM","COST_SET_STD","DATE"])
prog_last = (tmp_prog.groupby(["PROGRAM","COST_SET_STD"], as_index=False)["DATE"].max()
             .rename(columns={"DATE":"LSD_DATE"}))
lsd_prog = (tmp_prog.merge(prog_last, on=["PROGRAM","COST_SET_STD"], how="inner")
            .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
            .groupby(["PROGRAM","COST_SET_STD"], as_index=False)["HOURS"].sum()
            .rename(columns={"HOURS":"LSD_HRS"}))

# Pivot to wide for SPI/CPI math
ctd_pt_w   = pivot_costsets(ctd_pt,   ["PROGRAM","PRODUCT_TEAM"], "CTD_HRS", NEEDED)
lsd_pt_w   = pivot_costsets(lsd_pt,   ["PROGRAM","PRODUCT_TEAM"], "LSD_HRS", NEEDED)
ctd_prog_w = pivot_costsets(ctd_prog, ["PROGRAM"],              "CTD_HRS", NEEDED)
lsd_prog_w = pivot_costsets(lsd_prog, ["PROGRAM"],              "LSD_HRS", NEEDED)

# ============================================================
# Program_Overview (WIDE like your screenshot)
# ProgramID | SPI_LSD | SPI_CTD | CPI_LSD | CPI_CTD | ...colors...
# ============================================================
prog = keys_prog.merge(ctd_prog_w, on="PROGRAM", how="left").merge(lsd_prog_w, on="PROGRAM", how="left", suffixes=("_CTD","_LSD"))
prog = prog.rename(columns={"PROGRAM":"ProgramID"})

prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

Program_Overview = prog[["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]].copy()
Program_Overview["SPI_LSD_Color"] = Program_Overview["SPI_LSD"].map(color_spi_cpi)
Program_Overview["SPI_CTD_Color"] = Program_Overview["SPI_CTD"].map(color_spi_cpi)
Program_Overview["CPI_LSD_Color"] = Program_Overview["CPI_LSD"].map(color_spi_cpi)
Program_Overview["CPI_CTD_Color"] = Program_Overview["CPI_CTD"].map(color_spi_cpi)

# Optional debug columns (helpful when values look “half”)
Program_Overview["AS_OF_DATE"] = AS_OF_DATE
Program_Overview = Program_Overview.sort_values("ProgramID").reset_index(drop=True)

# ============================================================
# ProductTeam_SPI_CPI
# ============================================================
pt = keys_pt.merge(ctd_pt_w, on=["PROGRAM","PRODUCT_TEAM"], how="left").merge(
    lsd_pt_w, on=["PROGRAM","PRODUCT_TEAM"], how="left", suffixes=("_CTD","_LSD")
)
pt = pt.rename(columns={"PROGRAM":"ProgramID", "PRODUCT_TEAM":"Product Team"})

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

ProductTeam_SPI_CPI = pt[["ProgramID","Product Team","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]].copy()
ProductTeam_SPI_CPI["SPI_LSD_Color"] = ProductTeam_SPI_CPI["SPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = ProductTeam_SPI_CPI["SPI_CTD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = ProductTeam_SPI_CPI["CPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = ProductTeam_SPI_CPI["CPI_CTD"].map(color_spi_cpi)

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt)

# ============================================================
# ProductTeam_BAC_EAC_VAC
# BAC = sum(BCWS) across ALL available dates in your dataset's year of AS_OF_DATE (change if you want)
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# VAC_Color based on VAC/BAC thresholds
# ============================================================
year_start = date(AS_OF_DATE.year, 1, 1)
year_end   = date(AS_OF_DATE.year, 12, 31)

evms_year = evms[(evms["DATE"] >= year_start) & (evms["DATE"] <= year_end)].copy()

bac = (evms_year[evms_year["COST_SET_STD"] == "BCWS"]
       .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
       .rename(columns={"HOURS":"BAC"}))

# ACWP_CTD / ETC_CTD (from ctd_pt_w which is <= AS_OF_DATE)
ctd_cols = ctd_pt_w.rename(columns={"PROGRAM":"PROGRAM","PRODUCT_TEAM":"PRODUCT_TEAM"}).copy()
acwp_ctd = ctd_cols[["PROGRAM","PRODUCT_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_cols[["PROGRAM","PRODUCT_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","PRODUCT_TEAM"], how="outer")
eac["ACWP_CTD"] = _to_num(eac["ACWP_CTD"]).fillna(0.0)
eac["ETC_CTD"]  = _to_num(eac["ETC_CTD"]).fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = keys_pt.merge(bac, on=["PROGRAM","PRODUCT_TEAM"], how="left").merge(
    eac[["PROGRAM","PRODUCT_TEAM","EAC"]], on=["PROGRAM","PRODUCT_TEAM"], how="left"
)

bac_eac["BAC"] = _to_num(bac_eac["BAC"])
bac_eac["EAC"] = _to_num(bac_eac["EAC"])
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}).copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[
    ["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color",comment_pt]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC,
    ["ProgramID","Product Team"], comment_pt
)

# ============================================================
# Program_Manpower
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual / Demand * 100
# % Var Color required
# ============================================================
man = keys_prog.merge(ctd_prog_w, on="PROGRAM", how="left").rename(columns={"PROGRAM":"ProgramID"}).copy()
man["Demand Hours"] = _to_num(man["BCWS"])
man["Actual Hours"] = _to_num(man["ACWP"])
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

# Next month window: just the next 30 days after AS_OF_DATE (safe, no company-calendar dependency)
next_end = AS_OF_DATE + timedelta(days=30)
next_window = evms[(evms["DATE"] > AS_OF_DATE) & (evms["DATE"] <= next_end) & (evms["COST_SET_STD"].isin(["BCWS","ETC"]))].copy()

next_prog = (next_window.groupby(["PROGRAM","COST_SET_STD"], as_index=False)["HOURS"].sum()
             .pivot_table(index="PROGRAM", columns="COST_SET_STD", values="HOURS", aggfunc="sum")
             .reset_index())

if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog = next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"})

Program_Manpower = man.merge(
    next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]],
    on="ProgramID", how="left"
)

Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next Mo BCWS Hours","Next Mo ETC Hours",comment_pt]
].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ============================================================
# KUW sanity check
# ============================================================
kuw = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["ProgramID"]=="ABRAMS 22") & (ProductTeam_SPI_CPI["Product Team"]=="KUW")]
print("\nKUW check (ABRAMS 22):")
print(kuw if len(kuw) else "KUW not present (check that KUW exists in base data with mapped BCWS/BCWP/ACWP).")

# ============================================================
# WRITE EXCEL (sheet order matters)
# ============================================================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

print("""
Power BI formatting for the OVERVIEW table (WIDE like your screenshot):
1) Visual: Table
2) Fields: ProgramID, SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD
3) Conditional formatting (Background color -> Format by: Field value):
   - SPI_LSD uses SPI_LSD_Color
   - SPI_CTD uses SPI_CTD_Color
   - CPI_LSD uses CPI_LSD_Color
   - CPI_CTD uses CPI_CTD_Color
4) Turn off totals/subtotals for that visual.
5) Set numeric formatting to 2 decimals.
""")
