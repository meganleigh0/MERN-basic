# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# MUST START FROM: cobra_merged_df (your cleaned LONG dataset)
#
# Key fixes vs your last run:
#  1) Uses ONE common LSD_DATE per Program/ProductTeam (not per cost set)
#  2) Handles "cumulative vs incremental" Cobra exports safely:
#       - If values are cumulative-by-date, CTD = value at LSD_DATE,
#         LSD(period) = (value@LSD - value@prev)
#       - If values are incremental-by-date, CTD = sum<=LSD_DATE,
#         LSD(period) = value@LSD
#  3) Program_Overview is WIDE (separate SPI/CPI columns), with color columns
#  4) Sheet names are LOCKED exactly as requested
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
PROGRAMS_KEEP = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]
TODAY_OVERRIDE = None  # e.g. "2026-02-10"
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

# -------------------------
# PPT COLORS (hex)
# -------------------------
CLR_DARK_BLUE  = "#1F497D"  # 031,073,125 (header/nav)
CLR_LIGHT_BLUE = "#8EB4E3"  # 142,180,227
CLR_GREEN      = "#339966"  # 051,153,102
CLR_YELLOW     = "#FFFF99"  # 255,255,153
CLR_RED        = "#C0504D"  # 192,080,077

def _to_num(x):
    return pd.to_numeric(x, errors="coerce")

def safe_div(a, b):
    a = _to_num(a)
    b = _to_num(b)
    out = np.where((b == 0) | pd.isna(b), np.nan, a / b)
    return pd.to_numeric(out, errors="coerce")

# SPI/CPI thresholds (your PPT rounded bands)
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

# Manpower %Var thresholds (PPT)  (Actual/Demand * 100)
def color_manpower_pct(pct):
    pct = _to_num(pct)
    if pd.isna(pct): return None
    # RED >=110, YELLOW 105-110, GREEN 90-105, YELLOW 85-90, RED <85
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

# -------------------------
# BASIC NORMALIZATION (no remapping / no cost-set assumptions)
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

    # Map synonyms -> required
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
        raise ValueError(
            f"cobra_merged_df missing required columns: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )

    df["PROGRAM"] = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

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
# PERIOD LOGIC (use your data to align, no calendar guessing)
# -------------------------
def pick_lsd_date(df: pd.DataFrame, today: date) -> date:
    # Use the latest available DATE not in the future
    dmax = df.loc[df["DATE"] <= today, "DATE"].max()
    if pd.isna(dmax):
        # if everything is "future" somehow, fall back to max overall
        dmax = df["DATE"].max()
    if pd.isna(dmax):
        raise ValueError("No valid DATE values in cobra_merged_df after parsing.")
    return dmax

# -------------------------
# Determine whether a group’s values are cumulative-by-date or incremental-by-date
# -------------------------
def is_cumulative_series(s: pd.Series) -> bool:
    """
    Heuristic:
      - cumulative series is mostly non-decreasing over time
      - and tends to have 'level' significantly larger than typical step
    """
    s = pd.to_numeric(s, errors="coerce").dropna()
    if len(s) < 3:
        return False  # too little data; treat as incremental (safer for sums)
    diffs = s.diff().dropna()
    nondec_frac = (diffs >= -1e-9).mean()  # allow tiny float noise
    # if it's cumulative, diffs are usually small vs level
    level = float(s.iloc[-1])
    step_med = float(np.nanmedian(np.abs(diffs))) if len(diffs) else 0.0
    level_big_vs_step = (step_med == 0.0 and level != 0.0) or (step_med > 0 and level / step_med >= 5)
    return (nondec_frac >= 0.90) and level_big_vs_step

# -------------------------
# Build period & CTD values for BCWS/BCWP/ACWP/ETC
# -------------------------
NEEDED = ["BCWS","BCWP","ACWP","ETC"]

def build_evms_rollup(df_long: pd.DataFrame, group_cols):
    """
    Returns a wide table with:
      - LSD_DATE
      - PREV_DATE
      - *_CTD (cumulative-to-date at LSD_DATE)
      - *_LSD (period delta at LSD_DATE, i.e., LSD - prev)
    Where CTD and LSD are computed correctly whether source data is cumulative or incremental.
    """
    df = df_long[df_long["COST_SET"].isin(NEEDED)].copy()

    # one common LSD_DATE per group (across cost sets)
    lsd_dates = (
        df.groupby(group_cols, as_index=False)["DATE"].max()
        .rename(columns={"DATE":"LSD_DATE"})
    )

    # also capture prev date (previous status period) per group
    df_dates = df[group_cols + ["DATE"]].drop_duplicates().sort_values(group_cols + ["DATE"])
    df_dates["PREV_DATE"] = df_dates.groupby(group_cols)["DATE"].shift(1)
    prev_map = df_dates.merge(lsd_dates, on=group_cols, how="right")
    prev_map = prev_map[prev_map["DATE"] == prev_map["LSD_DATE"]][group_cols + ["LSD_DATE","PREV_DATE"]]

    # aggregate raw values by date/cost_set
    by_date = (
        df.groupby(group_cols + ["DATE","COST_SET"], as_index=False)["HOURS"].sum()
        .rename(columns={"HOURS":"VAL"})
        .sort_values(group_cols + ["COST_SET","DATE"])
    )

    # decide cumulative-vs-incremental per group+cost_set using the time series
    # (we do it on the aggregated series)
    flags = []
    for (keys, cs), sub in by_date.groupby(group_cols + ["COST_SET"]):
        flag = is_cumulative_series(sub["VAL"])
        flags.append((*keys, cs, flag))
    flags_df = pd.DataFrame(flags, columns=[*group_cols, "COST_SET", "IS_CUMULATIVE"])

    by_date = by_date.merge(flags_df, on=group_cols + ["COST_SET"], how="left")
    by_date["IS_CUMULATIVE"] = by_date["IS_CUMULATIVE"].fillna(False)

    # if incremental, build cumulative by cumsum
    by_date["CUM"] = np.where(
        by_date["IS_CUMULATIVE"],
        by_date["VAL"],
        by_date.groupby(group_cols + ["COST_SET"])["VAL"].cumsum()
    )

    # bring in LSD_DATE and PREV_DATE
    by_date = by_date.merge(prev_map, on=group_cols, how="left")

    # CTD at LSD_DATE: use CUM at LSD_DATE
    ctd = by_date[by_date["DATE"] == by_date["LSD_DATE"]].copy()
    ctd = ctd.pivot_table(index=group_cols, columns="COST_SET", values="CUM", aggfunc="first").reset_index()
    ctd.columns = [*group_cols] + [f"{c}_CTD" for c in ctd.columns[len(group_cols):]]

    # period (LSD) value:
    #   - if cumulative: period = CUM(LSD) - CUM(PREV)
    #   - if incremental: period = VAL at LSD_DATE
    # We compute both and select based on IS_CUMULATIVE.
    lsd_rows = by_date[by_date["DATE"] == by_date["LSD_DATE"]].copy()

    prev_rows = by_date.copy()
    prev_rows = prev_rows[prev_rows["DATE"] == prev_rows["PREV_DATE"]][group_cols + ["COST_SET","CUM"]]
    prev_rows = prev_rows.rename(columns={"CUM":"CUM_PREV"})

    lsd_rows = lsd_rows.merge(prev_rows, on=group_cols + ["COST_SET"], how="left")
    lsd_rows["PERIOD"] = np.where(
        lsd_rows["IS_CUMULATIVE"],
        lsd_rows["CUM"] - lsd_rows["CUM_PREV"],
        lsd_rows["VAL"]
    )

    lsd = lsd_rows.pivot_table(index=group_cols, columns="COST_SET", values="PERIOD", aggfunc="first").reset_index()
    lsd.columns = [*group_cols] + [f"{c}_LSD" for c in lsd.columns[len(group_cols):]]

    # join the dates for debugging/traceability (kept in export; you can hide in PBI)
    dates_out = prev_map.copy()
    out = dates_out.merge(ctd, on=group_cols, how="left").merge(lsd, on=group_cols, how="left")

    # ensure all needed columns exist
    for cs in NEEDED:
        for suf in ["CTD","LSD"]:
            col = f"{cs}_{suf}"
            if col not in out.columns:
                out[col] = np.nan

    return out

# ============================================================
# START: cobra_merged_df ONLY
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_to_long(cobra_merged_df)

# Filter programs
keep_norm = [normalize_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()

today = pd.to_datetime(TODAY_OVERRIDE).date() if TODAY_OVERRIDE else date.today()
GLOBAL_LSD = pick_lsd_date(base, today)

# If a program is missing that global date, we still compute LSD per-program,
# but we also export LSD_DATE so you can see alignment.
print("TODAY:", today)
print("GLOBAL_LSD (max date in data <= today):", GLOBAL_LSD)

# -------------------------
# PROGRAM rollup (ProgramID)
# -------------------------
prog_roll = build_evms_rollup(base, group_cols=["PROGRAM"])
prog_roll = prog_roll.rename(columns={"PROGRAM":"ProgramID"})

# compute SPI/CPI (CTD uses *_CTD; LSD uses *_LSD period deltas)
prog_roll["SPI_CTD"] = safe_div(prog_roll["BCWP_CTD"], prog_roll["BCWS_CTD"])
prog_roll["CPI_CTD"] = safe_div(prog_roll["BCWP_CTD"], prog_roll["ACWP_CTD"])
prog_roll["SPI_LSD"] = safe_div(prog_roll["BCWP_LSD"], prog_roll["BCWS_LSD"])
prog_roll["CPI_LSD"] = safe_div(prog_roll["BCWP_LSD"], prog_roll["ACWP_LSD"])

# WIDE Program_Overview (as you asked): separate SPI/CPI columns (no "Metric" column)
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
Program_Overview = preserve_comments(
    OUTPUT_XLSX, "Program_Overview",
    Program_Overview, ["ProgramID"], comment_overview
)

# -------------------------
# PRODUCT TEAM rollup (ProgramID + Product Team)
# -------------------------
pt_roll = build_evms_rollup(base, group_cols=["PROGRAM","PRODUCT_TEAM"])
pt_roll = pt_roll.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})

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
ProductTeam_SPI_CPI = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_SPI_CPI",
    ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt
)

# ============================================================
# PRODUCT TEAM BAC/EAC/VAC
# BAC = BCWS_CTD (at LSD)   (if your org defines BAC differently, swap here)
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# Color based on VAC/BAC
# ============================================================
bac_eac = pt_roll[["ProgramID","Product Team","BCWS_CTD","ACWP_CTD","ETC_CTD"]].copy()
bac_eac["BAC"] = _to_num(bac_eac["BCWS_CTD"])
bac_eac["ACWP_CTD"] = _to_num(bac_eac["ACWP_CTD"]).fillna(0.0)
bac_eac["ETC_CTD"]  = _to_num(bac_eac["ETC_CTD"]).fillna(0.0)
bac_eac["EAC"] = bac_eac["ACWP_CTD"] + bac_eac["ETC_CTD"]
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = bac_eac["VAC_BAC"].map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac[[
    "ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color"
]].copy()
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC",
    ProductTeam_BAC_EAC_VAC, ["ProgramID","Product Team"], comment_pt
)

# ============================================================
# PROGRAM MANPOWER
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual / Demand * 100  (your PPT thresholds)
# Add color column for % Var
# Next Mo BCWS/ETC = sum of PERIOD values between (LSD_DATE, next LSD_DATE]
# (we infer "next period" from your data dates, not from guessing calendar)
# ============================================================
Program_Manpower = prog_roll[["ProgramID","LSD_DATE","PREV_DATE","BCWS_CTD","ACWP_CTD"]].copy()
Program_Manpower = Program_Manpower.rename(columns={"BCWS_CTD":"Demand Hours","ACWP_CTD":"Actual Hours"})

Program_Manpower["Demand Hours"] = _to_num(Program_Manpower["Demand Hours"])
Program_Manpower["Actual Hours"] = _to_num(Program_Manpower["Actual Hours"])
Program_Manpower["% Var"] = safe_div(Program_Manpower["Actual Hours"], Program_Manpower["Demand Hours"]) * 100.0
Program_Manpower["% Var Color"] = Program_Manpower["% Var"].map(color_manpower_pct)

# Find each program's next status date (if any) from the data
prog_dates = base[["PROGRAM","DATE"]].drop_duplicates()
prog_dates["PROGRAM"] = prog_dates["PROGRAM"].map(normalize_program)
prog_dates = prog_dates.sort_values(["PROGRAM","DATE"])

lsd_map = prog_roll.rename(columns={"ProgramID":"PROGRAM"})[["PROGRAM","LSD_DATE"]].copy()
lsd_map["PROGRAM"] = lsd_map["PROGRAM"].map(normalize_program)

next_dates = prog_dates.merge(lsd_map, on="PROGRAM", how="inner")
next_dates = next_dates[next_dates["DATE"] > next_dates["LSD_DATE"]]
next_dates = next_dates.groupby("PROGRAM", as_index=False)["DATE"].min().rename(columns={"DATE":"NEXT_LSD_DATE"})

# compute next-window totals from LSD_DATE (exclusive) to NEXT_LSD_DATE (inclusive)
mw = base[base["COST_SET"].isin(["BCWS","ETC"])].copy()
mw = mw.merge(lsd_map, on="PROGRAM", how="left").merge(next_dates, on="PROGRAM", how="left")

# If a program has no next date yet, leave next-month columns blank
mw_in = mw[mw["NEXT_LSD_DATE"].notna()].copy()
mw_in = mw_in[(mw_in["DATE"] > mw_in["LSD_DATE"]) & (mw_in["DATE"] <= mw_in["NEXT_LSD_DATE"])]

next_prog = (
    mw_in.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog = next_prog.rename(columns={
    "PROGRAM":"ProgramID",
    "BCWS":"Next Mo BCWS Hours",
    "ETC":"Next Mo ETC Hours"
})

Program_Manpower = Program_Manpower.merge(next_prog, on="ProgramID", how="left")
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[[
    "ProgramID","LSD_DATE","PREV_DATE",
    "Demand Hours","Actual Hours","% Var","% Var Color",
    "Next Mo BCWS Hours","Next Mo ETC Hours",
    comment_pt
]].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower",
    Program_Manpower, ["ProgramID"], comment_pt
)

# ============================================================
# QUICK DIAGNOSTICS (why LSD was missing / why SPI looked wrong)
# ============================================================
print("\n--- DIAGNOSTICS ---")
print("Program LSD dates:")
print(Program_Overview[["ProgramID","LSD_DATE","PREV_DATE"]])

print("\nCheck KUW presence (ABRAMS 22):")
kuw = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["ProgramID"]=="ABRAMS 22") & (ProductTeam_SPI_CPI["Product Team"]=="KUW")]
print(kuw[["ProgramID","Product Team","LSD_DATE","PREV_DATE","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]] if len(kuw) else
      "KUW not present in ProductTeam_SPI_CPI output. Confirm cobra_merged_df has KUW rows for BCWS/BCWP/ACWP/ETC on LSD_DATE.")

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
# POWER BI FORMATTING (OVERVIEW table like your screenshot)
# ============================================================
print("""
Power BI - make the OVERVIEW table look like your design (WIDE SPI/CPI columns):
1) Visual: use a Table (not Matrix).
2) Add fields (in this order):
   - ProgramID
   - SPI_LSD
   - SPI_CTD
   - CPI_LSD
   - CPI_CTD
   (optionally hide LSD_DATE/PREV_DATE or keep on a debug page)
3) Conditional formatting (each metric cell):
   - SPI_LSD: Background color -> Format by: Field value -> Based on: SPI_LSD_Color
   - SPI_CTD: Background color -> Format by: Field value -> Based on: SPI_CTD_Color
   - CPI_LSD: Background color -> Format by: Field value -> Based on: CPI_LSD_Color
   - CPI_CTD: Background color -> Format by: Field value -> Based on: CPI_CTD_Color
4) Number formatting:
   - set to 2 decimals
5) Turn off totals/subtotals for that table visual.
""")