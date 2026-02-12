# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# MUST START FROM: cobra_merged_df (your cleaned LONG dataset)
# Assumes COST_SET already mapped to: BCWS, BCWP, ACWP, ETC (no remap)
#
# LSD = STATUS PERIOD WINDOW (PREV_DATE, AS_OF_DATE]
# CTD = <= AS_OF_DATE
#
# Output sheets (NAMES LOCKED):
#   Program_Overview            (WIDE: SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD)
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
PROGRAMS_KEEP = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]   # must match slicer labels
TODAY_OVERRIDE = None  # e.g. "2026-02-12"
ASOF_OVERRIDE  = None  # e.g. "2026-02-08" (leave None to use max date in data <= today)
OUTPUT_XLSX = Path(r"EVMS_PowerBI_Input.xlsx")

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

# SPI/CPI thresholds (PPT rounded bands)
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

def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip().upper().replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

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

def as_date(x):
    if x is None: return None
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    return pd.to_datetime(x, errors="coerce").date()

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
# INPUT COERCION (expects your cleaned long dataset)
# -------------------------
def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # hard stop if someone accidentally points to an output/wide table
    wide_markers = {"BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD"}
    if len(wide_markers.intersection(set(df.columns))) >= 2 and ("DATE" not in df.columns or "COST_SET" not in df.columns):
        raise ValueError(
            "cobra_merged_df looks like an OUTPUT/WIDE table (has *_CTD columns) not the raw LONG cobra dataset.\n"
            "Set cobra_merged_df to your cleaned long data with Program/ProductTeam/Date/Cost_Set/Hours."
        )

    # Map required columns from common synonyms (minimal assumptions)
    colmap = {}

    for c in ["PROGRAM","PROGRAMID","PROG","PROJECT","PROGRAM_NAME"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break

    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME","CONTROL_ACCOUNT"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break

    for c in ["DATE","PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
        if c in df.columns: colmap[c] = "DATE"; break

    for c in ["COST_SET","COSTSET","COST_SET_NAME","COST_CATEGORY","COSTSETNAME","COST_SET_TYPE","COST-SET"]:
        if c in df.columns: colmap[c] = "COST_SET"; break

    for c in ["HOURS","HRS","VALUE","AMOUNT","TOTAL_HOURS"]:
        if c in df.columns: colmap[c] = "HOURS"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Found: {list(df.columns)}")

    df["PROGRAM"] = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

# -------------------------
# STATUS PERIOD (LSD) DATES
# Uses the latest two distinct dates in the *data* per program.
# LSD window = (PREV_DATE, AS_OF_DATE]
# -------------------------
def program_status_dates(df: pd.DataFrame, today: date, asof_override: date | None):
    d = df[df["DATE"] <= today].copy()
    if asof_override is not None:
        as_of = asof_override
    else:
        if d.empty:
            raise ValueError("No rows in cobra_merged_df with DATE <= today. Check DATE parsing.")
        as_of = d["DATE"].max()

    # get prev distinct date per program (based on actual data dates)
    per_prog = (
        d[d["DATE"] <= as_of]
        .groupby("PROGRAM")["DATE"]
        .apply(lambda s: sorted(set(s)))
        .reset_index(name="DATES")
    )
    def pick_prev(dates):
        dates = [x for x in dates if x <= as_of]
        if len(dates) >= 2:
            return dates[-2]
        # fallback: assume bi-weekly if only one date exists
        return as_of - timedelta(days=14)

    per_prog["AS_OF_DATE"] = as_of
    per_prog["PREV_DATE"] = per_prog["DATES"].apply(pick_prev)
    per_prog = per_prog[["PROGRAM","AS_OF_DATE","PREV_DATE"]]
    return as_of, per_prog

def sum_window(df, key_cols, cost_sets, start_exclusive, end_inclusive):
    w = df[(df["DATE"] > start_exclusive) & (df["DATE"] <= end_inclusive) & (df["COST_SET"].isin(cost_sets))].copy()
    out = (w.groupby(key_cols + ["COST_SET"], as_index=False)["HOURS"].sum())
    pv = out.pivot_table(index=key_cols, columns="COST_SET", values="HOURS", aggfunc="sum").reset_index()
    for cs in cost_sets:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

def sum_ctd(df, key_cols, cost_sets, end_inclusive):
    w = df[(df["DATE"] <= end_inclusive) & (df["COST_SET"].isin(cost_sets))].copy()
    out = (w.groupby(key_cols + ["COST_SET"], as_index=False)["HOURS"].sum())
    pv = out.pivot_table(index=key_cols, columns="COST_SET", values="HOURS", aggfunc="sum").reset_index()
    for cs in cost_sets:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

# ============================================================
# START
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_to_long(cobra_merged_df)

keep_norm = [normalize_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()

# restrict to EVMS cost sets we need
NEEDED = ["BCWS","BCWP","ACWP","ETC"]
base["COST_SET"] = base["COST_SET"].map(normalize_cost_set)
base = base[base["COST_SET"].isin(NEEDED)].copy()

today = as_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
asof_override = as_date(ASOF_OVERRIDE) if ASOF_OVERRIDE else None

AS_OF_DATE, prog_dates = program_status_dates(base, today=today, asof_override=asof_override)

print("TODAY:", today)
print("GLOBAL AS_OF_DATE (LSD end):", AS_OF_DATE)
print("Programs found:", sorted(base["PROGRAM"].unique()))

# ============================================================
# PROGRAM-LEVEL: CTD + LSD (status period window)
# ============================================================
# Merge per-program dates to tag each row with that program’s AS_OF and PREV
tag = base.merge(prog_dates, on="PROGRAM", how="left")

# CTD = <= AS_OF_DATE
ctd_prog = (
    tag[tag["DATE"] <= tag["AS_OF_DATE"]]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
for cs in NEEDED:
    if cs not in ctd_prog.columns: ctd_prog[cs] = np.nan

# LSD = (PREV_DATE, AS_OF_DATE]
lsd_prog = (
    tag[(tag["DATE"] > tag["PREV_DATE"]) & (tag["DATE"] <= tag["AS_OF_DATE"])]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
for cs in NEEDED:
    if cs not in lsd_prog.columns: lsd_prog[cs] = np.nan

prog_w = (
    prog_dates.merge(ctd_prog, on="PROGRAM", how="left", suffixes=("",""))
             .merge(lsd_prog, on="PROGRAM", how="left", suffixes=("_CTD","_LSD"))
).rename(columns={"PROGRAM":"ProgramID"})

# SPI/CPI
prog_w["SPI_CTD"] = safe_div(prog_w["BCWP_CTD"], prog_w["BCWS_CTD"])
prog_w["CPI_CTD"] = safe_div(prog_w["BCWP_CTD"], prog_w["ACWP_CTD"])
prog_w["SPI_LSD"] = safe_div(prog_w["BCWP_LSD"], prog_w["BCWS_LSD"])
prog_w["CPI_LSD"] = safe_div(prog_w["BCWP_LSD"], prog_w["ACWP_LSD"])

# Colors for each metric column (what you need for conditional formatting per column)
prog_w["SPI_LSD_Color"] = pd.Series(prog_w["SPI_LSD"]).map(color_spi_cpi)
prog_w["SPI_CTD_Color"] = pd.Series(prog_w["SPI_CTD"]).map(color_spi_cpi)
prog_w["CPI_LSD_Color"] = pd.Series(prog_w["CPI_LSD"]).map(color_spi_cpi)
prog_w["CPI_CTD_Color"] = pd.Series(prog_w["CPI_CTD"]).map(color_spi_cpi)

Program_Overview = prog_w[
    ["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     "AS_OF_DATE","PREV_DATE"]
].copy()

# ============================================================
# PRODUCT TEAM SPI/CPI (uses SAME program status window)
# ============================================================
ctd_pt = (
    tag[tag["DATE"] <= tag["AS_OF_DATE"]]
    .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index=["PROGRAM","PRODUCT_TEAM"], columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
for cs in NEEDED:
    if cs not in ctd_pt.columns: ctd_pt[cs] = np.nan

lsd_pt = (
    tag[(tag["DATE"] > tag["PREV_DATE"]) & (tag["DATE"] <= tag["AS_OF_DATE"])]
    .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index=["PROGRAM","PRODUCT_TEAM"], columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
for cs in NEEDED:
    if cs not in lsd_pt.columns: lsd_pt[cs] = np.nan

pt = (ctd_pt.merge(lsd_pt, on=["PROGRAM","PRODUCT_TEAM"], how="outer", suffixes=("_CTD","_LSD"))
          .rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}))

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
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
# PRODUCT TEAM BAC/EAC/VAC
# BAC = Year-to-date BCWS (calendar year of AS_OF_DATE)
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# VAC_Color based on VAC/BAC
# ============================================================
YEAR = AS_OF_DATE.year
YEAR_START = date(YEAR, 1, 1)
YEAR_END   = date(YEAR, 12, 31)

base_year = tag[(tag["DATE"] >= YEAR_START) & (tag["DATE"] <= YEAR_END)].copy()

bac = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"BAC"})
)

acwp_ctd = (
    tag[(tag["DATE"] <= tag["AS_OF_DATE"]) & (tag["COST_SET"] == "ACWP")]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"ACWP_CTD"})
)

etc_ctd = (
    tag[(tag["DATE"] <= tag["AS_OF_DATE"]) & (tag["COST_SET"] == "ETC")]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"ETC_CTD"})
)

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","PRODUCT_TEAM"], how="outer")
eac["ACWP_CTD"] = _to_num(eac["ACWP_CTD"]).fillna(0.0)
eac["ETC_CTD"]  = _to_num(eac["ETC_CTD"]).fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bac.merge(eac[["PROGRAM","PRODUCT_TEAM","EAC"]], on=["PROGRAM","PRODUCT_TEAM"], how="outer")
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
# PROGRAM MANPOWER
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual / Demand * 100
# Next Mo BCWS/ETC: (AS_OF_DATE, AS_OF_DATE + 1 period] using next distinct date in data if exists
# ============================================================
man = prog_w.rename(columns={"BCWS_CTD":"Demand Hours", "ACWP_CTD":"Actual Hours"}).copy()
man["Demand Hours"] = _to_num(man["Demand Hours"])
man["Actual Hours"] = _to_num(man["Actual Hours"])
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

# Use the next distinct date in the data (global) as "next period end" if available; else +14d fallback
all_dates = sorted(set([d for d in base["DATE"].unique() if d is not None]))
future_dates = [d for d in all_dates if d > AS_OF_DATE]
NEXT_PERIOD_END = future_dates[0] if len(future_dates) else (AS_OF_DATE + timedelta(days=14))

next_window = tag[(tag["DATE"] > tag["AS_OF_DATE"]) & (tag["DATE"] <= NEXT_PERIOD_END) & (tag["COST_SET"].isin(["BCWS","ETC"]))].copy()
next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan
next_prog = next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"})

Program_Manpower = man.merge(next_prog, on="ProgramID", how="left")
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next Mo BCWS Hours","Next Mo ETC Hours",comment_pt]
].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ============================================================
# QUICK DIAGNOSTICS (to catch the "SPI_LSD ~2" root cause)
# ============================================================
print("\n--- Status period dates used (per program) ---")
print(prog_dates.sort_values("PROGRAM"))

print("\n--- Program denominators check (LSD) ---")
print(prog_w[["ProgramID","BCWS_LSD","BCWP_LSD","SPI_LSD","ACWP_LSD","CPI_LSD"]].sort_values("ProgramID"))

print("\nKUW check (ABRAMS 22):")
kuw = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["ProgramID"]=="ABRAMS 22") & (ProductTeam_SPI_CPI["Product Team"]=="KUW")]
print(kuw if len(kuw) else "KUW not present in ProductTeam_SPI_CPI output (check if KUW has BCWS/BCWP/ACWP rows inside the LSD window).")

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
Power BI formatting for Program_Overview (WIDE like your screenshot):
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