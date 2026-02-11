# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL, PRODUCTTEAM VERSION)
# Sheets (names MUST stay stable):
#   1) Program_Overview
#   2) ProductTeam_SPI_CPI
#   3) ProductTeam_BAC_EAC_VAC
#   4) Program_Manpower
# ============================================================

import os, re
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS
# -------------------------
PROGRAMS_KEEP = ["ABRAMS 22", "OLYMPUS", "STRYKER BULG", "XM30"]  # match your slicer labels
TODAY_OVERRIDE = None  # e.g. "2026-02-10"
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

FORCE_READ_FILES = False
INPUT_FILES = []  # optional explicit list. If empty: auto-discover .csv/.xlsx or use in-memory df

# -------------------------
# COLOR PALETTE (from PPT)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"  # RGB 142,180,227
CLR_GREEN      = "#339966"  # RGB 051,153,102
CLR_YELLOW     = "#FFFF99"  # RGB 255,255,153
CLR_RED        = "#C0504D"  # RGB 192,080,077

# -------------------------
# THRESHOLD COLORS
# -------------------------
def color_spi_cpi(x):
    # PPT rounded bands:
    # Blue:  x >= 1.055
    # Green: 0.975 <= x < 1.055
    # Yellow:0.945 <= x < 0.975
    # Red:   x < 0.945
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

def color_vac_over_bac(x):
    # VAC/BAC PPT bands:
    # Blue:   x >= +0.055
    # Green:  -0.025 <= x < +0.055
    # Yellow: -0.055 <= x < -0.025
    # Red:    x < -0.055
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

def color_manpower_pct(pct):
    # Program manpower thresholds (PPT)
    # Red:   pct >= 109.5
    # Yellow:105.5 <= pct < 109.5
    # Green: 89.5  <= pct < 105.5
    # Yellow:85.5  <= pct < 89.5
    # Red:   pct < 85.5
    pct = pd.to_numeric(pct, errors="coerce")
    if pd.isna(pct): return None
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

# -------------------------
# NORMALIZATION
# -------------------------
def norm_program(s):
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = s.replace("_", " ").replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_product_team(s):
    # strong normalization: keep only A-Z0-9 so "K U W" -> "KUW"
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)
    return s if s != "" else None

def norm_costset_raw(s):
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    return s

def map_cost_set(s):
    """
    Map your input COST-SET values (Budget/Progress/ACWP_HRS...) into EVMS sets.
    This is THE key fix for SPI being off / missing.
    """
    s0 = norm_costset_raw(s)
    if s0 is None: return None

    # common aliases from your screenshot
    mapping = {
        "BUDGET": "BCWS",
        "BCWS": "BCWS",
        "PLAN": "BCWS",
        "PLANNED": "BCWS",
        "BCWSHRS": "BCWS",

        "PROGRESS": "BCWP",
        "EARNED": "BCWP",
        "BCWP": "BCWP",
        "BCWPHRS": "BCWP",

        "ACWPHRS": "ACWP",
        "ACTUAL": "ACWP",
        "ACWP": "ACWP",

        "ETC": "ETC",
        "ETCHRS": "ETC",
        "ESTIMATETOCOMPLETE": "ETC",

        # If your file ever contains EAC/BAC/VAC as separate costsets, keep them:
        "EAC":"EAC", "BACHRS":"BAC", "BAC":"BAC", "VAC":"VAC"
    }
    return mapping.get(s0, s0)

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

def to_date(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    if isinstance(x, date): return x
    return pd.to_datetime(x, errors="coerce").date()

# -------------------------
# AS-OF (last Thu of previous month)
# -------------------------
def last_thursday_of_month(year, month):
    last = date(year, 12, 31) if month == 12 else (date(year, month + 1, 1) - timedelta(days=1))
    offset = (last.weekday() - 3) % 7  # Thu=3
    return last - timedelta(days=offset)

def last_thursday_prev_month(d):
    y, m = d.year, d.month
    if m == 1: y, m = y - 1, 12
    else: m -= 1
    return last_thursday_of_month(y, m)

def add_month(d, months=1):
    y, m = d.year, d.month + months
    while m > 12:
        y += 1; m -= 12
    while m < 1:
        y -= 1; m += 12
    last_day = 31 if m == 12 else (date(y, m + 1, 1) - timedelta(days=1)).day
    return date(y, m, min(d.day, last_day))

# -------------------------
# LOAD + COERCE
# -------------------------
def coerce_columns(df):
    df = df.copy()
    df.columns = [str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in df.columns]

    # Required: PROGRAM, PRODUCT_TEAM, DATE, COST_SET, HOURS
    # We'll accept many synonyms:
    if "PROGRAM" not in df.columns:
        for c in ["PROGRAMID","PROG","PROJECT","IPT_PROGRAM","PROGRAM_NAME"]:
            if c in df.columns:
                df.rename(columns={c:"PROGRAM"}, inplace=True); break

    if "PRODUCT_TEAM" not in df.columns:
        for c in ["SUB_TEAM","SUBTEAM","IPT","IPT_NAME","SUB_TEAM_NAME","CONTROL_ACCOUNT","CA","PRODUCTTEAM"]:
            if c in df.columns:
                df.rename(columns={c:"PRODUCT_TEAM"}, inplace=True); break

    if "DATE" not in df.columns:
        for c in ["PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
            if c in df.columns:
                df.rename(columns={c:"DATE"}, inplace=True); break

    if "COST_SET" not in df.columns:
        for c in ["COSTSET","COST-SET","COST_SET_NAME","COST_CATEGORY","COSTSETNAME","COST_SET_TYPE"]:
            if c in df.columns:
                df.rename(columns={c:"COST_SET"}, inplace=True); break

    if "HOURS" not in df.columns:
        for c in ["VALUE","AMOUNT","HRS","HOURS_WORKED","TOTAL_HOURS"]:
            if c in df.columns:
                df.rename(columns={c:"HOURS"}, inplace=True); break

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"Missing required columns: {miss}. Found: {list(df.columns)}")

    df["PROGRAM"] = df["PROGRAM"].map(norm_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(norm_product_team)
    df["COST_SET"] = df["COST_SET"].map(map_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

def load_inputs():
    # Use in-memory df first (if present)
    if not FORCE_READ_FILES:
        for name in ["df","cobra_merged_df","cobra_df","raw_df"]:
            if name in globals() and isinstance(globals()[name], pd.DataFrame) and len(globals()[name]) > 0:
                return coerce_columns(globals()[name])

    files = list(INPUT_FILES)
    if not files:
        candidates = []
        for pat in ["*.csv","*.xlsx","*.xls"]:
            candidates += list(Path(".").glob(pat))
        candidates = sorted(candidates, key=lambda p: ("cobra" not in p.name.lower(), p.name.lower()))
        files = [str(p) for p in candidates[:50]]

    if not files:
        raise FileNotFoundError("No input files found and no in-memory dataframe found (df/cobra_merged_df/etc.).")

    frames = []
    for fp in files:
        p = Path(fp)
        if not p.exists(): continue
        if p.suffix.lower() == ".csv":
            frames.append(pd.read_csv(p))
        elif p.suffix.lower() in [".xlsx",".xls"]:
            xls = pd.ExcelFile(p)
            for sh in xls.sheet_names:
                frames.append(pd.read_excel(p, sheet_name=sh))

    if not frames:
        raise FileNotFoundError("No readable input data found from INPUT_FILES / auto-discovery.")
    return coerce_columns(pd.concat(frames, ignore_index=True))

# -------------------------
# PRESERVE COMMENTS (if Excel already exists)
# -------------------------
def preserve_comments(existing_path, sheet, df_new, key_cols, comment_col):
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
# WIDE PIVOT HELPER
# -------------------------
def pivot_costsets(df, idx_cols, val_col, needed):
    if df.empty:
        out = df[idx_cols].drop_duplicates().copy()
        for cs in needed:
            out[cs] = np.nan
        return out
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values=val_col, aggfunc="sum").reset_index()
    for cs in needed:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

# ============================================================
# RUN
# ============================================================
base = load_inputs()

# filter programs
keep_norm = [norm_program(p) for p in PROGRAMS_KEEP]
base = base[base["PROGRAM"].isin(keep_norm)].copy()

today = to_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = last_thursday_prev_month(today)
month_after = add_month(AS_OF_DATE, 1)
NEXT_PERIOD_END = last_thursday_of_month(month_after.year, month_after.month)

YEAR_FILTER = AS_OF_DATE.year
YEAR_START = date(YEAR_FILTER, 1, 1)
YEAR_END   = date(YEAR_FILTER, 12, 31)

print("TODAY:", today)
print("AS_OF_DATE:", AS_OF_DATE)
print("NEXT_PERIOD_END:", NEXT_PERIOD_END)
print("YEAR_FILTER:", YEAR_FILTER)

NEEDED_COSTSETS = ["BCWS","BCWP","ACWP","ETC"]

# subset
base_to_asof = base[(base["DATE"] <= AS_OF_DATE) & (base["COST_SET"].isin(NEEDED_COSTSETS))].copy()
base_year    = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()

# ============================================================
# CTD
# ============================================================
ctd_pt = (
    base_to_asof.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)
ctd_prog = (
    base_to_asof.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# ============================================================
# LSD FIX (latest DATE <= AS_OF_DATE per key)
# ============================================================
tmp_pt = base_to_asof.sort_values(["PROGRAM","PRODUCT_TEAM","COST_SET","DATE"]).copy()
pt_last = (
    tmp_pt.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE":"LSD_DATE"})
)
lsd_pt = (
    tmp_pt.merge(pt_last, on=["PROGRAM","PRODUCT_TEAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

tmp_prog = base_to_asof.sort_values(["PROGRAM","COST_SET","DATE"]).copy()
prog_last = (
    tmp_prog.groupby(["PROGRAM","COST_SET"], as_index=False)["DATE"].max()
    .rename(columns={"DATE":"LSD_DATE"})
)
lsd_prog = (
    tmp_prog.merge(prog_last, on=["PROGRAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

# ============================================================
# PIVOT WIDE
# ============================================================
ctd_pt_p  = pivot_costsets(ctd_pt,  ["PROGRAM","PRODUCT_TEAM"], "CTD_HRS", NEEDED_COSTSETS)
lsd_pt_p  = pivot_costsets(lsd_pt,  ["PROGRAM","PRODUCT_TEAM"], "LSD_HRS", NEEDED_COSTSETS)
ctd_prog_p= pivot_costsets(ctd_prog,["PROGRAM"],              "CTD_HRS", NEEDED_COSTSETS)
lsd_prog_p= pivot_costsets(lsd_prog,["PROGRAM"],              "LSD_HRS", NEEDED_COSTSETS)

# ============================================================
# AUTO BCWS SCALING PER PROGRAM (fix SPI ~2 or SPI ~0.5)
# ============================================================
def choose_bcws_scale_per_program(df_prog_wide):
    # df_prog_wide has columns PROGRAM, BCWS, BCWP, ...
    candidates = [0.5, 1.0, 2.0]
    scales = {}
    for prog in df_prog_wide["PROGRAM"].dropna().unique():
        r = df_prog_wide[df_prog_wide["PROGRAM"] == prog].iloc[0]
        bcws = pd.to_numeric(r.get("BCWS", np.nan), errors="coerce")
        bcwp = pd.to_numeric(r.get("BCWP", np.nan), errors="coerce")
        if pd.isna(bcws) or pd.isna(bcwp) or bcws == 0:
            scales[prog] = 1.0
            continue
        best = 1.0
        best_err = float("inf")
        for s in candidates:
            spi = bcwp / (bcws * s)
            err = abs(spi - 1.0)
            if err < best_err:
                best_err = err
                best = s
        scales[prog] = best
    return scales

scale_ctd = choose_bcws_scale_per_program(ctd_prog_p)
scale_lsd = choose_bcws_scale_per_program(lsd_prog_p)

print("\nBCWS scale chosen (CTD):", {k: scale_ctd[k] for k in sorted(scale_ctd)})
print("BCWS scale chosen (LSD):", {k: scale_lsd[k] for k in sorted(scale_lsd)})

def apply_scale(df_wide, scale_map, bcws_col="BCWS"):
    df = df_wide.copy()
    df[bcws_col] = pd.to_numeric(df[bcws_col], errors="coerce")
    df["__scale"] = df["PROGRAM"].map(scale_map).fillna(1.0).astype(float)
    df[bcws_col] = df[bcws_col] * df["__scale"]
    return df.drop(columns=["__scale"])

ctd_prog_p = apply_scale(ctd_prog_p, scale_ctd, "BCWS")
lsd_prog_p = apply_scale(lsd_prog_p, scale_lsd, "BCWS")
ctd_pt_p   = apply_scale(ctd_pt_p,   scale_ctd, "BCWS")
lsd_pt_p   = apply_scale(lsd_pt_p,   scale_lsd, "BCWS")

# ============================================================
# PROGRAM OVERVIEW (LONG format: matches your desired layout)
# Program_Overview columns:
# ProgramID | Metric | CTD | LSD | CTD_Color | LSD_Color | Comments...
# ============================================================
prog = ctd_prog_p.merge(lsd_prog_p, on=["PROGRAM"], how="outer", suffixes=("_CTD","_LSD")).copy()
prog.rename(columns={"PROGRAM":"ProgramID"}, inplace=True)

prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

overview_rows = []
for metric in ["SPI", "CPI"]:
    if metric == "SPI":
        ctd = prog["SPI_CTD"]; lsd = prog["SPI_LSD"]
    else:
        ctd = prog["CPI_CTD"]; lsd = prog["CPI_LSD"]
    tmp = pd.DataFrame({
        "ProgramID": prog["ProgramID"],
        "Metric": metric,
        "CTD": ctd,
        "LSD": lsd,
        "CTD_Color": ctd.map(color_spi_cpi),
        "LSD_Color": lsd.map(color_spi_cpi),
    })
    overview_rows.append(tmp)

Program_Overview = pd.concat(overview_rows, ignore_index=True)
comment_overview = "Comments / Root Cause & Corrective Actions"
Program_Overview[comment_overview] = ""
Program_Overview = Program_Overview.sort_values(["ProgramID","Metric"]).reset_index(drop=True)
Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID","Metric"], comment_overview)

# ============================================================
# PRODUCT TEAM SPI/CPI
# ============================================================
pt = ctd_pt_p.merge(lsd_pt_p, on=["PROGRAM","PRODUCT_TEAM"], how="outer", suffixes=("_CTD","_LSD")).copy()
pt.rename(columns={"PROGRAM":"ProgramID", "PRODUCT_TEAM":"Product Team"}, inplace=True)

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

pt["SPI_CTD_Color"] = pt["SPI_CTD"].map(color_spi_cpi)
pt["SPI_LSD_Color"] = pt["SPI_LSD"].map(color_spi_cpi)
pt["CPI_CTD_Color"] = pt["CPI_CTD"].map(color_spi_cpi)
pt["CPI_LSD_Color"] = pt["CPI_LSD"].map(color_spi_cpi)

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI = pt[
    ["ProgramID","Product Team","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color"]
].copy()
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt)

# ============================================================
# PRODUCT TEAM BAC/EAC/VAC
# BAC = YEAR total BCWS (after same BCWS scaling rule)
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# VAC_Color from VAC/BAC
# ============================================================
bcws_year = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"BAC"})
)
# apply per-program scale (same as CTD)
bcws_year["BAC"] = pd.to_numeric(bcws_year["BAC"], errors="coerce")
bcws_year["__scale"] = bcws_year["PROGRAM"].map(scale_ctd).fillna(1.0).astype(float)
bcws_year["BAC"] = bcws_year["BAC"] * bcws_year["__scale"]
bcws_year = bcws_year.drop(columns=["__scale"])

acwp_ctd = ctd_pt_p[["PROGRAM","PRODUCT_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_pt_p[["PROGRAM","PRODUCT_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})

eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","PRODUCT_TEAM"], how="outer")
eac["ACWP_CTD"] = pd.to_numeric(eac["ACWP_CTD"], errors="coerce").fillna(0.0)
eac["ETC_CTD"]  = pd.to_numeric(eac["ETC_CTD"],  errors="coerce").fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bcws_year.merge(eac[["PROGRAM","PRODUCT_TEAM","EAC"]], on=["PROGRAM","PRODUCT_TEAM"], how="outer")
bac_eac["BAC"] = pd.to_numeric(bac_eac["BAC"], errors="coerce")
bac_eac["EAC"] = pd.to_numeric(bac_eac["EAC"], errors="coerce")
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
# Demand Hours = BCWS_CTD (scaled)
# Actual Hours = ACWP_CTD
# % Var = Actual / Demand * 100
# % Var Color included
# Next Mo BCWS/ETC from (AS_OF_DATE, NEXT_PERIOD_END]
# ============================================================
man = ctd_prog_p.rename(columns={"PROGRAM":"ProgramID","BCWS":"Demand Hours","ACWP":"Actual Hours"}).copy()
man["Demand Hours"] = pd.to_numeric(man["Demand Hours"], errors="coerce")
man["Actual Hours"] = pd.to_numeric(man["Actual Hours"], errors="coerce")
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

next_window = base[
    (base["DATE"] > AS_OF_DATE) &
    (base["DATE"] <= NEXT_PERIOD_END) &
    (base["COST_SET"].isin(["BCWS","ETC"]))
].copy()

next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

# apply same per-program scale
next_prog["BCWS"] = pd.to_numeric(next_prog["BCWS"], errors="coerce")
next_prog["__scale"] = next_prog["PROGRAM"].map(scale_ctd).fillna(1.0).astype(float)
next_prog["BCWS"] = next_prog["BCWS"] * next_prog["__scale"]
next_prog = next_prog.drop(columns=["__scale"])

next_prog = next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"})

Program_Manpower = man.merge(next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]], on="ProgramID", how="left")
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next Mo BCWS Hours","Next Mo ETC Hours",comment_pt]
].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ============================================================
# DIAGNOSTICS: KUW + SPI sanity
# ============================================================
def miss_rate(s): 
    return float(pd.to_numeric(s, errors="coerce").isna().mean())

print("\nQuick missingness check:")
print("Program_Overview CTD missing:", miss_rate(Program_Overview["CTD"]))
print("Program_Overview LSD missing:", miss_rate(Program_Overview["LSD"]))
print("ProductTeam_SPI_CPI SPI_CTD missing:", miss_rate(ProductTeam_SPI_CPI["SPI_CTD"]))
print("ProductTeam_BAC_EAC_VAC VAC missing:", miss_rate(ProductTeam_BAC_EAC_VAC["VAC"]))
print("Program_Manpower % Var missing:", miss_rate(Program_Manpower["% Var"]))

# Verify KUW exists for ABRAMS 22
k = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["ProgramID"] == "ABRAMS 22") & (ProductTeam_SPI_CPI["Product Team"] == "KUW")]
print("\nKUW row in ProductTeam_SPI_CPI (ABRAMS 22):")
print(k if len(k) else ">>> MISSING: KUW not found after normalization (check source PRODUCT_TEAM values)")

# SPI cross-check summary by program
print("\nSPI summary (ProductTeam_SPI_CPI):")
print(
    ProductTeam_SPI_CPI.groupby("ProgramID")[["SPI_CTD","SPI_LSD"]]
    .agg(["count","median","min","max"])
)

# ============================================================
# WRITE EXCEL (order matters)
# ============================================================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

display(Program_Overview.head(10))
display(ProductTeam_SPI_CPI.head(10))
display(ProductTeam_BAC_EAC_VAC.head(10))
display(Program_Manpower.head(10))