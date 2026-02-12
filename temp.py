# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL, NON-REDUNDANT)
# STARTS FROM: cobra_merged_df  (your cleaned LONG dataset)
#
# Fixes:
# - NO program filtering
# - LSD metrics use a STANDARD 4-week window (same for all cost sets)
# - Handles incremental vs cumulative series per (Program, ProductTeam, CostSet)
# - Prevents missing KUW rows by doing outer joins + safe fill
# - Ensures Program_Manpower always has Next Mo BCWS/ETC columns
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
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

TODAY_OVERRIDE = None   # e.g. "2026-02-12"
LSD_END_OVERRIDE = None # e.g. "2026-02-08"  (leave None => max DATE in data <= today)
LSD_WEEKS = 4           # STANDARD WINDOW you requested (4 weeks)

# If you want "Next Mo" window to be calendar-ish, you can keep this as 4 weeks too:
NEXT_WEEKS = 4

# -------------------------
# COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"
CLR_GREEN      = "#339966"
CLR_YELLOW     = "#FFFF99"
CLR_RED        = "#C0504D"

def _to_num(x):
    return pd.to_numeric(x, errors="coerce")

def safe_div(a, b):
    a = _to_num(a)
    b = _to_num(b)
    out = np.where((b == 0) | pd.isna(b), np.nan, a / b)
    return out

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
# COERCE / STANDARDIZE INPUT
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def coerce_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # Required semantic columns: PROGRAM, PRODUCT_TEAM, COST_SET, DATE, VALUE
    # (You said mapping is already done in cleansing; we just pick columns, no remap.)
    colmap = {}

    for c in ["PROGRAM", "PROG", "PROGRAM_NAME", "PROJECT"]:
        if c in df.columns:
            colmap[c] = "PROGRAM"
            break

    for c in ["PRODUCT_TEAM", "PRODUCTTEAM", "SUB_TEAM", "SUBTEAM", "IPT"]:
        if c in df.columns:
            colmap[c] = "PRODUCT_TEAM"
            break

    for c in ["COST_SET", "COSTSET", "COST_SET_NAME"]:
        if c in df.columns:
            colmap[c] = "COST_SET"
            break

    for c in ["DATE", "STATUS_DATE", "PERIOD_END", "PERIODEND"]:
        if c in df.columns:
            colmap[c] = "DATE"
            break

    # value column could be HOURS, VAL, VALUE, AMOUNT
    for c in ["HOURS", "VAL", "VALUE", "AMOUNT"]:
        if c in df.columns:
            colmap[c] = "VAL"
            break

    df = df.rename(columns=colmap)

    required = ["PROGRAM", "PRODUCT_TEAM", "COST_SET", "DATE", "VAL"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"cobra_merged_df is missing required columns: {missing}\n"
            f"Columns found: {list(df.columns)}"
        )

    # light normalization (do NOT remap cost sets)
    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].astype(str).str.strip()
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip().str.upper()
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["VAL"] = pd.to_numeric(df["VAL"], errors="coerce")

    df = df.dropna(subset=["PROGRAM", "PRODUCT_TEAM", "COST_SET", "DATE", "VAL"])
    return df

# -------------------------
# COMMENT PRESERVATION
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
    mask = out[oldcol].notna() & (out[oldcol].astype(str).str.strip() != "")
    out.loc[mask, comment_col] = out.loc[mask, oldcol]
    out = out.drop(columns=[oldcol])
    return out

# -------------------------
# INCREMENTAL VS CUMULATIVE DETECTION (PER SERIES)
# -------------------------
def detect_series_type(pivot_daily: pd.DataFrame) -> str:
    """
    pivot_daily: columns ['DATE','VAL'] sorted by DATE, already aggregated per DATE
    Returns: 'cumulative' or 'incremental'
    Heuristic:
      - if mostly nondecreasing AND max/sum is high => cumulative
      - else incremental
    """
    s = pivot_daily["VAL"].astype(float).values
    if len(s) < 3:
        return "incremental"
    diffs = np.diff(s)
    nondec_frac = np.mean(diffs >= -1e-9)
    total = np.nansum(s)
    mx = np.nanmax(s)
    ratio = (mx / total) if total and total > 0 else 0.0
    # cumulative series: typically increasing and last ~= max and sum is not much bigger than max
    if (nondec_frac >= 0.90) and (ratio >= 0.75):
        return "cumulative"
    return "incremental"

def compute_ctd_lsd(df_evms: pd.DataFrame, lsd_start: date, lsd_end: date):
    """
    df_evms columns: PROGRAM, PRODUCT_TEAM, COST_SET, DATE, VAL
    Produces CTD and LSD over same lsd window end:
      - CTD: up to lsd_end (cumulative: last value; incremental: sum)
      - LSD: within [lsd_start, lsd_end] (cumulative: last - value_before_window; incremental: sum in window)
    """
    # aggregate per day first (avoid duplicates exploding)
    daily = (
        df_evms.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET","DATE"], as_index=False)["VAL"].sum()
        .sort_values(["PROGRAM","PRODUCT_TEAM","COST_SET","DATE"])
    )

    # Determine CTD/LSD per (PROGRAM, PRODUCT_TEAM, COST_SET)
    out_rows = []
    for (prog, pt, cs), g in daily.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET"]):
        g = g[g["DATE"] <= lsd_end].copy()
        if g.empty:
            out_rows.append((prog, pt, cs, np.nan, np.nan))
            continue

        g = g.sort_values("DATE")
        series_type = detect_series_type(g[["DATE","VAL"]])

        if series_type == "cumulative":
            # CTD = last cumulative value at/before lsd_end
            ctd = float(g.iloc[-1]["VAL"])

            # value right before window start (<= lsd_start - 1 day)
            prev_cut = lsd_start - timedelta(days=1)
            g_prev = g[g["DATE"] <= prev_cut]
            prev_val = float(g_prev.iloc[-1]["VAL"]) if len(g_prev) else 0.0

            # value at/before lsd_end
            end_val = float(g.iloc[-1]["VAL"])
            lsd = end_val - prev_val
        else:
            # incremental
            ctd = float(g["VAL"].sum())
            win = g[(g["DATE"] >= lsd_start) & (g["DATE"] <= lsd_end)]
            lsd = float(win["VAL"].sum()) if len(win) else 0.0

        out_rows.append((prog, pt, cs, ctd, lsd))

    out = pd.DataFrame(out_rows, columns=["PROGRAM","PRODUCT_TEAM","COST_SET","CTD_VAL","LSD_VAL"])
    return out

def pivot_evms(ctd_lsd: pd.DataFrame, index_cols):
    """
    Returns wide columns like:
      BCWS_CTD, BCWS_LSD, BCWP_CTD, ...
    """
    pv_ctd = ctd_lsd.pivot_table(index=index_cols, columns="COST_SET", values="CTD_VAL", aggfunc="first").reset_index()
    pv_lsd = ctd_lsd.pivot_table(index=index_cols, columns="COST_SET", values="LSD_VAL", aggfunc="first").reset_index()

    # rename cost-set cols
    for cs in [c for c in pv_ctd.columns if c not in index_cols]:
        pv_ctd = pv_ctd.rename(columns={cs: f"{cs}_CTD"})
    for cs in [c for c in pv_lsd.columns if c not in index_cols]:
        pv_lsd = pv_lsd.rename(columns={cs: f"{cs}_LSD"})

    out = pv_ctd.merge(pv_lsd, on=index_cols, how="outer")
    return out

# ============================================================
# RUN
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_long(cobra_merged_df)

# Restrict to EVMS cost sets used in this export (no remap, just filter)
NEEDED = ["BCWS","BCWP","ACWP","ETC"]
evms = base[base["COST_SET"].isin(NEEDED)].copy()

today = pd.to_datetime(TODAY_OVERRIDE).date() if TODAY_OVERRIDE else date.today()

# LSD_END = the last date we trust (max date in data <= today) unless override
if LSD_END_OVERRIDE:
    lsd_end = pd.to_datetime(LSD_END_OVERRIDE).date()
else:
    evms_le_today = evms[evms["DATE"] <= today]
    if evms_le_today.empty:
        raise ValueError("No EVMS rows found with DATE <= today. Check DATE parsing or TODAY_OVERRIDE.")
    lsd_end = evms_le_today["DATE"].max()

# 4-week standard window
lsd_start = lsd_end - timedelta(days=(7 * LSD_WEEKS) - 1)  # inclusive window

# Previous date for display (start of window)
prev_date = lsd_start

print(f"TODAY: {today}")
print(f"LSD_END (max DATE in data <= today): {lsd_end}")
print(f"LSD window ({LSD_WEEKS} weeks): {lsd_start} .. {lsd_end}")

# Compute CTD/LSD at ProductTeam grain, then derive program grain from same logic
ctd_lsd_pt = compute_ctd_lsd(evms, lsd_start=lsd_start, lsd_end=lsd_end)

# Program-level: sum PT first (important: preserves cumulative/incremental handling already done per PT series)
ctd_lsd_prog = (
    ctd_lsd_pt.groupby(["PROGRAM","COST_SET"], as_index=False)[["CTD_VAL","LSD_VAL"]].sum()
)

# Wide
pt_w = pivot_evms(ctd_lsd_pt, index_cols=["PROGRAM","PRODUCT_TEAM"])
prog_w = pivot_evms(ctd_lsd_prog, index_cols=["PROGRAM"])

# =========================
# PROGRAM OVERVIEW (WIDE)
# =========================
prog = prog_w.rename(columns={"PROGRAM":"ProgramID"}).copy()

# Guarantee columns exist (avoid KeyError)
for cs in NEEDED:
    for suf in ["_CTD","_LSD"]:
        col = f"{cs}{suf}"
        if col not in prog.columns:
            prog[col] = np.nan

prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

Program_Overview = prog[["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]].copy()
Program_Overview["LSD_START"] = lsd_start
Program_Overview["LSD_END"] = lsd_end
Program_Overview["AS_OF_DATE"] = lsd_end
Program_Overview["PREV_DATE"] = prev_date

# Color columns (field value formatting in Power BI)
Program_Overview["SPI_LSD_Color"] = Program_Overview["SPI_LSD"].map(color_spi_cpi)
Program_Overview["SPI_CTD_Color"] = Program_Overview["SPI_CTD"].map(color_spi_cpi)
Program_Overview["CPI_LSD_Color"] = Program_Overview["CPI_LSD"].map(color_spi_cpi)
Program_Overview["CPI_CTD_Color"] = Program_Overview["CPI_CTD"].map(color_spi_cpi)

comment_overview = "Cause & Corrective Actions"
Program_Overview[comment_overview] = ""

Program_Overview = Program_Overview.sort_values("ProgramID").reset_index(drop=True)
Program_Overview = preserve_comments(
    OUTPUT_XLSX, "Program_Overview", Program_Overview,
    key_cols=["ProgramID"], comment_col=comment_overview
)

# =========================
# PRODUCT TEAM SPI/CPI
# =========================
pt = pt_w.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}).copy()

# Guarantee columns exist
for cs in NEEDED:
    for suf in ["_CTD","_LSD"]:
        col = f"{cs}{suf}"
        if col not in pt.columns:
            pt[col] = np.nan

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

ProductTeam_SPI_CPI = pt[["ProgramID","Product Team","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]].copy()
ProductTeam_SPI_CPI["SPI_LSD_Color"] = ProductTeam_SPI_CPI["SPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = ProductTeam_SPI_CPI["SPI_CTD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = ProductTeam_SPI_CPI["CPI_LSD"].map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = ProductTeam_SPI_CPI["CPI_CTD"].map(color_spi_cpi)

ProductTeam_SPI_CPI["LSD_START"] = lsd_start
ProductTeam_SPI_CPI["LSD_END"] = lsd_end
ProductTeam_SPI_CPI["AS_OF_DATE"] = lsd_end
ProductTeam_SPI_CPI["PREV_DATE"] = prev_date

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI,
    key_cols=["ProgramID","Product Team"], comment_col=comment_pt
)

# =========================
# PRODUCT TEAM BAC/EAC/VAC
# BAC: sum of BCWS in the same year as LSD_END (incremental plan-hours approach)
# EAC: ACWP_CTD + ETC_CTD
# VAC: BAC - EAC
# =========================
year_start = date(lsd_end.year, 1, 1)
year_end   = date(lsd_end.year, 12, 31)

bcws_year = (
    evms[(evms["COST_SET"] == "BCWS") & (evms["DATE"] >= year_start) & (evms["DATE"] <= year_end)]
    .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["VAL"].sum()
    .rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team","VAL":"BAC"})
)

# CTD ACWP/ETC from our computed CTD (already handles cumulative/incremental per series)
acwp_ctd = ctd_lsd_pt[ctd_lsd_pt["COST_SET"]=="ACWP"][["PROGRAM","PRODUCT_TEAM","CTD_VAL"]].rename(
    columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team","CTD_VAL":"ACWP_CTD"}
)
etc_ctd = ctd_lsd_pt[ctd_lsd_pt["COST_SET"]=="ETC"][["PROGRAM","PRODUCT_TEAM","CTD_VAL"]].rename(
    columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team","CTD_VAL":"ETC_CTD"}
)

# Universe of PTs (prevents KUW from disappearing)
universe_pt = (
    evms.groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)
    .size()
    .drop(columns=["size"])
    .rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
)

bac_eac = (
    universe_pt
    .merge(bcws_year, on=["ProgramID","Product Team"], how="left")
    .merge(acwp_ctd, on=["ProgramID","Product Team"], how="left")
    .merge(etc_ctd, on=["ProgramID","Product Team"], how="left")
)

# Fill-safe (so KUW doesn't go missing)
bac_eac["BAC"] = _to_num(bac_eac["BAC"]).fillna(0.0)
bac_eac["ACWP_CTD"] = _to_num(bac_eac["ACWP_CTD"]).fillna(0.0)
bac_eac["ETC_CTD"] = _to_num(bac_eac["ETC_CTD"]).fillna(0.0)

bac_eac["EAC"] = bac_eac["ACWP_CTD"] + bac_eac["ETC_CTD"]
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

ProductTeam_BAC_EAC_VAC = bac_eac[["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color"]].copy()
ProductTeam_BAC_EAC_VAC["AS_OF_DATE"] = lsd_end
ProductTeam_BAC_EAC_VAC[comment_pt] = ""

ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC,
    key_cols=["ProgramID","Product Team"], comment_col=comment_pt
)

# =========================
# PROGRAM MANPOWER
# Demand Hours = BCWS_CTD
# Actual Hours = ACWP_CTD
# % Var = Actual/Demand * 100
# Next Mo BCWS/ETC = next 4 weeks AFTER LSD_END
# =========================
man = prog.copy()
man = man.rename(columns={"BCWS_CTD":"Demand Hours", "ACWP_CTD":"Actual Hours"})[["ProgramID","Demand Hours","Actual Hours"]].copy()

man["Demand Hours"] = _to_num(man["Demand Hours"])
man["Actual Hours"] = _to_num(man["Actual Hours"])
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

next_start = lsd_end + timedelta(days=1)
next_end = lsd_end + timedelta(days=7 * NEXT_WEEKS)

# next window totals (incremental vs cumulative already handled in base data; for forecast we use SUM of rows in window)
next_window = evms[(evms["DATE"] >= next_start) & (evms["DATE"] <= next_end) & (evms["COST_SET"].isin(["BCWS","ETC"]))].copy()

next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["VAL"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="VAL", aggfunc="sum")
    .reset_index()
).rename(columns={"PROGRAM":"ProgramID"})

if "BCWS" not in next_prog.columns: next_prog["BCWS"] = 0.0
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = 0.0

next_prog = next_prog.rename(columns={"BCWS":"Next Mo BCWS Hours", "ETC":"Next Mo ETC Hours"})

Program_Manpower = man.merge(next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]], on="ProgramID", how="left")
Program_Manpower["Next Mo BCWS Hours"] = _to_num(Program_Manpower["Next Mo BCWS Hours"]).fillna(0.0)
Program_Manpower["Next Mo ETC Hours"]  = _to_num(Program_Manpower["Next Mo ETC Hours"]).fillna(0.0)

Program_Manpower["LSD_START"] = lsd_start
Program_Manpower["LSD_END"] = lsd_end
Program_Manpower["AS_OF_DATE"] = lsd_end
Program_Manpower["NEXT_START"] = next_start
Program_Manpower["NEXT_END"] = next_end
Program_Manpower[comment_pt] = ""

Program_Manpower = Program_Manpower.sort_values("ProgramID").reset_index(drop=True)
Program_Manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower", Program_Manpower,
    key_cols=["ProgramID"], comment_col=comment_pt
)

# =========================
# QUICK DEBUG PRINTS (so you can see why values are missing)
# =========================
def show_missing_overview(df):
    cols = ["SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]
    miss = df[df[cols].isna().any(axis=1)][["ProgramID"] + cols]
    print("\nPrograms with missing SPI/CPI values:")
    print(miss if len(miss) else "None")

show_missing_overview(Program_Overview)

# KUW sanity: confirm we have BAC/EAC/VAC row and SPI/CPI rows
kuw_spi = ProductTeam_SPI_CPI[(ProductTeam_SPI_CPI["Product Team"].str.upper()=="KUW")]
kuw_bac = ProductTeam_BAC_EAC_VAC[(ProductTeam_BAC_EAC_VAC["Product Team"].str.upper()=="KUW")]
print("\nKUW rows present (SPI/CPI):", len(kuw_spi))
print("KUW rows present (BAC/EAC/VAC):", len(kuw_bac))

# =========================
# WRITE EXCEL
# =========================
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    Program_Overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    ProductTeam_SPI_CPI.to_excel(writer, sheet_name="ProductTeam_SPI_CPI", index=False)
    ProductTeam_BAC_EAC_VAC.to_excel(writer, sheet_name="ProductTeam_BAC_EAC_VAC", index=False)
    Program_Manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

print("""
Power BI formatting (OVERVIEW table like your screenshot):
1) Visual: Table
2) Fields: ProgramID, SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD, (optional: LSD_START, LSD_END)
3) Conditional formatting (Background color -> Format by Field value):
   - SPI_LSD uses SPI_LSD_Color
   - SPI_CTD uses SPI_CTD_Color
   - CPI_LSD uses CPI_LSD_Color
   - CPI_CTD uses CPI_CTD_Color
4) Set numeric to 2 decimals.
5) Turn off totals/subtotals for the visual.
""")