# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# MUST START FROM: cobra_merged_df (your cleaned LONG dataset)
#
# Fixes:
# - NO PROGRAM FILTERING (per request)
# - LSD uses a STATUS WINDOW (default: last 2 distinct dates)
#   so BCWS/BCWP/ACWP are aligned and SPI_LSD doesn't blow up.
# - Program_Overview is WIDE (SPI/CPI as separate columns)
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
# SETTINGS (edit if needed)
# -------------------------
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

TODAY_OVERRIDE = None          # e.g. "2026-02-12"
STATUS_DATES_IN_WINDOW = 2     # <-- key fix: use last 2 DISTINCT dates for LSD window
NEXT_WINDOW_DATES = 2          # for "Next Mo" (use next 2 distinct dates after LSD)
REQUIRE_COSTSETS = ["BCWS","BCWP","ACWP","ETC"]  # EVMS cost sets assumed already mapped upstream

# -------------------------
# PPT COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"  # >= 1.05
CLR_GREEN      = "#339966"  # 0.98–1.05
CLR_YELLOW     = "#FFFF99"  # 0.95–0.98
CLR_RED        = "#C0504D"  # < 0.95

def _to_num(x):
    return pd.to_numeric(x, errors="coerce")

def safe_div(a, b):
    a = _to_num(a)
    b = _to_num(b)
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

# SPI/CPI thresholds
def color_spi_cpi(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 1.05: return CLR_LIGHT_BLUE
    if x >= 0.98: return CLR_GREEN
    if x >= 0.95: return CLR_YELLOW
    return CLR_RED

# VAC/BAC thresholds
def color_vac_over_bac(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 0.055:  return CLR_LIGHT_BLUE
    if x >= -0.025: return CLR_GREEN
    if x >= -0.055: return CLR_YELLOW
    return CLR_RED

# Manpower %Var thresholds
def color_manpower_pct(pct):
    pct = _to_num(pct)
    if pd.isna(pct): return None
    if pct >= 110: return CLR_RED
    if pct >= 106: return CLR_YELLOW
    if pct >= 90:  return CLR_GREEN
    if pct >= 86:  return CLR_YELLOW
    return CLR_RED

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
# COERCE INPUT (minimal; do NOT remap cost sets)
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def coerce_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    # Accept common variants without "assumptions"
    rename = {}
    if "PROGRAMID" in df.columns and "PROGRAM" not in df.columns: rename["PROGRAMID"] = "PROGRAM"
    if "SUB_TEAM" in df.columns and "PRODUCT_TEAM" not in df.columns: rename["SUB_TEAM"] = "PRODUCT_TEAM"
    if "SUBTEAM" in df.columns and "PRODUCT_TEAM" not in df.columns: rename["SUBTEAM"] = "PRODUCT_TEAM"
    if "COSTSET" in df.columns and "COST_SET" not in df.columns: rename["COSTSET"] = "COST_SET"
    if "HRS" in df.columns and "HOURS" not in df.columns: rename["HRS"] = "HOURS"
    df = df.rename(columns=rename)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"cobra_merged_df missing required columns: {missing}\n"
            f"Found columns: {list(df.columns)}"
        )

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    # Keep names as-is except trimming whitespace (you said cleansing already done)
    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].astype(str).str.strip()
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip().str.upper()

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

# ============================================================
# START: cobra_merged_df ONLY
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

base = coerce_long(cobra_merged_df)

today = as_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()

# Keep only EVMS sets we need (no remapping)
evms = base[base["COST_SET"].isin(REQUIRE_COSTSETS)].copy()

print("TODAY:", today)
print("Rows in cobra_merged_df:", len(base))
print("Rows in EVMS (BCWS/BCWP/ACWP/ETC):", len(evms))
print("Programs:", evms["PROGRAM"].nunique(), "| Product Teams:", evms["PRODUCT_TEAM"].nunique())

# -------------------------
# GLOBAL LSD END = max date in data <= today
# -------------------------
dates_le_today = sorted(d for d in evms["DATE"].unique() if d <= today)
if not dates_le_today:
    raise ValueError("No EVMS rows found with DATE <= today. Check DATE parsing or TODAY_OVERRIDE.")
LSD_END = dates_le_today[-1]

# LSD window uses last N distinct dates ending at LSD_END
if len(dates_le_today) < STATUS_DATES_IN_WINDOW:
    lsd_dates = dates_le_today
else:
    lsd_dates = dates_le_today[-STATUS_DATES_IN_WINDOW:]

PREV_DATE = lsd_dates[0] if len(lsd_dates) > 1 else None

print("LSD_END (max DATE <= today):", LSD_END)
print("LSD window dates (used for LSD metrics):", lsd_dates)
print("PREV_DATE:", PREV_DATE)

# -------------------------
# CTD: cumulative up to LSD_END
# LSD: sum over LSD window dates
# -------------------------
ctd = evms[evms["DATE"] <= LSD_END].copy()
lsd = evms[evms["DATE"].isin(lsd_dates)].copy()

# helper: aggregate to program or program+pt, then pivot cost sets
def agg_pivot(df, idx_cols, value_name):
    g = (df.groupby(idx_cols + ["COST_SET"], as_index=False)["HOURS"].sum()
           .rename(columns={"HOURS": value_name}))
    pv = g.pivot_table(index=idx_cols, columns="COST_SET", values=value_name, aggfunc="sum").reset_index()
    for cs in REQUIRE_COSTSETS:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

ctd_prog = agg_pivot(ctd, ["PROGRAM"], "CTD_HRS")
lsd_prog = agg_pivot(lsd, ["PROGRAM"], "LSD_HRS")

ctd_pt = agg_pivot(ctd, ["PROGRAM","PRODUCT_TEAM"], "CTD_HRS")
lsd_pt = agg_pivot(lsd, ["PROGRAM","PRODUCT_TEAM"], "LSD_HRS")

# ============================================================
# PROGRAM OVERVIEW (WIDE like your screenshot)
# Columns: ProgramID | SPI_LSD | SPI_CTD | CPI_LSD | CPI_CTD | *_Color | LSD_DATE | PREV_DATE | AS_OF_DATE
# ============================================================
prog = ctd_prog.merge(lsd_prog, on=["PROGRAM"], how="outer", suffixes=("_CTD","_LSD")).copy()
prog = prog.rename(columns={"PROGRAM":"ProgramID"})

# CTD ratios (cumulative)
prog["SPI_CTD"] = safe_div(prog["BCWP_CTD_HRS"], prog["BCWS_CTD_HRS"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD_HRS"], prog["ACWP_CTD_HRS"])

# LSD ratios (status window)
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD_HRS"], prog["BCWS_LSD_HRS"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD_HRS"], prog["ACWP_LSD_HRS"])

# Colors
prog["SPI_LSD_Color"] = prog["SPI_LSD"].map(color_spi_cpi)
prog["SPI_CTD_Color"] = prog["SPI_CTD"].map(color_spi_cpi)
prog["CPI_LSD_Color"] = prog["CPI_LSD"].map(color_spi_cpi)
prog["CPI_CTD_Color"] = prog["CPI_CTD"].map(color_spi_cpi)

# Date fields (these were missing / inconsistent before)
prog["LSD_DATE"] = LSD_END
prog["PREV_DATE"] = PREV_DATE
prog["AS_OF_DATE"] = LSD_END

comment_overview = "Comments / Root Cause & Corrective Actions"
prog[comment_overview] = ""

Program_Overview = prog[
    ["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "LSD_DATE","PREV_DATE","AS_OF_DATE",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     comment_overview]
].sort_values("ProgramID").reset_index(drop=True)

Program_Overview = preserve_comments(
    OUTPUT_XLSX, "Program_Overview", Program_Overview,
    ["ProgramID"], comment_overview
)

# ============================================================
# PRODUCT TEAM SPI/CPI
# ============================================================
pt = ctd_pt.merge(lsd_pt, on=["PROGRAM","PRODUCT_TEAM"], how="outer", suffixes=("_CTD","_LSD")).copy()
pt = pt.rename(columns={"PROGRAM":"ProgramID", "PRODUCT_TEAM":"Product Team"})

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD_HRS"], pt["BCWS_CTD_HRS"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD_HRS"], pt["ACWP_CTD_HRS"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD_HRS"], pt["BCWS_LSD_HRS"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD_HRS"], pt["ACWP_LSD_HRS"])

pt["SPI_LSD_Color"] = pt["SPI_LSD"].map(color_spi_cpi)
pt["SPI_CTD_Color"] = pt["SPI_CTD"].map(color_spi_cpi)
pt["CPI_LSD_Color"] = pt["CPI_LSD"].map(color_spi_cpi)
pt["CPI_CTD_Color"] = pt["CPI_CTD"].map(color_spi_cpi)

pt["LSD_DATE"] = LSD_END
pt["PREV_DATE"] = PREV_DATE
pt["AS_OF_DATE"] = LSD_END

comment_pt = "Cause & Corrective Actions"
pt[comment_pt] = ""

ProductTeam_SPI_CPI = pt[
    ["ProgramID","Product Team",
     "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "LSD_DATE","PREV_DATE","AS_OF_DATE",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     comment_pt]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

ProductTeam_SPI_CPI = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI,
    ["ProgramID","Product Team"], comment_pt
)

# ============================================================
# PRODUCT TEAM BAC/EAC/VAC
# BAC = sum(BCWS) for the fiscal year in data (year of LSD_END)
# EAC = ACWP_CTD + ETC_CTD
# VAC = BAC - EAC
# Color based on VAC/BAC
# ============================================================
year_start = date(LSD_END.year, 1, 1)
year_end   = date(LSD_END.year, 12, 31)

year_df = evms[(evms["DATE"] >= year_start) & (evms["DATE"] <= year_end)].copy()

bac = (year_df[year_df["COST_SET"] == "BCWS"]
       .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
       .rename(columns={"HOURS":"BAC"}))

# CTD components come from ctd_pt pivot (already cumulative up to LSD_END)
ctd_pt_simple = agg_pivot(ctd, ["PROGRAM","PRODUCT_TEAM"], "CTD_HRS")
ctd_pt_simple = ctd_pt_simple.rename(columns={
    "PROGRAM":"ProgramID",
    "PRODUCT_TEAM":"Product Team",
    "ACWP":"ACWP_CTD",
    "ETC":"ETC_CTD"
})

tmp = bac.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"}).merge(
    ctd_pt_simple[["ProgramID","Product Team","ACWP_CTD","ETC_CTD"]],
    on=["ProgramID","Product Team"], how="outer"
)

tmp["BAC"] = _to_num(tmp["BAC"])
tmp["ACWP_CTD"] = _to_num(tmp["ACWP_CTD"]).fillna(0.0)
tmp["ETC_CTD"]  = _to_num(tmp["ETC_CTD"]).fillna(0.0)
tmp["EAC"] = tmp["ACWP_CTD"] + tmp["ETC_CTD"]
tmp["VAC"] = tmp["BAC"] - tmp["EAC"]
tmp["VAC_BAC"] = safe_div(tmp["VAC"], tmp["BAC"])
tmp["VAC_Color"] = tmp["VAC_BAC"].map(color_vac_over_bac)

tmp[comment_pt] = ""

ProductTeam_BAC_EAC_VAC = tmp[
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
# Next Mo: use next N distinct dates after LSD_END (simple + consistent)
# ============================================================
man_src = ctd_prog.copy()
man_src = man_src.rename(columns={"PROGRAM":"ProgramID"})

# In man_src, CTD_HRS pivot produced columns BCWS, ACWP, etc? Actually agg_pivot names are BCWS,BCWP,ACWP,ETC but values are CTD_HRS
# Because of pivot_table values column name, the columns are cost set names directly.
man = pd.DataFrame({
    "ProgramID": man_src["ProgramID"],
    "Demand Hours": _to_num(man_src.get("BCWS")),
    "Actual Hours": _to_num(man_src.get("ACWP")),
})
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

# Next window: next N distinct dates after LSD_END
dates_gt = sorted(d for d in evms["DATE"].unique() if d > LSD_END)
next_dates = dates_gt[:NEXT_WINDOW_DATES] if dates_gt else []
next_df = evms[(evms["DATE"].isin(next_dates)) & (evms["COST_SET"].isin(["BCWS","ETC"]))].copy()

next_prog = (next_df.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
                 .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
                 .reset_index()
            ) if len(next_df) else pd.DataFrame({"PROGRAM": evms["PROGRAM"].unique()})

if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog = next_prog.rename(columns={
    "PROGRAM":"ProgramID",
    "BCWS":"Next Mo BCWS Hours",
    "ETC":"Next Mo ETC Hours"
})

Program_Manpower = man.merge(
    next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]],
    on="ProgramID", how="left"
)

Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next Mo BCWS Hours","Next Mo ETC Hours",comment_pt]
].sort_values("ProgramID").reset_index(drop=True)

Program_Manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower", Program_Manpower,
    ["ProgramID"], comment_pt
)

# ============================================================
# QUICK DIAGNOSTICS (why values were missing)
# ============================================================
print("\n--- Diagnostics ---")
print("Programs with missing SPI_CTD:", Program_Overview["SPI_CTD"].isna().sum())
print("Programs with missing SPI_LSD:", Program_Overview["SPI_LSD"].isna().sum())
print("If SPI_LSD is NaN, it usually means BCWS_LSD window sum is 0 or missing for that program.\n")

# Example check: show programs where BCWS_LSD is null/0
prog_check = prog.copy()
prog_check["BCWS_LSD_SUM"] = _to_num(prog_check["BCWS_LSD_HRS"])
bad = prog_check[(prog_check["BCWS_LSD_SUM"].isna()) | (prog_check["BCWS_LSD_SUM"] == 0)][["ProgramID","BCWS_LSD_HRS","BCWP_LSD_HRS","ACWP_LSD_HRS"]]
print("Programs with BCWS_LSD window sum missing/0 (will break SPI_LSD):")
print(bad.head(25) if len(bad) else "None")

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