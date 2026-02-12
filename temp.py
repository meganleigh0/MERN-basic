# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL, SNAPSHOT-SAFE)
# MUST START FROM: cobra_merged_df (cleaned LONG dataset)
#
# Key fix:
#   Treat COST_SET values as SNAPSHOT CTD at each DATE (common Cobra export pattern).
#   - CTD = value at latest status date (LSD_END)
#   - LSD (period) = CTD(LSD_END) - CTD(PREV_DATE)
#
# Output sheets (NAMES LOCKED):
#   Program_Overview              (WIDE: SPI_LSD, SPI_CTD, CPI_LSD, CPI_CTD + colors)
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

# Use a consistent standard window for the "status period" (LSD)
LSD_WINDOW_DAYS = 28          # 4-week window (change to 14 if your cadence is bi-weekly)
TODAY_OVERRIDE = None         # e.g. "2026-02-12" or None
ASOF_OVERRIDE = None          # e.g. "2026-02-08" or None (if None: uses GLOBAL_LSD)
NEXT_WINDOW_DAYS = 28         # used for "Next Mo" BCWS/ETC totals

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

def as_date(x):
    if x is None: return None
    if isinstance(x, date) and not isinstance(x, datetime): return x
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    return pd.to_datetime(x, errors="coerce").date()

def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def normalize_program(x):
    if pd.isna(x): return None
    s = str(x).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_product_team(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"[^A-Z0-9]+", "", s)   # keeps KUW stable
    return s if s else None

def normalize_cost_set(x):
    if pd.isna(x): return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("-", "").replace("_", "")
    return s

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
# COERCE INPUT (no remapping assumptions beyond required meanings)
# -------------------------
def coerce_to_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

    colmap = {}

    # PROGRAM
    for c in ["PROGRAM","PROG","PROJECT","PROGRAM_NAME"]:
        if c in df.columns: colmap[c] = "PROGRAM"; break

    # PRODUCT TEAM
    for c in ["PRODUCT_TEAM","PRODUCTTEAM","SUB_TEAM","SUBTEAM","IPT","IPT_NAME"]:
        if c in df.columns: colmap[c] = "PRODUCT_TEAM"; break

    # DATE
    for c in ["DATE","PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
        if c in df.columns: colmap[c] = "DATE"; break

    # COST SET
    for c in ["COST_SET","COSTSET","COST_SET_NAME","COST_CATEGORY","COSTSETNAME","COST_SET_TYPE"]:
        if c in df.columns: colmap[c] = "COST_SET"; break

    # HOURS / VALUE
    for c in ["HOURS","HRS","VALUE","AMOUNT","TOTAL_HOURS"]:
        if c in df.columns: colmap[c] = "HOURS"; break

    df = df.rename(columns=colmap)

    required = ["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"cobra_merged_df missing required columns: {missing}. Found: {list(df.columns)}")

    df["PROGRAM"] = df["PROGRAM"].map(normalize_program)
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].map(normalize_product_team)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

# ============================================================
# START: cobra_merged_df ONLY
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or is empty. Put your cleaned long Cobra data into cobra_merged_df first.")

today = as_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()

base = coerce_to_long(cobra_merged_df)

# Keep only EVMS cost sets we need (mapping assumed already done upstream)
NEEDED = ["BCWS","BCWP","ACWP","ETC"]
base = base[base["COST_SET"].isin(NEEDED)].copy()
if len(base) == 0:
    raise ValueError("After filtering to EVMS cost sets (BCWS/BCWP/ACWP/ETC), no rows remain. Check COST_SET values.")

print(f"TODAY: {today}")
print(f"Rows in EVMS (BCWS/BCWP/ACWP/ETC): {len(base)}")
print(f"Programs: {base['PROGRAM'].nunique()} | Product Teams: {base['PRODUCT_TEAM'].nunique()}")

# ------------------------------------------------------------
# 1) Aggregate to one row per (PROGRAM, PRODUCT_TEAM, COST_SET, DATE)
#    This is CRITICAL: totals at a DATE represent the snapshot total at that DATE.
# ------------------------------------------------------------
snap = (
    base.groupby(["PROGRAM","PRODUCT_TEAM","COST_SET","DATE"], as_index=False)["HOURS"]
        .sum()
        .rename(columns={"HOURS":"VAL"})
)

# ------------------------------------------------------------
# 2) Determine LSD_END (status date) and PREV_DATE using a consistent window
#    - GLOBAL_LSD = max DATE in data <= today
#    - AS_OF_DATE = override or GLOBAL_LSD
#    - PREV_DATE  = max DATE < AS_OF_DATE and >= AS_OF_DATE - LSD_WINDOW_DAYS
#      If none exists in that window, use the immediate prior DATE (if exists).
# ------------------------------------------------------------
all_dates = sorted({d for d in snap["DATE"].unique() if d is not None})
if len(all_dates) == 0:
    raise ValueError("No valid DATE values after coercion.")

global_lsd = max([d for d in all_dates if d <= today], default=max(all_dates))
AS_OF_DATE = as_date(ASOF_OVERRIDE) if ASOF_OVERRIDE else global_lsd

# clamp AS_OF_DATE to available dates if user picks a non-existing day
if AS_OF_DATE not in set(all_dates):
    # choose latest date <= AS_OF_DATE
    candidates = [d for d in all_dates if d <= AS_OF_DATE]
    if len(candidates) == 0:
        AS_OF_DATE = min(all_dates)
    else:
        AS_OF_DATE = max(candidates)

window_start = AS_OF_DATE - timedelta(days=LSD_WINDOW_DAYS)
prev_candidates = [d for d in all_dates if (d < AS_OF_DATE and d >= window_start)]
if len(prev_candidates) > 0:
    PREV_DATE = max(prev_candidates)
else:
    # fallback: immediate prior date in the full series
    prior = [d for d in all_dates if d < AS_OF_DATE]
    PREV_DATE = max(prior) if len(prior) else None

print(f"GLOBAL_LSD (max DATE in data <= today): {global_lsd}")
print(f"AS_OF_DATE (used for CTD): {AS_OF_DATE}")
print(f"LSD window start: {window_start}")
print(f"PREV_DATE (used for LSD delta): {PREV_DATE}")

# ------------------------------------------------------------
# 3) Build CTD snapshot at AS_OF_DATE and PREV snapshot at PREV_DATE
# ------------------------------------------------------------
ctd = snap[snap["DATE"] == AS_OF_DATE].copy()
ctd = ctd.rename(columns={"VAL":"CTD_VAL"}).drop(columns=["DATE"])

if PREV_DATE is not None:
    prv = snap[snap["DATE"] == PREV_DATE].copy()
    prv = prv.rename(columns={"VAL":"PRV_VAL"}).drop(columns=["DATE"])
else:
    prv = snap.head(0)[["PROGRAM","PRODUCT_TEAM","COST_SET"]].copy()
    prv["PRV_VAL"] = np.nan

# full outer so we don't lose teams like KUW if missing on one side
pair = (
    ctd.merge(prv, on=["PROGRAM","PRODUCT_TEAM","COST_SET"], how="outer")
)
pair["CTD_VAL"] = _to_num(pair["CTD_VAL"])
pair["PRV_VAL"] = _to_num(pair["PRV_VAL"])
pair["LSD_VAL"] = pair["CTD_VAL"] - pair["PRV_VAL"]

# ------------------------------------------------------------
# 4) Pivot to wide for easier ratios
# ------------------------------------------------------------
def pivot_cs(df, val_col):
    pv = df.pivot_table(
        index=["PROGRAM","PRODUCT_TEAM"],
        columns="COST_SET",
        values=val_col,
        aggfunc="sum"
    ).reset_index()
    for cs in NEEDED:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

pt_ctd = pivot_cs(pair, "CTD_VAL")
pt_lsd = pivot_cs(pair, "LSD_VAL")

# Program-level snapshots (sum across product teams)
prog_ctd = (
    pair.groupby(["PROGRAM","COST_SET"], as_index=False)["CTD_VAL"].sum()
        .pivot_table(index="PROGRAM", columns="COST_SET", values="CTD_VAL", aggfunc="sum")
        .reset_index()
)
prog_lsd = (
    pair.groupby(["PROGRAM","COST_SET"], as_index=False)["LSD_VAL"].sum()
        .pivot_table(index="PROGRAM", columns="COST_SET", values="LSD_VAL", aggfunc="sum")
        .reset_index()
)
for df_ in [prog_ctd, prog_lsd]:
    for cs in NEEDED:
        if cs not in df_.columns:
            df_[cs] = np.nan

# ------------------------------------------------------------
# 5) Program Overview (WIDE like your screenshot)
# ------------------------------------------------------------
Program_Overview = prog_ctd.merge(prog_lsd, on="PROGRAM", how="outer", suffixes=("_CTD","_LSD"))
Program_Overview = Program_Overview.rename(columns={"PROGRAM":"ProgramID"})

# Ratios
Program_Overview["SPI_CTD"] = safe_div(Program_Overview["BCWP_CTD"], Program_Overview["BCWS_CTD"])
Program_Overview["CPI_CTD"] = safe_div(Program_Overview["BCWP_CTD"], Program_Overview["ACWP_CTD"])
Program_Overview["SPI_LSD"] = safe_div(Program_Overview["BCWP_LSD"], Program_Overview["BCWS_LSD"])
Program_Overview["CPI_LSD"] = safe_div(Program_Overview["BCWP_LSD"], Program_Overview["ACWP_LSD"])

# Color columns
Program_Overview["SPI_LSD_Color"] = pd.Series(Program_Overview["SPI_LSD"]).map(color_spi_cpi)
Program_Overview["SPI_CTD_Color"] = pd.Series(Program_Overview["SPI_CTD"]).map(color_spi_cpi)
Program_Overview["CPI_LSD_Color"] = pd.Series(Program_Overview["CPI_LSD"]).map(color_spi_cpi)
Program_Overview["CPI_CTD_Color"] = pd.Series(Program_Overview["CPI_CTD"]).map(color_spi_cpi)

# Add dates (to validate in Excel / PowerBI)
Program_Overview["LSD_START"] = window_start
Program_Overview["LSD_END"]   = AS_OF_DATE
Program_Overview["AS_OF_DATE"] = AS_OF_DATE
Program_Overview["PREV_DATE"] = PREV_DATE

comment_overview = "Cause & Corrective Actions"
Program_Overview[comment_overview] = ""
Program_Overview = Program_Overview[
    ["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "LSD_START","LSD_END","AS_OF_DATE","PREV_DATE",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     comment_overview]
].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID"], comment_overview)

# ------------------------------------------------------------
# 6) Product Team SPI/CPI
# ------------------------------------------------------------
pt = pt_ctd.merge(pt_lsd, on=["PROGRAM","PRODUCT_TEAM"], how="outer", suffixes=("_CTD","_LSD"))
pt = pt.rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

ProductTeam_SPI_CPI = pt[["ProgramID","Product Team","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]].copy()
ProductTeam_SPI_CPI["SPI_LSD_Color"] = pd.Series(ProductTeam_SPI_CPI["SPI_LSD"]).map(color_spi_cpi)
ProductTeam_SPI_CPI["SPI_CTD_Color"] = pd.Series(ProductTeam_SPI_CPI["SPI_CTD"]).map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_LSD_Color"] = pd.Series(ProductTeam_SPI_CPI["CPI_LSD"]).map(color_spi_cpi)
ProductTeam_SPI_CPI["CPI_CTD_Color"] = pd.Series(ProductTeam_SPI_CPI["CPI_CTD"]).map(color_spi_cpi)
ProductTeam_SPI_CPI["LSD_START"] = window_start
ProductTeam_SPI_CPI["LSD_END"]   = AS_OF_DATE
ProductTeam_SPI_CPI["AS_OF_DATE"] = AS_OF_DATE
ProductTeam_SPI_CPI["PREV_DATE"] = PREV_DATE

comment_pt = "Cause & Corrective Actions"
ProductTeam_SPI_CPI[comment_pt] = ""
ProductTeam_SPI_CPI = ProductTeam_SPI_CPI.sort_values(["ProgramID","Product Team"]).reset_index(drop=True)
ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt)

# ------------------------------------------------------------
# 7) Product Team BAC/EAC/VAC
#    BAC = final baseline snapshot (BCWS at max DATE for that ProgramID/Product Team)
#    EAC = ACWP_CTD + ETC_CTD (as-of snapshot)
# ------------------------------------------------------------
# BAC snapshot at final date per Program/ProductTeam (not "sum of year")
bcws_series = snap[snap["COST_SET"] == "BCWS"].copy()
if len(bcws_series) == 0:
    bac = snap.head(0)[["PROGRAM","PRODUCT_TEAM"]].copy()
    bac["BAC"] = np.nan
else:
    # total BCWS snapshot per date already aggregated (snap)
    # take last available DATE per (PROGRAM, PRODUCT_TEAM)
    bcws_last_date = (
        bcws_series.groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["DATE"].max()
                 .rename(columns={"DATE":"BAC_DATE"})
    )
    bac = bcws_series.merge(bcws_last_date, on=["PROGRAM","PRODUCT_TEAM"], how="inner")
    bac = bac[bac["DATE"] == bac["BAC_DATE"]].copy()
    bac = bac.rename(columns={"VAL":"BAC"}).drop(columns=["COST_SET","DATE","BAC_DATE"])

# ACWP/ETC CTD snapshots (from pt_ctd)
acwp_ctd = pt_ctd[["PROGRAM","PRODUCT_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = pt_ctd[["PROGRAM","PRODUCT_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})

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
ProductTeam_BAC_EAC_VAC["AS_OF_DATE"] = AS_OF_DATE
ProductTeam_BAC_EAC_VAC[comment_pt] = ""
ProductTeam_BAC_EAC_VAC = ProductTeam_BAC_EAC_VAC[
    ["ProgramID","Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color","AS_OF_DATE",comment_pt]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

ProductTeam_BAC_EAC_VAC = preserve_comments(
    OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC,
    ["ProgramID","Product Team"], comment_pt
)

# ------------------------------------------------------------
# 8) Program Manpower
#    Demand Hours = BCWS_CTD snapshot at AS_OF_DATE
#    Actual Hours = ACWP_CTD snapshot at AS_OF_DATE
#    % Var = Actual / Demand * 100
#    Next Mo BCWS/ETC = sum of deltas over (AS_OF_DATE, AS_OF_DATE+NEXT_WINDOW_DAYS]
# ------------------------------------------------------------
# Program snapshots at AS_OF_DATE
prog_man = prog_ctd.rename(columns={"PROGRAM":"ProgramID"}).copy()
prog_man["Demand Hours"] = _to_num(prog_man["BCWS"])
prog_man["Actual Hours"] = _to_num(prog_man["ACWP"])
prog_man["% Var"] = safe_div(prog_man["Actual Hours"], prog_man["Demand Hours"]) * 100.0
prog_man["% Var Color"] = prog_man["% Var"].map(color_manpower_pct)

# Next-window deltas: use snapshot difference between AS_OF_DATE and the end-of-next-window date
end_next = AS_OF_DATE + timedelta(days=NEXT_WINDOW_DAYS)
# choose the closest available date <= end_next
end_candidates = [d for d in all_dates if d <= end_next]
END_NEXT_DATE = max(end_candidates) if len(end_candidates) else AS_OF_DATE

# snapshot at END_NEXT_DATE
ctd_next = snap[snap["DATE"] == END_NEXT_DATE].copy().rename(columns={"VAL":"NEXT_VAL"}).drop(columns=["DATE"])
# join with as-of snapshot for delta
asof_prog = snap[snap["DATE"] == AS_OF_DATE].copy().rename(columns={"VAL":"ASOF_VAL"}).drop(columns=["DATE"])

delta = (
    ctd_next.merge(asof_prog, on=["PROGRAM","PRODUCT_TEAM","COST_SET"], how="outer")
)
delta["NEXT_VAL"] = _to_num(delta["NEXT_VAL"])
delta["ASOF_VAL"] = _to_num(delta["ASOF_VAL"])
delta["DELTA"] = delta["NEXT_VAL"] - delta["ASOF_VAL"]

next_prog = (
    delta[delta["COST_SET"].isin(["BCWS","ETC"])]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["DELTA"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="DELTA", aggfunc="sum")
    .reset_index()
)
for cs in ["BCWS","ETC"]:
    if cs not in next_prog.columns:
        next_prog[cs] = np.nan

next_prog = next_prog.rename(columns={
    "PROGRAM":"ProgramID",
    "BCWS":"Next Mo BCWS Hours",
    "ETC":"Next Mo ETC Hours"
})
next_prog["NEXT_WINDOW_END_DATE"] = END_NEXT_DATE

Program_Manpower = prog_man.merge(
    next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours","NEXT_WINDOW_END_DATE"]],
    on="ProgramID", how="left"
)

Program_Manpower["AS_OF_DATE"] = AS_OF_DATE
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color",
     "Next Mo BCWS Hours","Next Mo ETC Hours",
     "AS_OF_DATE","NEXT_WINDOW_END_DATE",comment_pt]
].sort_values(["ProgramID"]).reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ------------------------------------------------------------
# 9) Quick KUW diagnostics (to prove it isn't being dropped)
# ------------------------------------------------------------
print("\nKUW check (rows in ProductTeam_BAC_EAC_VAC for KUW):")
kuw_bac = ProductTeam_BAC_EAC_VAC[ProductTeam_BAC_EAC_VAC["Product Team"]=="KUW"]
print(kuw_bac.head(10) if len(kuw_bac) else "No KUW rows found in BAC/EAC/VAC output (check PRODUCT_TEAM values in base).")

print("\nKUW check (rows in ProductTeam_SPI_CPI for KUW):")
kuw_spi = ProductTeam_SPI_CPI[ProductTeam_SPI_CPI["Product Team"]=="KUW"]
print(kuw_spi.head(10) if len(kuw_spi) else "No KUW rows found in SPI/CPI output (check PRODUCT_TEAM values in base).")

# ------------------------------------------------------------
# 10) Write Excel
# ------------------------------------------------------------
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
3) Conditional formatting (Background color -> Format by Field value):
   - SPI_LSD uses SPI_LSD_Color
   - SPI_CTD uses SPI_CTD_Color
   - CPI_LSD uses CPI_LSD_Color
   - CPI_CTD uses CPI_CTD_Color
4) Turn off totals/subtotals for the visual.
5) Set numeric formatting to 2 decimals.
""")