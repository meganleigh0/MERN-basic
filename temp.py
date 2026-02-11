# ============================================================
# EVMS -> PowerBI Excel (ONE CELL, FIXED + COLOR COLUMNS)
# Updates:
# - Adds VAC coloring (VAC/BAC thresholds) in SubTeam_BAC_EAC_VAC
# - Adds % Var coloring (Program Manpower thresholds) in Program_Manpower
# - Renames SubTeam -> Product Team in outputs
# - Re-saves ONE Excel with 4 sheets
# ============================================================

import os, re
from pathlib import Path
from datetime import date, datetime, timedelta
import numpy as np
import pandas as pd

# -------------------------
# SETTINGS
# -------------------------
PROGRAMS_KEEP = ["ABRAMS_22", "OLYMPUS", "STRYKER_BULG", "XM30"]
TODAY_OVERRIDE = None  # e.g. "2026-02-10" for testing
OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")

# If BCWP/ACWP represent 2 weeks but BCWS is effectively 1 week, scale BCWS up.
BCWS_SCALE_FACTOR = 2.0

FORCE_READ_FILES = False
INPUT_FILES = []  # optional explicit list; if empty, auto-discover or use in-memory df

# -------------------------
# GDLS COLOR PALETTE (from PPT)
# -------------------------
CLR_DARK_BLUE  = "#1F497D"  # RGB 031,073,125
CLR_LIGHT_BLUE = "#8EB4E3"  # RGB 142,180,227
CLR_GREEN      = "#339966"  # RGB 051,153,102
CLR_YELLOW     = "#FFFF99"  # RGB 255,255,153
CLR_RED        = "#C0504D"  # RGB 192,080,077

# -------------------------
# THRESHOLD FUNCTIONS (match PPT "adjustment for rounding" bands)
# -------------------------
def color_spi_cpi_bei(x):
    # Blue: x >= 1.055 | Green: >=0.975 | Yellow: >=0.945 | Red: <0.945
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
    return CLR_RED

def color_program_manpower(pct):
    # Red: >=109.5 | Yellow: >=105.5 | Green: >=89.5 | Yellow: >=85.5 | Red: <85.5
    pct = pd.to_numeric(pct, errors="coerce")
    if pd.isna(pct): return None
    if pct >= 109.5: return CLR_RED
    if pct >= 105.5: return CLR_YELLOW
    if pct >= 89.5:  return CLR_GREEN
    if pct >= 85.5:  return CLR_YELLOW
    return CLR_RED

def color_vac_over_bac(x):
    # Blue: >= +0.055 | Green: >= -0.025 | Yellow: >= -0.055 | Red: < -0.055
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 0.055:   return CLR_LIGHT_BLUE
    if x >= -0.025:  return CLR_GREEN
    if x >= -0.055:  return CLR_YELLOW
    return CLR_RED

# -------------------------
# HELPERS
# -------------------------
def normalize_key(s):
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", " ", s)
    s = s.replace("-", " ").replace("_", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def normalize_cost_set(s):
    if pd.isna(s): return None
    s = str(s).strip().upper()
    s = re.sub(r"\s+", "", s).replace("-", "").replace("_", "")
    aliases = {"BCWS":"BCWS","BCWP":"BCWP","ACWP":"ACWP","ETC":"ETC","EAC":"EAC","BAC":"BAC","VAC":"VAC"}
    return aliases.get(s, s)

def safe_div(a, b):
    a = pd.to_numeric(a, errors="coerce")
    b = pd.to_numeric(b, errors="coerce")
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

def _to_date(x):
    if x is None or (isinstance(x, float) and np.isnan(x)): return None
    if isinstance(x, (datetime, pd.Timestamp)): return x.date()
    if isinstance(x, date): return x
    return pd.to_datetime(x, errors="coerce").date()

def last_thursday_of_month(year: int, month: int) -> date:
    last = date(year, 12, 31) if month == 12 else (date(year, month+1, 1) - timedelta(days=1))
    offset = (last.weekday() - 3) % 7  # Thu=3
    return last - timedelta(days=offset)

def last_thursday_prev_month(d: date) -> date:
    y, m = d.year, d.month
    if m == 1: y, m = y-1, 12
    else: m -= 1
    return last_thursday_of_month(y, m)

def add_month(d: date, months: int = 1) -> date:
    y, m = d.year, d.month + months
    while m > 12: y, m = y+1, m-12
    while m < 1:  y, m = y-1, m+12
    last_day = 31 if m == 12 else (date(y, m+1, 1) - timedelta(days=1)).day
    return date(y, m, min(d.day, last_day))

def coerce_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    colmap = {c: str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in df.columns}
    df.rename(columns=colmap, inplace=True)

    if "PROGRAM" not in df.columns:
        for c in ["PROGRAMID","PROG","PROJECT","IPT_PROGRAM"]:
            if c in df.columns: df.rename(columns={c:"PROGRAM"}, inplace=True); break

    if "SUB_TEAM" not in df.columns:
        for c in ["SUBTEAM","SUB_TEAM_NAME","IPT","IPT_NAME","CONTROL_ACCOUNT","CA","SUBTEAM_NAME"]:
            if c in df.columns: df.rename(columns={c:"SUB_TEAM"}, inplace=True); break

    if "DATE" not in df.columns:
        for c in ["PERIOD_END","PERIODEND","STATUS_DATE","AS_OF_DATE"]:
            if c in df.columns: df.rename(columns={c:"DATE"}, inplace=True); break

    if "COST_SET" not in df.columns:
        for c in ["COSTSET","COST-SET","COST_SET_NAME","COST_CATEGORY"]:
            if c in df.columns: df.rename(columns={c:"COST_SET"}, inplace=True); break

    if "HOURS" not in df.columns:
        for c in ["VALUE","AMOUNT","HRS","HOURS_WORKED","TOTAL_HOURS"]:
            if c in df.columns: df.rename(columns={c:"HOURS"}, inplace=True); break

    required = ["PROGRAM","SUB_TEAM","DATE","COST_SET","HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns {missing}. Found: {list(df.columns)}")

    df["PROGRAM"]  = df["PROGRAM"].map(normalize_key)
    df["SUB_TEAM"] = df["SUB_TEAM"].map(normalize_key)
    df["COST_SET"] = df["COST_SET"].map(normalize_cost_set)
    df["DATE"]     = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"]    = pd.to_numeric(df["HOURS"], errors="coerce")

    return df.dropna(subset=["PROGRAM","SUB_TEAM","DATE","COST_SET","HOURS"])

def load_inputs() -> pd.DataFrame:
    if not FORCE_READ_FILES:
        for name in ["cobra_merged_df","cobra_df","df","raw_df"]:
            if name in globals() and isinstance(globals()[name], pd.DataFrame) and len(globals()[name]) > 0:
                return coerce_columns(globals()[name])

    files = list(INPUT_FILES)
    if not files:
        candidates = []
        for pat in ["*.csv","*.xlsx","*.xls"]:
            candidates += list(Path(".").glob(pat))
        candidates = sorted(candidates, key=lambda p: ("cobra" not in p.name.lower(), p.name.lower()))
        files = [str(p) for p in candidates[:30]]

    if not files:
        raise FileNotFoundError("No input files found and no in-memory dataframe found (cobra_merged_df/df/...).")

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

def pivot_costsets(df: pd.DataFrame, idx_cols, val_col, needed_costsets):
    if df.empty:
        out = df[idx_cols].drop_duplicates().copy()
        for cs in needed_costsets: out[cs] = np.nan
        return out
    pv = df.pivot_table(index=idx_cols, columns="COST_SET", values=val_col, aggfunc="sum").reset_index()
    for cs in needed_costsets:
        if cs not in pv.columns: pv[cs] = np.nan
    return pv

def preserve_comments(existing_path: Path, sheet: str, df_new: pd.DataFrame, key_cols, comment_col):
    if (not existing_path.exists()) or (comment_col not in df_new.columns):
        return df_new
    try:
        old = pd.read_excel(existing_path, sheet_name=sheet)
    except Exception:
        return df_new
    if old is None or len(old) == 0: return df_new
    if (comment_col not in old.columns) or (not all(k in old.columns for k in key_cols)):
        return df_new

    old = old[key_cols + [comment_col]].copy().dropna(subset=key_cols)
    old = old.rename(columns={comment_col: f"{comment_col}_old"})
    out = df_new.merge(old, on=key_cols, how="left")
    if f"{comment_col}_old" in out.columns:
        oldvals = out[f"{comment_col}_old"]
        mask = oldvals.notna() & (oldvals.astype(str).str.strip() != "")
        out.loc[mask, comment_col] = oldvals.loc[mask]
        out = out.drop(columns=[f"{comment_col}_old"])
    return out

# -------------------------
# LOAD + FILTER
# -------------------------
base = load_inputs()
base = base[base["PROGRAM"].isin([normalize_key(p) for p in PROGRAMS_KEEP])].copy()

# -------------------------
# AS-OF / NEXT PERIOD
# -------------------------
today = _to_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = last_thursday_prev_month(today)
month_after = add_month(AS_OF_DATE, 1)
NEXT_PERIOD_END = last_thursday_of_month(month_after.year, month_after.month)

YEAR_FILTER = AS_OF_DATE.year
YEAR_START  = date(YEAR_FILTER, 1, 1)
YEAR_END    = date(YEAR_FILTER, 12, 31)

print("As-of logic")
print("TODAY:", today)
print("AS_OF_DATE:", AS_OF_DATE)
print("NEXT_PERIOD_END:", NEXT_PERIOD_END)
print("YEAR_FILTER:", YEAR_FILTER)
print("BCWS_SCALE_FACTOR:", BCWS_SCALE_FACTOR)

# -------------------------
# CORE FILTERS
# -------------------------
NEEDED_COSTSETS = ["BCWS","BCWP","ACWP","ETC"]

base_to_asof = base[base["DATE"] <= AS_OF_DATE].copy()
base_year    = base[(base["DATE"] >= YEAR_START) & (base["DATE"] <= YEAR_END)].copy()

# -------------------------
# CTD (sum up to AS_OF_DATE)
# -------------------------
ctd_sub = (
    base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)]
    .groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)
ctd_prog = (
    base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# -------------------------
# LSD FIX (per COST_SET): latest DATE <= AS_OF_DATE for each key
# -------------------------
tmp_sub = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].sort_values(["PROGRAM","SUB_TEAM","COST_SET","DATE"]).copy()
sub_last_date = tmp_sub.groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["DATE"].max().rename(columns={"DATE":"LSD_DATE"})
lsd_sub = (
    tmp_sub.merge(sub_last_date, on=["PROGRAM","SUB_TEAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","SUB_TEAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

tmp_prog = base_to_asof[base_to_asof["COST_SET"].isin(NEEDED_COSTSETS)].sort_values(["PROGRAM","COST_SET","DATE"]).copy()
prog_last_date = tmp_prog.groupby(["PROGRAM","COST_SET"], as_index=False)["DATE"].max().rename(columns={"DATE":"LSD_DATE"})
lsd_prog = (
    tmp_prog.merge(prog_last_date, on=["PROGRAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

# -------------------------
# PIVOT COSTSETS
# -------------------------
ctd_sub_p  = pivot_costsets(ctd_sub,  ["PROGRAM","SUB_TEAM"], "CTD_HRS", NEEDED_COSTSETS)
lsd_sub_p  = pivot_costsets(lsd_sub,  ["PROGRAM","SUB_TEAM"], "LSD_HRS", NEEDED_COSTSETS)
ctd_prog_p = pivot_costsets(ctd_prog, ["PROGRAM"],          "CTD_HRS", NEEDED_COSTSETS)
lsd_prog_p = pivot_costsets(lsd_prog, ["PROGRAM"],          "LSD_HRS", NEEDED_COSTSETS)

# -------------------------
# BCWS SCALE FIX (keeps SPI from starting near ~2)
# -------------------------
for dfp in [ctd_sub_p, lsd_sub_p, ctd_prog_p, lsd_prog_p]:
    dfp["BCWS"] = pd.to_numeric(dfp["BCWS"], errors="coerce") * float(BCWS_SCALE_FACTOR)

# -------------------------
# PROGRAM OVERVIEW (SPI + CPI only; NO BEI)
# EXACT headers (+ color cols):
# ProgramID | Metric | CTD | LSD | CTD_Color | LSD_Color | Comments / Root Cause & Corrective Actions
# -------------------------
prog = ctd_prog_p.merge(lsd_prog_p, on=["PROGRAM"], how="outer", suffixes=("_CTD","_LSD")).rename(columns={"PROGRAM":"ProgramID"})

prog_spi_ctd = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog_spi_lsd = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog_cpi_ctd = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog_cpi_lsd = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

program_overview = pd.concat(
    [
        pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric":"SPI", "CTD":prog_spi_ctd, "LSD":prog_spi_lsd}),
        pd.DataFrame({"ProgramID": prog["ProgramID"], "Metric":"CPI", "CTD":prog_cpi_ctd, "LSD":prog_cpi_lsd}),
    ],
    ignore_index=True
)
program_overview["CTD_Color"] = program_overview["CTD"].map(color_spi_cpi_bei)
program_overview["LSD_Color"] = program_overview["LSD"].map(color_spi_cpi_bei)

comment_col_prog = "Comments / Root Cause & Corrective Actions"
program_overview[comment_col_prog] = ""
program_overview = program_overview[
    ["ProgramID","Metric","CTD","LSD","CTD_Color","LSD_Color",comment_col_prog]
].sort_values(["ProgramID","Metric"]).reset_index(drop=True)

# -------------------------
# PRODUCT TEAM SPI/CPI (renamed from SubTeam)
# EXACT headers (+ color cols):
# Product Team | SPI LSD | SPI CTD | CPI LSD | CPI CTD
# | SPI LSD Color | SPI CTD Color | CPI LSD Color | CPI CTD Color
# | Cause & Corrective Actions | ProgramID
# -------------------------
sub = ctd_sub_p.merge(lsd_sub_p, on=["PROGRAM","SUB_TEAM"], how="outer", suffixes=("_CTD","_LSD")).rename(columns={"PROGRAM":"ProgramID","SUB_TEAM":"Product Team"})

sub_spi_ctd = safe_div(sub["BCWP_CTD"], sub["BCWS_CTD"])
sub_spi_lsd = safe_div(sub["BCWP_LSD"], sub["BCWS_LSD"])
sub_cpi_ctd = safe_div(sub["BCWP_CTD"], sub["ACWP_CTD"])
sub_cpi_lsd = safe_div(sub["BCWP_LSD"], sub["ACWP_LSD"])

product_team_spi_cpi = pd.DataFrame({
    "Product Team": sub["Product Team"],
    "SPI LSD": sub_spi_lsd,
    "SPI CTD": sub_spi_ctd,
    "CPI LSD": sub_cpi_lsd,
    "CPI CTD": sub_cpi_ctd,
    "SPI LSD Color": pd.Series(sub_spi_lsd).map(color_spi_cpi_bei),
    "SPI CTD Color": pd.Series(sub_spi_ctd).map(color_spi_cpi_bei),
    "CPI LSD Color": pd.Series(sub_cpi_lsd).map(color_spi_cpi_bei),
    "CPI CTD Color": pd.Series(sub_cpi_ctd).map(color_spi_cpi_bei),
    "Cause & Corrective Actions": "",
    "ProgramID": sub["ProgramID"],
}).sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

# -------------------------
# PRODUCT TEAM BAC/EAC/VAC  (VAC coloring added)
# EXACT headers (+ new):
# Product Team | BAC | EAC | VAC | VAC_BAC | VAC_Color | Cause & Corrective Actions | ProgramID
# BAC = YEAR total BCWS (scaled)
# VAC_BAC = VAC / BAC  (ratio for thresholding)
# VAC_Color based on VAC/BAC thresholds (PPT)
# -------------------------
bcws_year = (
    base_year[base_year["COST_SET"] == "BCWS"]
    .groupby(["PROGRAM","SUB_TEAM"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"BAC"})
)
bcws_year["BAC"] = pd.to_numeric(bcws_year["BAC"], errors="coerce") * float(BCWS_SCALE_FACTOR)

acwp_ctd = ctd_sub_p[["PROGRAM","SUB_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD"})
etc_ctd  = ctd_sub_p[["PROGRAM","SUB_TEAM","ETC"]].rename(columns={"ETC":"ETC_CTD"})
eac = acwp_ctd.merge(etc_ctd, on=["PROGRAM","SUB_TEAM"], how="outer")
eac["ACWP_CTD"] = pd.to_numeric(eac["ACWP_CTD"], errors="coerce").fillna(0.0)
eac["ETC_CTD"]  = pd.to_numeric(eac["ETC_CTD"],  errors="coerce").fillna(0.0)
eac["EAC"] = eac["ACWP_CTD"] + eac["ETC_CTD"]

bac_eac = bcws_year.merge(eac[["PROGRAM","SUB_TEAM","EAC"]], on=["PROGRAM","SUB_TEAM"], how="outer")
bac_eac["BAC"] = pd.to_numeric(bac_eac["BAC"], errors="coerce")
bac_eac["EAC"] = pd.to_numeric(bac_eac["EAC"], errors="coerce")
bac_eac["VAC"] = bac_eac["BAC"] - bac_eac["EAC"]

# VAC/BAC ratio and color (this is what PPT thresholds are defined on)
bac_eac["VAC_BAC"] = safe_div(bac_eac["VAC"], bac_eac["BAC"])
bac_eac["VAC_Color"] = pd.Series(bac_eac["VAC_BAC"]).map(color_vac_over_bac)

product_team_bac_eac_vac = bac_eac.rename(columns={"PROGRAM":"ProgramID","SUB_TEAM":"Product Team"}).copy()
product_team_bac_eac_vac["Cause & Corrective Actions"] = ""
product_team_bac_eac_vac = product_team_bac_eac_vac[
    ["Product Team","BAC","EAC","VAC","VAC_BAC","VAC_Color","Cause & Corrective Actions","ProgramID"]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

# -------------------------
# PROGRAM MANPOWER (Hours)  (% Var coloring added)
# EXACT headers (+ new):
# ProgramID | Demand Hours | Actual Hours | % Var | % Var Color | Next Mo BCWS Hours | Next Mo ETC Hours | Cause & Corrective Actions
# % Var = (Actual / Demand)*100
# % Var Color uses PPT Program Manpower thresholds
# -------------------------
man = ctd_prog_p.rename(columns={"PROGRAM":"ProgramID","BCWS":"Demand Hours","ACWP":"Actual Hours"}).copy()
man["Demand Hours"] = pd.to_numeric(man["Demand Hours"], errors="coerce")
man["Actual Hours"] = pd.to_numeric(man["Actual Hours"], errors="coerce")
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_program_manpower)

next_window = base[(base["DATE"] > AS_OF_DATE) & (base["DATE"] <= NEXT_PERIOD_END) & (base["COST_SET"].isin(["BCWS","ETC"]))].copy()
next_prog = (
    next_window.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
    .reset_index()
)
if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan
next_prog["BCWS"] = pd.to_numeric(next_prog["BCWS"], errors="coerce") * float(BCWS_SCALE_FACTOR)

next_prog = next_prog.rename(columns={"PROGRAM":"ProgramID","BCWS":"Next Mo BCWS Hours","ETC":"Next Mo ETC Hours"})
program_manpower = man.merge(next_prog[["ProgramID","Next Mo BCWS Hours","Next Mo ETC Hours"]], on="ProgramID", how="left")
program_manpower["Cause & Corrective Actions"] = ""
program_manpower = program_manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next Mo BCWS Hours","Next Mo ETC Hours","Cause & Corrective Actions"]
].sort_values(["ProgramID"]).reset_index(drop=True)

# -------------------------
# PRESERVE EXISTING COMMENTS (key cols updated for "Product Team")
# -------------------------
program_overview = preserve_comments(
    OUTPUT_XLSX, "Program_Overview", program_overview,
    key_cols=["ProgramID","Metric"], comment_col="Comments / Root Cause & Corrective Actions"
)
product_team_spi_cpi = preserve_comments(
    OUTPUT_XLSX, "SubTeam_SPI_CPI", product_team_spi_cpi,   # keep sheet name stable unless you want it renamed
    key_cols=["ProgramID","Product Team"], comment_col="Cause & Corrective Actions"
)
product_team_bac_eac_vac = preserve_comments(
    OUTPUT_XLSX, "SubTeam_BAC_EAC_VAC", product_team_bac_eac_vac,  # keep sheet name stable unless you want it renamed
    key_cols=["ProgramID","Product Team"], comment_col="Cause & Corrective Actions"
)
program_manpower = preserve_comments(
    OUTPUT_XLSX, "Program_Manpower", program_manpower,
    key_cols=["ProgramID"], comment_col="Cause & Corrective Actions"
)

# -------------------------
# WRITE ONE EXCEL (PowerBI)  (re-saves)
# -------------------------
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    program_overview.to_excel(writer, sheet_name="Program_Overview", index=False)
    product_team_spi_cpi.to_excel(writer, sheet_name="SubTeam_SPI_CPI", index=False)
    product_team_bac_eac_vac.to_excel(writer, sheet_name="SubTeam_BAC_EAC_VAC", index=False)
    program_manpower.to_excel(writer, sheet_name="Program_Manpower", index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")

# -------------------------
# QUICK DIAGNOSTICS
# -------------------------
print("\nSanity checks:")
print("Program_Overview columns:", list(program_overview.columns))
print("SPI/CPI colors null rate:",
      float(program_overview["CTD_Color"].isna().mean()),
      float(program_overview["LSD_Color"].isna().mean()))
print("Product Team BAC/EAC/VAC colors null rate:", float(product_team_bac_eac_vac["VAC_Color"].isna().mean()))
print("Program_Manpower %Var colors null rate:", float(program_manpower["% Var Color"].isna().mean()))