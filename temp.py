# ============================================================
# EVMS -> PowerBI "Program Overview Card" Export (ONE CELL)
# Output table is ONE ROW per Program with SPI/CPI as COLUMNS:
# ProgramID | SPI_CTD | SPI_LSD | SPI_CTD_Color | SPI_LSD_Color
#          | CPI_CTD | CPI_LSD | CPI_CTD_Color | CPI_LSD_Color
#          | Comments / Root Cause & Corrective Actions
#
# - Hardcodes 4 programs
# - As-of = last Thursday of previous month (relative to TODAY)
# - LSD FIX per COST_SET: latest available DATE <= AS_OF_DATE for (Program, Cost_Set)
# - BCWS_SCALE_FACTOR to correct SPI inflation (BCWS denom too small)
# - Preserves existing comments if Excel already exists
# - Writes one Excel: EVMS_PowerBI_Overview.xlsx (sheet: Program_Overview_Card)
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
TODAY_OVERRIDE = None  # e.g. "2026-02-10"
BCWS_SCALE_FACTOR = 2.0

OUTPUT_XLSX = Path("EVMS_PowerBI_Overview.xlsx")
SHEET_NAME  = "Program_Overview_Card"

FORCE_READ_FILES = False
INPUT_FILES = []  # if empty, auto-discover or use in-memory df

# -------------------------
# GDLS COLOR PALETTE (from PPT)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"  # RGB 142,180,227
CLR_GREEN      = "#339966"  # RGB 051,153,102
CLR_YELLOW     = "#FFFF99"  # RGB 255,255,153
CLR_RED        = "#C0504D"  # RGB 192,080,077

# -------------------------
# SPI/CPI THRESHOLDS (use PPT rounding-adjusted cutoffs)
# Blue: >= 1.055 | Green: >= 0.975 | Yellow: >= 0.945 | Red: < 0.945
# -------------------------
def color_spi_cpi(x):
    x = pd.to_numeric(x, errors="coerce")
    if pd.isna(x): return None
    if x >= 1.055: return CLR_LIGHT_BLUE
    if x >= 0.975: return CLR_GREEN
    if x >= 0.945: return CLR_YELLOW
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
        # prefer cobra-looking files
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
    oldcol = f"{comment_col}_old"
    if oldcol in out.columns:
        oldvals = out[oldcol]
        mask = oldvals.notna() & (oldvals.astype(str).str.strip() != "")
        out.loc[mask, comment_col] = oldvals.loc[mask]
        out = out.drop(columns=[oldcol])
    return out

# -------------------------
# LOAD + FILTER
# -------------------------
base = load_inputs()
base = base[base["PROGRAM"].isin([normalize_key(p) for p in PROGRAMS_KEEP])].copy()

# -------------------------
# AS-OF DATE
# -------------------------
today = _to_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()
AS_OF_DATE = last_thursday_prev_month(today)

print("TODAY:", today)
print("AS_OF_DATE:", AS_OF_DATE)
print("BCWS_SCALE_FACTOR:", BCWS_SCALE_FACTOR)

# -------------------------
# FILTER + COSTSETS
# -------------------------
NEEDED_COSTSETS = ["BCWS","BCWP","ACWP"]
base_to_asof = base[(base["DATE"] <= AS_OF_DATE) & (base["COST_SET"].isin(NEEDED_COSTSETS))].copy()

# -------------------------
# CTD PROGRAM (sum to AS_OF)
# -------------------------
ctd_prog = (
    base_to_asof.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"CTD_HRS"})
)

# -------------------------
# LSD PROGRAM (latest DATE <= AS_OF per Program+CostSet)
# -------------------------
tmp = base_to_asof.sort_values(["PROGRAM","COST_SET","DATE"]).copy()
last_date = tmp.groupby(["PROGRAM","COST_SET"], as_index=False)["DATE"].max().rename(columns={"DATE":"LSD_DATE"})
lsd_prog = (
    tmp.merge(last_date, on=["PROGRAM","COST_SET"], how="inner")
    .loc[lambda d: d["DATE"] == d["LSD_DATE"]]
    .groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
    .rename(columns={"HOURS":"LSD_HRS"})
)

# -------------------------
# PIVOT
# -------------------------
ctd_p = ctd_prog.pivot_table(index="PROGRAM", columns="COST_SET", values="CTD_HRS", aggfunc="sum").reset_index()
lsd_p = lsd_prog.pivot_table(index="PROGRAM", columns="COST_SET", values="LSD_HRS", aggfunc="sum").reset_index()
for cs in NEEDED_COSTSETS:
    if cs not in ctd_p.columns: ctd_p[cs] = np.nan
    if cs not in lsd_p.columns: lsd_p[cs] = np.nan

# Apply BCWS scaling fix
ctd_p["BCWS"] = pd.to_numeric(ctd_p["BCWS"], errors="coerce") * float(BCWS_SCALE_FACTOR)
lsd_p["BCWS"] = pd.to_numeric(lsd_p["BCWS"], errors="coerce") * float(BCWS_SCALE_FACTOR)

# -------------------------
# COMPUTE SPI/CPI (CTD and LSD)
# SPI = BCWP / BCWS
# CPI = BCWP / ACWP
# -------------------------
df = ctd_p.merge(lsd_p, on="PROGRAM", how="outer", suffixes=("_CTD","_LSD")).rename(columns={"PROGRAM":"ProgramID"})

df["SPI_CTD"] = safe_div(df["BCWP_CTD"], df["BCWS_CTD"])
df["SPI_LSD"] = safe_div(df["BCWP_LSD"], df["BCWS_LSD"])
df["CPI_CTD"] = safe_div(df["BCWP_CTD"], df["ACWP_CTD"])
df["CPI_LSD"] = safe_div(df["BCWP_LSD"], df["ACWP_LSD"])

# Color definitions per metric cell
df["SPI_CTD_Color"] = df["SPI_CTD"].map(color_spi_cpi)
df["SPI_LSD_Color"] = df["SPI_LSD"].map(color_spi_cpi)
df["CPI_CTD_Color"] = df["CPI_CTD"].map(color_spi_cpi)
df["CPI_LSD_Color"] = df["CPI_LSD"].map(color_spi_cpi)

# Comments column (preserved if exists)
comment_col = "Comments / Root Cause & Corrective Actions"
df[comment_col] = ""

# Final column order (Power BI friendly)
overview_card = df[
    ["ProgramID",
     "SPI_CTD","SPI_LSD","SPI_CTD_Color","SPI_LSD_Color",
     "CPI_CTD","CPI_LSD","CPI_CTD_Color","CPI_LSD_Color",
     comment_col]
].sort_values(["ProgramID"]).reset_index(drop=True)

# Preserve existing comments by ProgramID
overview_card = preserve_comments(
    OUTPUT_XLSX, SHEET_NAME, overview_card,
    key_cols=["ProgramID"], comment_col=comment_col
)

# Write Excel
with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
    overview_card.to_excel(writer, sheet_name=SHEET_NAME, index=False)

print(f"\nSaved: {OUTPUT_XLSX.resolve()}")
print("\nPreview:")
display(overview_card.head(20))