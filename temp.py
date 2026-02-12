# ============================================================
# EVMS -> PowerBI Excel Export (ONE CELL)
# START FROM: cobra_merged_df (cleaned LONG dataset)
#
# STANDARD WINDOW:
#   LSD_END  = max DATE <= today
#   LSD_START = LSD_END - 27 days  (4-week fixed window)
#   LSD metrics computed over that 4-week window (sum within range)
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

OUTPUT_XLSX = Path("EVMS_PowerBI_Input.xlsx")
TODAY_OVERRIDE = None  # e.g. "2026-02-12"
LSD_WINDOW_DAYS = 28   # <-- 4-week window

EVMS_COSTSETS = ["BCWS","BCWP","ACWP","ETC"]

# -------------------------
# COLORS (hex)
# -------------------------
CLR_LIGHT_BLUE = "#8EB4E3"
CLR_GREEN      = "#339966"
CLR_YELLOW     = "#FFFF99"
CLR_RED        = "#C0504D"

def _to_num(x): return pd.to_numeric(x, errors="coerce")

def safe_div(a, b):
    a = _to_num(a); b = _to_num(b)
    return np.where((b == 0) | pd.isna(b), np.nan, a / b)

def color_spi_cpi(x):
    x = _to_num(x)
    if pd.isna(x): return None
    if x >= 1.05: return CLR_LIGHT_BLUE
    if x >= 0.98: return CLR_GREEN
    if x >= 0.95: return CLR_YELLOW
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
# Comments preservation
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
# Minimal coercion (NO cost-set remap)
# -------------------------
def clean_colname(c):
    return re.sub(r"[^A-Z0-9_]+", "_", str(c).strip().upper().replace(" ", "_").replace("-", "_"))

def coerce_long(df_in: pd.DataFrame) -> pd.DataFrame:
    df = df_in.copy()
    df.columns = [clean_colname(c) for c in df.columns]

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
        raise ValueError(f"cobra_merged_df missing columns: {missing}\nFound: {list(df.columns)}")

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")
    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    df["PRODUCT_TEAM"] = df["PRODUCT_TEAM"].astype(str).str.strip()
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip().str.upper()

    df = df.dropna(subset=["PROGRAM","PRODUCT_TEAM","DATE","COST_SET","HOURS"])
    return df

def agg_pivot(df, idx_cols):
    g = df.groupby(idx_cols + ["COST_SET"], as_index=False)["HOURS"].sum()
    pv = g.pivot_table(index=idx_cols, columns="COST_SET", values="HOURS", aggfunc="sum").reset_index()
    for cs in EVMS_COSTSETS:
        if cs not in pv.columns:
            pv[cs] = np.nan
    return pv

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns:
            df[c] = np.nan
    return df

# ============================================================
# START: cobra_merged_df
# ============================================================
if "cobra_merged_df" not in globals() or not isinstance(cobra_merged_df, pd.DataFrame) or len(cobra_merged_df) == 0:
    raise ValueError("cobra_merged_df is not defined or empty.")

base = coerce_long(cobra_merged_df)
today = as_date(TODAY_OVERRIDE) if TODAY_OVERRIDE else date.today()

evms = base[base["COST_SET"].isin(EVMS_COSTSETS)].copy()

dates_le_today = sorted(d for d in evms["DATE"].unique() if d <= today)
if not dates_le_today:
    raise ValueError("No EVMS rows found with DATE <= today. Check DATE parsing or TODAY_OVERRIDE.")

LSD_END = dates_le_today[-1]
LSD_START = LSD_END - timedelta(days=LSD_WINDOW_DAYS - 1)  # inclusive range
AS_OF_DATE = LSD_END

print("TODAY:", today)
print("Rows in cobra_merged_df:", len(base))
print("Rows in EVMS:", len(evms))
print("Programs:", evms["PROGRAM"].nunique(), "| Product Teams:", evms["PRODUCT_TEAM"].nunique())
print("AS_OF_DATE / LSD_END:", LSD_END)
print("LSD_START (4-week window):", LSD_START)

# Windows
ctd = evms[evms["DATE"] <= LSD_END].copy()
lsd = evms[(evms["DATE"] >= LSD_START) & (evms["DATE"] <= LSD_END)].copy()

# ============================================================
# PROGRAM OVERVIEW (WIDE)
# ============================================================
ctd_prog = agg_pivot(ctd, ["PROGRAM"]).rename(columns={"PROGRAM":"ProgramID"})
lsd_prog = agg_pivot(lsd, ["PROGRAM"]).rename(columns={"PROGRAM":"ProgramID"})

prog = ctd_prog.merge(lsd_prog, on="ProgramID", how="outer", suffixes=("_CTD","_LSD"))
prog = ensure_cols(prog, ["BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD","ETC_LSD"])

prog["SPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["BCWS_CTD"])
prog["CPI_CTD"] = safe_div(prog["BCWP_CTD"], prog["ACWP_CTD"])
prog["SPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["BCWS_LSD"])
prog["CPI_LSD"] = safe_div(prog["BCWP_LSD"], prog["ACWP_LSD"])

prog["SPI_LSD_Color"] = prog["SPI_LSD"].map(color_spi_cpi)
prog["SPI_CTD_Color"] = prog["SPI_CTD"].map(color_spi_cpi)
prog["CPI_LSD_Color"] = prog["CPI_LSD"].map(color_spi_cpi)
prog["CPI_CTD_Color"] = prog["CPI_CTD"].map(color_spi_cpi)

prog["LSD_START"] = LSD_START
prog["LSD_END"] = LSD_END
prog["AS_OF_DATE"] = AS_OF_DATE

comment_overview = "Comments / Root Cause & Corrective Actions"
prog[comment_overview] = ""

Program_Overview = prog[
    ["ProgramID","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "LSD_START","LSD_END","AS_OF_DATE",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     comment_overview]
].sort_values("ProgramID").reset_index(drop=True)

Program_Overview = preserve_comments(OUTPUT_XLSX, "Program_Overview", Program_Overview, ["ProgramID"], comment_overview)

# ============================================================
# PRODUCT TEAM SPI/CPI
# ============================================================
ctd_pt = agg_pivot(ctd, ["PROGRAM","PRODUCT_TEAM"]).rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})
lsd_pt = agg_pivot(lsd, ["PROGRAM","PRODUCT_TEAM"]).rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team"})

pt = ctd_pt.merge(lsd_pt, on=["ProgramID","Product Team"], how="outer", suffixes=("_CTD","_LSD"))
pt = ensure_cols(pt, ["BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CTD","BCWS_LSD","BCWP_LSD","ACWP_LSD","ETC_LSD"])

pt["SPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["BCWS_CTD"])
pt["CPI_CTD"] = safe_div(pt["BCWP_CTD"], pt["ACWP_CTD"])
pt["SPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["BCWS_LSD"])
pt["CPI_LSD"] = safe_div(pt["BCWP_LSD"], pt["ACWP_LSD"])

pt["SPI_LSD_Color"] = pt["SPI_LSD"].map(color_spi_cpi)
pt["SPI_CTD_Color"] = pt["SPI_CTD"].map(color_spi_cpi)
pt["CPI_LSD_Color"] = pt["CPI_LSD"].map(color_spi_cpi)
pt["CPI_CTD_Color"] = pt["CPI_CTD"].map(color_spi_cpi)

pt["LSD_START"] = LSD_START
pt["LSD_END"] = LSD_END
pt["AS_OF_DATE"] = AS_OF_DATE

comment_pt = "Cause & Corrective Actions"
pt[comment_pt] = ""

ProductTeam_SPI_CPI = pt[
    ["ProgramID","Product Team",
     "SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD",
     "LSD_START","LSD_END","AS_OF_DATE",
     "SPI_LSD_Color","SPI_CTD_Color","CPI_LSD_Color","CPI_CTD_Color",
     comment_pt]
].sort_values(["ProgramID","Product Team"]).reset_index(drop=True)

ProductTeam_SPI_CPI = preserve_comments(OUTPUT_XLSX, "ProductTeam_SPI_CPI", ProductTeam_SPI_CPI, ["ProgramID","Product Team"], comment_pt)

# ============================================================
# PRODUCT TEAM BAC/EAC/VAC
# BAC = sum(BCWS) for calendar year of LSD_END (change if your fiscal differs)
# EAC = ACWP_CTD + ETC_CTD
# ============================================================
year_start = date(LSD_END.year, 1, 1)
year_end   = date(LSD_END.year, 12, 31)

year_df = evms[(evms["DATE"] >= year_start) & (evms["DATE"] <= year_end)].copy()
bac = (year_df[year_df["COST_SET"] == "BCWS"]
       .groupby(["PROGRAM","PRODUCT_TEAM"], as_index=False)["HOURS"].sum()
       .rename(columns={"PROGRAM":"ProgramID","PRODUCT_TEAM":"Product Team","HOURS":"BAC"}))

ctd_pt_for_eac = ensure_cols(ctd_pt.copy(), ["ACWP","ETC"]).rename(columns={"ACWP":"ACWP_CTD","ETC":"ETC_CTD"})

tmp = bac.merge(ctd_pt_for_eac[["ProgramID","Product Team","ACWP_CTD","ETC_CTD"]],
                on=["ProgramID","Product Team"], how="outer")

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

ProductTeam_BAC_EAC_VAC = preserve_comments(OUTPUT_XLSX, "ProductTeam_BAC_EAC_VAC", ProductTeam_BAC_EAC_VAC, ["ProgramID","Product Team"], comment_pt)

# ============================================================
# PROGRAM MANPOWER (CTD) + "Next" window = next 28 days after LSD_END
# ============================================================
man = pd.DataFrame({
    "ProgramID": ctd_prog["ProgramID"],
    "Demand Hours": _to_num(ctd_prog.get("BCWS")),
    "Actual Hours": _to_num(ctd_prog.get("ACWP")),
})
man["% Var"] = safe_div(man["Actual Hours"], man["Demand Hours"]) * 100.0
man["% Var Color"] = man["% Var"].map(color_manpower_pct)

NEXT_START = LSD_END + timedelta(days=1)
NEXT_END   = LSD_END + timedelta(days=LSD_WINDOW_DAYS)

next_df = evms[(evms["DATE"] >= NEXT_START) & (evms["DATE"] <= NEXT_END) & (evms["COST_SET"].isin(["BCWS","ETC"]))].copy()
if len(next_df):
    next_prog = (next_df.groupby(["PROGRAM","COST_SET"], as_index=False)["HOURS"].sum()
                 .pivot_table(index="PROGRAM", columns="COST_SET", values="HOURS", aggfunc="sum")
                 .reset_index())
else:
    next_prog = pd.DataFrame({"PROGRAM": evms["PROGRAM"].unique()})

if "BCWS" not in next_prog.columns: next_prog["BCWS"] = np.nan
if "ETC"  not in next_prog.columns: next_prog["ETC"]  = np.nan

next_prog = next_prog.rename(columns={
    "PROGRAM":"ProgramID",
    "BCWS":"Next 4W BCWS Hours",
    "ETC":"Next 4W ETC Hours"
})

Program_Manpower = man.merge(next_prog[["ProgramID","Next 4W BCWS Hours","Next 4W ETC Hours"]],
                             on="ProgramID", how="left")
Program_Manpower[comment_pt] = ""
Program_Manpower = Program_Manpower[
    ["ProgramID","Demand Hours","Actual Hours","% Var","% Var Color","Next 4W BCWS Hours","Next 4W ETC Hours",comment_pt]
].sort_values("ProgramID").reset_index(drop=True)

Program_Manpower = preserve_comments(OUTPUT_XLSX, "Program_Manpower", Program_Manpower, ["ProgramID"], comment_pt)

# ============================================================
# WHY "MISSING" HAPPENS (pinpoint)
# ============================================================
print("\n--- Missing-data diagnostics (Program Overview LSD window) ---")
diag = prog[["ProgramID","BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"]].copy()
diag["BCWS_LSD_is0_orNaN"] = _to_num(diag["BCWS_LSD"]).isna() | (_to_num(diag["BCWS_LSD"]) == 0)
diag["ACWP_LSD_is0_orNaN"] = _to_num(diag["ACWP_LSD"]).isna() | (_to_num(diag["ACWP_LSD"]) == 0)
print("Programs where SPI_LSD blanks because BCWS_LSD missing/0:")
print(diag.loc[diag["BCWS_LSD_is0_orNaN"], ["ProgramID","BCWS_LSD","BCWP_LSD"]].head(50))
print("Programs where CPI_LSD blanks because ACWP_LSD missing/0:")
print(diag.loc[diag["ACWP_LSD_is0_orNaN"], ["ProgramID","ACWP_LSD","BCWP_LSD"]].head(50))

# ============================================================
# Write Excel
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