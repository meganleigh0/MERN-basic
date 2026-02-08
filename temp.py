import pandas as pd
import numpy as np

# =========================
# Assumptions / Inputs
# =========================
# cobra_merged_df has (at least): PROGRAM, SUB_TEAM, COST-SET, DATE, HOURS
# COST-SET already normalized to: BCWS, ACWP, BCWP, ETC
# DATE is week-ending / status date
#
# Accounting calendar: we will treat the "latest accounting period close date"
# as the latest available DATE in your cobra_merged_df.
# (If you have an explicit list of close dates, replace `latest_close_date` with that value.)

df = cobra_merged_df.copy()

# -------------------------
# Clean types
# -------------------------
df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0)

# Optional: enforce strings
for c in ["PROGRAM", "SUB_TEAM", "COST-SET"]:
    if c in df.columns:
        df[c] = df[c].astype("string").fillna("").str.strip()

# Latest closed status period (replace with explicit calendar close date if needed)
latest_close_date = df["DATE"].max()

# Fiscal year start (adjust if your FY != calendar year)
fy_start = pd.Timestamp(year=latest_close_date.year, month=1, day=1)

# Status slice (CTD/YTD should be through latest close)
df_status = df[df["DATE"].notna() & (df["DATE"] <= latest_close_date)].copy()

# Helper: safe divide
def _sdiv(num, den):
    return np.where(den == 0, np.nan, num / den)

# Helper: pivot cost sets into wide columns
def _costset_wide(d, idx_cols, value_col="HOURS"):
    w = (
        d.pivot_table(index=idx_cols, columns="COST-SET", values=value_col, aggfunc="sum", fill_value=0)
         .reset_index()
    )
    # Ensure expected cols exist
    for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if col not in w.columns:
            w[col] = 0.0
    return w

# =========================
# 1) EVMS by PROGRAM: SPI/CPI for CTD and YTD
# =========================
ctd_prog = _costset_wide(df_status, ["PROGRAM"])
ytd_prog = _costset_wide(df_status[(df_status["DATE"] >= fy_start)], ["PROGRAM"])

df_program_evms = ctd_prog.merge(
    ytd_prog,
    on=["PROGRAM"],
    how="outer",
    suffixes=("_CTD", "_YTD"),
).fillna(0)

df_program_evms["SPI_CTD"] = _sdiv(df_program_evms["BCWP_CTD"], df_program_evms["BCWS_CTD"])
df_program_evms["CPI_CTD"] = _sdiv(df_program_evms["BCWP_CTD"], df_program_evms["ACWP_CTD"])
df_program_evms["SPI_YTD"] = _sdiv(df_program_evms["BCWP_YTD"], df_program_evms["BCWS_YTD"])
df_program_evms["CPI_YTD"] = _sdiv(df_program_evms["BCWP_YTD"], df_program_evms["ACWP_YTD"])

# Keep it tidy
df_program_evms = df_program_evms[
    ["PROGRAM",
     "BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CTD","SPI_CTD","CPI_CTD",
     "BCWS_YTD","BCWP_YTD","ACWP_YTD","ETC_YTD","SPI_YTD","CPI_YTD"]
].sort_values(["PROGRAM"]).reset_index(drop=True)

# =========================
# 2) EVMS by PROGRAM + SUB_TEAM: SPI/CPI for CTD and YTD
# =========================
ctd_ps = _costset_wide(df_status, ["PROGRAM", "SUB_TEAM"])
ytd_ps = _costset_wide(df_status[(df_status["DATE"] >= fy_start)], ["PROGRAM", "SUB_TEAM"])

df_program_subteam_evms = ctd_ps.merge(
    ytd_ps,
    on=["PROGRAM", "SUB_TEAM"],
    how="outer",
    suffixes=("_CTD", "_YTD"),
).fillna(0)

df_program_subteam_evms["SPI_CTD"] = _sdiv(df_program_subteam_evms["BCWP_CTD"], df_program_subteam_evms["BCWS_CTD"])
df_program_subteam_evms["CPI_CTD"] = _sdiv(df_program_subteam_evms["BCWP_CTD"], df_program_subteam_evms["ACWP_CTD"])
df_program_subteam_evms["SPI_YTD"] = _sdiv(df_program_subteam_evms["BCWP_YTD"], df_program_subteam_evms["BCWS_YTD"])
df_program_subteam_evms["CPI_YTD"] = _sdiv(df_program_subteam_evms["BCWP_YTD"], df_program_subteam_evms["ACWP_YTD"])

df_program_subteam_evms = df_program_subteam_evms[
    ["PROGRAM","SUB_TEAM",
     "BCWS_CTD","BCWP_CTD","ACWP_CTD","ETC_CTD","SPI_CTD","CPI_CTD",
     "BCWS_YTD","BCWP_YTD","ACWP_YTD","ETC_YTD","SPI_YTD","CPI_YTD"]
].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# =========================
# 3) BAC, EAC, VAC by SUB_TEAM (within PROGRAM)
# =========================
# BAC (total budget hours) = sum of BCWS over ALL available dates for the group
bac = _costset_wide(df[df["DATE"].notna()], ["PROGRAM","SUB_TEAM"])[["PROGRAM","SUB_TEAM","BCWS"]].rename(columns={"BCWS":"BAC_HRS"})

# CTD ACWP through status
acwp_ctd = _costset_wide(df_status, ["PROGRAM","SUB_TEAM"])[["PROGRAM","SUB_TEAM","ACWP"]].rename(columns={"ACWP":"ACWP_CTD_HRS"})

# ETC at status date (NOT cumulative): sum ETC for the latest close date only
etc_status = (
    df_status[df_status["DATE"] == latest_close_date]
    .pivot_table(index=["PROGRAM","SUB_TEAM"], columns="COST-SET", values="HOURS", aggfunc="sum", fill_value=0)
    .reset_index()
)
if "ETC" not in etc_status.columns:
    etc_status["ETC"] = 0.0
etc_status = etc_status[["PROGRAM","SUB_TEAM","ETC"]].rename(columns={"ETC":"ETC_STATUS_HRS"})

df_subteam_bac_eac_vac = (
    bac.merge(acwp_ctd, on=["PROGRAM","SUB_TEAM"], how="left")
       .merge(etc_status, on=["PROGRAM","SUB_TEAM"], how="left")
       .fillna(0)
)

# EAC = CTD ACWP + ETC (status)
df_subteam_bac_eac_vac["EAC_HRS"] = df_subteam_bac_eac_vac["ACWP_CTD_HRS"] + df_subteam_bac_eac_vac["ETC_STATUS_HRS"]
# VAC = BAC - EAC
df_subteam_bac_eac_vac["VAC_HRS"] = df_subteam_bac_eac_vac["BAC_HRS"] - df_subteam_bac_eac_vac["EAC_HRS"]
# %COMP (optional, commonly used): CTD BCWP / BAC
bcwp_ctd = _costset_wide(df_status, ["PROGRAM","SUB_TEAM"])[["PROGRAM","SUB_TEAM","BCWP"]].rename(columns={"BCWP":"BCWP_CTD_HRS"})
df_subteam_bac_eac_vac = df_subteam_bac_eac_vac.merge(bcwp_ctd, on=["PROGRAM","SUB_TEAM"], how="left").fillna(0)
df_subteam_bac_eac_vac["PCT_COMP"] = _sdiv(df_subteam_bac_eac_vac["BCWP_CTD_HRS"], df_subteam_bac_eac_vac["BAC_HRS"])

df_subteam_bac_eac_vac = df_subteam_bac_eac_vac[
    ["PROGRAM","SUB_TEAM","BAC_HRS","ACWP_CTD_HRS","ETC_STATUS_HRS","EAC_HRS","VAC_HRS","BCWP_CTD_HRS","PCT_COMP"]
].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# =========================
# 4) Demand Hours, Actual Hours, %Var, Next Mo BCWS Hours, Next Mo ETC Hours (by PROGRAM)
# =========================
# Interpretations (hours-based):
# - Demand Hours = current-month BCWS (status month)
# - Actual Hours = current-month ACWP (status month)
# - %Var = (Actual - Demand) / Demand
# - Next Mo BCWS Hours = next-month BCWS
# - Next Mo ETC Hours = next-month ETC
#
# If your definitions differ, this is the cleanest place to adjust.

status_month_start = latest_close_date.to_period("M").to_timestamp()
status_month_end   = (latest_close_date.to_period("M").to_timestamp() + pd.offsets.MonthEnd(0))

next_month_start = (status_month_start + pd.offsets.MonthBegin(1))
next_month_end   = (next_month_start + pd.offsets.MonthEnd(0))

def _month_sum(d, start, end, group_cols):
    x = d[(d["DATE"] >= start) & (d["DATE"] <= end)]
    w = _costset_wide(x, group_cols)
    return w

cur_mo = _month_sum(df, status_month_start, status_month_end, ["PROGRAM"])
nxt_mo = _month_sum(df, next_month_start, next_month_end, ["PROGRAM"])

df_program_hours = cur_mo.merge(nxt_mo, on=["PROGRAM"], how="outer", suffixes=("_CUR_MO","_NEXT_MO")).fillna(0)

df_program_hours["DEMAND_HRS"] = df_program_hours["BCWS_CUR_MO"]
df_program_hours["ACTUAL_HRS"] = df_program_hours["ACWP_CUR_MO"]
df_program_hours["PCT_VAR"]    = _sdiv(df_program_hours["ACTUAL_HRS"] - df_program_hours["DEMAND_HRS"], df_program_hours["DEMAND_HRS"])

df_program_hours["NEXT_MO_BCWS_HRS"] = df_program_hours["BCWS_NEXT_MO"]
df_program_hours["NEXT_MO_ETC_HRS"]  = df_program_hours["ETC_NEXT_MO"]

df_program_hours = df_program_hours[
    ["PROGRAM","DEMAND_HRS","ACTUAL_HRS","PCT_VAR","NEXT_MO_BCWS_HRS","NEXT_MO_ETC_HRS"]
].sort_values(["PROGRAM"]).reset_index(drop=True)

# =========================
# Outputs (4 metric tables)
# =========================
# 1) df_program_evms
# 2) df_program_subteam_evms
# 3) df_subteam_bac_eac_vac
# 4) df_program_hours

print("Latest Close Date Used:", latest_close_date.date())
print("FY Start Used:", fy_start.date())
display(df_program_evms.head(10))
display(df_program_subteam_evms.head(10))
display(df_subteam_bac_eac_vac.head(10))
display(df_program_hours.head(10))