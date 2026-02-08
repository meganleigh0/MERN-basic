import pandas as pd
import numpy as np

# ---------------------------
# 0) Prep / normalize
# ---------------------------
df = cobra_merged_df.copy()

df["DATE"]  = pd.to_datetime(df["DATE"], errors="coerce")
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0)

for c in ["PROGRAM","SUB_TEAM","COST-SET"]:
    df[c] = df[c].astype("string").fillna("").str.strip()

df["COST-SET"] = df["COST-SET"].str.upper()

# Optional: If you have non-hour rows, keep only hours
if "PLUG" in df.columns:
    df["PLUG"] = df["PLUG"].astype("string").fillna("").str.upper().str.strip()
    df = df[df["PLUG"].isin(["HOURS",""])].copy()

# Keep only the 4 programs you care about if you want (comment out if not)
KEEP_PROGRAMS = ["ABRAMS_22","OLYMPUS","STRYKER_BULG","XM30"]
df = df[df["PROGRAM"].isin(KEEP_PROGRAMS)].copy()

# ---------------------------
# 1) Accounting Calendar -> Last Status Date (LSD)
#    From your image: "Accounting Period Closing" are the circled/outlined dates.
#    Hardcode 2026 close dates (edit/add as needed).
# ---------------------------
close_dates_2026 = pd.to_datetime([
    # Jan
    "2026-01-04","2026-01-23",
    # Feb
    "2026-02-01","2026-02-20",
    # Mar
    "2026-03-01","2026-03-20","2026-03-29",
    # Apr
    "2026-04-05","2026-04-24",
    # May
    "2026-05-03","2026-05-31",
    # Jun
    "2026-06-07","2026-06-26",
    # Jul
    "2026-07-05","2026-07-24",
    # Aug
    "2026-08-02","2026-08-30",
    # Sep
    "2026-09-27",
    # Oct
    "2026-10-04","2026-10-30",
    # Nov
    "2026-11-01","2026-11-29",
    # Dec
    "2026-12-06","2026-12-27"
], errors="coerce").dropna().sort_values()

# Choose an "as of" date:
# Use latest ACWP/BCWP date in the data (best proxy for what is actually posted)
asof_date = df.loc[df["COST-SET"].isin(["ACWP","BCWP"]) & df["DATE"].notna(), "DATE"].max()

# LSD = last close date <= asof_date
LSD = close_dates_2026[close_dates_2026 <= asof_date].max()

# Previous close date (for "LSD period" deltas if needed)
PREV_LSD = close_dates_2026[close_dates_2026 < LSD].max()

# Fiscal year start (your legend shows FY end close date; FY likely calendar-year here)
FY_START = pd.Timestamp(LSD.year, 1, 1)

# ---------------------------
# 2) Helper: safe division (no NaNs)
# ---------------------------
def safe_div(num, den):
    num = num.astype(float)
    den = den.astype(float)
    out = np.zeros(len(num), dtype=float)
    m = den != 0
    out[m] = num[m] / den[m]
    return out

# ---------------------------
# 3) Build base measures for CTD and LSD period
# ---------------------------
m_ctd = df["DATE"].notna() & (df["DATE"] <= LSD)
m_ytd = df["DATE"].notna() & (df["DATE"] >= FY_START) & (df["DATE"] <= LSD)

# For LSD-period: choose ONE of these definitions:
# A) "On the LSD date only" (works if your weekly rows land on LSD)
m_lsd = df["DATE"].notna() & (df["DATE"] == LSD)

# B) "Between PREV_LSD (exclusive) and LSD (inclusive)" (use if your data is daily/weekly but not exactly on LSD)
# m_lsd = df["DATE"].notna() & (df["DATE"] > PREV_LSD) & (df["DATE"] <= LSD)

def sum_hours(mask, by_cols):
    p = (df.loc[mask]
           .pivot_table(index=by_cols, columns="COST-SET", values="HOURS", aggfunc="sum", fill_value=0)
           .reindex(columns=["BCWS","BCWP","ACWP"], fill_value=0)   # ONLY the ones we need
           .reset_index())
    return p

# ---------------------------
# TABLE 1: Program EVMS (CTD + LSD): SPI/CPI
# ---------------------------
ctd_prog = sum_hours(m_ctd, ["PROGRAM"]).rename(columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD"})
lsd_prog = sum_hours(m_lsd, ["PROGRAM"]).rename(columns={"BCWS":"BCWS_LSD","BCWP":"BCWP_LSD","ACWP":"ACWP_LSD"})

t1 = ctd_prog.merge(lsd_prog, on="PROGRAM", how="outer").fillna(0)

t1["SPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["BCWS_CTD"])
t1["CPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["ACWP_CTD"])
t1["SPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["BCWS_LSD"])
t1["CPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["ACWP_LSD"])

t1 = t1[[
    "PROGRAM",
    "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
    "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"
]].sort_values("PROGRAM").reset_index(drop=True)

# ---------------------------
# TABLE 2: Program + Subteam EVMS (CTD + LSD): SPI/CPI
# ---------------------------
ctd_pt = sum_hours(m_ctd, ["PROGRAM","SUB_TEAM"]).rename(columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD"})
lsd_pt = sum_hours(m_lsd, ["PROGRAM","SUB_TEAM"]).rename(columns={"BCWS":"BCWS_LSD","BCWP":"BCWP_LSD","ACWP":"ACWP_LSD"})

t2 = ctd_pt.merge(lsd_pt, on=["PROGRAM","SUB_TEAM"], how="outer").fillna(0)

t2["SPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["BCWS_CTD"])
t2["CPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["ACWP_CTD"])
t2["SPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["BCWS_LSD"])
t2["CPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["ACWP_LSD"])

t2 = t2[[
    "PROGRAM","SUB_TEAM",
    "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
    "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"
]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# ---------------------------
# TABLE 3: BAC / EAC / VAC by Program + Subteam
# Definitions (hours-based):
#   BAC = BCWS_CTD (total budget to date — if you truly want TOTAL BAC, you need a separate BAC field; using BCWS_CTD matches your slide note "BAC is total budget in hours")
#   EAC = ACWP_CTD + ETC_current
#   VAC = BAC - EAC
#
# Since you said you DON'T need ETC_CTD/YTD, we will use ETC as "current forecast" at LSD.
# If ETC is missing for a group, we treat it as 0 (still fully populated).
# ---------------------------
# Pull "current ETC" at LSD period (same mask as LSD)
etc_pt = (
    df.loc[m_lsd & df["COST-SET"].eq("ETC")]
      .groupby(["PROGRAM","SUB_TEAM"])["HOURS"].sum()
      .reset_index()
      .rename(columns={"HOURS":"ETC_LSD"})
)

t3 = t2.merge(etc_pt, on=["PROGRAM","SUB_TEAM"], how="left").fillna({"ETC_LSD":0})

t3["BAC_HRS"] = t3["BCWS_CTD"]
t3["EAC_HRS"] = t3["ACWP_CTD"] + t3["ETC_LSD"]
t3["VAC_HRS"] = t3["BAC_HRS"] - t3["EAC_HRS"]

t3 = t3[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# ---------------------------
# TABLE 4: Program "Other Measures"
# Demand Hours, Actual Hours, %Var, Next Mo BCWS Hours, Next Mo ETC Hours
#
# From your notes/screenshots, "Demand" generally aligns with BCWS (planned), "Actual" aligns with ACWP.
# "Next month" values should come from dates AFTER LSD.
#
# We'll approximate "Next Mo" as the next calendar month after LSD (month boundaries).
# If you use accounting periods, we can swap to next close-date window instead.
# ---------------------------
next_month_start = (LSD + pd.offsets.MonthBegin(1)).normalize()
next_month_end   = (LSD + pd.offsets.MonthEnd(1)).normalize()

m_nextmo = df["DATE"].notna() & (df["DATE"] >= next_month_start) & (df["DATE"] <= next_month_end)

# Demand/Actual for LSD period (you can switch to CTD if needed)
demand_lsd = df.loc[m_lsd & df["COST-SET"].eq("BCWS")].groupby("PROGRAM")["HOURS"].sum()
actual_lsd = df.loc[m_lsd & df["COST-SET"].eq("ACWP")].groupby("PROGRAM")["HOURS"].sum()

nextmo_bcws = df.loc[m_nextmo & df["COST-SET"].eq("BCWS")].groupby("PROGRAM")["HOURS"].sum()
nextmo_etc  = df.loc[m_nextmo & df["COST-SET"].eq("ETC") ].groupby("PROGRAM")["HOURS"].sum()

prog_idx = pd.Index(sorted(df["PROGRAM"].unique()), name="PROGRAM")
t4 = pd.DataFrame({"PROGRAM": prog_idx}).reset_index(drop=True)

t4["Demand_Hours_LSD"] = demand_lsd.reindex(prog_idx, fill_value=0).to_numpy()
t4["Actual_Hours_LSD"] = actual_lsd.reindex(prog_idx, fill_value=0).to_numpy()
t4["PctVar_LSD"] = np.where(t4["Demand_Hours_LSD"].to_numpy()==0, 0, (t4["Actual_Hours_LSD"] - t4["Demand_Hours_LSD"]) / t4["Demand_Hours_LSD"])

t4["NextMo_BCWS_Hours"] = nextmo_bcws.reindex(prog_idx, fill_value=0).to_numpy()
t4["NextMo_ETC_Hours"]  = nextmo_etc.reindex(prog_idx, fill_value=0).to_numpy()

t4 = t4.sort_values("PROGRAM").reset_index(drop=True)

# ---------------------------
# Final: ensure "fully populated" (no NaNs)
# ---------------------------
for _t in ["t1","t2","t3","t4"]:
    pass

assert not t1.isna().any().any(), "t1 still has NaNs"
assert not t2.isna().any().any(), "t2 still has NaNs"
assert not t3.isna().any().any(), "t3 still has NaNs"
assert not t4.isna().any().any(), "t4 still has NaNs"

print("ASOF:", asof_date.date(), "| LSD used:", LSD.date(), "| Prev LSD:", PREV_LSD.date(), "| NextMo:", next_month_start.date(), "to", next_month_end.date())
display(t1)  # Program EVMS (CTD + LSD)
display(t2)  # Program + Subteam EVMS (CTD + LSD)
display(t3)  # BAC/EAC/VAC by Program + Subteam
display(t4)  # Other measures by Program