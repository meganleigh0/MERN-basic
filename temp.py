import pandas as pd
import numpy as np

# =========================
# EVMS PIPELINE (NO CALENDAR YET)
# Placeholder LSD = 2 weeks prior to today
# =========================

df = cobra_merged_df.copy()

# ---- Clean / normalize ----
df["DATE"]  = pd.to_datetime(df["DATE"], errors="coerce")
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0.0)

for c in ["PROGRAM", "SUB_TEAM", "COST-SET"]:
    if c in df.columns:
        df[c] = df[c].astype("string").fillna("").str.strip()

df["COST-SET"] = df["COST-SET"].str.upper()

# If PLUG exists, keep HOURS rows (optional)
if "PLUG" in df.columns:
    df["PLUG"] = df["PLUG"].astype("string").fillna("").str.strip().str.upper()
    df = df[df["PLUG"].isin(["HOURS", ""])].copy()

# Keep only the core EVMS cost sets we need
keep_sets = ["BCWS", "BCWP", "ACWP", "EAC", "ETC", "BUDGET", "PROGRESS", "ACTUALS", "ACWP_HRS"]
df = df[df["COST-SET"].isin(keep_sets)].copy()

# Collapse variants -> canonical
cost_set_map = {
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    "ACTUALS": "ACWP",
    "ACWP_HRS": "ACWP",
    "ACWP": "ACWP",

    # We are NOT using ETC in outputs, but keep mapping clean
    "EAC": "ETC",
    "ETC": "ETC",
}
df["COST-SET"] = df["COST-SET"].map(cost_set_map).fillna(df["COST-SET"])

# Drop any rows missing essentials
df = df[(df["DATE"].notna()) & (df["PROGRAM"] != "")].copy()
if "SUB_TEAM" not in df.columns:
    df["SUB_TEAM"] = ""

# ---- Placeholder periods ----
today = pd.Timestamp.today().normalize()
LSD = today - pd.Timedelta(days=14)            # placeholder last status date
lsd_start = LSD - pd.Timedelta(days=13)        # 14-day window ending on LSD
next_start = LSD + pd.Timedelta(days=1)
next_end   = LSD + pd.Timedelta(days=28)       # placeholder "next month" = next 4 weeks

m_ctd  = df["DATE"] <= LSD
m_lsd  = (df["DATE"] >= lsd_start) & (df["DATE"] <= LSD)
m_next = (df["DATE"] >= next_start) & (df["DATE"] <= next_end)

def pivot_sum(mask, idx_cols):
    p = (df.loc[mask]
           .pivot_table(index=idx_cols, columns="COST-SET", values="HOURS",
                        aggfunc="sum", fill_value=0.0)
           .reset_index())
    for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if col not in p.columns:
            p[col] = 0.0
    return p

def safe_div(num, den):
    num = num.astype(float)
    den = den.astype(float)
    out = np.zeros(len(num), dtype=float)
    m = den != 0
    out[m] = num[m] / den[m]
    return out

# =========================
# TABLE 1: PROGRAM SPI/CPI (CTD + LSD)
# =========================
ctd_prog = pivot_sum(m_ctd, ["PROGRAM"]).rename(columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD"})
lsd_prog = pivot_sum(m_lsd, ["PROGRAM"]).rename(columns={"BCWS":"BCWS_LSD","BCWP":"BCWP_LSD","ACWP":"ACWP_LSD"})

t1 = ctd_prog.merge(lsd_prog, on="PROGRAM", how="outer").fillna(0.0)

t1["SPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["BCWS_CTD"])
t1["CPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["ACWP_CTD"])
t1["SPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["BCWS_LSD"])
t1["CPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["ACWP_LSD"])

t1 = t1[[
    "PROGRAM",
    "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
    "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"
]].sort_values("PROGRAM").reset_index(drop=True)

# =========================
# TABLE 2: PROGRAM + SUB_TEAM SPI/CPI (CTD + LSD)
# =========================
ctd_pt = pivot_sum(m_ctd, ["PROGRAM","SUB_TEAM"]).rename(columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD"})
lsd_pt = pivot_sum(m_lsd, ["PROGRAM","SUB_TEAM"]).rename(columns={"BCWS":"BCWS_LSD","BCWP":"BCWP_LSD","ACWP":"ACWP_LSD"})

t2 = ctd_pt.merge(lsd_pt, on=["PROGRAM","SUB_TEAM"], how="outer").fillna(0.0)

t2["SPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["BCWS_CTD"])
t2["CPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["ACWP_CTD"])
t2["SPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["BCWS_LSD"])
t2["CPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["ACWP_LSD"])

t2 = t2[[
    "PROGRAM","SUB_TEAM",
    "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
    "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"
]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# =========================
# TABLE 3: PROGRAM + SUB_TEAM BAC/EAC/VAC (using CTD + LSD)
# BAC (hours) = total budget hours -> use BCWS_CTD as best available budget proxy
# EAC (hours) = ACWP_CTD + (remaining budget) where remaining = BAC - BCWP_CTD
# VAC (hours) = BAC - EAC
# (No ETC needed)
# =========================
t3 = t2[["PROGRAM","SUB_TEAM","BCWS_CTD","BCWP_CTD","ACWP_CTD"]].copy()
t3["BAC_HRS"] = t3["BCWS_CTD"].astype(float)

remaining = (t3["BAC_HRS"] - t3["BCWP_CTD"]).astype(float)
remaining = np.where(remaining < 0, 0.0, remaining)  # don't allow negative remaining

t3["EAC_HRS"] = t3["ACWP_CTD"].astype(float) + remaining
t3["VAC_HRS"] = t3["BAC_HRS"].astype(float) - t3["EAC_HRS"].astype(float)

t3 = t3[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# =========================
# TABLE 4: PROGRAM Demand/Actual/%Var + NextMo BCWS/ETC hours
# Demand_LSD = BCWS_LSD, Actual_LSD = ACWP_LSD
# NextMo_BCWS = BCWS in next window, NextMo_ETC = ETC in next window (if present)
# =========================
next_prog = pivot_sum(m_next, ["PROGRAM"]).rename(columns={"BCWS":"NextMo_BCWS_Hours","ETC":"NextMo_ETC_Hours"})
base_prog = lsd_prog.merge(next_prog[["PROGRAM","NextMo_BCWS_Hours","NextMo_ETC_Hours"]], on="PROGRAM", how="left").fillna(0.0)

t4 = pd.DataFrame({"PROGRAM": base_prog["PROGRAM"]})
t4["Demand_Hours_LSD"] = base_prog["BCWS_LSD"].astype(float).to_numpy()
t4["Actual_Hours_LSD"] = base_prog["ACWP_LSD"].astype(float).to_numpy()

d = t4["Demand_Hours_LSD"].to_numpy()
a = t4["Actual_Hours_LSD"].to_numpy()
t4["PctVar_LSD"] = np.where(d == 0, 0.0, (a - d) / d)

t4["NextMo_BCWS_Hours"] = base_prog["NextMo_BCWS_Hours"].astype(float).to_numpy()
t4["NextMo_ETC_Hours"]  = base_prog["NextMo_ETC_Hours"].astype(float).to_numpy()

t4 = t4.sort_values("PROGRAM").reset_index(drop=True)

# ---- Guarantee no NaNs anywhere ----
t1 = t1.fillna(0.0)
t2 = t2.fillna(0.0)
t3 = t3.fillna(0.0)
t4 = t4.fillna(0.0)

print("Placeholder LSD used:", LSD.date(), "| LSD window:", lsd_start.date(), "to", LSD.date(), "| Next window:", next_start.date(), "to", next_end.date())
display(t1)
display(t2)
display(t3)
display(t4)
