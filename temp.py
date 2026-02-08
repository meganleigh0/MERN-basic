import pandas as pd
import numpy as np

# ----------------------------
# 0) Prep
# ----------------------------
df = cobra_merged_df.copy()

df["DATE"]  = pd.to_datetime(df["DATE"], errors="coerce")
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0)

for c in ["PROGRAM","SUB_TEAM","COST-SET"]:
    df[c] = df[c].astype("string").fillna("").str.strip()

df["COST-SET"] = df["COST-SET"].str.upper()

# Optional: keep only hour rows if PLUG exists
if "PLUG" in df.columns:
    df["PLUG"] = df["PLUG"].astype("string").fillna("").str.strip().str.upper()
    df = df[df["PLUG"].isin(["HOURS",""])].copy()

# ----------------------------
# 1) Accounting close dates (EDIT THIS LIST to match your calendar)
# ----------------------------
close_dates = pd.to_datetime([
    "2026-01-04","2026-01-23",
    "2026-02-01","2026-02-20",
    "2026-03-01","2026-03-20","2026-03-29",
    "2026-04-05","2026-04-24",
    "2026-05-03","2026-05-31",
    "2026-06-07","2026-06-26",
    "2026-07-05","2026-07-24",
    "2026-08-02","2026-08-30",
    "2026-09-27",
    "2026-10-04","2026-10-30",
    "2026-11-01","2026-11-29",
    "2026-12-06","2026-12-27",
], errors="coerce").dropna().sort_values()

# ----------------------------
# 2) Map each row DATE -> STATUS_CLOSE (next close date on/after DATE)
#    THIS is the missing-data fix.
# ----------------------------
cal = pd.DataFrame({"STATUS_CLOSE": close_dates}).sort_values("STATUS_CLOSE")
tmp = df[["DATE"]].sort_values("DATE").copy()
tmp["ROW_ID"] = np.arange(len(tmp))

tmp = pd.merge_asof(
    tmp.sort_values("DATE"),
    cal,
    left_on="DATE",
    right_on="STATUS_CLOSE",
    direction="forward",          # map to next close date
    allow_exact_matches=True
)

df2 = df.copy()
df2 = df2.loc[tmp.index].copy()   # align to sorted tmp
df2["STATUS_CLOSE"] = tmp["STATUS_CLOSE"].to_numpy()
df2["DATE"] = tmp["DATE"].to_numpy()

# Drop rows that don't map to a close date (outside calendar)
df2 = df2[df2["STATUS_CLOSE"].notna()].copy()

# ----------------------------
# 3) Choose LSD based on where ACTUALS exist (ACWP/BCWP)
# ----------------------------
mask_actual = df2["COST-SET"].isin(["ACWP","BCWP"])
LSD = df2.loc[mask_actual, "STATUS_CLOSE"].max()

# Next close date
NEXT_CLOSE = cal.loc[cal["STATUS_CLOSE"] > LSD, "STATUS_CLOSE"].min()

# CTD includes all closes <= LSD
m_ctd = df2["STATUS_CLOSE"] <= LSD

# LSD period includes rows mapped to LSD
m_lsd = df2["STATUS_CLOSE"] == LSD

# Next period includes rows mapped to NEXT_CLOSE
m_next = df2["STATUS_CLOSE"] == NEXT_CLOSE

# ----------------------------
# 4) Helpers
# ----------------------------
def pivot_sum(mask, idx_cols):
    p = (df2.loc[mask]
           .pivot_table(index=idx_cols, columns="COST-SET", values="HOURS",
                        aggfunc="sum", fill_value=0)
           .reset_index())
    # Ensure needed columns exist
    for col in ["BCWS","BCWP","ACWP","ETC"]:
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

# ----------------------------
# TABLE 1) EVMS by PROGRAM: SPI/CPI CTD + LSD
# ----------------------------
ctd_prog = pivot_sum(m_ctd, ["PROGRAM"]).rename(columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD"})
lsd_prog = pivot_sum(m_lsd, ["PROGRAM"]).rename(columns={"BCWS":"BCWS_LSD","BCWP":"BCWP_LSD","ACWP":"ACWP_LSD"})

t1 = ctd_prog.merge(lsd_prog, on="PROGRAM", how="outer").fillna(0)
t1["SPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["BCWS_CTD"])
t1["CPI_CTD"] = safe_div(t1["BCWP_CTD"], t1["ACWP_CTD"])
t1["SPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["BCWS_LSD"])
t1["CPI_LSD"] = safe_div(t1["BCWP_LSD"], t1["ACWP_LSD"])

t1 = t1[["PROGRAM",
         "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
         "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"]].sort_values("PROGRAM").reset_index(drop=True)

# ----------------------------
# TABLE 2) EVMS by PROGRAM + SUB_TEAM: SPI/CPI CTD + LSD
# ----------------------------
ctd_pt = pivot_sum(m_ctd, ["PROGRAM","SUB_TEAM"]).rename(columns={"BCWS":"BCWS_CTD","BCWP":"BCWP_CTD","ACWP":"ACWP_CTD"})
lsd_pt = pivot_sum(m_lsd, ["PROGRAM","SUB_TEAM"]).rename(columns={"BCWS":"BCWS_LSD","BCWP":"BCWP_LSD","ACWP":"ACWP_LSD"})

t2 = ctd_pt.merge(lsd_pt, on=["PROGRAM","SUB_TEAM"], how="outer").fillna(0)
t2["SPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["BCWS_CTD"])
t2["CPI_CTD"] = safe_div(t2["BCWP_CTD"], t2["ACWP_CTD"])
t2["SPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["BCWS_LSD"])
t2["CPI_LSD"] = safe_div(t2["BCWP_LSD"], t2["ACWP_LSD"])

t2 = t2[["PROGRAM","SUB_TEAM",
         "BCWS_CTD","BCWP_CTD","ACWP_CTD","SPI_CTD","CPI_CTD",
         "BCWS_LSD","BCWP_LSD","ACWP_LSD","SPI_LSD","CPI_LSD"]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# ----------------------------
# TABLE 3) BAC/EAC/VAC by PROGRAM + SUB_TEAM
# You said you don't want ETC_CTD/YTD — so treat ETC as the current forecast at LSD period.
# ----------------------------
etc_lsd = pivot_sum(m_lsd, ["PROGRAM","SUB_TEAM"])[["PROGRAM","SUB_TEAM","ETC"]].rename(columns={"ETC":"ETC_LSD"})
t3 = t2.merge(etc_lsd, on=["PROGRAM","SUB_TEAM"], how="left").fillna({"ETC_LSD":0})

t3["BAC_HRS"] = t3["BCWS_CTD"]                 # matches your “BAC is total budget in hours” slide convention
t3["EAC_HRS"] = t3["ACWP_CTD"] + t3["ETC_LSD"]
t3["VAC_HRS"] = t3["BAC_HRS"] - t3["EAC_HRS"]

t3 = t3[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

# ----------------------------
# TABLE 4) Other measures by PROGRAM:
# Demand (BCWS_LSD), Actual (ACWP_LSD), %Var, Next period BCWS, Next period ETC
# ----------------------------
next_prog = pivot_sum(m_next, ["PROGRAM"]).rename(columns={"BCWS":"NextMo_BCWS_Hours","ETC":"NextMo_ETC_Hours"})
base_prog = lsd_prog.merge(next_prog[["PROGRAM","NextMo_BCWS_Hours","NextMo_ETC_Hours"]], on="PROGRAM", how="left").fillna(0)

t4 = base_prog[["PROGRAM"]].copy()
t4["Demand_Hours_LSD"] = base_prog["BCWS_LSD"].to_numpy()
t4["Actual_Hours_LSD"] = base_prog["ACWP_LSD"].to_numpy()

# %Var: if demand is 0, set 0 (not -1, not NaN)
d = t4["Demand_Hours_LSD"].to_numpy()
a = t4["Actual_Hours_LSD"].to_numpy()
t4["PctVar_LSD"] = np.where(d == 0, 0.0, (a - d) / d)

t4["NextMo_BCWS_Hours"] = base_prog["NextMo_BCWS_Hours"].to_numpy()
t4["NextMo_ETC_Hours"]  = base_prog["NextMo_ETC_Hours"].to_numpy()
t4 = t4.sort_values("PROGRAM").reset_index(drop=True)

# ----------------------------
# Final validation: no NaNs anywhere
# ----------------------------
for name, table in [("t1",t1),("t2",t2),("t3",t3),("t4",t4)]:
    if table.isna().any().any():
        raise ValueError(f"{name} still has NaNs")

print("LSD used:", LSD.date(), "| NEXT_CLOSE used:", (NEXT_CLOSE.date() if pd.notna(NEXT_CLOSE) else None))
display(t1)
display(t2)
display(t3)
display(t4)