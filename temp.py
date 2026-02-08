import pandas as pd
import numpy as np

d = cobra_merged_df.copy()
d["DATE"]  = pd.to_datetime(d["DATE"], errors="coerce")
d["HOURS"] = pd.to_numeric(d["HOURS"], errors="coerce").fillna(0)

d["PROGRAM"]  = d["PROGRAM"].astype("string").fillna("").str.strip()
d["COST-SET"] = d["COST-SET"].astype("string").fillna("").str.strip().str.upper()

# ✅ Status close date must be driven by ACTUAL/PROGRESS (ACWP/BCWP), NOT future BCWS/ETC
close_date = d.loc[d["COST-SET"].isin(["ACWP","BCWP"]) & d["DATE"].notna(), "DATE"].max()

# FY start (adjust if your FY doesn't start Jan 1)
fy_start = pd.Timestamp(close_date.year, 1, 1)

# Masks
m_ctd = d["DATE"].notna() & (d["DATE"] <= close_date)
m_ytd = d["DATE"].notna() & (d["DATE"] >= fy_start) & (d["DATE"] <= close_date)

# Helper: sum HOURS for a costset under a mask
def sum_cs(mask, cs):
    return d.loc[mask & (d["COST-SET"] == cs)].groupby("PROGRAM")["HOURS"].sum()

programs = pd.Index(sorted(d["PROGRAM"].unique()), name="PROGRAM")

df_program_evms = pd.DataFrame(index=programs).reset_index()

# CTD totals (always present; missing -> 0)
df_program_evms["BCWS_CTD"] = sum_cs(m_ctd, "BCWS").reindex(programs, fill_value=0).to_numpy()
df_program_evms["BCWP_CTD"] = sum_cs(m_ctd, "BCWP").reindex(programs, fill_value=0).to_numpy()
df_program_evms["ACWP_CTD"] = sum_cs(m_ctd, "ACWP").reindex(programs, fill_value=0).to_numpy()
df_program_evms["ETC_CTD"]  = sum_cs(m_ctd, "ETC").reindex(programs, fill_value=0).to_numpy()

# YTD totals (missing -> 0)
df_program_evms["BCWS_YTD"] = sum_cs(m_ytd, "BCWS").reindex(programs, fill_value=0).to_numpy()
df_program_evms["BCWP_YTD"] = sum_cs(m_ytd, "BCWP").reindex(programs, fill_value=0).to_numpy()
df_program_evms["ACWP_YTD"] = sum_cs(m_ytd, "ACWP").reindex(programs, fill_value=0).to_numpy()
df_program_evms["ETC_YTD"]  = sum_cs(m_ytd, "ETC").reindex(programs, fill_value=0).to_numpy()

# Ratios with NO NaNs (return 0 when denom==0)
df_program_evms["SPI_CTD"] = np.where(df_program_evms["BCWS_CTD"].to_numpy() == 0, 0, df_program_evms["BCWP_CTD"] / df_program_evms["BCWS_CTD"])
df_program_evms["CPI_CTD"] = np.where(df_program_evms["ACWP_CTD"].to_numpy() == 0, 0, df_program_evms["BCWP_CTD"] / df_program_evms["ACWP_CTD"])
df_program_evms["SPI_YTD"] = np.where(df_program_evms["BCWS_YTD"].to_numpy() == 0, 0, df_program_evms["BCWP_YTD"] / df_program_evms["BCWS_YTD"])
df_program_evms["CPI_YTD"] = np.where(df_program_evms["ACWP_YTD"].to_numpy() == 0, 0, df_program_evms["BCWP_YTD"] / df_program_evms["ACWP_YTD"])

# Final: guarantee no missing anywhere
df_program_evms = df_program_evms.fillna(0)

print("Status close date used:", close_date.date())
print("FY start used:", fy_start.date())
display(df_program_evms)