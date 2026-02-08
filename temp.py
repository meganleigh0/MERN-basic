import pandas as pd
import numpy as np

d = cobra_merged_df.copy()
d["DATE"]  = pd.to_datetime(d["DATE"], errors="coerce")
d["HOURS"] = pd.to_numeric(d["HOURS"], errors="coerce").fillna(0)

d["PROGRAM"]  = d["PROGRAM"].astype("string").fillna("").str.strip()
d["COST-SET"] = d["COST-SET"].astype("string").fillna("").str.strip().str.upper()

# --- per-program close date (max DATE where ACWP or BCWP exists)
close_by_prog = (
    d.loc[d["COST-SET"].isin(["ACWP","BCWP"]) & d["DATE"].notna()]
    .groupby("PROGRAM")["DATE"].max()
)

# If you truly want ONE close date for all programs, uncomment the next line:
# close_by_prog = close_by_prog.apply(lambda _: close_by_prog.max())

# --- fiscal year start per-program (calendar-year start; change if FY differs)
fy_start_by_prog = close_by_prog.apply(lambda x: pd.Timestamp(x.year, 1, 1))

# --- build PROGRAM index
programs = pd.Index(sorted(d["PROGRAM"].unique()), name="PROGRAM")
df_program_evms = pd.DataFrame({"PROGRAM": programs})

# --- helper sums
def sum_cs_upto(cs, cutoff_series, start_series=None):
    x = d[d["COST-SET"] == cs].copy()
    x = x.merge(cutoff_series.rename("CLOSE"), left_on="PROGRAM", right_index=True, how="left")
    if start_series is not None:
        x = x.merge(start_series.rename("START"), left_on="PROGRAM", right_index=True, how="left")
        mask = x["DATE"].notna() & x["CLOSE"].notna() & (x["DATE"] <= x["CLOSE"]) & (x["DATE"] >= x["START"])
    else:
        mask = x["DATE"].notna() & x["CLOSE"].notna() & (x["DATE"] <= x["CLOSE"])
    out = x.loc[mask].groupby("PROGRAM")["HOURS"].sum().reindex(programs, fill_value=0)
    return out.to_numpy()

def point_in_time(cs, cutoff_series):
    x = d[d["COST-SET"] == cs].copy()
    x = x.merge(cutoff_series.rename("CLOSE"), left_on="PROGRAM", right_index=True, how="left")
    # pull cs ONLY at the program’s close date
    out = x.loc[x["DATE"].notna() & x["CLOSE"].notna() & (x["DATE"] == x["CLOSE"])].groupby("PROGRAM")["HOURS"].sum()
    return out.reindex(programs, fill_value=0).to_numpy()

# --- CTD sums (through close)
df_program_evms["BCWS_CTD"] = sum_cs_upto("BCWS", close_by_prog)
df_program_evms["BCWP_CTD"] = sum_cs_upto("BCWP", close_by_prog)
df_program_evms["ACWP_CTD"] = sum_cs_upto("ACWP", close_by_prog)

# --- ETC at status close date (not cumulative)
df_program_evms["ETC_CTD"]  = point_in_time("ETC", close_by_prog)

# --- YTD sums (fy start through close)
df_program_evms["BCWS_YTD"] = sum_cs_upto("BCWS", close_by_prog, fy_start_by_prog)
df_program_evms["BCWP_YTD"] = sum_cs_upto("BCWP", close_by_prog, fy_start_by_prog)
df_program_evms["ACWP_YTD"] = sum_cs_upto("ACWP", close_by_prog, fy_start_by_prog)

# --- ETC_YTD: keep as point-in-time too (Cobra usually treats ETC as “current forecast”)
df_program_evms["ETC_YTD"]  = point_in_time("ETC", close_by_prog)

# --- ratios (no NaNs)
df_program_evms["SPI_CTD"] = np.where(df_program_evms["BCWS_CTD"].to_numpy() == 0, 0, df_program_evms["BCWP_CTD"] / df_program_evms["BCWS_CTD"])
df_program_evms["CPI_CTD"] = np.where(df_program_evms["ACWP_CTD"].to_numpy() == 0, 0, df_program_evms["BCWP_CTD"] / df_program_evms["ACWP_CTD"])
df_program_evms["SPI_YTD"] = np.where(df_program_evms["BCWS_YTD"].to_numpy() == 0, 0, df_program_evms["BCWP_YTD"] / df_program_evms["BCWS_YTD"])
df_program_evms["CPI_YTD"] = np.where(df_program_evms["ACWP_YTD"].to_numpy() == 0, 0, df_program_evms["BCWP_YTD"] / df_program_evms["ACWP_YTD"])

# --- final cleanup
df_program_evms = df_program_evms.fillna(0)

# quick diagnostics: show which programs have ETC missing at close date (should now be 0, but we can detect)
diag = (
    pd.DataFrame({"PROGRAM": programs})
    .assign(CLOSE=close_by_prog.reindex(programs))
)
print("Per-program close dates used:\n", diag)

display(df_program_evms)