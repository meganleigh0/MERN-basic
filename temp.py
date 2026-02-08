import pandas as pd

d = cobra_merged_df.copy()
d["DATE"] = pd.to_datetime(d["DATE"], errors="coerce")
d["HOURS"] = pd.to_numeric(d["HOURS"], errors="coerce")

# 1) What is the max DATE by COST-SET? (this catches "future BCWS/ETC" vs status-limited ACWP/BCWP)
max_by_costset = d.groupby("COST-SET")["DATE"].max().sort_values(ascending=False)

# 2) For each PROGRAM, what is the last date where we actually have ACWP or BCWP?
# (This is the best proxy for "latest close/status date" in weekly Cobra feeds)
last_actual_prog = (
    d[d["COST-SET"].isin(["ACWP","BCWP"])]
    .groupby("PROGRAM")["DATE"].max()
    .sort_values(ascending=False)
)

# 3) Show which PROGRAMs have YTD=0 for ACWP/BCWP, but CTD>0 (means your YTD start is wrong)
latest_close_proxy = d[d["COST-SET"].isin(["ACWP","BCWP"])]["DATE"].max()
fy_start_guess = pd.Timestamp(latest_close_proxy.year, 1, 1)

ctd = (d[(d["DATE"] <= latest_close_proxy)]
       .pivot_table(index="PROGRAM", columns="COST-SET", values="HOURS", aggfunc="sum", fill_value=0))
ytd = (d[(d["DATE"] <= latest_close_proxy) & (d["DATE"] >= fy_start_guess)]
       .pivot_table(index="PROGRAM", columns="COST-SET", values="HOURS", aggfunc="sum", fill_value=0))

for col in ["ACWP","BCWP","BCWS","ETC"]:
    if col not in ctd.columns: ctd[col]=0
    if col not in ytd.columns: ytd[col]=0

check = pd.DataFrame({
    "CTD_ACWP": ctd["ACWP"],
    "CTD_BCWP": ctd["BCWP"],
    "YTD_ACWP": ytd["ACWP"],
    "YTD_BCWP": ytd["BCWP"],
})
check["FLAG_ctd_has_data_ytd_zero"] = ((check["CTD_ACWP"]>0) | (check["CTD_BCWP"]>0)) & ((check["YTD_ACWP"]==0) & (check["YTD_BCWP"]==0))
flagged = check[check["FLAG_ctd_has_data_ytd_zero"]].sort_values(["CTD_ACWP","CTD_BCWP"], ascending=False)

print("Max DATE by COST-SET:\n", max_by_costset, "\n")
print("Latest close date proxy (max DATE where ACWP/BCWP exists):", latest_close_proxy, "\n")
print("Last ACWP/BCWP date by PROGRAM (top 10):\n", last_actual_prog.head(10), "\n")
print("Programs with CTD data but YTD=0 using Jan-1 start (top 20):\n", flagged.head(20))