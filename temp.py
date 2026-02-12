p = "ABRAMS STS 2022"   # your exact program label
tmp = snap[(snap["PROGRAM"]==p) & (snap["PRODUCT_TEAM"]=="KUW")].copy()
print(tmp.groupby("COST_SET")["DATE"].agg(["min","max","nunique"]))
print(tmp[tmp["COST_SET"].isin(["BCWS","ACWP","ETC"])].sort_values(["COST_SET","DATE"]).tail(30))