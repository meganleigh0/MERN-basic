# 1) show the last 10 unique dates for one program that looks wrong
p = "ABRAMS STS 2022"   # change to the one you’re debugging
d = base_evms.loc[base_evms["PROGRAM"]==p, "DATE"].drop_duplicates().sort_values()
print(d.tail(10).to_list())

# 2) confirm whether BCWS rows are incremental or cumulative
sample = base_evms[(base_evms["PROGRAM"]==p) & (base_evms["COST_SET"]=="BCWS")][["DATE","HOURS"]].sort_values("DATE")
print(sample.tail(15))