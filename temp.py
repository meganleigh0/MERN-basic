import pandas as pd

# Hard-coded COST-SET normalization
cost_set_map = {
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    "ACWP": "ACWP",
    "ACWP_HRS": "ACWP",
    "ACTUALS": "ACWP",

    "BCWP": "BCWP",
    "PROGRESS": "BCWP",

    "ETC": "ETC",
    "EAC": "ETC",
}

cobra_merged_df["COST-SET"] = [
    cost_set_map.get(str(v).strip().upper(), str(v).strip().upper())
    for v in cobra_merged_df["COST-SET"]
]

# quick sanity check
cobra_merged_df["COST-SET"].value_counts()