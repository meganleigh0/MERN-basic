import pandas as pd

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

cobra_merged_df["COST-SET"] = (
    cobra_merged_df["COST-SET"]
        .astype(str)                 # safe because originals are non-null
        .str.strip()
        .str.upper()
        .map(cost_set_map)
        .fillna(
            cobra_merged_df["COST-SET"]
                .astype(str)
                .str.strip()
                .str.upper()
        )
)

# verify
cobra_merged_df["COST-SET"].value_counts()