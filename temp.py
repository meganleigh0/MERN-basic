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

# normalize without turning NaN into the string "nan"
s = cobra_merged_df["COST-SET"].astype("string").str.strip().str.upper()

# collapse variants, keep already-canonical values as-is
cobra_merged_df["COST-SET"] = s.map(cost_set_map).fillna(s)

# drop any rows where COST-SET is still missing (this is the only way to guarantee NONE remain)
cobra_merged_df = cobra_merged_df[cobra_merged_df["COST-SET"].notna()]

# verify: ONLY the 4 categories, and no NaN row
cobra_merged_df["COST-SET"].value_counts(dropna=False)