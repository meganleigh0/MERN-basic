cobra_merged_df["DATE"].max()
cobra_merged_df.loc[cobra_merged_df["COST-SET"].str.upper().isin(["BCWS","BUDGET","ETC"]), ["DATE","COST-SET","HOURS"]].sort_values("DATE").tail(20)