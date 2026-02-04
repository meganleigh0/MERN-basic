import pandas as pd

# --- Load / stack your CAP extracts ---
abrams_df   = pd.read_excel("data/Cobra-Abrams STS.xlsx", sheet_name="CAP_Extract")
abrams22_df = pd.read_excel("data/Cobra-Abrams STS 2022.xlsx", sheet_name="CAP_Extract")

merged_df = pd.concat([abrams22_df, abrams_df], ignore_index=True)

# --- Load IPT reference ---
ipt_ref = pd.read_excel("data/abrams_ipt_ref.xlsx")

def norm_key(s: pd.Series) -> pd.Series:
    # handles ints, floats like 1300.0, strings with spaces, etc.
    return (
        s.astype("string")
         .str.strip()
         .str.replace(r"\.0$", "", regex=True)
    )

# Normalize BOTH sides of the join key
merged_df["SUB_TEAM_KEY"] = norm_key(merged_df["SUB_TEAM"])
ipt_ref["CA_KEY"] = norm_key(ipt_ref["Control Account No"])

# If the ref has duplicates per key, pick one deterministically (or adjust rule as needed)
ipt_ref = (
    ipt_ref.sort_values(["CA_KEY"])
           .drop_duplicates(subset=["CA_KEY"], keep="first")
)

# LEFT join from your data -> reference (keeps your rows, fills IPT when match exists)
abrams_m_df = merged_df.merge(
    ipt_ref[["CA_KEY", "IPT", "Control Account No", "Activity Desc."]],
    left_on="SUB_TEAM_KEY",
    right_on="CA_KEY",
    how="left"
)

# Quick diagnostics
print("IPT missing:", abrams_m_df["IPT"].isna().sum(), "of", len(abrams_m_df))
print(abrams_m_df["IPT"].value_counts(dropna=False).head(20))

# Optional: see which SUB_TEAMs still don't map
unmapped = (abrams_m_df.loc[abrams_m_df["IPT"].isna(), "SUB_TEAM_KEY"]
            .value_counts()
            .head(30))
print("Top unmapped SUB_TEAMs:\n", unmapped)