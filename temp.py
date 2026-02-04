import pandas as pd
import re

def normalize_ca_key(s: pd.Series) -> pd.Series:
    """
    Normalizes keys for EXACT matching only (no fuzzy logic):
    - convert to string
    - strip leading/trailing whitespace (incl NBSP)
    - remove all internal whitespace
    - normalize hyphen spacing (A - 1 -> A-1)
    - uppercase
    - remove trailing .0 (Excel floats)
    """
    s = s.astype("string")

    # Replace non-breaking spaces + trim
    s = s.str.replace("\u00A0", " ", regex=False).str.strip()

    # Remove trailing .0 (common when Excel numeric becomes float)
    s = s.str.replace(r"\.0$", "", regex=True)

    # Normalize hyphens/dashes to plain "-"
    s = s.str.replace("–", "-", regex=False).str.replace("—", "-", regex=False)

    # Remove spaces around hyphens: "2096 - 4" -> "2096-4"
    s = s.str.replace(r"\s*-\s*", "-", regex=True)

    # Remove ALL remaining whitespace inside the string: "2096 - 4 " / "2096  -4" -> "2096-4"
    s = s.str.replace(r"\s+", "", regex=True)

    # Uppercase for consistency (in case of aa vs AA)
    s = s.str.upper()

    # Treat empty strings as missing
    s = s.replace("", pd.NA)

    return s


# --- Build normalized join keys ---
merged_df["SUB_TEAM_KEY"] = normalize_ca_key(merged_df["SUB_TEAM"])
ipt_ref["CA_KEY"] = normalize_ca_key(ipt_ref["Control Account No"])

# Optional: enforce uniqueness on the reference side (recommended)
# If CA_KEY duplicates exist, this can create duplicated rows after merge.
dupes = ipt_ref["CA_KEY"].duplicated(keep=False)
if dupes.any():
    print("WARNING: Duplicate Control Account No keys in reference (showing first 20):")
    print(ipt_ref.loc[dupes, ["Control Account No", "CA_KEY", "IPT"]].head(20))
    # choose a rule; simplest is keep first
    ipt_ref = ipt_ref.drop_duplicates(subset=["CA_KEY"], keep="first")

# --- EXACT merge (post-normalization) ---
abrams_m_df = merged_df.merge(
    ipt_ref[["CA_KEY", "IPT"]],
    left_on="SUB_TEAM_KEY",
    right_on="CA_KEY",
    how="left"
)

# --- Diagnostics ---
print("IPT missing:", abrams_m_df["IPT"].isna().sum(), "of", len(abrams_m_df))
print(abrams_m_df["IPT"].value_counts(dropna=False).head(20))

unmapped = abrams_m_df.loc[abrams_m_df["IPT"].isna(), "SUB_TEAM_KEY"].value_counts().head(30)
print("Top unmapped SUB_TEAM_KEYs:\n", unmapped)