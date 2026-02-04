import pandas as pd

# ---------- HARD-CODE FILES / SHEETS ----------
CAP_FILES = [
    ("data/Cobra-Abrams STS 2022.xlsx", "CAP_Extract"),
    ("data/Cobra-Abrams STS.xlsx",      "CAP_Extract"),
]
IPT_REF_FILE  = "data/abrams_ipt_ref.xlsx"
IPT_CA_COL    = "Control Account No"   # in abrams_ipt_ref.xlsx
MERGE_SUB_COL = "SUB_TEAM"             # in CAP_Extract

# ---------- NORMALIZE KEYS (EXACT MATCH ONLY) ----------
def norm_key(x: pd.Series) -> pd.Series:
    x = x.astype("string")
    x = x.str.replace("\u00A0", " ", regex=False).str.strip()       # NBSP + trim
    x = x.str.replace(r"\.0$", "", regex=True)                      # drop trailing .0
    x = x.str.replace("–", "-", regex=False).str.replace("—", "-", regex=False)
    x = x.str.replace(r"\s*-\s*", "-", regex=True)                  # "A - 1" -> "A-1"
    x = x.str.replace(r"\s+", "", regex=True).str.upper()           # drop internal spaces + case
    return x.replace("", pd.NA)

# ---------- READ + STACK CAP EXTRACTS ----------
cap_dfs = [pd.read_excel(path, sheet_name=sheet) for path, sheet in CAP_FILES]
merged_df = pd.concat(cap_dfs, ignore_index=True)

# ---------- READ IPT REFERENCE + DEDUPE ----------
ipt_ref = pd.read_excel(IPT_REF_FILE)
ipt_ref["CA_KEY"] = norm_key(ipt_ref[IPT_CA_COL])
ipt_ref = ipt_ref.dropna(subset=["CA_KEY"]).drop_duplicates("CA_KEY", keep="first")[["CA_KEY", "IPT"]]

# ---------- BUILD JOIN KEY + EXACT MERGE ----------
merged_df["SUB_TEAM_KEY"] = norm_key(merged_df[MERGE_SUB_COL])

abrams_m_df = merged_df.merge(
    ipt_ref,
    left_on="SUB_TEAM_KEY",
    right_on="CA_KEY",
    how="left"
).drop(columns=["CA_KEY"])  # keep only IPT + your CAP columns

# ---------- QUICK QA ----------
print("IPT missing:", abrams_m_df["IPT"].isna().sum(), "of", len(abrams_m_df))
print(abrams_m_df["IPT"].value_counts(dropna=False).head(15))
print("\nTop unmapped SUB_TEAM_KEYs:\n",
      abrams_m_df.loc[abrams_m_df["IPT"].isna(), "SUB_TEAM_KEY"].value_counts().head(20))