import pandas as pd

# ------------------ FILES / COLUMNS (STS 2022 ONLY) ------------------
STS2022_FILE   = "data/Cobra-Abrams STS 2022.xlsx"
STS2022_SHEET  = "CAP_Extract"
REF_FILE       = "data/abrams_ipt_ref.xlsx"

CAP_SUBTEAM_COL = "SUB_TEAM"            # in CAP_Extract
REF_CA_COL      = "Control Account No"  # in abrams_ipt_ref.xlsx

# ------------------ NORMALIZE KEYS (EXACT MATCH ONLY) ------------------
def normalize_key(s: pd.Series) -> pd.Series:
    s = s.astype("string")
    s = s.str.replace("\u00A0", " ", regex=False).str.strip()     # NBSP + trim
    s = s.str.replace(r"\.0$", "", regex=True)                    # drop trailing .0
    s = s.str.replace("–", "-", regex=False).str.replace("—", "-", regex=False)
    s = s.str.replace(r"\s*-\s*", "-", regex=True)                # "A - 1" -> "A-1"
    s = s.str.replace(r"\s+", "", regex=True).str.upper()         # remove internal spaces + case
    return s.replace("", pd.NA)

# ------------------ READ DATA ------------------
cap = pd.read_excel(STS2022_FILE, sheet_name=STS2022_SHEET)
ref = pd.read_excel(REF_FILE)

# ------------------ BUILD JOIN KEYS ------------------
cap["sub_team_key"] = normalize_key(cap[CAP_SUBTEAM_COL])
ref["control_acct_key"] = normalize_key(ref[REF_CA_COL])

# Optional (recommended): dedupe reference keys to avoid duplicate rows after merge
ref = (
    ref.dropna(subset=["control_acct_key"])
       .drop_duplicates(subset=["control_acct_key"], keep="first")
)

# ------------------ MERGE (EXACT MATCH AFTER NORMALIZATION) ------------------
sts2022_with_ipt = cap.merge(
    ref[["control_acct_key", "IPT"]],
    left_on="sub_team_key",
    right_on="control_acct_key",
    how="left"
).drop(columns=["control_acct_key"])

# ------------------ QUICK QA ------------------
total = len(sts2022_with_ipt)
missing = int(sts2022_with_ipt["IPT"].isna().sum())
print(f"STS2022 rows: {total:,} | IPT missing: {missing:,} ({missing/total:.1%})")

print("\nTop IPT values:")
print(sts2022_with_ipt["IPT"].value_counts(dropna=False).head(15))

print("\nTop unmapped SUB_TEAM keys:")
print(sts2022_with_ipt.loc[sts2022_with_ipt["IPT"].isna(), "sub_team_key"].value_counts().head(20))