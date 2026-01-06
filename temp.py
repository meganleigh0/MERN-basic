"""
SOL: ClauseBot+ Starter (Fresh + Clear)
What this does:
1) Loads your 3 Excel files (Results, Guidance, FAR/DFARS database)
2) Creates ONE clean master table: clauses_detected_enriched
3) Creates a simple "risk features" starter table for NLP/ML
4) Handles the reality that your contract PDF is scanned (no text) and shows what to do next

You can run this and immediately see:
- what clauses were detected
- what guidance exists
- how to map to FAR/DFARS reference rows
- what fields you can model on
"""

from pathlib import Path
import pandas as pd
import re

# ----------------------------
# A) CONFIG (change this)
# ----------------------------
DATA_DIR = Path(r"./CONTRACTS/data")  # <-- update path if needed

RESULTS_XLSX  = DATA_DIR / "BSCA-65AC2-2544 (Executed) Results.xlsx"
GUIDANCE_XLSX = DATA_DIR / "BSCA-65AC2-2544 (Executed) Guidance.xlsx"
REF_XLSX      = DATA_DIR / "GDMS FAR_DFARS Database 03-12-2025.xlsx"

pd.set_option("display.max_columns", 200)
pd.set_option("display.width", 140)

# ----------------------------
# B) LOAD THE TABLES
# ----------------------------
results = pd.read_excel(RESULTS_XLSX, sheet_name="Sheet1")
guid_raw = pd.read_excel(GUIDANCE_XLSX, sheet_name="Guidance.Raw")
guid_nodup = pd.read_excel(GUIDANCE_XLSX, sheet_name="Guidance.Sort.NoDupes")

far_ref = pd.read_excel(REF_XLSX, sheet_name="FAR_DATABASE")
dfars_ref = pd.read_excel(REF_XLSX, sheet_name="DFARS_DATABASE")
eff_thresholds = pd.read_excel(REF_XLSX, sheet_name="Effective Date- Thresholds")

print("\n=== What files/tables you have ===")
print(f"Results:  {results.shape}  (ClauseBot detected clauses for this contract)")
print(f"Guidance Raw: {guid_raw.shape} (Guidance text per clause)")
print(f"Guidance NoDupes: {guid_nodup.shape} (Same, cleaned)")
print(f"FAR Reference:   {far_ref.shape} (Master FAR clause library)")
print(f"DFARS Reference: {dfars_ref.shape} (Master DFARS clause library)")
print(f"Effective thresholds: {eff_thresholds.shape} (Rules / thresholds used by reference)")

print("\n=== First look: Results (top rows) ===")
display(results.head(10))

print("\n=== First look: Guidance (top rows) ===")
display(guid_nodup.head(10))

# ----------------------------
# C) STANDARDIZE COLUMN NAMES (so joins are easier)
# ----------------------------
def clean_cols(df):
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df

results = clean_cols(results)
guid = clean_cols(guid_nodup)
far_ref = clean_cols(far_ref)
dfars_ref = clean_cols(dfars_ref)

# Identify likely clause id columns (we’ll be defensive because we don’t know exact names)
def find_clause_col(df):
    candidates = [c for c in df.columns if "clause" in c.lower()]
    # Prefer ones that look like "Clause in Contract" or "Clause"
    pref = [c for c in candidates if "contract" in c.lower()] + candidates
    return pref[0] if pref else None

results_clause_col = find_clause_col(results)
guid_clause_col = find_clause_col(guid)

print("\n=== Detected key columns ===")
print("Results clause column:", results_clause_col)
print("Guidance clause column:", guid_clause_col)

# ----------------------------
# D) NORMALIZE CLAUSE IDS
# ----------------------------
def normalize_clause_id(x):
    if pd.isna(x): 
        return None
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)

    # If it contains FAR/DFARS label, keep it
    # Example patterns can vary; we keep the whole string but also create a numeric-ish token for matching
    # e.g. "DFARS 252.227-7013" or "52.204-21" etc
    s_compact = re.sub(r"\s+", "", s)  # remove spaces
    return s, s_compact

# Build normalized columns
results["clause_raw"], results["clause_key"] = zip(*results[results_clause_col].map(normalize_clause_id))
guid["clause_raw"], guid["clause_key"] = zip(*guid[guid_clause_col].map(normalize_clause_id))

# ----------------------------
# E) BUILD YOUR FIRST "MASTER" TABLE (this is the core dataset)
# ----------------------------
# 1) Join detected clauses + guidance (same contract)
clauses_detected = results.merge(
    guid.drop(columns=[guid_clause_col]),  # keep one copy of original clause column
    on="clause_key",
    how="left",
    suffixes=("_result", "_guid")
)

# 2) Attach FAR/DFARS reference (best-effort: we don’t know exact key columns there either)
def build_ref(ref_df, source_name):
    ref = ref_df.copy()
    # find a clause-like column in the reference
    ref_clause_col = find_clause_col(ref)
    if ref_clause_col is None:
        ref["ref_clause_key"] = None
        return ref, None
    ref["ref_clause_raw"], ref["ref_clause_key"] = zip(*ref[ref_clause_col].map(normalize_clause_id))
    ref["ref_source"] = source_name
    return ref, ref_clause_col

far_ref2, far_ref_clause_col = build_ref(far_ref, "FAR")
dfars_ref2, dfars_ref_clause_col = build_ref(dfars_ref, "DFARS")

ref_all = pd.concat([far_ref2, dfars_ref2], ignore_index=True)

# Join to reference
clauses_detected_enriched = clauses_detected.merge(
    ref_all.drop_duplicates(subset=["ref_clause_key"]),
    left_on="clause_key",
    right_on="ref_clause_key",
    how="left"
)

print("\n=== ✅ MASTER TABLE: clauses_detected_enriched ===")
print("Shape:", clauses_detected_enriched.shape)
print("This is your ONE row per detected clause + guidance + FAR/DFARS reference match.")
display(clauses_detected_enriched.head(20))

# ----------------------------
# F) CREATE A SIMPLE "RISK FEATURES" TABLE FOR NLP/ML
# ----------------------------
# This is not "AI magic" yet — it’s the clean features table you can model from.
def simple_risk_category(clause_text: str) -> str:
    if not clause_text:
        return "unknown"
    t = clause_text.lower()
    if "data" in t or "rights" in t or "technical" in t:
        return "ip/data_rights"
    if "termination" in t or "default" in t:
        return "termination"
    if "inspection" in t or "acceptance" in t or "quality" in t:
        return "quality/acceptance"
    if "delivery" in t or "schedule" in t or "delay" in t:
        return "schedule"
    if "payment" in t or "invoice" in t or "price" in t:
        return "payments/pricing"
    return "general"

# Try to pick a clause title-ish column from guidance or ref
title_candidates = [c for c in clauses_detected_enriched.columns if "title" in c.lower()]
title_col = title_candidates[0] if title_candidates else None

risk_features = pd.DataFrame({
    "contract_id": ["BSCA-65AC2-2544"] * len(clauses_detected_enriched),
    "clause_key": clauses_detected_enriched["clause_key"],
    "clause_raw": clauses_detected_enriched["clause_raw"],
    "clause_title": clauses_detected_enriched[title_col] if title_col else None,
    "flowdown_mandatory_flag": clauses_detected_enriched.get("Mandatory (M) or Optional (O) Flowdown", None),
    "functional_accountability": clauses_detected_enriched.get("Functional Accountability", None),
    "risk_bucket_guess": (clauses_detected_enriched[title_col].fillna("").map(simple_risk_category) if title_col else "unknown"),
})

print("\n=== Starter Risk Features Table (for ML/NLP) ===")
display(risk_features)

# Save clean outputs so you can reuse without reloading Excel every time
OUT_DIR = DATA_DIR / "_clausebot_plus_outputs"
OUT_DIR.mkdir(exist_ok=True)

clauses_detected_enriched.to_csv(OUT_DIR / "clauses_detected_enriched.csv", index=False)
risk_features.to_csv(OUT_DIR / "risk_features.csv", index=False)

print(f"\nSaved:\n- {OUT_DIR / 'clauses_detected_enriched.csv'}\n- {OUT_DIR / 'risk_features.csv'}")

# ----------------------------
# G) IMPORTANT: Your PDF is scanned. That means no text to embed/diff/search yet.
# What to do next:
# 1) Get the original contract as Word OR text-based PDF if available
# 2) Or run OCR (company-approved tool) to produce a searchable text layer
# ----------------------------
print("\n=== Next blocker to unlock NLP on the contract body ===")
print("Your executed contract PDF appears scanned. For document-to-document comparison/search we need OCR text.")
print("Once OCR exists, we'll create a 'contract_paragraphs' table and run embeddings + semantic diff + keyword risk scans.")