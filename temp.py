# ==========================
# DEBUG CELL (copy/paste)
# ==========================
import pandas as pd
import numpy as np

df = cobra_merged_df.copy()

# normalize cols
df.columns = (
    pd.Index(df.columns).astype(str).str.strip()
    .str.replace(" ", "_", regex=False)
    .str.replace("-", "_", regex=False)
    .str.upper()
)
if "COSTSET" in df.columns and "COST_SET" not in df.columns:
    df = df.rename(columns={"COSTSET":"COST_SET"})
if "SUBTEAM" in df.columns and "SUB_TEAM" not in df.columns:
    df = df.rename(columns={"SUBTEAM":"SUB_TEAM"})

print("COLUMNS:", list(df.columns)[:40])
need = ["DATE","COST_SET","HOURS","PROGRAM"]
print("MISSING REQUIRED:", [c for c in need if c not in df.columns])

df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce").fillna(0.0)

# if program is missing, you'll see it here
if "PROGRAM" not in df.columns:
    df["PROGRAM"] = "UNKNOWN_PROGRAM"

df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip().str.upper()
if "SUB_TEAM" in df.columns:
    df["SUB_TEAM"] = df["SUB_TEAM"].astype(str).str.strip().str.upper()
else:
    df["SUB_TEAM"] = "ALL"

# year filter check
d2026 = df[df["DATE"].dt.year == 2026].copy()
print("\nRAW DATE RANGE (all):", df["DATE"].min(), "to", df["DATE"].max(), " rows:", len(df))
print("RAW DATE RANGE (2026):", d2026["DATE"].min(), "to", d2026["DATE"].max(), " rows:", len(d2026))

# show top COST_SET values (this is usually the root cause)
print("\nTOP COST_SET (2026):")
display(d2026["COST_SET"].astype(str).str.strip().str.upper().value_counts().head(25).to_frame("count"))

# specifically check for BCWP/ACWP existence by exact matches + common alternates
s = d2026["COST_SET"].astype(str).str.strip().str.upper()
flags = pd.DataFrame({
    "BCWS_exact": s.eq("BCWS") | s.eq("BUDGET"),
    "BCWP_exact": s.eq("BCWP") | s.eq("PROGRESS"),
    "ACWP_exact": s.eq("ACWP") | s.str.contains("ACWP", na=False),
    "ETC_exact":  s.eq("ETC")  | s.str.contains(r"\bETC\b", na=False),
})
print("\nDO WE EVEN HAVE THESE BUCKETS IN RAW (2026)?")
display(flags.mean().to_frame("pct_of_rows"))

# pick one program that is failing (top by row count) and show last 50 rows for that program for those cost-sets
prog = d2026["PROGRAM"].value_counts().index[0]
print("\nInvestigating program:", prog)

dP = d2026[d2026["PROGRAM"] == prog].copy()
dP = dP.sort_values("DATE")

print("\nProgram date range:", dP["DATE"].min(), "to", dP["DATE"].max(), "rows:", len(dP))
print("Top COST_SET for program:")
display(dP["COST_SET"].astype(str).str.upper().value_counts().head(25).to_frame("count"))

# last 80 rows that look like EVMS signals (BCWS/BCWP/ACWP/ETC/BUDGET/PROGRESS)
mask = dP["COST_SET"].astype(str).str.upper().str.contains(r"BCWS|BCWP|ACWP|ETC|BUDGET|PROGRESS", regex=True, na=False)
print("\nLast 80 EVMS-like rows for program:")
display(dP.loc[mask, ["DATE","COST_SET","HOURS","SUB_TEAM"]].tail(80))