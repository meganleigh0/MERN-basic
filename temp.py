# ==========================================================
# CELL 2 — DEBUGGING / INVESTIGATION (paste output back here)
# ==========================================================
# This cell will tell us *why* you were getting zeros/missing:
# usually LSD was being chosen in a future budget-only period,
# or ACWP/BCWP were not present in the “latest” periods.

df_dbg = cobra_merged_df.copy()
df_dbg.columns = [c.strip().upper().replace(" ", "_") for c in df_dbg.columns]
if "COST-SET" in df_dbg.columns: df_dbg = df_dbg.rename(columns={"COST-SET": "COST_SET"})
if "COSTSET"  in df_dbg.columns: df_dbg = df_dbg.rename(columns={"COSTSET": "COST_SET"})
if "SUBTEAM"  in df_dbg.columns: df_dbg = df_dbg.rename(columns={"SUBTEAM": "SUB_TEAM"})
if "COST_SET" not in df_dbg.columns:
    raise ValueError(f"Can't find COST_SET. Columns: {list(df_dbg.columns)}")

df_dbg["DATE"] = pd.to_datetime(df_dbg["DATE"], errors="coerce")
df_dbg["HOURS"] = pd.to_numeric(df_dbg["HOURS"], errors="coerce")
df_dbg = df_dbg.dropna(subset=["DATE", "HOURS"])
df_dbg["EVMS_BUCKET"] = _map_cost_sets_to_evms_bucket(df_dbg["COST_SET"])
df_dbg = df_dbg.dropna(subset=["EVMS_BUCKET"]).copy()

print("Date range:", df_dbg["DATE"].min(), "to", df_dbg["DATE"].max())
print("\nTop COST_SET values overall:\n", df_dbg["COST_SET"].astype(str).str.upper().value_counts().head(15))

# show which programs are missing which buckets
bucket_presence = (
    df_dbg.groupby(["PROGRAM", "EVMS_BUCKET"], as_index=False)["HOURS"].sum()
    .pivot_table(index="PROGRAM", columns="EVMS_BUCKET", values="HOURS", aggfunc="sum")
    .fillna(0)
)
display(bucket_presence.head(20))

# Pick one program you care about (ABRAMS_22 etc.)
PROGRAM_TO_INSPECT = "ABRAMS_22"
df_p = df_dbg[df_dbg["PROGRAM"].astype(str).str.strip().eq(PROGRAM_TO_INSPECT)].copy()
print(f"\nInspecting program: {PROGRAM_TO_INSPECT}  rows={len(df_p):,}")

# build period ends and period table for this program
period_ends = build_445_period_ends(first_period_end="2026-01-04", years=8)
df_p["PERIOD_END"] = assign_period_end(df_p["DATE"], period_ends)

p_period = _compute_period_table(df_p, keys=["PROGRAM"])  # program-level
p_period = _add_ctd_and_lsd(p_period, keys=["PROGRAM"])

# Show the last 20 periods with the key signals
cols = ["PERIOD_END", "BCWS", "BCWP", "ACWP", "ETC", "IS_LSD", "LAST_STATUS_PERIOD_END", "SPI_LSD", "CPI_LSD"]
cols = [c for c in cols if c in p_period.columns]
display(p_period.sort_values("PERIOD_END").tail(20)[cols])

# Also show: what is the *last* period where ACWP or BCWP exists?
mask = (p_period["ACWP"] > 0) | (p_period["BCWP"] > 0)
if mask.any():
    last_real = p_period.loc[mask, "PERIOD_END"].max()
    print("\nLast PERIOD_END with ACWP>0 or BCWP>0:", last_real)
    display(p_period[p_period["PERIOD_END"].eq(last_real)][["PERIOD_END","BCWS","BCWP","ACWP","ETC"]])
else:
    print("\nThis program has NO periods with ACWP>0 or BCWP>0. That would explain missing SPI/CPI.")