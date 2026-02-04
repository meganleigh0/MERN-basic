import pandas as pd
import numpy as np

# -----------------------------
# 1) Accounting period close dates
# -----------------------------
# IMPORTANT:
# These 2026 period_end dates are inferred from your own debug table screenshot.
# If ANY differ from your official calendar, update the list below.
PERIOD_ENDS_2026 = pd.to_datetime([
    "2026-01-04",
    "2026-02-01",
    "2026-03-01",
    "2026-04-05",
    "2026-05-03",
    "2026-06-07",
    "2026-07-05",
    "2026-08-02",
    "2026-09-27",
    "2026-10-04",
    "2026-11-01",
    "2026-12-27",
]).sort_values()

def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # normalize column names
    df.columns = (
        pd.Index(df.columns)
        .astype(str)
        .str.strip()
        .str.replace("-", "_", regex=False)
        .str.replace(" ", "_", regex=False)
        .str.upper()
    )

    # common aliases
    rename = {}
    if "COSTSET" in df.columns and "COST_SET" not in df.columns:
        rename["COSTSET"] = "COST_SET"
    if "COST_SET" not in df.columns and "COST_SET" not in rename:
        # try original if present
        pass

    if "HOURS" not in df.columns:
        # sometimes Cobra exports use "HRS"
        if "HRS" in df.columns:
            rename["HRS"] = "HOURS"

    if "SUBTEAM" in df.columns and "SUB_TEAM" not in df.columns:
        rename["SUBTEAM"] = "SUB_TEAM"

    df = df.rename(columns=rename)

    # hard requirements
    required = ["DATE", "COST_SET", "HOURS"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)[:50]}")

    # ensure PROGRAM exists (you have it)
    if "PROGRAM" not in df.columns:
        raise ValueError("Missing PROGRAM column. (Pipeline assumes multi-program merged DF.)")

    # SUB_TEAM optional; create a stable placeholder
    if "SUB_TEAM" not in df.columns:
        df["SUB_TEAM"] = "ALL"

    return df

def _clean_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")
    # COST_SET -> string safely
    df["COST_SET"] = df["COST_SET"].astype("string")
    df["COST_SET"] = df["COST_SET"].str.strip().str.upper()

    # PROGRAM, SUB_TEAM normalize
    df["PROGRAM"] = df["PROGRAM"].astype("string").str.strip()
    df["SUB_TEAM"] = df["SUB_TEAM"].astype("string").str.strip().fillna("ALL")

    # drop unusable rows
    df = df.dropna(subset=["DATE", "HOURS", "COST_SET", "PROGRAM"])
    return df

def _map_cost_set_to_bucket(cost_set: pd.Series) -> pd.Series:
    """
    Map raw Cobra COST_SET values into EVMS buckets.
    Fixes the dtype promotion error by staying in pandas 'string/object' land.
    """
    s = cost_set.astype("string").str.strip().str.upper()

    # direct mapping (your screenshots show these exact values)
    direct = {
        "BUDGET": "BCWS",
        "BCWS": "BCWS",
        "PROGRESS": "BCWP",
        "BCWP": "BCWP",
        "ACWP_HRS": "ACWP",
        "ACWP": "ACWP",
        "ACTUALS": "ACWP",   # sometimes used interchangeably in exports
        "ETC": "ETC",
        "EAC": "EAC",        # kept in case you want it later
    }

    out = s.map(direct)

    # conservative heuristics (only fill if still missing)
    # Use Series.where / mask (NO np.where) to avoid dtype promotion errors.
    missing = out.isna()

    # NOTE: keep these conservative to avoid mis-bucketing
    out = out.mask(missing & s.str.contains(r"\bBCWS\b|\bBUDGET\b", regex=True), "BCWS")
    missing = out.isna()
    out = out.mask(missing & s.str.contains(r"\bBCWP\b|\bPROGRESS\b", regex=True), "BCWP")
    missing = out.isna()
    out = out.mask(missing & s.str.contains(r"\bACWP\b|\bACTUAL\b", regex=True), "ACWP")
    missing = out.isna()
    out = out.mask(missing & s.str.contains(r"\bETC\b", regex=True), "ETC")

    # return as object to keep compatibility with groupby/pivot
    return out.astype("object")

def _assign_period_end(dates: pd.Series, period_ends: pd.DatetimeIndex) -> pd.Series:
    """
    Assign each transaction date to the FIRST period_end >= DATE (ceiling).
    If DATE is beyond last close date, returns NaT (we'll handle that).
    """
    pe = pd.to_datetime(period_ends).sort_values().to_numpy(dtype="datetime64[ns]")
    d = pd.to_datetime(dates).to_numpy(dtype="datetime64[ns]")

    idx = np.searchsorted(pe, d, side="left")
    # idx == len(pe) => beyond last period end
    out = np.where(idx < len(pe), pe[idx], np.datetime64("NaT"))
    return pd.to_datetime(out)

def _pick_lsd_period(as_of_date: pd.Timestamp, period_ends: pd.DatetimeIndex) -> pd.Timestamp:
    """
    LSD period = greatest period_end <= as_of_date
    """
    pe = pd.to_datetime(period_ends).sort_values()
    as_of_date = pd.to_datetime(as_of_date)
    valid = pe[pe <= as_of_date]
    if len(valid) == 0:
        return pe.min()
    return valid.max()

def _next_period_end(lsd_end: pd.Timestamp, period_ends: pd.DatetimeIndex) -> pd.Timestamp:
    pe = pd.to_datetime(period_ends).sort_values()
    later = pe[pe > pd.to_datetime(lsd_end)]
    return later.min() if len(later) else pd.NaT

def _safe_div(n: pd.Series, d: pd.Series) -> pd.Series:
    d0 = d.replace(0, np.nan)
    return n / d0

def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    period_ends: pd.DatetimeIndex = PERIOD_ENDS_2026,
    as_of_date: str | pd.Timestamp | None = None,
    year_filter: int | None = 2026,  # set None if you truly want multi-year (your data shows 2028 rows)
):
    """
    Outputs:
      program_overview_evms
      subteam_spi_cpi
      subteam_bac_eac_vac
      program_hours_forecast
      plus an 'issues' list describing any coverage gaps
    """
    issues = []

    df = _standardize_cols(cobra_merged_df)
    df = _clean_types(df)

    # Optional: prevent 2028/etc from polluting 2026 dashboard outputs
    if year_filter is not None:
        df = df[df["DATE"].dt.year == year_filter].copy()

    # Map cost sets
    df["EVMS_BUCKET"] = _map_cost_set_to_bucket(df["COST_SET"])
    unmapped = df["EVMS_BUCKET"].isna().mean()
    if unmapped > 0:
        issues.append(f"Unmapped COST_SET rows: {unmapped:.2%} (will be excluded from EVMS calcs).")
        df = df.dropna(subset=["EVMS_BUCKET"]).copy()

    # Assign accounting period end
    df["PERIOD_END"] = _assign_period_end(df["DATE"], period_ends)
    if df["PERIOD_END"].isna().any():
        pct = df["PERIOD_END"].isna().mean()
        issues.append(
            f"{pct:.2%} of rows fall after last period_end (PERIOD_END=NaT). "
            f"Update period_ends or filter years."
        )
        df = df.dropna(subset=["PERIOD_END"]).copy()

    # Pick AS_OF_DATE and LSD
    if as_of_date is None:
        # safest default: use max DATE in filtered data
        as_of_date = df["DATE"].max()
    as_of_date = pd.to_datetime(as_of_date)

    lsd_end = _pick_lsd_period(as_of_date, period_ends)
    nxt_end = _next_period_end(lsd_end, period_ends)

    # -----------------------------
    # 2) Period-level pivot (Program + Subteam + Period)
    # -----------------------------
    g = (
        df.groupby(["PROGRAM", "SUB_TEAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .reset_index()
    )

    pivot = (
        g.pivot_table(
            index=["PROGRAM", "SUB_TEAM", "PERIOD_END"],
            columns="EVMS_BUCKET",
            values="HOURS",
            aggfunc="sum",
        )
        .reset_index()
    )

    # Ensure required bucket columns exist (as columns)
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in pivot.columns:
            pivot[c] = np.nan  # keep NaN (do NOT force 0 yet)

    # Sort for cumulative sums
    pivot = pivot.sort_values(["PROGRAM", "SUB_TEAM", "PERIOD_END"]).reset_index(drop=True)

    # For period *amounts*, a missing bucket in that period should be treated as 0 hours (not "unknown")
    # AFTER mapping is correct, this is appropriate.
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        pivot[c] = pivot[c].fillna(0.0)

    # CTD cumulative sums per program+subteam
    for c in ["BCWS", "BCWP", "ACWP"]:
        pivot[f"{c}_CTD"] = pivot.groupby(["PROGRAM", "SUB_TEAM"], dropna=False)[c].cumsum()

    # CPI/SPI CTD at each period_end
    pivot["SPI_CTD"] = _safe_div(pivot["BCWP_CTD"], pivot["BCWS_CTD"])
    pivot["CPI_CTD"] = _safe_div(pivot["BCWP_CTD"], pivot["ACWP_CTD"])

    # -----------------------------
    # 3) Slice LSD rows (period values) + CTD at LSD
    # -----------------------------
    p_lsd = pivot[pivot["PERIOD_END"] == lsd_end].copy()
    if p_lsd.empty:
        issues.append(f"No pivot rows found for LSD period_end={lsd_end.date()}. Check period mapping.")
        # create empty tables to avoid hard crash
        return (
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues
        )

    # LSD ratios
    p_lsd["SPI_LSD"] = _safe_div(p_lsd["BCWP"], p_lsd["BCWS"])
    p_lsd["CPI_LSD"] = _safe_div(p_lsd["BCWP"], p_lsd["ACWP"])

    # Next period values (for forecast table)
    if pd.isna(nxt_end):
        issues.append("No NEXT period_end exists after LSD (at end of calendar). NEXT period fields will be NaN.")
        next_vals = pivot.iloc[0:0].copy()
    else:
        next_vals = pivot[pivot["PERIOD_END"] == nxt_end].copy()

    # -----------------------------
    # 4) Program overview table (aggregate subteams -> program)
    # -----------------------------
    # Sum across subteams at LSD for period values + CTD values (already CTD within subteam, sum OK if subteams partition)
    prog_lsd = (
        p_lsd.groupby(["PROGRAM"], dropna=False)[
            ["BCWS", "BCWP", "ACWP", "ETC", "BCWS_CTD", "BCWP_CTD", "ACWP_CTD"]
        ].sum()
        .reset_index()
    )
    prog_lsd["LAST_STATUS_PERIOD_END"] = lsd_end

    # LSD ratios at program level
    prog_lsd["SPI_LSD"] = _safe_div(prog_lsd["BCWP"], prog_lsd["BCWS"])
    prog_lsd["CPI_LSD"] = _safe_div(prog_lsd["BCWP"], prog_lsd["ACWP"])

    # CTD ratios at program level
    prog_lsd["SPI_CTD"] = _safe_div(prog_lsd["BCWP_CTD"], prog_lsd["BCWS_CTD"])
    prog_lsd["CPI_CTD"] = _safe_div(prog_lsd["BCWP_CTD"], prog_lsd["ACWP_CTD"])

    # -----------------------------
    # 5) Subteam SPI/CPI table (LSD + CTD)
    # -----------------------------
    subteam_spi_cpi = p_lsd[[
        "PROGRAM", "SUB_TEAM", "LAST_STATUS_PERIOD_END", "PERIOD_END",
        "SPI_LSD", "SPI_CTD", "CPI_LSD", "CPI_CTD"
    ]].copy()

    # Make columns match your expected names
    subteam_spi_cpi = subteam_spi_cpi.rename(columns={"PERIOD_END": "LSD_PERIOD_END"})
    subteam_spi_cpi["LAST_STATUS_PERIOD_END"] = lsd_end  # explicit

    # -----------------------------
    # 6) Subteam BAC/EAC/VAC table (in hours)
    # -----------------------------
    # BAC = total budget in hours -> use BCWS_CTD at LSD (contract-to-date budget accumulated to date)
    # EAC = CTD ACWP + ETC (ETC for period; if ETC is only monthly, LSD is typically the one you want)
    sub_bac = p_lsd.groupby(["PROGRAM", "SUB_TEAM"], dropna=False)[["BCWS_CTD", "ACWP_CTD", "ETC"]].sum().reset_index()
    sub_bac["BAC_HRS"] = sub_bac["BCWS_CTD"]
    sub_bac["EAC_HRS"] = sub_bac["ACWP_CTD"] + sub_bac["ETC"]
    sub_bac["VAC_HRS"] = sub_bac["BAC_HRS"] - sub_bac["EAC_HRS"]
    subteam_bac_eac_vac = sub_bac[["PROGRAM", "SUB_TEAM", "BAC_HRS", "EAC_HRS", "VAC_HRS"]].copy()

    # -----------------------------
    # 7) Program hours forecast table
    # -----------------------------
    # demand (CTD) ~ BCWS_CTD, actual (CTD) ~ ACWP_CTD, variance = (actual - demand)/demand
    prog_fore = prog_lsd[["PROGRAM", "LAST_STATUS_PERIOD_END", "BCWS_CTD", "ACWP_CTD"]].copy()
    prog_fore = prog_fore.rename(columns={"BCWS_CTD": "DEMAND_HRS_CTD", "ACWP_CTD": "ACTUAL_HRS_CTD"})
    prog_fore["PCT_VARIANCE_CTD"] = _safe_div(prog_fore["ACTUAL_HRS_CTD"] - prog_fore["DEMAND_HRS_CTD"], prog_fore["DEMAND_HRS_CTD"])
    prog_fore["NEXT_PERIOD_END"] = nxt_end

    if not pd.isna(nxt_end) and not next_vals.empty:
        next_prog = (
            next_vals.groupby("PROGRAM", dropna=False)[["BCWS", "ETC"]].sum().reset_index()
            .rename(columns={"BCWS": "NEXT_PERIOD_BCWS_HRS", "ETC": "NEXT_PERIOD_ETC_HRS"})
        )
        prog_fore = prog_fore.merge(next_prog, on="PROGRAM", how="left")
    else:
        prog_fore["NEXT_PERIOD_BCWS_HRS"] = np.nan
        prog_fore["NEXT_PERIOD_ETC_HRS"] = np.nan

    # NEXT_PERIOD_N (1..12) based on index in calendar
    pe = pd.to_datetime(period_ends).sort_values().tolist()
    try:
        prog_fore["NEXT_PERIOD_N"] = pe.index(pd.to_datetime(nxt_end)) + 1 if not pd.isna(nxt_end) else np.nan
    except ValueError:
        prog_fore["NEXT_PERIOD_N"] = np.nan

    program_hours_forecast = prog_fore.copy()

    # Final: rename program overview columns to your typical naming
    program_overview_evms = prog_lsd.rename(columns={
        "BCWS": "BCWS_LSD",
        "BCWP": "BCWP_LSD",
        "ACWP": "ACWP_LSD",
        "ETC": "ETC_LSD",
    })

    # If you want to keep only key columns:
    program_overview_evms = program_overview_evms[[
        "PROGRAM",
        "LAST_STATUS_PERIOD_END",
        "ACWP_CTD", "BCWP_CTD", "BCWS_CTD",
        "ETC_LSD", "BCWS_LSD", "BCWP_LSD", "ACWP_LSD",
        "SPI_CTD", "CPI_CTD", "SPI_LSD", "CPI_LSD"
    ]]

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues


# -----------------------------
# RUN IT (example)
# -----------------------------
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    period_ends=PERIOD_ENDS_2026,
    as_of_date=None,      # uses max DATE in data (after year_filter)
    year_filter=2026      # IMPORTANT to prevent 2028 rows from breaking NEXT period logic
)

print("issues:", issues)
display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))



# --- DEBUG CELL ---
dbg_df = _clean_types(_standardize_cols(cobra_merged_df))

# If you want to mirror pipeline behavior:
dbg_df = dbg_df[dbg_df["DATE"].dt.year == 2026].copy()

dbg_df["EVMS_BUCKET"] = _map_cost_set_to_bucket(dbg_df["COST_SET"])
dbg_df["PERIOD_END"] = _assign_period_end(dbg_df["DATE"], PERIOD_ENDS_2026)

print("Date range:", dbg_df["DATE"].min(), "to", dbg_df["DATE"].max())
print("Unmapped COST_SET %:", float(dbg_df["EVMS_BUCKET"].isna().mean()))

print("\nTop COST_SET values (overall):")
display(dbg_df["COST_SET"].value_counts().head(25))

print("\nEVMS_BUCKET counts (post mapping):")
display(dbg_df["EVMS_BUCKET"].value_counts(dropna=False))

# Pick a program to investigate
program_to_check = dbg_df["PROGRAM"].dropna().astype(str).sort_values().unique()[0]
print("\nInvestigating program:", program_to_check)

p = dbg_df[dbg_df["PROGRAM"] == program_to_check].copy()
print("\nTop COST_SET values for this program:")
display(p["COST_SET"].value_counts().head(25))

print("\nEVMS_BUCKET counts for this program:")
display(p["EVMS_BUCKET"].value_counts(dropna=False))

# Show last few period_end values present
p_period = (
    p.dropna(subset=["EVMS_BUCKET", "PERIOD_END"])
     .groupby(["PERIOD_END", "EVMS_BUCKET"])["HOURS"].sum()
     .reset_index()
     .pivot(index="PERIOD_END", columns="EVMS_BUCKET", values="HOURS")
     .sort_index()
)

print("\nLast 15 period rows (program-level):")
display(p_period.tail(15))

# Identify LSD used by pipeline
as_of = p["DATE"].max()
lsd = _pick_lsd_period(as_of, PERIOD_ENDS_2026)
nxt = _next_period_end(lsd, PERIOD_ENDS_2026)
print("\nAS_OF_DATE:", as_of)
print("LSD PERIOD_END:", lsd)
print("NEXT PERIOD_END:", nxt)

print("\nLSD row values (BCWS/BCWP/ACWP/ETC):")
if lsd in p_period.index:
    display(p_period.loc[[lsd], ["BCWS","BCWP","ACWP","ETC"]].fillna("Missing value"))
else:
    print("No LSD row found in p_period (means PERIOD_END assignment/buckets missing for this program).")

print("\nRaw EVMS rows in LSD period (last 200):")
raw_lsd = p[p["PERIOD_END"] == lsd][["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]].sort_values("DATE").tail(200)
display(raw_lsd)