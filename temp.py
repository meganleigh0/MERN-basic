# ============================================================
# EVMS (Cobra) Pipeline — single-cell, robust, debug-friendly
# Assumes you already have: cobra_merged_df (raw merged Cobra rows)
# Required columns (case-insensitive): PROGRAM, SUB_TEAM, COST_SET, DATE, HOURS
# Optional: CHG#, PLUG (ignored)
# ============================================================

import pandas as pd
import numpy as np

# ----------------------------
# 1) Configure period ends (2026)
# ----------------------------
# IMPORTANT:
# - These must be the *accounting period end* dates used by your org (the Sundays / period closes).
# - Replace with your exact 2026 list if this differs.
PERIOD_ENDS_2026 = pd.to_datetime([
    "2026-01-04",
    "2026-02-01",
    "2026-03-01",
    "2026-04-05",
    "2026-05-03",
    "2026-06-07",
    "2026-07-05",
    "2026-08-02",
    "2026-09-06",
    "2026-10-04",
    "2026-11-01",
    "2026-12-27",  # matches your screenshots
]).sort_values()

# ----------------------------
# 2) Helpers
# ----------------------------
def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Upper/strip column names; return a copy."""
    out = df.copy()
    out.columns = [str(c).strip().upper() for c in out.columns]
    return out

def _normalize_program(x):
    if pd.isna(x): return pd.NA
    s = str(x).strip()
    return s

def _normalize_subteam(x):
    if pd.isna(x): return pd.NA
    s = str(x).strip()
    # keep numeric-like subteams as string
    return s

def _normalize_costset(x):
    if pd.isna(x): return pd.NA
    s = str(x).strip().upper()
    s = s.replace("COST-SET", "COST_SET").replace("COST SET", "COST_SET")
    return s

def map_cost_set_to_bucket(cost_set_series: pd.Series) -> pd.Series:
    """
    Robust mapping that WILL NOT throw dtype promotion errors.
    Returns pd.Series(dtype="object") with values in {BCWS, BCWP, ACWP, ETC} or <NA>.
    """
    s = cost_set_series.astype("string").str.strip().str.upper()

    out = pd.Series(pd.NA, index=s.index, dtype="object")

    # Exact matches first (your data often already has BCWS/BCWP/ACWP/ETC)
    exact_map = {"BCWS": "BCWS", "BCWP": "BCWP", "ACWP": "ACWP", "ETC": "ETC"}
    out = out.where(~s.isin(exact_map.keys()), s.map(exact_map).astype("object"))

    # Conservative keyword fallbacks (only used when not already mapped)
    unm = out.isna()

    # Typical alternates you showed earlier: BUDGET ~ BCWS, PROGRESS ~ BCWP, ACWP_HRS ~ ACWP
    out.loc[unm & s.str.contains(r"\bBUDGET\b", regex=True, na=False)] = "BCWS"
    out.loc[unm & s.str.contains(r"\bPROGRESS\b", regex=True, na=False)] = "BCWP"
    out.loc[unm & s.str.contains(r"\bACWP\b", regex=True, na=False)] = "ACWP"
    out.loc[unm & s.str.contains(r"\bETC\b", regex=True, na=False)] = "ETC"

    return out

def assign_period_end(dates: pd.Series, period_ends: pd.DatetimeIndex) -> pd.Series:
    """
    Assign each DATE to the first period_end >= DATE.
    Fixes your prior IndexError by safe-clipping indices.
    """
    pe = pd.to_datetime(pd.Series(period_ends)).sort_values().to_numpy(dtype="datetime64[ns]")
    d = pd.to_datetime(dates, errors="coerce").to_numpy(dtype="datetime64[ns]")

    # searchsorted on pe for each date
    idx = np.searchsorted(pe, d, side="left")

    # idx == len(pe) means DATE after last period end => NaT
    out = np.full(shape=len(d), fill_value=np.datetime64("NaT"), dtype="datetime64[ns]")
    ok = idx < len(pe)
    out[ok] = pe[idx[ok]]

    return pd.to_datetime(out)

def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce")
    out = numer / denom.replace({0: np.nan})
    return out

def _pick_lsd_period_end(df_year: pd.DataFrame, as_of_date=None) -> pd.Timestamp:
    """
    LSD = "last status period end" based on as_of_date (default: max DATE in filtered year).
    """
    if as_of_date is None:
        as_of_date = df_year["DATE"].max()
    else:
        as_of_date = pd.to_datetime(as_of_date)

    # as_of_date may be between period ends; choose first period end >= as_of_date
    # if beyond last period end, clamp to last available period_end in list
    pe = df_year["PERIOD_END"].dropna().sort_values().unique()
    if len(pe) == 0:
        return pd.NaT

    pe = pd.to_datetime(pe)
    idx = np.searchsorted(pe.to_numpy(dtype="datetime64[ns]"), np.datetime64(as_of_date), side="left")
    if idx >= len(pe):
        return pd.Timestamp(pe[-1])
    return pd.Timestamp(pe[idx])

def _next_period_end(lsd: pd.Timestamp, period_ends: pd.DatetimeIndex) -> pd.Timestamp:
    pe = pd.to_datetime(pd.Series(period_ends)).sort_values()
    if pd.isna(lsd):
        return pd.NaT
    idx = np.searchsorted(pe.to_numpy(dtype="datetime64[ns]"), np.datetime64(lsd), side="right")
    if idx >= len(pe):
        return pd.NaT
    return pd.Timestamp(pe.iloc[idx])

# ----------------------------
# 3) Main builder
# ----------------------------
def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    period_ends=PERIOD_ENDS_2026,
    year_filter: int = 2026,
    as_of_date=None,
    debug_program: str | None = None,   # e.g., "ABRAMS_22"
):
    issues = []
    df0 = _standardize_cols(cobra_merged_df)

    required = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required if c not in df0.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df0.columns)}")

    # Normalize columns
    df = df0.copy()
    df["PROGRAM"]  = df["PROGRAM"].map(_normalize_program)
    df["SUB_TEAM"] = df["SUB_TEAM"].map(_normalize_subteam)
    df["COST_SET"] = df["COST_SET"].map(_normalize_costset)

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    # Drop unusable
    df = df.dropna(subset=["PROGRAM", "DATE", "HOURS", "COST_SET"]).copy()

    # Filter year (fixes the “2028 rows break next period logic” you saw)
    df = df[df["DATE"].dt.year == int(year_filter)].copy()
    if df.empty:
        issues.append(f"No rows after year_filter={year_filter}. Check DATE parsing / filter.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    # Map EVMS bucket
    df["EVMS_BUCKET"] = map_cost_set_to_bucket(df["COST_SET"])
    unmapped_pct = df["EVMS_BUCKET"].isna().mean()
    if unmapped_pct > 0:
        issues.append(f"{unmapped_pct:.2%} of rows have unmapped COST_SET -> EVMS_BUCKET.")

    df = df.dropna(subset=["EVMS_BUCKET"]).copy()

    # Assign period end
    df["PERIOD_END"] = assign_period_end(df["DATE"], pd.DatetimeIndex(period_ends))
    nat_pct = df["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"{nat_pct:.2%} of rows could not be assigned a PERIOD_END (DATE beyond calendar?).")

    df = df.dropna(subset=["PERIOD_END"]).copy()

    # ------------------------------------------
    # Period aggregation (program and program+subteam)
    # ------------------------------------------
    def _pivot_period(base: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        g = (base.groupby(keys + ["PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
                 .sum()
                 .reset_index())
        p = (g.pivot_table(index=keys + ["PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum"))
        p = p.reset_index()
        for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
            if col not in p.columns:
                p[col] = np.nan  # keep NaN to signal truly missing vs. 0
        # ensure numeric
        for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
            p[col] = pd.to_numeric(p[col], errors="coerce")
        return p

    period_prog = _pivot_period(df, ["PROGRAM"])
    period_sub  = _pivot_period(df, ["PROGRAM", "SUB_TEAM"])

    # ------------------------------------------
    # Add cumulative + CTD deltas
    # ------------------------------------------
    def _add_cum_and_ctd(p: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        out = p.sort_values(keys + ["PERIOD_END"]).copy()
        for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
            out[f"{col}_CUM"] = out.groupby(keys, dropna=False)[col].cumsum(min_count=1)
            out[f"{col}_CTD"] = out.groupby(keys, dropna=False)[f"{col}_CUM"].diff().fillna(out[f"{col}_CUM"])
        # Ratios on PERIOD (LSD-style) and on CUM (CTD-style)
        out["SPI_LSD"] = safe_div(out["BCWP"], out["BCWS"])
        out["CPI_LSD"] = safe_div(out["BCWP"], out["ACWP"])
        out["SPI_CTD"] = safe_div(out["BCWP_CUM"], out["BCWS_CUM"])
        out["CPI_CTD"] = safe_div(out["BCWP_CUM"], out["ACWP_CUM"])
        return out

    period_prog = _add_cum_and_ctd(period_prog, ["PROGRAM"])
    period_sub  = _add_cum_and_ctd(period_sub,  ["PROGRAM", "SUB_TEAM"])

    # ------------------------------------------
    # LSD and next period selection
    # ------------------------------------------
    lsd_pe = _pick_lsd_period_end(df, as_of_date=as_of_date)
    next_pe = _next_period_end(lsd_pe, pd.DatetimeIndex(period_ends))

    if pd.isna(lsd_pe):
        issues.append("Could not determine LSD PERIOD_END (no PERIOD_END values after filtering).")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    # PROGRAM OVERVIEW (one row per program, using the LSD period)
    prog_lsd = period_prog[period_prog["PERIOD_END"] == lsd_pe].copy()
    if prog_lsd.empty:
        issues.append(f"No program-level rows found for LSD period_end={lsd_pe.date()}.")

    program_overview_evms = prog_lsd[[
        "PROGRAM", "PERIOD_END",
        "BCWS", "BCWP", "ACWP", "ETC",
        "SPI_LSD", "CPI_LSD",
        "BCWS_CUM", "BCWP_CUM", "ACWP_CUM", "ETC_CUM",
        "SPI_CTD", "CPI_CTD",
    ]].rename(columns={
        "PERIOD_END": "LAST_STATUS_PERIOD_END",
        "BCWS": "BCWS_LSD",
        "BCWP": "BCWP_LSD",
        "ACWP": "ACWP_LSD",
        "ETC": "ETC_LSD",
    }).reset_index(drop=True)

    # SUBTEAM SPI/CPI table (LSD & CTD)
    sub_lsd = period_sub[period_sub["PERIOD_END"] == lsd_pe].copy()
    subteam_spi_cpi = sub_lsd[[
        "PROGRAM", "SUB_TEAM", "PERIOD_END",
        "SPI_LSD", "CPI_LSD", "SPI_CTD", "CPI_CTD",
        "BCWS", "BCWP", "ACWP", "ETC"
    ]].rename(columns={"PERIOD_END": "LAST_STATUS_PERIOD_END"}).reset_index(drop=True)

    # BAC/EAC/VAC (hours) per PROGRAM+SUB_TEAM at LSD:
    # BAC ~ BCWS_CUM, EAC ~ ACWP_CUM + ETC_CUM (if ETC exists), VAC = BAC - EAC
    bac_eac = period_sub[period_sub["PERIOD_END"] == lsd_pe].copy()
    bac_eac["BAC_HRS"] = bac_eac["BCWS_CUM"]
    bac_eac["EAC_HRS"] = bac_eac["ACWP_CUM"] + bac_eac["ETC_CUM"]
    bac_eac["VAC_HRS"] = bac_eac["BAC_HRS"] - bac_eac["EAC_HRS"]
    subteam_bac_eac_vac = bac_eac[["PROGRAM", "SUB_TEAM", "BAC_HRS", "EAC_HRS", "VAC_HRS"]].reset_index(drop=True)

    # Program hours forecast table (CTD & next-period)
    # Demand_HRS_CTD ~ BCWS_CUM, Actual_HRS_CTD ~ ACWP_CUM (hours); pct_var = (actual-demand)/demand
    prog_ctd = period_prog[period_prog["PERIOD_END"] == lsd_pe].copy()
    prog_ctd["DEMAND_HRS_CTD"] = prog_ctd["BCWS_CUM"]
    prog_ctd["ACTUAL_HRS_CTD"] = prog_ctd["ACWP_CUM"]
    prog_ctd["PCT_VARIANCE_CTD"] = safe_div(prog_ctd["ACTUAL_HRS_CTD"] - prog_ctd["DEMAND_HRS_CTD"], prog_ctd["DEMAND_HRS_CTD"])

    # Next period pulls
    next_rows = period_prog[period_prog["PERIOD_END"] == next_pe][["PROGRAM", "BCWS", "ETC"]].copy()
    next_rows = next_rows.rename(columns={"BCWS": "NEXT_PERIOD_BCWS_HRS", "ETC": "NEXT_PERIOD_ETC_HRS"})

    program_hours_forecast = prog_ctd[[
        "PROGRAM", "PERIOD_END", "DEMAND_HRS_CTD", "ACTUAL_HRS_CTD", "PCT_VARIANCE_CTD"
    ]].rename(columns={"PERIOD_END": "LAST_STATUS_PERIOD_END"}).merge(
        next_rows, on="PROGRAM", how="left"
    )
    program_hours_forecast["NEXT_PERIOD_END"] = next_pe

    # Add NEXT_PERIOD_N = number of distinct DATEs present for that program in next period window
    # Window definition: (lsd_pe, next_pe] based on assigned PERIOD_END (simpler, consistent)
    if not pd.isna(next_pe):
        df_next = df[df["PERIOD_END"] == next_pe].copy()
        next_n = (df_next.groupby("PROGRAM")["DATE"].nunique().reset_index(name="NEXT_PERIOD_N"))
        program_hours_forecast = program_hours_forecast.merge(next_n, on="PROGRAM", how="left")
    else:
        program_hours_forecast["NEXT_PERIOD_N"] = np.nan
        issues.append("No next period end exists after LSD (LSD is last period in calendar).")

    # ------------------------------------------
    # Quality checks to catch the “too many zeros/missings” problem
    # ------------------------------------------
    def _flag_missing_reason(period_df: pd.DataFrame, level_name: str):
        # Missing BCWP/ACWP in LSD period is the #1 reason SPI/CPI go NaN or 0-looking
        lsd = period_df[period_df["PERIOD_END"] == lsd_pe].copy()
        if lsd.empty:
            issues.append(f"{level_name}: No rows at LSD period {lsd_pe.date()}.")
            return

        miss_bcwp = lsd["BCWP"].isna().mean()
        miss_acwp = lsd["ACWP"].isna().mean()
        miss_bcws = lsd["BCWS"].isna().mean()
        if miss_bcws > 0 or miss_bcwp > 0 or miss_acwp > 0:
            issues.append(
                f"{level_name}: LSD missing rates — BCWS:{miss_bcws:.1%}, BCWP:{miss_bcwp:.1%}, ACWP:{miss_acwp:.1%}. "
                "If these should exist, the issue is upstream: COST_SET mapping or data not present for that period."
            )

    _flag_missing_reason(period_prog, "PROGRAM")
    _flag_missing_reason(period_sub,  "SUBTEAM")

    # ------------------------------------------
    # Optional deep debug for one program
    # ------------------------------------------
    if debug_program is not None:
        dbg = str(debug_program).strip()
        print("\n" + "="*70)
        print(f"DEBUG PROGRAM: {dbg}")
        dprog = df[df["PROGRAM"] == dbg].copy()
        print("Rows in year:", len(dprog))
        if len(dprog):
            print("DATE range:", dprog["DATE"].min(), "to", dprog["DATE"].max())
            print("\nTop COST_SET:")
            display(dprog["COST_SET"].value_counts().head(15).to_frame("count"))
            print("\nEVMS_BUCKET counts:")
            display(dprog["EVMS_BUCKET"].value_counts(dropna=False).to_frame("count"))
            print("\nLast 15 period totals (program-level):")
            disp = period_prog[period_prog["PROGRAM"] == dbg].sort_values("PERIOD_END").tail(15)
            display(disp[["PERIOD_END","BCWS","BCWP","ACWP","ETC","SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD"]])
            print("\nRaw rows in LSD period (last 120):")
            lsd_rows = dprog[dprog["PERIOD_END"] == lsd_pe].sort_values("DATE").tail(120)
            display(lsd_rows[["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]])

        print("="*70 + "\n")

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues


# ============================================================
# RUN IT (example)
# ============================================================
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    period_ends=PERIOD_ENDS_2026,
    year_filter=2026,
    as_of_date=None,              # or set a specific date like "2026-12-31"
    debug_program=None,           # set to "ABRAMS_22" if you want deep debug printed
)

print("ISSUES:")
for i in issues:
    print("-", i)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))