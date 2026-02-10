# ============================================================
# EVMS PIPELINE (single-cell) — robust to:
# - COST-SET vs COST_SET naming
# - older pandas groupby/cumsum quirks (no numpy ops on groupby, no min_count)
# - year filtering (prevents 2028 rows from breaking NEXT period logic)
# - safe PERIOD_END assignment (no index-out-of-bounds)
# - cost-set mapping (BCWS/BCWP/ACWP/ETC and also BUDGET/PROGRESS variants)
#
# INPUT:  cobra_merged_df  (your merged Cobra dataframe)
#
# OUTPUTS:
#   program_overview_evms : one row per PROGRAM (LSD/CTD metrics)
#   subteam_spi_cpi       : PROGRAM x SUB_TEAM (SPI/CPI LSD/CTD + dates)
#   subteam_bac_eac_vac   : PROGRAM x SUB_TEAM (BAC/EAC/VAC hours)
#   program_hours_forecast: one row per PROGRAM (Demand/Actual CTD + next period hrs)
#   issues                : list[str] of warnings
#
# Optional: call debug_evms(cobra_merged_df, year_filter=2026, debug_program="ABRAMS_22")
# ============================================================

import pandas as pd
import numpy as np

# -----------------------------
# Helpers
# -----------------------------
def _to_datetime(s):
    return pd.to_datetime(s, errors="coerce")

def _safe_div(num, den):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    out = num / den
    out = out.where((den.notna()) & (den != 0))
    return out

def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    # normalize column names (keep originals but create canonical)
    cols = {c: c.strip().upper().replace(" ", "_").replace("-", "_") for c in d.columns}
    d.rename(columns=cols, inplace=True)

    # Canonical required names
    # PROGRAM, SUB_TEAM, COST_SET, DATE, HOURS
    # Try common aliases
    if "COST_SET" not in d.columns and "COSTSET" in d.columns:
        d["COST_SET"] = d["COSTSET"]
    if "SUBTEAM" in d.columns and "SUB_TEAM" not in d.columns:
        d["SUB_TEAM"] = d["SUBTEAM"]
    if "DT" in d.columns and "DATE" not in d.columns:
        d["DATE"] = d["DT"]

    return d

def _map_cost_set_to_bucket(cost_set: pd.Series) -> pd.Series:
    """
    Conservative mapping:
      - exact matches (ETC, BCWS, BCWP, ACWP) win
      - also supports BUDGET->BCWS, PROGRESS->BCWP, etc.
    Uses pure pandas masks (no np.where dtype promotion errors).
    """
    s = cost_set.astype(str).str.strip().str.upper()

    out = pd.Series(pd.NA, index=s.index, dtype="object")

    # Exact/common first
    out.loc[s.eq("BCWS")] = "BCWS"
    out.loc[s.eq("BCWP")] = "BCWP"
    out.loc[s.eq("ACWP")] = "ACWP"
    out.loc[s.eq("ETC")]  = "ETC"

    # Variants you’ve shown before
    out.loc[out.isna() & s.str.contains(r"\bBUDGET\b", regex=True)]   = "BCWS"
    out.loc[out.isna() & s.str.contains(r"\bBCWS\b", regex=True)]     = "BCWS"

    out.loc[out.isna() & s.str.contains(r"\bPROGRESS\b", regex=True)] = "BCWP"
    out.loc[out.isna() & s.str.contains(r"\bBCWP\b", regex=True)]     = "BCWP"

    out.loc[out.isna() & s.str.contains(r"\bACWP\b", regex=True)]     = "ACWP"

    out.loc[out.isna() & s.str.contains(r"\bETC\b", regex=True)]      = "ETC"

    return out

def _assign_period_end(dates: pd.Series, period_ends: list[pd.Timestamp]) -> pd.Series:
    """
    Assign each DATE to the *next* period_end >= DATE (searchsorted left).
    If DATE beyond last period_end -> NaT (no crash).
    """
    pe = pd.to_datetime(pd.Series(period_ends), errors="coerce").dropna().sort_values().unique()
    pe = pd.to_datetime(pe)

    d = _to_datetime(dates)
    arr = d.values.astype("datetime64[ns]")

    # handle all-NaT
    if len(pe) == 0:
        return pd.to_datetime(pd.Series([pd.NaT] * len(d), index=d.index))

    pe_arr = pe.values.astype("datetime64[ns]")
    idx = np.searchsorted(pe_arr, arr, side="left")  # first pe >= date

    # idx == len(pe) means beyond last period
    out = pd.Series(pd.NaT, index=d.index, dtype="datetime64[ns]")
    m = (idx >= 0) & (idx < len(pe_arr)) & (~pd.isna(d))
    out.loc[m] = pe_arr[idx[m]]
    return pd.to_datetime(out)

def _pick_lsd_period_end(period_df: pd.DataFrame, as_of_date: pd.Timestamp) -> pd.Timestamp:
    """
    LSD period end = latest PERIOD_END <= as_of_date that exists in period_df.
    """
    if period_df.empty:
        return pd.NaT
    pe = period_df["PERIOD_END"].dropna().sort_values().unique()
    if len(pe) == 0:
        return pd.NaT
    as_of = pd.to_datetime(as_of_date, errors="coerce")
    if pd.isna(as_of):
        return pe[-1]
    pe_le = pe[pe <= as_of]
    return pe_le[-1] if len(pe_le) else pe[-1]

def _last_nonnull(series: pd.Series):
    s = series.dropna()
    return s.iloc[-1] if len(s) else np.nan

# -----------------------------
# Core builder
# -----------------------------
def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    period_ends: list[pd.Timestamp] | None = None,
    year_filter: int | None = None,
    as_of_date: str | pd.Timestamp | None = None,
    debug_program: str | None = None,
):
    issues = []

    df0 = _standardize_cols(cobra_merged_df)

    required_any = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required_any if c not in df0.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df0.columns)}")

    df = df0.copy()
    df["DATE"] = _to_datetime(df["DATE"])
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")
    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    df["SUB_TEAM"] = df["SUB_TEAM"].astype(str).str.strip()
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip()

    # Filter year early (prevents 2028 from breaking NEXT period logic)
    if year_filter is not None:
        df = df[df["DATE"].dt.year == int(year_filter)].copy()
        if df.empty:
            issues.append(f"No rows after year_filter={year_filter}. Check DATE parsing and filter.")
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    # as_of_date defaults to max DATE in filtered data
    if as_of_date is None:
        as_of = df["DATE"].max()
    else:
        as_of = pd.to_datetime(as_of_date, errors="coerce")
        if pd.isna(as_of):
            as_of = df["DATE"].max()
            issues.append("as_of_date could not be parsed; using max DATE in data.")

    # cost set -> bucket
    df["EVMS_BUCKET"] = _map_cost_set_to_bucket(df["COST_SET"])
    before = len(df)
    df = df.dropna(subset=["EVMS_BUCKET", "DATE", "HOURS"]).copy()
    dropped = before - len(df)
    if dropped:
        issues.append(f"Dropped {dropped:,} rows missing EVMS_BUCKET/DATE/HOURS after cleaning.")

    # period ends: if not provided, infer from unique period-like dates (sorted) — but best to pass calendar list
    if period_ends is None:
        # infer by taking sorted unique DATEs (this can be large; downsample to week-ending Sundays if needed)
        # Here: use sorted unique of DATE normalized to date; then treat as period ends directly
        pe = pd.to_datetime(sorted(df["DATE"].dt.normalize().unique()))
        period_ends = list(pe)
        issues.append("period_ends was None; inferred from unique DATEs. Pass PERIOD_ENDS_2026 for calendar accuracy.")

    # assign period end safely
    df["PERIOD_END"] = _assign_period_end(df["DATE"], period_ends)
    nat_pct = df["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"{nat_pct:.1%} of rows have PERIOD_END=NaT (DATE beyond last period_end). Extend period_ends list.")

    df = df.dropna(subset=["PERIOD_END"]).copy()

    # -----------------------------
    # Aggregate to period totals
    # -----------------------------
    period_prog = (
        df.groupby(["PROGRAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .unstack("EVMS_BUCKET")
          .reset_index()
    )

    period_sub = (
        df.groupby(["PROGRAM", "SUB_TEAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .unstack("EVMS_BUCKET")
          .reset_index()
    )

    for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if col not in period_prog.columns:
            period_prog[col] = np.nan
        if col not in period_sub.columns:
            period_sub[col] = np.nan
        period_prog[col] = pd.to_numeric(period_prog[col], errors="coerce")
        period_sub[col] = pd.to_numeric(period_sub[col], errors="coerce")

    # -----------------------------
    # Cumulative sums for BCWS/BCWP/ACWP
    # (ETC is often not additive; but users want Next Period ETC hours too.
    # We'll compute ETC_CUM for completeness but metrics will use ETC_LSD (last non-null) not ETC_CUM.
    # -----------------------------
    def _add_cum(df_period: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        out = df_period.sort_values(keys + ["PERIOD_END"]).copy()

        for col in ["BCWS", "BCWP", "ACWP"]:
            out[f"{col}_CTD"] = out[col]
            out[f"{col}_CUM"] = out[col].fillna(0).groupby(out[keys], dropna=False).cumsum()

        # ETC: keep both CTD and CUM (but use LSD last value later)
        out["ETC_CTD"] = out["ETC"]
        out["ETC_CUM"] = out["ETC"].fillna(0).groupby(out[keys], dropna=False).cumsum()

        out["SPI_LSD"] = _safe_div(out["BCWP"], out["BCWS"])
        out["CPI_LSD"] = _safe_div(out["BCWP"], out["ACWP"])
        out["SPI_CTD"] = _safe_div(out["BCWP_CUM"], out["BCWS_CUM"])
        out["CPI_CTD"] = _safe_div(out["BCWP_CUM"], out["ACWP_CUM"])

        return out

    period_prog = _add_cum(period_prog, ["PROGRAM"])
    period_sub  = _add_cum(period_sub,  ["PROGRAM", "SUB_TEAM"])

    # -----------------------------
    # Determine LSD period end per PROGRAM (and per SUBTEAM within PROGRAM) based on as_of_date
    # -----------------------------
    lsd_by_program = (
        period_prog.groupby("PROGRAM", dropna=False)["PERIOD_END"]
        .apply(lambda s: _pick_lsd_period_end(pd.DataFrame({"PERIOD_END": s}), as_of))
        .to_dict()
    )

    # For subteams: LSD tied to program LSD to match reporting cadence
    # (you can change later to subteam-specific LSD if needed)
    period_sub["LSD_PERIOD_END"] = period_sub["PROGRAM"].map(lsd_by_program)
    period_prog["LSD_PERIOD_END"] = period_prog["PROGRAM"].map(lsd_by_program)

    # -----------------------------
    # Pull LSD snapshots + CTD as-of LSD
    # -----------------------------
    # Program LSD row
    prog_lsd = period_prog[period_prog["PERIOD_END"] == period_prog["LSD_PERIOD_END"]].copy()

    # If some programs have no exact match (rare), fallback to last period row
    if prog_lsd.empty and not period_prog.empty:
        issues.append("No exact PROGRAM LSD match found; using last PERIOD_END per program.")
        prog_lsd = period_prog.sort_values(["PROGRAM", "PERIOD_END"]).groupby("PROGRAM", dropna=False).tail(1).copy()

    # Subteam LSD rows (use program LSD end)
    sub_lsd = period_sub[period_sub["PERIOD_END"] == period_sub["LSD_PERIOD_END"]].copy()

    # -----------------------------
    # Program Overview EVMS (one row per program)
    # Also fix “missing SPI_LSD in program table” by falling back to last valid SPI_LSD within the program timeline.
    # -----------------------------
    def _last_valid_ratio(df_period, key, ratio_col):
        d = df_period[df_period["PROGRAM"] == key].sort_values("PERIOD_END")
        return _last_nonnull(d[ratio_col])

    out_prog = []
    for prog, lsd_end in lsd_by_program.items():
        d = period_prog[period_prog["PROGRAM"] == prog].sort_values("PERIOD_END")
        if d.empty:
            continue

        # LSD row
        lsd_row = d[d["PERIOD_END"] == lsd_end].tail(1)
        if lsd_row.empty:
            lsd_row = d.tail(1)

        lsd_row = lsd_row.iloc[0]

        # CTD row = same as LSD row cumulative (as-of LSD)
        # (Because we computed cumulative by period.)
        spi_lsd = lsd_row["SPI_LSD"]
        cpi_lsd = lsd_row["CPI_LSD"]

        # fallback: last non-null ratio across timeline
        if pd.isna(spi_lsd):
            spi_lsd = _last_nonnull(d["SPI_LSD"])
        if pd.isna(cpi_lsd):
            cpi_lsd = _last_nonnull(d["CPI_LSD"])

        # Next period end (calendar)
        pe_sorted = pd.to_datetime(sorted(set(period_ends)))
        try:
            idx = np.where(pe_sorted == pd.to_datetime(lsd_end))[0]
            idx = int(idx[0]) if len(idx) else None
        except Exception:
            idx = None
        next_end = pe_sorted[idx + 1] if (idx is not None and idx + 1 < len(pe_sorted)) else pd.NaT

        # Next period totals
        next_row = d[d["PERIOD_END"] == next_end].tail(1)
        next_bcws = float(next_row["BCWS"].iloc[0]) if len(next_row) else np.nan
        next_etc  = float(next_row["ETC"].iloc[0])  if len(next_row) else np.nan

        out_prog.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_END": pd.to_datetime(lsd_end),
            "NEXT_PERIOD_END": pd.to_datetime(next_end),

            "BCWS_LSD": lsd_row["BCWS"],
            "BCWP_LSD": lsd_row["BCWP"],
            "ACWP_LSD": lsd_row["ACWP"],
            "ETC_LSD":  lsd_row["ETC"],

            "SPI_LSD": spi_lsd,
            "CPI_LSD": cpi_lsd,

            "BCWS_CTD": lsd_row["BCWS_CUM"],
            "BCWP_CTD": lsd_row["BCWP_CUM"],
            "ACWP_CTD": lsd_row["ACWP_CUM"],
            "SPI_CTD":  lsd_row["SPI_CTD"],
            "CPI_CTD":  lsd_row["CPI_CTD"],

            # “Next period” hours
            "NEXT_PERIOD_BCWS_HRS": next_bcws,
            "NEXT_PERIOD_ETC_HRS":  next_etc,
        })

    program_overview_evms = pd.DataFrame(out_prog).sort_values(["PROGRAM"]).reset_index(drop=True)

    # -----------------------------
    # Subteam SPI/CPI table
    # -----------------------------
    subteam_spi_cpi = sub_lsd.copy()
    if not subteam_spi_cpi.empty:
        # fallback ratios if missing at LSD
        def _fallback_ratio(group, col):
            v = group.loc[group["PERIOD_END"] == group["LSD_PERIOD_END"].iloc[0], col]
            v = v.iloc[0] if len(v) else np.nan
            if pd.isna(v):
                v = _last_nonnull(group.sort_values("PERIOD_END")[col])
            return v

        # compute fallback per (PROGRAM, SUB_TEAM)
        gb = period_sub.groupby(["PROGRAM", "SUB_TEAM"], dropna=False, as_index=False)
        fallback_spi = gb.apply(lambda g: _fallback_ratio(g, "SPI_LSD")).rename(columns={None: "SPI_LSD_FIX"})
        fallback_cpi = gb.apply(lambda g: _fallback_ratio(g, "CPI_LSD")).rename(columns={None: "CPI_LSD_FIX"})

        # the apply above can create odd frames depending on pandas; normalize:
        if isinstance(fallback_spi, pd.DataFrame) and "SPI_LSD_FIX" in fallback_spi.columns:
            pass
        else:
            # older pandas sometimes returns a Series
            fallback_spi = gb.apply(lambda g: _fallback_ratio(g, "SPI_LSD")).reset_index(name="SPI_LSD_FIX")
        if isinstance(fallback_cpi, pd.DataFrame) and "CPI_LSD_FIX" in fallback_cpi.columns:
            pass
        else:
            fallback_cpi = gb.apply(lambda g: _fallback_ratio(g, "CPI_LSD")).reset_index(name="CPI_LSD_FIX")

        # Build final
        subteam_spi_cpi = (
            sub_lsd[["PROGRAM","SUB_TEAM","LSD_PERIOD_END","PERIOD_END","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]]
            .rename(columns={"LSD_PERIOD_END":"LAST_STATUS_PERIOD_END"})
            .merge(fallback_spi[["PROGRAM","SUB_TEAM","SPI_LSD_FIX"]], on=["PROGRAM","SUB_TEAM"], how="left")
            .merge(fallback_cpi[["PROGRAM","SUB_TEAM","CPI_LSD_FIX"]], on=["PROGRAM","SUB_TEAM"], how="left")
        )
        subteam_spi_cpi["SPI_LSD"] = subteam_spi_cpi["SPI_LSD"].fillna(subteam_spi_cpi["SPI_LSD_FIX"])
        subteam_spi_cpi["CPI_LSD"] = subteam_spi_cpi["CPI_LSD"].fillna(subteam_spi_cpi["CPI_LSD_FIX"])
        subteam_spi_cpi.drop(columns=["SPI_LSD_FIX","CPI_LSD_FIX","PERIOD_END"], inplace=True)
        subteam_spi_cpi = subteam_spi_cpi.sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
    else:
        subteam_spi_cpi = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","LAST_STATUS_PERIOD_END","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"])

    # -----------------------------
    # BAC/EAC/VAC by PROGRAM + SUB_TEAM
    # - BAC_HRS: sum of BCWS across periods (cumulative at LSD)
    # - EAC_HRS: ACWP_CTD + ETC_LSD  (common EVMS hours interpretation in your earlier labor table)
    # - VAC_HRS: BAC - EAC
    # -----------------------------
    subteam_bac_eac_vac = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"])
    if not sub_lsd.empty:
        tmp = sub_lsd.copy()
        tmp["BAC_HRS"] = tmp["BCWS_CUM"]
        tmp["EAC_HRS"] = tmp["ACWP_CUM"] + tmp["ETC"]
        tmp["VAC_HRS"] = tmp["BAC_HRS"] - tmp["EAC_HRS"]
        subteam_bac_eac_vac = (
            tmp[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]]
            .sort_values(["PROGRAM","SUB_TEAM"])
            .reset_index(drop=True)
        )

    # -----------------------------
    # Program Hours Forecast (program-level)
    # Demand CTD = BCWS_CUM at LSD
    # Actual CTD = ACWP_CUM at LSD
    # Pct variance = (Actual - Demand) / Demand
    # Next period BCWS/ETC hours from next period totals
    # Next period N: placeholder unless you have an "N" column (or can derive from headcount)
    # -----------------------------
    program_hours_forecast = pd.DataFrame(columns=[
        "PROGRAM",
        "LAST_STATUS_PERIOD_END",
        "NEXT_PERIOD_END",
        "DEMAND_HRS_CTD",
        "ACTUAL_HRS_CTD",
        "PCT_VARIANCE_CTD",
        "NEXT_PERIOD_BCWS_HRS",
        "NEXT_PERIOD_ETC_HRS",
        "NEXT_PERIOD_N",
    ])

    if not program_overview_evms.empty:
        phf = program_overview_evms.copy()
        phf["DEMAND_HRS_CTD"] = phf["BCWS_CTD"]
        phf["ACTUAL_HRS_CTD"] = phf["ACWP_CTD"]
        phf["PCT_VARIANCE_CTD"] = _safe_div(phf["ACTUAL_HRS_CTD"] - phf["DEMAND_HRS_CTD"], phf["DEMAND_HRS_CTD"])
        phf["NEXT_PERIOD_N"] = np.nan  # placeholder (you can fill later if you have N logic)
        program_hours_forecast = phf[[
            "PROGRAM","LAST_STATUS_PERIOD_END","NEXT_PERIOD_END",
            "DEMAND_HRS_CTD","ACTUAL_HRS_CTD","PCT_VARIANCE_CTD",
            "NEXT_PERIOD_BCWS_HRS","NEXT_PERIOD_ETC_HRS","NEXT_PERIOD_N"
        ]].sort_values(["PROGRAM"]).reset_index(drop=True)

    # -----------------------------
    # Optional deep debug printout for one program
    # -----------------------------
    if debug_program is not None:
        p = str(debug_program).strip()
        print(f"\n=== DEBUG PROGRAM: {p} ===")
        d_raw = df[df["PROGRAM"] == p].copy()
        print("Raw rows:", len(d_raw))
        print("Date range:", d_raw["DATE"].min(), "to", d_raw["DATE"].max())
        print("\nTop COST_SET:")
        display(d_raw["COST_SET"].value_counts().head(20).to_frame("count"))
        print("\nEVMS_BUCKET counts:")
        display(d_raw["EVMS_BUCKET"].value_counts(dropna=False).to_frame("count"))

        d_per = period_prog[period_prog["PROGRAM"] == p].sort_values("PERIOD_END").copy()
        print("\nLast 15 period totals (program):")
        display(d_per[["PERIOD_END","BCWS","BCWP","ACWP","ETC","SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD"]].tail(15))

        lsd_end = lsd_by_program.get(p, pd.NaT)
        print("\nPicked LSD PERIOD_END:", lsd_end)
        d_lsd_rows = df[(df["PROGRAM"] == p) & (df["PERIOD_END"] == lsd_end)].sort_values("DATE").tail(120)
        print("\nRaw rows inside LSD period (last 120):")
        display(d_lsd_rows[["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]])

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues


# -----------------------------
# Debug helper you can call anytime
# -----------------------------
def debug_evms(cobra_merged_df: pd.DataFrame, year_filter=2026, debug_program="ABRAMS_22", period_ends=None):
    prog, sub_spi, sub_bac, phf, issues = build_evms_tables(
        cobra_merged_df,
        period_ends=period_ends,
        year_filter=year_filter,
        as_of_date=None,
        debug_program=debug_program,
    )
    print("\nISSUES:", issues)
    print("\nprogram_overview_evms shape:", prog.shape)
    print("subteam_spi_cpi shape:", sub_spi.shape)
    print("subteam_bac_eac_vac shape:", sub_bac.shape)
    print("program_hours_forecast shape:", phf.shape)
    return prog, sub_spi, sub_bac, phf, issues


# -----------------------------
# RUN (edit these two lines only)
# -----------------------------
# 1) Put your 2026 calendar period ends here (RECOMMENDED). If you don’t, it will infer from DATEs.
# Example:
# PERIOD_ENDS_2026 = [
#     "2026-01-04","2026-02-01","2026-03-01","2026-04-05","2026-05-03","2026-06-07",
#     "2026-07-05","2026-08-02","2026-09-27","2026-10-04","2026-11-01","2026-12-27"
# ]
# PERIOD_ENDS_2026 = pd.to_datetime(PERIOD_ENDS_2026).tolist()

PERIOD_ENDS_2026 = None  # <-- set this to your real list for best accuracy

# 2) Build outputs
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    period_ends=PERIOD_ENDS_2026,
    year_filter=2026,
    as_of_date=None,
    debug_program=None,   # set to "ABRAMS_22" for deep prints
)

print("ISSUES:")
for i in issues:
    print("-", i)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))