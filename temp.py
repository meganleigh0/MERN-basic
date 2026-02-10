# ============================
# EVMS PIPELINE (ONE CELL)
# - Fixes column name variants (COST-SET vs COST_SET, etc.)
# - Buckets: BCWS/BCWP/ACWP/ETC from COST_SET
# - Period end = last Thursday of EACH month
# - "Last Status Date" = last Thursday of PREVIOUS month relative to today
# - Produces:
#   1) program_overview_evms
#   2) subteam_spi_cpi
#   3) subteam_bac_eac_vac
#   4) program_hours_forecast
#   5) issues (list of strings)
# ============================

import numpy as np
import pandas as pd

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    out = n / d
    out = out.where((d != 0) & (~d.isna()), np.nan)
    return out

def _last_thursday_of_month(year: int, month: int) -> pd.Timestamp:
    # last day of month -> walk back to Thursday (weekday=3)
    last_day = (pd.Timestamp(year=year, month=month, day=1) + pd.offsets.MonthEnd(0)).normalize()
    offset = (last_day.weekday() - 3) % 7
    return (last_day - pd.Timedelta(days=offset)).normalize()

def _last_thursday_previous_month(today=None) -> pd.Timestamp:
    today = pd.Timestamp.today().normalize() if today is None else pd.Timestamp(today).normalize()
    prev_month_end = (today.replace(day=1) - pd.Timedelta(days=1)).normalize()
    return _last_thursday_of_month(prev_month_end.year, prev_month_end.month)

def _make_period_ends(min_date: pd.Timestamp, max_date: pd.Timestamp) -> pd.DatetimeIndex:
    # Build last-Thursday period ends for each month spanning [min_date, max_date]
    if pd.isna(min_date) or pd.isna(max_date):
        return pd.DatetimeIndex([])
    start = pd.Timestamp(min_date.year, min_date.month, 1)
    end   = pd.Timestamp(max_date.year, max_date.month, 1)
    months = pd.date_range(start=start, end=end, freq="MS")
    pes = [_last_thursday_of_month(d.year, d.month) for d in months]
    return pd.DatetimeIndex(sorted(pd.unique(pes)))

def _assign_period_end(dates: pd.Series, period_ends: pd.DatetimeIndex) -> pd.Series:
    # PERIOD_END = first period_end >= DATE (safe searchsorted)
    if len(period_ends) == 0:
        return pd.Series(pd.NaT, index=dates.index)

    d = pd.to_datetime(dates, errors="coerce").dt.normalize()
    pe = pd.DatetimeIndex(pd.to_datetime(period_ends, errors="coerce")).sort_values().unique()

    # Use numpy datetime64 arrays (avoid ".values" attr errors on DatetimeArray)
    pe_arr = np.asarray(pe, dtype="datetime64[ns]")
    d_arr  = np.asarray(pd.DatetimeIndex(d), dtype="datetime64[ns]")

    idx = np.searchsorted(pe_arr, d_arr, side="left")
    out = np.full(len(d_arr), np.datetime64("NaT"), dtype="datetime64[ns]")
    ok = idx < len(pe_arr)
    out[ok] = pe_arr[idx[ok]]
    return pd.to_datetime(out)

def build_evs_tables(
    cobra_merged_df: pd.DataFrame,
    year_filter: int = None,
    as_of_today=None,
    debug_program: str = None,
):
    issues = []

    # ----------------------------
    # 0) Standardize columns
    # ----------------------------
    df0 = cobra_merged_df.copy()

    # Normalize column names to uppercase + underscores
    df0.columns = [str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in df0.columns]

    # Common aliases
    rename_map = {}
    if "COST_SET" not in df0.columns and "COSTSET" in df0.columns:
        rename_map["COSTSET"] = "COST_SET"
    if "SUBTEAM" in df0.columns and "SUB_TEAM" not in df0.columns:
        rename_map["SUBTEAM"] = "SUB_TEAM"
    if "PROGRAM_NAME" in df0.columns and "PROGRAM" not in df0.columns:
        rename_map["PROGRAM_NAME"] = "PROGRAM"
    if rename_map:
        df0 = df0.rename(columns=rename_map)

    required = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required if c not in df0.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df0.columns)}")

    # Coerce types
    df0["PROGRAM"]  = df0["PROGRAM"].astype(str).str.strip()
    df0["SUB_TEAM"] = df0["SUB_TEAM"].astype(str).str.strip()
    df0["COST_SET"] = df0["COST_SET"].astype(str).str.strip().str.upper()

    df0["DATE"]  = pd.to_datetime(df0["DATE"], errors="coerce").dt.normalize()
    df0["HOURS"] = pd.to_numeric(df0["HOURS"], errors="coerce")

    # Drop null essentials
    before = len(df0)
    df0 = df0.dropna(subset=["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"])
    dropped = before - len(df0)
    if dropped:
        issues.append(f"Dropped {dropped:,} rows with nulls in PROGRAM/SUB_TEAM/COST_SET/DATE/HOURS.")

    # ----------------------------
    # 1) As-of date = last Thursday of previous month
    # ----------------------------
    as_of_date = _last_thursday_previous_month(as_of_today)
    issues.append(f"As-of date (last Thursday of previous month): {as_of_date.date()}")

    # Optional year filter (strongly recommended, based on your screenshots)
    if year_filter is not None:
        df0 = df0[df0["DATE"].dt.year == int(year_filter)].copy()
        issues.append(f"Filtered to year={year_filter}. Rows now: {len(df0):,}")

    # Remove anything after as_of_date (prevents "future" rows messing NEXT period logic)
    df0 = df0[df0["DATE"] <= as_of_date].copy()

    if df0.empty:
        issues.append("After filters, df is empty. Check year_filter/as_of_date relative to your data.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    # ----------------------------
    # 2) Map COST_SET -> EVMS_BUCKET (exact + conservative contains)
    #    Your data has values like: BCWS, ACWP, BCWP, ETC (also seen: BUDGET/PROGRESS/ACWP_HRS)
    # ----------------------------
    s = df0["COST_SET"]

    # Exact mapping first
    exact_map = {
        "BCWS": "BCWS",
        "BCWP": "BCWP",
        "ACWP": "ACWP",
        "ETC":  "ETC",
    }
    bucket = s.map(exact_map)

    # Conservative fallback (only if exact missing)
    m = bucket.isna()
    if m.any():
        ss = s[m].astype(str)
        bucket.loc[m] = np.select(
            [
                ss.str.contains(r"\bBCWS\b|BUDGET", regex=True),
                ss.str.contains(r"\bBCWP\b|PROGRESS", regex=True),
                ss.str.contains(r"\bACWP\b", regex=True),
                ss.str.contains(r"\bETC\b", regex=True),
            ],
            ["BCWS", "BCWP", "ACWP", "ETC"],
            default=np.nan
        )

    df0["EVMS_BUCKET"] = bucket

    unmapped_pct = df0["EVMS_BUCKET"].isna().mean()
    if unmapped_pct > 0:
        issues.append(f"Unmapped EVMS_BUCKET rows: {unmapped_pct:.2%}. Those rows will be dropped.")
    df = df0.dropna(subset=["EVMS_BUCKET"]).copy()

    # ----------------------------
    # 3) Period ends (last Thursday per month)
    # ----------------------------
    min_d = df["DATE"].min()
    max_d = df["DATE"].max()
    period_ends = _make_period_ends(min_d, max_d)

    # Ensure as_of_date itself is a valid period end; if not, we still keep it as the LSD anchor
    if as_of_date not in period_ends:
        # add it (this handles months where the "last Thursday" is computed differently in your corporate calendar)
        period_ends = period_ends.append(pd.DatetimeIndex([as_of_date])).sort_values().unique()
        issues.append("As-of date was not in inferred period_ends; added it to period_ends.")

    df["PERIOD_END"] = _assign_period_end(df["DATE"], period_ends)

    nat_pct = df["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"PERIOD_END NaT rate: {nat_pct:.2%}. Dropping NaT PERIOD_END rows.")
    df = df.dropna(subset=["PERIOD_END"]).copy()

    # ----------------------------
    # 4) Period totals (program + subteam)
    # ----------------------------
    # Program-period-bucket totals
    g_prog = (
        df.groupby(["PROGRAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .unstack("EVMS_BUCKET")
          .reset_index()
    )
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in g_prog.columns:
            g_prog[c] = 0.0
    g_prog[["BCWS", "BCWP", "ACWP", "ETC"]] = g_prog[["BCWS", "BCWP", "ACWP", "ETC"]].fillna(0.0)

    # Subteam-period-bucket totals
    g_sub = (
        df.groupby(["PROGRAM", "SUB_TEAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .unstack("EVMS_BUCKET")
          .reset_index()
    )
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in g_sub.columns:
            g_sub[c] = 0.0
    g_sub[["BCWS", "BCWP", "ACWP", "ETC"]] = g_sub[["BCWS", "BCWP", "ACWP", "ETC"]].fillna(0.0)

    # ----------------------------
    # 5) Add CTD (cumulative-to-date) safely
    # ----------------------------
    def add_ctd(frame: pd.DataFrame, keys):
        out = frame.sort_values(keys + ["PERIOD_END"]).copy()
        for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
            # IMPORTANT: groupby on a Series (not on a DataFrame) to avoid "not 1-dimensional" errors
            out[f"{col}_CTD"] = out.groupby(keys, dropna=False)[col].cumsum()
            out.rename(columns={col: f"{col}_LSD"}, inplace=True)
        # Ratios
        out["SPI_LSD"] = _safe_div(out["BCWP_LSD"], out["BCWS_LSD"])
        out["CPI_LSD"] = _safe_div(out["BCWP_LSD"], out["ACWP_LSD"])
        out["SPI_CTD"] = _safe_div(out["BCWP_CTD"], out["BCWS_CTD"])
        out["CPI_CTD"] = _safe_div(out["BCWP_CTD"], out["ACWP_CTD"])
        return out

    period_prog = add_ctd(g_prog, keys=["PROGRAM"])
    period_sub  = add_ctd(g_sub,  keys=["PROGRAM", "SUB_TEAM"])

    # ----------------------------
    # 6) Pick LSD (last status period end) + Next period
    # LSD = as_of_date
    # Next = next period end after LSD (if present in data)
    # ----------------------------
    lsd_end = pd.Timestamp(as_of_date).normalize()

    # next period end based on calendar list (even if no data)
    pe_sorted = pd.DatetimeIndex(period_ends).sort_values().unique()
    idx_lsd = pe_sorted.searchsorted(np.datetime64(lsd_end), side="right")
    next_end = pe_sorted[idx_lsd] if idx_lsd < len(pe_sorted) else pd.NaT

    # Program overview: one row per program at LSD
    prog_lsd = period_prog[period_prog["PERIOD_END"] == lsd_end].copy()
    if prog_lsd.empty:
        issues.append(f"No program totals found at LSD period_end={lsd_end.date()}. Your data may not include that period end.")
        # Still return frames (empty overview) so you can inspect
        program_overview_evms = pd.DataFrame()
    else:
        # Next-period values from period_prog where PERIOD_END == next_end
        prog_next = period_prog[period_prog["PERIOD_END"] == next_end][
            ["PROGRAM", "PERIOD_END", "BCWS_LSD", "ETC_LSD"]
        ].rename(columns={
            "PERIOD_END": "NEXT_PERIOD_END",
            "BCWS_LSD": "NEXT_PERIOD_BCWS_HRS",
            "ETC_LSD":  "NEXT_PERIOD_ETC_HRS",
        })

        program_overview_evms = (
            prog_lsd.merge(prog_next, on="PROGRAM", how="left")
                  .assign(
                      LAST_STATUS_PERIOD_END=pd.to_datetime(lsd_end),
                      NEXT_PERIOD_END=pd.to_datetime(next_end) if pd.notna(next_end) else pd.NaT
                  )
        )

        # Make the column order feel like your table
        keep_cols = [
            "PROGRAM",
            "LAST_STATUS_PERIOD_END",
            "NEXT_PERIOD_END",
            "BCWS_LSD", "BCWP_LSD", "ACWP_LSD", "ETC_LSD",
            "SPI_LSD", "CPI_LSD",
            "BCWS_CTD", "BCWP_CTD", "ACWP_CTD", "ETC_CTD",
            "SPI_CTD", "CPI_CTD",
            "NEXT_PERIOD_BCWS_HRS", "NEXT_PERIOD_ETC_HRS",
        ]
        for c in keep_cols:
            if c not in program_overview_evms.columns:
                program_overview_evms[c] = np.nan
        program_overview_evms = program_overview_evms[keep_cols].sort_values("PROGRAM")

    # Subteam SPI/CPI table at LSD
    subteam_spi_cpi = period_sub[period_sub["PERIOD_END"] == lsd_end].copy()
    if not subteam_spi_cpi.empty:
        subteam_spi_cpi = subteam_spi_cpi.assign(LAST_STATUS_PERIOD_END=pd.to_datetime(lsd_end))
        subteam_spi_cpi = subteam_spi_cpi[[
            "PROGRAM", "SUB_TEAM", "LAST_STATUS_PERIOD_END",
            "SPI_LSD", "SPI_CTD", "CPI_LSD", "CPI_CTD",
            "BCWS_LSD", "BCWP_LSD", "ACWP_LSD", "ETC_LSD",
            "BCWS_CTD", "BCWP_CTD", "ACWP_CTD", "ETC_CTD",
        ]].sort_values(["PROGRAM", "SUB_TEAM"])
    else:
        issues.append(f"No subteam totals found at LSD period_end={lsd_end.date()}.")

    # BAC/EAC/VAC at subteam level
    # BAC_HRS = max BCWS_CTD across all periods (planned at completion)
    # EAC_HRS = ACWP_CTD + ETC_CTD at LSD
    # VAC_HRS = BAC_HRS - EAC_HRS
    if not period_sub.empty:
        bac = (
            period_sub.groupby(["PROGRAM", "SUB_TEAM"], dropna=False)["BCWS_CTD"]
                     .max()
                     .reset_index()
                     .rename(columns={"BCWS_CTD": "BAC_HRS"})
        )
        lsd_ctd = period_sub[period_sub["PERIOD_END"] == lsd_end][
            ["PROGRAM", "SUB_TEAM", "ACWP_CTD", "ETC_CTD"]
        ].copy()
        lsd_ctd["EAC_HRS"] = lsd_ctd["ACWP_CTD"] + lsd_ctd["ETC_CTD"]
        subteam_bac_eac_vac = bac.merge(lsd_ctd[["PROGRAM", "SUB_TEAM", "EAC_HRS"]], on=["PROGRAM", "SUB_TEAM"], how="left")
        subteam_bac_eac_vac["VAC_HRS"] = subteam_bac_eac_vac["BAC_HRS"] - subteam_bac_eac_vac["EAC_HRS"]
        subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["PROGRAM", "SUB_TEAM"])
    else:
        subteam_bac_eac_vac = pd.DataFrame()

    # Program hours forecast (simple, consistent, PowerBI-friendly)
    # demand_ctd = BCWS_CTD, actual_ctd = ACWP_CTD, pct_var = (actual-demand)/demand
    if not period_prog.empty:
        lsd_prog = period_prog[period_prog["PERIOD_END"] == lsd_end].copy()
        prog_next2 = period_prog[period_prog["PERIOD_END"] == next_end][
            ["PROGRAM", "BCWS_LSD", "ETC_LSD"]
        ].rename(columns={
            "BCWS_LSD": "NEXT_PERIOD_BCWS_HRS",
            "ETC_LSD":  "NEXT_PERIOD_ETC_HRS",
        })
        program_hours_forecast = lsd_prog.merge(prog_next2, on="PROGRAM", how="left")
        program_hours_forecast["DEMAND_HRS_CTD"] = program_hours_forecast["BCWS_CTD"]
        program_hours_forecast["ACTUAL_HRS_CTD"] = program_hours_forecast["ACWP_CTD"]
        program_hours_forecast["PCT_VARIANCE_CTD"] = _safe_div(
            program_hours_forecast["ACTUAL_HRS_CTD"] - program_hours_forecast["DEMAND_HRS_CTD"],
            program_hours_forecast["DEMAND_HRS_CTD"]
        )
        program_hours_forecast = program_hours_forecast.assign(LAST_STATUS_PERIOD_END=pd.to_datetime(lsd_end), NEXT_PERIOD_END=pd.to_datetime(next_end) if pd.notna(next_end) else pd.NaT)
        program_hours_forecast = program_hours_forecast[[
            "PROGRAM",
            "LAST_STATUS_PERIOD_END",
            "NEXT_PERIOD_END",
            "DEMAND_HRS_CTD",
            "ACTUAL_HRS_CTD",
            "PCT_VARIANCE_CTD",
            "NEXT_PERIOD_BCWS_HRS",
            "NEXT_PERIOD_ETC_HRS",
            "SPI_CTD",
            "CPI_CTD",
        ]].sort_values("PROGRAM")
    else:
        program_hours_forecast = pd.DataFrame()

    # ----------------------------
    # 7) Optional deep debug print for one program
    # ----------------------------
    if debug_program is not None:
        p = str(debug_program).strip()
        print(f"\n===== DEBUG: {p} =====")
        dpp = df[df["PROGRAM"] == p].copy()
        print("Rows:", len(dpp))
        print("DATE range:", dpp["DATE"].min(), "to", dpp["DATE"].max())
        print("\nTop COST_SET:")
        display(dpp["COST_SET"].value_counts().head(20).to_frame("count"))
        print("\nEVMS_BUCKET counts:")
        display(dpp["EVMS_BUCKET"].value_counts().to_frame("count"))
        print("\nLast 15 periods (program totals):")
        display(period_prog[period_prog["PROGRAM"] == p].sort_values("PERIOD_END").tail(15))
        print("\nRaw last 80 EVMS-like rows:")
        display(dpp.sort_values("DATE").tail(80)[["DATE", "COST_SET", "EVMS_BUCKET", "HOURS", "SUB_TEAM"]])

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues


# ============================
# RUN IT (edit year_filter if needed)
# ============================
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evs_tables(
    cobra_merged_df=cobra_merged_df,   # <-- must exist already
    year_filter=2026,                  # <-- set to None if you want all years (not recommended)
    as_of_today=None,                  # <-- default uses today's date
    debug_program=None                 # <-- set e.g. "ABRAMS_22" to print deep debug
)

print("ISSUES:")
for x in issues:
    print("-", x)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))