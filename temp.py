# ============================================
# CELL 1 — EVMS PIPELINE (end-to-end, robust)
# ============================================
import numpy as np
import pandas as pd

def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().upper().replace(" ", "_") for c in df.columns]

    # normalize expected names
    rename = {}
    if "COST-SET" in df.columns: rename["COST-SET"] = "COST_SET"
    if "COSTSET" in df.columns:  rename["COSTSET"]  = "COST_SET"
    if "SUBTEAM" in df.columns:  rename["SUBTEAM"]  = "SUB_TEAM"
    if "SUB_TEAM" not in df.columns and "SUB_TEAM_" in df.columns: rename["SUB_TEAM_"] = "SUB_TEAM"
    df = df.rename(columns=rename)

    # required columns
    req = ["PROGRAM", "DATE", "COST_SET", "HOURS"]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    # if SUB_TEAM doesn't exist, create a single bucket so program-level still works
    if "SUB_TEAM" not in df.columns:
        df["SUB_TEAM"] = "ALL"
    df["SUB_TEAM"] = df["SUB_TEAM"].astype(str).str.strip()

    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")
    df = df.dropna(subset=["DATE", "HOURS"])
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip()

    return df


def _map_cost_sets_to_evms_bucket(cost_set_series: pd.Series) -> pd.Series:
    """
    Map your COBRA COST_SET values onto the EVMS buckets we need:
      BCWS (planned/budget), BCWP (progress/earned), ACWP (actual), ETC (estimate-to-complete)

    IMPORTANT: This is intentionally tolerant of variants seen in your screenshots:
      BUDGET, BCWS
      PROGRESS, BCWP
      ACWP_HRS, ACWP, ACWP HRS
      ETC
    """
    s = cost_set_series.astype(str).str.upper().str.strip()

    # normalize separators
    s = s.str.replace(" ", "_", regex=False).str.replace("-", "_", regex=False)

    # explicit mappings first
    mapping = {
        "BUDGET": "BCWS",
        "BCWS": "BCWS",

        "PROGRESS": "BCWP",
        "BCWP": "BCWP",

        "ACWP": "ACWP",
        "ACWP_HRS": "ACWP",
        "ACWP_HOUR": "ACWP",
        "ACWP_HOURS": "ACWP",

        "ETC": "ETC",
        "ETC_HRS": "ETC",
        "ETC_HOURS": "ETC",
    }

    out = s.map(mapping)

    # if still missing, do a light heuristic match
    # (kept conservative to avoid mis-bucketing)
    out = out.fillna(
        np.where(s.str.contains("BCWS|BUDGET", regex=True), "BCWS",
        np.where(s.str.contains("BCWP|PROGRESS", regex=True), "BCWP",
        np.where(s.str.contains("ACWP", regex=True), "ACWP",
        np.where(s.str.contains("ETC", regex=True), "ETC", np.nan))))
    )

    return pd.Series(out, index=cost_set_series.index, name="EVMS_BUCKET")


def build_445_period_ends(first_period_end: str, years: int = 5) -> pd.DatetimeIndex:
    """
    Build a 4-4-5 calendar of PERIOD_END Sundays starting at first_period_end (inclusive).
    Many GDLS-style calendars behave like 4-4-5 with occasional 53rd week handled by the pattern.

    Provide first_period_end as an actual known period end from your calendar.
    From your screenshot, 2026 looks like it starts at 2026-01-04 (Sunday).
    """
    first = pd.Timestamp(first_period_end).normalize()

    # 4-4-5 weeks per quarter => 13 weeks per quarter => 52 weeks per year
    pattern_weeks = [4, 4, 5] * 4  # 12 periods
    ends = [first]
    cur = first
    for _ in range(years):
        for w in pattern_weeks:
            cur = cur + pd.Timedelta(days=7*w)
            ends.append(cur)
    # ends includes the "start" and then subsequent period ends; drop duplicates
    return pd.DatetimeIndex(sorted(set([d.normalize() for d in ends])))


def assign_period_end(dates: pd.Series, period_ends: pd.DatetimeIndex) -> pd.Series:
    """
    Assign each DATE to the first PERIOD_END >= DATE (ceiling to period end).
    If DATE is after the last period end, returns NaT.
    """
    pe = pd.DatetimeIndex(pd.to_datetime(period_ends)).sort_values()
    d = pd.to_datetime(dates).values.astype("datetime64[ns]")
    pe_vals = pe.values.astype("datetime64[ns]")

    idx = np.searchsorted(pe_vals, d, side="left")
    out = np.where(idx < len(pe_vals), pe_vals[idx], np.datetime64("NaT"))
    return pd.to_datetime(out)


def _safe_div(a: pd.Series, b: pd.Series) -> pd.Series:
    b2 = b.replace(0, np.nan)
    return a / b2


def _compute_period_table(df_evms: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Make a period-level pivot with columns BCWS, BCWP, ACWP, ETC summed hours.
    keys must include PROGRAM and may include SUB_TEAM.
    """
    gcols = keys + ["PERIOD_END", "EVMS_BUCKET"]
    period = (
        df_evms
        .groupby(gcols, as_index=False)["HOURS"].sum()
        .pivot_table(index=keys + ["PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum")
        .fillna(0.0)
        .reset_index()
    )
    # ensure columns exist
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in period.columns:
            period[c] = 0.0

    # sort
    period = period.sort_values(keys + ["PERIOD_END"]).reset_index(drop=True)
    return period


def _add_ctd_and_lsd(period: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Adds CTD columns and LSD columns using a *data-driven* LSD period selection:
      LSD period = latest PERIOD_END where (BCWP>0 or ACWP>0) for that key group.
    This fixes your main issue where you were selecting the final calendar close
    even though only BCWS exists there (future budget with no actual/progress),
    which forces SPI_LSD / CPI_LSD to 0 or NaN.
    """
    period = period.copy()

    # CTD cumulative
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        period[f"{c}_CTD"] = period.groupby(keys)[c].cumsum()

    # identify LSD period end per group (must have actual OR progress)
    has_actual_or_progress = (period["ACWP"] > 0) | (period["BCWP"] > 0)

    # if a group has no ACWP/BCWP anywhere, LSD is NaT (we'll keep NaNs)
    lsd_end = (
        period.loc[has_actual_or_progress]
        .groupby(keys)["PERIOD_END"]
        .max()
        .rename("LAST_STATUS_PERIOD_END")
        .reset_index()
    )
    period = period.merge(lsd_end, on=keys, how="left")

    # also capture prior period end to compute LSD deltas cleanly
    period["IS_LSD"] = period["PERIOD_END"].eq(period["LAST_STATUS_PERIOD_END"])

    # compute LSD deltas: value at LSD period minus previous period value
    # (if there is no previous period, LSD uses the LSD period itself)
    def _lsd_delta(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("PERIOD_END").copy()
        for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
            prev = group[c].shift(1)
            group[f"{c}_LSD"] = np.where(group["IS_LSD"], group[c] - prev.fillna(0.0), np.nan)
        return group

    period = period.groupby(keys, group_keys=False).apply(_lsd_delta)

    # compute SPI/CPI CTD and LSD on the *rows that matter* (LSD row),
    # then we’ll collapse to one row per group.
    period["SPI_CTD"] = _safe_div(period["BCWP_CTD"], period["BCWS_CTD"])
    period["CPI_CTD"] = _safe_div(period["BCWP_CTD"], period["ACWP_CTD"])

    period["SPI_LSD"] = _safe_div(period["BCWP_LSD"], period["BCWS_LSD"])
    period["CPI_LSD"] = _safe_div(period["BCWP_LSD"], period["ACWP_LSD"])

    return period


def _collapse_to_one_row_at_lsd(period: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Return one row per key group containing the CTD values at LSD and the LSD metrics.
    """
    # pick the LSD row per group
    lsd_rows = period.loc[period["IS_LSD"]].copy()

    # if some groups have no LSD row (never had ACWP/BCWP), keep a placeholder row
    if lsd_rows.empty:
        # return empty with expected columns
        cols = keys + ["LAST_STATUS_PERIOD_END"]
        return pd.DataFrame(columns=cols)

    keep_cols = (
        keys
        + ["LAST_STATUS_PERIOD_END"]
        + ["BCWS_CTD", "BCWP_CTD", "ACWP_CTD", "ETC_CTD"]
        + ["BCWS_LSD", "BCWP_LSD", "ACWP_LSD", "ETC_LSD"]
        + ["SPI_CTD", "CPI_CTD", "SPI_LSD", "CPI_LSD"]
    )
    # some may not exist if weird input, so intersect
    keep_cols = [c for c in keep_cols if c in lsd_rows.columns]
    out = lsd_rows[keep_cols].drop_duplicates(subset=keys).reset_index(drop=True)
    return out


def _next_period_metrics(period: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """
    Adds NEXT_PERIOD_END, NEXT_PERIOD_BCWS_HRS, NEXT_PERIOD_ETC_HRS for each group.
    Next period is the period immediately after the LSD period *in that group's period table*.
    """
    period = period.sort_values(keys + ["PERIOD_END"]).copy()

    # get next period end for each row
    period["NEXT_PERIOD_END"] = period.groupby(keys)["PERIOD_END"].shift(-1)
    period["NEXT_BCWS"] = period.groupby(keys)["BCWS"].shift(-1)
    period["NEXT_ETC"] = period.groupby(keys)["ETC"].shift(-1)

    # only keep these on LSD rows, then collapse
    lsd = period.loc[period["IS_LSD"], keys + ["LAST_STATUS_PERIOD_END", "NEXT_PERIOD_END", "NEXT_BCWS", "NEXT_ETC"]].copy()
    lsd = lsd.rename(columns={
        "NEXT_BCWS": "NEXT_PERIOD_BCWS_HRS",
        "NEXT_ETC": "NEXT_PERIOD_ETC_HRS",
    })
    return lsd.reset_index(drop=True)


def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    first_period_end: str = "2026-01-04",
    years: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      1) program_overview_evms
      2) subteam_spi_cpi
      3) subteam_bac_eac_vac
      4) program_hours_forecast
    """
    df = _standardize_cols(cobra_merged_df)

    # map cost sets -> EVMS bucket
    df["EVMS_BUCKET"] = _map_cost_sets_to_evms_bucket(df["COST_SET"])

    # keep only rows we can use
    df = df.dropna(subset=["EVMS_BUCKET"]).copy()

    # build period ends across the span of your data
    min_date = df["DATE"].min().normalize()
    max_date = df["DATE"].max().normalize()

    # if you don’t pass years, build enough to cover your data range (plus buffer)
    if years is None:
        span_years = max(3, int(np.ceil((max_date - min_date).days / 365.25)) + 2)
    else:
        span_years = years

    period_ends = build_445_period_ends(first_period_end=first_period_end, years=span_years)

    # ensure the period_ends cover the dataset
    # if data starts before the first_period_end, prepend a few periods backwards
    if min_date < period_ends.min():
        # back up by 12 periods at a time until covered
        pe = list(period_ends)
        cur = period_ends.min()
        # approximate 12 periods ~ 364 days; step back in chunks
        while min_date < cur:
            cur = cur - pd.Timedelta(days=364)
            pe.extend(build_445_period_ends(first_period_end=str(cur.date()), years=1).tolist())
        period_ends = pd.DatetimeIndex(sorted(set(pd.to_datetime(pe))))

    df["PERIOD_END"] = assign_period_end(df["DATE"], period_ends)
    df = df.dropna(subset=["PERIOD_END"]).copy()

    # --------------------------
    # SUBTEAM-LEVEL TABLES
    # --------------------------
    sub_period = _compute_period_table(df, keys=["PROGRAM", "SUB_TEAM"])
    sub_period = _add_ctd_and_lsd(sub_period, keys=["PROGRAM", "SUB_TEAM"])
    sub_one = _collapse_to_one_row_at_lsd(sub_period, keys=["PROGRAM", "SUB_TEAM"])

    subteam_spi_cpi = sub_one[[
        "PROGRAM", "SUB_TEAM", "LAST_STATUS_PERIOD_END",
        "SPI_LSD", "SPI_CTD", "CPI_LSD", "CPI_CTD"
    ]].copy()

    # BAC/EAC/VAC (hours)
    # BAC = total planned budget (BCWS) across all periods (not CTD at LSD if budget extends beyond)
    bac = (
        sub_period
        .groupby(["PROGRAM", "SUB_TEAM"], as_index=False)["BCWS"].sum()
        .rename(columns={"BCWS": "BAC_HRS"})
    )
    subteam_bac_eac_vac = sub_one.merge(bac, on=["PROGRAM", "SUB_TEAM"], how="left")
    subteam_bac_eac_vac["EAC_HRS"] = subteam_bac_eac_vac["ACWP_CTD"] + subteam_bac_eac_vac["ETC_CTD"]
    subteam_bac_eac_vac["VAC_HRS"] = subteam_bac_eac_vac["BAC_HRS"] - subteam_bac_eac_vac["EAC_HRS"]
    subteam_bac_eac_vac = subteam_bac_eac_vac[[
        "PROGRAM", "SUB_TEAM", "BAC_HRS", "EAC_HRS", "VAC_HRS"
    ]].copy()

    # --------------------------
    # PROGRAM-LEVEL OVERVIEW
    # (sum across subteams first, then compute the same logic)
    # --------------------------
    prog_period = (
        sub_period
        .groupby(["PROGRAM", "PERIOD_END"], as_index=False)[["BCWS", "BCWP", "ACWP", "ETC"]].sum()
    )
    # ensure expected columns
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in prog_period.columns:
            prog_period[c] = 0.0

    prog_period = prog_period.sort_values(["PROGRAM", "PERIOD_END"]).reset_index(drop=True)
    prog_period = _add_ctd_and_lsd(prog_period, keys=["PROGRAM"])
    prog_one = _collapse_to_one_row_at_lsd(prog_period, keys=["PROGRAM"])

    program_overview_evms = prog_one[[
        "PROGRAM", "LAST_STATUS_PERIOD_END",
        "BCWS_CTD", "BCWP_CTD", "ACWP_CTD", "ETC_CTD",
        "SPI_CTD", "CPI_CTD",
        "BCWS_LSD", "BCWP_LSD", "ACWP_LSD", "ETC_LSD",
        "SPI_LSD", "CPI_LSD",
    ]].copy()

    # --------------------------
    # PROGRAM HOURS FORECAST TABLE
    # demand vs actual (% var) + next period BCWS/ETC
    # Demand Hours (CTD) = BCWS_CTD at LSD
    # Actual Hours (CTD) = ACWP_CTD at LSD
    # Next month BCWS/ETC = the period immediately after LSD
    # --------------------------
    prog_next = _next_period_metrics(prog_period, keys=["PROGRAM"])

    program_hours_forecast = prog_one.merge(prog_next, on=["PROGRAM", "LAST_STATUS_PERIOD_END"], how="left")
    program_hours_forecast = program_hours_forecast.rename(columns={
        "BCWS_CTD": "DEMAND_HRS_CTD",
        "ACWP_CTD": "ACTUAL_HRS_CTD",
    })
    program_hours_forecast["PCT_VARIANCE_CTD"] = _safe_div(
        (program_hours_forecast["ACTUAL_HRS_CTD"] - program_hours_forecast["DEMAND_HRS_CTD"]),
        program_hours_forecast["DEMAND_HRS_CTD"]
    )
    program_hours_forecast = program_hours_forecast[[
        "PROGRAM", "LAST_STATUS_PERIOD_END",
        "NEXT_PERIOD_END",
        "DEMAND_HRS_CTD", "ACTUAL_HRS_CTD", "PCT_VARIANCE_CTD",
        "NEXT_PERIOD_BCWS_HRS", "NEXT_PERIOD_ETC_HRS",
    ]].copy()

    # nice-to-have: consistent sorting
    program_overview_evms = program_overview_evms.sort_values("PROGRAM").reset_index(drop=True)
    subteam_spi_cpi = subteam_spi_cpi.sort_values(["PROGRAM", "SUB_TEAM"]).reset_index(drop=True)
    subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["PROGRAM", "SUB_TEAM"]).reset_index(drop=True)
    program_hours_forecast = program_hours_forecast.sort_values("PROGRAM").reset_index(drop=True)

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast


# ---- RUN IT ----
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast = build_evms_tables(
    cobra_merged_df,
    first_period_end="2026-01-04",  # matches your screenshot; adjust if needed
    years=None,                    # auto-covers your full data range
)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))