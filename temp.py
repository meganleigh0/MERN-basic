import pandas as pd
import numpy as np

# ============================================================
# PIPELINE: EVMS tables from cobra_merged_df
# - Correct COST-SET mapping (Budget/Progress/ACWP_HRS/etc)
# - Last Status Period (LSD) based on GDLS Accounting Period Close calendar dates
#
# Expected columns (case-insensitive):
#   PROGRAM, SUB_TEAM, COST-SET (or COST_SET), DATE, HOURS
# ============================================================

# ----------------------------
# 1) Accounting period close calendar (EDIT IF NEEDED)
# ----------------------------
# From your 2026 calendar image, the "Accounting Period Closing" dates are the black ovals.
# NOTE: Some months show two close dates (ex: Nov has 1 and 29). Keep them all.
ACCOUNTING_CLOSE_DATES_2026 = pd.to_datetime([
    "2026-01-04",
    "2026-02-01",
    "2026-03-01",
    "2026-04-05",
    "2026-05-03",
    "2026-06-07",
    "2026-07-05",
    "2026-08-02",
    # September close date is hard to see in the photo crop; set it explicitly here if different:
    "2026-09-27",
    "2026-10-04",
    "2026-11-01",
    "2026-11-29",
    "2026-12-27",
])

# Optional (not used for LSD): Fiscal year end close date on the calendar is circled in purple
FISCAL_YEAR_END_CLOSE_2026 = pd.to_datetime("2026-12-31")


# ----------------------------
# 2) COST-SET mapping (EDIT IF NEEDED)
# ----------------------------
# Your value_counts shows: Budget, ACWP_HRS, Progress, BCWS, ACWP, ETC, BCWP, EAC, Actuals
# Typical hours-based EVMS buckets:
#   - BCWS (Planned/Budgeted work scheduled)
#   - BCWP (Earned/Progress)
#   - ACWP (Actuals)
#   - ETC  (Estimate to Complete, remaining)
#
# IMPORTANT:
# - Treat "Budget" as BCWS-hours (planned/budget). Keep BCWS too.
# - Treat "Progress" as BCWP-hours (earned). Keep BCWP too.
# - Treat "ACWP_HRS", "ACWP", "Actuals" as ACWP-hours.
# - Keep ETC as ETC-hours.
COSTSET_TO_BUCKET = {
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    "ACWP_HRS": "ACWP",
    "ACWP": "ACWP",
    "ACTUALS": "ACWP",

    "ETC": "ETC",

    # Not needed for SPI/CPI math but kept if you want it later:
    "EAC": "EAC",
}


# ============================================================
# Helpers
# ============================================================
def _normalize_cobra(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in out.columns]

    # Normalize key column names
    if "COSTSET" in out.columns and "COST_SET" not in out.columns:
        out = out.rename(columns={"COSTSET": "COST_SET"})

    required = {"PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"}
    missing = required - set(out.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out["PROGRAM"] = out["PROGRAM"].astype(str).str.strip()
    out["SUB_TEAM"] = out["SUB_TEAM"].astype(str).str.strip()
    out["COST_SET"] = out["COST_SET"].astype(str).str.strip().str.upper()

    out["HOURS"] = pd.to_numeric(out["HOURS"], errors="coerce").fillna(0.0)
    out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")
    out = out.dropna(subset=["DATE"]).copy()

    return out


def _build_accounting_periods(close_dates: pd.DatetimeIndex) -> pd.DataFrame:
    """
    Creates a period table:
      PERIOD_END (close date), PERIOD_START (day after previous close)
    """
    closes = pd.to_datetime(pd.Series(close_dates)).sort_values().drop_duplicates().reset_index(drop=True)
    periods = pd.DataFrame({"PERIOD_END": closes})

    # start is day after previous close; first start left as NaT (we'll fill with min(data_date))
    periods["PERIOD_START"] = periods["PERIOD_END"].shift(1) + pd.Timedelta(days=1)
    return periods


def _assign_period_end(dates: pd.Series, period_ends: pd.Series) -> pd.Series:
    """
    Assign each transaction date to the *next* accounting close date (PERIOD_END) that is >= DATE.
    """
    ends = pd.to_datetime(period_ends).sort_values().values
    idx = np.searchsorted(ends, dates.values.astype("datetime64[ns]"), side="left")
    out = np.where(idx < len(ends), ends[idx], np.datetime64("NaT"))
    return pd.to_datetime(out)


def _safe_div(a, b):
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    return np.where(b == 0, np.nan, a / b)


# ============================================================
# Main builder
# ============================================================
def build_evms_tables_with_accounting_calendar(
    cobra_merged_df: pd.DataFrame,
    close_dates: pd.DatetimeIndex = ACCOUNTING_CLOSE_DATES_2026,
    costset_to_bucket: dict = COSTSET_TO_BUCKET,
):
    df = _normalize_cobra(cobra_merged_df)

    # Map COST_SET -> EVMS bucket
    df["EVMS_BUCKET"] = df["COST_SET"].map({k.upper(): v for k, v in costset_to_bucket.items()})

    # Keep only buckets we need for the 4 EVMS tables
    keep = {"BCWS", "BCWP", "ACWP", "ETC"}
    evms = df[df["EVMS_BUCKET"].isin(keep)].copy()

    # Build periods + assign PERIOD_END
    periods = _build_accounting_periods(close_dates)
    evms["PERIOD_END"] = _assign_period_end(evms["DATE"], periods["PERIOD_END"])

    # Drop rows that fall after the last known close date (no PERIOD_END assignment)
    evms = evms.dropna(subset=["PERIOD_END"]).copy()

    # Determine LSD period end = latest close date that exists in data
    last_period_end = evms["PERIOD_END"].max()
    next_period_end = periods.loc[periods["PERIOD_END"] > last_period_end, "PERIOD_END"].min()

    # Convenience: filters
    evms_ctd = evms[evms["DATE"] <= last_period_end]
    evms_lsd = evms[evms["PERIOD_END"] == last_period_end]

    # ----------------------------
    # TABLE 1: Program Overview EVMS (CTD + LSD)
    # ----------------------------
    def _pivot_hours(frame, idx_cols):
        p = (
            frame.groupby(idx_cols + ["EVMS_BUCKET"], as_index=False)["HOURS"]
            .sum()
            .pivot(index=idx_cols, columns="EVMS_BUCKET", values="HOURS")
            .reset_index()
        )
        for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
            if c not in p.columns:
                p[c] = 0.0
        return p

    prog_ctd = _pivot_hours(evms_ctd, ["PROGRAM"])
    prog_lsd = _pivot_hours(evms_lsd, ["PROGRAM"])

    program_overview_evms = prog_ctd.rename(columns={
        "BCWS": "BCWS_CTD",
        "BCWP": "BCWP_CTD",
        "ACWP": "ACWP_CTD",
        "ETC": "ETC_CTD",
    }).merge(
        prog_lsd.rename(columns={
            "BCWS": "BCWS_LSD",
            "BCWP": "BCWP_LSD",
            "ACWP": "ACWP_LSD",
            "ETC": "ETC_LSD",
        }),
        on="PROGRAM",
        how="left",
    )

    program_overview_evms.insert(1, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))

    program_overview_evms["SPI_CTD"] = _safe_div(program_overview_evms["BCWP_CTD"], program_overview_evms["BCWS_CTD"])
    program_overview_evms["CPI_CTD"] = _safe_div(program_overview_evms["BCWP_CTD"], program_overview_evms["ACWP_CTD"])
    program_overview_evms["SPI_LSD"] = _safe_div(program_overview_evms["BCWP_LSD"], program_overview_evms["BCWS_LSD"])
    program_overview_evms["CPI_LSD"] = _safe_div(program_overview_evms["BCWP_LSD"], program_overview_evms["ACWP_LSD"])

    # ----------------------------
    # TABLE 2: Subteam SPI/CPI (CTD + LSD)
    # ----------------------------
    st_ctd = _pivot_hours(evms_ctd, ["PROGRAM", "SUB_TEAM"])
    st_lsd = _pivot_hours(evms_lsd, ["PROGRAM", "SUB_TEAM"])

    subteam_spi_cpi = st_ctd.rename(columns={
        "BCWS": "BCWS_CTD",
        "BCWP": "BCWP_CTD",
        "ACWP": "ACWP_CTD",
        "ETC": "ETC_CTD",
    }).merge(
        st_lsd.rename(columns={
            "BCWS": "BCWS_LSD",
            "BCWP": "BCWP_LSD",
            "ACWP": "ACWP_LSD",
            "ETC": "ETC_LSD",
        }),
        on=["PROGRAM", "SUB_TEAM"],
        how="left",
    )

    subteam_spi_cpi.insert(2, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))

    subteam_spi_cpi["SPI_CTD"] = _safe_div(subteam_spi_cpi["BCWP_CTD"], subteam_spi_cpi["BCWS_CTD"])
    subteam_spi_cpi["CPI_CTD"] = _safe_div(subteam_spi_cpi["BCWP_CTD"], subteam_spi_cpi["ACWP_CTD"])
    subteam_spi_cpi["SPI_LSD"] = _safe_div(subteam_spi_cpi["BCWP_LSD"], subteam_spi_cpi["BCWS_LSD"])
    subteam_spi_cpi["CPI_LSD"] = _safe_div(subteam_spi_cpi["BCWP_LSD"], subteam_spi_cpi["ACWP_LSD"])

    # Keep just the requested SPI/CPI fields (rename if you want SPICTD/CPILSD/etc)
    subteam_spi_cpi = subteam_spi_cpi[[
        "PROGRAM", "SUB_TEAM", "LAST_STATUS_PERIOD_END",
        "SPI_LSD", "SPI_CTD",
        "CPI_LSD", "CPI_CTD",
    ]].rename(columns={
        "SPI_LSD": "SPILSD",
        "SPI_CTD": "SPICTD",
        "CPI_LSD": "CPILSD",
        "CPI_CTD": "CPICTD",
    })

    # ----------------------------
    # TABLE 3: Subteam BAC / EAC / VAC (hours)
    # ----------------------------
    # BAC (hours)  = total planned = BCWS_CTD at last closed period
    # EAC (hours)  = ACWP_CTD + ETC_CTD  (if ETC exists) else BAC / CPI_CTD fallback
    # VAC (hours)  = BAC - EAC
    st_full_ctd = _pivot_hours(evms_ctd, ["PROGRAM", "SUB_TEAM"]).rename(columns={
        "BCWS": "BAC_HRS",
        "ACWP": "ACWP_CTD_HRS",
        "ETC": "ETC_CTD_HRS",
        "BCWP": "BCWP_CTD_HRS",
    })

    st_full_ctd["CPI_CTD"] = _safe_div(st_full_ctd["BCWP_CTD_HRS"], st_full_ctd["ACWP_CTD_HRS"])

    etc_present = (st_full_ctd["ETC_CTD_HRS"].fillna(0).sum() > 0)

    if etc_present:
        st_full_ctd["EAC_HRS"] = st_full_ctd["ACWP_CTD_HRS"] + st_full_ctd["ETC_CTD_HRS"]
    else:
        st_full_ctd["EAC_HRS"] = np.where(
            st_full_ctd["CPI_CTD"] > 0,
            st_full_ctd["BAC_HRS"] / st_full_ctd["CPI_CTD"],
            np.nan
        )

    st_full_ctd["VAC_HRS"] = st_full_ctd["BAC_HRS"] - st_full_ctd["EAC_HRS"]

    subteam_bac_eac_vac = st_full_ctd[[
        "PROGRAM", "SUB_TEAM", "BAC_HRS", "EAC_HRS", "VAC_HRS"
    ]].copy()

    # ----------------------------
    # TABLE 4: Program demand/actual + next period BCWS/ETC
    # ----------------------------
    prog_full_ctd = _pivot_hours(evms_ctd, ["PROGRAM"]).rename(columns={
        "BCWS": "DEMAND_HRS_CTD",
        "ACWP": "ACTUAL_HRS_CTD",
        "ETC": "ETC_CTD_HRS",
        "BCWP": "BCWP_CTD_HRS",
    })

    prog_full_ctd["PCT_VARIANCE_CTD"] = _safe_div(
        (prog_full_ctd["ACTUAL_HRS_CTD"] - prog_full_ctd["DEMAND_HRS_CTD"]),
        prog_full_ctd["DEMAND_HRS_CTD"]
    )

    # Next accounting period totals
    if pd.isna(next_period_end):
        next_bcws = pd.DataFrame({"PROGRAM": prog_full_ctd["PROGRAM"], "NEXT_PERIOD_BCWS_HRS": np.nan, "NEXT_PERIOD_ETC_HRS": np.nan})
        next_period_end_value = pd.NaT
    else:
        evms_next = evms[evms["PERIOD_END"] == next_period_end]
        prog_next = _pivot_hours(evms_next, ["PROGRAM"])
        next_bcws = prog_next[["PROGRAM", "BCWS", "ETC"]].rename(columns={
            "BCWS": "NEXT_PERIOD_BCWS_HRS",
            "ETC": "NEXT_PERIOD_ETC_HRS",
        })
        next_period_end_value = pd.Timestamp(next_period_end)

    program_hours_forecast = prog_full_ctd.merge(next_bcws, on="PROGRAM", how="left")
    program_hours_forecast.insert(1, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))
    program_hours_forecast.insert(2, "NEXT_PERIOD_END", next_period_end_value)

    program_hours_forecast = program_hours_forecast[[
        "PROGRAM",
        "LAST_STATUS_PERIOD_END",
        "DEMAND_HRS_CTD",
        "ACTUAL_HRS_CTD",
        "PCT_VARIANCE_CTD",
        "NEXT_PERIOD_BCWS_HRS",
        "NEXT_PERIOD_ETC_HRS",
    ]]

    # Sort outputs
    program_overview_evms = program_overview_evms.sort_values("PROGRAM").reset_index(drop=True)
    subteam_spi_cpi = subteam_spi_cpi.sort_values(["PROGRAM", "SUB_TEAM"]).reset_index(drop=True)
    subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["PROGRAM", "SUB_TEAM"]).reset_index(drop=True)
    program_hours_forecast = program_hours_forecast.sort_values("PROGRAM").reset_index(drop=True)

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast


# ============================================================
# RUN IT
# ============================================================
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast = (
    build_evms_tables_with_accounting_calendar(cobra_merged_df)
)

print("program_overview_evms:", program_overview_evms.shape)
print("subteam_spi_cpi:", subteam_spi_cpi.shape)
print("subteam_bac_eac_vac:", subteam_bac_eac_vac.shape)
print("program_hours_forecast:", program_hours_forecast.shape)

display(program_overview_evms.head())
display(subteam_spi_cpi.head())
display(subteam_bac_eac_vac.head())
display(program_hours_forecast.head())