import pandas as pd
import numpy as np

# ============================================================
# FIXED PIPELINE:
# - Robust assignment of PERIOD_END (no IndexError)
# - COST-SET mapping using your actual Cobra values
# - LSD = latest accounting close date that exists in your data
# ============================================================

# ----------------------------
# 1) Accounting period close dates (2026)  (edit if needed)
# ----------------------------
ACCOUNTING_CLOSE_DATES_2026 = pd.to_datetime([
    "2026-01-04",
    "2026-02-01",
    "2026-03-01",
    "2026-04-05",
    "2026-05-03",
    "2026-06-07",
    "2026-07-05",
    "2026-08-02",
    "2026-09-27",  # confirm if needed
    "2026-10-04",
    "2026-11-01",
    "2026-11-29",
    "2026-12-27",
])

# ----------------------------
# 2) COST-SET mapping (from your value_counts)
# ----------------------------
COSTSET_TO_BUCKET = {
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    "ACWP_HRS": "ACWP",
    "ACWP": "ACWP",
    "ACTUALS": "ACWP",

    "ETC": "ETC",

    # not used in math below, but safe to keep around
    "EAC": "EAC",
}


# ============================================================
# Helpers
# ============================================================
def _normalize_cobra(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in out.columns]

    # normalize COST_SET naming
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


def _safe_div(a, b):
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    return np.where(b == 0, np.nan, a / b)


def _assign_period_end_safe(dates: pd.Series, period_ends: pd.Series) -> pd.Series:
    """
    Assign each transaction date to the first PERIOD_END that is >= DATE.
    Safe (no IndexError): out-of-range indices become NaT.
    """
    ends = np.sort(pd.to_datetime(period_ends).dropna().unique())
    d = pd.to_datetime(dates).values.astype("datetime64[ns]")

    idx = np.searchsorted(ends, d, side="left")  # can be == len(ends)
    out = np.full(shape=len(d), fill_value=np.datetime64("NaT"), dtype="datetime64[ns]")

    in_range = idx < len(ends)
    out[in_range] = ends[idx[in_range]]
    return pd.to_datetime(out)


def _pivot_hours(frame: pd.DataFrame, idx_cols: list[str]) -> pd.DataFrame:
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
    cmap = {k.upper(): v for k, v in costset_to_bucket.items()}
    df["EVMS_BUCKET"] = df["COST_SET"].map(cmap)

    # Keep only buckets needed for tables
    keep = {"BCWS", "BCWP", "ACWP", "ETC"}
    evms = df[df["EVMS_BUCKET"].isin(keep)].copy()

    # Assign accounting PERIOD_END safely
    evms["PERIOD_END"] = _assign_period_end_safe(evms["DATE"], close_dates)

    # Drop rows with no PERIOD_END (i.e., after the last known close date in your calendar list)
    evms = evms.dropna(subset=["PERIOD_END"]).copy()

    # LSD = latest period end present in the (mapped) data
    last_period_end = evms["PERIOD_END"].max()

    # Next period end = next close date after LSD (if it exists)
    closes_sorted = np.sort(pd.to_datetime(close_dates).dropna().unique())
    next_candidates = closes_sorted[closes_sorted > np.datetime64(last_period_end)]
    next_period_end = pd.to_datetime(next_candidates[0]) if len(next_candidates) else pd.NaT

    # CTD: everything up through LSD (inclusive)
    evms_ctd = evms[evms["DATE"] <= last_period_end].copy()
    # LSD rows: those assigned to the LSD period end
    evms_lsd = evms[evms["PERIOD_END"] == last_period_end].copy()

    # ----------------------------
    # TABLE 1: Program Overview EVMS (CTD + LSD)
    # ----------------------------
    prog_ctd = _pivot_hours(evms_ctd, ["PROGRAM"]).rename(columns={
        "BCWS": "BCWS_CTD", "BCWP": "BCWP_CTD", "ACWP": "ACWP_CTD", "ETC": "ETC_CTD"
    })
    prog_lsd = _pivot_hours(evms_lsd, ["PROGRAM"]).rename(columns={
        "BCWS": "BCWS_LSD", "BCWP": "BCWP_LSD", "ACWP": "ACWP_LSD", "ETC": "ETC_LSD"
    })

    program_overview_evms = prog_ctd.merge(prog_lsd, on="PROGRAM", how="left")
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

    subteam_spi_cpi = st_ctd.merge(st_lsd, on=["PROGRAM", "SUB_TEAM"], how="left", suffixes=("_CTD", "_LSD"))
    subteam_spi_cpi.insert(2, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))

    # after merge, columns are BCWS_CTD etc only if pivot created them; we used suffixes:
    subteam_spi_cpi["SPICTD"] = _safe_div(subteam_spi_cpi["BCWP_CTD"], subteam_spi_cpi["BCWS_CTD"])
    subteam_spi_cpi["CPICTD"] = _safe_div(subteam_spi_cpi["BCWP_CTD"], subteam_spi_cpi["ACWP_CTD"])
    subteam_spi_cpi["SPILSD"] = _safe_div(subteam_spi_cpi["BCWP_LSD"], subteam_spi_cpi["BCWS_LSD"])
    subteam_spi_cpi["CPILSD"] = _safe_div(subteam_spi_cpi["BCWP_LSD"], subteam_spi_cpi["ACWP_LSD"])

    subteam_spi_cpi = subteam_spi_cpi[[
        "PROGRAM", "SUB_TEAM", "LAST_STATUS_PERIOD_END",
        "SPILSD", "SPICTD", "CPILSD", "CPICTD"
    ]]

    # ----------------------------
    # TABLE 3: Subteam BAC/EAC/VAC (hours)
    # ----------------------------
    st_full = _pivot_hours(evms_ctd, ["PROGRAM", "SUB_TEAM"]).rename(columns={
        "BCWS": "BAC_HRS",
        "BCWP": "BCWP_CTD_HRS",
        "ACWP": "ACWP_CTD_HRS",
        "ETC": "ETC_CTD_HRS",
    })
    st_full["CPI_CTD"] = _safe_div(st_full["BCWP_CTD_HRS"], st_full["ACWP_CTD_HRS"])

    etc_present = st_full["ETC_CTD_HRS"].fillna(0).sum() > 0
    if etc_present:
        st_full["EAC_HRS"] = st_full["ACWP_CTD_HRS"] + st_full["ETC_CTD_HRS"]
    else:
        st_full["EAC_HRS"] = np.where(st_full["CPI_CTD"] > 0, st_full["BAC_HRS"] / st_full["CPI_CTD"], np.nan)

    st_full["VAC_HRS"] = st_full["BAC_HRS"] - st_full["EAC_HRS"]

    subteam_bac_eac_vac = st_full[["PROGRAM", "SUB_TEAM", "BAC_HRS", "EAC_HRS", "VAC_HRS"]]

    # ----------------------------
    # TABLE 4: Program demand/actual + next period BCWS/ETC
    # ----------------------------
    prog_full = _pivot_hours(evms_ctd, ["PROGRAM"]).rename(columns={
        "BCWS": "DEMAND_HRS_CTD",
        "ACWP": "ACTUAL_HRS_CTD",
        "BCWP": "BCWP_CTD_HRS",
        "ETC": "ETC_CTD_HRS",
    })
    prog_full["PCT_VARIANCE_CTD"] = _safe_div(
        (prog_full["ACTUAL_HRS_CTD"] - prog_full["DEMAND_HRS_CTD"]),
        prog_full["DEMAND_HRS_CTD"]
    )

    if pd.isna(next_period_end):
        next_tbl = prog_full[["PROGRAM"]].copy()
        next_tbl["NEXT_PERIOD_BCWS_HRS"] = np.nan
        next_tbl["NEXT_PERIOD_ETC_HRS"] = np.nan
    else:
        evms_next = evms[evms["PERIOD_END"] == next_period_end].copy()
        prog_next = _pivot_hours(evms_next, ["PROGRAM"])
        next_tbl = prog_next[["PROGRAM", "BCWS", "ETC"]].rename(columns={
            "BCWS": "NEXT_PERIOD_BCWS_HRS",
            "ETC": "NEXT_PERIOD_ETC_HRS",
        })

    program_hours_forecast = prog_full.merge(next_tbl, on="PROGRAM", how="left")
    program_hours_forecast.insert(1, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))
    program_hours_forecast.insert(2, "NEXT_PERIOD_END", pd.Timestamp(next_period_end) if not pd.isna(next_period_end) else pd.NaT)

    program_hours_forecast = program_hours_forecast[[
        "PROGRAM",
        "LAST_STATUS_PERIOD_END",
        "NEXT_PERIOD_END",
        "DEMAND_HRS_CTD",
        "ACTUAL_HRS_CTD",
        "PCT_VARIANCE_CTD",
        "NEXT_PERIOD_BCWS_HRS",
        "NEXT_PERIOD_ETC_HRS",
    ]]

    # Sort for cleanliness
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