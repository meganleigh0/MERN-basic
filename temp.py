limport pandas as pd
import numpy as np

# =========================
# 1) Accounting close dates (edit if needed)
# =========================
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

# =========================
# 2) COST-SET mapping (from your value_counts)
# =========================
COSTSET_TO_BUCKET = {
    "BUDGET": "BCWS",
    "BCWS": "BCWS",

    "PROGRESS": "BCWP",
    "BCWP": "BCWP",

    "ACWP_HRS": "ACWP",
    "ACWP": "ACWP",
    "ACTUALS": "ACWP",

    "ETC": "ETC",

    # kept for reference, not used in EVMS math below
    "EAC": "EAC",
}

NEEDED_BUCKETS = ["BCWS", "BCWP", "ACWP", "ETC"]


# =========================
# Helpers
# =========================
def _normalize_cobra(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in out.columns]
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


def _assign_period_end_safe(dates: pd.Series, period_ends: pd.Series) -> pd.Series:
    ends = np.sort(pd.to_datetime(period_ends).dropna().unique())
    d = pd.to_datetime(dates).values.astype("datetime64[ns]")

    idx = np.searchsorted(ends, d, side="left")  # can be == len(ends)
    out = np.full(len(d), np.datetime64("NaT"), dtype="datetime64[ns]")
    mask = idx < len(ends)
    out[mask] = ends[idx[mask]]
    return pd.to_datetime(out)


def _safe_div(a, b, ratio_on_zero_denom=np.nan):
    """
    If denom==0 -> return ratio_on_zero_denom (set to 0.0 if you want fewer blanks)
    """
    a = np.asarray(a, dtype="float64")
    b = np.asarray(b, dtype="float64")
    return np.where(b == 0, ratio_on_zero_denom, a / b)


def _pivot_hours(frame: pd.DataFrame, idx_cols: list[str]) -> pd.DataFrame:
    p = (
        frame.groupby(idx_cols + ["EVMS_BUCKET"], as_index=False)["HOURS"]
        .sum()
        .pivot(index=idx_cols, columns="EVMS_BUCKET", values="HOURS")
        .reset_index()
    )
    # IMPORTANT: fill missing buckets with 0 (this eliminates most "Missing value" cells)
    for c in NEEDED_BUCKETS:
        if c not in p.columns:
            p[c] = 0.0
    p[NEEDED_BUCKETS] = p[NEEDED_BUCKETS].fillna(0.0)
    return p


# =========================
# Main builder
# =========================
def build_evms_tables_with_calendar(
    cobra_merged_df: pd.DataFrame,
    close_dates: pd.DatetimeIndex = ACCOUNTING_CLOSE_DATES_2026,
    costset_to_bucket: dict = COSTSET_TO_BUCKET,
    ratio_on_zero_denom: float | None = np.nan,   # <- set to 0.0 to eliminate ratio NaNs
):
    df = _normalize_cobra(cobra_merged_df)

    cmap = {k.upper(): v for k, v in costset_to_bucket.items()}
    df["EVMS_BUCKET"] = df["COST_SET"].map(cmap)

    evms = df[df["EVMS_BUCKET"].isin(NEEDED_BUCKETS)].copy()

    # Assign accounting period end, drop rows beyond last known close
    evms["PERIOD_END"] = _assign_period_end_safe(evms["DATE"], close_dates)
    evms = evms.dropna(subset=["PERIOD_END"]).copy()

    # Sort closes and determine LSD window
    closes_sorted = np.sort(pd.to_datetime(close_dates).dropna().unique())
    last_period_end = evms["PERIOD_END"].max()

    # prev close (for LSD window start)
    prev_candidates = closes_sorted[closes_sorted < np.datetime64(last_period_end)]
    prev_period_end = pd.to_datetime(prev_candidates[-1]) if len(prev_candidates) else pd.NaT
    lsd_start = (prev_period_end + pd.Timedelta(days=1)) if not pd.isna(prev_period_end) else evms["DATE"].min()

    # Next period end
    next_candidates = closes_sorted[closes_sorted > np.datetime64(last_period_end)]
    next_period_end = pd.to_datetime(next_candidates[0]) if len(next_candidates) else pd.NaT

    # CTD up through LSD close date
    evms_ctd = evms[evms["DATE"] <= last_period_end].copy()
    # LSD = only dates in the last accounting window (prev_close+1 .. close)
    evms_lsd = evms[(evms["DATE"] >= lsd_start) & (evms["DATE"] <= last_period_end)].copy()

    # -------------------------
    # TABLE 1: Program overview (CTD + LSD)
    # -------------------------
    prog_ctd = _pivot_hours(evms_ctd, ["PROGRAM"]).rename(columns={
        "BCWS": "BCWS_CTD", "BCWP": "BCWP_CTD", "ACWP": "ACWP_CTD", "ETC": "ETC_CTD"
    })
    prog_lsd = _pivot_hours(evms_lsd, ["PROGRAM"]).rename(columns={
        "BCWS": "BCWS_LSD", "BCWP": "BCWP_LSD", "ACWP": "ACWP_LSD", "ETC": "ETC_LSD"
    })

    program_overview_evms = prog_ctd.merge(prog_lsd, on="PROGRAM", how="left")
    # Fill missing LSD hours with 0 (critical)
    for c in ["BCWS_LSD","BCWP_LSD","ACWP_LSD","ETC_LSD"]:
        program_overview_evms[c] = program_overview_evms[c].fillna(0.0)

    program_overview_evms.insert(1, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))
    program_overview_evms.insert(2, "LAST_STATUS_PERIOD_START", pd.Timestamp(lsd_start))

    program_overview_evms["SPI_CTD"] = _safe_div(program_overview_evms["BCWP_CTD"], program_overview_evms["BCWS_CTD"], ratio_on_zero_denom)
    program_overview_evms["CPI_CTD"] = _safe_div(program_overview_evms["BCWP_CTD"], program_overview_evms["ACWP_CTD"], ratio_on_zero_denom)
    program_overview_evms["SPI_LSD"] = _safe_div(program_overview_evms["BCWP_LSD"], program_overview_evms["BCWS_LSD"], ratio_on_zero_denom)
    program_overview_evms["CPI_LSD"] = _safe_div(program_overview_evms["BCWP_LSD"], program_overview_evms["ACWP_LSD"], ratio_on_zero_denom)

    # -------------------------
    # TABLE 2: Subteam SPI/CPI (CTD + LSD)
    # -------------------------
    st_ctd = _pivot_hours(evms_ctd, ["PROGRAM", "SUB_TEAM"]).rename(columns={
        "BCWS": "BCWS_CTD", "BCWP": "BCWP_CTD", "ACWP": "ACWP_CTD", "ETC": "ETC_CTD"
    })
    st_lsd = _pivot_hours(evms_lsd, ["PROGRAM", "SUB_TEAM"]).rename(columns={
        "BCWS": "BCWS_LSD", "BCWP": "BCWP_LSD", "ACWP": "ACWP_LSD", "ETC": "ETC_LSD"
    })

    subteam = st_ctd.merge(st_lsd, on=["PROGRAM","SUB_TEAM"], how="left")
    for c in ["BCWS_LSD","BCWP_LSD","ACWP_LSD","ETC_LSD"]:
        subteam[c] = subteam[c].fillna(0.0)

    subteam.insert(2, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))
    subteam.insert(3, "LAST_STATUS_PERIOD_START", pd.Timestamp(lsd_start))

    subteam["SPICTD"] = _safe_div(subteam["BCWP_CTD"], subteam["BCWS_CTD"], ratio_on_zero_denom)
    subteam["CPICTD"] = _safe_div(subteam["BCWP_CTD"], subteam["ACWP_CTD"], ratio_on_zero_denom)
    subteam["SPILSD"] = _safe_div(subteam["BCWP_LSD"], subteam["BCWS_LSD"], ratio_on_zero_denom)
    subteam["CPILSD"] = _safe_div(subteam["BCWP_LSD"], subteam["ACWP_LSD"], ratio_on_zero_denom)

    subteam_spi_cpi = subteam[[
        "PROGRAM","SUB_TEAM",
        "LAST_STATUS_PERIOD_START","LAST_STATUS_PERIOD_END",
        "SPILSD","SPICTD","CPILSD","CPICTD"
    ]]

    # -------------------------
    # TABLE 3: Subteam BAC/EAC/VAC (hours)
    # Per your reference:
    #   BAC = Total Budget (hours)  -> use CTD BCWS total
    #   EAC = CTD ACWP + ETC (hours)
    #   VAC = BAC - EAC
    # IMPORTANT: treat missing ETC as 0 (NOT NaN)
    # -------------------------
    st_full = _pivot_hours(evms_ctd, ["PROGRAM", "SUB_TEAM"]).rename(columns={
        "BCWS": "BAC_HRS",
        "ACWP": "ACWP_CTD_HRS",
        "ETC":  "ETC_CTD_HRS",
        "BCWP": "BCWP_CTD_HRS",
    })
    st_full["ETC_CTD_HRS"] = st_full["ETC_CTD_HRS"].fillna(0.0)

    st_full["EAC_HRS"] = st_full["ACWP_CTD_HRS"].fillna(0.0) + st_full["ETC_CTD_HRS"].fillna(0.0)
    st_full["VAC_HRS"] = st_full["BAC_HRS"].fillna(0.0) - st_full["EAC_HRS"].fillna(0.0)

    subteam_bac_eac_vac = st_full[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]]

    # -------------------------
    # TABLE 4: Program demand/actual/%var + next period BCWS/ETC
    # demand = BCWS_CTD, actual = ACWP_CTD, %var = (actual-demand)/demand
    # next period = BCWS/ETC in next accounting window (if exists)
    # -------------------------
    prog_full = _pivot_hours(evms_ctd, ["PROGRAM"]).rename(columns={
        "BCWS": "DEMAND_HRS_CTD",
        "ACWP": "ACTUAL_HRS_CTD",
        "ETC":  "ETC_CTD_HRS",
        "BCWP": "BCWP_CTD_HRS",
    })

    prog_full["PCT_VARIANCE_CTD"] = _safe_div(
        (prog_full["ACTUAL_HRS_CTD"] - prog_full["DEMAND_HRS_CTD"]),
        prog_full["DEMAND_HRS_CTD"],
        ratio_on_zero_denom
    )

    if pd.isna(next_period_end):
        next_tbl = prog_full[["PROGRAM"]].copy()
        next_tbl["NEXT_PERIOD_BCWS_HRS"] = np.nan
        next_tbl["NEXT_PERIOD_ETC_HRS"] = np.nan
    else:
        evms_next = evms[evms["PERIOD_END"] == next_period_end].copy()
        prog_next = _pivot_hours(evms_next, ["PROGRAM"])
        next_tbl = prog_next[["PROGRAM","BCWS","ETC"]].rename(columns={
            "BCWS":"NEXT_PERIOD_BCWS_HRS",
            "ETC":"NEXT_PERIOD_ETC_HRS"
        })

    program_hours_forecast = prog_full.merge(next_tbl, on="PROGRAM", how="left")
    program_hours_forecast.insert(1, "LAST_STATUS_PERIOD_END", pd.Timestamp(last_period_end))
    program_hours_forecast.insert(2, "LAST_STATUS_PERIOD_START", pd.Timestamp(lsd_start))
    program_hours_forecast.insert(3, "NEXT_PERIOD_END", pd.Timestamp(next_period_end) if not pd.isna(next_period_end) else pd.NaT)

    program_hours_forecast = program_hours_forecast[[
        "PROGRAM",
        "LAST_STATUS_PERIOD_START","LAST_STATUS_PERIOD_END",
        "NEXT_PERIOD_END",
        "DEMAND_HRS_CTD","ACTUAL_HRS_CTD","PCT_VARIANCE_CTD",
        "NEXT_PERIOD_BCWS_HRS","NEXT_PERIOD_ETC_HRS"
    ]]

    # Sort
    program_overview_evms = program_overview_evms.sort_values("PROGRAM").reset_index(drop=True)
    subteam_spi_cpi = subteam_spi_cpi.sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
    subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
    program_hours_forecast = program_hours_forecast.sort_values("PROGRAM").reset_index(drop=True)

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast


# =========================
# RUN (set ratio_on_zero_denom=0.0 if you want NO missing SPI/CPI)
# =========================
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast = build_evms_tables_with_calendar(
    cobra_merged_df,
    ratio_on_zero_denom=np.nan   # change to 0.0 if leadership wants blanks avoided
)

print("program_overview_evms:", program_overview_evms.shape)
print("subteam_spi_cpi:", subteam_spi_cpi.shape)
print("subteam_bac_eac_vac:", subteam_bac_eac_vac.shape)
print("program_hours_forecast:", program_hours_forecast.shape)

display(program_overview_evms.head(10))
display(subteam_spi_cpi.head(10))
display(subteam_bac_eac_vac.head(10))
display(program_hours_forecast.head(10))