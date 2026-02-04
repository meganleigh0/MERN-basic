import pandas as pd
import numpy as np

# ----------------------------
# 1) Accounting close dates (2026)  (edit if needed)
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
}

BUCKETS = ["BCWS", "BCWP", "ACWP", "ETC"]


# ============================================================
# Helpers
# ============================================================
def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in out.columns]
    if "COSTSET" in out.columns and "COST_SET" not in out.columns:
        out = out.rename(columns={"COSTSET": "COST_SET"})

    req = {"PROGRAM","SUB_TEAM","COST_SET","DATE","HOURS"}
    miss = req - set(out.columns)
    if miss:
        raise ValueError(f"Missing required columns: {sorted(miss)}")

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
    m = idx < len(ends)
    out[m] = ends[idx[m]]
    return pd.to_datetime(out)


def _ensure_bucket_cols(piv: pd.DataFrame) -> pd.DataFrame:
    for b in BUCKETS:
        if b not in piv.columns:
            piv[b] = 0.0
    piv[BUCKETS] = piv[BUCKETS].fillna(0.0)
    return piv


def _compute_ratios_with_ffill(g: pd.DataFrame) -> pd.DataFrame:
    """
    Within an entity timeseries (sorted by PERIOD_END), compute:
      - period SPI/CPI (LSD-style) using period sums
      - cumulative SPI/CPI (CTD-style) using cumulative sums
    And forward-fill ratios when denom hits 0 (like your older pipeline).
    """
    g = g.sort_values("PERIOD_END").copy()

    # cumulative sums
    g["BCWS_CUM"] = g["BCWS"].cumsum()
    g["BCWP_CUM"] = g["BCWP"].cumsum()
    g["ACWP_CUM"] = g["ACWP"].cumsum()
    g["ETC_CUM"]  = g["ETC"].cumsum()

    # monthly/period ratios (LSD)
    spi_lsd = g["BCWP"] / g["BCWS"].replace(0, np.nan)
    cpi_lsd = g["BCWP"] / g["ACWP"].replace(0, np.nan)

    # cumulative ratios (CTD)
    spi_ctd = g["BCWP_CUM"] / g["BCWS_CUM"].replace(0, np.nan)
    cpi_ctd = g["BCWP_CUM"] / g["ACWP_CUM"].replace(0, np.nan)

    # forward-fill ratios to avoid blanks when denom is 0 in a given period
    g["SPI_LSD"] = spi_lsd.ffill()
    g["CPI_LSD"] = cpi_lsd.ffill()
    g["SPI_CTD"] = spi_ctd.ffill()
    g["CPI_CTD"] = cpi_ctd.ffill()

    return g


def _pick_last_period_with_data(g: pd.DataFrame) -> pd.Timestamp:
    """
    Choose LSD_END as the last PERIOD_END where this entity has any
    BCWS/BCWP/ACWP activity (sum>0). This prevents 'missing' LSD rows.
    """
    has_perf = (g["BCWS"] > 0) | (g["BCWP"] > 0) | (g["ACWP"] > 0)
    if not has_perf.any():
        return g["PERIOD_END"].max()  # fallback (still may be all zeros)
    return g.loc[has_perf, "PERIOD_END"].max()


def _next_period_end(after_end: pd.Timestamp, all_closes: pd.DatetimeIndex):
    closes = np.sort(pd.to_datetime(all_closes).dropna().unique())
    nxt = closes[closes > np.datetime64(after_end)]
    return pd.to_datetime(nxt[0]) if len(nxt) else pd.NaT


# ============================================================
# Main EVMS builder (4 tables)
# ============================================================
def build_evms_tables_no_missing(
    cobra_merged_df: pd.DataFrame,
    close_dates: pd.DatetimeIndex = ACCOUNTING_CLOSE_DATES_2026,
    costset_to_bucket: dict = COSTSET_TO_BUCKET,
):
    df = _normalize(cobra_merged_df)

    cmap = {k.upper(): v for k, v in costset_to_bucket.items()}
    df["EVMS_BUCKET"] = df["COST_SET"].map(cmap)
    df = df[df["EVMS_BUCKET"].isin(BUCKETS)].copy()

    # Map each row to accounting period end; drop anything beyond last provided close date
    df["PERIOD_END"] = _assign_period_end_safe(df["DATE"], close_dates)
    df = df.dropna(subset=["PERIOD_END"]).copy()

    # ----------------------------
    # Build PERIOD_END x entity pivot
    # ----------------------------
    base = (
        df.groupby(["PROGRAM","SUB_TEAM","PERIOD_END","EVMS_BUCKET"], as_index=False)["HOURS"]
          .sum()
          .pivot(index=["PROGRAM","SUB_TEAM","PERIOD_END"], columns="EVMS_BUCKET", values="HOURS")
          .reset_index()
    )
    base = _ensure_bucket_cols(base)

    # Compute ratios with ffill per PROGRAM+SUB_TEAM
    base = (
        base.groupby(["PROGRAM","SUB_TEAM"], group_keys=False)
            .apply(_compute_ratios_with_ffill)
            .reset_index(drop=True)
    )

    # Also make a PROGRAM-only timeseries by summing subteams within period
    prog_ts = (
        base.groupby(["PROGRAM","PERIOD_END"], as_index=False)[BUCKETS].sum()
    )
    prog_ts = (
        prog_ts.groupby(["PROGRAM"], group_keys=False)
               .apply(_compute_ratios_with_ffill)
               .reset_index(drop=True)
    )

    # ----------------------------
    # TABLE 1: Program Overview EVMS (entity-specific LSD)
    # ----------------------------
    out_prog = []
    for prog, g in prog_ts.groupby("PROGRAM"):
        lsd_end = _pick_last_period_with_data(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        # previous period end for display (optional)
        closes_sorted = np.sort(pd.to_datetime(close_dates).dropna().unique())
        prevs = closes_sorted[closes_sorted < np.datetime64(lsd_end)]
        lsd_start = (pd.to_datetime(prevs[-1]) + pd.Timedelta(days=1)) if len(prevs) else g["PERIOD_END"].min()

        out_prog.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_START": pd.Timestamp(lsd_start),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),

            # CTD (cumulative)
            "BCWS_CTD": float(row["BCWS_CUM"]),
            "BCWP_CTD": float(row["BCWP_CUM"]),
            "ACWP_CTD": float(row["ACWP_CUM"]),
            "ETC_CTD":  float(row["ETC_CUM"]),
            "SPI_CTD":  float(row["SPI_CTD"]) if pd.notna(row["SPI_CTD"]) else np.nan,
            "CPI_CTD":  float(row["CPI_CTD"]) if pd.notna(row["CPI_CTD"]) else np.nan,

            # LSD (period)
            "BCWS_LSD": float(row["BCWS"]),
            "BCWP_LSD": float(row["BCWP"]),
            "ACWP_LSD": float(row["ACWP"]),
            "ETC_LSD":  float(row["ETC"]),
            "SPI_LSD":  float(row["SPI_LSD"]) if pd.notna(row["SPI_LSD"]) else np.nan,
            "CPI_LSD":  float(row["CPI_LSD"]) if pd.notna(row["CPI_LSD"]) else np.nan,
        })

    program_overview_evms = pd.DataFrame(out_prog).sort_values("PROGRAM").reset_index(drop=True)

    # ----------------------------
    # TABLE 2: Subteam SPI/CPI (entity-specific LSD)
    # ----------------------------
    out_st = []
    for (prog, st), g in base.groupby(["PROGRAM","SUB_TEAM"]):
        lsd_end = _pick_last_period_with_data(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        closes_sorted = np.sort(pd.to_datetime(close_dates).dropna().unique())
        prevs = closes_sorted[closes_sorted < np.datetime64(lsd_end)]
        lsd_start = (pd.to_datetime(prevs[-1]) + pd.Timedelta(days=1)) if len(prevs) else g["PERIOD_END"].min()

        out_st.append({
            "PROGRAM": prog,
            "SUB_TEAM": st,
            "LAST_STATUS_PERIOD_START": pd.Timestamp(lsd_start),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),

            # naming like your dashboard: SPILSD/SPICTD/CPILSD/CPICTD
            "SPILSD": float(row["SPI_LSD"]) if pd.notna(row["SPI_LSD"]) else np.nan,
            "SPICTD": float(row["SPI_CTD"]) if pd.notna(row["SPI_CTD"]) else np.nan,
            "CPILSD": float(row["CPI_LSD"]) if pd.notna(row["CPI_LSD"]) else np.nan,
            "CPICTD": float(row["CPI_CTD"]) if pd.notna(row["CPI_CTD"]) else np.nan,
        })

    subteam_spi_cpi = pd.DataFrame(out_st).sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

    # ----------------------------
    # TABLE 3: Subteam BAC / EAC / VAC
    # Per your notes:
    #   BAC (hrs) = total budget in hours  -> use BCWS_CUM at LSD_END
    #   EAC (hrs) = CTD ACWP + ETC (hrs)
    #   VAC (hrs) = BAC - EAC
    # ----------------------------
    out_labor = []
    for (prog, st), g in base.groupby(["PROGRAM","SUB_TEAM"]):
        lsd_end = _pick_last_period_with_data(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        bac = float(row["BCWS_CUM"])
        eac = float(row["ACWP_CUM"] + row["ETC_CUM"])
        vac = bac - eac

        out_labor.append({
            "PROGRAM": prog,
            "SUB_TEAM": st,
            "BAC_HRS": bac,
            "EAC_HRS": eac,
            "VAC_HRS": vac,
        })

    subteam_bac_eac_vac = pd.DataFrame(out_labor).sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

    # ----------------------------
    # TABLE 4: Program Demand/Actual/%Var + Next Period BCWS/ETC
    # demand = BCWS_CTD, actual = ACWP_CTD, %var = (actual-demand)/demand
    # next period BCWS/ETC = period sums for next accounting close after LSD_END
    # ----------------------------
    out_mp = []
    for prog, g in prog_ts.groupby("PROGRAM"):
        lsd_end = _pick_last_period_with_data(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        demand_ctd = float(row["BCWS_CUM"])
        actual_ctd = float(row["ACWP_CUM"])
        pct_var = (actual_ctd - demand_ctd) / demand_ctd if demand_ctd != 0 else np.nan

        nxt_end = _next_period_end(lsd_end, close_dates)
        if pd.isna(nxt_end):
            next_bcws = np.nan
            next_etc = np.nan
        else:
            nxt_row = g[g["PERIOD_END"] == nxt_end]
            if len(nxt_row):
                nxt_row = nxt_row.tail(1).iloc[0]
                next_bcws = float(nxt_row["BCWS"])
                next_etc = float(nxt_row["ETC"])
            else:
                next_bcws = 0.0
                next_etc = 0.0

        closes_sorted = np.sort(pd.to_datetime(close_dates).dropna().unique())
        prevs = closes_sorted[closes_sorted < np.datetime64(lsd_end)]
        lsd_start = (pd.to_datetime(prevs[-1]) + pd.Timedelta(days=1)) if len(prevs) else g["PERIOD_END"].min()

        out_mp.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_START": pd.Timestamp(lsd_start),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),
            "NEXT_PERIOD_END": pd.Timestamp(nxt_end) if not pd.isna(nxt_end) else pd.NaT,
            "DEMAND_HRS_CTD": demand_ctd,
            "ACTUAL_HRS_CTD": actual_ctd,
            "PCT_VARIANCE_CTD": pct_var,
            "NEXT_PERIOD_BCWS_HRS": next_bcws,
            "NEXT_PERIOD_ETC_HRS": next_etc,
        })

    program_hours_forecast = pd.DataFrame(out_mp).sort_values("PROGRAM").reset_index(drop=True)

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast


# ============================================================
# RUN
# ============================================================
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast = build_evms_tables_no_missing(
    cobra_merged_df
)

print("program_overview_evms:", program_overview_evms.shape)
print("subteam_spi_cpi:", subteam_spi_cpi.shape)
print("subteam_bac_eac_vac:", subteam_bac_eac_vac.shape)
print("program_hours_forecast:", program_hours_forecast.shape)

display(program_overview_evms.head(10))
display(subteam_spi_cpi.head(10))
display(subteam_bac_eac_vac.head(10))
display(program_hours_forecast.head(10))