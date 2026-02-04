import pandas as pd
import numpy as np

# ============================================================
# CONFIG
# ============================================================

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

# Optional: explicit read-only ranges (like the yellow weeks / holidays).
# Add as needed; leaving empty means "no special read-only enforced".
READ_ONLY_RANGES_2026 = [
    # ("2026-07-01", "2026-07-05"),  # example
]

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
# CALENDAR BUILD (applies 2025 logic to 2026)
#   - For each period, define:
#       STATUS_ETC_WINDOW: last 7 days ending on close date
#       STATUS_ONLY_WINDOW: 14 days before that (days -21 to -8)
#       OPEN/NORMAL: everything else in the period
#   - READ_ONLY applied via explicit ranges (optional)
# ============================================================

def build_accounting_calendar(close_dates: pd.DatetimeIndex,
                             read_only_ranges=None) -> pd.DataFrame:
    closes = pd.to_datetime(sorted(pd.unique(close_dates)))
    periods = []

    prev_end = pd.NaT
    for i, end in enumerate(closes, start=1):
        start = (prev_end + pd.Timedelta(days=1)) if pd.notna(prev_end) else (end - pd.Timedelta(days=27))
        # (If you have true period starts, replace that 27-day fallback with your known first start.)
        periods.append({
            "PERIOD_N": i,
            "PERIOD_START": pd.Timestamp(start).normalize(),
            "PERIOD_END": pd.Timestamp(end).normalize(),
            "STATUS_ETC_START": (pd.Timestamp(end) - pd.Timedelta(days=6)).normalize(),
            "STATUS_ETC_END": pd.Timestamp(end).normalize(),
            "STATUS_ONLY_START": (pd.Timestamp(end) - pd.Timedelta(days=20)).normalize(),
            "STATUS_ONLY_END": (pd.Timestamp(end) - pd.Timedelta(days=7)).normalize(),
        })
        prev_end = pd.Timestamp(end).normalize()

    cal = pd.DataFrame(periods)

    # Apply explicit read-only day flags
    cal["HAS_READ_ONLY"] = False
    read_only_ranges = read_only_ranges or []
    ro = []
    for a, b in read_only_ranges:
        ro.append((pd.to_datetime(a).normalize(), pd.to_datetime(b).normalize()))
    cal.attrs["read_only_ranges"] = ro
    return cal


def status_state_for_date(d: pd.Timestamp, cal: pd.DataFrame) -> str:
    d = pd.to_datetime(d).normalize()

    # read-only override
    for a, b in cal.attrs.get("read_only_ranges", []):
        if a <= d <= b:
            return "READ_ONLY"

    # period-based logic
    row = cal[(cal["PERIOD_START"] <= d) & (d <= cal["PERIOD_END"])]
    if row.empty:
        return "OUTSIDE_CALENDAR"
    row = row.iloc[0]

    if row["STATUS_ETC_START"] <= d <= row["STATUS_ETC_END"]:
        return "STATUS_AND_ETC"
    if row["STATUS_ONLY_START"] <= d <= row["STATUS_ONLY_END"]:
        return "STATUS_ONLY"
    return "OPEN"


# ============================================================
# DATA NORMALIZATION + PERIOD ASSIGNMENT
# ============================================================

def _normalize_cobra(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in out.columns]
    if "COSTSET" in out.columns and "COST_SET" not in out.columns:
        out = out.rename(columns={"COSTSET": "COST_SET"})

    req = {"PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"}
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
    idx = np.searchsorted(ends, d, side="left")
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


# ============================================================
# METRICS (match your older pipeline style)
#   - ratios computed with denom->nan then ffill
#   - if LSD ratio still missing at end, fallback to CTD ratio
# ============================================================

def _compute_ratios_ffill(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("PERIOD_END").copy()

    g["BCWS_CUM"] = g["BCWS"].cumsum()
    g["BCWP_CUM"] = g["BCWP"].cumsum()
    g["ACWP_CUM"] = g["ACWP"].cumsum()
    g["ETC_CUM"]  = g["ETC"].cumsum()

    spi_lsd = g["BCWP"] / g["BCWS"].replace(0, np.nan)
    cpi_lsd = g["BCWP"] / g["ACWP"].replace(0, np.nan)

    spi_ctd = g["BCWP_CUM"] / g["BCWS_CUM"].replace(0, np.nan)
    cpi_ctd = g["BCWP_CUM"] / g["ACWP_CUM"].replace(0, np.nan)

    g["SPI_LSD"] = spi_lsd.ffill()
    g["CPI_LSD"] = cpi_lsd.ffill()
    g["SPI_CTD"] = spi_ctd.ffill()
    g["CPI_CTD"] = cpi_ctd.ffill()

    # If LSD ratio is still missing at the end, use CTD ratio
    g["SPI_LSD"] = g["SPI_LSD"].fillna(g["SPI_CTD"])
    g["CPI_LSD"] = g["CPI_LSD"].fillna(g["CPI_CTD"])

    return g


def _pick_last_period_with_activity(g: pd.DataFrame) -> pd.Timestamp:
    has_perf = (g["BCWS"] > 0) | (g["BCWP"] > 0) | (g["ACWP"] > 0)
    if has_perf.any():
        return g.loc[has_perf, "PERIOD_END"].max()
    return g["PERIOD_END"].max()


# ============================================================
# PIPELINE (4 EVMS tables + proper next period pulls + status)
# ============================================================

def build_evms_tables_full(cobra_merged_df: pd.DataFrame,
                           close_dates=ACCOUNTING_CLOSE_DATES_2026,
                           read_only_ranges=READ_ONLY_RANGES_2026,
                           costset_to_bucket=COSTSET_TO_BUCKET):

    cal = build_accounting_calendar(close_dates, read_only_ranges)
    df = _normalize_cobra(cobra_merged_df)

    cmap = {k.upper(): v for k, v in costset_to_bucket.items()}
    df["EVMS_BUCKET"] = df["COST_SET"].map(cmap)
    df = df[df["EVMS_BUCKET"].isin(BUCKETS)].copy()

    df["PERIOD_END"] = _assign_period_end_safe(df["DATE"], close_dates)
    df = df.dropna(subset=["PERIOD_END"]).copy()

    # Add STATUS_STATE (daily) – this is what you can use if you need state-based filtering later
    df["STATUS_STATE"] = df["DATE"].apply(lambda x: status_state_for_date(x, cal))

    # --- Period sums at PROGRAM+SUB_TEAM level across ALL periods (this is key for next-period) ---
    st_period = (
        df.groupby(["PROGRAM","SUB_TEAM","PERIOD_END","EVMS_BUCKET"], as_index=False)["HOURS"]
          .sum()
          .pivot(index=["PROGRAM","SUB_TEAM","PERIOD_END"], columns="EVMS_BUCKET", values="HOURS")
          .reset_index()
    )
    st_period = _ensure_bucket_cols(st_period)

    # ratios per subteam
    st_period = (
        st_period.groupby(["PROGRAM","SUB_TEAM"], group_keys=False)
                 .apply(_compute_ratios_ffill)
                 .reset_index(drop=True)
    )

    # --- Program period sums across ALL periods (also key for next-period) ---
    prog_period = (
        st_period.groupby(["PROGRAM","PERIOD_END"], as_index=False)[BUCKETS].sum()
    )
    prog_period = (
        prog_period.groupby(["PROGRAM"], group_keys=False)
                   .apply(_compute_ratios_ffill)
                   .reset_index(drop=True)
    )

    closes_sorted = np.sort(pd.to_datetime(close_dates).dropna().unique())

    def prev_period_start(end):
        prevs = closes_sorted[closes_sorted < np.datetime64(end)]
        return (pd.to_datetime(prevs[-1]) + pd.Timedelta(days=1)) if len(prevs) else pd.NaT

    def next_period_end(end):
        nxt = closes_sorted[closes_sorted > np.datetime64(end)]
        return pd.to_datetime(nxt[0]) if len(nxt) else pd.NaT

    # ============================================================
    # TABLE 1: Program Overview (CTD + LSD) with correct SPI_LSD + CPI_LSD
    # ============================================================
    prog_rows = []
    for prog, g in prog_period.groupby("PROGRAM"):
        lsd_end = _pick_last_period_with_activity(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        prog_rows.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_START": prev_period_start(lsd_end),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),

            "BCWS_CTD": float(row["BCWS_CUM"]),
            "BCWP_CTD": float(row["BCWP_CUM"]),
            "ACWP_CTD": float(row["ACWP_CUM"]),
            "ETC_CTD":  float(row["ETC_CUM"]),
            "SPI_CTD":  float(row["SPI_CTD"]) if pd.notna(row["SPI_CTD"]) else np.nan,
            "CPI_CTD":  float(row["CPI_CTD"]) if pd.notna(row["CPI_CTD"]) else np.nan,

            "BCWS_LSD": float(row["BCWS"]),
            "BCWP_LSD": float(row["BCWP"]),
            "ACWP_LSD": float(row["ACWP"]),
            "ETC_LSD":  float(row["ETC"]),
            "SPI_LSD":  float(row["SPI_LSD"]) if pd.notna(row["SPI_LSD"]) else np.nan,
            "CPI_LSD":  float(row["CPI_LSD"]) if pd.notna(row["CPI_LSD"]) else np.nan,
        })

    program_overview_evms = pd.DataFrame(prog_rows).sort_values("PROGRAM").reset_index(drop=True)

    # ============================================================
    # TABLE 2: Subteam SPI/CPI (CTD + LSD)
    # ============================================================
    st_rows = []
    for (prog, st), g in st_period.groupby(["PROGRAM","SUB_TEAM"]):
        lsd_end = _pick_last_period_with_activity(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        st_rows.append({
            "PROGRAM": prog,
            "SUB_TEAM": st,
            "LAST_STATUS_PERIOD_START": prev_period_start(lsd_end),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),

            "SPILSD": float(row["SPI_LSD"]) if pd.notna(row["SPI_LSD"]) else np.nan,
            "SPICTD": float(row["SPI_CTD"]) if pd.notna(row["SPI_CTD"]) else np.nan,
            "CPILSD": float(row["CPI_LSD"]) if pd.notna(row["CPI_LSD"]) else np.nan,
            "CPICTD": float(row["CPI_CTD"]) if pd.notna(row["CPI_CTD"]) else np.nan,
        })

    subteam_spi_cpi = pd.DataFrame(st_rows).sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

    # ============================================================
    # TABLE 3: Subteam BAC/EAC/VAC (hours) using your definitions
    #   BAC = total budget hours = BCWS_CUM
    #   EAC = CTD ACWP + ETC
    #   VAC = BAC - EAC
    # ============================================================
    labor_rows = []
    for (prog, st), g in st_period.groupby(["PROGRAM","SUB_TEAM"]):
        lsd_end = _pick_last_period_with_activity(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        bac = float(row["BCWS_CUM"])
        eac = float(row["ACWP_CUM"] + row["ETC_CUM"])
        vac = bac - eac

        labor_rows.append({
            "PROGRAM": prog,
            "SUB_TEAM": st,
            "BAC_HRS": bac,
            "EAC_HRS": eac,
            "VAC_HRS": vac,
        })

    subteam_bac_eac_vac = pd.DataFrame(labor_rows).sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

    # ============================================================
    # TABLE 4: Program demand/actual/%var + NEXT PERIOD pulls
    # IMPORTANT FIX: Next period BCWS/ETC are pulled from the FULL program period table,
    # not from the CTD row.
    # Also include NEXT_PERIOD_N
    # ============================================================
    mp_rows = []
    for prog, g in prog_period.groupby("PROGRAM"):
        lsd_end = _pick_last_period_with_activity(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        demand_ctd = float(row["BCWS_CUM"])
        actual_ctd = float(row["ACWP_CUM"])
        pct_var = (actual_ctd - demand_ctd) / demand_ctd if demand_ctd != 0 else np.nan

        nxt_end = next_period_end(lsd_end)
        if pd.isna(nxt_end):
            next_bcws = np.nan
            next_etc = np.nan
            next_n = np.nan
        else:
            nxt_row = g[g["PERIOD_END"] == nxt_end]
            if len(nxt_row):
                nxt_row = nxt_row.tail(1).iloc[0]
                next_bcws = float(nxt_row["BCWS"])
                next_etc = float(nxt_row["ETC"])
            else:
                # period exists in calendar but no data rows for this program
                next_bcws = 0.0
                next_etc = 0.0

            # NEXT_PERIOD_N from calendar
            next_n = int(cal.loc[cal["PERIOD_END"] == pd.to_datetime(nxt_end).normalize(), "PERIOD_N"].iloc[0]) \
                     if (cal["PERIOD_END"] == pd.to_datetime(nxt_end).normalize()).any() else np.nan

        mp_rows.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_START": prev_period_start(lsd_end),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),
            "NEXT_PERIOD_END": pd.Timestamp(nxt_end) if not pd.isna(nxt_end) else pd.NaT,
            "NEXT_PERIOD_N": next_n,

            "DEMAND_HRS_CTD": demand_ctd,
            "ACTUAL_HRS_CTD": actual_ctd,
            "PCT_VARIANCE_CTD": pct_var,
            "NEXT_PERIOD_BCWS_HRS": next_bcws,
            "NEXT_PERIOD_ETC_HRS": next_etc,
        })

    program_hours_forecast = pd.DataFrame(mp_rows).sort_values("PROGRAM").reset_index(drop=True)

    return cal, program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast


# ============================================================
# RUN
# ============================================================
cal_2026, program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast = build_evms_tables_full(
    cobra_merged_df
)

print("calendar:", cal_2026.shape)
print("program_overview_evms:", program_overview_evms.shape)
print("subteam_spi_cpi:", subteam_spi_cpi.shape)
print("subteam_bac_eac_vac:", subteam_bac_eac_vac.shape)
print("program_hours_forecast:", program_hours_forecast.shape)

display(program_overview_evms.head(10))
display(subteam_spi_cpi.head(10))
display(subteam_bac_eac_vac.head(10))
display(program_hours_forecast.head(10))
