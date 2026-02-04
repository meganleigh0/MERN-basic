import pandas as pd
import numpy as np

# ============================================================
# 0) CONFIG
# ============================================================

# ---- 2026 close dates (replace/confirm if needed) ----
ACCOUNTING_CLOSE_DATES_2026 = pd.to_datetime([
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
    "2026-11-29",
    "2026-12-27",
])

# Optional explicit read-only ranges (yellow weeks/holidays) – leave empty if not enforcing
READ_ONLY_RANGES_2026 = [
    # ("2026-07-01", "2026-07-05"),
]

# COST-SET → EVMS bucket mapping (hours-based)
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
# 1) CALENDAR (2025-style windows applied to 2026)
#   - STATUS_AND_ETC: last 7 days ending on close date
#   - STATUS_ONLY: 14 days before that (days -21 to -8)
#   - OPEN: otherwise
#   - READ_ONLY: optional override by explicit date ranges
# ============================================================

def build_accounting_calendar(close_dates, read_only_ranges=None):
    closes = pd.to_datetime(sorted(pd.unique(close_dates)))
    periods = []
    prev_end = pd.NaT

    for i, end in enumerate(closes, start=1):
        end = pd.Timestamp(end).normalize()
        # If you know the true first start date, replace this fallback.
        start = (prev_end + pd.Timedelta(days=1)) if pd.notna(prev_end) else (end - pd.Timedelta(days=27))
        start = pd.Timestamp(start).normalize()

        periods.append({
            "PERIOD_N": i,
            "PERIOD_START": start,
            "PERIOD_END": end,
            "STATUS_ETC_START": (end - pd.Timedelta(days=6)).normalize(),
            "STATUS_ETC_END": end,
            "STATUS_ONLY_START": (end - pd.Timedelta(days=20)).normalize(),
            "STATUS_ONLY_END": (end - pd.Timedelta(days=7)).normalize(),
        })
        prev_end = end

    cal = pd.DataFrame(periods)

    ro = []
    for a, b in (read_only_ranges or []):
        ro.append((pd.to_datetime(a).normalize(), pd.to_datetime(b).normalize()))
    cal.attrs["read_only_ranges"] = ro

    return cal


def status_state_for_date(d, cal):
    d = pd.to_datetime(d).normalize()

    for a, b in cal.attrs.get("read_only_ranges", []):
        if a <= d <= b:
            return "READ_ONLY"

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
# 2) NORMALIZE + PERIOD ASSIGNMENT
# ============================================================

def normalize_cobra(df):
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
    out["HOURS"] = pd.to_numeric(out["HOURS"], errors="coerce")
    out = out.dropna(subset=["HOURS"]).copy()  # keep true missing out
    out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")
    out = out.dropna(subset=["DATE"]).copy()

    return out


def assign_period_end(dates: pd.Series, period_ends: pd.Series) -> pd.Series:
    """Assign each date to the next close date (left searchsorted).
    If date > last close date, PERIOD_END will be NaT."""
    ends = np.sort(pd.to_datetime(period_ends).dropna().unique())
    d = pd.to_datetime(dates).values.astype("datetime64[ns]")

    idx = np.searchsorted(ends, d, side="left")
    out = np.full(len(d), np.datetime64("NaT"), dtype="datetime64[ns]")
    m = idx < len(ends)
    out[m] = ends[idx[m]]
    return pd.to_datetime(out)


# ============================================================
# 3) PERIOD TABLES: DO NOT FILL BUCKETS WITH 0
# ============================================================

def ensure_bucket_cols_nan(piv: pd.DataFrame) -> pd.DataFrame:
    for b in BUCKETS:
        if b not in piv.columns:
            piv[b] = np.nan
    return piv


# ============================================================
# 4) METRICS: preserve missing; cum uses 0; LSD ffill; fallback to CTD
# ============================================================

def compute_ratios_ffill(g: pd.DataFrame) -> pd.DataFrame:
    g = g.sort_values("PERIOD_END").copy()

    # cum sums treat missing as 0 for totals
    bcws0 = g["BCWS"].fillna(0.0)
    bcwp0 = g["BCWP"].fillna(0.0)
    acwp0 = g["ACWP"].fillna(0.0)
    etc0  = g["ETC"].fillna(0.0)

    g["BCWS_CUM"] = bcws0.cumsum()
    g["BCWP_CUM"] = bcwp0.cumsum()
    g["ACWP_CUM"] = acwp0.cumsum()
    g["ETC_CUM"]  = etc0.cumsum()

    # LSD ratios: missing stays NaN; denom 0 -> NaN
    spi_lsd = g["BCWP"] / g["BCWS"].replace(0, np.nan)
    cpi_lsd = g["BCWP"] / g["ACWP"].replace(0, np.nan)

    # CTD ratios
    spi_ctd = g["BCWP_CUM"] / g["BCWS_CUM"].replace(0, np.nan)
    cpi_ctd = g["BCWP_CUM"] / g["ACWP_CUM"].replace(0, np.nan)

    g["SPI_LSD"] = spi_lsd.ffill()
    g["CPI_LSD"] = cpi_lsd.ffill()
    g["SPI_CTD"] = spi_ctd.ffill()
    g["CPI_CTD"] = cpi_ctd.ffill()

    # fallback: LSD missing -> CTD
    g["SPI_LSD"] = g["SPI_LSD"].fillna(g["SPI_CTD"])
    g["CPI_LSD"] = g["CPI_LSD"].fillna(g["CPI_CTD"])

    return g


def pick_last_period_for_perf(g: pd.DataFrame) -> pd.Timestamp:
    """LSD for SPI/CPI must be last period with BCWS/BCWP/ACWP presence (ETC-only must not count)."""
    perf_cols = ["BCWS", "BCWP", "ACWP"]
    has_perf = g[perf_cols].notna().any(axis=1) & (
        (g["BCWS"].fillna(0) != 0) | (g["BCWP"].fillna(0) != 0) | (g["ACWP"].fillna(0) != 0)
    )
    if has_perf.any():
        return g.loc[has_perf, "PERIOD_END"].max()
    return g["PERIOD_END"].max()


# ============================================================
# 5) MAIN BUILDER: returns (calendar + 4 tables)
# ============================================================

def build_evms_tables(cobra_merged_df: pd.DataFrame,
                      close_dates=ACCOUNTING_CLOSE_DATES_2026,
                      read_only_ranges=READ_ONLY_RANGES_2026,
                      costset_to_bucket=COSTSET_TO_BUCKET):

    cal = build_accounting_calendar(close_dates, read_only_ranges)
    df = normalize_cobra(cobra_merged_df)

    cmap = {k.upper(): v for k, v in costset_to_bucket.items()}
    df["EVMS_BUCKET"] = df["COST_SET"].map(cmap)
    df = df[df["EVMS_BUCKET"].isin(BUCKETS)].copy()

    df["PERIOD_END"] = assign_period_end(df["DATE"], close_dates)
    df = df.dropna(subset=["PERIOD_END"]).copy()

    df["STATUS_STATE"] = df["DATE"].apply(lambda x: status_state_for_date(x, cal))

    # --- Subteam period sums (keep NaN for missing buckets) ---
    st_period = (
        df.groupby(["PROGRAM", "SUB_TEAM", "PERIOD_END", "EVMS_BUCKET"], as_index=False)["HOURS"]
          .sum()
          .pivot(index=["PROGRAM", "SUB_TEAM", "PERIOD_END"], columns="EVMS_BUCKET", values="HOURS")
          .reset_index()
    )
    st_period = ensure_bucket_cols_nan(st_period)

    # ratios per subteam
    st_period = (
        st_period.groupby(["PROGRAM","SUB_TEAM"], group_keys=False)
                 .apply(compute_ratios_ffill)
                 .reset_index(drop=True)
    )

    # --- Program period sums (min_count=1 preserves NaN if all missing) ---
    prog_period = (
        st_period.groupby(["PROGRAM","PERIOD_END"], as_index=False)[BUCKETS]
                 .sum(min_count=1)
    )
    prog_period = (
        prog_period.groupby("PROGRAM", group_keys=False)
                   .apply(compute_ratios_ffill)
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
    # TABLE 1: Program Overview EVMS (CTD + LSD)
    # ============================================================
    program_rows = []
    for prog, g in prog_period.groupby("PROGRAM"):
        lsd_end = pick_last_period_for_perf(g)
        row = g[g["PERIOD_END"] == lsd_end].tail(1).iloc[0]

        program_rows.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_START": prev_period_start(lsd_end),
            "LAST_STATUS_PERIOD_END": pd.Timestamp(lsd_end),

            "BCWS_CTD": float(row["BCWS_CUM"]),
            "BCWP_CTD": float(row["BCWP_CUM"]),
            "ACWP_CTD": float(row["ACWP_CUM"]),
            "ETC_CTD":  float(row["ETC_CUM"]),

            "SPI_CTD":  float(row["SPI_CTD"]) if pd.notna(row["SPI_CTD"]) else np.nan,
            "CPI_CTD":  float(row["CPI_CTD"]) if pd.notna(row["CPI_CTD"]) else np.nan,

            # LSD bucket values (NaN if missing)
            "BCWS_LSD": row["BCWS"],
            "BCWP_LSD": row["BCWP"],
            "ACWP_LSD": row["ACWP"],
            "ETC_LSD":  row["ETC"],

            "SPI_LSD":  float(row["SPI_LSD"]) if pd.notna(row["SPI_LSD"]) else np.nan,
            "CPI_LSD":  float(row["CPI_LSD"]) if pd.notna(row["CPI_LSD"]) else np.nan,
        })

    program_overview_evms = pd.DataFrame(program_rows).sort_values("PROGRAM").reset_index(drop=True)

    # ============================================================
    # TABLE 2: Subteam SPI/CPI table (CTD + LSD)
    # ============================================================
    st_rows = []
    for (prog, st), g in st_period.groupby(["PROGRAM","SUB_TEAM"]):
        lsd_end = pick_last_period_for_perf(g)
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
    # TABLE 3: Subteam BAC/EAC/VAC (hours)
    #   BAC = total budget hours = BCWS_CUM
    #   EAC = CTD ACWP + CTD ETC
    #   VAC = BAC - EAC
    # ============================================================
    labor_rows = []
    for (prog, st), g in st_period.groupby(["PROGRAM","SUB_TEAM"]):
        lsd_end = pick_last_period_for_perf(g)
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
    # TABLE 4: Program hours forecast (CTD demand/actual/%var + next period pulls)
    # ============================================================
    mp_rows = []
    for prog, g in prog_period.groupby("PROGRAM"):
        lsd_end = pick_last_period_for_perf(g)
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
                next_bcws = nxt_row["BCWS"]  # keep NaN if missing
                next_etc  = nxt_row["ETC"]
            else:
                next_bcws = np.nan
                next_etc = np.nan

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

    return cal, df, st_period, prog_period, program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast


# ============================================================
# 6) RUN IT
# ============================================================

cal_2026, df_evms, st_period, prog_period, program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast = build_evms_tables(
    cobra_merged_df
)

print("calendar:", cal_2026.shape)
print("df_evms:", df_evms.shape)
print("st_period:", st_period.shape)
print("prog_period:", prog_period.shape)
print("program_overview_evms:", program_overview_evms.shape)
print("subteam_spi_cpi:", subteam_spi_cpi.shape)
print("subteam_bac_eac_vac:", subteam_bac_eac_vac.shape)
print("program_hours_forecast:", program_hours_forecast.shape)

display(program_overview_evms)
display(subteam_spi_cpi.head(25))
display(subteam_bac_eac_vac.head(25))
display(program_hours_forecast)




# Pick a program that looks wrong in program_overview_evms
bad_prog = program_overview_evms.loc[
    (program_overview_evms["SPI_LSD"].isna()) | (program_overview_evms["SPI_LSD"] == 0),
    "PROGRAM"
].head(1)

bad_prog = bad_prog.iloc[0] if len(bad_prog) else program_overview_evms["PROGRAM"].iloc[0]
print("Investigating program:", bad_prog)

# 1) What COST_SET values exist for this program?
print("\nTop COST_SET values for this program:")
display(
    cobra_merged_df.loc[cobra_merged_df["PROGRAM"] == bad_prog, "COST-SET"]
    .astype(str).str.upper().str.strip()
    .value_counts()
    .head(25)
)

# 2) For EVMS buckets after mapping, do we actually have BCWS/BCWP/ACWP?
print("\nEVMS_BUCKET counts (post mapping):")
display(df_evms.loc[df_evms["PROGRAM"] == bad_prog, "EVMS_BUCKET"].value_counts(dropna=False))

# 3) Show last 15 periods for the program (period buckets + ratios)
print("\nLast 15 period rows (program-level):")
cols = ["PERIOD_END","BCWS","BCWP","ACWP","ETC","SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD","BCWS_CUM","BCWP_CUM","ACWP_CUM","ETC_CUM"]
display(prog_period.loc[prog_period["PROGRAM"] == bad_prog, cols].sort_values("PERIOD_END").tail(15))

# 4) Show which period was picked as LSD for this program and why
g = prog_period.loc[prog_period["PROGRAM"] == bad_prog].sort_values("PERIOD_END")
lsd = pick_last_period_for_perf(g)
print("\nPicked LSD PERIOD_END:", lsd)
display(g.loc[g["PERIOD_END"] == lsd, cols])

# 5) Verify raw rows feeding the LSD period (bucket-level)
print("\nRaw EVMS rows in LSD period (program, last 200 rows):")
raw = df_evms.loc[(df_evms["PROGRAM"] == bad_prog) & (df_evms["PERIOD_END"] == lsd), ["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]]
display(raw.sort_values("DATE").tail(200))