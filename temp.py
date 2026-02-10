# ============================================================
# EVMS (Cobra) Pipeline — single-cell, robust, debug-friendly
# Works with COST-SET (hyphen) and COST_SET (underscore)
# ============================================================

import pandas as pd
import numpy as np

# ----------------------------
# 1) Configure period ends (2026)
# ----------------------------
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
    "2026-12-27",
]).sort_values()

# ----------------------------
# 2) Column canonicalization
# ----------------------------
def canonicalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make column names consistent so downstream logic can rely on:
    PROGRAM, SUB_TEAM, COST_SET, DATE, HOURS
    """
    out = df.copy()
    # normalize column names: strip, upper, collapse spaces
    cols = [str(c).strip().upper() for c in out.columns]
    out.columns = cols

    # rename common variants -> canonical
    rename_map = {
        "COST-SET": "COST_SET",
        "COST SET": "COST_SET",
        "COSTSET": "COST_SET",

        "SUBTEAM": "SUB_TEAM",
        "SUB-TEAM": "SUB_TEAM",
        "SUB TEAM": "SUB_TEAM",

        "PROGRAM ": "PROGRAM",
        "HRS": "HOURS",
        "HOUR": "HOURS",
    }
    out = out.rename(columns={k: v for k, v in rename_map.items() if k in out.columns})

    return out

def _normalize_program(x):
    if pd.isna(x): return pd.NA
    return str(x).strip()

def _normalize_subteam(x):
    if pd.isna(x): return pd.NA
    return str(x).strip()

def _normalize_costset(x):
    if pd.isna(x): return pd.NA
    return str(x).strip().upper()

# ----------------------------
# 3) EVMS bucket mapping (dtype-safe)
# ----------------------------
def map_cost_set_to_bucket(cost_set_series: pd.Series) -> pd.Series:
    s = cost_set_series.astype("string").str.strip().str.upper()
    out = pd.Series(pd.NA, index=s.index, dtype="object")

    # exact bucket names
    exact = {"BCWS", "BCWP", "ACWP", "ETC"}
    out.loc[s.isin(exact)] = s.loc[s.isin(exact)].astype("object")

    unm = out.isna()
    # conservative fallbacks (only if still unmapped)
    out.loc[unm & s.str.contains(r"\bBUDGET\b", regex=True, na=False)]   = "BCWS"
    out.loc[unm & s.str.contains(r"\bPROGRESS\b", regex=True, na=False)] = "BCWP"
    out.loc[unm & s.str.contains(r"\bBCWS\b", regex=True, na=False)]     = "BCWS"
    out.loc[unm & s.str.contains(r"\bBCWP\b", regex=True, na=False)]     = "BCWP"
    out.loc[unm & s.str.contains(r"\bACWP\b", regex=True, na=False)]     = "ACWP"
    out.loc[unm & s.str.contains(r"\bETC\b", regex=True, na=False)]      = "ETC"

    return out

# ----------------------------
# 4) Period assignment (IndexError-safe)
# ----------------------------
def assign_period_end(dates: pd.Series, period_ends: pd.DatetimeIndex) -> pd.Series:
    pe = pd.to_datetime(pd.Series(period_ends)).sort_values().to_numpy(dtype="datetime64[ns]")
    d = pd.to_datetime(dates, errors="coerce").to_numpy(dtype="datetime64[ns]")

    idx = np.searchsorted(pe, d, side="left")

    out = np.full(len(d), np.datetime64("NaT"), dtype="datetime64[ns]")
    ok = idx < len(pe)
    out[ok] = pe[idx[ok]]
    return pd.to_datetime(out)

def safe_div(numer: pd.Series, denom: pd.Series) -> pd.Series:
    numer = pd.to_numeric(numer, errors="coerce")
    denom = pd.to_numeric(denom, errors="coerce")
    return numer / denom.replace({0: np.nan})

def pick_lsd_period_end(df_year: pd.DataFrame, as_of_date=None) -> pd.Timestamp:
    if as_of_date is None:
        as_of_date = df_year["DATE"].max()
    else:
        as_of_date = pd.to_datetime(as_of_date)

    pe = df_year["PERIOD_END"].dropna().sort_values().unique()
    if len(pe) == 0:
        return pd.NaT

    pe = pd.to_datetime(pe)
    idx = np.searchsorted(pe.to_numpy(dtype="datetime64[ns]"), np.datetime64(as_of_date), side="left")
    if idx >= len(pe):
        return pd.Timestamp(pe[-1])
    return pd.Timestamp(pe[idx])

def next_period_end(lsd: pd.Timestamp, period_ends: pd.DatetimeIndex) -> pd.Timestamp:
    pe = pd.to_datetime(pd.Series(period_ends)).sort_values()
    if pd.isna(lsd):
        return pd.NaT
    idx = np.searchsorted(pe.to_numpy(dtype="datetime64[ns]"), np.datetime64(lsd), side="right")
    if idx >= len(pe):
        return pd.NaT
    return pd.Timestamp(pe.iloc[idx])

# ----------------------------
# 5) Main build
# ----------------------------
def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    period_ends=PERIOD_ENDS_2026,
    year_filter: int = 2026,
    as_of_date=None,
    debug_program: str | None = None,
):
    issues = []

    df0 = canonicalize_columns(cobra_merged_df)

    required = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required if c not in df0.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df0.columns)}")

    df = df0.copy()
    df["PROGRAM"]  = df["PROGRAM"].map(_normalize_program)
    df["SUB_TEAM"] = df["SUB_TEAM"].map(_normalize_subteam)
    df["COST_SET"] = df["COST_SET"].map(_normalize_costset)

    df["DATE"]  = pd.to_datetime(df["DATE"], errors="coerce")
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")

    df = df.dropna(subset=["PROGRAM", "DATE", "HOURS", "COST_SET"]).copy()

    # year filter (prevents 2028 from breaking next-period logic)
    df = df[df["DATE"].dt.year == int(year_filter)].copy()
    if df.empty:
        issues.append(f"No rows after year_filter={year_filter}. Check DATE parsing/filter.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    df["EVMS_BUCKET"] = map_cost_set_to_bucket(df["COST_SET"])
    unmapped_pct = df["EVMS_BUCKET"].isna().mean()
    if unmapped_pct > 0:
        issues.append(f"{unmapped_pct:.2%} of rows have unmapped COST_SET -> EVMS_BUCKET.")

    df = df.dropna(subset=["EVMS_BUCKET"]).copy()

    df["PERIOD_END"] = assign_period_end(df["DATE"], pd.DatetimeIndex(period_ends))
    nat_pct = df["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"{nat_pct:.2%} of rows could not be assigned a PERIOD_END (DATE beyond calendar?).")

    df = df.dropna(subset=["PERIOD_END"]).copy()

    def pivot_period(base: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        g = (base.groupby(keys + ["PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
                 .sum()
                 .reset_index())
        p = (g.pivot_table(index=keys + ["PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum")
               .reset_index())
        for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
            if col not in p.columns:
                p[col] = np.nan
            p[col] = pd.to_numeric(p[col], errors="coerce")
        return p

    period_prog = pivot_period(df, ["PROGRAM"])
    period_sub  = pivot_period(df, ["PROGRAM", "SUB_TEAM"])

    def add_cum_and_ctd(p: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
        out = p.sort_values(keys + ["PERIOD_END"]).copy()
        for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
            out[f"{col}_CUM"] = out.groupby(keys, dropna=False)[col].cumsum(min_count=1)
            out[f"{col}_CTD"] = out.groupby(keys, dropna=False)[f"{col}_CUM"].diff().fillna(out[f"{col}_CUM"])
        out["SPI_LSD"] = safe_div(out["BCWP"], out["BCWS"])
        out["CPI_LSD"] = safe_div(out["BCWP"], out["ACWP"])
        out["SPI_CTD"] = safe_div(out["BCWP_CUM"], out["BCWS_CUM"])
        out["CPI_CTD"] = safe_div(out["BCWP_CUM"], out["ACWP_CUM"])
        return out

    period_prog = add_cum_and_ctd(period_prog, ["PROGRAM"])
    period_sub  = add_cum_and_ctd(period_sub,  ["PROGRAM", "SUB_TEAM"])

    lsd_pe  = pick_lsd_period_end(df, as_of_date=as_of_date)
    next_pe = next_period_end(lsd_pe, pd.DatetimeIndex(period_ends))

    if pd.isna(lsd_pe):
        issues.append("Could not determine LSD PERIOD_END (no PERIOD_END values after filtering).")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    # Program overview at LSD
    prog_lsd = period_prog[period_prog["PERIOD_END"] == lsd_pe].copy()
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

    # Subteam SPI/CPI at LSD
    sub_lsd = period_sub[period_sub["PERIOD_END"] == lsd_pe].copy()
    subteam_spi_cpi = sub_lsd[[
        "PROGRAM", "SUB_TEAM", "PERIOD_END",
        "SPI_LSD", "CPI_LSD", "SPI_CTD", "CPI_CTD",
        "BCWS", "BCWP", "ACWP", "ETC"
    ]].rename(columns={"PERIOD_END": "LAST_STATUS_PERIOD_END"}).reset_index(drop=True)

    # BAC/EAC/VAC at LSD (hours)
    bac_eac = period_sub[period_sub["PERIOD_END"] == lsd_pe].copy()
    bac_eac["BAC_HRS"] = bac_eac["BCWS_CUM"]
    bac_eac["EAC_HRS"] = bac_eac["ACWP_CUM"] + bac_eac["ETC_CUM"]
    bac_eac["VAC_HRS"] = bac_eac["BAC_HRS"] - bac_eac["EAC_HRS"]
    subteam_bac_eac_vac = bac_eac[["PROGRAM", "SUB_TEAM", "BAC_HRS", "EAC_HRS", "VAC_HRS"]].reset_index(drop=True)

    # Program hours forecast
    prog_ctd = period_prog[period_prog["PERIOD_END"] == lsd_pe].copy()
    prog_ctd["DEMAND_HRS_CTD"] = prog_ctd["BCWS_CUM"]
    prog_ctd["ACTUAL_HRS_CTD"] = prog_ctd["ACWP_CUM"]
    prog_ctd["PCT_VARIANCE_CTD"] = safe_div(
        prog_ctd["ACTUAL_HRS_CTD"] - prog_ctd["DEMAND_HRS_CTD"],
        prog_ctd["DEMAND_HRS_CTD"]
    )

    next_rows = period_prog[period_prog["PERIOD_END"] == next_pe][["PROGRAM", "BCWS", "ETC"]].copy()
    next_rows = next_rows.rename(columns={"BCWS": "NEXT_PERIOD_BCWS_HRS", "ETC": "NEXT_PERIOD_ETC_HRS"})

    program_hours_forecast = prog_ctd[[
        "PROGRAM", "PERIOD_END", "DEMAND_HRS_CTD", "ACTUAL_HRS_CTD", "PCT_VARIANCE_CTD"
    ]].rename(columns={"PERIOD_END": "LAST_STATUS_PERIOD_END"}).merge(
        next_rows, on="PROGRAM", how="left"
    )
    program_hours_forecast["NEXT_PERIOD_END"] = next_pe

    if not pd.isna(next_pe):
        df_next = df[df["PERIOD_END"] == next_pe].copy()
        next_n = (df_next.groupby("PROGRAM")["DATE"].nunique().reset_index(name="NEXT_PERIOD_N"))
        program_hours_forecast = program_hours_forecast.merge(next_n, on="PROGRAM", how="left")
    else:
        program_hours_forecast["NEXT_PERIOD_N"] = np.nan
        issues.append("No next period end exists after LSD (LSD is last period in calendar).")

    # Debug block (optional)
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
# RUN
# ============================================================
program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    period_ends=PERIOD_ENDS_2026,
    year_filter=2026,
    as_of_date=None,
    debug_program=None,  # set to "ABRAMS_22" if needed
)

print("ISSUES:")
for i in issues:
    print("-", i)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))