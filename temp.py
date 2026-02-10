import numpy as np
import pandas as pd

# ============================================================
# 1) CALENDAR: period ends for 2026
#    IMPORTANT: keep this list aligned with your close dates.
#    (From your screenshots, last close looks like 2026-12-27)
# ============================================================
PERIOD_ENDS_2026 = pd.to_datetime([
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
    "2026-11-30",
    "2026-12-27",
])

# ============================================================
# 2) Standardization helpers
# ============================================================
def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out.columns = [str(c).strip().upper().replace(" ", "_").replace("-", "_") for c in out.columns]
    # common aliases
    rename_map = {
        "COST-SET": "COST_SET",
        "COSTSET": "COST_SET",
        "COST_SET_NAME": "COST_SET",
        "HRS": "HOURS",
    }
    for k, v in rename_map.items():
        if k in out.columns and v not in out.columns:
            out = out.rename(columns={k: v})
    return out

def _clean_types(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    # DATE
    if "DATE" in out.columns:
        out["DATE"] = pd.to_datetime(out["DATE"], errors="coerce")

    # HOURS
    if "HOURS" in out.columns:
        out["HOURS"] = pd.to_numeric(out["HOURS"], errors="coerce")

    # PROGRAM
    if "PROGRAM" in out.columns:
        out["PROGRAM"] = out["PROGRAM"].astype(str).str.strip().str.upper()

    # SUB_TEAM
    # keep as string (you have numeric-looking subteams)
    if "SUB_TEAM" in out.columns:
        out["SUB_TEAM"] = out["SUB_TEAM"].astype(str).str.strip()

    # COST_SET
    if "COST_SET" in out.columns:
        out["COST_SET"] = out["COST_SET"].astype(str).str.strip().str.upper()

    return out

def _ensure_required_cols(df: pd.DataFrame, required: list[str]) -> list[str]:
    missing = [c for c in required if c not in df.columns]
    return missing

# ============================================================
# 3) COST_SET -> EVMS_BUCKET mapping (matches your data)
#    Your debug screenshot showed:
#      BUDGET   -> BCWS
#      PROGRESS -> BCWP
#      ACWP_HRS -> ACWP
#    Also supports ETC if it exists.
# ============================================================
def _map_cost_set_to_bucket(cost_set: pd.Series) -> pd.Series:
    s = cost_set.astype(str).str.strip().str.upper()

    # produce object dtype so we never trigger dtype promotion errors
    out = pd.Series(index=s.index, dtype="object")

    # exact & conservative mapping (expand as needed)
    out.loc[s.eq("BUDGET")] = "BCWS"
    out.loc[s.eq("PROGRESS")] = "BCWP"
    out.loc[s.eq("ACWP_HRS")] = "ACWP"
    out.loc[s.eq("ACWP HRS")] = "ACWP"
    out.loc[s.eq("ACWP")] = "ACWP"
    out.loc[s.eq("ETC")] = "ETC"
    out.loc[s.eq("EAC")] = "EAC"   # rarely present as a bucket; kept for safety

    # light fallback (only if still missing)
    missing = out.isna()
    if missing.any():
        ss = s[missing]
        out.loc[missing & ss.str.contains(r"\bBUDGET\b", regex=True)] = "BCWS"
        out.loc[missing & ss.str.contains(r"\bPROGRESS\b", regex=True)] = "BCWP"
        out.loc[missing & ss.str.contains(r"\bBCWS\b", regex=True)] = "BCWS"
        out.loc[missing & ss.str.contains(r"\bBCWP\b", regex=True)] = "BCWP"
        out.loc[missing & ss.str.contains(r"\bACWP\b", regex=True)] = "ACWP"
        out.loc[missing & ss.str.contains(r"\bETC\b", regex=True)] = "ETC"

    return out

# ============================================================
# 4) Safe period end assignment (fixes IndexError)
#    Assign DATE -> first PERIOD_END >= DATE
# ============================================================
def _assign_period_end(dates: pd.Series, period_ends: pd.DatetimeIndex) -> pd.Series:
    pe = pd.to_datetime(period_ends).sort_values().to_numpy(dtype="datetime64[ns]")
    d = pd.to_datetime(dates).to_numpy(dtype="datetime64[ns]")

    idx = np.searchsorted(pe, d, side="left")

    out = np.empty(len(d), dtype="datetime64[ns]")
    out[:] = np.datetime64("NaT")

    valid = idx < len(pe)
    out[valid] = pe[idx[valid]]

    return pd.to_datetime(out)

# ============================================================
# 5) Build period totals (PROGRAM and PROGRAM+SUB_TEAM)
# ============================================================
def _build_period_totals(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    issues = []

    req = ["PROGRAM", "DATE", "COST_SET", "HOURS"]
    missing = _ensure_required_cols(df, req)
    if missing:
        return pd.DataFrame(), [f"Missing required columns: {missing}"]

    d = df.dropna(subset=["DATE", "HOURS"]).copy()
    d["EVMS_BUCKET"] = _map_cost_set_to_bucket(d["COST_SET"])
    before = len(d)
    d = d.dropna(subset=["EVMS_BUCKET"]).copy()
    dropped = before - len(d)
    if dropped:
        issues.append(f"Dropped {dropped} rows with unmapped COST_SET.")

    # Assign PERIOD_END
    d["PERIOD_END"] = _assign_period_end(d["DATE"], PERIOD_ENDS_2026)

    # Some rows may fall after last period_end (NaT) -> drop but report
    nat_pct = d["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"{nat_pct:.2%} of rows have DATE > last PERIOD_END (PERIOD_END=NaT) and were excluded.")
    d = d.dropna(subset=["PERIOD_END"]).copy()

    # Pivot to period totals
    period_prog = (
        d.pivot_table(index=["PROGRAM", "PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum")
         .reset_index()
    )
    period_prog.columns.name = None
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in period_prog.columns:
            period_prog[c] = np.nan

    period_sub = (
        d.pivot_table(index=["PROGRAM", "SUB_TEAM", "PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum")
         .reset_index()
    )
    period_sub.columns.name = None
    for c in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if c not in period_sub.columns:
            period_sub[c] = np.nan

    return (period_prog, period_sub), issues

# ============================================================
# 6) CTD build (cumsums) + LSD selection
# ============================================================
def _add_ctd(df_period: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    out = df_period.sort_values(keys + ["PERIOD_END"]).copy()
    for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
        out[f"{col}_CTD"] = out.groupby(keys, dropna=False)[col].cumsum(min_count=1)
    return out

def _safe_div(n, d):
    n = pd.to_numeric(n, errors="coerce")
    d = pd.to_numeric(d, errors="coerce")
    out = n / d
    out = out.where(d != 0)
    return out

def _pick_lsd_period_program(period_prog_ctd: pd.DataFrame) -> pd.DataFrame:
    """
    LSD = latest PERIOD_END where we have usable data.
    "Usable" = BCWS > 0 and (BCWP or ACWP present).
    """
    p = period_prog_ctd.copy()
    p["has_bcws"] = pd.to_numeric(p["BCWS"], errors="coerce").fillna(0) > 0
    p["has_work_or_cost"] = pd.to_numeric(p["BCWP"], errors="coerce").notna() | pd.to_numeric(p["ACWP"], errors="coerce").notna()
    usable = p[p["has_bcws"] & p["has_work_or_cost"]].copy()

    # if nothing matches, fall back to max period_end per program
    if usable.empty:
        return p.groupby("PROGRAM", as_index=False)["PERIOD_END"].max().rename(columns={"PERIOD_END":"LSD_PERIOD_END"})

    lsd = usable.groupby("PROGRAM", as_index=False)["PERIOD_END"].max().rename(columns={"PERIOD_END":"LSD_PERIOD_END"})
    return lsd

# ============================================================
# 7) Output tables
# ============================================================
def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    year_filter: int = 2026,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, list[str]]:
    issues = []

    df = _clean_types(_standardize_cols(cobra_merged_df))

    # filter to year (prevents 2028 rows breaking next-period logic)
    if "DATE" in df.columns:
        df = df[df["DATE"].dt.year == year_filter].copy()
    else:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), ["Missing DATE column."])

    # build period totals
    (period_prog, period_sub), issues0 = _build_period_totals(df)
    issues += issues0
    if period_prog.empty:
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    # CTD
    prog_ctd = _add_ctd(period_prog, keys=["PROGRAM"])
    sub_ctd  = _add_ctd(period_sub, keys=["PROGRAM", "SUB_TEAM"])

    # Pick LSD per program
    lsd_map = _pick_lsd_period_program(prog_ctd)

    # --- PROGRAM OVERVIEW (LSD row per program + CTD/LSD metrics) ---
    prog = prog_ctd.merge(lsd_map, on="PROGRAM", how="left")
    prog_lsd = prog[prog["PERIOD_END"] == prog["LSD_PERIOD_END"]].copy()

    # LSD period deltas are just the period values in that row
    prog_lsd["BCWS_LSD"] = prog_lsd["BCWS"]
    prog_lsd["BCWP_LSD"] = prog_lsd["BCWP"]
    prog_lsd["ACWP_LSD"] = prog_lsd["ACWP"]
    prog_lsd["ETC_LSD"]  = prog_lsd["ETC"]

    # SPI/CPI
    prog_lsd["SPI_CTD"] = _safe_div(prog_lsd["BCWP_CTD"], prog_lsd["BCWS_CTD"])
    prog_lsd["CPI_CTD"] = _safe_div(prog_lsd["BCWP_CTD"], prog_lsd["ACWP_CTD"])
    prog_lsd["SPI_LSD"] = _safe_div(prog_lsd["BCWP_LSD"], prog_lsd["BCWS_LSD"])
    prog_lsd["CPI_LSD"] = _safe_div(prog_lsd["BCWP_LSD"], prog_lsd["ACWP_LSD"])

    program_overview_evms = prog_lsd[[
        "PROGRAM",
        "LSD_PERIOD_END",
        "BCWS_CTD","BCWP_CTD","ACWP_CTD",
        "BCWS_LSD","BCWP_LSD","ACWP_LSD",
        "ETC_LSD",
        "SPI_CTD","CPI_CTD","SPI_LSD","CPI_LSD",
    ]].sort_values("PROGRAM").reset_index(drop=True)

    # --- SUBTEAM SPI/CPI (use each program’s LSD period) ---
    sub = sub_ctd.merge(lsd_map, on="PROGRAM", how="left")
    sub_lsd = sub[sub["PERIOD_END"] == sub["LSD_PERIOD_END"]].copy()

    sub_lsd["BCWS_LSD"] = sub_lsd["BCWS"]
    sub_lsd["BCWP_LSD"] = sub_lsd["BCWP"]
    sub_lsd["ACWP_LSD"] = sub_lsd["ACWP"]

    sub_lsd["SPICTD"] = _safe_div(sub_lsd["BCWP_CTD"], sub_lsd["BCWS_CTD"])
    sub_lsd["CPICTD"] = _safe_div(sub_lsd["BCWP_CTD"], sub_lsd["ACWP_CTD"])
    sub_lsd["SPILSD"] = _safe_div(sub_lsd["BCWP_LSD"], sub_lsd["BCWS_LSD"])
    sub_lsd["CPILSD"] = _safe_div(sub_lsd["BCWP_LSD"], sub_lsd["ACWP_LSD"])

    subteam_spi_cpi = sub_lsd[[
        "PROGRAM","SUB_TEAM","LSD_PERIOD_END",
        "SPILSD","SPICTD","CPILSD","CPICTD"
    ]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

    # --- BAC / EAC / VAC (hours) ---
    # BAC = max cumulative BCWS across the year (or whatever range exists)
    bac_by_sub = (
        sub_ctd.groupby(["PROGRAM","SUB_TEAM"], as_index=False)["BCWS_CTD"]
        .max()
        .rename(columns={"BCWS_CTD":"BAC_HRS"})
    )

    # EAC = ACWP_CTD (at LSD) + ETC_LSD (at LSD, point estimate)
    eac_by_sub = sub_lsd[["PROGRAM","SUB_TEAM","ACWP_CTD","ETC"]].copy()
    eac_by_sub["ETC"] = pd.to_numeric(eac_by_sub["ETC"], errors="coerce").fillna(0)
    eac_by_sub["EAC_HRS"] = pd.to_numeric(eac_by_sub["ACWP_CTD"], errors="coerce").fillna(0) + eac_by_sub["ETC"]
    eac_by_sub = eac_by_sub[["PROGRAM","SUB_TEAM","EAC_HRS"]]

    subteam_bac_eac_vac = (
        bac_by_sub.merge(eac_by_sub, on=["PROGRAM","SUB_TEAM"], how="left")
    )
    subteam_bac_eac_vac["VAC_HRS"] = subteam_bac_eac_vac["BAC_HRS"] - subteam_bac_eac_vac["EAC_HRS"]
    subteam_bac_eac_vac = subteam_bac_eac_vac.sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)

    # --- PROGRAM HOURS FORECAST (Demand/Actual/%Var + Next period BCWS/ETC) ---
    # Demand Hours (CTD) = BCWS_CTD at LSD
    # Actual Hours (CTD) = ACWP_CTD at LSD
    hours = prog_lsd[["PROGRAM","LSD_PERIOD_END","BCWS_CTD","ACWP_CTD"]].copy()
    hours = hours.rename(columns={"BCWS_CTD":"DEMAND_HRS_CTD","ACWP_CTD":"ACTUAL_HRS_CTD"})
    hours["PCT_VARIANCE_CTD"] = _safe_div(hours["ACTUAL_HRS_CTD"] - hours["DEMAND_HRS_CTD"], hours["DEMAND_HRS_CTD"])

    # Next period end
    pe_sorted = pd.to_datetime(PERIOD_ENDS_2026).sort_values().to_list()
    pe_index = {d: i for i, d in enumerate(pe_sorted)}

    def _next_pe(curr):
        if pd.isna(curr): return pd.NaT
        i = pe_index.get(pd.to_datetime(curr), None)
        if i is None or i+1 >= len(pe_sorted): return pd.NaT
        return pe_sorted[i+1]

    hours["NEXT_PERIOD_END"] = hours["LSD_PERIOD_END"].apply(_next_pe)

    # Next period BCWS/ETC are period totals in that next period (not CTD)
    next_vals = period_prog[["PROGRAM","PERIOD_END","BCWS","ETC"]].copy()
    next_vals = next_vals.rename(columns={"PERIOD_END":"NEXT_PERIOD_END",
                                          "BCWS":"NEXT_PERIOD_BCWS_HRS",
                                          "ETC":"NEXT_PERIOD_ETC_HRS"})
    program_hours_forecast = hours.merge(next_vals, on=["PROGRAM","NEXT_PERIOD_END"], how="left")

    # If next period exists but has no rows, leave NaN (not 0) to avoid “fake zeros”
    program_hours_forecast = program_hours_forecast[[
        "PROGRAM","LSD_PERIOD_END","NEXT_PERIOD_END",
        "DEMAND_HRS_CTD","ACTUAL_HRS_CTD","PCT_VARIANCE_CTD",
        "NEXT_PERIOD_BCWS_HRS","NEXT_PERIOD_ETC_HRS"
    ]].sort_values("PROGRAM").reset_index(drop=True)

    # Some high-signal issue summaries
    if program_overview_evms[["BCWP_CTD","ACWP_CTD"]].isna().any(axis=1).any():
        issues.append("Some programs have missing BCWP_CTD or ACWP_CTD at LSD. Use debug cell to inspect COST_SET coverage by period.")

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues


# ============================================================
# 8) Debug helper: explain WHY a program is missing values
# ============================================================
def debug_program(cobra_merged_df: pd.DataFrame, program: str, year_filter: int = 2026, nrows: int = 50):
    df = _clean_types(_standardize_cols(cobra_merged_df))
    df = df[df["DATE"].dt.year == year_filter].copy()
    df = df[df["PROGRAM"].astype(str).str.upper() == str(program).strip().upper()].copy()

    print(f"\nInvestigating program: {program}")
    if df.empty:
        print("No rows for this program in the selected year.")
        return

    print("\nTop COST_SET values (raw):")
    display(df["COST_SET"].value_counts().head(25).to_frame("count"))

    df["EVMS_BUCKET"] = _map_cost_set_to_bucket(df["COST_SET"])
    print("\nEVMS_BUCKET counts (post mapping):")
    display(df["EVMS_BUCKET"].value_counts(dropna=False).to_frame("count"))

    df["PERIOD_END"] = _assign_period_end(df["DATE"], PERIOD_ENDS_2026)
    print("\nDATE range:", df["DATE"].min(), "to", df["DATE"].max())
    print("Last calendar period_end:", PERIOD_ENDS_2026.max())
    print("Rows with PERIOD_END=NaT:", df["PERIOD_END"].isna().sum())

    # Show latest periods with totals
    d = df.dropna(subset=["PERIOD_END","EVMS_BUCKET","HOURS"]).copy()
    piv = (
        d.pivot_table(index=["PERIOD_END"], columns="EVMS_BUCKET", values="HOURS", aggfunc="sum")
         .sort_index()
    )
    print("\nLast 15 periods (program totals):")
    display(piv.tail(15))

    # Show raw rows near last period
    last_pe = d["PERIOD_END"].max()
    print("\nPicked LAST PERIOD_END in data:", last_pe)
    raw_last = d[d["PERIOD_END"] == last_pe][["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]].sort_values("DATE").tail(nrows)
    print(f"\nRaw rows in last period (last {nrows}):")
    display(raw_last)
    