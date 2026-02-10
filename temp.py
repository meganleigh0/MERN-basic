# ============================================================
# EVMS PIPELINE (single-cell) — FIXED for your error:
# AttributeError: 'DatetimeArray' object has no attribute 'values'
# (this happens on some pandas versions when pe is a DatetimeArray)
#
# Key changes:
# - In _assign_period_end(): convert period ends to numpy datetime64 via np.asarray(pe, dtype="datetime64[ns]")
# - Also avoid .values on DatetimeArray everywhere
#
# INPUT:  cobra_merged_df
# OUTPUT: program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues
# ============================================================

import pandas as pd
import numpy as np

def _to_datetime(s):
    return pd.to_datetime(s, errors="coerce")

def _safe_div(num, den):
    num = pd.to_numeric(num, errors="coerce")
    den = pd.to_numeric(den, errors="coerce")
    out = num / den
    return out.where((den.notna()) & (den != 0))

def _standardize_cols(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in d.columns]

    # Canonical aliases
    if "COST_SET" not in d.columns and "COSTSET" in d.columns:
        d["COST_SET"] = d["COSTSET"]
    if "SUBTEAM" in d.columns and "SUB_TEAM" not in d.columns:
        d["SUB_TEAM"] = d["SUBTEAM"]
    if "DT" in d.columns and "DATE" not in d.columns:
        d["DATE"] = d["DT"]
    return d

def _map_cost_set_to_bucket(cost_set: pd.Series) -> pd.Series:
    s = cost_set.astype(str).str.strip().str.upper()
    out = pd.Series(pd.NA, index=s.index, dtype="object")

    # exact
    out.loc[s.eq("BCWS")] = "BCWS"
    out.loc[s.eq("BCWP")] = "BCWP"
    out.loc[s.eq("ACWP")] = "ACWP"
    out.loc[s.eq("ETC")]  = "ETC"

    # variants
    out.loc[out.isna() & s.str.contains(r"\bBUDGET\b", regex=True)]   = "BCWS"
    out.loc[out.isna() & s.str.contains(r"\bPROGRESS\b", regex=True)] = "BCWP"
    out.loc[out.isna() & s.str.contains(r"\bBCWS\b", regex=True)]     = "BCWS"
    out.loc[out.isna() & s.str.contains(r"\bBCWP\b", regex=True)]     = "BCWP"
    out.loc[out.isna() & s.str.contains(r"\bACWP\b", regex=True)]     = "ACWP"
    out.loc[out.isna() & s.str.contains(r"\bETC\b", regex=True)]      = "ETC"
    return out

def _assign_period_end(dates: pd.Series, period_ends) -> pd.Series:
    """
    Assign each DATE to the next period_end >= DATE.
    Robust to pandas DatetimeArray / Index variations.
    """
    d = _to_datetime(dates)

    pe = pd.to_datetime(period_ends, errors="coerce")
    # pe can be DatetimeIndex/array; normalize to sorted unique numpy datetime64[ns]
    pe = pd.Series(pe).dropna().sort_values().unique()
    pe_arr = np.asarray(pe, dtype="datetime64[ns]")  # <--- FIX (no .values)

    if pe_arr.size == 0:
        return pd.to_datetime(pd.Series([pd.NaT] * len(d), index=d.index))

    arr = np.asarray(d, dtype="datetime64[ns]")
    idx = np.searchsorted(pe_arr, arr, side="left")

    out = pd.Series(pd.NaT, index=d.index, dtype="datetime64[ns]")
    m = (~pd.isna(d)) & (idx >= 0) & (idx < pe_arr.size)
    out.loc[m] = pe_arr[idx[m]]
    return pd.to_datetime(out)

def _pick_lsd_period_end_for_program(period_prog: pd.DataFrame, prog: str, as_of: pd.Timestamp):
    d = period_prog[period_prog["PROGRAM"] == prog][["PERIOD_END"]].dropna().sort_values("PERIOD_END")
    if d.empty:
        return pd.NaT
    pe = d["PERIOD_END"].unique()
    as_of = pd.to_datetime(as_of, errors="coerce")
    if pd.isna(as_of):
        return pe[-1]
    pe_le = pe[pe <= as_of]
    return pe_le[-1] if len(pe_le) else pe[-1]

def _last_nonnull(series: pd.Series):
    s = series.dropna()
    return s.iloc[-1] if len(s) else np.nan

def build_evms_tables(
    cobra_merged_df: pd.DataFrame,
    period_ends=None,
    year_filter: int | None = None,
    as_of_date=None,
    debug_program=None
):
    issues = []
    df0 = _standardize_cols(cobra_merged_df)

    required = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required if c not in df0.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df0.columns)}")

    df = df0.copy()
    df["DATE"] = _to_datetime(df["DATE"])
    df["HOURS"] = pd.to_numeric(df["HOURS"], errors="coerce")
    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    df["SUB_TEAM"] = df["SUB_TEAM"].astype(str).str.strip()
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip()

    if year_filter is not None:
        df = df[df["DATE"].dt.year == int(year_filter)].copy()
        if df.empty:
            issues.append(f"No rows after year_filter={year_filter}.")
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    if as_of_date is None:
        as_of = df["DATE"].max()
    else:
        as_of = pd.to_datetime(as_of_date, errors="coerce")
        if pd.isna(as_of):
            as_of = df["DATE"].max()
            issues.append("as_of_date invalid; used max DATE.")

    df["EVMS_BUCKET"] = _map_cost_set_to_bucket(df["COST_SET"])
    before = len(df)
    df = df.dropna(subset=["EVMS_BUCKET", "DATE", "HOURS"]).copy()
    dropped = before - len(df)
    if dropped:
        issues.append(f"Dropped {dropped:,} rows missing EVMS_BUCKET/DATE/HOURS.")

    if period_ends is None:
        issues.append("period_ends was None; inferred from unique DATEs (not calendar-accurate).")
        period_ends = sorted(df["DATE"].dt.normalize().unique())

    df["PERIOD_END"] = _assign_period_end(df["DATE"], period_ends)
    nat_pct = df["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"{nat_pct:.1%} rows have PERIOD_END=NaT (DATE beyond last period_end).")

    df = df.dropna(subset=["PERIOD_END"]).copy()

    # --- aggregate by period ---
    period_prog = (
        df.groupby(["PROGRAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .unstack("EVMS_BUCKET")
          .reset_index()
    )
    period_sub = (
        df.groupby(["PROGRAM", "SUB_TEAM", "PERIOD_END", "EVMS_BUCKET"], dropna=False)["HOURS"]
          .sum()
          .unstack("EVMS_BUCKET")
          .reset_index()
    )

    for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if col not in period_prog.columns: period_prog[col] = np.nan
        if col not in period_sub.columns:  period_sub[col]  = np.nan
        period_prog[col] = pd.to_numeric(period_prog[col], errors="coerce")
        period_sub[col]  = pd.to_numeric(period_sub[col], errors="coerce")

    # --- cumulative / ratios ---
    def _add_cum(df_period: pd.DataFrame, keys):
        out = df_period.sort_values(keys + ["PERIOD_END"]).copy()
        # cum of BCWS/BCWP/ACWP
        for col in ["BCWS", "BCWP", "ACWP"]:
            out[f"{col}_CUM"] = out[col].fillna(0).groupby(out[keys], dropna=False).cumsum()
        # ETC
        out["ETC_CUM"] = out["ETC"].fillna(0).groupby(out[keys], dropna=False).cumsum()

        out["SPI_LSD"] = _safe_div(out["BCWP"], out["BCWS"])
        out["CPI_LSD"] = _safe_div(out["BCWP"], out["ACWP"])
        out["SPI_CTD"] = _safe_div(out["BCWP_CUM"], out["BCWS_CUM"])
        out["CPI_CTD"] = _safe_div(out["BCWP_CUM"], out["ACWP_CUM"])
        return out

    period_prog = _add_cum(period_prog, ["PROGRAM"])
    period_sub  = _add_cum(period_sub,  ["PROGRAM", "SUB_TEAM"])

    # --- LSD per program ---
    programs = period_prog["PROGRAM"].dropna().unique().tolist()
    lsd_by_program = {p: _pick_lsd_period_end_for_program(period_prog, p, as_of) for p in programs}

    # --- build program overview (LSD row + next period hrs) ---
    pe_sorted = np.asarray(pd.to_datetime(period_ends), dtype="datetime64[ns]")
    pe_sorted = np.sort(np.unique(pe_sorted))

    rows = []
    for prog in programs:
        d = period_prog[period_prog["PROGRAM"] == prog].sort_values("PERIOD_END")
        if d.empty:
            continue

        lsd_end = lsd_by_program.get(prog, pd.NaT)
        lsd_row = d[d["PERIOD_END"] == lsd_end].tail(1)
        if lsd_row.empty:
            lsd_row = d.tail(1)
            lsd_end = lsd_row["PERIOD_END"].iloc[0]
        lsd_row = lsd_row.iloc[0]

        # fallback ratios if LSD missing
        spi_lsd = lsd_row["SPI_LSD"]
        cpi_lsd = lsd_row["CPI_LSD"]
        if pd.isna(spi_lsd): spi_lsd = _last_nonnull(d["SPI_LSD"])
        if pd.isna(cpi_lsd): cpi_lsd = _last_nonnull(d["CPI_LSD"])

        # next period end
        lsd64 = np.asarray(pd.to_datetime([lsd_end]), dtype="datetime64[ns]")[0]
        idxs = np.where(pe_sorted == lsd64)[0]
        next_end = pe_sorted[idxs[0] + 1] if (len(idxs) and idxs[0] + 1 < len(pe_sorted)) else np.datetime64("NaT")

        next_row = d[np.asarray(pd.to_datetime(d["PERIOD_END"]), dtype="datetime64[ns]") == next_end].tail(1)
        next_bcws = float(next_row["BCWS"].iloc[0]) if len(next_row) else np.nan
        next_etc  = float(next_row["ETC"].iloc[0])  if len(next_row) else np.nan

        rows.append({
            "PROGRAM": prog,
            "LAST_STATUS_PERIOD_END": pd.to_datetime(lsd_end),
            "NEXT_PERIOD_END": pd.to_datetime(next_end),

            "BCWS_LSD": lsd_row["BCWS"],
            "BCWP_LSD": lsd_row["BCWP"],
            "ACWP_LSD": lsd_row["ACWP"],
            "ETC_LSD":  lsd_row["ETC"],

            "SPI_LSD": spi_lsd,
            "CPI_LSD": cpi_lsd,

            "BCWS_CTD": lsd_row["BCWS_CUM"],
            "BCWP_CTD": lsd_row["BCWP_CUM"],
            "ACWP_CTD": lsd_row["ACWP_CUM"],
            "SPI_CTD":  lsd_row["SPI_CTD"],
            "CPI_CTD":  lsd_row["CPI_CTD"],

            "NEXT_PERIOD_BCWS_HRS": next_bcws,
            "NEXT_PERIOD_ETC_HRS":  next_etc,
        })

    program_overview_evms = pd.DataFrame(rows).sort_values("PROGRAM").reset_index(drop=True)

    # --- subteam spi/cpi (LSD rows aligned to program LSD) ---
    if not period_sub.empty:
        period_sub["LSD_PERIOD_END"] = period_sub["PROGRAM"].map(lsd_by_program)
        sub_lsd = period_sub[period_sub["PERIOD_END"] == period_sub["LSD_PERIOD_END"]].copy()

        # fallback ratio per (PROGRAM,SUB_TEAM)
        def _fallback(group, col):
            v = group.loc[group["PERIOD_END"] == group["LSD_PERIOD_END"].iloc[0], col]
            v = v.iloc[0] if len(v) else np.nan
            return v if not pd.isna(v) else _last_nonnull(group.sort_values("PERIOD_END")[col])

        fb = period_sub.groupby(["PROGRAM","SUB_TEAM"], dropna=False).apply(
            lambda g: pd.Series({
                "SPI_LSD_FIX": _fallback(g, "SPI_LSD"),
                "CPI_LSD_FIX": _fallback(g, "CPI_LSD"),
            })
        ).reset_index()

        subteam_spi_cpi = (
            sub_lsd[["PROGRAM","SUB_TEAM","LSD_PERIOD_END","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]]
            .rename(columns={"LSD_PERIOD_END":"LAST_STATUS_PERIOD_END"})
            .merge(fb, on=["PROGRAM","SUB_TEAM"], how="left")
        )
        subteam_spi_cpi["SPI_LSD"] = subteam_spi_cpi["SPI_LSD"].fillna(subteam_spi_cpi["SPI_LSD_FIX"])
        subteam_spi_cpi["CPI_LSD"] = subteam_spi_cpi["CPI_LSD"].fillna(subteam_spi_cpi["CPI_LSD_FIX"])
        subteam_spi_cpi = subteam_spi_cpi.drop(columns=["SPI_LSD_FIX","CPI_LSD_FIX"]).sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
    else:
        subteam_spi_cpi = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","LAST_STATUS_PERIOD_END","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"])

    # --- BAC/EAC/VAC by subteam ---
    if not period_sub.empty:
        sub_lsd = period_sub[period_sub["PERIOD_END"] == period_sub["LSD_PERIOD_END"]].copy() if "LSD_PERIOD_END" in period_sub.columns else pd.DataFrame()
        if not sub_lsd.empty:
            sub_lsd["BAC_HRS"] = sub_lsd["BCWS_CUM"]
            sub_lsd["EAC_HRS"] = sub_lsd["ACWP_CUM"] + sub_lsd["ETC"]
            sub_lsd["VAC_HRS"] = sub_lsd["BAC_HRS"] - sub_lsd["EAC_HRS"]
            subteam_bac_eac_vac = sub_lsd[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
        else:
            subteam_bac_eac_vac = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"])
    else:
        subteam_bac_eac_vac = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"])

    # --- program hours forecast ---
    if not program_overview_evms.empty:
        phf = program_overview_evms.copy()
        phf["DEMAND_HRS_CTD"] = phf["BCWS_CTD"]
        phf["ACTUAL_HRS_CTD"] = phf["ACWP_CTD"]
        phf["PCT_VARIANCE_CTD"] = _safe_div(phf["ACTUAL_HRS_CTD"] - phf["DEMAND_HRS_CTD"], phf["DEMAND_HRS_CTD"])
        phf["NEXT_PERIOD_N"] = np.nan
        program_hours_forecast = phf[[
            "PROGRAM","LAST_STATUS_PERIOD_END","NEXT_PERIOD_END",
            "DEMAND_HRS_CTD","ACTUAL_HRS_CTD","PCT_VARIANCE_CTD",
            "NEXT_PERIOD_BCWS_HRS","NEXT_PERIOD_ETC_HRS","NEXT_PERIOD_N"
        ]].sort_values("PROGRAM").reset_index(drop=True)
    else:
        program_hours_forecast = pd.DataFrame(columns=[
            "PROGRAM","LAST_STATUS_PERIOD_END","NEXT_PERIOD_END",
            "DEMAND_HRS_CTD","ACTUAL_HRS_CTD","PCT_VARIANCE_CTD",
            "NEXT_PERIOD_BCWS_HRS","NEXT_PERIOD_ETC_HRS","NEXT_PERIOD_N"
        ])

    # --- optional deep debug ---
    if debug_program is not None:
        p = str(debug_program).strip()
        print(f"\n=== DEBUG PROGRAM: {p} ===")
        d_raw = df[df["PROGRAM"] == p].copy()
        print("Rows:", len(d_raw))
        print("Date range:", d_raw["DATE"].min(), "to", d_raw["DATE"].max())
        print("\nTop COST_SET:")
        display(d_raw["COST_SET"].value_counts().head(20).to_frame("count"))
        print("\nEVMS_BUCKET counts:")
        display(d_raw["EVMS_BUCKET"].value_counts(dropna=False).to_frame("count"))

        d_per = period_prog[period_prog["PROGRAM"] == p].sort_values("PERIOD_END").copy()
        print("\nLast 15 program periods:")
        display(d_per[["PERIOD_END","BCWS","BCWP","ACWP","ETC","SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD"]].tail(15))

        lsd_end = lsd_by_program.get(p, pd.NaT)
        print("\nPicked LSD PERIOD_END:", lsd_end)
        d_lsd = df[(df["PROGRAM"] == p) & (df["PERIOD_END"] == lsd_end)].sort_values("DATE").tail(120)
        print("\nRaw rows inside LSD (last 120):")
        display(d_lsd[["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]])

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues


# -----------------------------
# RUN
# -----------------------------
PERIOD_ENDS_2026 = None  # put your real calendar list here if you have it

program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    period_ends=PERIOD_ENDS_2026,
    year_filter=2026,
    as_of_date=None,
    debug_program=None
)

print("ISSUES:")
for i in issues:
    print("-", i)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))