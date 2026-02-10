# ============================================================
# EVMS PIPELINE (ONE CELL) — fixes "datetime64 values must have a unit specified"
# ============================================================

import pandas as pd
import numpy as np

def _to_dt(x):
    return pd.to_datetime(x, errors="coerce")

def _num(x):
    return pd.to_numeric(x, errors="coerce")

def _safe_div(a, b):
    a = _num(a)
    b = _num(b)
    out = a / b
    return out.where((b.notna()) & (b != 0))

def _std_cols(df):
    d = df.copy()
    d.columns = [c.strip().upper().replace(" ", "_").replace("-", "_") for c in d.columns]
    if "COST_SET" not in d.columns and "COSTSET" in d.columns:
        d["COST_SET"] = d["COSTSET"]
    if "SUBTEAM" in d.columns and "SUB_TEAM" not in d.columns:
        d["SUB_TEAM"] = d["SUBTEAM"]
    if "DT" in d.columns and "DATE" not in d.columns:
        d["DATE"] = d["DT"]
    return d

def _map_bucket(cost_set):
    s = cost_set.astype(str).str.strip().str.upper()
    out = pd.Series(pd.NA, index=s.index, dtype="object")

    out.loc[s.eq("BCWS")] = "BCWS"
    out.loc[s.eq("BCWP")] = "BCWP"
    out.loc[s.eq("ACWP")] = "ACWP"
    out.loc[s.eq("ETC")]  = "ETC"

    out.loc[out.isna() & s.str.contains(r"\bBUDGET\b", regex=True)]   = "BCWS"
    out.loc[out.isna() & s.str.contains(r"\bPROGRESS\b", regex=True)] = "BCWP"

    out.loc[out.isna() & s.str.contains(r"\bBCWS\b", regex=True)] = "BCWS"
    out.loc[out.isna() & s.str.contains(r"\bBCWP\b", regex=True)] = "BCWP"
    out.loc[out.isna() & s.str.contains(r"\bACWP\b", regex=True)] = "ACWP"
    out.loc[out.isna() & s.str.contains(r"\bETC\b",  regex=True)] = "ETC"

    return out

def _infer_period_ends_from_data(df):
    return sorted(df["DATE"].dt.normalize().dropna().unique())

def _assign_period_end(dates, period_ends):
    d = _to_dt(dates)
    pe = pd.Series(_to_dt(period_ends)).dropna().sort_values().unique()
    if len(pe) == 0:
        return pd.to_datetime(pd.Series([pd.NaT] * len(d), index=d.index))

    pe_arr = np.asarray(pe, dtype="datetime64[ns]")
    arr = np.asarray(d, dtype="datetime64[ns]")
    idx = np.searchsorted(pe_arr, arr, side="left")

    out = pd.Series(pd.NaT, index=d.index, dtype="datetime64[ns]")
    m = (~pd.isna(d)) & (idx >= 0) & (idx < len(pe_arr))
    out.loc[m] = pe_arr[idx[m]]
    return pd.to_datetime(out)

def _last_nonnull(s):
    s2 = s.dropna()
    return s2.iloc[-1] if len(s2) else np.nan

def _add_cums_and_ratios(period_df, keys):
    out = period_df.sort_values(keys + ["PERIOD_END"]).copy()

    for col in ["BCWS", "BCWP", "ACWP", "ETC"]:
        if col not in out.columns:
            out[col] = np.nan
        out[col] = _num(out[col])

    g = out.groupby(keys, dropna=False)  # <-- keys is list[str], not a DF slice

    # use groupby.apply -> align back safely
    out["BCWS_CUM"] = g["BCWS"].apply(lambda s: s.fillna(0).cumsum()).reset_index(level=list(range(len(keys))), drop=True)
    out["BCWP_CUM"] = g["BCWP"].apply(lambda s: s.fillna(0).cumsum()).reset_index(level=list(range(len(keys))), drop=True)
    out["ACWP_CUM"] = g["ACWP"].apply(lambda s: s.fillna(0).cumsum()).reset_index(level=list(range(len(keys))), drop=True)
    out["ETC_CUM"]  = g["ETC" ].apply(lambda s: s.fillna(0).cumsum()).reset_index(level=list(range(len(keys))), drop=True)

    out["SPI_LSD"] = _safe_div(out["BCWP"], out["BCWS"])
    out["CPI_LSD"] = _safe_div(out["BCWP"], out["ACWP"])
    out["SPI_CTD"] = _safe_div(out["BCWP_CUM"], out["BCWS_CUM"])
    out["CPI_CTD"] = _safe_div(out["BCWP_CUM"], out["ACWP_CUM"])
    return out

def build_evms_tables(cobra_merged_df, period_ends=None, year_filter=None, as_of_date=None, debug_program=None):
    issues = []
    df0 = _std_cols(cobra_merged_df)

    required = ["PROGRAM", "SUB_TEAM", "COST_SET", "DATE", "HOURS"]
    missing = [c for c in required if c not in df0.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Found: {list(df0.columns)}")

    df = df0.copy()
    df["PROGRAM"] = df["PROGRAM"].astype(str).str.strip()
    df["SUB_TEAM"] = df["SUB_TEAM"].astype(str).str.strip()
    df["COST_SET"] = df["COST_SET"].astype(str).str.strip()
    df["DATE"] = _to_dt(df["DATE"])
    df["HOURS"] = _num(df["HOURS"])

    if year_filter is not None:
        df = df[df["DATE"].dt.year == int(year_filter)].copy()
        if df.empty:
            issues.append(f"No rows after year_filter={year_filter}.")
            return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), issues)

    as_of = _to_dt(as_of_date) if as_of_date is not None else df["DATE"].max()
    if pd.isna(as_of):
        as_of = df["DATE"].max()

    df["EVMS_BUCKET"] = _map_bucket(df["COST_SET"])
    before = len(df)
    df = df.dropna(subset=["EVMS_BUCKET", "DATE", "HOURS"]).copy()
    dropped = before - len(df)
    if dropped:
        issues.append(f"Dropped {dropped:,} rows missing EVMS_BUCKET/DATE/HOURS.")

    if period_ends is None:
        issues.append("period_ends is None; inferred from unique DATEs (calendar may be off).")
        period_ends = _infer_period_ends_from_data(df)

    df["PERIOD_END"] = _assign_period_end(df["DATE"], period_ends)
    nat_pct = df["PERIOD_END"].isna().mean()
    if nat_pct > 0:
        issues.append(f"{nat_pct:.1%} rows got PERIOD_END=NaT (DATE beyond last period_end).")

    df = df.dropna(subset=["PERIOD_END"]).copy()

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

    period_prog = _add_cums_and_ratios(period_prog, keys=["PROGRAM"])
    period_sub  = _add_cums_and_ratios(period_sub,  keys=["PROGRAM", "SUB_TEAM"])

    # LSD per program
    lsd_by_program = {}
    for prog, g in period_prog.groupby("PROGRAM", dropna=False):
        g = g.sort_values("PERIOD_END")
        pe = g["PERIOD_END"].dropna().unique()
        pe_le = pe[pe <= as_of]
        lsd_by_program[prog] = pe_le[-1] if len(pe_le) else (pe[-1] if len(pe) else pd.NaT)

    # Prepare sorted period_ends as pandas Timestamps (NO unit-less np.datetime64)
    pe_sorted = pd.Series(_to_dt(period_ends)).dropna().sort_values().unique()
    pe_sorted = pd.to_datetime(pe_sorted)

    # Program overview
    rows = []
    for prog in period_prog["PROGRAM"].dropna().unique():
        g = period_prog[period_prog["PROGRAM"] == prog].sort_values("PERIOD_END").copy()
        if g.empty:
            continue

        lsd_end = pd.to_datetime(lsd_by_program.get(prog, pd.NaT))
        lsd_row = g[g["PERIOD_END"] == lsd_end].tail(1)
        if lsd_row.empty:
            lsd_row = g.tail(1)
            lsd_end = pd.to_datetime(lsd_row["PERIOD_END"].iloc[0])
        lsd_row = lsd_row.iloc[0]

        spi_lsd = lsd_row["SPI_LSD"]
        cpi_lsd = lsd_row["CPI_LSD"]
        if pd.isna(spi_lsd): spi_lsd = _last_nonnull(g["SPI_LSD"])
        if pd.isna(cpi_lsd): cpi_lsd = _last_nonnull(g["CPI_LSD"])

        # next period end (safe)
        next_end = pd.NaT
        if pd.notna(lsd_end) and len(pe_sorted) > 0:
            # find exact match index, else next by searchsorted
            pos = np.searchsorted(pe_sorted, lsd_end)
            if pos < len(pe_sorted) and pe_sorted[pos] == lsd_end:
                if pos + 1 < len(pe_sorted):
                    next_end = pe_sorted[pos + 1]
            else:
                # lsd_end might not be in calendar; choose first period_end > lsd_end
                if pos < len(pe_sorted):
                    next_end = pe_sorted[pos]

        next_row = g[g["PERIOD_END"] == next_end].tail(1)
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

    # Subteam SPI/CPI (LSD)
    period_sub["LAST_STATUS_PERIOD_END"] = period_sub["PROGRAM"].map(lsd_by_program)
    sub_lsd = period_sub[period_sub["PERIOD_END"] == period_sub["LAST_STATUS_PERIOD_END"]].copy()

    if not period_sub.empty:
        fb = (
            period_sub.sort_values(["PROGRAM","SUB_TEAM","PERIOD_END"])
            .groupby(["PROGRAM","SUB_TEAM"], dropna=False)
            .agg(
                SPI_LSD_FALLBACK=("SPI_LSD", _last_nonnull),
                CPI_LSD_FALLBACK=("CPI_LSD", _last_nonnull),
            )
            .reset_index()
        )
        subteam_spi_cpi = (
            sub_lsd[["PROGRAM","SUB_TEAM","LAST_STATUS_PERIOD_END","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"]]
            .merge(fb, on=["PROGRAM","SUB_TEAM"], how="left")
        )
        subteam_spi_cpi["SPI_LSD"] = subteam_spi_cpi["SPI_LSD"].fillna(subteam_spi_cpi["SPI_LSD_FALLBACK"])
        subteam_spi_cpi["CPI_LSD"] = subteam_spi_cpi["CPI_LSD"].fillna(subteam_spi_cpi["CPI_LSD_FALLBACK"])
        subteam_spi_cpi = subteam_spi_cpi.drop(columns=["SPI_LSD_FALLBACK","CPI_LSD_FALLBACK"]).sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
    else:
        subteam_spi_cpi = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","LAST_STATUS_PERIOD_END","SPI_LSD","SPI_CTD","CPI_LSD","CPI_CTD"])

    # BAC/EAC/VAC by subteam
    if not sub_lsd.empty:
        sub_lsd["BAC_HRS"] = sub_lsd["BCWS_CUM"]
        sub_lsd["EAC_HRS"] = sub_lsd["ACWP_CUM"] + sub_lsd["ETC"]
        sub_lsd["VAC_HRS"] = sub_lsd["BAC_HRS"] - sub_lsd["EAC_HRS"]
        subteam_bac_eac_vac = sub_lsd[["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"]].sort_values(["PROGRAM","SUB_TEAM"]).reset_index(drop=True)
    else:
        subteam_bac_eac_vac = pd.DataFrame(columns=["PROGRAM","SUB_TEAM","BAC_HRS","EAC_HRS","VAC_HRS"])

    # Program hours forecast
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

    # Optional deep debug
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
        d_per = period_prog[period_prog["PROGRAM"] == p].sort_values("PERIOD_END")
        print("\nLast 15 program periods:")
        display(d_per[["PERIOD_END","BCWS","BCWP","ACWP","ETC","SPI_LSD","CPI_LSD","SPI_CTD","CPI_CTD"]].tail(15))
        lsd_end = lsd_by_program.get(p, pd.NaT)
        print("\nPicked LSD PERIOD_END:", lsd_end)
        d_lsd = df[(df["PROGRAM"] == p) & (df["PERIOD_END"] == lsd_end)].sort_values("DATE").tail(120)
        print("\nRaw rows inside LSD (last 120):")
        display(d_lsd[["DATE","COST_SET","EVMS_BUCKET","HOURS","SUB_TEAM"]])

    return program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues

# ----------------------------
# RUN
# ----------------------------
PERIOD_ENDS_2026 = None  # replace with your calendar list if you have it

program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    period_ends=PERIOD_ENDS_2026,
    year_filter=2026,
    as_of_date=None,
    debug_program=None,   # set "ABRAMS_22" to print deep debug
)

print("ISSUES:")
for x in issues:
    print("-", x)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))