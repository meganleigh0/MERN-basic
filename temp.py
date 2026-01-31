import pandas as pd
import numpy as np

# -------------------------------
# 1. NORMALIZE COST-SET LANGUAGE
# -------------------------------
COST_SET_MAP = {
    "budget": "BAC",
    "progress": "BCWP",
    "bcws": "BCWS",
    "acwp": "ACWP",
    "acwp_wkl": "ACWP_LSD",
    "weekly actuals": "ACWP_LSD",
    "acwp_hrs": "ACT_HRS",
    "etc": "ETC",
    "eac": "EAC"
}

cobra_df["cost_set_norm"] = (
    cobra_df["COST-SET"]
    .str.lower()
    .str.strip()
    .map(COST_SET_MAP)
)

cobra_df = cobra_df[~cobra_df["cost_set_norm"].isna()].copy()

# -------------------------------
# 2. STANDARDIZE VALUE COLUMN
# -------------------------------
VALUE_COL = "HOURS" if "HOURS" in cobra_df.columns else "AMOUNT"

cobra_df[VALUE_COL] = pd.to_numeric(cobra_df[VALUE_COL], errors="coerce").fillna(0)

# -------------------------------
# 3. SPLIT CTD vs LSD
# -------------------------------
cobra_df["status_type"] = np.where(
    cobra_df["cost_set_norm"].str.contains("LSD"),
    "LSD",
    "CTD"
)

# -------------------------------
# 4. PIVOT TO EVMS SHAPE
# -------------------------------
evms_base = (
    cobra_df
    .pivot_table(
        index=["source", "SUB_TEAM", "status_type"],
        columns="cost_set_norm",
        values=VALUE_COL,
        aggfunc="sum",
        fill_value=0
    )
    .reset_index()
)

# Ensure all required columns exist
for col in ["BAC", "BCWS", "BCWP", "ACWP", "EAC", "ETC", "ACT_HRS"]:
    if col not in evms_base:
        evms_base[col] = 0.0

# -------------------------------
# 5. EVMS METRICS
# -------------------------------
evms_base = evms_base.assign(
    SPI=lambda d: np.where(d["BCWS"] != 0, d["BCWP"] / d["BCWS"], np.nan),
    CPI=lambda d: np.where(d["ACWP"] != 0, d["BCWP"] / d["ACWP"], np.nan),
    BEI=lambda d: np.where(d["BAC"] != 0, d["BCWP"] / d["BAC"], np.nan),
    VAC=lambda d: d["BAC"] - d["EAC"]
)

# -------------------------------
# 6. TABLE 1 — SOURCE LEVEL (CTD & LSD)
# -------------------------------
source_evms = (
    evms_base
    .groupby(["source", "status_type"], as_index=False)
    .agg(
        BAC=("BAC", "sum"),
        BCWS=("BCWS", "sum"),
        BCWP=("BCWP", "sum"),
        ACWP=("ACWP", "sum"),
        EAC=("EAC", "sum")
    )
    .assign(
        SPI=lambda d: d["BCWP"] / d["BCWS"],
        CPI=lambda d: d["BCWP"] / d["ACWP"],
        BEI=lambda d: d["BCWP"] / d["BAC"],
        VAC=lambda d: d["BAC"] - d["EAC"]
    )
)

# -------------------------------
# 7. TABLE 2 — SOURCE + SUB TEAM (CTD & LSD)
# -------------------------------
subteam_evms = (
    evms_base
    .groupby(["source", "SUB_TEAM", "status_type"], as_index=False)
    .agg(
        BAC=("BAC", "sum"),
        BCWS=("BCWS", "sum"),
        BCWP=("BCWP", "sum"),
        ACWP=("ACWP", "sum"),
        EAC=("EAC", "sum"),
        ACT_HRS=("ACT_HRS", "sum"),
        ETC=("ETC", "sum")
    )
    .assign(
        SPI=lambda d: d["BCWP"] / d["BCWS"],
        CPI=lambda d: d["BCWP"] / d["ACWP"],
        VAC=lambda d: d["BAC"] - d["EAC"]
    )
)

# -------------------------------
# 8. TABLE 3 — HOURS / FORECAST
# -------------------------------
hours_forecast = (
    subteam_evms
    .assign(
        Demand_Hours=lambda d: d["BCWS"],
        Actual_Hours=lambda d: d["ACT_HRS"],
        Hours_Var_Pct=lambda d: np.where(
            d["BCWS"] != 0,
            (d["ACT_HRS"] - d["BCWS"]) / d["BCWS"],
            np.nan
        ),
        Next_Month_ETC_Hours=lambda d: d["ETC"]
    )
    [["source", "SUB_TEAM", "status_type",
      "Demand_Hours", "Actual_Hours",
      "Hours_Var_Pct", "Next_Month_ETC_Hours"]]
)

# -------------------------------
# DONE
# -------------------------------
print("✅ EVMS tables created:")
print(" - source_evms")
print(" - subteam_evms")
print(" - hours_forecast")