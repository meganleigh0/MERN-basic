# ---------- READ + STACK CAP EXTRACTS (WITH SOURCE TAG) ----------
cap_2022 = pd.read_excel("data/Cobra-Abrams STS 2022.xlsx", sheet_name="CAP_Extract")
cap_2022["CAP_SOURCE"] = "STS_2022"

cap_sts = pd.read_excel("data/Cobra-Abrams STS.xlsx", sheet_name="CAP_Extract")
cap_sts["CAP_SOURCE"] = "STS"

merged_df = pd.concat([cap_2022, cap_sts], ignore_index=True)

# (run your normalization + merge exactly as before)

# ---------- MISSING IPT COUNTS BY FILE ----------
missing_by_source = (
    abrams_m_df
    .assign(IPT_MISSING=lambda d: d["IPT"].isna())
    .groupby("CAP_SOURCE")["IPT_MISSING"]
    .agg(total_rows="count", missing="sum")
)

missing_by_source["pct_missing"] = (
    missing_by_source["missing"] / missing_by_source["total_rows"]
).round(3)

print(missing_by_source)