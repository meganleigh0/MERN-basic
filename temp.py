program_overview_evms, subteam_spi_cpi, subteam_bac_eac_vac, program_hours_forecast, issues = build_evms_tables(
    cobra_merged_df,
    year_filter=2026
)

print("ISSUES:")
for i in issues:
    print(" -", i)

display(program_overview_evms.head(20))
display(subteam_spi_cpi.head(20))
display(subteam_bac_eac_vac.head(20))
display(program_hours_forecast.head(20))