DimProgram =
DISTINCT(
    UNION(
        SELECTCOLUMNS(Program_Overview, "ProgramID", Program_Overview[ProgramID]),
        SELECTCOLUMNS(SubTeam_SPI_CPI, "ProgramID", SubTeam_SPI_CPI[ProgramID]),
        SELECTCOLUMNS(SubTeam_BAC_EAC_VAC, "ProgramID", SubTeam_BAC_EAC_VAC[ProgramID]),
        SELECTCOLUMNS(Program_Manpower, "ProgramID", Program_Manpower[ProgramID])
    )
)

PO CTD Color =
VAR metric = SELECTEDVALUE ( Program_Overview[Metric] )
VAR v0     = SELECTEDVALUE ( Program_Overview[CTD] )
VAR v      = ROUND ( v0, 2 )
RETURN
SWITCH (
    TRUE(),
    ISBLANK(v), BLANK(),

    metric IN { "SPI", "CPI" } && v >= 1.05, "#8EB4E3",
    metric IN { "SPI", "CPI" } && v >= 0.98, "#339966",
    metric IN { "SPI", "CPI" } && v >= 0.95, "#FFFF99",
    metric IN { "SPI", "CPI" },                "#C0504D",

    BLANK()
)