DimProgram =
DISTINCT(
    UNION(
        SELECTCOLUMNS(Program_Overview, "ProgramID", Program_Overview[ProgramID]),
        SELECTCOLUMNS(SubTeam_SPI_CPI, "ProgramID", SubTeam_SPI_CPI[ProgramID]),
        SELECTCOLUMNS(SubTeam_BAC_EAC_VAC, "ProgramID", SubTeam_BAC_EAC_VAC[ProgramID]),
        SELECTCOLUMNS(Program_Manpower, "ProgramID", Program_Manpower[ProgramID])
    )
)