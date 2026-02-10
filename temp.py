DimProgram =
DISTINCT(
    UNION(
        SELECTCOLUMNS(Program_Overview, "ProgramID", Program_Overview[ProgramID]),
        SELECTCOLUMNS(SubTeam_SPI_CPI, "ProgramID", SubTeam_SPI_CPI[ProgramID]),
        SELECTCOLUMNS(SubTeam_BAC_EAC_VAC, "ProgramID", SubTeam_BAC_EAC_VAC[ProgramID]),
        SELECTCOLUMNS(Program_Manpower, "ProgramID", Program_Manpower[ProgramID])
    )
)

Color_SPI_CTD =
VAR x = SELECTEDVALUE(Program_Overview[CTD])
RETURN
SWITCH(
    TRUE(),
    ISBLANK(x), BLANK(),
    x >= 1.05, "#8EB4E3",
    x >= 0.98, "#339966",
    x >= 0.95, "#FFFF99",
    "#C0504D"
)

Color_ST_SPI_CTD =
VAR x = SELECTEDVALUE(SubTeam_SPI_CPI[SPI CTD])
RETURN
SWITCH(
    TRUE(),
    ISBLANK(x), BLANK(),
    x >= 1.05, "#8EB4E3",
    x >= 0.98, "#339966",
    x >= 0.95, "#FFFF99",
    "#C0504D"
)