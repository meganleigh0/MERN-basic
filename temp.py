erDiagram
    PROGRAMS ||--o{ OVERVIEW_FACTS : has
    PROGRAMS ||--o{ SCHEDULE_GRAPHICS : has
    SCHEDULE_GRAPHICS ||--o{ SCHEDULE_FILES : includes
    PROGRAMS ||--o{ PERFORMANCE_TILES : has
    PROGRAMS ||--o{ ISSUES : has
    PROGRAMS ||--o{ RISKS : has
    PROGRAMS ||--o{ DECISIONS : has

    DEPARTMENTS ||--o{ DEPT_LINKS : publishes
    PROGRAMS ||--o{ PROGRAM_DEPT_LINKS : uses
    DEPT_LINKS ||--o{ PROGRAM_DEPT_LINKS : assigned_to

    USERS ||--o{ ISSUES : owns
    USERS ||--o{ RISKS : owns
    USERS ||--o{ DECISIONS : owns

    CONTROL_ACCOUNTS ||--o{ ISSUE_CONTROL_ACCOUNTS : tags
    ISSUES ||--o{ ISSUE_CONTROL_ACCOUNTS : tagged_with

    CONTROL_ACCOUNTS ||--o{ RISK_CONTROL_ACCOUNTS : tags
    RISKS ||--o{ RISK_CONTROL_ACCOUNTS : tagged_with

    CONTROL_ACCOUNTS ||--o{ DECISION_CONTROL_ACCOUNTS : tags
    DECISIONS ||--o{ DECISION_CONTROL_ACCOUNTS : tagged_with

    PRODUCT_TEAMS ||--o{ PERFORMANCE_TILES : segments

    PROGRAMS {
      string ProgramID PK
      string ProgramName
      string Customer
      string ContractNumber
      string ContractType
      date   AsOfDate
      string Status
    }

    OVERVIEW_FACTS {
      int    FactID PK
      string ProgramID FK
      string FieldGroup
      string FieldLabel
      string FieldValue
      int    SortOrder
      date   EffectiveDate
    }

    SCHEDULE_GRAPHICS {
      int    GraphicID PK
      string ProgramID FK
      string GraphicTitle
      string GraphicImageURL
      string Notes
      date   SnapshotDate
    }

    SCHEDULE_FILES {
      int    FileID PK
      int    GraphicID FK
      string FileLabel
      string FileType
      string FileURL
      date   UploadedDate
    }

    PERFORMANCE_TILES {
      int    TileID PK
      string ProgramID FK
      string TileName
      string TileImageURL
      string TileURL
      string ProductTeamID FK
      string MetricKey
      decimal MetricValue
      date   SnapshotDate
    }

    ISSUES {
      int    IssueID PK
      string ProgramID FK
      string Title
      string Description
      string OwnerUserID FK
      date   DateIdentified
      date   TargetResolutionDate
      date   ActualResolutionDate
      string Status
      string ResolutionPlan
      string Priority
    }

    RISKS {
      int    RiskID PK
      string ProgramID FK
      string Description
      string Type
      string OwnerUserID FK
      int    Likelihood
      int    Consequence
      string InitialScore
      string CurrentScore
      string MitigationPlan
      string ResidualScore
      date   LastReviewedDate
      string Status
    }

    DECISIONS {
      int    DecisionID PK
      string ProgramID FK
      string DecisionDescription
      string ImpactedCAsText
      string OwnerUserID FK
      date   DueDate
      string Status
      string ImpactOfNoDecision
    }

    USERS {
      string UserID PK
      string DisplayName
      string Email
      string Org
    }

    CONTROL_ACCOUNTS {
      string ControlAccountID PK
      string ControlAccountCode
      string ControlAccountName
      string WBS
      string CAMUserID FK
    }

    ISSUE_CONTROL_ACCOUNTS {
      int    IssueID FK
      string ControlAccountID FK
    }

    RISK_CONTROL_ACCOUNTS {
      int    RiskID FK
      string ControlAccountID FK
    }

    DECISION_CONTROL_ACCOUNTS {
      int    DecisionID FK
      string ControlAccountID FK
    }

    DEPARTMENTS {
      string DepartmentID PK
      string DepartmentName
      int    SortOrder
    }

    DEPT_LINKS {
      int    LinkID PK
      string DepartmentID FK
      string LinkLabel
      string LinkURL
      string IconURL
      bool   IsActive
    }

    PROGRAM_DEPT_LINKS {
      string ProgramID FK
      int    LinkID FK
    }

    PRODUCT_TEAMS {
      string ProductTeamID PK
      string ProductTeamName
      int    SortOrder
    }
    
    
    Relationship Diagram (plain-English)

One-to-many (1 → N)
	•	PROGRAMS (1) → OVERVIEW_FACTS (N)
	•	PROGRAMS (1) → SCHEDULE_GRAPHICS (N)
	•	SCHEDULE_GRAPHICS (1) → SCHEDULE_FILES (N)
	•	PROGRAMS (1) → PERFORMANCE_TILES (N)
	•	PROGRAMS (1) → ISSUES (N)
	•	PROGRAMS (1) → RISKS (N)
	•	PROGRAMS (1) → DECISIONS (N)

Many-to-many (N ↔ N) implemented via bridge tables
	•	ISSUES ↔ CONTROL_ACCOUNTS using ISSUE_CONTROL_ACCOUNTS
	•	RISKS ↔ CONTROL_ACCOUNTS using RISK_CONTROL_ACCOUNTS
	•	DECISIONS ↔ CONTROL_ACCOUNTS using DECISION_CONTROL_ACCOUNTS
	•	PROGRAMS ↔ DEPT_LINKS using PROGRAM_DEPT_LINKS (optional — only if links vary by program)

⸻

Why this design works (and matches your portal tabs)

PROGRAMS (the hub)

This is your slicer: pick a program, and every tab filters to that ProgramID.

OVERVIEW_FACTS (Facts & Assumptions)

Instead of hardcoding columns, this is a flexible key/value structure:
	•	You can add new “facts” without changing the schema
	•	Sort order controls display order on the Overview tab

SCHEDULE_GRAPHICS + SCHEDULE_FILES (Schedule tab)

This matches what you showed:
	•	The schedule image is a “graphic snapshot”
	•	Under it, you attach files (IMS export, critical path, risk assess’t)

PERFORMANCE_TILES

Represents “Overall Program”, “By Product Team”, “EAC-VAC” tiles.
	•	Can hold images, links, or metrics
	•	Optionally tied to a Product Team

ISSUES / RISKS / DECISIONS

These mirror the grids/forms you showed.
	•	OwnerUserID ties to USERS so names are consistent
	•	Control Account tagging uses bridge tables (because one issue can hit multiple CAs)

DEPT_LINKS (+ Departments)

Your right-side menu is a simple link catalog.
	•	If every program uses the same links: you don’t need PROGRAM_DEPT_LINKS
	•	If links vary by program: use the bridge

⸻

How this maps to Excel today (and SQL tomorrow)

You can keep your Excel tables exactly named like:
	•	Programs.xlsx → PROGRAMS
	•	Issues.xlsx → ISSUES
	•	Risks.xlsx → RISKS
…and when you’re ready, migrate those tables into SQL with the same columns.

Power BI relationships will mirror the ERD:
	•	Programs[ProgramID] → all tab tables’ ProgramID
	•	Bridge tables connect to control accounts

⸻

Presentation Notes (speaker-ready)

Slide 1 — Goal

Notes:
“We’re building a Program Portal dashboard that behaves like a tabbed application. The data model needs to support each tab — Overview, Schedule, Performance, Issues, Risks, Decisions — while keeping everything filterable by Program.”

⸻

Slide 2 — Design Principles

Notes:
“Three principles:
	1.	Program is the hub key — everything relates back to ProgramID
	2.	Use reference tables for consistency (Users, Control Accounts)
	3.	Use bridge tables for many-to-many tagging like Issues ↔ Control Accounts.”

⸻

Slide 3 — ERD Overview

Notes:
“This diagram shows PROGRAMS at the center, with one-to-many relationships out to each tab dataset. The tabs are separate fact tables so refreshes and ownership can be distributed.”

⸻

Slide 4 — Overview Tab: Facts & Assumptions

Notes:
“Facts are stored as rows, not columns. That gives us flexibility — we can add new fields without altering the schema. SortOrder lets us control the exact display order in the portal.”

⸻

Slide 5 — Schedule Tab: Graphics + Files

Notes:
“We store schedule visuals as snapshots. Each snapshot can have multiple linked artifacts like the IMS export, driving path slide, or risk assessment file. This matches the portal design: a main graphic plus file tiles.”

⸻

Slide 6 — Performance Tab: Tiles

Notes:
“Performance content is modeled as tiles so we can mix: images, links, and numeric metrics. It also supports breakdown views like ‘By Product Team’ using a ProductTeams reference table.”

⸻

Slide 7 — Issues / Risks / Decisions

Notes:
“These are straightforward tables with ownership, dates, and status. The model supports filtering by Program and tracking lifecycle changes over time.”

⸻

Slide 8 — Control Account Tagging (Many-to-Many)

Notes:
“A key requirement is tagging issues/risks/decisions to multiple control accounts. That’s why we use bridge tables — it avoids duplicating rows and makes slicing by Control Account possible in Power BI.”

⸻

Slide 9 — Department Links Menu

Notes:
“The right-side menu is driven by a link catalog. If links differ by program, we add the ProgramDeptLinks bridge; otherwise we keep it global.”

⸻

Slide 10 — Power BI Relationship Setup

Notes:
“In Power BI, Programs is the dimension table. Every tab table is a fact table. We enforce single-direction filtering from Programs to tab tables, and bridges connect to ControlAccounts.”

⸻

Slide 11 — Update Workflow

Notes:
“In the short term, Excel refresh updates these tables. In the long term, these same tables map cleanly into SQL/Dataverse with minimal redesign.”

⸻

Slide 12 — Close / Next Steps

Notes:
“Next steps: finalize required fields per tab, confirm whether links are global or per-program, and decide whether ‘in-dashboard entry’ is needed — if yes, we embed Power Apps for writeback.”

⸻

If you want, I can tailor this to your exact portal screenshots

If you tell me whether you want Control Accounts and Users as real tables (recommended) or just text columns (quick prototype), I’ll:
	•	simplify the ERD for prototype or
	•	harden it for production (audit history, snapshots, approvals, etc.)