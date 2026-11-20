# Project Initialization and Phase 1 Implementation — ABC Retail Intelligence

You are working on `abc-ms-fabric-retail-demo`, a Microsoft Fabric demo project for Alex Berger Consulting.

Your task is to establish the project's architecture, documentation and development structure, and implement Phase 1 of the development roadmap.

This is both a practical learning project and a public professional portfolio asset. Prioritise understandable, maintainable and realistic implementation over unnecessary complexity.

Before making changes, inspect the existing repository and its files.

Also inspect the existing project at:

`C:\Users\alex\Desktop\projects\abc-prediction-platform`

Use it as a reference for the visual design system, naming conventions and general code quality. Do not modify that project.

---

# 1. Project objectives

Build an end-to-end retail analytics platform using Microsoft Fabric.

**Business case:** Retail Performance & Customer Intelligence.

The platform should enable a retail business to analyse:
- Sales and revenue development.
- Product and category performance.
- Customer purchasing behaviour.
- Repeat purchases.
- Returns and cancellations.
- Geographic sales distribution.

**Dataset:** UCI Online Retail II.

Source: https://archive.ics.uci.edu/dataset/502/online+retail+ii

The dataset has already been downloaded locally. The expected location is:

`data/raw/online_retail_II.xlsx`

Preserve the original dataset. Do not modify it.

Do not commit raw dataset files to Git.

---

# 2. Architecture

Use the Medallion architecture:

**Bronze → Silver → Gold**

### Bronze
Raw ingested source data.

- Preserve source data.
- Store data in Delta format in the Lakehouse.
- Include basic ingestion metadata where appropriate.

### Silver
Cleaned and standardised data.

- Correct data types.
- Handle missing values.
- Remove duplicates where appropriate.
- Handle cancellations and returns explicitly.
- Apply basic data quality checks.
- Prepare data for analytical consumption.

### Gold
Business-ready analytical data.

Use a dimensional model suitable for Power BI.

The initial model should include a sales fact table and relevant dimensions, such as:
- FactSales
- DimDate
- DimProduct
- DimCustomer
- DimGeography

Adapt the model to the actual characteristics of the dataset. Document important modelling decisions.

Use one primary Fabric Lakehouse.

Use its SQL analytics endpoint for T-SQL access. Do not introduce a separate Fabric Warehouse in the initial implementation.

### General data architecture principles

- Use Microsoft Fabric as the managed cloud execution environment.
- Use OneLake for storage.
- Use Delta Lake tables.
- Use Fabric Data Factory for orchestration and ingestion.
- Use Fabric notebooks with PySpark and Spark SQL for transformations.
- Keep Bronze, Silver and Gold logically separated.
- Prefer simple, understandable solutions over elaborate abstractions.

---

# 3. Technology choices

The primary technology stack is:

- Microsoft Fabric
- OneLake
- Fabric Lakehouse
- Data Factory pipelines
- PySpark
- Spark SQL
- Delta Lake
- SQL analytics endpoint
- Power BI
- GitHub

Use Python for supporting scripts and utilities where appropriate.

Cursor and GitHub are the primary local development and source control tools.

Fabric itself runs in Microsoft's managed cloud environment.

Do not attempt to run or containerise Fabric locally.

Do not introduce Docker, Container Apps, Kubernetes or an elaborate local development environment in the initial implementation.

A Dev Container is not required at this stage.

---

# 4. Development principles

Follow these principles throughout the project.

### 4.1 Keep the implementation proportionate

This is an 80/20 project.

Focus on the Fabric concepts that are most relevant to typical real-world data platform assignments.

Avoid implementing every available Fabric feature.

### 4.2 Learning-oriented implementation

The developer is experienced in Python, SQL, data science and cloud application development, but has no practical Microsoft Fabric experience.

Consequently:
- Explain Fabric-specific concepts in the documentation.
- Write readable, well-structured PySpark code.
- Avoid unnecessary abstractions.
- Document why important architectural choices were made.
- Distinguish between Fabric-specific functionality and general Spark/Python functionality.

The project should make it possible to understand the architecture by reading and following the implementation.

### 4.3 Fabric portal versus local development

Not everything can or should be created through Cursor.

Use the Fabric portal for operations that naturally belong there, such as workspace configuration, Lakehouse creation and certain item configurations.

Document manual portal operations clearly.

Where Fabric supports source-controlled item definitions, keep them in GitHub.

Do not invent unsupported deployment or automation mechanisms.

### 4.4 Reproducibility

The project should be reproducible from its source repository and documentation.

Keep source code, configuration, documentation and Fabric item definitions in Git where supported.

Do not commit:
- Raw datasets.
- Credentials.
- Access tokens.
- Secrets.
- Generated or temporary files.

Use environment variables or appropriate secret management for sensitive configuration.

### 4.5 Incremental development

Implement one phase at a time.

Do not prematurely implement future-phase functionality.

Each phase should result in a working and demonstrable increment.

---

# 5. Development roadmap

Document the following four phases in AGENTS.md.

## Phase 1 — Fabric Foundations

**Objective:** Establish the Fabric data platform and implement the Bronze/Silver/Gold processing pipeline.

Scope:

1. Create a Fabric trial capacity and development workspace.
2. Connect the workspace to the GitHub repository.
3. Create a Lakehouse.
4. Understand OneLake and the distinction between Files and Tables.
5. Ingest the Online Retail II dataset.
6. Implement the Bronze layer.
7. Implement Silver transformations using PySpark.
8. Implement Gold analytical tables.
9. Demonstrate PySpark DataFrame transformations and Spark SQL.
10. Introduce basic data quality validation.
11. Demonstrate the SQL analytics endpoint with T-SQL.
12. Implement a simple orchestrated pipeline connecting ingestion and notebook execution.

Where appropriate, introduce a basic incremental processing pattern. Keep it simple; more advanced incremental loading and production hardening belong in Phase 3.

**Learning outcomes:**

Understand Fabric workspaces, OneLake, Lakehouse architecture, Delta tables, PySpark, Spark SQL, Medallion architecture, pipelines and SQL analytics endpoints.

**Completion criteria:**

A working Fabric pipeline can ingest the source data, process it through Bronze, Silver and Gold, and expose the Gold data through the SQL analytics endpoint.

## Phase 2 — Business Intelligence

**Objective:** Build a business-facing analytical solution using Power BI.

Scope:

1. Design and refine the Gold dimensional model.
2. Create a Power BI semantic model.
3. Use Direct Lake where appropriate.
4. Implement a selection of DAX measures.
5. Build an interactive Power BI report.

The initial report should contain three pages:

- Executive Overview
- Product & Sales Analysis
- Customer Analysis

Include meaningful business KPIs and visualisations.

**Learning outcomes:**

Understand dimensional modelling, semantic models, Direct Lake, DAX and Power BI integration with Fabric.

**Completion criteria:**

A working Power BI report provides meaningful retail performance insights based on the Gold layer.

## Phase 3 — Production Practices

**Objective:** Improve reliability, maintainability and deployment practices.

Scope:

- Incremental ingestion and processing.
- MERGE/upsert patterns.
- Schema handling and evolution.
- Idempotency.
- Expanded data quality validation.
- Pipeline scheduling and monitoring.
- Lineage and access control.
- Git-based development workflow.
- Deployment pipelines or an appropriate CI/CD approach.
- Selected infrastructure automation where justified.

**Learning outcomes:**

Understand how to develop and operate maintainable, reliable Fabric data platforms.

Do not implement this phase during the initial iteration.

## Phase 4 — AI Extension

**Objective:** Integrate an AI-powered business intelligence capability.

Potential solution: Retail Intelligence Agent.

Potential functionality:

- Natural-language questions about business performance.
- Analytical tools backed by curated Gold data.
- Investigation of unusual changes in business metrics.
- Evidence-based explanations.
- Business performance summaries.

Potential implementation:

- Python
- FastAPI
- LLM integration
- Read-only analytical tools
- SQL access to curated data

Keep the AI application separate from the core Fabric data platform.

The exact architecture and scope will be decided when this phase is reached.

Do not implement AI functionality during the initial iteration.

---

# 6. Design system

Inspect `abc-prediction-platform` before defining any visual design conventions.

Reuse its existing visual identity where appropriate.

Inspect:
- CSS variables.
- Tailwind configuration.
- Colour palette.
- Typography.
- Spacing conventions.
- Existing UI components.

Document the relevant design tokens in the project documentation.

When implementing Power BI reports in Phase 2, translate the established design tokens into a Power BI theme JSON file.

Do not invent a competing visual identity.

Avoid building a custom React frontend for the initial project. Power BI is the intended business-facing interface.

---

# 7. Repository structure

Use the following as the intended starting structure. Adjust only where there is a clear technical reason.

abc-ms-fabric-retail-demo/

- README.md
- AGENTS.md
- .gitignore
- data/
  - README.md
  - raw/
- architecture/
  - architecture.md
  - diagrams/
- fabric/
  - lakehouse/
  - notebooks/
  - pipelines/
  - semantic-model/
  - reports/
- src/
  - ingestion/
  - transformations/
  - quality/
- tests/
  - unit/
  - integration/
- infrastructure/
  - terraform/
- docs/
  - data-model.md
  - data-dictionary.md
  - decisions.md
  - setup.md

Do not create empty directories or placeholder files without a clear purpose.

Fabric item definitions should follow the actual supported Fabric Git integration format.

Do not assume that all Fabric items are represented as conventional source files or that all data can be version-controlled.

The AI agent directory should only be introduced when Phase 4 begins.

---

# 8. Documentation requirements

## AGENTS.md

Create a comprehensive but practical AGENTS.md.

It must serve as the long-term project reference for future development sessions.

Include:

- Project overview and objectives.
- Business case and dataset.
- Architecture.
- Technology choices and rationale.
- Medallion architecture.
- Repository structure.
- Development principles.
- Design system guidance.
- Four-phase development roadmap.
- Scope boundaries and deliberately excluded functionality.
- Instructions for future coding agents.

Clearly distinguish current implementation from planned future functionality.

Do not imply that a feature is implemented just because it appears in the roadmap.

## README.md

Keep the root README concise and suitable for a public GitHub repository.

It should be readable in a short amount of time.

Include:

- Project name and short description.
- Business case.
- Architecture diagram or a simple architecture overview.
- Main technology stack.
- Current project status.
- Development roadmap.

Avoid duplicating the detailed content of AGENTS.md.

Link to relevant documentation where appropriate.

## Additional documentation

Create documentation where it adds real value.

At minimum:
- Architecture overview.
- Data model documentation.
- Dataset and setup instructions.
- Architectural decision log.

Keep documentation accurate and aligned with the actual implementation.

---

# 9. Initial implementation scope

This iteration should establish the project foundation AND implement Phase 1.

The implementation target is a functioning Fabric data platform.

Work through the following sequence:

1. Inspect the existing repository and dataset.
2. Inspect the reference project for design conventions.
3. Create AGENTS.md.
4. Update README.md.
5. Establish the repository structure.
6. Document the proposed Fabric architecture.
7. Prepare the necessary Fabric notebook and pipeline definitions.
8. Implement the Bronze, Silver and Gold processing logic.
9. Implement basic data quality checks.
10. Prepare the orchestration approach.
11. Document the required Fabric portal configuration and manual steps.
12. Provide clear instructions for deploying and executing the solution in Fabric.

Be pragmatic about what can be implemented without an active Fabric workspace.

Where a task requires actual Fabric resources or portal interaction, prepare the source definitions and explain exactly what the developer needs to do.

Do not pretend that a cloud resource has been created or tested if it has not.

Do not claim the pipeline works until it has actually been executed and validated in Fabric.

---

# 10. Out of scope

Deliberately exclude the following from the initial implementation:

- AI agents or LLM functionality.
- Streaming and Real-Time Intelligence.
- Complex Spark performance optimisation.
- A separate Fabric Warehouse.
- Machine learning forecasting.
- Complex enterprise governance.
- Elaborate three-environment deployment infrastructure.
- A custom React dashboard.
- Docker and Container Apps.
- Advanced infrastructure automation.

These can be reconsidered in later phases if there is a clear reason.

---

# 11. How to work

Before implementing:

1. Inspect the repository.
2. Inspect the actual dataset structure, sheet names, columns, data types and basic characteristics.
3. Inspect the reference project's design configuration.
4. Identify any assumptions or technical limitations.

Then implement the project foundation and Phase 1.

Do not ask unnecessary questions. Make reasonable, documented decisions where the available information is sufficient.

However, do not invent Fabric capabilities, unsupported item formats or configuration details. Verify against current Microsoft documentation where necessary.

Prioritise a working, understandable implementation over architectural sophistication.

At the end, provide a summary containing:

- Files and directories created or changed.
- Architectural decisions made.
- Phase 1 functionality implemented.
- Fabric portal actions still required.
- How to execute and validate the implementation.
- Known limitations and outstanding issues.
- Confirmation of what has actually been tested versus what remains unverified.

**Most important:** Build a solid foundation and a working first phase. Do not attempt to implement the entire four-phase roadmap in one iteration.