# Tools & Environment

## Why this section exists

This section is not only a list of tools. Its main purpose is to explain **why** I used these tools, **what role** each one played in the project, and **how** my working environment evolved during implementation.

This project started in a more traditional local setup and later continued in a cloud-assisted workflow. That transition was not a change in project direction, but an adaptation in execution. The core architecture, modeling decisions, SQL logic, and analytical reasoning remained mine throughout the project.

## Project environments

### 1) Initial local environment

The project was originally developed in a local setup using:

- **PostgreSQL**
- **pgAdmin**
- **DBeaver**
- **Python**
- **VS Code**
- **Git / Git commits**
- **Power BI**
- **draw.io**
- **TestRail**
- **JIRA**

This was the environment where I built the database logic, worked on SQL development, and structured the project in a conventional data engineering workflow.

### 2) Later cloud-assisted environment

As my local machine became too limited for sustained work, I moved part of the workflow to a lighter environment based on:

- **Google Colab**
- **Google Drive**
- **Github Copilot Terminal**
- **Claude**
- **CodeX**
This change allowed me to continue the project in a more flexible and resource-aware way. I see this transition as an example of environment adaptation rather than a compromise in technical ownership. The tools changed, but the project logic did not.

## Core tools and why I used them

### PostgreSQL

PostgreSQL is the core database system of this project. It is the environment where schemas, tables, users, roles, and SQL logic come together.

I used PostgreSQL as the main relational database engine because the project is centered on Data Warehouse thinking: structured data, layered modeling, controlled transformations, and explicit schema design.

### pgAdmin

I used **pgAdmin** mainly for administrative tasks such as:

- connecting to the PostgreSQL server
- creating databases
- restoring databases when needed
- inspecting server-level objects
- working with users, roles, and ownership concepts

From my perspective, pgAdmin represents the **administration layer** of the project. It helps make the server–database–schema hierarchy visible and encourages thinking beyond query execution alone.

### DBeaver

I used **DBeaver** as my primary SQL development environment.

Its role in the project was mainly:

- writing and organizing SQL scripts
- exploring data interactively
- maintaining reusable query files
- working in a development-oriented interface

In practice, I did not want to treat administration and SQL development as the same activity. DBeaver gave me a cleaner space for development work, while pgAdmin helped me think more clearly about the database system itself.

### Python

I used **Python** where procedural logic and dataset manipulation were easier to express outside plain SQL.

This was especially useful when I needed to reshape the source data, generate additional synthetic structures for the project scenario, or prepare source files before loading them into the warehouse flow. I also used it for additional data profiling implementations. In this project, Python was a strong side tool for me.

### VS Code

I used **VS Code** as a general development workspace to keep scripts, notes, and supporting files organized, and as the place where changes were committed and pushed to GitHub.

### Google Colab & Drive

Later in the project, **Google Colab** and **Google Drive** became part of the workflow when I needed a lighter and more portable setup.

I used them as practical execution and storage layers after the local environment became less reliable. This also pushed me toward a more cloud-oriented working style, which is relevant to how modern data work is often performed under real constraints.

## Why I used pgAdmin and DBeaver together

Using both tools was a deliberate choice.

If I used only pgAdmin, development and administration would be mixed too closely. If I used only DBeaver, some important server-level thinking would become less visible.

Using both allowed me to separate two related but different perspectives:

- **pgAdmin** answers: *How is the database system structured?*
- **DBeaver** answers: *How do I develop and work with data inside that system?*

This separation reflects an engineering mindset more than a tool-centric one.

## Other tools in simple terms

Besides the main database tools, the project also relied on several supporting technologies.

**SQL** was the main language for defining structures, changing data, and controlling access. In technical terms, this includes data definition, data manipulation, and access control.

**Python** was used when procedural logic or file-level data processing was more convenient than expressing everything directly in SQL.

**VS Code** served as the main development workspace for organizing scripts and keeping project materials easier to manage.

In some early-stage data work, spreadsheet-style inspection can also be useful before loading data into a database system. I treat that as part of practical workflow realism rather than as a substitute for database design.

## Business intent behind loading strategy

The loading approach in this project was guided by business logic rather than by technical demonstration alone.

- **Initial bulk load (95%)** represents the historical snapshot needed to start the warehouse.
- **Incremental load (5%)** represents newly arriving data at a later point in time.

This second part was also useful for demonstrating how later changes can be processed in a controlled warehouse workflow, including scenarios relevant to historical tracking and change handling.

## Grain and downstream BI thinking

At the staging level, the current grain is **one record per row in the CSV**, which means transaction-level or line-item-level detail depending on the source structure.

Later, the analytical layer is expected to move from raw ingestion toward structured warehouse outputs. In this project, downstream reporting tools such as Power BI are intended to consume facts and dimensions created after the core warehouse layers are built, especially in:

- `nf`
- `dim`

This reflects an important design principle in the project: source data should not be confused with reporting-ready data.

## AI-assisted workflow and technical ownership

I also use AI tools as part of my workflow, but in a controlled and purposeful way.

For example, I use tools such as **Codex** mainly for repository support, documentation refinement, and improving project organization after the architectural and coding work is already defined.

I do **not** treat AI as a replacement for technical understanding. The database logic, warehouse modeling decisions, SQL structure, and analytical direction remain under my control. I see AI-assisted tooling as a way to improve execution speed and repository quality while keeping the technical knowledge and decision-making process with me.

**This is also connected to a broader goal of mine:** becoming more **AI-native** in the way I build data systems. In the longer term, I want to move toward more automated and agentic workflows for Data Warehouse design and implementation. In that sense, using tools like Codex is not separate from the project mindset; it is part of how I prepare for more advanced DWH automation and agentic workflow design.

---

