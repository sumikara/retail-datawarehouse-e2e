## Purpose of the Landing Layer

The landing (`stg`) layer serves as the operational entry point of the pipeline. It receives raw source data, rebuilds clean staging tables, and centralizes the execution flow through master and source-specific procedures. In this project, it also acts as the control layer where ingestion order, batch execution, and staging orchestration are managed consistently. :contentReference[oaicite:0]{index=0}

Technically, this layer is responsible for standardizing raw values before they move downstream. It applies reusable cleansing logic such as `LOWER`, `TRIM`, `COALESCE`, `REPLACE`, `CASE WHEN`, safe numeric casting, and format-aware date / timestamp parsing, so that raw text-heavy source files are converted into structured and typed staging tables. :contentReference[oaicite:1]{index=1}

Analytically, the landing layer is not yet the business modeling layer; its role is to preserve source-level detail while making the data usable, traceable, and controllable. By combining raw ingestion, standardized staging outputs, and ETL logging structures such as batch, file, step, and event tracking, it creates a reliable handoff point for the normalized and dimensional layers that follow. :contentReference[oaicite:2]{index=2}
