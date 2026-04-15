# Retail DWH — Architectural Design PNG Playbook

> Bu doküman, repository içindeki SQL tasarımına (`sql/01_landing` → `sql/05_orchestrastion` → `sql/08_reporting`) birebir uyumlu şekilde, istenen tüm diyagramları **tek yerde**, **analitik sırada**, **PNG çıktısına hazır Mermaid kaynaklarıyla** sunar.

## Diagram Sequencing (Best-Judgement Order)

1. Project Architecture Overview Diagram  
2. Full Architecture Diagram (Horizontal Pipeline)  
3. Architecture in Pipeline (Flow Visual)  
6. Pipeline Orchestration Flow Diagram  
8. ERD Diagram (Whole SQL Landscape, Cross-Layer)  
9. Snowflake Schema ERD (NF 3NF, 13 tables)  
10. Star Schema Diagram (Dim/Kimball)  
11. SCD Type 0 / Type 1 / Type 2 Comparison Diagram  
12. DQ Framework Diagram (6-cell)
13. (In Progress..) AI Automation & Agentic Workflow Diagram (DWH-Centric)

---

## 1) Project Architecture Overview Diagram (One-page)

**Suggested PNG output:** `docs/architecture/01_project_architecture_overview.png`

```mermaid
flowchart LR
    classDef src fill:#E3F2FD,stroke:#1565C0,color:#0D47A1;
    classDef lnd fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20;
    classDef map fill:#FFF3E0,stroke:#EF6C00,color:#5D4037;
    classDef nf fill:#F3E5F5,stroke:#8E24AA,color:#4A148C;
    classDef dim fill:#E0F2F1,stroke:#00695C,color:#004D40;
    classDef bi fill:#FCE4EC,stroke:#C2185B,color:#880E4F;

    S["CSV Sources\nonline + offline\n~500K + ~500K rows"]:::src
    L["Landing\nsl_online_retail / sl_offline_retail\nfrg_* -> src_*_raw -> src_*"]:::lnd
    M["Mapping + Orchestration\nstg.mapping_* + stg.etl_*\nrow_sig lineage + log tables"]:::map
    N["NF / 3NF Integration\nnf 13 tables"]:::nf
    D["Dimensional / Kimball\ndim.dim_* + dim.fct_transactions_dd_dd"]:::dim
    B["Power BI / Reporting"]:::bi

    S --> L --> M --> N --> D --> B
```

---

## 2) Full Architecture Diagram (Horizontal, All Schemas)

**Suggested PNG output:** `docs/architecture/02_full_architecture_horizontal.png`

```mermaid
flowchart LR
    subgraph SRC[External CSV]
      C1[online_retail.csv]
      C2[offline_retail.csv]
    end

    subgraph LND[01_landing]
      F1[frg_online_retail]
      F2[frg_offline_retail]
      R1[src_online_retail_raw]
      R2[src_offline_retail_raw]
      S1[src_online_retail]
      S2[src_offline_retail]
    end

    subgraph MAP[02_mapping / stg]
      M1[mapping_customers]
      M2[mapping_stores]
      M3[mapping_products]
      M4[mapping_promotions]
      M5[mapping_deliveries]
      M6[mapping_engagements]
      M7[mapping_employees]
      M8["mapping_transactions\nunique row_sig"]
    end

    subgraph NF[03_normalized / nf]
      N1[nf_states -> nf_cities -> nf_addresses]
      N2[nf_customers / nf_stores / nf_products]
      N3[nf_promotions / nf_deliveries / nf_engagements]
      N4[nf_employees_scd]
      N5[nf_transactions]
    end

    subgraph DIM[04_marts / dim]
      D1[dim_customers]
      D2[dim_stores]
      D3[dim_products]
      D4[dim_promotions]
      D5[dim_deliveries]
      D6[dim_engagements]
      D7[dim_employees_scd]
      D8[dim_dates]
      FCT["fct_transactions_dd_dd\nmonthly partitions"]
    end

    subgraph BI[08_reporting]
      PBI[Power BI Semantic Model]
    end

    C1 --> F1 --> R1 --> S1
    C2 --> F2 --> R2 --> S2
    S1 --> M1 & M3 & M4 & M5 & M6 & M8
    S2 --> M1 & M2 & M3 & M4 & M5 & M7 & M8
    M1 --> N2
    M2 --> N2
    M3 --> N2
    M4 --> N3
    M5 --> N3
    M6 --> N3
    M7 --> N4
    M8 --> N5
    N1 --> N2
    N2 --> D1 & D2 & D3
    N3 --> D4 & D5 & D6
    N4 --> D7
    N5 --> FCT
    D1 --> FCT
    D2 --> FCT
    D3 --> FCT
    D4 --> FCT
    D5 --> FCT
    D6 --> FCT
    D7 --> FCT
    D8 --> FCT
    FCT --> PBI
```

---

## 3) Architecture in Pipeline (Flow Visual)

**Suggested PNG output:** `docs/architecture/03_pipeline_flow_visual.png`

```mermaid
flowchart LR
    A["CSV Row"] --> B["frg_* foreign table"]
    B --> C["src_*_raw"]
    C --> D["src_* standardized"]
    D --> E["stg.mapping_*"]
    E --> F["nf.* 3NF"]
    F --> G["dim.* + fct_transactions_dd_dd"]
    G --> H["Power BI"]
```



## 6) Pipeline Orchestration Flow Diagram (Swimlane + hierarchy + log)

**Suggested PNG output:** `docs/architecture/06_pipeline_orchestration_flow.png`

```mermaid
flowchart LR
    subgraph INGESTION[Ingestion Lane]
      I0[stg.master_ingestion_load]
      I1[stg.load_raw_sources]
      I2[stg.load_raw_offline]
      I3[stg.load_raw_online]
      I4[stg.build_clean_staging]
      I5[stg.build_clean_offline]
      I6[stg.build_clean_online]
    end

    subgraph FULL[Full DWH Lane]
      F0[stg.master_full_load]
      F1["load_map_customers/stores/products/promotions/deliveries/engagements/employees/transactions"]
      F2["load_ce_states/cities/addresses/product_categories/promotion_types/shipping_partners"]
      F3["load_ce_customers/stores/products/promotions/deliveries/engagements/employees_scd/transactions"]
      F4["load_dim_customers/stores/products/promotions/deliveries/engagements/employees_scd/dates"]
      F5[stg.master_transactions_monthly_load]
      F6[dim.load_fct_transactions_dd_by_month]
    end

    subgraph OBS[Log Lane]
      O1[stg.log_etl_event]
      O2[stg.etl_batch_run]
      O3[stg.etl_step_run]
      O4[stg.etl_file_registry]
    end

    I0 --> I1 --> I2
    I1 --> I3
    I0 --> I4 --> I5
    I4 --> I6

    F0 --> F1 --> F2 --> F3 --> F4 --> F5 --> F6

    I0 --> O1
    I1 --> O2
    I4 --> O3
    I2 --> O4
    I3 --> O4
    F0 --> O1
    F1 --> O3
    F2 --> O3
    F3 --> O3
    F4 --> O3
    F6 --> O1
```


## 8) ERD Diagram (Whole SQL Landscape, Cross-Layer)

**Suggested PNG output:** `docs/architecture/08_cross_layer_erd.png`

```mermaid
erDiagram
    STG_MAPPING_CUSTOMERS ||--o{ NF_CUSTOMERS : feeds
    STG_MAPPING_STORES ||--o{ NF_STORES : feeds
    STG_MAPPING_PRODUCTS ||--o{ NF_PRODUCTS : feeds
    STG_MAPPING_PROMOTIONS ||--o{ NF_PROMOTIONS : feeds
    STG_MAPPING_DELIVERIES ||--o{ NF_DELIVERIES : feeds
    STG_MAPPING_ENGAGEMENTS ||--o{ NF_ENGAGEMENTS : feeds
    STG_MAPPING_EMPLOYEES ||--o{ NF_EMPLOYEES_SCD : feeds
    STG_MAPPING_TRANSACTIONS ||--o{ NF_TRANSACTIONS : feeds

    NF_CUSTOMERS ||--o{ DIM_CUSTOMERS : type1
    NF_STORES ||--o{ DIM_STORES : type0
    NF_PRODUCTS ||--o{ DIM_PRODUCTS : type1
    NF_PROMOTIONS ||--o{ DIM_PROMOTIONS : type0
    NF_DELIVERIES ||--o{ DIM_DELIVERIES : type0
    NF_ENGAGEMENTS ||--o{ DIM_ENGAGEMENTS : type0
    NF_EMPLOYEES_SCD ||--o{ DIM_EMPLOYEES_SCD : type2
    NF_TRANSACTIONS ||--o{ FCT_TRANSACTIONS_DD_DD : measures
    DIM_DATES ||--o{ FCT_TRANSACTIONS_DD_DD : transaction_date
```

---

## 9) Snowflake Schema ERD PNG (NF 3NF, 13 tables)

**Suggested PNG output:** `docs/architecture/09_nf_snowflake_erd.png`

```mermaid
erDiagram
    NF_STATES ||--o{ NF_CITIES : state_id
    NF_CITIES ||--o{ NF_ADDRESSES : city_id
    NF_ADDRESSES ||--o{ NF_CUSTOMERS : address_id
    NF_ADDRESSES ||--o{ NF_STORES : address_id

    NF_PRODUCT_CATEGORIES ||--o{ NF_PRODUCTS : product_category_id
    NF_PROMOTION_TYPES ||--o{ NF_PROMOTIONS : promotion_type_id
    NF_SHIPPING_PARTNERS ||--o{ NF_DELIVERIES : shipping_partner_id

    NF_CUSTOMERS ||--o{ NF_TRANSACTIONS : customer_id
    NF_PRODUCTS ||--o{ NF_TRANSACTIONS : product_id
    NF_PROMOTIONS ||--o{ NF_TRANSACTIONS : promotion_id
    NF_DELIVERIES ||--o{ NF_TRANSACTIONS : delivery_id
    NF_ENGAGEMENTS ||--o{ NF_TRANSACTIONS : engagement_id
    NF_STORES ||--o{ NF_TRANSACTIONS : store_id
    NF_CITIES ||--o{ NF_TRANSACTIONS : city_id
    NF_EMPLOYEES_SCD ||--o{ NF_TRANSACTIONS : employee_id
```

---

## 10) Star Schema (Dim Layer — Kimball)

**Suggested PNG output:** `docs/architecture/10_star_schema_dim.png`

```mermaid
flowchart TB
    F[(dim.fct_transactions_dd_dd)]
    D1[(dim.dim_customers)]
    D2[(dim.dim_stores)]
    D3[(dim.dim_products)]
    D4[(dim.dim_promotions)]
    D5[(dim.dim_deliveries)]
    D6[(dim.dim_engagements)]
    D7[(dim.dim_employees_scd)]
    D8[(dim.dim_dates)]

    D1 -->|customer_surr_id| F
    D2 -->|store_surr_id| F
    D3 -->|product_surr_id| F
    D4 -->|promotion_surr_id| F
    D5 -->|delivery_surr_id| F
    D6 -->|engagement_surr_id| F
    D7 -->|employee_surr_id| F
    D8 -->|transaction_date_key| F
```

---

## 11) SCD Type 0 / 1 / 2 Comparison Diagram

**Suggested PNG output:** `docs/architecture/11_scd_type_comparison.png`

```mermaid
flowchart LR
    subgraph S0[SCD Type 0 No Change]
      A1[Before: promotion_id=10, promotion_channel=social_media]
      A2[Incoming: promotion_channel=website]
      A3[After: unchanged row remains social_media]
      A1 --> A2 --> A3
    end

    subgraph S1[SCD Type 1 Overwrite]
      B1[Before: customer_id=20, marital_status=single]
      B2[Incoming: marital_status=married]
      B3[After: same row updated to married]
      B1 --> B2 --> B3
    end

    subgraph S2[SCD Type 2 History]
      C1[Before: employee_name=emp2, position=cashier, is_current=t]
      C2[Incoming: position=manager]
      C3[Old row closed: is_current=f, valid_to=observed_ts]
      C4[New row inserted: is_current=t, valid_from=start_dt]
      C1 --> C2 --> C3 --> C4
    end
```

---

## 12) DQ Framework Diagram (6-cell Grid)

**Suggested PNG output:** `docs/architecture/12_dq_framework_grid.png`

```mermaid
flowchart TB
    A["Completeness\n🟢 Required fields populated\nNull threshold checks"]
    B["Validity\n🟡 Type/domain/pattern rules\nDate & numeric format controls"]
    C["Uniqueness\n🟢 NK + row_sig duplicate tests\nDedup confidence"]
    D["Consistency\n🟡 Cross-layer reconciliation\nsource vs nf vs dim counts"]
    E["Timeliness\n🟢 SLA freshness checks\nload_dts monitoring"]
    F["Integrity\n🟢 PK/FK + SCD conformance\nUnknown minus 1 fallback safety"]

    A --- B --- C
    D --- E --- F
```

---

## 13) AI Automation & Agentic Workflow Diagram (DWH-Centric)

**WORK IN PROGRESS:** 

> Aim: Wrapping the existing DWH Pipeline (landing → mapping → nf → dim → reporting) up witth AI-native automation system design, which works with human (human--in-the-loop, hooks -preHook, postHook, guards .. etc), catching the anomalies, self-healing orchestration, strengthening built-in agents and task specific sub-agents.

```mermaid
flowchart LR
    classDef data fill:#E3F2FD,stroke:#1565C0,color:#0D47A1;
    classDef dwh fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20;
    classDef ai fill:#F3E5F5,stroke:#8E24AA,color:#4A148C;
    classDef ops fill:#FFF3E0,stroke:#EF6C00,color:#5D4037;
    classDef gov fill:#FCE4EC,stroke:#C2185B,color:#880E4F;
    classDef out fill:#E0F2F1,stroke:#00695C,color:#004D40;

    subgraph SOURCES[Data Sources]
      S1[Online + Offline CSV / API / ERP]:::data
      S2[CRM + Campaign + Delivery feeds]:::data
    end

    subgraph DWH[Retail DWH Core Pipeline]
      D1[Landing: frg_* / src_*_raw / src_*]:::dwh
      D2[Mapping: stg.mapping_* + row_sig]:::dwh
      D3[NF: nf_* integrated model]:::dwh
      D4[Dimensional: dim_* + fct_transactions_dd_dd]:::dwh
      D5[Reporting: Power BI / semantic views]:::dwh
      D1 --> D2 --> D3 --> D4 --> D5
    end

    subgraph AIPLATFORM[AI Automation Platform]
      A0[Feature & Context Builder\nfrom mapping, logs, DQ, KPI history]:::ai
      A1[Agent Orchestrator\nplanner + tool router + memory]:::ai
      A2[DQ & Drift Agent\nschema, freshness, null, outlier checks]:::ai
      A3[Forecasting Agent\ndemand / promo uplift / stock risk]:::ai
      A4[Root-Cause Agent\nlineage + SQL run logs + anomaly trace]:::ai
      A5[Remediation Agent\nretry, backfill, rollback, ticket ops]:::ai
      A6[NL Analytics Copilot\nbusiness Q&A over governed marts]:::ai
      A0 --> A1
      A1 --> A2 & A3 & A4 & A5 & A6
    end

    subgraph OPS[Operational Systems]
      O1[Airflow / Dagster / dbt Cloud]:::ops
      O2[Monitoring: Prometheus + Grafana]:::ops
      O3[Incident: Slack / Teams / Jira]:::ops
      O4[Model Registry + Experiment Tracking]:::ops
    end

    subgraph GOV[Governance & Security]
      G1[Policy Engine: RLS, masking, PII tags]:::gov
      G2[Audit Trail: agent action log + approval log]:::gov
      G3[Human-in-the-loop approval gates]:::gov
    end

    subgraph OUT[Business Outcomes]
      B1[Autonomous data quality triage]:::out
      B2[Proactive pipeline healing]:::out
      B3[Faster RCA + lower MTTR]:::out
      B4[Forecast-driven planning]:::out
      B5[Trusted self-service analytics]:::out
    end

    S1 --> D1
    S2 --> D1

    D2 --> A0
    D4 --> A0
    D5 --> A0
    O1 --> A0
    O2 --> A0

    A2 --> O2
    A4 --> O3
    A5 --> O1
    A5 --> O3
    A3 --> O4
    A6 --> D5

    G1 --> A1
    G2 --> A1
    G3 --> A5

    A2 --> B1
    A5 --> B2
    A4 --> B3
    A3 --> B4
    A6 --> B5
```

### Suggested Agentic Workflow (Execution Loop)

```mermaid
flowchart TD
    T0[Trigger: schedule / event / anomaly alert] --> T1[Planner Agent creates run plan]
    T1 --> T2[Context pull: lineage + ETL logs + DQ metrics + KPI deltas]
    T2 --> T3{Decision policy check}
    T3 -->|pass| T4[Specialist agents run in parallel]
    T3 -->|fail| T9[Escalate to human approval]
    T4 --> T5[DQ/Drift + Forecast + RCA outputs merged]
    T5 --> T6{Action type}
    T6 -->|safe auto-fix| T7[Remediation Agent executes retry/backfill]
    T6 -->|risky/high-impact| T9
    T7 --> T8[Post-check + regression guard + audit write]
    T8 --> T10[Publish status to Slack/Jira + BI Ops dashboard]
    T9 --> T11[Human decision: approve/reject/adjust]
    T11 --> T7
```

### System Blueprint (How to build around this DWH)

1. **Signals Layer:** `stg.log_etl_event`, batch/step logs, row counts, DQ scores, freshness SLA metrics.  
2. **Agent Runtime Layer:** Planner + specialist agents (DQ, Forecast, RCA, Remediation, Copilot).  
3. **Tooling Layer:** SQL runner, metadata/lineage reader, orchestration API, ticketing API, notification API.  
4. **Policy Layer:** Auto-action thresholds, PII guardrails, approval workflows, rollback criteria.  
5. **Learning Layer:** Incident outcome feedback + retraining cadence + prompt/version registry.  
6. **Business Layer:** BI semantic model + conversational analytics + proactive planning outputs.
