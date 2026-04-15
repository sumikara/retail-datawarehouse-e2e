# Mapping Layer — Architectural Design (Visual Playbook)

> This document is a visual-first architecture guide prepared from `sql/01_landing`, `sql/02_mapping`, `sql/03_normalized`, `sql/04_marts`, and `sql/05_orchestrastion` SQL assets. It is intentionally designed as a modern diagram set that can also be exported to PNG from Mermaid-compatible tooling.
## 0) How to use this document

- All diagrams are provided in Mermaid for version control friendliness.
- For PNG delivery, render each diagram in Mermaid Live Editor, draw.io, dbdiagram.io, pgAdmin ERD, or your CI docs renderer and export as PNG.
- Diagram order follows end-to-end pipeline readability: ingestion → mapping → 3NF → dimensional model → BI/ops/DQ.

---

## 1) Architecture in Pipeline (Flow Visual)

```mermaid
flowchart LR
    classDef src fill:#E8F0FE,stroke:#1A73E8,stroke-width:1.5,color:#0B3A75;
    classDef stg fill:#E6F4EA,stroke:#1E8E3E,stroke-width:1.5,color:#0B3D1F;
    classDef map fill:#FFF4E5,stroke:#FB8C00,stroke-width:1.5,color:#7A3E00;
    classDef nf fill:#F3E8FD,stroke:#8E24AA,stroke-width:1.5,color:#4A148C;
    classDef dim fill:#E0F7FA,stroke:#0097A7,stroke-width:1.5,color:#004D57;
    classDef bi fill:#FCE8E6,stroke:#D93025,stroke-width:1.5,color:#7A1C16;
    A[CSV Sources\nOnline + Offline]:::src --> B[Foreign Tables\nfrg_*]:::src
    B --> C[RAW Source Tables\nsl_*]:::stg
    C --> D[Clean Standardized Sources\nstg cleaned set]:::stg
    D --> E[Mapping Layer\nstg.mapping_*]:::map
    E --> F[NF / 3NF Layer\nnf.*]:::nf
    F --> G[Dimensional Layer\ndim.*]:::dim
    G --> H[Power BI / Reporting]:::bi
```

---

## 2) Data Flow Diagram (Single Transaction Journey)

```mermaid
flowchart LR
    classDef data fill:#EEF7FF,stroke:#1976D2,color:#0D47A1;
    classDef proc fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20;
    classDef key fill:#FFF8E1,stroke:#F9A825,color:#6D4C41;
    R1[CSV Row\ntransaction_id + attributes]:::data
    F1[frg_* foreign table row]:::data
    S1[src_raw row\nsl_online_retail/src_offline_retail]:::data
    S2[src_standardized row\nclean typed columns]:::data
    M1[mapping_transactions row\nrow_sig=md5(concat_ws('|',...))]:::key
    N1[nf_transactions row\n8 FK resolution]:::key
    D1[fct_transactions_dd_dd row\njoined by surrogate keys]:::key
    R1 --> F1 --> S1 --> S2 --> M1 --> N1 --> D1
```

---

## 3) Project Architecture Overview (One-page)

```mermaid
flowchart LR
    classDef ext fill:#F5F5F5,stroke:#616161,color:#212121;
    classDef l1 fill:#E3F2FD,stroke:#1565C0,color:#0D47A1;
    classDef l2 fill:#E8F5E9,stroke:#2E7D32,color:#1B5E20;
    classDef l3 fill:#FFF3E0,stroke:#EF6C00,color:#5D4037;
    classDef l4 fill:#F3E5F5,stroke:#8E24AA,color:#4A148C;
    classDef l5 fill:#E0F2F1,stroke:#00695C,color:#004D40;
    SRC[CSV Files\n~475K offline + ~475K online]:::ext --> LND[Schema: sl_* / stg raw-clean\nLanding + Standardization]:::l1
    LND --> MAP[Schema: stg.mapping_*\nSemantic alignment + lineage]:::l2
    MAP --> NF[Schema: nf.*\n13 normalized entities]:::l3
    NF --> DIM[Schema: dim.*\n8 dimensions + fact]:::l4
    DIM --> BI[Power BI\nDashboards + KPI consumption]:::l5
```

---

## 4) Full Architecture Diagram (Horizontal, All Layers)

```mermaid
flowchart LR
    subgraph SOURCES[External Sources]
        CSV1[online_retail.csv]
        CSV2[offline_retail.csv]
    end
    subgraph LANDING[01_landing / stg controls]
        L1[stg.load_raw_online]
        L2[stg.load_raw_offline]
        L3[stg.build_clean_online]
        L4[stg.build_clean_offline]
    end
    subgraph MAPPING[02_mapping]
        M1[stg.mapping_customers]
        M2[stg.mapping_products]
        M3[stg.mapping_transactions]
    end
    subgraph NORMALIZED[03_normalized]
        N1[nf reference entities]
        N2[nf core entities]
        N3[nf_transactions]
    end
    subgraph MARTS[04_marts]
        D1[dim.dim_*]
        D2[dim.fct_transactions_dd_dd]
    end
    subgraph REPORTING[08_reporting]
        PBI[Power BI semantic model]
    end
    CSV1 --> L1
    CSV2 --> L2
    L1 --> L3
    L2 --> L4
    L3 --> M1
    L4 --> M1
    M1 --> N1 --> D1 --> PBI
    M2 --> N2 --> D1
    M3 --> N3 --> D2 --> PBI
```

---

## 5) ERD Diagram (Whole SQL Landscape, High-level)

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
    DIM_DATES ||--o{ FCT_TRANSACTIONS_DD_DD : date_key
```

---

## 6) Professional Architecture Diagram (Mapping-focused)

```mermaid
flowchart LR
    subgraph STG[Staging / Operational Control]
        A1[sl_online_retail.src_online_retail]
        A2[sl_offline_retail.src_offline_retail]
        A3[Optional: src_offline_retail_employee_inc]
        A4[stg.log_etl_event]
    end
    subgraph MAP[Mapping Layer]
        M1[load_map_customers]
        M2[load_map_stores]
        M3[load_map_products]
        M4[load_map_promotions]
        M5[load_map_deliveries]
        M6[load_map_engagements]
        M7[load_map_employees]
        M8[load_map_transactions]
        T1[(stg.mapping_customers)]
        T2[(stg.mapping_stores)]
        T3[(stg.mapping_products)]
        T4[(stg.mapping_promotions)]
        T5[(stg.mapping_deliveries)]
        T6[(stg.mapping_engagements)]
        T7[(stg.mapping_employees)]
        T8[(stg.mapping_transactions\nrow_sig unique index)]
    end
    subgraph NF[NF / 3NF]
        N1[Entity resolution + survivorship + SCD]
    end
    A1 --> M1 & M3 & M4 & M5 & M6 & M8
    A2 --> M1 & M2 & M3 & M4 & M5 & M7 & M8
    A3 --> M7
    M1 --> T1
    M2 --> T2
    M3 --> T3
    M4 --> T4
    M5 --> T5
    M6 --> T6
    M7 --> T7
    M8 --> T8
    M1 --> A4
    M2 --> A4
    M3 --> A4
    M4 --> A4
    M5 --> A4
    M6 --> A4
    M7 --> A4
    M8 --> A4
    T1 --> N1
    T2 --> N1
    T3 --> N1
    T4 --> N1
    T5 --> N1
    T6 --> N1
    T7 --> N1
    T8 --> N1
```

---

## 7) Snowflake Schema ERD (NF layer, 13 Tables)

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

## 8) Star Schema Diagram (Dim layer — Kimball)

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
    D8 -->|date_id (role-playing date keys)| F
```

---

## 9) Pipeline Orchestration Flow (Procedure hierarchy + logs)

```mermaid
flowchart LR
    subgraph INGESTION[Ingestion Entry]
        I0[stg.master_ingestion_load]
        I1[stg.load_raw_sources]
        I2[stg.load_raw_offline]
        I3[stg.load_raw_online]
        I4[stg.build_clean_staging]
        I5[stg.build_clean_offline]
        I6[stg.build_clean_online]
    end
    subgraph FULLLOAD[Full DWH Build Entry]
        F0[stg.master_full_load]
        F1[stg.load_map_*]
        F2[stg.load_ce_*]
        F3[stg.load_dim_*]
        F4[stg.master_transactions_monthly_load]
        F5[dim.load_fct_transactions_dd_by_month]
    end
    subgraph OBS[Observability]
        O1[stg.log_etl_event]
    end
    I0 --> I1
    I1 --> I2
    I1 --> I3
    I0 --> I4
    I4 --> I5
    I4 --> I6
    F0 --> F1 --> F2 --> F3 --> F4 --> F5
    I2 --> O1
    I3 --> O1
    I5 --> O1
    I6 --> O1
    F1 --> O1
    F2 --> O1
    F3 --> O1
    F5 --> O1
```

---

## 10) Incremental vs Bulk Load Flow (Comparison)

```mermaid
flowchart TB
    subgraph BULK[Bulk Mode]
        B1[Read full online/offline source sets]
        B2[Rebuild full clean staging]
        B3[Run complete mapping load set]
        B4[Run complete nf load set]
        B5[Run full dimensional refresh]
        B6[Monthly fact partition procedure]
        B1 --> B2 --> B3 --> B4 --> B5 --> B6
    end
    subgraph INCR[Incremental Mode]
        I1[Read delta files / inc table]
        I2[Append + merge targeted clean staging]
        I3[Targeted map refresh (entity-specific)]
        I4[Targeted nf upsert / SCD process]
        I5[Dim upsert + monthly fact partition slice]
        I1 --> I2 --> I3 --> I4 --> I5
    end
```

---

## 11) SCD Type 0 / Type 1 / Type 2 Comparison

```mermaid
flowchart LR
    subgraph T0[SCD Type 0 - Immutable]
        T0A[Before: key=10, city=Boston]
        T0B[Change event: city=Chicago]
        T0C[After: unchanged row\nkey=10, city=Boston]
        T0A --> T0B --> T0C
    end
    subgraph T1[SCD Type 1 - Overwrite]
        T1A[Before: key=20, email=a@x.com]
        T1B[Change event: email=b@x.com]
        T1C[After: same key overwritten\nkey=20, email=b@x.com]
        T1A --> T1B --> T1C
    end
    subgraph T2[SCD Type 2 - History]
        T2A[Before: surr=301, src=E77, role=Agent, is_current=Y]
        T2B[Change event: role=Lead Agent]
        T2C[After #1: surr=301 closed\nvalid_to=event_ts, is_current=N]
        T2D[After #2: surr=455 new current\nrole=Lead Agent, valid_from=event_ts, is_current=Y]
        T2A --> T2B --> T2C --> T2D
    end
```

---

## 12) DQ Framework Diagram (6-cell Grid)

```mermaid
flowchart TB
    D1[Completeness\n🟢 Required columns non-null\nStatus: Green]
    D2[Validity\n🟡 Domain/type pattern checks\nStatus: Yellow]
    D3[Uniqueness\n🟢 row_sig / NK collision tests\nStatus: Green]
    D4[Consistency\n🟡 cross-layer reconciliation\nStatus: Yellow]
    D5[Timeliness\n🟢 batch SLA / load_dts freshness\nStatus: Green]
    D6[Integrity\n🟢 FK and SCD conformance\nStatus: Green]
    D1 --- D2 --- D3
    D4 --- D5 --- D6
```

---

## 13) Practical Notes for PNG Exports

1. **Mermaid path (fastest):** paste each code block into Mermaid Live Editor and export PNG.
2. **ERD path (authoritative):** generate NF/table ERD via pgAdmin ERD or dbdiagram.io, then export PNG.
3. **BI-ready images:** keep all outputs in 16:9 format for slide decks and architecture reviews.
4. **Versioning:** keep Mermaid source in git and store generated PNG files under `docs/architecture/`.

---

## 14) Final Design Statement

The mapping layer remains the semantic bridge between staged data and normalized business entities. Its key value is preserving transaction-grain evidence while making source-to-target logic explicit and traceable, so NF/3NF and dimensional layers can resolve entities and analytics safely.
