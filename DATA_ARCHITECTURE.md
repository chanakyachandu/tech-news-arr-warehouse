# 🏛️ Data Architecture & Governance: Tech News & ARR Warehouse

## 1. Architecture Overview & Medallion Design
This data platform implements a production-grade **Medallion Architecture** (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`) designed for financial metric extraction, entity canonicalization, and dimensional modeling.

```
       +---------------------------------------------+
       |                BRONZE LAYER                 |
       |  Source CSVs Ingestion & Metadata Loading   |
       |  - Append raw data from source/ & data/raw/ |
       |  - Load canonical company_metadata.json     |
       |  - Ingest with zero destructive mutations   |
       +---------------------------------------------+
                              |
                              v
       +---------------------------------------------+
       |                SILVER LAYER                 |
       |  Data Cleaning, Standardization & Joins     |
       |  - 46 -> 21 Company Alias Canonicalization  |
       |  - Flag 5 Unmatched Companies (False flag)  |
       |  - 19 -> 7 Taxonomy Category Mapping        |
       |  - Multi-Currency FX to USD (EUR, GBP, JPY) |
       |  - Date Parsing to dd-mm-yyyy + Year/Qtr    |
       |  - Feature Engineering: age & size category |
       +---------------------------------------------+
                              |
                              v
       +---------------------------------------------+
       |                 GOLD LAYER                  |
       |  Dimensional Warehouse & Analytical Tables  |
       |  - dim_company (Company Dimension: N = 26)  |
       |  - fct_article (Article Fact: N = 750)      |
       |  - fct_arr_observation (ARR Fact: N = 558)  |
       |  - agg_company_quarterly_arr (Rollup: N=315)|
       |  - view_company_latest_arr (Snapshot: N=26) |
       |  - ai_articles_enriched (Deliverable: N=124)|
       +---------------------------------------------+
```

---

## 2. Dimensional Data Model & Table Specifications

The Gold Layer is structured as an analytical **Star Schema** with deterministic surrogate keys (`company_id`, `observation_id`):

```
       +------------------------------------+
       |            dim_company             |
       +------------------------------------+
       | PK company_id (COMP001, ...)       |
       |    company_name                    |
       |    industry                        |
       |    headquarters                    |
       |    founded_year                    |
       |    employee_count                  |
       |    company_size_category           |
       |    is_public                       |
       |    stock_ticker                    |
       |    has_company_metadata            |
       +------------------------------------+
                |                   |
        1:N     |                   | 1:N
                v                   v
+-------------------------------+   +------------------------------------+
|          fct_article          |   |        fct_arr_observation         |
+-------------------------------+   +------------------------------------+
| PK article_id (ART0001, ...)  |   | PK observation_id (ARR0001, ...)   |
|    original_index             |   | FK article_id                      |
| FK company_id                 |   | FK company_id                      |
|    title                      |   |    observation_date (dd-mm-yyyy)   |
|    category                   |   |    observation_year                |
|    category_clean             |   |    observation_quarter             |
|    author                     |   |    observation_year_month          |
|    published_date (dd-mm-yyyy)|   |    arr_usd_M (Int64 Millions)      |
|    published_year             |   |    arr_usd (Int64 Exact Dollars)   |
|    published_quarter          |   +------------------------------------+
|    published_month            |
|    published_year_month       |
|    word_count                 |
|    summary                    |
|    url                        |
+-------------------------------+
```

### Table Grains & Key Designations:
1. **`dim_company`**
   * **Grain**: Exactly 1 row per canonical company ($N = 26$).
   * **Primary Key**: `company_id` (`COMP001`, `COMP002`, ...).
   * **Surrogate Key Rationale**: Decouples analytical pipelines from future company rebranding or mergers.
   * **Unmatched Companies Handling**: Companies present in articles but absent from the seed metadata (Cohere, Hugging Face, Mistral AI, Perplexity AI, xAI) are preserved with `has_company_metadata = False`.

2. **`fct_article`**
   * **Grain**: Exactly 1 row per tech news article ($N = 750$).
   * **Primary Key**: `article_id` (`ART0001`, `ART0002`, ...).
   * **Foreign Keys**: `company_id` referencing `dim_company`.

3. **`fct_arr_observation`**
   * **Grain**: Exactly 1 row per valid financial revenue/ARR point-in-time observation ($N = 558$).
   * **Primary Key**: `observation_id` (`ARR0001`, `ARR0002`, ...).
   * **Foreign Keys**: `article_id` referencing `fct_article`, `company_id` referencing `dim_company`.
   * **Missing/Undisclosed Handling**: Articles with undisclosed or unparseable revenue are preserved in `fct_article` with null revenue, but excluded from `fct_arr_observation` so non-observations do not distort financial aggregations.

4. **`agg_company_quarterly_arr`**
   * **Grain**: 1 row per company per calendar year-quarter ($N = 315$).
   * **Calculations**: `observations_count`, `latest_arr_usd_M`, `avg_arr_usd_M`, `max_arr_usd_M`, `min_arr_usd_M`.

5. **`view_company_latest_arr`**
   * **Grain**: 1 row per company showing the most recent ARR observation available ($N = 26$).

6. **`ai_articles_enriched.csv`**
   * **Grain**: 1 row per filtered high-growth AI article ($N = 124$).
   * **Filters**: Category AI/ML OR Industry AI/ML, published 2022–2024, valid ARR > $50M USD. Contains 17 required columns.

---

## 3. Data Governance & Critical Principles

### ⚠️ Article ARR is NOT Master Data
A foundational governance rule enforced throughout this pipeline:
> **"Do not treat article ARR values as company master data without source lineage and caveats."**

* **Why?** News articles report estimated, forward-looking, leaked, or annualized run rates that may contradict official SEC filings, press releases, or subsequent quarters.
* **Our Implementation**:
  1. Financial metrics are modeled as discrete **temporal point-in-time observations** (`fct_arr_observation`), never static attributes on `dim_company`.
  2. Every ARR observation retains explicit **source lineage** back to `article_id`, publication timestamp, author, and source URL.
  3. Missing or undisclosed revenue values are preserved as nulls (`<NA>`) and excluded from financial observations so averages are not skewed by artificial zeroes.

---

## 4. Engineering Design Choices & Trade-offs

1. **Modular Medallion Architecture (`medallion/`)**:
   * *Decision*: Decomposed monolithic pipeline into `bronze.py`, `silver.py`, and `gold.py`.
   * *Trade-off*: Slightly more files, but achieves enterprise maintainability, independent testing, and clear data lineage.
2. **Surrogate Keys vs Natural Keys**:
   * *Decision*: Assigned deterministic surrogate keys (`COMP001`, `ARR0001`).
   * *Trade-off*: Adds an ID assignment step, but ensures fast integer joins and prevents breakage when companies rebrand or merge.
3. **Nullable Types (`Int64`, `boolean`)**:
   * *Decision*: Enforced pandas nullable integer types across all IDs, years, employee counts, and ages.
   * *Trade-off*: Avoids unwanted float conversion (`2022.0`), maintaining strict warehouse data integrity.

---

## 5. Production Cloud Scaling & Idempotency

```
 +------------------+     +------------------+     +-------------------+
 |  Kafka / Kinesis | --> |  Airflow / Prefect| -->|  Databricks / EMR |
 |  (Streaming Ingest)    |  (Orchestration) |     |  (Spark / Delta)  |
 +------------------+     +------------------+     +-------------------+
                                                               |
                                                               v
                        +-----------------------------------------------+
                        |  Iceberg / Delta Lake / Snowflake / BigQuery  |
                        +-----------------------------------------------+
```

1. **Idempotent Backfills & Deduplication**:
   - All surrogate keys (`company_id`, `observation_id`) and natural keys (`article_id`) are generated deterministically.
   - Ingestion uses `MERGE INTO` (upsert) logic keyed on `article_id` and content hash (`SHA-256(title + url)`), ensuring that re-running pipelines produces **zero duplicate records**.

2. **Schema Evolution Handling**:
   - **Silver Layer Contract**: Employs explicit column selection and typed casting with nullable integer (`Int64`) and boolean support.
   - New metadata columns in source JSON/CSVs are dynamically captured without breaking existing relational schemas.

3. **Automated Quality SLA Monitoring**:
   - Pytest suite (`tests/test_pipeline.py`) validates 100% metadata join coverage, range midpoint arithmetic, foreign key referential integrity, and required export filters.
