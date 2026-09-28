# 🏛️ Data Architecture: Tech News & ARR Data Platform

## 1. Architecture Overview (Medallion Design)
This platform processes unstructured tech news and company metadata into an analytical warehouse using the **Medallion Architecture** (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`):

```
[ BRONZE LAYER: Raw Ingestion ]
  - Ingest raw source files (tech_news.csv, company_metadata.json) as-is.
  - Zero mutations; preserves full historical lineage.
          │
          ▼
[ SILVER LAYER: Cleaning & Enrichment ]
  - Canonicalize company name aliases (e.g. AWS -> Amazon Web Services).
  - Standardize 19 messy categories into 7 clean taxonomy values (e.g. AI_ML).
  - Parse multi-currency revenue (EUR, GBP, JPY, ranges) into normalized USD integers.
  - Standardize dates into ISO / dd-mm-yyyy and extract year & quarter.
  - Calculate company age and company size category (Small, Medium, Large).
          │
          ▼
[ GOLD LAYER: Star Schema Warehouse ]
  - Structured into 1 Dimension and 2 Fact tables for fast SQL analytics:
    * dim_company (Master company dimension)
    * fct_article (All news articles with standardized dates & categories)
    * fct_arr_observation (Point-in-time revenue observations over time)
    * agg_company_quarterly_arr (Pre-computed quarterly rollups)
    * view_company_latest_arr (Latest point-in-time snapshot per company)
    * ai_articles_enriched.csv (Filtered export: AI/ML, 2022–2024, ARR > $50M)
```

---

## 2. Dimensional Data Model & Table Specifications

### A. Raw Source Columns (Input Data)
The pipeline starts with raw tech news articles containing these 10 core columns:

| Raw Column | Description & Examples | Where It Maps in the Gold Model |
| :--- | :--- | :--- |
| **`article_id`** | Unique article identifier (`ART0001`, `ART0002`) | Primary Key in `fct_article`, Foreign Key in `fct_arr_observation` |
| **`title`** | News headline text | Preserved in `fct_article.title` |
| **`company_name`** | Mentioned company name (includes aliases like AWS) | Canonicalized & linked via `company_id` to `dim_company` |
| **`published_date`** | Messy dates (`21-Feb-20`, `02/23/2023`, `13-12-2023`) | Parsed to datetime $\rightarrow$ `fct_article.published_date` (`dd-mm-yyyy`) |
| **`category`** | Unstandardized category (`Artificial Intelligence`, `AI/ML`) | Standardized $\rightarrow$ `fct_article.category_clean` (`AI_ML`) |
| **`revenue`** | Raw revenue strings (`$980.0M`, `£244M`, `$10M - $20M`) | Parsed to normalized USD $\rightarrow$ `fct_arr_observation.arr_usd` |
| **`summary`** | Article executive brief | Preserved in `fct_article.summary` |
| **`url`** | Source article URL link | Preserved in `fct_article.url` for provenance |
| **`author`** | Journalist / reporter name | Preserved in `fct_article.author` |
| **`word_count`** | Article word length integer | Preserved in `fct_article.word_count` |

*(Enriched by `company_metadata.json`: `founded_year`, `headquarters`, `employee_count`, `industry`, `is_public`, `stock_ticker`)*

---

### B. The Modeled Star Schema
To eliminate data redundancy and enable fast analytical SQL queries, the raw input columns are separated into a clean **Star Schema**:

```
                 +--------------------------------------+
                 |             dim_company              |
                 +--------------------------------------+
                 | PK company_id (COMP001, ...)         |
                 |    company_name                      |
                 |    industry                          |
                 |    headquarters                      |
                 |    founded_year                      |
                 |    employee_count                    |
                 |    company_size_category (Small/M/L) |
                 |    is_public (True / False)          |
                 |    stock_ticker                      |
                 |    has_company_metadata              |
                 +--------------------------------------+
                        |                       |
               1-to-Many|               1-to-Many
                        v                       v
+-------------------------------+   +------------------------------------+
|          fct_article          |   |        fct_arr_observation         |
+-------------------------------+   +------------------------------------+
| PK article_id (ART0001, ...)  |   | PK observation_id (ARR0001, ...)   |
| FK company_id                 |   | FK article_id                      |
|    title                      |   | FK company_id                      |
|    category_clean             |   |    observation_date (dd-mm-yyyy)   |
|    published_date (dd-mm-yyyy)|   |    observation_year                |
|    published_year             |   |    observation_quarter             |
|    published_quarter          |   |    arr_usd (Normalized Integer $)  |
|    published_month            |   |    arr_usd_M (ARR in Millions)     |
|    author                     |   +------------------------------------+
|    word_count                 |
|    summary                    |
|    url                        |
+-------------------------------+
```

---

### C. Table Grains & Key Responsibilities

1. **`dim_company`** (26 rows)
   * **Grain**: 1 row per unique canonical company.
   * **Keys**: Primary Key = `company_id` (`COMP001`, `COMP002`, ...).
   * **Role**: Holds static company metadata (headquarters, employee count, industry, size category).

2. **`fct_article`** (750 rows)
   * **Grain**: 1 row per published news article.
   * **Keys**: Primary Key = `article_id`, Foreign Key = `company_id`.
   * **Role**: Stores article content, publication dates, cleaned categories, authors, and source URLs.

3. **`fct_arr_observation`** (558 rows)
   * **Grain**: 1 row per valid revenue/ARR observation.
   * **Keys**: Primary Key = `observation_id`, Foreign Keys = `article_id`, `company_id`.
   * **Role**: Stores point-in-time financial observations converted to exact USD integers. Articles with missing or undisclosed revenue are excluded to avoid distorting averages.

4. **`agg_company_quarterly_arr`** (315 rows)
   * **Grain**: 1 row per company per calendar quarter.
   * **Role**: Pre-aggregated rollups (`latest_arr_usd_M`, `avg_arr_usd_M`, `max_arr_usd_M`, `min_arr_usd_M`).

5. **`view_company_latest_arr`** (26 rows)
   * **Grain**: 1 row per company.
   * **Role**: Latest point-in-time ARR snapshot for each company.

6. **`ai_articles_enriched.csv`** (124 rows)
   * **Grain**: 1 row per high-growth AI article.
   * **Role**: Final business deliverable filtered for AI/ML category or industry, published 2022–2024, with ARR > $50M.

---

## 3. Core Business & Cleaning Rules

1. **Multi-Currency Normalization to USD**:
   * `EUR` $\times$ 1.10 = USD
   * `GBP` $\times$ 1.27 = USD
   * `JPY` $\div$ 150 = USD
   * Revenue ranges (e.g. `"$10M - $20M"`) are calculated by taking the midpoint (`$15,000,000`).
   * Output values are stored as exact integers (`arr_usd`) and millions (`arr_usd_M`).

2. **Article ARR is NOT Company Master Data**:
   * News articles report estimated, forward-looking, or leaked revenue that may conflict with audited reports.
   * Therefore, revenue is modeled as **point-in-time observations** (`fct_arr_observation`) with full lineage back to the source article, rather than a single static field on `dim_company`.

3. **Missing & Undisclosed Revenue**:
   * Missing revenue values are preserved as nulls in `fct_article`, but completely excluded from `fct_arr_observation` so they never introduce false zeroes into financial averages.

4. **Company Size Thresholds**:
   * **Small**: $< 10,000$ employees
   * **Medium**: $10,000$ – $30,000$ employees
   * **Large**: $> 30,000$ employees
