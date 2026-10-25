# 🗺️ Part 1: Tech News & Company ARR Lakehouse — Comprehensive Refresher

**Master Orchestrators:** 
- **Lakehouse Pipeline:** `pipeline.py` (`run_part1_pipeline()`)
- **AI Text-to-SQL:** `text_to_sql.py` (`ai_text_to_sql/cli.py`)
- **Semantic Search:** `advanced_semantic_search/semantic_search.ipynb`

---

## 📊 End-to-End System Architecture

```
                    [ Raw Tech News CSVs & Company Metadata JSON ]
                                          │
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│                     1. BRONZE LAYER: INGESTION (bronze.py)                    │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ • Scans data/raw/ or source/ via _find_project_root()                   │  │
│  │ • Ingests df_raw_articles (750 rows) & df_raw_meta (50+ companies)      │  │
│  │ • Immutable append-only raw state: zero mutations or type conversions   │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          │ (Raw Ingested DataFrames)
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│              2. SILVER LAYER: CLEANING & ENRICHMENT (silver.py)               │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ • Entity Resolution: Maps 20+ company variants to canonical names       │  │
│  │ • Currency Normalization: Converts foreign EUR, GBP, CAD ARR to USD     │  │
│  │ • Category Standardization: Normalizes tags into clean taxonomy (AI_ML) │  │
│  │ • Temporal Parsing: Extracts published_year, quarter, YYYY-MM-DD date   │  │
│  │ • Deduplication & Missing Value Imputation                              │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          │ (Cleaned & Standardized df_silver)
                                          ▼
┌───────────────────────────────────────────────────────────────────────────────┐
│               3. GOLD LAYER: DIMENSIONAL STAR SCHEMA (gold.py)                │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │ • dim_company: Canonical company dimension (Grain: 1 row per company)   │  │
│  │   Surrogate Keys: Deterministic alphanumeric IDs (COMP001, COMP002...)  │  │
│  ├─────────────────────────────────────────────────────────────────────────┤  │
│  │ • fct_article: Article facts (Grain: 1 row per tech news article)       │  │
│  │ • fct_arr_observation: Financial milestones (Grain: 1 row per event)    │  │
│  ├─────────────────────────────────────────────────────────────────────────┤  │
│  │ • Deliverable Exports: Materializes warehouse CSVs into data/gold/       │  │
│  └─────────────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────────────┘
                                          │
                     ┌────────────────────┴────────────────────┐
                     ▼                                         ▼
┌────────────────────────────────────────┐ ┌───────────────────────────────────┐
│    4. AI TEXT-TO-SQL (ai_text_to_sql)  │ │  5. ADVANCED SEMANTIC SEARCH      │
│  ┌──────────────────────────────────┐  │ │  ┌─────────────────────────────┐  │
│  │ • schema_provider.py             │  │ │  │ • article_embeddings.npy    │  │
│  │   Bootstraps DuckDB & DDL schemas│  │ │  │   384-dim dense embeddings  │  │
│  │ • sql_generator.py (Gemini LLM)  │  │ │  │ • semantic_search.ipynb     │  │
│  │   Translates natural text to SQL │  │ │  │   Cosine similarity search  │  │
│  │ • sql_guard.py (Security Gate)   │  │ │  │ • Hybrid Retrieval          │  │
│  │   AST checks: Blocks DROP/DELETE │  │ │  │   Combines SQL filters +    │  │
│  │   SELECT-only, auto LIMIT 100    │  │ │  │   dense vector similarity   │  │
│  └──────────────────────────────────┘  │ │  └─────────────────────────────┘  │
└────────────────────────────────────────┘ └───────────────────────────────────┘
```

---

## 📋 Component & Subsystem Code Map

### 1. Core Medallion Lakehouse Pipeline

#### bronze.py
- **Core Functions**: `get_raw_data()`, `_find_project_root()`
- **Role & Mechanics**: Dynamically locates raw CSVs and company metadata. Ingests raw data without mutation to preserve an immutable audit trail.

#### silver.py
- **Core Functions**: `clean_articles_pipeline()`, `COMPANY_NAME_MAPPING`, `CATEGORY_MAPPING`
- **Role & Mechanics**: Cleans messy text, maps company name variants (e.g., "AWS" ➔ "Amazon Web Services") to canonical entities, parses dates, normalizes EUR/GBP currencies to USD, and handles missing values.

#### gold.py
- **Core Functions**: `build_and_export_warehouse()`, `build_dim_company()`, `build_fct_article()`, `build_fct_arr_observation()`
- **Role & Mechanics**: Generates deterministic surrogate keys (`COMP001`...), establishes dimensional star-schema relationships, and exports warehouse CSVs to `data/warehouse/`.

#### pipeline.py
- **Core Functions**: `run_part1_pipeline()`
- **Role & Mechanics**: Master orchestrator that executes Bronze ➔ Silver ➔ Gold sequentially with sub-second execution telemetry.

---

### 2. AI Text-to-SQL Subsystem (`ai_text_to_sql/`)

#### schema_provider.py
- **Core Functions**: `init_duckdb_warehouse()`, `get_warehouse_schema_context()`
- **Role & Mechanics**: Loads Gold tables into an in-memory DuckDB instance and extracts table DDL, types, and sample data to inject schema context into LLM prompts.

#### sql_generator.py
- **Core Functions**: `generate_sql_query()`, `explain_query_results()`, `get_gemini_client()`
- **Role & Mechanics**: Sends prompts with schema context to the Gemini API (`gemini-2.5-flash`), generates ANSI/DuckDB SQL from user questions, and writes a plain English summary of query results.

#### sql_guard.py
- **Core Functions**: `clean_sql_query()`, `validate_sql_safety()`, `enforce_row_limit()`
- **Role & Mechanics**: Security gate that strips markdown fences, blocks multi-statement injection (`SELECT 1; DROP TABLE...`), rejects forbidden keywords (`DROP`, `DELETE`, `UPDATE`, `ALTER`, `TRUNCATE`), enforces strict `SELECT`-only execution, and auto-appends `LIMIT 100`.

#### cli.py & text_to_sql.py
- **Core Functions**: `main()`
- **Role & Mechanics**: Interactive terminal interface (`DataTalker AI`) allowing users to query the lakehouse conversationally in natural language.

---

### 3. Advanced Semantic Search Subsystem (`advanced_semantic_search/`)

#### Pre-computed Embeddings (`data/embeddings/article_embeddings.npy`)
- **Role & Mechanics**: 384-dimensional dense feature vectors for 750 tech news articles derived from concatenated `title + summary`.

#### Vector Similarity Search (`semantic_search.ipynb` -> `find_similar_articles`)
- **Role & Mechanics**: Computes cosine similarity between query vector and article vectors to find conceptually relevant articles even without exact keyword matches.

#### Nearest-Neighbor Recommendations (`semantic_search.ipynb` -> `top_similar_articles`)
- **Role & Mechanics**: Generates an item-to-item similarity matrix across articles for automated content recommendations.

#### Hybrid Query Engine (`semantic_search.ipynb` -> `hybrid_search`)
- **Role & Mechanics**: Combines relational SQL filtering (`category == 'AI_ML'` and `year >= 2024`) with dense vector ranking to return top semantic hits.

#### Enriched Dataset Materialization (`data/warehouse/ai_articles_enriched.csv`)
- **Role & Mechanics**: Exports the enriched article dataset with canonical company metadata and embedding vectors for downstream analytics and modeling.

---

### 4. Interactive Development Notebooks (`notebooks/`)

#### 01_bronze.ipynb
- **Role & Validation**: Inspects raw CSV schemas, null counts, column anomalies, and metadata JSON structure.

#### 02_silver.ipynb
- **Role & Validation**: Interactively tests entity resolution mappings, currency conversion rates, and category standardization.

#### 03_gold.ipynb
- **Role & Validation**: Validates star schema integrity, surrogate key uniqueness, and foreign key relationships between facts and dimensions.

#### 04_sql_queries.ipynb
- **Role & Validation**: Runs analytical SQL benchmarks (window functions, YoY growth, ARR per employee) over in-memory DuckDB.

#### 05_ai_text_to_sql.ipynb
- **Role & Validation**: Interactive test sandbox for generating SQL from plain English and testing security guardrail rejection behavior.

---

## 🔍 Deep-Dive Technical Mechanics

### 1. Entity Resolution Engine
* **The Problem:** Scraped tech news mentions companies inconsistently (`"AWS"`, `"Amazon Web Services (AWS)"`, `"AWS Inc."`).
* **The Solution:** A dictionary-driven resolution dictionary (`COMPANY_NAME_MAPPING`) converts raw variants into canonical keys prior to Gold surrogate key assignment.
* **Why it matters:** Guarantees that fact records always link to a single conformed `company_id` without splintering company metrics.

### 2. Multi-Currency Normalization (FX Engine)
* **The Problem:** Global companies report financial numbers and ARR in local currencies (EUR, GBP, CAD).
* **The Solution:** Extracts currency symbols and applies standardized conversion rates:
  * `EUR -> USD (* 1.08)`
  * `GBP -> USD (* 1.28)`
  * `CAD -> USD (* 0.74)`
* **Result:** `arr_usd` in `fct_arr_observation` is conformed to a single currency denomination for accurate comparative aggregations.

### 3. Star Schema Grain & Dimensional Integrity
* **`dim_company` (Dimension):**
  * **Grain:** Exactly 1 row per canonical company.
  * **Primary Key:** `company_id` (deterministic surrogate key: `COMP001`, `COMP002`...).
  * **Attributes:** `industry`, `headquarters`, `founded_year`, `employee_count`, `company_size_category`, `is_public`, `stock_ticker`.
* **`fct_article` (Fact):**
  * **Grain:** Exactly 1 row per unique news article.
  * **Foreign Key:** `company_id` referencing `dim_company`.
  * **Metrics:** Publication dates, quarter, category taxonomy, sentiment score.
* **`fct_arr_observation` (Fact):**
  * **Grain:** Exactly 1 row per reported financial ARR milestone.
  * **Foreign Key:** `company_id` referencing `dim_company`.
  * **Metrics:** `arr_usd`, `reporting_date`, `growth_rate`.

### 4. Text-to-SQL Guardrails & AST Security
* **Multi-Statement Blocking:** Detects and halts semicolon-delimited queries (`SELECT 1; DROP TABLE dim_company`) by stripping quoted string literals before tokenizing statements.
* **Command Whitelisting:** Regex scanning enforces that queries must begin strictly with `SELECT` or `WITH ... SELECT`.
* **Resource Protection:** Inspects the query for `LIMIT`; if absent, it automatically appends `LIMIT 100` to prevent memory exhaustion on DuckDB.
* **Zero Mutation Guarantee:** Any query containing `DROP`, `DELETE`, `UPDATE`, `INSERT`, `ALTER`, `TRUNCATE`, `COPY`, `INSTALL`, or `PRAGMA` is immediately rejected before execution.

### 5. Semantic Vector Indexing & Hybrid Search
* **Embedding Matrix:** Stores 384-dimensional dense vectors in a binary `.npy` format for high-speed linear algebra operations with NumPy.
* **Hybrid Search Flow:**
  1. SQL metadata filter trims the candidate pool (e.g. filter by `company_id` or date range).
  2. Cosine dot product calculates semantic similarity across remaining candidates.
  3. Returns top-$k$ articles ranked by combined semantic relevance and metadata recency.
