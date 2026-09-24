# 📰 Tech News & ARR Analytical Data Platform

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue.svg)](https://www.python.org/)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%E2%86%92Silver%E2%86%92Gold)-orange.svg)](DATA_ARCHITECTURE.md)
[![Data Warehouse](https://img.shields.io/badge/Warehouse-Dimensional%20Star%20Schema-green.svg)](DATA_ARCHITECTURE.md)
[![Tests](https://img.shields.io/badge/Tests-12%20Passing-brightgreen.svg)](tests/test_pipeline.py)
[![SQL](https://img.shields.io/badge/Query%20Engine-DuckDB%20%7C%20SQLite-yellow.svg)](notebooks/04_sql_queries.ipynb)

An end-to-end data platform implementing the **Medallion Architecture** (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`) for tech news ingestion, company entity resolution, multi-currency ARR normalization, and dimensional star-schema modeling.

---

## 📁 Repository Structure

```
yipit/
├── medallion/                         # ⚙️ Core Medallion Architecture Python Package
│   ├── __init__.py                    # Exposes pipeline APIs
│   ├── bronze.py                      # Bronze layer: Ingestion & schema validation
│   ├── silver.py                      # Silver layer: Cleaning, entity resolution & enrichment
│   └── gold.py                        # Gold layer: Star schema modeling, rollups & exports
│
├── notebooks/                         # 📓 Interactive Walkthrough & Demonstration Notebooks
│   ├── python_syntax_guide.ipynb      # 🐍 Python Complete Syntax Handbook (Index & Executable cells)
│   ├── sql_syntax_guide.ipynb         # 🗄️ SQL Complete Syntax Handbook (Index & Executable queries)
│   ├── 01_bronze.ipynb                # Ingestion & raw data profiling
│   ├── 02_silver.ipynb                # Cleaning, FX normalization & entity resolution
│   ├── 03_gold.ipynb                  # Relational star schema warehouse & aggregations
│   └── 04_sql_queries.ipynb           # SQL analytics (DuckDB & SQLite) over warehouse tables
│
├── tests/                             # 🧪 Automated Test Suite (pytest)
│   ├── __init__.py
│   └── test_pipeline.py               # Data quality, FX math, table grain & referential integrity
│
├── advanced_semantic_search/          # 🤖 Advanced Vector Search & Embeddings Module
│   ├── semantic_search.ipynb          # Cosine similarity, vector search & hybrid filtering
│   ├── README.md                      # Semantic search documentation
│   └── data/embeddings/              # Pre-computed dense vector matrix (.npy)
│
├── pipeline.py                        # 🚀 End-to-end CLI pipeline orchestrator
├── DATA_ARCHITECTURE.md               # Dimensional star schema specifications & governance
├── README.md                          # Documentation and portfolio guide
├── Task.txt                          # Project specification document
├── requirements.txt                   # Pinned Python dependencies
├── .gitignore                         # Git exclusion rules for clean commits
│
└── data/                              # 💾 Data Storage
    ├── raw/                           # Source CSVs & canonical metadata
    │   ├── tech_news.csv
    │   └── company_metadata.json
    ├── processed/                     # Cleaned Silver dataset
    │   └── tech_news_clean.csv
    └── warehouse/                     # Gold relational warehouse tables & deliverables
        ├── dim_company.csv            # Company dimension (N = 26)
        ├── fct_article.csv            # Article fact table (N = 750)
        ├── fct_arr_observation.csv    # ARR point-in-time observations (N = 558)
        ├── agg_company_quarterly_arr.csv # Quarterly rollups (N = 315)
        ├── view_company_latest_arr.csv   # Latest ARR snapshot per company (N = 26)
        └── ai_articles_enriched.csv   # Downstream AI deliverable (N = 124)
```

---

## 🚀 1. System Requirements & Installation

### Requirements
* **Python**: 3.9+ (Tested on Python 3.12 & 3.13)
* **OS**: Windows, macOS, Linux
* **Memory**: Minimum 4GB RAM

### Installation
Clone the repository and install dependencies:
```bash
pip install -r requirements.txt
```

---

## ⚡ 2. How to Run the Pipeline

### Single Command Execution (End-to-End Orchestrator)
Execute the entire Medallion pipeline from ingestion through warehouse export:
```bash
python pipeline.py
```

### Running Individual Medallion Stages
Each medallion stage can also be executed independently as a standalone module:
```bash
python -m medallion.bronze   # Run Bronze raw ingestion
python -m medallion.silver   # Run Silver cleaning & export tech_news_clean.csv
python -m medallion.gold     # Run Gold dimensional modeling & exports
```

---

## 🧪 3. Running Automated Tests

A comprehensive `pytest` test suite validates currency parsing, range midpoints, category taxonomies, star schema grains, and referential integrity:

```bash
pytest tests/ -v
```

Output:
```text
tests/test_pipeline.py::test_currency_extraction PASSED
tests/test_pipeline.py::test_revenue_parsing_formats PASSED
tests/test_pipeline.py::test_revenue_range_midpoint PASSED
tests/test_pipeline.py::test_fx_conversion_rates PASSED
tests/test_pipeline.py::test_company_size_categorization PASSED
tests/test_pipeline.py::test_category_taxonomy_mapping PASSED
tests/test_pipeline.py::test_company_alias_resolution PASSED
tests/test_pipeline.py::test_silver_structure PASSED
tests/test_pipeline.py::test_dim_company_grain PASSED
tests/test_pipeline.py::test_referential_integrity PASSED
tests/test_pipeline.py::test_fct_arr_observation_integrity PASSED
tests/test_pipeline.py::test_ai_articles_enriched_criteria PASSED
============================= 12 passed in 0.65s ==============================
```

---

## 🔍 4. Analytical SQL Queries

You can execute analytical queries directly over the warehouse CSVs using **DuckDB** or **SQLite**:

### 1. View ARR Observations for a Company Over Time
```sql
SELECT 
    c.company_name,
    c.industry,
    a.observation_year,
    a.observation_quarter,
    a.arr_usd_M
FROM fct_arr_observation a
JOIN dim_company c ON a.company_id = c.company_id
WHERE c.company_name = 'Databricks'
ORDER BY a.observation_year, a.observation_quarter;
```

### 2. Find Source Article for an ARR Observation
```sql
SELECT 
    o.observation_id,
    o.arr_usd_M,
    a.title,
    a.published_date_clean,
    a.url
FROM fct_arr_observation o
JOIN fct_article a ON o.article_id = a.article_id
WHERE o.observation_id = 'ARR0001';
```

### 3. Industry Performance Aggregations
```sql
SELECT 
    c.industry,
    COUNT(DISTINCT c.company_id) AS total_companies,
    COUNT(a.article_id) AS total_articles,
    ROUND(AVG(o.arr_usd_M), 1) AS avg_arr_usd_M,
    MAX(o.arr_usd_M) AS max_arr_usd_M
FROM dim_company c
LEFT JOIN fct_article a ON c.company_id = a.company_id
LEFT JOIN fct_arr_observation o ON a.article_id = o.article_id
GROUP BY c.industry
ORDER BY total_articles DESC;
```

---

## 🏛️ 5. Data Architecture & Governance

For full architectural designs, dimensional star schema specifications, surrogate key rationale, and cloud scaling patterns:
* See **[DATA_ARCHITECTURE.md](DATA_ARCHITECTURE.md)**.
