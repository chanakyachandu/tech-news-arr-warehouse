"""
Automated Pytest Suite for Medallion Data Platform
Validates data ingestion, cleaning transformations, FX math, dimensional warehouse integrity, and downstream AI deliverable criteria.
"""

import math
import numpy as np
import pandas as pd
import pytest

from medallion.bronze import get_raw_data
from medallion.silver import (
    extract_currency,
    parse_single_revenue,
    parse_revenue,
    assign_size_category,
    clean_articles_pipeline,
    FX_RATES_TO_USD,
    COMPANY_NAME_MAPPING,
    CATEGORY_MAPPING
)
from medallion.gold import (
    build_and_export_warehouse,
    generate_company_id_map,
    build_dim_company,
    build_fct_article,
    build_fct_arr_observation,
    build_ai_articles_enriched
)

# -------------------------------------------------------------------------
# 1. Silver Transformation Unit Tests
# -------------------------------------------------------------------------

def test_currency_extraction():
    """Validates detection of multi-currency symbols and codes."""
    assert extract_currency("£244,094,488") == 'GBP'
    assert extract_currency("€1,254,545,455") == 'EUR'
    assert extract_currency("¥360,000,000,000") == 'JPY'
    assert extract_currency("$980.0M") == 'USD'
    assert extract_currency("500M USD") == 'USD'
    assert pd.isna(extract_currency("Not disclosed"))
    assert pd.isna(extract_currency("N/A"))
    assert pd.isna(extract_currency(np.nan))

def test_revenue_parsing_formats():
    """Validates single revenue string parsing across varied scales (B, M, K)."""
    assert parse_single_revenue("$5,200,000,000") == 5_200_000_000.0
    assert parse_single_revenue("5.2B") == 5_200_000_000.0
    assert parse_single_revenue("5.2 billion") == 5_200_000_000.0
    assert parse_single_revenue("500M USD") == 500_000_000.0
    assert parse_single_revenue("$170.0M") == 170_000_000.0
    assert pd.isna(parse_single_revenue("Not disclosed"))

def test_revenue_range_midpoint():
    """Validates midpoint calculation for ranges as per Task.txt requirement."""
    # $10M - $20M midpoint should be $15M
    assert parse_revenue("$10M - $20M") == 15_000_000.0
    # $100M to $200M midpoint should be $150M
    assert parse_revenue("$100M to $200M") == 150_000_000.0

def test_fx_conversion_rates():
    """Validates required exchange rates for EUR, GBP, and JPY."""
    assert FX_RATES_TO_USD['EUR'] == 1.1
    assert FX_RATES_TO_USD['GBP'] == 1.27
    assert FX_RATES_TO_USD['JPY'] == pytest.approx(1 / 150)

def test_company_size_categorization():
    """Validates company employee count thresholds."""
    assert assign_size_category(5000) == 'Small'
    assert assign_size_category(9999) == 'Small'
    assert assign_size_category(10000) == 'Medium'
    assert assign_size_category(30000) == 'Medium'
    assert assign_size_category(30001) == 'Large'
    assert assign_size_category(np.nan) == 'Unknown'

def test_category_taxonomy_mapping():
    """Validates that similar category labels correctly map to canonical taxonomy."""
    for ai_cat in ['AI & ML', 'Artificial Intelligence', 'Machine Learning', 'AI/ML']:
        assert CATEGORY_MAPPING.get(ai_cat) == 'AI_ML'
    assert CATEGORY_MAPPING.get('Cloud Services') == 'Cloud_Computing'
    assert CATEGORY_MAPPING.get('Financial Technology') == 'FinTech'

def test_company_alias_resolution():
    """Validates alias mapping for primary enterprise variations."""
    assert COMPANY_NAME_MAPPING.get('AWS') == 'Amazon Web Services'
    assert COMPANY_NAME_MAPPING.get('Azure') == 'Microsoft'
    assert COMPANY_NAME_MAPPING.get('DeepMind') == 'Google DeepMind'
    assert COMPANY_NAME_MAPPING.get('Mongo DB') == 'MongoDB'

# -------------------------------------------------------------------------
# 2. End-to-End Pipeline & Warehouse Integrity Tests
# -------------------------------------------------------------------------

@pytest.fixture(scope="module")
def pipeline_data():
    """Ingests raw data and executes Silver transformation once for tests."""
    df_raw_articles, df_raw_companies = get_raw_data()
    df_silver = clean_articles_pipeline(df_raw_articles, df_raw_companies)
    company_id_map = generate_company_id_map(df_silver)
    
    dim_company = build_dim_company(df_silver, company_id_map)
    fct_article = build_fct_article(df_silver, company_id_map)
    fct_arr_observation = build_fct_arr_observation(df_silver, company_id_map)
    ai_articles_enriched = build_ai_articles_enriched(df_silver)
    
    return {
        'df_silver': df_silver,
        'dim_company': dim_company,
        'fct_article': fct_article,
        'fct_arr_observation': fct_arr_observation,
        'ai_articles_enriched': ai_articles_enriched
    }

def test_silver_structure(pipeline_data):
    """Asserts that Silver transformation preserves all 750 articles and produces required columns."""
    df_silver = pipeline_data['df_silver']
    assert len(df_silver) == 750
    expected_cols = [
        'article_id', 'company_name_clean', 'has_company_metadata',
        'category_clean', 'published_date_clean', 'published_year',
        'revenue_usd_M', 'company_age', 'company_size_category'
    ]
    for col in expected_cols:
        assert col in df_silver.columns

def test_dim_company_grain(pipeline_data):
    """Asserts that dim_company has exactly 1 row per canonical company and unique PK."""
    dim_comp = pipeline_data['dim_company']
    assert dim_comp['company_id'].is_unique
    assert dim_comp['company_name'].is_unique
    assert len(dim_comp) == 26

def test_referential_integrity(pipeline_data):
    """Asserts that all foreign keys in fact tables resolve to dim_company primary keys."""
    dim_comp = pipeline_data['dim_company']
    fct_art = pipeline_data['fct_article']
    fct_arr = pipeline_data['fct_arr_observation']
    
    valid_company_ids = set(dim_comp['company_id'])
    assert fct_art['company_id'].isin(valid_company_ids).all()
    assert fct_arr['company_id'].isin(valid_company_ids).all()
    
    valid_article_ids = set(fct_art['article_id'])
    assert fct_arr['article_id'].isin(valid_article_ids).all()

def test_fct_arr_observation_integrity(pipeline_data):
    """Asserts that fct_arr_observation contains only valid non-null positive revenue observations."""
    fct_arr = pipeline_data['fct_arr_observation']
    assert fct_arr['observation_id'].is_unique
    assert (fct_arr['arr_usd_M'] > 0).all()
    assert (fct_arr['arr_usd'] > 0).all()
    assert fct_arr['arr_usd_M'].notnull().all()

def test_ai_articles_enriched_criteria(pipeline_data):
    """
    Validates Section 3 requirements of Task.txt:
    1. Category or Industry indicates AI/ML.
    2. Published between 2022 and 2024.
    3. Valid ARR > $50M USD.
    4. Exact 17 required columns.
    """
    ai_df = pipeline_data['ai_articles_enriched']
    
    # 17 columns check
    expected_17_cols = [
        'article_id', 'title', 'company_name', 'published_date', 'category',
        'arr_usd', 'summary', 'url', 'industry', 'founded_year',
        'headquarters', 'employee_count', 'is_public', 'stock_ticker',
        'company_age', 'company_size_category', 'embedding'
    ]
    assert list(ai_df.columns) == expected_17_cols
    
    # Positive volume
    assert len(ai_df) > 0
    
    # Criteria validation
    assert (ai_df['arr_usd'] > 50_000_000).all()
    
    # Year between 2022 and 2024
    years = pd.to_datetime(ai_df['published_date'], format='%d-%m-%Y').dt.year
    assert (years >= 2022).all()
    assert (years <= 2024).all()
    
    # AI Category or Industry
    ai_condition = (ai_df['category'] == 'AI_ML') | (ai_df['industry'] == 'AI/ML')
    assert ai_condition.all()
