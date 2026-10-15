"""
Gold Layer: Dimensional Warehouse Modeling & Deliverable Export Module
Builds dimensional tables (dim_company, fct_article, fct_arr_observation), business aggregations,
and downstream exports (ai_articles_enriched.csv).
"""

import os
from pathlib import Path
import pandas as pd
from .bronze import _find_project_root

def generate_company_id_map(df_silver):
    """Generates a deterministic mapping of canonical company name to surrogate ID (COMP001, ...)."""
    companies = sorted(df_silver['company_name_clean'].unique())
    return {name: f"COMP{i+1:03d}" for i, name in enumerate(companies)}

def build_dim_company(df_silver, company_id_map):
    """
    Builds the Company Dimension table (1 row per canonical company).
    Grain: Exactly 1 row per company.
    """
    dim_company = df_silver.groupby('company_name_clean').agg({
        'industry': 'first',
        'headquarters': 'first',
        'founded_year': 'first',
        'employee_count': 'first',
        'company_size_category': 'first',
        'is_public': 'first',
        'stock_ticker': 'first',
        'has_company_metadata': 'first'
    }).reset_index()
    
    dim_company.insert(0, 'company_id', dim_company['company_name_clean'].map(company_id_map))
    dim_company.rename(columns={'company_name_clean': 'company_name'}, inplace=True)
    return dim_company

def build_fct_article(df_silver, company_id_map):
    """
    Builds the Article Fact table (1 row per news article).
    Grain: Exactly 1 row per tech news article.
    """
    fct_article = df_silver[[
        'article_id', 'original_index', 'company_name_clean', 'title',
        'category', 'category_clean', 'author', 'published_date_clean',
        'published_year', 'published_quarter', 'published_month', 'published_year_month',
        'word_count', 'summary', 'url'
    ]].copy()
    
    fct_article.insert(2, 'company_id', fct_article['company_name_clean'].map(company_id_map))
    fct_article.drop(columns=['company_name_clean'], inplace=True)
    return fct_article

def build_fct_arr_observation(df_silver, company_id_map):
    """
    Builds the ARR Observation Fact table (1 row per valid financial revenue observation).
    Grain: Exactly 1 row per valid financial point-in-time observation.
    Filters out missing and undisclosed revenues to preserve metric integrity.
    """
    valid_arr = df_silver[df_silver['revenue_usd_M'].notnull()].copy()
    fct_arr_observation = pd.DataFrame({
        'observation_id': [f"ARR{i+1:04d}" for i in range(len(valid_arr))],
        'article_id': valid_arr['article_id'].values,
        'company_id': valid_arr['company_name_clean'].map(company_id_map).values,
        'observation_date': valid_arr['published_date_clean'].values,
        'observation_year': valid_arr['published_year'].values,
        'observation_quarter': valid_arr['published_quarter'].values,
        'observation_year_month': valid_arr['published_year_month'].values,
        'arr_usd_M': valid_arr['revenue_usd_M'].values,
        'arr_usd': (valid_arr['revenue_usd_M'].values * 1_000_000).astype('Int64')
    })
    return fct_arr_observation

def build_agg_company_quarterly_arr(fct_arr_observation, dim_company):
    """
    Builds quarterly aggregated ARR rollups per company.
    Grain: 1 row per company per calendar year-quarter.
    """
    agg_quarterly = fct_arr_observation.groupby(['company_id', 'observation_year', 'observation_quarter']).agg(
        observations_count=('observation_id', 'count'),
        latest_arr_usd_M=('arr_usd_M', 'last'),
        avg_arr_usd_M=('arr_usd_M', lambda x: round(x.mean(), 1)),
        max_arr_usd_M=('arr_usd_M', 'max'),
        min_arr_usd_M=('arr_usd_M', 'min')
    ).reset_index()
    
    agg_quarterly = pd.merge(
        agg_quarterly,
        dim_company[['company_id', 'company_name', 'industry']],
        on='company_id',
        how='left'
    )
    return agg_quarterly

def build_view_company_latest_arr(fct_arr_observation, dim_company):
    """
    Builds the latest known ARR view per company.
    Grain: 1 row per company showing the most recent ARR metric available.
    """
    view_latest = fct_arr_observation.sort_values(
        by=['company_id', 'observation_year', 'observation_quarter']
    ).groupby('company_id').agg(
        latest_observation_id=('observation_id', 'last'),
        latest_article_id=('article_id', 'last'),
        latest_observation_date=('observation_date', 'last'),
        latest_arr_usd_M=('arr_usd_M', 'last'),
        total_observations_recorded=('observation_id', 'count')
    ).reset_index()
    
    view_latest = pd.merge(
        dim_company[['company_id', 'company_name', 'industry', 'company_size_category', 'is_public']],
        view_latest,
        on='company_id',
        how='left'
    )
    return view_latest

def build_ai_articles_enriched(df_silver):
    """
    Exports required downstream AI Article Dataset as per Section 3 of Task.txt.
    Criteria:
      - category indicates AI/ML OR company industry indicates AI/ML
      - published between 2022 and 2024
      - valid ARR > $50M USD
    """
    ai_mask = (
        ((df_silver['category_clean'] == 'AI_ML') | (df_silver['industry'] == 'AI/ML')) &
        (df_silver['published_year'] >= 2022) &
        (df_silver['published_year'] <= 2024) &
        (df_silver['revenue_usd_M'] > 50)
    )
    df_ai = df_silver[ai_mask].copy()
    df_ai['company_name'] = df_ai['company_name_clean']
    df_ai['published_date'] = df_ai['published_date_clean']
    df_ai['arr_usd'] = (df_ai['revenue_usd_M'] * 1_000_000).astype('Int64')
    df_ai['category'] = df_ai['category_clean']
    df_ai['embedding'] = "[]"
    
    ai_cols = [
        'article_id', 'title', 'company_name', 'published_date', 'category',
        'arr_usd', 'summary', 'url', 'industry', 'founded_year',
        'headquarters', 'employee_count', 'is_public', 'stock_ticker',
        'company_age', 'company_size_category', 'embedding'
    ]
    return df_ai[ai_cols].reset_index(drop=True)

def build_and_export_warehouse(df_silver, warehouse_dir=None, processed_dir=None):
    """
    Builds the complete Gold layer warehouse dimensional model and exports all deliverables as CSVs.
    
    Args:
        df_silver (pd.DataFrame): The cleaned and enriched Silver dataframe.
        warehouse_dir (str or Path, optional): Directory to save warehouse CSVs.
        processed_dir (str or Path, optional): Directory to save processed Silver CSV.
        
    Returns:
        dict: Mapping of destination file paths to exported DataFrames.
    """
    root = _find_project_root()
    if warehouse_dir is None:
        warehouse_dir = root / 'data' / 'warehouse'
    else:
        warehouse_dir = Path(warehouse_dir)
        
    if processed_dir is None:
        processed_dir = root / 'data' / 'processed'
    else:
        processed_dir = Path(processed_dir)
        
    warehouse_dir.mkdir(parents=True, exist_ok=True)
    processed_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate deterministic company ID mappings
    company_id_map = generate_company_id_map(df_silver)
    
    # 1. Dimension & Fact Tables
    dim_company = build_dim_company(df_silver, company_id_map)
    fct_article = build_fct_article(df_silver, company_id_map)
    fct_arr_observation = build_fct_arr_observation(df_silver, company_id_map)
    
    # 2. Aggregations and Views
    agg_quarterly = build_agg_company_quarterly_arr(fct_arr_observation, dim_company)
    view_latest = build_view_company_latest_arr(fct_arr_observation, dim_company)
    
    # 3. Downstream AI Deliverable
    ai_articles_enriched = build_ai_articles_enriched(df_silver)
    
    exports = {
        processed_dir / 'tech_news_clean.csv': df_silver,
        warehouse_dir / 'dim_company.csv': dim_company,
        warehouse_dir / 'fct_article.csv': fct_article,
        warehouse_dir / 'fct_arr_observation.csv': fct_arr_observation,
        warehouse_dir / 'agg_company_quarterly_arr.csv': agg_quarterly,
        warehouse_dir / 'view_company_latest_arr.csv': view_latest,
        warehouse_dir / 'ai_articles_enriched.csv': ai_articles_enriched
    }
    
    for fpath, df_exp in exports.items():
        try:
            df_exp.to_csv(fpath, index=False)
            print(f"[Gold] Exported '{fpath.name}' ({len(df_exp)} rows x {len(df_exp.columns)} cols)")
        except PermissionError:
            print(f"[WARN] '{fpath.name}' is currently open in another program. Please close to overwrite.")
            
    return exports

if __name__ == '__main__':
    from .silver import run_silver
    df_silver = run_silver(save_csv=False)
    build_and_export_warehouse(df_silver)
