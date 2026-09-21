"""
Silver Layer: Data Cleaning, Standardization & Metadata Enrichment Module
Transforms raw Bronze articles into a cleaned, enriched, and standardized Silver dataset.
"""

import os
import re
from pathlib import Path
import numpy as np
import pandas as pd
from .bronze import get_raw_data, _find_project_root

COMPANY_NAME_MAPPING = {
    'AWS': 'Amazon Web Services',
    'Amazon Web Services (AWS)': 'Amazon Web Services',    
    'CloudFlare': 'Cloudflare',
    'Data Robot': 'DataRobot',
    'Databricks Inc.': 'Databricks',
    'DeepMind': 'Google DeepMind',
    'Google Deepmind': 'Google DeepMind',
    'Facebook AI Research': 'Meta AI',
    'Meta AI Research': 'Meta AI',
    'Azure': 'Microsoft',
    'Microsoft Azure': 'Microsoft',
    'Mongo DB': 'MongoDB',
    'Nvidia': 'NVIDIA',
    'NVIDIA Corporation': 'NVIDIA',
    'Open AI': 'OpenAI',
    'OpenAI Inc.': 'OpenAI',
    'Palantir Technologies': 'Palantir',
    'Snowflake Inc.': 'Snowflake',
    'The Boring Company / SpaceX': 'SpaceX',
    'Stripe Inc.': 'Stripe'
}

CATEGORY_MAPPING = {
    'AI & ML': 'AI_ML',
    'Artificial Intelligence': 'AI_ML',
    'Machine Learning': 'AI_ML',
    'AI/ML': 'AI_ML',
    'Cloud': 'Cloud_Computing',
    'Cloud Services': 'Cloud_Computing',
    'Cloud Computing': 'Cloud_Computing',
    'Cybersecurity': 'Cybersecurity',
    'InfoSec': 'Cybersecurity',
    'Security': 'Cybersecurity',
    'Data Analytics': 'Data_Analytics',
    'Analytics': 'Data_Analytics',
    'Big Data': 'Data_Analytics',
    'FinTech': 'FinTech',
    'Finance': 'FinTech',
    'Financial Technology': 'FinTech',
    'Enterprise Software': 'Enterprise_Software',
    'Software': 'Enterprise_Software',
    'SaaS': 'SaaS'
}

FX_RATES_TO_USD = {
    'USD': 1.0,
    'GBP': 1.27,    # multiply by 1.27
    'EUR': 1.1,     # multiply by 1.1
    'JPY': 1 / 150  # divide by 150
}

def extract_currency(val):
    """Detects currency symbol or code; defaults to USD for undisclosed/unspecified amounts."""
    if pd.isna(val) or not isinstance(val, str):
        return np.nan
    val = val.strip()
    if val.lower() in ['not disclosed', 'unknown', 'n/a', 'none', '-', '', 'nan']:
        return np.nan
    if '£' in val or 'GBP' in val.upper():
        return 'GBP'
    elif '€' in val or 'EUR' in val.upper():
        return 'EUR'
    elif '¥' in val or 'JPY' in val.upper():
        return 'JPY'
    elif '$' in val or 'USD' in val.upper():
        return 'USD'
    return 'USD'

def parse_single_revenue(s):
    """Parses a single revenue text value with multipliers (B, M, K)."""
    if not s or pd.isna(s):
        return np.nan
    s = str(s).strip().lower()
    if s in ['not disclosed', 'unknown', 'n/a', 'none', '-', '', 'nan']:
        return np.nan
    
    multiplier = 1.0
    if 'billion' in s or re.search(r'(\d|\.)\s*b\b', s) or s.endswith('b'):
        multiplier = 1e9
    elif 'million' in s or re.search(r'(\d|\.)\s*m\b', s) or s.endswith('m') or 'm usd' in s:
        multiplier = 1e6
    elif 'thousand' in s or re.search(r'(\d|\.)\s*k\b', s) or s.endswith('k'):
        multiplier = 1e3
        
    num_match = re.search(r'[\d,]+(?:\.\d+)?', s)
    if num_match:
        try:
            return float(num_match.group(0).replace(',', '')) * multiplier
        except ValueError:
            return np.nan
    return np.nan

def parse_revenue(val):
    """
    Parses a revenue entry, taking the midpoint for ranges ($10M - $20M or $10M to $20M).
    Returns raw parsed amount as float or NaN.
    """
    if pd.isna(val) or not isinstance(val, str):
        return np.nan
    val = val.strip()
    if val.lower() in ['not disclosed', 'unknown', 'n/a', 'none', '-', '', 'nan']:
        return np.nan
    if ' - ' in val or ' to ' in val.lower():
        parts = re.split(r'\s+-\s+|\s+to\s+', val, flags=re.IGNORECASE)
        parsed = [parse_single_revenue(p) for p in parts]
        valid = [p for p in parsed if pd.notna(p)]
        return sum(valid) / len(valid) if valid else np.nan
    return parse_single_revenue(val)

def assign_size_category(emp):
    """Categorizes company by employee count according to Task requirements."""
    if pd.isna(emp):
        return 'Unknown'
    elif emp < 10000:
        return 'Small'
    elif emp <= 30000:
        return 'Medium'
    else:
        return 'Large'

def clean_articles_pipeline(df_articles, df_companies):
    """
    Transforms raw articles and company metadata into the standardized Silver dataset.
    
    Args:
        df_articles (pd.DataFrame): Raw articles dataframe
        df_companies (pd.DataFrame): Raw company metadata dataframe
        
    Returns:
        pd.DataFrame: Cleaned, validated, and enriched Silver dataframe
    """
    df_art = df_articles.copy()
    df_comp = df_companies.copy()
    
    # 1. Company Mapping & Validation Flag
    df_art['company_name_clean'] = df_art['company_name'].replace(COMPANY_NAME_MAPPING)
    canonical_set = set(df_comp['company_name'])
    df_art['has_company_metadata'] = df_art['company_name_clean'].isin(canonical_set)
    
    # 2. String Cleaning & Category Mapping
    str_cols = ['article_id', 'title', 'category', 'summary', 'url', 'author']
    for col in str_cols:
        if col in df_art.columns:
            df_art[col] = df_art[col].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan, '': np.nan})
    df_art['category_clean'] = df_art['category'].replace(CATEGORY_MAPPING)
    
    # 3. Dates: standardized to dd-mm-yyyy and extracted temporal dimensions
    # dayfirst=False documents how ambiguous numeric dates (e.g., 02/03/2023) are handled (US MM/DD/YYYY)
    dt = pd.to_datetime(df_art['published_date'], format='mixed', dayfirst=False, utc=True, errors='coerce')
    df_art['published_date_clean'] = dt.dt.strftime('%d-%m-%Y')
    df_art['published_year'] = dt.dt.year.astype('Int64')
    df_art['published_quarter'] = dt.dt.quarter.astype('Int64')
    df_art['published_month'] = dt.dt.month.astype('Int64')
    df_art['published_year_month'] = dt.dt.strftime('%Y-%m')
    
    # 4. Revenue: multi-currency FX to USD, range midpoints, and scale to $M USD (Int64)
    df_art['revenue_currency'] = df_art['revenue'].apply(extract_currency)
    df_art['revenue_clean'] = df_art['revenue'].apply(parse_revenue)
    df_art['revenue_usd'] = df_art.apply(
        lambda r: r['revenue_clean'] * FX_RATES_TO_USD.get(r['revenue_currency'], 1.0) if pd.notna(r['revenue_clean']) else np.nan,
        axis=1
    )
    df_art['revenue_usd_M'] = (df_art['revenue_usd'] / 1e6).round(0).astype('Int64')
    
    # 5. Author, Word Count & Row Ordering
    df_art['author'] = df_art['author'].fillna('Unknown')
    df_art['word_count'] = pd.to_numeric(df_art['word_count'], errors='coerce').astype('Int64')
    df_art['original_index'] = df_art.index.astype('Int64')
    
    # Deterministic chronological sort
    df_art = df_art.sort_values(by=['company_name_clean', 'category_clean', 'author', 'published_date_clean']).reset_index(drop=True)
    
    # 6. Metadata Formatting & Left Join
    df_comp['founded_year'] = pd.to_numeric(df_comp['founded_year'], errors='coerce').astype('Int64')
    df_comp['employee_count'] = pd.to_numeric(df_comp['employee_count'], errors='coerce').astype('Int64')
    df_comp['is_public'] = df_comp['is_public'].astype('boolean')
    
    article_cols = [
        'article_id', 'original_index', 'company_name_clean', 'has_company_metadata', 'title',
        'category', 'category_clean', 'author', 'published_date_clean',
        'published_year', 'published_quarter', 'published_month', 'published_year_month',
        'word_count', 'revenue_usd_M', 'summary', 'url'
    ]
    company_cols = ['industry', 'headquarters', 'founded_year', 'employee_count', 'is_public', 'stock_ticker']
    
    df_merged = pd.merge(
        df_art[article_cols],
        df_comp[['company_name'] + company_cols],
        left_on='company_name_clean',
        right_on='company_name',
        how='left'
    ).drop(columns=['company_name'])
    
    df_merged['founded_year'] = df_merged['founded_year'].astype('Int64')
    df_merged['employee_count'] = df_merged['employee_count'].astype('Int64')
    df_merged['is_public'] = df_merged['is_public'].astype('boolean')
    df_merged['company_age'] = (df_merged['published_year'] - df_merged['founded_year']).astype('Int64')
    df_merged['company_size_category'] = df_merged['employee_count'].apply(assign_size_category)
    
    return df_merged

def run_silver(save_csv=True, output_dir=None):
    """Executes Bronze ingestion followed by Silver transformation, saving to tech_news_clean.csv."""
    root = _find_project_root()
    df_articles_raw, df_companies_raw = get_raw_data()
    df_silver = clean_articles_pipeline(df_articles_raw, df_companies_raw)
    
    if save_csv:
        if output_dir is None:
            output_dir = root / 'data' / 'processed'
        else:
            output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        csv_path = output_dir / 'tech_news_clean.csv'
        df_silver.to_csv(csv_path, index=False)
        print(f"[Silver] Exported {len(df_silver)} rows to '{csv_path}'")
        
    return df_silver

if __name__ == '__main__':
    run_silver()
