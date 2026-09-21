"""
Bronze Layer: Raw Data Ingestion Module
Ingests raw article CSVs and company metadata JSON without destructive mutations.
"""

import glob
import json
import os
from pathlib import Path
import pandas as pd

def _find_project_root():
    """Finds the project root directory containing 'data' or 'source'."""
    current = Path(__file__).resolve().parent
    for p in [current, current.parent, current.parent.parent]:
        if (p / 'data').is_dir() or (p / 'source').is_dir():
            return p
    return Path.cwd()

def get_raw_data(source_dir=None, metadata_file=None):
    """
    Ingests all raw article CSVs and company metadata JSON.
    Dynamically resolves paths relative to project root or specified paths.
    
    Returns:
        tuple: (df_articles_raw: pd.DataFrame, df_companies_raw: pd.DataFrame)
    """
    root = _find_project_root()
    
    # 1. Resolve source directory
    if source_dir is None:
        candidates = [
            root / 'data' / 'raw',
            root / 'source',
            Path('data/raw'),
            Path('source'),
            Path('.')
        ]
        for candidate in candidates:
            if candidate.is_dir():
                csvs = list(candidate.glob('*.csv'))
                # Exclude processed/warehouse output CSVs
                csvs = [
                    f for f in csvs 
                    if not f.name.endswith(('_clean.csv', '_arr.csv', '_enriched.csv')) 
                    and not f.name.startswith(('dim_', 'fct_', 'agg_', 'view_'))
                ]
                if csvs:
                    source_dir = str(candidate)
                    break
        if source_dir is None:
            source_dir = str(root / 'data' / 'raw')

    # 2. Resolve metadata JSON file
    if metadata_file is None:
        candidates = [
            Path(source_dir) / 'company_metadata.json',
            root / 'data' / 'raw' / 'company_metadata.json',
            root / 'source' / 'company_metadata.json',
            Path('data/raw/company_metadata.json'),
            Path('source/company_metadata.json'),
            Path('company_metadata.json')
        ]
        for candidate in candidates:
            if candidate.is_file():
                metadata_file = str(candidate)
                break
        if metadata_file is None:
            metadata_file = str(root / 'data' / 'raw' / 'company_metadata.json')

    # 3. Find and read all raw CSVs
    raw_path = Path(source_dir)
    csv_files = sorted([
        f for f in raw_path.glob('*.csv')
        if not f.name.endswith(('_clean.csv', '_arr.csv', '_enriched.csv'))
        and not f.name.startswith(('dim_', 'fct_', 'agg_', 'view_'))
    ])

    if not csv_files:
        raise FileNotFoundError(f"No raw CSV files found in '{source_dir}' directory.")

    # Concatenate all raw CSV files
    df_articles_raw = pd.concat([pd.read_csv(f, encoding='utf-8') for f in csv_files], ignore_index=True)
    if 'article_id' in df_articles_raw.columns:
        df_articles_raw = df_articles_raw.drop_duplicates(subset=['article_id']).reset_index(drop=True)

    # Load raw Company Metadata JSON
    with open(metadata_file, 'r', encoding='utf-8') as f:
        metadata_dict = json.load(f)
    df_companies_raw = (
        pd.DataFrame.from_dict(metadata_dict, orient='index')
        .reset_index()
        .rename(columns={'index': 'company_name'})
    )

    return df_articles_raw, df_companies_raw

if __name__ == '__main__':
    articles, companies = get_raw_data()
    print(f"[Bronze] Ingested {len(articles)} raw articles and {len(companies)} company metadata records.")
