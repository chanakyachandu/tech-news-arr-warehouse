"""
Tech News & ARR Data Warehouse - End-to-End Pipeline Orchestrator

Executes the complete Medallion Architecture data platform:
1. Bronze Layer: Ingests raw articles and canonical company metadata.
2. Silver Layer: Cleans currencies, parses dates, canonicalizes entities & enriches features.
3. Gold Layer: Builds dimensional star-schema warehouse tables, aggregations, views, and downstream AI deliverables.
"""

import sys
import time
from pathlib import Path

# Add current directory to path to ensure medallion package is discoverable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from medallion.bronze import get_raw_data
from medallion.silver import clean_articles_pipeline
from medallion.gold import build_and_export_warehouse

def run_pipeline():
    """Runs the end-to-end Medallion pipeline and outputs all warehouse deliverables."""
    start_time = time.time()
    
    print("=" * 60)
    print("Running Tech News & ARR Medallion Data Pipeline")
    print("=" * 60)
    
    # 1. Bronze Layer
    print("\n[1/3] [Bronze] Ingesting raw data...")
    t0 = time.time()
    df_articles_raw, df_companies_raw = get_raw_data()
    print(f"      -> Ingested {len(df_articles_raw)} raw articles and {len(df_companies_raw)} metadata companies ({time.time() - t0:.2f}s)")
    
    # 2. Silver Layer
    print("\n[2/3] [Silver] Running transformations & entity resolution...")
    t0 = time.time()
    df_silver = clean_articles_pipeline(df_articles_raw, df_companies_raw)
    print(f"      -> Cleaned & Enriched Dataset: {df_silver.shape[0]} rows x {df_silver.shape[1]} columns ({time.time() - t0:.2f}s)")
    
    # 3. Gold Layer
    print("\n[3/3] [Gold] Building warehouse dimensional model & exporting CSVs...")
    t0 = time.time()
    exports = build_and_export_warehouse(df_silver)
    print(f"      -> Successfully exported {len(exports)} warehouse deliverables ({time.time() - t0:.2f}s)")
    
    total_time = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"[SUCCESS] Pipeline Finished Successfully in {total_time:.2f} seconds!")
    print("          All tables materialized into 'data/processed/' and 'data/warehouse/'.")
    print("=" * 60)
    return exports

if __name__ == '__main__':
    run_pipeline()
