"""
pipeline.py - Multi-Project Data Engineering Platform Orchestrator
Sequentially executes all 4 distinct data engineering projects:
  - Project 1: Tech News & ARR Analytical Lakehouse (Star Schema Batch ETL)
  - Project 2: Slowly Changing Dimensions (SCD Types 1, 2, and 3)
  - Project 3: Real-Time Streaming & Change Data Capture (CDC Reconciliation)
  - Project 4: Enterprise Data Contracts & Automated Lineage Governance
"""

import sys
import time
import argparse
from pathlib import Path

# Add current directory to path to ensure project packages are discoverable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from part1_tech_news_lakehouse.bronze import get_raw_data
from part1_tech_news_lakehouse.silver import clean_articles_pipeline
from part1_tech_news_lakehouse.gold import (
    build_and_export_warehouse,
    generate_company_id_map,
    build_dim_company,
    build_fct_article,
    build_fct_arr_observation
)
from part2_slowly_changing_dimensions.scd_pipeline import run_scd_pipeline
from part3_realtime_streaming_cdc.streaming_cdc import run_streaming_pipeline
from part4_data_contracts_lineage.data_contracts import DataContractValidator
from part4_data_contracts_lineage.lineage import DataLineageCatalog


def run_all_pipelines(include_streaming: bool = False, include_lineage: bool = False):
    """Runs all 4 data engineering projects sequentially."""
    master_start = time.time()
    
    print("=" * 80)
    print("[+] MULTI-PROJECT DATA ENGINEERING PLATFORM ORCHESTRATOR")
    print("=" * 80)
    
    # -------------------------------------------------------------------------
    # PROJECT 1: Tech News & ARR Analytical Lakehouse (Star Schema)
    # -------------------------------------------------------------------------
    print("\n" + "#" * 80)
    print("PROJECT 1: TECH NEWS & ARR FINANCIAL STAR SCHEMA LAKEHOUSE")
    print("#" * 80)
    
    t0 = time.time()
    print("\n[1/3] [Bronze] Ingesting raw tech news articles & metadata...")
    df_articles_raw, df_companies_raw = get_raw_data()
    print(f"      -> Ingested {len(df_articles_raw)} raw articles and {len(df_companies_raw)} metadata companies ({time.time() - t0:.2f}s)")
    
    t0 = time.time()
    print("\n[2/3] [Silver] Running FX normalization & entity resolution...")
    df_silver = clean_articles_pipeline(df_articles_raw, df_companies_raw)
    print(f"      -> Cleaned & Enriched Dataset: {df_silver.shape[0]} rows x {df_silver.shape[1]} columns ({time.time() - t0:.2f}s)")
    
    t0 = time.time()
    print("\n[3/3] [Gold] Building dimensional star-schema & exporting CSVs...")
    exports = build_and_export_warehouse(df_silver)
    print(f"      -> Exported {len(exports)} warehouse deliverables ({time.time() - t0:.2f}s)")

    # -------------------------------------------------------------------------
    # PROJECT 2: Slowly Changing Dimensions (SCD Types 1, 2, and 3)
    # -------------------------------------------------------------------------
    print("\n" + "#" * 80)
    print("PROJECT 2: SLOWLY CHANGING DIMENSIONS (SCD TYPES 1, 2, AND 3)")
    print("#" * 80)
    scd_metrics = run_scd_pipeline()

    # -------------------------------------------------------------------------
    # PROJECT 3: Real-Time Streaming & CDC Reconciliation
    # -------------------------------------------------------------------------
    if include_streaming:
        print("\n" + "#" * 80)
        print("PROJECT 3: REAL-TIME STREAMING & CDC RECONCILIATION")
        print("#" * 80)
        run_streaming_pipeline()

    # -------------------------------------------------------------------------
    # PROJECT 4: Enterprise Data Contracts & Governance Lineage Catalog
    # -------------------------------------------------------------------------
    print("\n" + "#" * 80)
    print("PROJECT 4: DATA CONTRACTS VALIDATION & LINEAGE GOVERNANCE")
    print("#" * 80)
    
    co_map = generate_company_id_map(df_silver)
    news_gold_dict = {
        "dim_company": build_dim_company(df_silver, co_map),
        "fct_article": build_fct_article(df_silver, co_map),
        "fct_arr_observation": build_fct_arr_observation(df_silver, co_map)
    }

    validator = DataContractValidator()
    report = validator.validate_all(
        tech_news_gold=news_gold_dict,
        ecommerce_gold=scd_metrics.get("gold_data", {})
    )
    print("\n")
    validator.print_scorecard(report)

    lineage_engine = DataLineageCatalog()
    catalog = lineage_engine.generate_catalog(
        tech_news_gold=news_gold_dict,
        ecommerce_gold=scd_metrics.get("gold_data", {})
    )
    if include_lineage:
        print("\n")
        lineage_engine.print_lineage_summary(catalog)

    total_time = round(time.time() - master_start, 3)
    print("\n" + "=" * 80)
    print(f"[SUCCESS] Multi-Project Execution Completed in {total_time}s!")
    print("          All Lakehouse deliverables materialized into 'data/warehouse/'.")
    print("          Quality Scorecard -> 'data/data_quality_report.json'")
    print("          Governance Lineage Catalog -> 'data/metadata_catalog.json'")
    print("=" * 80)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Multi-Project Data Engineering Platform Runner")
    parser.add_argument("--streaming", action="store_true", help="Include real-time streaming CDC simulation")
    parser.add_argument("--lineage", action="store_true", help="Print visual Data Lineage & Governance Catalog summary")
    args = parser.parse_args()
    
    run_all_pipelines(include_streaming=args.streaming, include_lineage=args.lineage)
