"""
pipeline.py - Multi-Domain Enterprise Lakehouse Pipeline Orchestrator
Executes Medallion Architecture across both domains:
Domain A: Tech News & ARR Analytical Warehouse (Articles, Observations, AI Deliverables)
Domain B: E-Commerce & Omnichannel Retail Warehouse (Orders, Products, SCD Type 2 Customers, RFM Analytics)
"""

import sys
import time
import argparse
from pathlib import Path

# Add current directory to path to ensure medallion package is discoverable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from medallion.bronze import get_raw_data
from medallion.silver import clean_articles_pipeline
from medallion.gold import build_and_export_warehouse
from medallion.streaming_cdc import run_streaming_pipeline
from medallion.ecommerce_medallion import run_ecommerce_pipeline


def run_all_pipelines(include_streaming: bool = False):
    """Runs end-to-end Medallion pipelines across all enterprise domains."""
    master_start = time.time()
    
    print("=" * 80)
    print("[+] MULTI-DOMAIN ENTERPRISE LAKEHOUSE PLATFORM ORCHESTRATOR")
    print("=" * 80)
    
    # -------------------------------------------------------------------------
    # DOMAIN A: Tech News & ARR Analytical Lakehouse
    # -------------------------------------------------------------------------
    print("\n" + "#" * 80)
    print("DOMAIN A: TECH NEWS & ARR FINANCIAL WAREHOUSE")
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
    # DOMAIN B: E-Commerce Omnichannel Retail Lakehouse & SCD Type 2
    # -------------------------------------------------------------------------
    print("\n" + "#" * 80)
    print("DOMAIN B: E-COMMERCE & OMNICHANNEL RETAIL (SCD TYPE 2 & RFM ANALYTICS)")
    print("#" * 80)
    ecom_metrics = run_ecommerce_pipeline()

    # Optional: Streaming CDC simulation
    if include_streaming:
        print("\n" + "#" * 80)
        print("DOMAIN C: REAL-TIME STREAMING & CDC RECONCILIATION")
        print("#" * 80)
        run_streaming_pipeline()

    total_time = round(time.time() - master_start, 3)
    print("\n" + "=" * 80)
    print(f"[SUCCESS] Multi-Domain Platform Execution Completed in {total_time}s!")
    print("          All Lakehouse deliverables materialized into 'data/warehouse/'.")
    print("=" * 80)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Multi-Domain Lakehouse Pipeline Runner")
    parser.add_argument("--streaming", action="store_true", help="Include real-time streaming CDC simulation")
    args = parser.parse_args()
    
    run_all_pipelines(include_streaming=args.streaming)
