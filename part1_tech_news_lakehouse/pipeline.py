"""
part1_tech_news_lakehouse/pipeline.py
======================================
Part 1 Runner: Tech News & ARR Analytical Lakehouse Pipeline.
Executes Bronze -> Silver -> Gold Star Schema materialization.
"""

import time
from .bronze import get_raw_data
from .silver import clean_articles_pipeline
from .gold import build_and_export_warehouse


def run_part1_pipeline():
    """Runs Part 1 end-to-end lakehouse pipeline."""
    start_time = time.time()
    print("=" * 80)
    print("[+] PART 1: TECH NEWS & COMPANY ARR STAR SCHEMA LAKEHOUSE")
    print("=" * 80)

    # 1. Bronze
    t0 = time.time()
    print("\n[1/3] [Bronze] Ingesting raw tech news articles & company metadata...")
    df_raw_articles, df_raw_meta = get_raw_data()
    print(f"      -> Ingested {len(df_raw_articles)} articles, {len(df_raw_meta)} companies ({time.time() - t0:.2f}s)")

    # 2. Silver
    t0 = time.time()
    print("\n[2/3] [Silver] Cleaning, FX normalization, entity resolution & category mapping...")
    df_silver = clean_articles_pipeline(df_raw_articles, df_raw_meta)
    print(f"      -> Cleaned Silver Dataset: {df_silver.shape[0]} rows x {df_silver.shape[1]} cols ({time.time() - t0:.2f}s)")

    # 3. Gold
    t0 = time.time()
    print("\n[3/3] [Gold] Building dimensional star schema & exporting deliverables...")
    exports = build_and_export_warehouse(df_silver)
    print(f"      -> Materialized {len(exports)} Gold deliverables ({time.time() - t0:.2f}s)")

    duration = round(time.time() - start_time, 3)
    print("\n" + "=" * 80)
    print(f"[SUCCESS] Part 1 Lakehouse Completed in {duration}s!")
    print("=" * 80)

    return {
        "duration_sec": duration,
        "df_raw_articles": df_raw_articles,
        "df_raw_meta": df_raw_meta,
        "df_silver": df_silver,
        "gold_exports": exports
    }


if __name__ == "__main__":
    run_part1_pipeline()
