"""
part1_tech_news_lakehouse
=========================
Part 1: Tech News & Company ARR Lakehouse (Star Schema)
Provides Bronze (Ingestion), Silver (Cleaning & Normalization), and Gold (Dimensional Warehouse) modules.
"""

from .bronze import get_raw_data
from .silver import clean_articles_pipeline, run_silver
from .gold import (
    generate_company_id_map,
    build_dim_company,
    build_fct_article,
    build_fct_arr_observation,
    build_agg_company_quarterly_arr,
    build_view_company_latest_arr,
    build_ai_articles_enriched,
    build_and_export_warehouse
)

__all__ = [
    'get_raw_data',
    'clean_articles_pipeline',
    'run_silver',
    'generate_company_id_map',
    'build_dim_company',
    'build_fct_article',
    'build_fct_arr_observation',
    'build_agg_company_quarterly_arr',
    'build_view_company_latest_arr',
    'build_ai_articles_enriched',
    'build_and_export_warehouse'
]
