"""
Medallion Architecture Package
Provides Bronze (Ingestion), Silver (Cleaning & Enrichment), and Gold (Warehouse Modeling) modules.
"""

from .bronze import get_raw_data
from .silver import clean_articles_pipeline, run_silver
from .gold import build_and_export_warehouse
from .streaming_cdc import CDCStreamProcessor, run_streaming_pipeline
from .ecommerce_medallion import EcommerceMedallionPipeline, run_ecommerce_pipeline

__all__ = [
    'get_raw_data',
    'clean_articles_pipeline',
    'run_silver',
    'build_and_export_warehouse',
    'CDCStreamProcessor',
    'run_streaming_pipeline',
    'EcommerceMedallionPipeline',
    'run_ecommerce_pipeline'
]
