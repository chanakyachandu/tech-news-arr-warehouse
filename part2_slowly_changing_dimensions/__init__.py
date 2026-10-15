"""
part2_slowly_changing_dimensions
================================
Part 2: Slowly Changing Dimensions (SCD Types 1, 2, and 3).
Demonstrates Kimball SCD history tracking strategies using pure procedural functions.
"""

from .scd_pipeline import (
    get_ecommerce_paths,
    load_raw_customers,
    load_customer_spend,
    clean_customers,
    build_scd1_dimension,
    build_scd2_dimension,
    build_scd3_dimension,
    point_in_time_query,
    run_scd_pipeline,
    run_ecommerce_pipeline
)

__all__ = [
    'get_ecommerce_paths',
    'load_raw_customers',
    'load_customer_spend',
    'clean_customers',
    'build_scd1_dimension',
    'build_scd2_dimension',
    'build_scd3_dimension',
    'point_in_time_query',
    'run_scd_pipeline',
    'run_ecommerce_pipeline'
]
