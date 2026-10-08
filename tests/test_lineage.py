"""
tests/test_lineage.py
======================
Automated unit tests for the Data Lineage and Governance Catalog engine.
"""

import os
import pytest
from medallion.lineage import DataLineageCatalog
from medallion.ecommerce_medallion import EcommerceMedallionPipeline
from medallion.bronze import get_raw_data
from medallion.silver import clean_articles_pipeline
from medallion.gold import (
    generate_company_id_map,
    build_dim_company,
    build_fct_article,
    build_fct_arr_observation
)


@pytest.fixture(scope="module")
def lakehouse_gold_data():
    """Builds both Tech News and E-Commerce Gold datasets."""
    df_raw, df_meta = get_raw_data()
    df_silver = clean_articles_pipeline(df_raw, df_meta)
    co_map = generate_company_id_map(df_silver)
    
    gold_news = {
        "dim_company": build_dim_company(df_silver, co_map),
        "fct_article": build_fct_article(df_silver, co_map),
        "fct_arr_observation": build_fct_arr_observation(df_silver, co_map)
    }

    ecom_pipeline = EcommerceMedallionPipeline()
    ecom_bronze = ecom_pipeline.run_bronze()
    ecom_silver = ecom_pipeline.run_silver(ecom_bronze)
    ecom_gold = ecom_pipeline.run_gold(ecom_silver)

    return {
        "news_gold": gold_news,
        "ecom_gold": ecom_gold
    }


def test_lineage_catalog_generation(lakehouse_gold_data):
    """Verifies that the DataLineageCatalog produces a complete governance catalog."""
    catalog_engine = DataLineageCatalog()
    catalog = catalog_engine.generate_catalog(
        tech_news_gold=lakehouse_gold_data["news_gold"],
        ecommerce_gold=lakehouse_gold_data["ecom_gold"]
    )

    # 1. Verify top-level structure
    assert "catalog_metadata" in catalog
    assert "table_lineage_graph" in catalog
    assert "column_lineage_rules" in catalog
    assert "tables" in catalog

    # 2. Verify all 3 domains are present in lineage graph
    assert len(catalog["table_lineage_graph"]) == 3

    # 3. Verify column-level transformation rules exist
    assert len(catalog["column_lineage_rules"]) >= 5

    # 4. Verify table profiles exist with column metadata
    assert len(catalog["tables"]) >= 8
    assert "ecommerce.dim_customer_scd2" in catalog["tables"]
    assert "tech_news.dim_company" in catalog["tables"]

    # 5. Verify JSON file was written
    catalog_path = os.path.join(catalog_engine.output_dir, "metadata_catalog.json")
    assert os.path.exists(catalog_path)
