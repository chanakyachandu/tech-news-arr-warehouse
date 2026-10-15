"""
tests/test_part4_governance.py
==============================
Automated tests for Part 4: Data Contracts and Lineage Governance.
"""
import os
import pytest
import pandas as pd
from part4_data_contracts_lineage.data_contracts import DataContractValidator
from part2_slowly_changing_dimensions.scd_pipeline import run_scd_pipeline
from part1_tech_news_lakehouse.bronze import get_raw_data
from part1_tech_news_lakehouse.silver import clean_articles_pipeline
from part1_tech_news_lakehouse.gold import (
    generate_company_id_map,
    build_dim_company,
    build_fct_article,
    build_fct_arr_observation
)


@pytest.fixture(scope="module")
def lakehouse_gold_data():
    """Builds both Tech News and E-Commerce Gold datasets."""
    # Domain A Gold
    df_raw, df_meta = get_raw_data()
    df_silver = clean_articles_pipeline(df_raw, df_meta)
    co_map = generate_company_id_map(df_silver)
    
    gold_news = {
        "dim_company": build_dim_company(df_silver, co_map),
        "fct_article": build_fct_article(df_silver, co_map),
        "fct_arr_observation": build_fct_arr_observation(df_silver, co_map)
    }

    # Domain B Gold (SCD Dimensions)
    ecom_results = run_scd_pipeline()
    ecom_gold = ecom_results["gold_data"]

    return {
        "news_gold": gold_news,
        "ecom_gold": ecom_gold
    }


def test_data_contract_evaluates_100_percent_pass(lakehouse_gold_data):
    """Verifies that all production Gold tables pass 100% of defined Data Contracts."""
    validator = DataContractValidator()
    report = validator.validate_all(
        tech_news_gold=lakehouse_gold_data["news_gold"],
        ecommerce_gold=lakehouse_gold_data["ecom_gold"]
    )

    assert report["overall_status"] == "PASSED"
    assert report["overall_health_score_pct"] == 100.0
    assert report["summary"]["failed_count"] == 0
    assert report["summary"]["total_constraints_evaluated"] >= 15


def test_data_contract_catches_null_violation():
    """Verifies that NOT NULL constraint violation is caught accurately."""
    validator = DataContractValidator()
    bad_df = pd.DataFrame({"article_id": ["ART001", None, "ART003"]})
    passed, null_count, pass_rate, details = validator.check_not_null(bad_df, "article_id")

    assert not passed
    assert null_count == 1
    assert pass_rate == round(2 / 3 * 100, 2)


def test_data_contract_catches_range_violation():
    """Verifies that CHECK range violation (e.g. negative ARR) is caught."""
    validator = DataContractValidator()
    bad_df = pd.DataFrame({"arr_usd_M": [100.0, -50.0, 300.0]})
    passed, violating_count, pass_rate, details = validator.check_range(bad_df, "arr_usd_M", min_val=0.0)

    assert not passed
    assert violating_count == 1


def test_data_contract_catches_orphan_foreign_key():
    """Verifies that orphan Foreign Keys not found in parent table are caught."""
    validator = DataContractValidator()
    parent_df = pd.DataFrame({"company_id": ["COMP001", "COMP002"]})
    child_df = pd.DataFrame({"company_id": ["COMP001", "COMP999"]})  # COMP999 is orphan

    passed, orphan_count, pass_rate, details = validator.check_foreign_key(
        child_df=child_df,
        child_col="company_id",
        parent_df=parent_df,
        parent_col="company_id"
    )

    assert not passed
    assert orphan_count == 1

# Lineage Tests
from part4_data_contracts_lineage.lineage import DataLineageCatalog

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
    assert len(catalog["tables"]) >= 6
    assert "ecommerce.dim_customer_scd2" in catalog["tables"]
    assert "tech_news.dim_company" in catalog["tables"]

    # 5. Verify JSON file was written
    catalog_path = os.path.join(catalog_engine.output_dir, "metadata_catalog.json")
    assert os.path.exists(catalog_path)
