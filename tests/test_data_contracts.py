"""
tests/test_data_contracts.py
==============================
Automated unit tests for the Data Contract Validator and Quality Scorecard engine.
Verifies NOT NULL, CHECK range, PRIMARY KEY, FOREIGN KEY, and ENUM constraint checks.
"""

import pytest
import pandas as pd
from medallion.data_contracts import DataContractValidator
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
    # Domain A Gold
    df_raw, df_meta = get_raw_data()
    df_silver = clean_articles_pipeline(df_raw, df_meta)
    co_map = generate_company_id_map(df_silver)
    
    gold_news = {
        "dim_company": build_dim_company(df_silver, co_map),
        "fct_article": build_fct_article(df_silver, co_map),
        "fct_arr_observation": build_fct_arr_observation(df_silver, co_map)
    }

    # Domain B Gold
    ecom_pipeline = EcommerceMedallionPipeline()
    ecom_bronze = ecom_pipeline.run_bronze()
    ecom_silver = ecom_pipeline.run_silver(ecom_bronze)
    ecom_gold = ecom_pipeline.run_gold(ecom_silver)

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
