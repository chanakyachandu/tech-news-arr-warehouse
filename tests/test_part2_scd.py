"""
tests/test_ecommerce.py
========================
Automated Unit Tests for Slowly Changing Dimensions (SCD Types 1, 2, and 3).
Verifies surrogate keys, validity date ranges, point-in-time temporal queries,
and in-place vs historical row versioning mechanics.
"""

import os
import pytest
import pandas as pd
from part2_slowly_changing_dimensions.scd_pipeline import (
    get_ecommerce_paths,
    load_raw_customers,
    load_customer_spend,
    clean_customers,
    build_scd1_dimension,
    build_scd2_dimension,
    build_scd3_dimension,
    point_in_time_query,
    run_scd_pipeline
)


@pytest.fixture(scope="module")
def scd_pipeline_data():
    """Initializes and runs the SCD pipeline once for test verification."""
    results = run_scd_pipeline()
    return results


def test_scd_raw_ingestion(scd_pipeline_data):
    """Verifies raw customer data ingestion."""
    bronze = scd_pipeline_data["bronze_data"]
    assert "customers" in bronze
    df_cust = bronze["customers"]
    assert len(df_cust) == 91
    assert "customerID" in df_cust.columns
    assert "companyName" in df_cust.columns


def test_scd1_in_place_overwrite(scd_pipeline_data):
    """Verifies SCD Type 1: In-Place Overwrite (1 row per customer, latest tier only)."""
    df_scd1 = scd_pipeline_data["gold_data"]["dim_customer_scd1"]

    assert len(df_scd1) == 91
    assert df_scd1["customer_id"].is_unique
    assert "loyalty_tier" in df_scd1.columns
    assert "credit_limit_usd" in df_scd1.columns
    assert "last_updated_at" in df_scd1.columns

    # High spender BERGS should have current Platinum tier
    bergs = df_scd1[df_scd1["customer_id"] == "BERGS"].iloc[0]
    assert bergs["loyalty_tier"] == "Platinum"
    assert bergs["credit_limit_usd"] == 50000


def test_scd2_surrogate_keys_and_versioning(scd_pipeline_data):
    """Verifies SCD Type 2: Surrogate keys, date ranges, and current active flags."""
    df_scd2 = scd_pipeline_data["gold_data"]["dim_customer_scd2"]

    # 1. Unique surrogate keys
    assert df_scd2["customer_sk"].is_unique

    # 2. Exactly 1 active version per customer (91 active records)
    current_versions = df_scd2[df_scd2["is_current_flag"]]
    assert len(current_versions) == 91
    assert current_versions["customer_id"].is_unique
    assert (current_versions["effective_end_date"] == "9999-12-31").all()

    # 3. Expired historical records have valid chronological date ranges
    historical = df_scd2[~df_scd2["is_current_flag"]]
    assert len(historical) > 0
    assert (historical["effective_start_date"] < historical["effective_end_date"]).all()
    assert (historical["effective_end_date"] != "9999-12-31").all()


def test_scd2_temporal_point_in_time_query(scd_pipeline_data):
    """Verifies Point-in-Time temporal queries against SCD Type 2 dimension."""
    df_scd2 = scd_pipeline_data["gold_data"]["dim_customer_scd2"]

    # In 1996, customer BERGS was Silver
    t1 = point_in_time_query(df_scd2, "BERGS", "1996-08-15")
    assert t1 is not None
    assert t1["loyalty_tier"] == "Silver"
    assert t1["credit_limit_usd"] == 10000
    assert not t1["is_current_flag"]

    # In late 1997, customer BERGS was Gold
    t2 = point_in_time_query(df_scd2, "BERGS", "1997-10-01")
    assert t2 is not None
    assert t2["loyalty_tier"] == "Gold"
    assert t2["credit_limit_usd"] == 25000
    assert not t2["is_current_flag"]

    # In 1998+, customer BERGS is Platinum (Active)
    t3 = point_in_time_query(df_scd2, "BERGS", "1998-05-01")
    assert t3 is not None
    assert t3["loyalty_tier"] == "Platinum"
    assert t3["credit_limit_usd"] == 50000
    assert t3["is_current_flag"]


def test_scd3_current_vs_previous_columns(scd_pipeline_data):
    """Verifies SCD Type 3: Current and previous state preserved in adjacent columns."""
    df_scd3 = scd_pipeline_data["gold_data"]["dim_customer_scd3"]

    assert len(df_scd3) == 91
    assert df_scd3["customer_id"].is_unique
    assert "current_loyalty_tier" in df_scd3.columns
    assert "previous_loyalty_tier" in df_scd3.columns
    assert "current_credit_limit_usd" in df_scd3.columns
    assert "previous_credit_limit_usd" in df_scd3.columns
    assert "tier_change_date" in df_scd3.columns

    # High spender BERGS has current Platinum and previous Gold
    bergs = df_scd3[df_scd3["customer_id"] == "BERGS"].iloc[0]
    assert bergs["current_loyalty_tier"] == "Platinum"
    assert bergs["previous_loyalty_tier"] == "Gold"
    assert bergs["current_credit_limit_usd"] == 50000
    assert bergs["previous_credit_limit_usd"] == 25000


def test_scd_pipeline_execution_and_exports(scd_pipeline_data):
    """Verifies that warehouse CSV files are successfully written to disk."""
    gold = scd_pipeline_data["gold_data"]
    assert "dim_customer_scd1" in gold
    assert "dim_customer_scd2" in gold
    assert "dim_customer_scd3" in gold

    paths = get_ecommerce_paths()
    for name in ["dim_customer_scd1", "dim_customer_scd2", "dim_customer_scd3"]:
        csv_file = os.path.join(paths["warehouse_dir"], f"{name}.csv")
        assert os.path.exists(csv_file)
        assert os.path.getsize(csv_file) > 0
