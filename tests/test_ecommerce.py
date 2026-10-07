"""
test_ecommerce.py - Automated Unit & Integration Tests for E-Commerce Medallion Pipeline
Verifies Bronze schemas, Silver financial calculations, Gold SCD Type 2 history,
Star Schema referential integrity, and RFM customer segmentation analytics.
"""

import os
import pytest
import pandas as pd
from medallion.ecommerce_medallion import EcommerceMedallionPipeline


@pytest.fixture(scope="module")
def ecom_pipeline():
    """Initializes and runs the E-Commerce pipeline once for test verification."""
    pipeline = EcommerceMedallionPipeline()
    bronze = pipeline.run_bronze()
    silver = pipeline.run_silver(bronze)
    gold = pipeline.run_gold(silver)
    return {
        "pipeline": pipeline,
        "bronze": bronze,
        "silver": silver,
        "gold": gold
    }


def test_ecommerce_bronze_ingestion(ecom_pipeline):
    """Verifies that all 5 raw E-Commerce tables are loaded with expected minimum volumes."""
    bronze = ecom_pipeline["bronze"]
    assert "customers" in bronze
    assert "products" in bronze
    assert "categories" in bronze
    assert "orders" in bronze
    assert "order_details" in bronze

    assert len(bronze["customers"]) == 91
    assert len(bronze["products"]) == 77
    assert len(bronze["categories"]) == 8
    assert len(bronze["orders"]) == 830
    assert len(bronze["order_details"]) == 2155


def test_ecommerce_silver_financial_calculations(ecom_pipeline):
    """Verifies that Silver layer accurately calculates gross, discount, and net amounts."""
    df_items = ecom_pipeline["silver"]["silver_order_items"]
    
    # Assert line math: net = gross - discount_amount
    calculated_net = (df_items["line_gross_amount"] - df_items["discount_amount"]).round(2)
    assert (df_items["line_net_amount"].round(2) == calculated_net).all()
    assert (df_items["line_gross_amount"] >= df_items["line_net_amount"]).all()

    df_orders = ecom_pipeline["silver"]["silver_orders"]
    assert "fulfillment_days" in df_orders.columns
    assert "total_net_amount" in df_orders.columns
    assert (df_orders["total_net_amount"] >= 0).all()


def test_ecommerce_scd2_customer_dimension(ecom_pipeline):
    """Verifies SCD Type 2 mechanics: surrogate keys, validity dates, and active flags."""
    df_scd2 = ecom_pipeline["gold"]["dim_customer_scd2"]
    
    # 1. Unique Surrogate Keys
    assert df_scd2["customer_sk"].is_unique

    # 2. Every customer has exactly 1 active current version
    current_versions = df_scd2[df_scd2["is_current_flag"]]
    assert len(current_versions) == 91
    assert current_versions["customer_id"].is_unique

    # 3. Active versions have end_date = '9999-12-31'
    assert (current_versions["effective_end_date"] == "9999-12-31").all()

    # 4. Historical versions have start_date < end_date
    historical = df_scd2[~df_scd2["is_current_flag"]]
    assert (historical["effective_start_date"] < historical["effective_end_date"]).all()
    assert (historical["effective_end_date"] != "9999-12-31").all()


def test_ecommerce_star_schema_referential_integrity(ecom_pipeline):
    """Verifies zero orphan foreign keys in fact tables."""
    gold = ecom_pipeline["gold"]
    df_orders = gold["fct_orders"]
    df_items = gold["fct_order_items"]
    df_cust = gold["dim_customer_scd2"]
    df_prod = gold["dim_product"]

    # Check that every order's customer_sk exists in dim_customer_scd2
    valid_cust_sks = set(df_cust["customer_sk"])
    assert df_orders["customer_sk"].isin(valid_cust_sks).all()

    # Check that every order item's order_id exists in fct_orders
    valid_order_ids = set(df_orders["order_id"])
    assert df_items["order_id"].isin(valid_order_ids).all()

    # Check that every order item's product_sk exists in dim_product
    valid_prod_sks = set(df_prod["product_sk"])
    assert df_items["product_sk"].isin(valid_prod_sks).all()


def test_ecommerce_rfm_customer_segments(ecom_pipeline):
    """Verifies RFM segmentation scores and segment allocations."""
    df_rfm = ecom_pipeline["gold"]["agg_customer_rfm"]
    
    assert len(df_rfm) == 89  # 89 customers who placed orders
    assert df_rfm["customer_id"].is_unique
    
    # Assert score ranges
    assert df_rfm["r_score"].isin([1, 2, 3, 4]).all()
    assert df_rfm["f_score"].isin([1, 2, 3, 4]).all()
    assert df_rfm["m_score"].isin([1, 2, 3, 4]).all()
    
    # Assert segments are assigned
    valid_segments = {
        "Champions", "Loyal Customers", "Recent Customers",
        "At Risk - High Spenders", "Lost Customers", "Promising / Needs Attention"
    }
    assert df_rfm["customer_segment"].isin(valid_segments).all()
