"""
ecommerce_medallion.py - E-Commerce Omnichannel Retail Medallion Pipeline
Implements Bronze (Raw Ingestion), Silver (Cleaning & Enrichment), and Gold
(Star Schema Dimensional Warehouse, SCD Type 2 Customer Dimension, and RFM Segmentation).
"""

import os
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Tuple, Optional
import pandas as pd
from .bronze import _find_project_root


class EcommerceMedallionPipeline:
    """
    End-to-end Medallion Architecture Pipeline for E-Commerce & Retail Sales:
    - Bronze: Raw relational schema validation and profiling
    - Silver: Cleansed, enriched tables with calculated financial margins and dates
    - Gold: Star schema dimensional warehouse, SCD Type 2 customer history, and RFM analytics
    """

    def __init__(self, project_root: Optional[str] = None):
        if project_root is None:
            self.project_root = _find_project_root()
        else:
            self.project_root = os.path.abspath(project_root)

        self.raw_dir = os.path.join(self.project_root, "data", "raw", "ecommerce")
        self.processed_dir = os.path.join(self.project_root, "data", "processed", "ecommerce")
        self.warehouse_dir = os.path.join(self.project_root, "data", "warehouse", "ecommerce")

        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.processed_dir, exist_ok=True)
        os.makedirs(self.warehouse_dir, exist_ok=True)

    # =========================================================================
    # 1. BRONZE LAYER: Ingestion & Schema Profiling
    # =========================================================================
    def run_bronze(self) -> Dict[str, pd.DataFrame]:
        """
        Ingests all 5 raw E-Commerce CSVs and validates schema presence.
        Returns dictionary of Bronze dataframes.
        """
        required_files = {
            "customers": "customers.csv",
            "products": "products.csv",
            "categories": "categories.csv",
            "orders": "orders.csv",
            "order_details": "order_details.csv"
        }

        bronze_data = {}
        for key, fname in required_files.items():
            fpath = os.path.join(self.raw_dir, fname)
            if not os.path.exists(fpath):
                raise FileNotFoundError(f"Missing raw E-Commerce CSV file: {fpath}")
            df = pd.read_csv(fpath)
            bronze_data[key] = df

        return bronze_data

    # =========================================================================
    # 2. SILVER LAYER: Cleaning, Normalization & Feature Engineering
    # =========================================================================
    def run_silver(self, bronze_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Cleans and enriches relational E-Commerce tables:
        - Dates standardization (ISO 8601) and fulfillment durations
        - Financial calculations (Gross, Discounts, Net Line Totals)
        - Product & Category denormalization
        - Customer contact and geographic cleaning
        """
        df_cust = bronze_data["customers"].copy()
        df_prod = bronze_data["products"].copy()
        df_cat = bronze_data["categories"].copy()
        df_ord = bronze_data["orders"].copy()
        df_det = bronze_data["order_details"].copy()

        # 1. Clean Customers
        df_cust["companyName"] = df_cust["companyName"].str.strip()
        df_cust["contactName"] = df_cust["contactName"].str.strip()
        df_cust["city"] = df_cust["city"].fillna("Unknown").str.strip()
        df_cust["country"] = df_cust["country"].fillna("Unknown").str.strip()
        df_cust["region"] = df_cust["region"].fillna("N/A").str.strip()
        df_cust["phone"] = df_cust["phone"].fillna("N/A").str.strip()

        # 2. Clean Products & Join Category
        df_cat_clean = df_cat[["categoryID", "categoryName", "description"]].copy()
        df_cat_clean.rename(columns={"description": "categoryDescription"}, inplace=True)
        
        df_prod_clean = pd.merge(df_prod, df_cat_clean, on="categoryID", how="left")
        df_prod_clean["productName"] = df_prod_clean["productName"].str.strip()
        df_prod_clean["unitPrice"] = df_prod_clean["unitPrice"].round(2)
        df_prod_clean["is_discontinued"] = df_prod_clean["discontinued"].apply(lambda x: True if x == 1 else False)
        
        # Stock health classification
        def classify_stock(row):
            if row["unitsInStock"] == 0:
                return "Out of Stock"
            elif row["unitsInStock"] < row["reorderLevel"]:
                return "Low Stock"
            return "In Stock"
        
        df_prod_clean["stock_status"] = df_prod_clean.apply(classify_stock, axis=1)

        # 3. Clean Order Items & Financial Formulas
        df_det_clean = df_det.copy()
        df_det_clean["line_gross_amount"] = (df_det_clean["unitPrice"] * df_det_clean["quantity"]).round(2)
        df_det_clean["discount_amount"] = (df_det_clean["line_gross_amount"] * df_det_clean["discount"]).round(2)
        df_det_clean["line_net_amount"] = (df_det_clean["line_gross_amount"] - df_det_clean["discount_amount"]).round(2)

        # 4. Clean Orders & Date Parsing
        df_ord_clean = df_ord.copy()
        df_ord_clean["orderDate"] = pd.to_datetime(df_ord_clean["orderDate"], errors="coerce")
        df_ord_clean["requiredDate"] = pd.to_datetime(df_ord_clean["requiredDate"], errors="coerce")
        df_ord_clean["shippedDate"] = pd.to_datetime(df_ord_clean["shippedDate"], errors="coerce")

        # Fulfillment duration in days
        df_ord_clean["fulfillment_days"] = (df_ord_clean["shippedDate"] - df_ord_clean["orderDate"]).dt.days
        df_ord_clean["fulfillment_days"] = df_ord_clean["fulfillment_days"].fillna(-1).astype(int)
        df_ord_clean["is_shipped"] = df_ord_clean["shippedDate"].notnull()
        df_ord_clean["freight"] = df_ord_clean["freight"].round(2)
        df_ord_clean["shipCountry"] = df_ord_clean["shipCountry"].fillna("Unknown").str.strip()

        # Aggregate order item totals up to order level
        order_financials = df_det_clean.groupby("orderID").agg({
            "line_gross_amount": "sum",
            "discount_amount": "sum",
            "line_net_amount": "sum",
            "quantity": "sum"
        }).reset_index().rename(columns={
            "line_gross_amount": "total_gross_amount",
            "discount_amount": "total_discount_amount",
            "line_net_amount": "total_net_amount",
            "quantity": "total_items_count"
        })

        df_ord_clean = pd.merge(df_ord_clean, order_financials, on="orderID", how="left")
        df_ord_clean["total_net_amount"] = df_ord_clean["total_net_amount"].fillna(0.0).round(2)
        df_ord_clean["total_gross_amount"] = df_ord_clean["total_gross_amount"].fillna(0.0).round(2)
        df_ord_clean["total_items_count"] = df_ord_clean["total_items_count"].fillna(0).astype(int)

        # Save silver tables to processed directory
        silver_data = {
            "silver_customers": df_cust,
            "silver_products": df_prod_clean,
            "silver_orders": df_ord_clean,
            "silver_order_items": df_det_clean
        }

        for name, df in silver_data.items():
            df.to_csv(os.path.join(self.processed_dir, f"{name}.csv"), index=False)

        return silver_data

    # =========================================================================
    # 3. GOLD LAYER: Star Schema, SCD Type 2 & RFM Analytics
    # =========================================================================
    def run_gold(self, silver_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Builds the Gold Lakehouse Warehouse:
        - dim_customer_scd2: Slowly Changing Dimension (Type 2) with historical tier versioning
        - dim_product: Enriched product dimension with category metadata
        - dim_date: Generated date hierarchy
        - fct_orders: Order-level transactional fact table
        - fct_order_items: Line-item grain fact table
        - agg_customer_rfm: Recency, Frequency, Monetary value customer segmentation
        """
        df_cust = silver_data["silver_customers"].copy()
        df_prod = silver_data["silver_products"].copy()
        df_ord = silver_data["silver_orders"].copy()
        df_det = silver_data["silver_order_items"].copy()

        # ---------------------------------------------------------------------
        # A. Build SCD Types (Type 1, Type 2, and Type 3) Customer Dimensions
        # ---------------------------------------------------------------------
        # Calculate customer lifetime spend to simulate loyalty tier changes
        cust_spend = df_ord.groupby("customerID")["total_net_amount"].sum().to_dict()

        scd1_rows = []
        scd2_rows = []
        scd3_rows = []
        sk_counter = 1

        for _, row in df_cust.iterrows():
            cid = row["customerID"]
            cname = row["companyName"]
            contact = row["contactName"]
            city = row["city"]
            country = row["country"]
            spend = cust_spend.get(cid, 0.0)

            # Determine loyalty progression
            if spend >= 15000:
                # --- SCD Type 1: Only Latest Current State ---
                scd1_rows.append({
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Platinum",
                    "credit_limit_usd": 50000,
                    "last_updated_at": "1998-04-01"
                })

                # --- SCD Type 3: Current vs Previous State Columns ---
                scd3_rows.append({
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "current_loyalty_tier": "Platinum",
                    "previous_loyalty_tier": "Gold",
                    "current_credit_limit_usd": 50000,
                    "previous_credit_limit_usd": 25000,
                    "tier_change_date": "1998-04-01"
                })

                # --- SCD Type 2: Full Historical Version Rows with Date Ranges ---
                # Version 1: Silver Tier (1996 - 1997)
                scd2_rows.append({
                    "customer_sk": f"CUST_SK_{sk_counter:04d}",
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Silver",
                    "credit_limit_usd": 10000,
                    "effective_start_date": "1996-01-01",
                    "effective_end_date": "1997-06-30",
                    "is_current_flag": False
                })
                sk_counter += 1

                # Version 2: Gold Tier (1997 - 1998)
                scd2_rows.append({
                    "customer_sk": f"CUST_SK_{sk_counter:04d}",
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Gold",
                    "credit_limit_usd": 25000,
                    "effective_start_date": "1997-07-01",
                    "effective_end_date": "1998-03-31",
                    "is_current_flag": False
                })
                sk_counter += 1

                # Version 3: Platinum Tier (Active)
                scd2_rows.append({
                    "customer_sk": f"CUST_SK_{sk_counter:04d}",
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Platinum",
                    "credit_limit_usd": 50000,
                    "effective_start_date": "1998-04-01",
                    "effective_end_date": "9999-12-31",
                    "is_current_flag": True
                })
                sk_counter += 1

            elif spend >= 5000:
                # --- SCD Type 1 ---
                scd1_rows.append({
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Gold",
                    "credit_limit_usd": 20000,
                    "last_updated_at": "1998-01-01"
                })

                # --- SCD Type 3 ---
                scd3_rows.append({
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "current_loyalty_tier": "Gold",
                    "previous_loyalty_tier": "Bronze",
                    "current_credit_limit_usd": 20000,
                    "previous_credit_limit_usd": 5000,
                    "tier_change_date": "1998-01-01"
                })

                # --- SCD Type 2 ---
                # Version 1: Bronze (1996 - 1997)
                scd2_rows.append({
                    "customer_sk": f"CUST_SK_{sk_counter:04d}",
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Bronze",
                    "credit_limit_usd": 5000,
                    "effective_start_date": "1996-01-01",
                    "effective_end_date": "1997-12-31",
                    "is_current_flag": False
                })
                sk_counter += 1

                # Version 2: Gold (Active)
                scd2_rows.append({
                    "customer_sk": f"CUST_SK_{sk_counter:04d}",
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Gold",
                    "credit_limit_usd": 20000,
                    "effective_start_date": "1998-01-01",
                    "effective_end_date": "9999-12-31",
                    "is_current_flag": True
                })
                sk_counter += 1

            else:
                # --- SCD Type 1 ---
                scd1_rows.append({
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Bronze",
                    "credit_limit_usd": 5000,
                    "last_updated_at": "1996-01-01"
                })

                # --- SCD Type 3 ---
                scd3_rows.append({
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "current_loyalty_tier": "Bronze",
                    "previous_loyalty_tier": None,
                    "current_credit_limit_usd": 5000,
                    "previous_credit_limit_usd": None,
                    "tier_change_date": None
                })

                # --- SCD Type 2 ---
                scd2_rows.append({
                    "customer_sk": f"CUST_SK_{sk_counter:04d}",
                    "customer_id": cid,
                    "company_name": cname,
                    "contact_name": contact,
                    "city": city,
                    "country": country,
                    "loyalty_tier": "Bronze",
                    "credit_limit_usd": 5000,
                    "effective_start_date": "1996-01-01",
                    "effective_end_date": "9999-12-31",
                    "is_current_flag": True
                })
                sk_counter += 1

        df_dim_customer_scd1 = pd.DataFrame(scd1_rows)
        df_dim_customer_scd2 = pd.DataFrame(scd2_rows)
        df_dim_customer_scd3 = pd.DataFrame(scd3_rows)

        # ---------------------------------------------------------------------
        # B. Build Product Dimension (dim_product)
        # ---------------------------------------------------------------------
        df_dim_product = df_prod[[
            "productID", "productName", "categoryName", "categoryDescription",
            "quantityPerUnit", "unitPrice", "unitsInStock", "unitsOnOrder",
            "reorderLevel", "is_discontinued", "stock_status"
        ]].copy()
        
        df_dim_product.insert(0, "product_sk", [f"PROD_SK_{i+1:03d}" for i in range(len(df_dim_product))])
        df_dim_product.rename(columns={
            "productID": "product_id",
            "productName": "product_name",
            "categoryName": "category_name",
            "categoryDescription": "category_description",
            "quantityPerUnit": "quantity_per_unit",
            "unitPrice": "unit_price",
            "unitsInStock": "units_in_stock",
            "unitsOnOrder": "units_on_order",
            "reorderLevel": "reorder_level"
        }, inplace=True)

        # ---------------------------------------------------------------------
        # C. Build Date Dimension (dim_date)
        # ---------------------------------------------------------------------
        valid_dates = df_ord["orderDate"].dropna()
        min_date = valid_dates.min()
        max_date = valid_dates.max() + timedelta(days=60)
        date_range = pd.date_range(start=min_date, end=max_date, freq="D")

        date_rows = []
        for d in date_range:
            date_rows.append({
                "date_sk": int(d.strftime("%Y%m%d")),
                "calendar_date": d.strftime("%Y-%m-%d"),
                "year": d.year,
                "quarter": f"Q{d.quarter}",
                "month": d.month,
                "month_name": d.strftime("%B"),
                "day_of_month": d.day,
                "day_name": d.strftime("%A"),
                "is_weekend": d.weekday() >= 5
            })
        df_dim_date = pd.DataFrame(date_rows)

        # ---------------------------------------------------------------------
        # D. Build Fact Tables (fct_orders, fct_order_items)
        # ---------------------------------------------------------------------
        # Create map from customer_id to active customer_sk
        active_cust_sk_map = df_dim_customer_scd2[df_dim_customer_scd2["is_current_flag"]].set_index("customer_id")["customer_sk"].to_dict()
        prod_sk_map = df_dim_product.set_index("product_id")["product_sk"].to_dict()

        df_fct_orders = df_ord[[
            "orderID", "customerID", "orderDate", "requiredDate", "shippedDate",
            "fulfillment_days", "is_shipped", "freight", "shipCountry",
            "total_gross_amount", "total_discount_amount", "total_net_amount", "total_items_count"
        ]].copy()

        df_fct_orders["customer_sk"] = df_fct_orders["customerID"].map(active_cust_sk_map)
        df_fct_orders["order_date_sk"] = df_fct_orders["orderDate"].apply(lambda d: int(d.strftime("%Y%m%d")) if pd.notnull(d) else -1)
        df_fct_orders["order_date"] = df_fct_orders["orderDate"].dt.strftime("%Y-%m-%d")
        df_fct_orders["required_date"] = df_fct_orders["requiredDate"].dt.strftime("%Y-%m-%d")
        df_fct_orders["shipped_date"] = df_fct_orders["shippedDate"].dt.strftime("%Y-%m-%d")
        
        df_fct_orders = df_fct_orders[[
            "orderID", "customer_sk", "customerID", "order_date_sk", "order_date",
            "required_date", "shipped_date", "fulfillment_days", "is_shipped",
            "freight", "shipCountry", "total_gross_amount", "total_discount_amount",
            "total_net_amount", "total_items_count"
        ]].rename(columns={"orderID": "order_id", "customerID": "customer_id", "shipCountry": "ship_country"})

        df_fct_order_items = df_det[[
            "orderID", "productID", "unitPrice", "quantity", "discount",
            "line_gross_amount", "discount_amount", "line_net_amount"
        ]].copy()
        
        df_fct_order_items.insert(0, "order_item_sk", [f"ITEM_SK_{i+1:05d}" for i in range(len(df_fct_order_items))])
        df_fct_order_items["product_sk"] = df_fct_order_items["productID"].map(prod_sk_map)
        df_fct_order_items.rename(columns={
            "orderID": "order_id",
            "productID": "product_id",
            "unitPrice": "unit_price",
            "discount": "discount_rate"
        }, inplace=True)

        # ---------------------------------------------------------------------
        # E. Build Customer 360 RFM Analytics (agg_customer_rfm)
        # ---------------------------------------------------------------------
        snapshot_date = df_ord["orderDate"].max() + timedelta(days=1)
        
        rfm_table = df_ord.groupby("customerID").agg(
            recency_days=("orderDate", lambda dates: (snapshot_date - dates.max()).days),
            frequency_orders_count=("orderID", "count"),
            monetary_total_spend=("total_net_amount", "sum")
        ).reset_index()

        # Score quartiles 1-4
        rfm_table["r_score"] = pd.qcut(rfm_table["recency_days"], 4, labels=[4, 3, 2, 1]).astype(int)
        rfm_table["f_score"] = pd.qcut(rfm_table["frequency_orders_count"].rank(method="first"), 4, labels=[1, 2, 3, 4]).astype(int)
        rfm_table["m_score"] = pd.qcut(rfm_table["monetary_total_spend"], 4, labels=[1, 2, 3, 4]).astype(int)
        rfm_table["rfm_combined_score"] = rfm_table["r_score"].astype(str) + rfm_table["f_score"].astype(str) + rfm_table["m_score"].astype(str)

        # Segment Assignment Rule
        def assign_segment(row):
            r = row["r_score"]
            fm = (row["f_score"] + row["m_score"]) / 2.0
            if r >= 3 and fm >= 3:
                return "Champions"
            elif r >= 3 and fm >= 2:
                return "Loyal Customers"
            elif r >= 3 and fm < 2:
                return "Recent Customers"
            elif r < 2 and fm >= 3:
                return "At Risk - High Spenders"
            elif r < 2 and fm < 2:
                return "Lost Customers"
            return "Promising / Needs Attention"

        rfm_table["customer_segment"] = rfm_table.apply(assign_segment, axis=1)

        # Join customer names
        cust_lookup = df_cust.set_index("customerID")[["companyName", "country"]].to_dict(orient="index")
        rfm_table["company_name"] = rfm_table["customerID"].apply(lambda cid: cust_lookup.get(cid, {}).get("companyName", "Unknown"))
        rfm_table["country"] = rfm_table["customerID"].apply(lambda cid: cust_lookup.get(cid, {}).get("country", "Unknown"))
        rfm_table["monetary_total_spend"] = rfm_table["monetary_total_spend"].round(2)
        rfm_table.rename(columns={"customerID": "customer_id"}, inplace=True)

        gold_tables = {
            "dim_customer_scd1": df_dim_customer_scd1,
            "dim_customer_scd2": df_dim_customer_scd2,
            "dim_customer_scd3": df_dim_customer_scd3,
            "dim_product": df_dim_product,
            "dim_date": df_dim_date,
            "fct_orders": df_fct_orders,
            "fct_order_items": df_fct_order_items,
            "agg_customer_rfm": rfm_table
        }

        for name, df in gold_tables.items():
            df.to_csv(os.path.join(self.warehouse_dir, f"{name}.csv"), index=False)

        return gold_tables

    def run_pipeline(self) -> Dict[str, Any]:
        """
        Runs the full end-to-end E-Commerce Medallion pipeline.
        """
        start_time = time.time()
        print("=" * 80)
        print("[+] STARTING E-COMMERCE OMNICHANNEL RETAIL MEDALLION PIPELINE")
        print("=" * 80)

        # 1. Bronze
        print("\n---> [1/3] Executing Bronze Layer (Ingestion & Profiling)...")
        bronze_data = self.run_bronze()
        for k, df in bronze_data.items():
            print(f"     • Ingested Bronze `{k:<15}`: {len(df):>5} rows | {len(df.columns):>2} cols")

        # 2. Silver
        print("\n---> [2/3] Executing Silver Layer (Cleaning & Financial Enrichment)...")
        silver_data = self.run_silver(bronze_data)
        for k, df in silver_data.items():
            print(f"     • Materialized Silver `{k:<18}`: {len(df):>5} rows | {len(df.columns):>2} cols")

        # 3. Gold
        print("\n---> [3/3] Executing Gold Layer (SCD Type 2, Star Schema & RFM)...")
        gold_data = self.run_gold(silver_data)
        for k, df in gold_data.items():
            print(f"     • Materialized Gold `{k:<20}`: {len(df):>5} rows | {len(df.columns):>2} cols")

        duration = round(time.time() - start_time, 3)
        print("\n" + "=" * 80)
        print(f"[SUCCESS] E-Commerce Medallion Pipeline Completed in {duration}s!")
        print(f"          Warehouse Tables Exported to: {self.warehouse_dir}")
        print("=" * 80)

        return {
            "duration_sec": duration,
            "bronze_tables": len(bronze_data),
            "silver_tables": len(silver_data),
            "gold_tables": len(gold_data)
        }


def run_ecommerce_pipeline() -> Dict[str, Any]:
    """Convenience runner function."""
    pipeline = EcommerceMedallionPipeline()
    return pipeline.run_pipeline()


if __name__ == "__main__":
    run_ecommerce_pipeline()
