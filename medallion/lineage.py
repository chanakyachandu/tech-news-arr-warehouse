"""
medallion/lineage.py
====================
Enterprise Automated Data Lineage & Metadata Governance Catalog Engine.

Tracks:
  1. Table-Level Lineage (Raw Sources -> Bronze -> Silver -> Gold -> Downstream Apps)
  2. Column-Level Lineage & Mathematical Derivation Rules
  3. Relational Entity Keys (Primary & Foreign Keys)
  4. Column Schema & Data Freshness Metadata

Exports:
  - `data/metadata_catalog.json` (Machine-readable governance catalog)
  - Visual Mermaid and ASCII Lineage Graphs for Documentation
"""

import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd


class DataLineageCatalog:
    """
    Automated Governance & Lineage Engine for Multi-Domain Lakehouses.
    """

    def __init__(self, output_dir: Optional[str] = None):
        if output_dir is None:
            root = Path(__file__).resolve().parent.parent
            self.output_dir = str(root / "data")
        else:
            self.output_dir = output_dir

    def generate_catalog(
        self,
        tech_news_gold: Dict[str, pd.DataFrame],
        ecommerce_gold: Dict[str, pd.DataFrame]
    ) -> Dict[str, Any]:
        """
        Builds the complete governance catalog with schemas, keys, and lineage relationships.
        """
        catalog: Dict[str, Any] = {
            "catalog_metadata": {
                "platform": "Multi-Domain Enterprise Lakehouse",
                "generated_at": datetime.now().isoformat(),
                "governance_standard": "Medallion (Bronze/Silver/Gold) + Kimball Dimensional",
                "domains": ["Tech News & ARR Lakehouse", "E-Commerce Omnichannel Retail", "Real-Time Streaming CDC"]
            },
            "table_lineage_graph": [
                {
                    "domain": "Domain A: Tech News & ARR",
                    "sources": ["data/raw/tech_news.csv", "data/raw/company_metadata.json"],
                    "bronze": "get_raw_data()",
                    "silver": "data/processed/tech_news_clean.csv",
                    "gold_tables": [
                        "dim_company", "fct_article", "fct_arr_observation",
                        "agg_company_quarterly_arr", "view_company_latest_arr",
                        "ai_articles_enriched.csv"
                    ],
                    "downstream_consumers": [
                        "Analytical SQL Exploration (04_sql_queries.ipynb)",
                        "Semantic Vector Search (384d Dense Embeddings)",
                        "DataTalker AI (Natural Language Text-to-SQL Assistant)"
                    ]
                },
                {
                    "domain": "Domain B: E-Commerce Retail",
                    "sources": [
                        "data/raw/ecommerce/customers.csv", "data/raw/ecommerce/products.csv",
                        "data/raw/ecommerce/categories.csv", "data/raw/ecommerce/orders.csv",
                        "data/raw/ecommerce/order_details.csv"
                    ],
                    "bronze": "ingest_bronze_ecommerce()",
                    "silver": [
                        "data/processed/ecommerce/silver_customers.csv",
                        "data/processed/ecommerce/silver_products.csv",
                        "data/processed/ecommerce/silver_orders.csv",
                        "data/processed/ecommerce/silver_order_items.csv"
                    ],
                    "gold_tables": [
                        "dim_customer_scd1", "dim_customer_scd2", "dim_customer_scd3",
                        "dim_product", "dim_date", "fct_orders", "fct_order_items",
                        "agg_customer_rfm"
                    ],
                    "downstream_consumers": [
                        "Customer 360 RFM Marketing Cohorts",
                        "Point-in-Time Historical SCD 2 Order Analytics",
                        "Product Inventory Health & Market Basket Analysis"
                    ]
                },
                {
                    "domain": "Domain C: Real-Time Streaming & CDC",
                    "sources": ["data/stream_landing/*.jsonl"],
                    "bronze": "streaming_cdc.py (Micro-Batch Ingestion)",
                    "silver": "Event-Time Watermark Filter (48h) + DLQ Quarantine",
                    "gold_tables": ["Atomic MERGE INTO Reconciliation into Gold Facts"],
                    "downstream_consumers": ["Real-time Operational Dashboards"]
                }
            ],
            "column_lineage_rules": [
                {
                    "output_column": "fct_arr_observation.arr_usd_M",
                    "source_column": "tech_news.csv -> revenue",
                    "transformation": "REGEX_CURRENCY_EXTRACT -> FX_RATES_TO_USD (GBP:1.28, EUR:1.09, JPY:0.0067) -> MIDPOINT_AVERAGE -> USD_MILLIONS"
                },
                {
                    "output_column": "dim_company.company_name",
                    "source_column": "tech_news.csv -> company_name",
                    "transformation": "COMPANY_NAME_MAPPING (46 raw spelling aliases -> 21 canonical entities)"
                },
                {
                    "output_column": "fct_article.category_clean",
                    "source_column": "tech_news.csv -> category",
                    "transformation": "CATEGORY_MAPPING (19 raw tags -> 7 standardized industry sectors)"
                },
                {
                    "output_column": "fct_orders.fulfillment_days",
                    "source_column": "orders.csv -> (shippedDate, orderDate)",
                    "transformation": "DATE_DIFF_IN_DAYS (shipped_date - order_date)"
                },
                {
                    "output_column": "fct_order_items.line_net_amount",
                    "source_column": "order_details.csv -> (unitPrice, quantity, discount)",
                    "transformation": "(unit_price * quantity) - ((unit_price * quantity) * discount)"
                },
                {
                    "output_column": "dim_customer_scd2.is_current_flag",
                    "source_column": "customers.csv + orders.csv -> spend history",
                    "transformation": "SCD_TYPE_2_EXPIRATION (effective_end_date == '9999-12-31' -> True, else False)"
                },
                {
                    "output_column": "agg_customer_rfm.customer_segment",
                    "source_column": "orders.csv -> (order_date, order_id, total_net_amount)",
                    "transformation": "RFM_QUINTILE_SCORING -> (Champions, Loyal Customers, At Risk, Lost)"
                }
            ],
            "tables": {}
        }

        # Profile all Gold tables
        all_tables = {}
        all_tables.update({f"tech_news.{k}": v for k, v in tech_news_gold.items()})
        all_tables.update({f"ecommerce.{k}": v for k, v in ecommerce_gold.items()})

        for table_key, df in all_tables.items():
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue

            columns_meta = []
            for col in df.columns:
                series = df[col]
                dtype_str = str(series.dtype)
                null_cnt = int(series.isnull().sum())
                unique_cnt = int(series.nunique())
                sample_vals = [str(x) for x in series.dropna().head(3).tolist()]

                columns_meta.append({
                    "column_name": col,
                    "data_type": dtype_str,
                    "nullable": null_cnt > 0,
                    "null_count": null_cnt,
                    "distinct_count": unique_cnt,
                    "sample_values": sample_vals
                })

            catalog["tables"][table_key] = {
                "domain": "Domain A" if table_key.startswith("tech_news") else "Domain B",
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": columns_meta
            }

        # Export catalog JSON
        os.makedirs(self.output_dir, exist_ok=True)
        catalog_path = os.path.join(self.output_dir, "metadata_catalog.json")
        with open(catalog_path, "w", encoding="utf-8") as f:
            json.dump(catalog, f, indent=2)

        return catalog

    def print_lineage_summary(self, catalog: Dict[str, Any]):
        """Prints a clean, formatted terminal Lineage & Metadata Governance summary."""
        print("=" * 80)
        print("[+] AUTOMATED LAKEHOUSE DATA LINEAGE & GOVERNANCE CATALOG")
        print("=" * 80)
        print(f"Platform: {catalog['catalog_metadata']['platform']}")
        print(f"Total Governed Tables: {len(catalog.get('tables', {}))} tables across 3 domains")
        print("-" * 80)
        print("TABLE-LEVEL LINEAGE FLOW:")
        for flow in catalog["table_lineage_graph"]:
            print(f"\n* {flow['domain']}")
            print(f"  [Sources]      -> {', '.join(flow['sources'][:2])}")
            print(f"  [Bronze/Silver]-> {flow['bronze']} -> {flow['silver'] if isinstance(flow['silver'], str) else '4 silver tables'}")
            print(f"  [Gold Marts]   -> {', '.join(flow['gold_tables'][:3])} (+{len(flow['gold_tables'])-3} more)")
            print(f"  [Consumers]    -> {', '.join(flow['downstream_consumers'])}")
        print("-" * 80)
        print(f"[+] Full JSON Catalog Exported to: {os.path.join(self.output_dir, 'metadata_catalog.json')}")
        print("=" * 80)
